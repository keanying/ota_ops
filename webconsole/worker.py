"""浏览器工作线程：把服务器上的 Chromium 变成可远程实时操作的对象。

两种画面传输模式
----------------
``vnc``（默认，真正的实时流）
    浏览器跑在 Xvfb 虚拟显示上，x11vnc 用 RFB 协议做**增量矩形更新**——
    只传变化的那块区域，事件驱动，静止时零流量。鼠标键盘由 noVNC 直接送到
    X 输入栈，与本地操作完全同源，拖拽滑块跟手。
    浏览器还能跑**有头模式**，navigator.webdriver 为 undefined、UA 不含
    Headless、innerHeight 与窗口一致。

``cdp``（兜底，装不了 x11vnc 时用）
    CDP ``Page.startScreencast``：合成器在画面变化时推送整张 JPEG，
    无帧间压缩，带宽比 VNC 大不少；且静止页面不推帧，需要补一张关键帧
    才有画面。输入用 ``Input.dispatchMouseEvent`` 等派发，是真事件。

为什么要单开一个线程
--------------------
Playwright 同步 API 绑定创建它的线程，不能跨线程调用。因此这里用
「命令队列进、事件队列出」的模式：FastAPI 的 asyncio 侧只跟队列打交道。

另一个坑：同步 API 只在执行 Playwright 调用期间派发事件。若主循环一直
空转，``Page.screencastFrame`` 回调永远不会触发。所以循环里必须周期性
调用 ``page.wait_for_timeout()`` 来「泵」事件。
"""

from __future__ import annotations

import os
import queue
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from ctrip.logging_utils import get_logger

# 与 browser.py 的 _context_kwargs 保持一致；坐标换算依赖它
VIEWPORT = {"width": 1440, "height": 1080}

SCREENCAST_OPTS = {
    "format": "jpeg",
    "quality": 60,
    "maxWidth": VIEWPORT["width"],
    "maxHeight": VIEWPORT["height"],
    "everyNthFrame": 1,
}

BUTTONS = {"left": 1, "right": 2, "middle": 4}


class BrowserWorker(threading.Thread):
    """持有一个账号的浏览器会话，对外只暴露队列。"""

    def __init__(self, cfg, account_id: str, *, mode: str = "vnc",
                 display=None, keep_alive: bool = True) -> None:
        super().__init__(name=f"browser-{account_id}", daemon=True)
        self.cfg = cfg
        self.account_id = account_id
        self.mode = mode                  # vnc | cdp
        self.display = display            # Display 实例（vnc 模式）
        self.keep_alive = keep_alive      # 浏览器常驻，下次直接 CDP 复用
        self.log = get_logger(f"console.{account_id}")

        self.cmds: queue.Queue[dict] = queue.Queue()
        self.frames: queue.Queue[dict] = queue.Queue(maxsize=3)  # 只留最新几帧
        self.events: queue.Queue[dict] = queue.Queue()

        # 注意：不能叫 _stop —— Thread 内部有个同名方法，覆盖后 join() 会崩
        self._stopping = threading.Event()
        self._ready = threading.Event()
        self._acks: list[str] = []
        self._pending_page: Any = None
        self._session: Any = None
        self._page: Any = None
        self._cdp: Any = None
        self.error: str | None = None
        self.started_at: str = ""
        self.frame_count = 0
        self.last_url = ""
        self.quality = SCREENCAST_OPTS["quality"]
        self.surface = dict(VIEWPORT)
        self.login_active = False
        self.login_flow = None
        self._need_viewport_fix = False
        self._last_frame_at = 0.0

    # ------------------------------------------------------------ 对外
    def submit(self, cmd: dict) -> None:
        self.cmds.put(cmd)

    def emit(self, kind: str, **payload: Any) -> None:
        self.events.put({"type": kind, "at": datetime.now().strftime("%H:%M:%S"),
                         **payload})

    def wait_ready(self, timeout: float = 60) -> bool:
        return self._ready.wait(timeout)

    def shutdown(self) -> None:
        self._stopping.set()

    @property
    def alive(self) -> bool:
        return self.is_alive() and not self._stopping.is_set()

    # ------------------------------------------------------------ 线程体
    def run(self) -> None:
        from ctrip.browser import BrowserBusyError, open_session

        try:
            if self.mode == "vnc":
                # Playwright 从进程环境读 DISPLAY，必须在 launch 之前设好。
                # app 层用锁保证同一时刻只有一个 worker 在启动。
                os.environ["DISPLAY"] = self.display.display
                self.emit("status", state="starting",
                          message=f"正在启动有头浏览器（显示 {self.display.display}）…")
            else:
                first_run = not self.cfg.browser_profile_dir(self.account_id).exists()
                self.emit("status", state="starting",
                          message="正在启动无头浏览器…" + (
                              "（新账号首次启动要初始化浏览器目录，会慢一些）"
                              if first_run else ""))
            # cdp 模式跑无头；只有 vnc 需要有头（x11vnc 得有窗口才能抓）。
            #
            # keep_alive=True 是关键：浏览器作为独立进程常驻并监听 CDP 端口。
            # 好处是「已经开着的账号直接连上去」——控制台重启、切走再切回来、
            # 命令行采集，都复用同一个浏览器，既不用重新登录，也不会撞上
            # 「profile 正被占用」。open_session 内部按 CDP 复用 → 常驻拉起 →
            # 持久化 profile 的顺序自己挑，这里只是允许它走前两条路。
            self._session = open_session(self.cfg, self.account_id,
                                         headless=(self.mode != "vnc"),
                                         keep_alive=self.keep_alive, log=self.log)
            if self._session.reused:
                self.emit("log", level="info",
                          message="已连上该账号正在运行的浏览器，无需重新启动")
        except BrowserBusyError as exc:
            self.error = str(exc)
            self.emit("status", state="error", message=str(exc))
            self._ready.set()
            return
        except Exception as exc:  # noqa: BLE001
            self.error = f"浏览器启动失败：{exc}"
            self.emit("status", state="error", message=self.error)
            self._ready.set()
            return

        self.started_at = datetime.now().strftime("%H:%M:%S")
        context = self._session.context

        # 站点可能新开标签页（如登录后跳转），要跟着切过去，否则画面卡在旧页
        context.on("page", self._on_new_page)

        try:
            page = self._session.page()
            self._attach(page)
            self._ready.set()
            self.emit("status", state="live", message="浏览器已就绪，画面开始推送")

            # 停在 about:blank 上没有任何内容可看，直接打开登录页。
            # 用 commit 语义，页面一开始渲染就有帧推过来，不必干等整页加载完。
            try:
                if page.url in ("", "about:blank"):
                    self._nav({"action": "goto"})
            except Exception as exc:  # noqa: BLE001
                self.emit("log", level="warn", message=f"打开登录页失败：{exc}")
            self._loop()
        except Exception as exc:  # noqa: BLE001
            self.error = str(exc)
            self.emit("status", state="error", message=f"会话异常：{exc}")
        finally:
            self._teardown()

    def _loop(self) -> None:
        idle = 0
        while not self._stopping.is_set():
            handled = 0
            while True:
                try:
                    cmd = self.cmds.get_nowait()
                except queue.Empty:
                    break
                try:
                    self._dispatch(cmd)
                except Exception as exc:  # noqa: BLE001
                    self.emit("log", level="warn",
                              message=f"操作失败 {cmd.get('type')}：{exc}")
                handled += 1
                if handled > 200:      # 输入洪峰时先让出，保证画面继续推
                    break

            if self._pending_page is not None:
                page, self._pending_page = self._pending_page, None
                try:
                    self._attach(page)
                    self.emit("log", level="info", message=f"已切换到新标签页：{page.url}")
                except Exception as exc:  # noqa: BLE001
                    self.emit("log", level="warn", message=f"切换标签页失败：{exc}")

            # 泵事件：同步 API 只在 Playwright 调用期间派发 screencastFrame
            try:
                self._page.wait_for_timeout(20 if handled else 40)
            except Exception:  # noqa: BLE001
                if self._stopping.is_set():
                    break
                time.sleep(0.1)

            self.flush_acks()

            if self.mode == "cdp" and self._need_viewport_fix:
                self._need_viewport_fix = False
                self._fix_viewport()

            idle += 1
            if idle % 25 == 0:
                self._report_location()

    def _align_viewport(self) -> None:
        """让「页面视口」与「实际能看到的画面」一致。

        headless 下 Playwright 把 viewport 设成 1440x1080，但浏览器窗口的合成
        表面只有 1440x941 —— 页面以为自己有 1080 高，实际只有 941 会出现在帧里。
        底部那 139px 渲染了却看不见也点不到，登录按钮要是在那儿就没法用。

        先把窗口撑到目标尺寸；撑不动就反过来把视口缩到窗口大小，
        两者对齐即可，1:1 映射才成立。
        """
        try:
            win = self._cdp.send("Browser.getWindowForTarget")
            self._cdp.send("Browser.setWindowBounds", {
                "windowId": win["windowId"],
                "bounds": {"width": VIEWPORT["width"], "height": VIEWPORT["height"],
                           "windowState": "normal"},
            })
            self._page.wait_for_timeout(200)
        except Exception as exc:  # noqa: BLE001
            self.emit("log", level="warn", message=f"调整窗口尺寸失败：{exc}")

        # 复核：拿一帧的 metadata 看真实可视区，不一致就把视口对齐过去
        try:
            metrics = self._cdp.send("Page.getLayoutMetrics")
            vv = metrics.get("cssVisualViewport") or {}
            css_w = int(vv.get("clientWidth") or VIEWPORT["width"])
            css_h = int(vv.get("clientHeight") or VIEWPORT["height"])
        except Exception:  # noqa: BLE001
            return
        self.surface = {"width": css_w, "height": css_h}

    def keyframe(self) -> None:
        """补一张首帧。

        ``Page.startScreencast`` 只在画面**有变化**时推帧——这是对的，
        画面没变本来就没有新东西可传，canvas 保留上一帧即可。
        但刚绑定页面时 canvas 还是空的，需要一张起始帧垫底。

        **只在这些时机调用**：绑定页面、导航完成、手动点「重取画面」。
        不做任何周期性轮询——那就退化成截图流了。
        """
        try:
            shot = self._cdp.send("Page.captureScreenshot",
                                  {"format": "jpeg", "quality": self.quality})
        except Exception:  # noqa: BLE001
            return
        data = shot.get("data")
        if not data:
            return
        self.frame_count += 1
        frame = {"type": "frame", "data": data, "meta": {}, "n": self.frame_count,
                 "keyframe": True}
        try:
            self.frames.put_nowait(frame)
        except queue.Full:
            try:
                self.frames.get_nowait()
                self.frames.put_nowait(frame)
            except queue.Empty:
                pass
        self._last_frame_at = time.monotonic()

    def _fix_viewport(self) -> None:
        """帧里的可视区与页面视口不一致时，把页面视口对齐到可视区。

        这样页面布局就是你看到的样子，不会有「渲染了但永远看不到」的区域。
        """
        try:
            metrics = self._cdp.send("Page.getLayoutMetrics")
            vv = metrics.get("cssVisualViewport") or {}
            if int(vv.get("clientHeight") or 0) == self.surface["height"]:
                return
            self._cdp.send("Emulation.setDeviceMetricsOverride", {
                "width": self.surface["width"], "height": self.surface["height"],
                "deviceScaleFactor": 1, "mobile": False,
            })
            self.emit("log", level="info",
                      message=f"视口已对齐到实际可视区 "
                              f"{self.surface['width']}×{self.surface['height']}")
        except Exception as exc:  # noqa: BLE001
            self.emit("log", level="warn", message=f"对齐视口失败：{exc}")

    def _report_location(self, force: bool = False) -> None:
        """页面地址/标题变化时通知前端。标题也是「输入是否真的生效」的观察点。"""
        try:
            url, title = self._page.url, self._page.title()
        except Exception:  # noqa: BLE001
            return
        stamp = f"{url}\n{title}"
        if force or stamp != self.last_url:
            self.last_url = stamp
            self.emit("url", url=url, title=title)

    def flush_acks(self) -> None:
        """发送积压的帧确认。

        ``Page.screencastFrameAck`` 是**必须**的：浏览器收不到上一帧的确认
        就不再推下一帧。所以凡是「泵了事件」的地方，都必须跟着把 ack 发出去。

        登录流程会长时间占用工作线程，主循环进不来；如果只在主循环里发 ack，
        推流会在一两帧后彻底停住 —— 表现就是「登录期间画面定格，中止登录后
        才突然刷新」。所以 LoginFlow._pump 也要调这个方法。

        必须在事件回调之外调用，避免在回调里重入 CDP。
        """
        while self._acks:
            sid = self._acks.pop(0)
            try:
                self._cdp.send("Page.screencastFrameAck", {"sessionId": sid})
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------ 页面绑定
    def _on_new_page(self, page) -> None:
        self._pending_page = page

    def _attach(self, page) -> None:
        if self._cdp is not None:
            try:
                self._cdp.send("Page.stopScreencast")
            except Exception:  # noqa: BLE001
                pass
            try:
                self._cdp.detach()
            except Exception:  # noqa: BLE001
                pass

        self._page = page
        try:
            page.set_viewport_size(VIEWPORT)
        except Exception:  # noqa: BLE001
            pass          # persistent context 下可能不允许改，忽略
        self._cdp = self._session.context.new_cdp_session(page)
        self._cdp.on("Page.screencastFrame", self._on_frame)
        self.last_url = ""
        if self.mode == "vnc":
            # 画面由 x11vnc 负责，CDP 只用来导航、填表、保存登录态
            return
        self._align_viewport()
        opts = dict(SCREENCAST_OPTS)
        opts["quality"] = self.quality
        self._cdp.send("Page.startScreencast", opts)
        self.keyframe()          # 静止页面也要先有画面

    def _on_frame(self, params: dict) -> None:
        self.frame_count += 1
        sid = params.get("sessionId")
        if sid is not None:
            # 优先在回调里立刻确认：screencast 逐帧确认，晚一步浏览器就
            # 不推下一帧。等到主循环或 _pump 才 ack 的话，单个耗时几秒的
            # Playwright 调用期间画面就是定格的。
            # Playwright 同步 API 允许在事件回调里再调 API（对话框处理就是
            # 这个模式）；万一某个版本不允许，退回到延迟确认，不让它中断推流。
            try:
                self._cdp.send("Page.screencastFrameAck", {"sessionId": sid})
            except Exception:  # noqa: BLE001
                self._acks.append(sid)
        self._last_frame_at = time.monotonic()
        meta = params.get("metadata") or {}
        dw, dh = meta.get("deviceWidth"), meta.get("deviceHeight")
        if dw and dh and (dw, dh) != (self.surface["width"], self.surface["height"]):
            self.surface = {"width": int(dw), "height": int(dh)}
            self._need_viewport_fix = True
        frame = {"type": "frame", "data": params.get("data", ""),
                 "meta": params.get("metadata", {}), "n": self.frame_count}
        try:
            self.frames.put_nowait(frame)
        except queue.Full:
            # 前端跟不上时丢旧帧，保证看到的始终是最新画面
            try:
                self.frames.get_nowait()
                self.frames.put_nowait(frame)
            except queue.Empty:
                pass

    # ------------------------------------------------------------ 命令派发
    def _dispatch(self, cmd: dict) -> None:
        kind = cmd.get("type")
        if kind == "mouse":
            self._mouse(cmd)
        elif kind == "wheel":
            self._wheel(cmd)
        elif kind == "key":
            self._key(cmd)
        elif kind == "text":
            self._cdp.send("Input.insertText", {"text": cmd.get("text", "")})
        elif kind == "nav":
            self._nav(cmd)
        elif kind == "fill_login":
            self._fill_login()
        elif kind == "login_start":
            self._run_login(cmd)
        elif kind == "sms_code":
            # 不在登录流程中收到验证码：直接填进当前页面的验证码框
            self._fill_sms_direct(str(cmd.get("code", "")))
        elif kind == "save_state":
            self._save_state()
        elif kind == "refresh_frame":
            self.keyframe()
        elif kind == "quality":
            opts = dict(SCREENCAST_OPTS)
            self.quality = max(20, min(90, int(cmd.get("value", 60))))
            opts["quality"] = self.quality
            self._cdp.send("Page.stopScreencast")
            self._cdp.send("Page.startScreencast", opts)
            self.keyframe()
            self.emit("log", level="info", message=f"画质已设为 {self.quality}")

    def _mouse(self, cmd: dict) -> None:
        action = cmd.get("action")
        types = {"move": "mouseMoved", "down": "mousePressed", "up": "mouseReleased"}
        button = cmd.get("button", "left")
        payload = {
            "type": types.get(action, "mouseMoved"),
            "x": float(cmd.get("x", 0)),
            "y": float(cmd.get("y", 0)),
            "modifiers": int(cmd.get("modifiers", 0)),
            "button": button if action != "move" else cmd.get("button", "none"),
            "buttons": int(cmd.get("buttons", 0)),
            "clickCount": int(cmd.get("clickCount", 1)) if action != "move" else 0,
        }
        self._cdp.send("Input.dispatchMouseEvent", payload)

    def _wheel(self, cmd: dict) -> None:
        self._cdp.send("Input.dispatchMouseEvent", {
            "type": "mouseWheel",
            "x": float(cmd.get("x", 0)),
            "y": float(cmd.get("y", 0)),
            "deltaX": float(cmd.get("deltaX", 0)),
            "deltaY": float(cmd.get("deltaY", 0)),
            "modifiers": int(cmd.get("modifiers", 0)),
            "pointerType": "mouse",
        })

    def _key(self, cmd: dict) -> None:
        action = cmd.get("action", "down")
        key = cmd.get("key", "")
        text = cmd.get("text", "")
        payload = {
            "type": "keyDown" if action == "down" else "keyUp",
            "key": key,
            "code": cmd.get("code", ""),
            "windowsVirtualKeyCode": int(cmd.get("keyCode", 0)),
            "nativeVirtualKeyCode": int(cmd.get("keyCode", 0)),
            "modifiers": int(cmd.get("modifiers", 0)),
        }
        # 可打印字符要带 text，否则输入框收不到内容
        if action == "down" and text and len(text) == 1:
            payload["text"] = text
            payload["unmodifiedText"] = text
            payload["type"] = "keyDown"
        self._cdp.send("Input.dispatchKeyEvent", payload)

    def _nav(self, cmd: dict) -> None:
        """导航。

        用 ``wait_until="commit"`` 而不是 ``domcontentloaded``：
        commit 在服务端响应刚到、开始渲染时就返回，后续加载过程由推流呈现，
        你能一边看着页面出内容一边操作。等 domcontentloaded 会把工作线程
        阻塞到整页 DOM 就绪为止 —— 这期间事件泵停转，既没有帧、也接不了
        鼠标键盘，表现就是「点完之后画面死住好几秒」。
        """
        action = cmd.get("action")
        timeout = self.cfg.ctrip.nav_timeout * 1000
        if action == "goto":
            url = cmd.get("url") or self.cfg.ctrip.login_url
            self.emit("log", level="info", message=f"打开 {url}")
            self._page.goto(url, wait_until="commit", timeout=timeout)
        elif action == "back":
            self._page.go_back(wait_until="commit", timeout=timeout)
        elif action == "forward":
            self._page.go_forward(wait_until="commit", timeout=timeout)
        elif action == "reload":
            self._page.reload(wait_until="commit", timeout=timeout)
        self._report_location(force=True)
        if self.mode == "cdp":
            self.keyframe()      # 导航后垫一张首帧，等首次变化再由推流接管

    # ------------------------------------------------------------ 业务动作
    def _account(self):
        for a in self.cfg.ctrip.accounts:
            if a.account_id == self.account_id:
                return a
        return None

    def _fill_login(self) -> None:
        """走项目自己的填表逻辑，选择器与 login() 完全一致。"""
        from ctrip.auth import _click_login, _dismiss_popups, _fill_credentials

        account = self._account()
        if account is None or not account.account or not account.password:
            self.emit("log", level="warn",
                      message="账号或密码为空，请在浏览器里手动输入")
            return
        _dismiss_popups(self._page, self.log)
        if not _fill_credentials(self._page, account, self.log):
            self.emit("log", level="warn", message="未定位到登录表单，请手动填写")
            return
        self.emit("log", level="info", message="已填入账号密码")
        if _click_login(self._page, self.log):
            self.emit("log", level="info",
                      message="已提交登录，若出现验证码请在画面中直接操作")
        else:
            self.emit("log", level="warn", message="未能自动点击登录按钮，请手动点击")

    def _run_login(self, cmd: dict) -> None:
        """跑完整登录编排。整个过程占用工作线程，期间靠 LoginFlow 自己排空命令队列。"""
        from .login_flow import LoginFlow, LoginRequest
        from .notify import Settings

        if self.login_active:
            self.emit("log", level="warn", message="已有登录流程在跑，忽略重复请求")
            return
        req = LoginRequest(
            account_id=self.account_id,
            account=cmd.get("account", "") or "",
            password=cmd.get("password", "") or "",
            timeout=int(cmd.get("timeout") or 600),
            sms_wait=int(cmd.get("sms_wait") or 300),
            persist=bool(cmd.get("persist", True)),
        )
        self.login_active = True
        try:
            flow = LoginFlow(self, req, Settings.load().selectors,
                             on_event=self._on_login_event)
            self.login_flow = flow
            flow.run()
        except Exception as exc:  # noqa: BLE001
            self.emit("login", stage="failed", message=f"登录流程异常：{exc}")
        finally:
            self.login_active = False
            self.login_flow = None

    def _on_login_event(self, stage: str, payload: dict) -> None:
        if stage in ("polling", "filled"):
            # 这两个是过程播报，不是状态迁移，单独走一类事件，
            # 免得前端把步骤条改错
            self.events.put({"type": "login_note", "stage": stage,
                             "at": datetime.now().strftime("%H:%M:%S"), **payload})
            return
        if stage == "config_changed":
            self.events.put({"type": "config_changed",
                             "account_id": self.account_id,
                             "fields": payload.get("fields", []),
                             "at": datetime.now().strftime("%H:%M:%S")})
            return
        if stage in ("notify_captcha", "notify_sms"):
            self.events.put({"type": stage, "account_id": self.account_id,
                             "hint": payload.get("hint", ""),
                             "phone": payload.get("phone", ""),
                             "at": datetime.now().strftime("%H:%M:%S")})
            return
        self.events.put({"type": "login", "stage": stage,
                         "at": datetime.now().strftime("%H:%M:%S"), **payload})

    def _fill_sms_direct(self, code: str) -> None:
        from .notify import Settings

        if not code:
            return
        sel = Settings.load().selectors
        for css in sel.sms_input_selectors:
            try:
                loc = self._page.locator(css).first
                if loc.is_visible(timeout=500):
                    loc.fill(code, timeout=3000)
                    self.emit("log", level="info", message=f"已填入验证码（{css}）")
                    return
            except Exception:  # noqa: BLE001
                continue
        self.emit("log", level="warn", message="页面上没找到验证码输入框")

    def _save_state(self) -> None:
        """把当前浏览器登录态落盘到 auth/{account_id}/，与 login() 行为一致。"""
        from ctrip.auth import CookieStore, verify_cookie

        account = self._account()
        cookie_file: Path = self.cfg.cookie_file(self.account_id)
        cookie_file.parent.mkdir(parents=True, exist_ok=True)

        # 先进后台首页，确保业务域 Cookie 也写进来
        try:
            self._page.goto(self.cfg.ctrip.home_url, wait_until="domcontentloaded",
                            timeout=self.cfg.ctrip.nav_timeout * 1000)
            self._page.wait_for_timeout(2500)
        except Exception as exc:  # noqa: BLE001
            self.emit("log", level="warn", message=f"访问首页失败（仍继续保存）：{exc}")

        self._session.context.storage_state(path=str(cookie_file))
        CookieStore(self.cfg).save_meta(
            self.account_id,
            login_at=datetime.now().isoformat(timespec="seconds"),
            login_name=account.account if account else "",
            headless=True,
            saved_from="webconsole",
        )
        self.emit("log", level="info", message=f"登录态已保存：{cookie_file}")

        ok, detail = verify_cookie(self.cfg, self.account_id, self.log)
        self.emit("log", level="info" if ok else "error",
                  message=f"登录态校验：{detail}")
        self.emit("auth", ok=ok, detail=detail)

    # ------------------------------------------------------------ 收尾
    def _teardown(self) -> None:
        try:
            if self._cdp is not None:
                try:
                    self._cdp.send("Page.stopScreencast")
                except Exception:  # noqa: BLE001
                    pass
            if self._session is not None:
                # CDP 模式下 close() 只断开连接、不关浏览器（见 BrowserSession.close），
                # 所以常驻浏览器会留着，下次秒连。要真正关掉用「释放 profile 占用」。
                self._session.close()
            if self.mode == "vnc" and self.display is not None:
                self.display.stop()
        except Exception as exc:  # noqa: BLE001
            self.log.warning("关闭浏览器会话失败：%s", exc)
        self.emit("status", state="closed", message="浏览器已关闭")
