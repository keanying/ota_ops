"""登录编排状态机。

流程
----
    收到账号密码（WebSocket 推入）
        ↓ 打开登录页、填表、点登录
        ↓ 轮询页面，判断落到了哪种状态
        ├── 滑块/图形验证 ── 发飞书通知 ── 人工在 VNC 画面里拖 ── 继续轮询
        ├── 短信验证      ── 自动点「获取验证码」── 等客户端回传 6 位码
        │                    ── 自动填入 ── 自动点登录 ── 继续轮询
        ├── 登录成功      ── 保存 storage_state + meta ── 校验 ── 结束
        └── 超时/失败     ── 落盘截图与 HTML 证据 ── 结束

状态机跑在浏览器工作线程里（Playwright 同步 API 不能跨线程），
轮询期间会主动排空命令队列，好接住客户端随时送来的短信验证码或中止指令。
"""

from __future__ import annotations

import copy
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable


# 两次点击「获取验证码」的最小间隔（秒）。短信有额度，别反复点。
SMS_RESEND_GAP = 60

# 轮询期间的播报间隔（秒）。太密会刷屏，太疏会让人以为卡死了。
HEARTBEAT_GAP = 5

# 泵事件的切片粒度（毫秒）。每片结束都发一次帧确认，直接决定登录期间的帧率。
PUMP_SLICE = 80


class Stage(str, Enum):
    IDLE = "idle"
    OPENING = "opening"          # 打开登录页
    FILLING = "filling"          # 填账号密码
    SUBMITTED = "submitted"      # 已点登录，等页面反应
    NEED_CAPTCHA = "need_captcha"  # 需要人工滑块/图形验证
    NEED_SMS = "need_sms"        # 需要手机短信验证码
    SMS_FILLING = "sms_filling"  # 收到码，正在填并提交
    SUCCESS = "success"
    FAILED = "failed"
    ABORTED = "aborted"


@dataclass
class LoginRequest:
    account_id: str
    account: str
    password: str = field(repr=False, default="")
    name: str = ""
    timeout: int = 600           # 整个流程上限（秒），人工验证需要时间
    sms_wait: int = 300          # 等短信验证码的上限（秒）
    persist: bool = True         # 登录成功后把凭据写回 config.yaml


class LoginFlow:
    """一次登录尝试。由 BrowserWorker 在自己的线程里驱动。"""

    def __init__(self, worker, req: LoginRequest, selectors,
                 on_event: Callable[[str, dict], None]) -> None:
        self.w = worker
        self.req = req
        self.sel = selectors
        self.on_event = on_event
        self.stage = Stage.IDLE
        self.sms_code: str | None = None
        self.aborted = False
        self.started = time.monotonic()
        self.credential_source = "配置文件"
        self.account_used = ""
        self.notified_captcha = False
        self.notified_sms = False
        # 用 None 而不是 0.0：monotonic() 从开机算起，机器刚启动时接近 0，
        # 用 0.0 做基准会让「距上次发送超过 60 秒」在头一分钟里恒为假，
        # 结果是服务重启后第一次登录永远不点「获取验证码」。
        self.sms_sent_at: float | None = None

    # ------------------------------------------------------------ 工具
    @property
    def page(self):
        return self.w._page

    @property
    def log(self):
        return self.w.log

    def emit(self, stage: Stage, message: str, **extra) -> None:
        self.stage = stage
        self.on_event(stage.value, {"message": message, **extra})

    def _elapsed(self) -> float:
        return time.monotonic() - self.started

    def _pump(self, ms: int = 400) -> None:
        """让出时间片：泵 Playwright 事件，同时接住新命令（短信码 / 中止）。"""
        import queue as _q

        # 切成小片轮流「泵事件 + 发帧确认」。
        # screencast 是逐帧确认的：上一帧没 ack，浏览器就不推下一帧。
        # 等满 ms 再统一 ack 的话，每个周期只能过一帧（700ms 就是 1.4 fps，
        # 看起来仍然很卡）。切片后每 80ms 放行一次，约 12 fps。
        remaining = max(ms, PUMP_SLICE)
        while remaining > 0:
            slice_ms = min(PUMP_SLICE, remaining)
            remaining -= slice_ms
            try:
                self.page.wait_for_timeout(slice_ms)
            except Exception:  # noqa: BLE001
                time.sleep(slice_ms / 1000)
            try:
                self.w.flush_acks()
            except Exception:  # noqa: BLE001
                pass
        while True:
            try:
                cmd = self.w.cmds.get_nowait()
            except _q.Empty:
                break
            kind = cmd.get("type")
            if kind == "sms_code":
                self.sms_code = str(cmd.get("code", "")).strip()
                self.log.info("收到短信验证码（%d 位）", len(self.sms_code))
            elif kind == "login_abort":
                self.aborted = True
            else:
                # 登录期间其他命令照常执行（比如手动导航、重取画面）
                try:
                    self.w._dispatch(cmd)
                except Exception:  # noqa: BLE001
                    pass

    def _fill_step_by_step(self, account) -> bool:
        """自己走一遍填表，每一步之间把帧确认发出去。

        ``ctrip.auth._fill_credentials`` 内部是一串连续的阻塞调用，中间没有
        任何让我们插手的机会 —— screencast 逐帧确认的机制下，这几秒里
        浏览器只推得出一帧，画面就定格了（实测过 28 秒空白）。
        这里复用它的选择器常量，把步骤拆开，每步之后 pump 一次。
        选择器仍是项目的唯一真源，页面改版只需要改 ctrip/auth.py。
        """
        from ctrip.auth import (_PASSWORD_SELECTORS, _USERNAME_SELECTORS,
                                _first_visible)

        user = _first_visible(self.page, _USERNAME_SELECTORS, timeout=4000)
        self._pump(PUMP_SLICE)
        if user is None:
            self.log.error("未找到账号输入框")
            return False
        user.fill(account.account)
        self.log.info("已填入登录账号")
        self.on_event("filled", {"message": f"已填入账号 {account.account}"})
        self._pump(PUMP_SLICE * 2)

        pwd = _first_visible(self.page, _PASSWORD_SELECTORS, timeout=4000)
        self._pump(PUMP_SLICE)
        if pwd is None:
            self.log.error("未找到密码输入框")
            return False
        pwd.fill(account.password)
        self.log.info("已填入密码")
        self.on_event("filled", {"message": "已填入密码"})
        self._pump(PUMP_SLICE * 2)

        # 勾选「阅读并同意」：携程用自定义样式，直接 check input 常常无效，
        # 所以先 check、失败再点 label —— 与项目原实现保持一致
        try:
            box = self.page.locator('input[type="checkbox"]').first
            box.wait_for(state="attached", timeout=1500)
            self._pump(PUMP_SLICE)
            if not box.is_checked():
                try:
                    box.check(force=True, timeout=2000)
                except Exception:  # noqa: BLE001
                    self.page.locator('label:has(input[type="checkbox"])').first.click(
                        timeout=2000)
                self.on_event("filled", {"message": "已勾选用户协议"})
        except Exception:  # noqa: BLE001
            self.log.debug("未发现协议勾选框，跳过")
        self._pump(PUMP_SLICE)
        return True

    def _click_login_step_by_step(self) -> bool:
        """逐个候选按钮尝试点击，每次尝试之间发帧确认。

        原实现连着试 3 个选择器、每个等 2.5 秒，最坏 7.5 秒里画面一动不动。
        按钮文案仍取自 ctrip.auth 的常量，页面改版只改那一处。
        """
        import re as _re

        from ctrip.auth import _LOGIN_BUTTON_PATTERNS

        for pattern in _LOGIN_BUTTON_PATTERNS:
            try:
                btn = self.page.get_by_role("button", name=_re.compile(pattern)).first
                btn.wait_for(state="visible", timeout=2500)
                self._pump(PUMP_SLICE)
                btn.click()
                self.log.info("已点击登录按钮（%s）", pattern)
                return True
            except Exception:  # noqa: BLE001
                self._pump(PUMP_SLICE)
                continue
        try:
            self.page.keyboard.press("Enter")     # 兜底：回车提交
            self.log.info("未匹配到登录按钮，改用回车提交")
            return True
        except Exception as exc:  # noqa: BLE001
            self.log.error("提交登录表单失败：%s", exc)
            return False

    def _text(self) -> str:
        try:
            return self.page.inner_text("body", timeout=2500) or ""
        except Exception:  # noqa: BLE001
            return ""

    # ------------------------------------------------------------ 检测
    def detect_captcha(self) -> str | None:
        """滑块/图形验证：先看文案，再看典型元素，避免只靠文案漏判。"""
        body = self._text()
        for hint in self.sel.captcha_hints:
            if hint in body:
                return hint
        for css in self.sel.captcha_selectors:
            try:
                loc = self.page.locator(css).first
                if loc.is_visible(timeout=400):
                    return f"检测到验证元素 {css}"
            except Exception:  # noqa: BLE001
                continue
        return None

    def find_sms_input(self):
        for css in self.sel.sms_input_selectors:
            try:
                loc = self.page.locator(css).first
                if loc.is_visible(timeout=400):
                    return loc
            except Exception:  # noqa: BLE001
                continue
        return None

    def find_sms_button(self):
        for text in self.sel.sms_send_texts:
            try:
                btn = self.page.get_by_role("button", name=re.compile(text)).first
                if btn.is_visible(timeout=400):
                    return btn
            except Exception:  # noqa: BLE001
                pass
            try:
                btn = self.page.locator(f'text="{text}"').first
                if btn.is_visible(timeout=400):
                    return btn
            except Exception:  # noqa: BLE001
                continue
        return None

    def detect_sms(self) -> bool:
        """短信验证：既要有验证码输入框，也要有发送按钮或页面文案佐证，
        否则容易把图形验证码输入框误判成短信验证。"""
        if self.find_sms_input() is None:
            return False
        if self.find_sms_button() is not None:
            return True
        body = self._text()
        return any(h in body for h in self.sel.sms_page_hints)

    def is_logged_in(self) -> bool:
        from ctrip.auth import _is_logged_in_url

        try:
            return _is_logged_in_url(self.page.url)
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------ 主流程
    def run(self) -> Stage:
        from ctrip.auth import _dismiss_popups

        cfg = self.w.cfg
        account = self.w._account()
        if account is None:
            self.emit(Stage.FAILED, f"配置里没有账号 {self.req.account_id}")
            return self.stage

        # WebSocket 传来的账号密码优先于配置文件。
        # 关键：用副本，不改共享的配置对象 —— 直接赋值会让这次登录的凭据
        # 悄悄影响后续所有操作（比如页面上的「自动填账号密码」按钮），
        # 而且重启后又变回配置里的值，行为不一致也难排查。
        # 这份副本只在本次登录的内存里存在，不落盘。
        if self.req.account or self.req.password:
            # 用 copy.copy 而不是 dataclasses.replace：前者对任何对象都成立，
            # 不假设账号配置一定是 dataclass（换成 pydantic 模型或别的类型也不会崩）
            account = copy.copy(account)
            if self.req.account:
                account.account = self.req.account
            if self.req.password:
                account.password = self.req.password
            self.credential_source = "接口传入"
        else:
            self.credential_source = "配置文件"

        if not account.account or not account.password:
            self.emit(Stage.FAILED,
                      f"账号或密码为空（凭据来源：{self.credential_source}）")
            return self.stage
        self.account_used = account.account

        # ---- 1. 已登录就直接结束，别白白再触发一次风控 ----
        self.emit(Stage.OPENING, "正在打开登录页")
        try:
            # commit 而非 domcontentloaded：页面一开始渲染就返回，
            # 剩下的加载过程由 _pump 一边泵事件一边等，画面和输入都不中断
            self.page.goto(cfg.ctrip.login_url, wait_until="commit",
                           timeout=cfg.ctrip.nav_timeout * 1000)
        except Exception as exc:  # noqa: BLE001
            self.emit(Stage.FAILED, f"打开登录页失败：{exc}")
            return self.stage
        # 分多次 pump，期间画面持续推送、鼠标键盘可用
        for _ in range(6):
            self._pump(400)
            if self.aborted:
                self.emit(Stage.ABORTED, "已中止登录")
                return self.stage
        _dismiss_popups(self.page, self.log)
        if self.is_logged_in():
            return self._finish_success("profile 里已是登录态，无需重新登录")

        # ---- 2. 填表 + 提交 ----
        self.emit(Stage.FILLING,
                  f"正在填入账号密码：{account.account}"
                  f"（凭据来源：{self.credential_source}）",
                  login_account=account.account,
                  credential_source=self.credential_source)
        # 填表和点登录都是一串阻塞调用，中间插泵：让画面帧继续推、
        # 也让你在这期间发的鼠标键盘事件尽快被处理，而不是排队到几十秒后
        self._pump(PUMP_SLICE)
        if not self._fill_step_by_step(account):
            self.emit(Stage.FAILED, "未找到登录表单，可能页面改版")
            return self.stage
        self.on_event("filled", {"message": "账号密码已填入，正在提交"})
        self._pump(PUMP_SLICE * 2)
        if not self._click_login_step_by_step():
            self.emit(Stage.FAILED, "提交登录表单失败")
            return self.stage
        self._pump(PUMP_SLICE * 3)
        self.emit(Stage.SUBMITTED, "已提交登录，正在识别页面状态")

        # ---- 3. 轮询状态 ----
        last_beat = 0.0
        while self._elapsed() < self.req.timeout:
            self._pump(700)

            # 定期播报当前页面，否则日志会停在「已提交登录」一动不动，
            # 你既不知道它还活着，也不知道页面上到底是什么
            if time.monotonic() - last_beat > HEARTBEAT_GAP:
                last_beat = time.monotonic()
                try:
                    title = self.page.title()
                except Exception:  # noqa: BLE001
                    title = ""
                self.on_event("polling", {
                    "message": f"识别中（已等 {int(self._elapsed())}s）："
                               f"{title or '无标题'} — {self._url()}",
                    "url": self._url(), "title": title,
                    "elapsed": int(self._elapsed()),
                })
            if self.aborted:
                self.emit(Stage.ABORTED, "已中止登录")
                return self.stage

            if self.is_logged_in():
                return self._finish_success("登录成功")

            _dismiss_popups(self.page, self.log)

            # 短信验证优先于滑块判断：短信页上也可能带滑块文案，
            # 但只要出现了验证码输入框，就该走短信分支。
            if self.detect_sms():
                if not self._handle_sms():
                    return self.stage
                continue

            hint = self.detect_captcha()
            if hint:
                self._handle_captcha(hint)
                continue

            self.notified_captcha = False    # 验证消失，允许下次再通知

        self._dump("timeout")
        self.emit(Stage.FAILED, f"登录超时（{self.req.timeout}s），"
                                f"当前地址 {self._url()}")
        return self.stage

    # ------------------------------------------------------------ 分支
    def _handle_captcha(self, hint: str) -> None:
        """滑块/图形验证：机器过不去，通知人来 VNC 画面里拖。"""
        if self.stage != Stage.NEED_CAPTCHA:
            self.emit(Stage.NEED_CAPTCHA,
                      f"需要人工验证：{hint}。请在画面中完成拖动",
                      hint=hint)
        if not self.notified_captcha:
            self.notified_captcha = True
            self.on_event("notify_captcha", {"hint": hint})

    def _handle_sms(self) -> bool:
        """短信验证：自动点发送 → 等客户端回传验证码 → 填入 → 点登录。"""
        btn = self.find_sms_button()
        fresh_enough = (self.sms_sent_at is None
                        or time.monotonic() - self.sms_sent_at > SMS_RESEND_GAP)
        if btn is not None and fresh_enough:
            try:
                btn.click(timeout=3000)
                self.sms_sent_at = time.monotonic()
                self.log.info("已点击「获取验证码」")
            except Exception as exc:  # noqa: BLE001
                self.log.warning("点击获取验证码失败：%s", exc)

        phone = self._guess_phone()
        self.sms_code = None
        self.emit(Stage.NEED_SMS,
                  f"已请求短信验证码{f'（{phone}）' if phone else ''}，等待回传",
                  phone=phone)
        # 短信同样要通知：人可能不在控制台前面，收不到码整个流程就卡死
        if not self.notified_sms:
            self.notified_sms = True
            self.on_event("notify_sms", {"phone": phone})

        deadline = time.monotonic() + self.req.sms_wait
        while time.monotonic() < deadline:
            self._pump(500)
            if self.aborted:
                self.emit(Stage.ABORTED, "已中止登录")
                return False
            if self.is_logged_in():
                self._finish_success("登录成功")
                return False
            if self.sms_code:
                break
        else:
            self.emit(Stage.FAILED, f"等待短信验证码超时（{self.req.sms_wait}s）")
            return False

        code = self.sms_code
        self.sms_code = None
        self.emit(Stage.SMS_FILLING, "已收到验证码，正在填入并提交")

        box = self.find_sms_input()
        if box is None:
            self.emit(Stage.FAILED, "验证码输入框消失，无法填入")
            return False
        try:
            box.fill(code, timeout=3000)
        except Exception as exc:  # noqa: BLE001
            self.emit(Stage.FAILED, f"填入验证码失败：{exc}")
            return False

        self._click_login_step_by_step()
        self._pump(2000)
        return True

    def _guess_phone(self) -> str:
        """从页面文案里抓被打码的手机号，方便人对上是哪个号收码。"""
        m = re.search(r"1\d{2}\*{2,6}\d{2,4}", self._text())
        return m.group(0) if m else ""

    # ------------------------------------------------------------ 收尾
    def _url(self) -> str:
        try:
            return self.page.url
        except Exception:  # noqa: BLE001
            return "?"

    def _dump(self, tag: str) -> None:
        from ctrip.auth import _dump_evidence

        try:
            _dump_evidence(self.page, self.w.cfg.evidence_dir(self.req.account_id),
                           tag, self.log)
        except Exception:  # noqa: BLE001
            pass

    def _finish_success(self, message: str) -> Stage:
        """保存登录态并校验——与项目 login() 的收尾行为保持一致。"""
        from ctrip.auth import CookieStore, verify_cookie

        cfg = self.w.cfg
        try:
            self.page.goto(cfg.ctrip.home_url, wait_until="commit",
                           timeout=cfg.ctrip.nav_timeout * 1000)
            for _ in range(8):
                self._pump(400)
        except Exception as exc:  # noqa: BLE001
            self.log.warning("访问首页失败（仍继续保存）：%s", exc)

        cookie_file = cfg.cookie_file(self.req.account_id)
        cookie_file.parent.mkdir(parents=True, exist_ok=True)
        self.w._session.context.storage_state(path=str(cookie_file))
        CookieStore(cfg).save_meta(
            self.req.account_id,
            login_at=datetime.now().isoformat(timespec="seconds"),
            login_name=self.account_used or self.req.account or "",
            headless=False,
            saved_from="webconsole-login",
        )
        ok, detail = verify_cookie(cfg, self.req.account_id, self.log)

        # 只有真的登进去了才保存凭据：存一个登不进去的密码没有意义，
        # 还会覆盖掉配置里原本可能正确的那个。
        saved = {}
        if ok and self.req.persist and self.credential_source == "接口传入":
            try:
                from .accounts_admin import save_credentials
                fresh, saved = save_credentials(
                    cfg, self.req.account_id,
                    account=self.account_used, password=self.req.password)
                if saved:
                    self.log.info("凭据已写回 config.yaml：%s", list(saved))
                    self.on_event("config_changed", {"fields": list(saved)})
            except Exception as exc:  # noqa: BLE001
                self.log.warning("保存凭据失败（不影响本次登录）：%s", exc)
                saved = {"error": str(exc)}

        note = ""
        if saved and "error" not in saved:
            note = f"；凭据已保存到配置（{'、'.join(saved)}）"
        elif saved:
            note = f"；凭据保存失败：{saved['error']}"
        self.emit(Stage.SUCCESS if ok else Stage.FAILED,
                  f"{message}；登录态校验：{detail}{note}",
                  verified=ok, cookie_file=str(cookie_file),
                  persisted=bool(saved and "error" not in saved),
                  persisted_fields=[k for k in saved if k != "error"])
        return self.stage
