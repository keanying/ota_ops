"""浏览器持久化与常驻会话管理。

为什么需要这个模块
------------------
原实现用 ``p.chromium.launch()`` + ``browser.new_context()``，每次运行都是
**全新的临时 profile**：没有历史 localStorage / IndexedDB / 缓存，Canvas 与
WebGL 指纹、device id 每次都可能不同。虽然 ``storage_state.json`` 保存了
Cookie，但那只是 Cookie —— 设备指纹与浏览器本地存储并未保留。携程风控看到的
是「陌生设备再次登录」，于是反复弹滑块或短信验证。

本模块改用 ``launch_persistent_context(user_data_dir=...)``，把 Chromium 的
用户数据目录固定在项目内 ``browser/{account_id}/profile/``。这样：

* Cookie、localStorage、IndexedDB、缓存、Service Worker 全部留在磁盘上；
* 设备指纹（含 device id 类持久化标识）在多次运行间保持一致；
* 对服务端而言是「同一台设备、同一个浏览器、已有登录态」，不再触发验证。

两种复用模式
------------
``mode=profile``（默认，推荐）
    进程退出但 profile 留在磁盘上。下次启动读取同一 profile，
    启动开销约 1~2 秒。稳健、无守护进程，适合定时任务。

``mode=cdp``（可选，``cdp_port > 0`` 且已有常驻浏览器时自动启用）
    浏览器常驻不退出，监听 CDP 远程调试端口，后续运行
    ``connect_over_cdp`` 直接复用已打开的浏览器 —— 连页面都不用重开。
    适合「希望浏览器一直挂着」的场景。

两者共用同一个 profile 目录，可无缝切换。
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import AppConfig
from .logging_utils import get_logger
from .procutil import pid_alive

logger = get_logger("browser")

# 隐藏自动化特征，降低被风控识别的概率
STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
window.chrome = window.chrome || {runtime: {}};
"""

# 持久化 profile 下仍需保留的启动参数。
# 注意不要加 --incognito / --guest，那会让持久化失效。
_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    # 关掉首次运行引导页与默认浏览器询问，避免污染 profile 与遮挡页面
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-features=Translate,OptimizationHints",
]


class BrowserBusyError(RuntimeError):
    """同一 profile 已被另一个进程占用。"""


# 浏览器同样一律直连：Chromium 默认也读系统代理，代理软件没开时登录页根本打不开
_NO_PROXY_ARGS = ["--no-proxy-server"]


def _busy_message(cfg: AppConfig, account_id: str, detail: str) -> str:
    """profile 被占用时的提示。占用者是控制台本身时说清楚该去哪儿处理。"""
    meta = read_session_meta(cfg, account_id)
    holder = int(meta.get("holder_pid") or 0)
    console = int(os.environ.get("CTRIP_CONSOLE_PID") or meta.get("console_pid") or 0)
    if holder and console and holder == console:
        return (f"账号 {account_id} 的浏览器正在控制台里开着（{detail}），本次采集无法再打开它来重新登录。"
                f"请在控制台「账户管理 → 浏览器」弹窗里完成登录（登录成功会自动保存登录态），"
                f"或先关闭该浏览器，再重跑这次任务")
    return (f"账号 {account_id} 的浏览器 profile 正被占用（{detail}）。"
            f"同一 profile 不能被两个进程同时打开。"
            f"如果是控制台里这个账号的浏览器还开着，在控制台弹窗里登录或关闭浏览器；"
            f"否则等待上一次运行结束，或执行 "
            f"python -m ctrip.cli browser --stop --account-id {account_id}")


@dataclass
class BrowserSession:
    """一次浏览器会话。

    ``context`` 是统一的操作入口，无论底层是 persistent context、
    CDP 复用还是传统 launch，调用方只需要 ``session.context``。
    """

    context: Any
    mode: str                      # profile | cdp | ephemeral
    account_id: str
    profile_dir: Path | None = None
    keep_alive: bool = False
    _playwright: Any = None
    _browser: Any = None           # 仅 cdp / ephemeral 模式有独立 browser 对象
    _cfg: Any = None               # profile 模式下用于退出时清除占用标记
    _console_url: str = ""         # 打开前请控制台让出了浏览器：关闭后要请它重新打开

    @property
    def reused(self) -> bool:
        """是否复用了已存在的浏览器进程（CDP 模式）。"""
        return self.mode == "cdp"

    def page(self):
        """取一个可用页面：优先复用已打开的，避免每次新开标签页。"""
        pages = [p for p in self.context.pages if not p.is_closed()]
        if pages:
            return pages[0]
        return self.context.new_page()

    def close(self) -> None:
        """结束会话。

        ``keep_alive=True`` 时只断开连接而**不关闭浏览器**，浏览器继续挂在
        后台；否则关闭 context（persistent 模式下等价于关闭浏览器）。
        """
        try:
            if self.mode == "cdp":
                # 复用常驻浏览器：无论是否 keep_alive 都不能关掉它，
                # 否则下一次运行就得重开浏览器，常驻就失去意义。
                # 只关自己新开的页面，保留浏览器与原有 context。
                for pg in list(self.context.pages)[1:]:
                    try:
                        pg.close()
                    except Exception:  # noqa: BLE001
                        pass
                # 先断 CDP 连接再 stop playwright，顺序颠倒会留下未完成的 asyncio task
                if self._browser is not None:
                    try:
                        self._browser.close()   # 仅断开 CDP 连接
                    except Exception:  # noqa: BLE001
                        pass
            elif self.keep_alive:
                pass
            else:
                try:
                    self.context.close()
                except Exception:  # noqa: BLE001
                    pass
                if self._browser is not None:
                    try:
                        self._browser.close()
                    except Exception:  # noqa: BLE001
                        pass
        finally:
            if self._playwright is not None:
                try:
                    self._playwright.stop()
                except Exception:  # noqa: BLE001
                    pass
                # Playwright 的后台连接协程来不及收尾，解释器退出时会向
                # stderr 打印 "Task was destroyed but it is pending"。
                # 这是无害噪声（连接断开是预期行为），
                # 但会让用户误以为采集出错，因此主动静默它。
                # 三种模式都可能出现，不只是 cdp。
                if self.mode == "cdp":
                    time.sleep(0.3)
                _silence_asyncio_teardown_noise()
            # 释放占用标记，否则下次运行会误判为「profile 被占用」
            if self.mode == "profile" and self._cfg is not None:
                try:
                    write_session_meta(self._cfg, self.account_id, holder_pid=0,
                                       holder_since="",
                                       last_close_at=datetime.now().isoformat(timespec="seconds"))
                except Exception:  # noqa: BLE001
                    pass
            # 浏览器是从控制台那儿借来的，用完还回去
            if self._console_url:
                url, self._console_url = self._console_url, ""
                ask_console_reopen(url, self.account_id)


# =============================================================================
# 常驻会话元信息
# =============================================================================

def read_session_meta(cfg: AppConfig, account_id: str) -> dict[str, Any]:
    p = cfg.browser_lock_file(account_id)
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def write_session_meta(cfg: AppConfig, account_id: str, **kwargs: Any) -> None:
    meta = read_session_meta(cfg, account_id)
    meta.update(kwargs)
    meta["account_id"] = account_id
    meta["updated_at"] = datetime.now().isoformat(timespec="seconds")
    p = cfg.browser_lock_file(account_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def clear_session_meta(cfg: AppConfig, account_id: str) -> None:
    p = cfg.browser_lock_file(account_id)
    if p.exists():
        try:
            p.unlink()
        except OSError:
            pass


def _port_open(port: int, host: str = "127.0.0.1", timeout: float = 0.6) -> bool:
    """CDP 端口是否可连接。"""
    if port <= 0:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _pid_alive(pid: int) -> bool:
    # 不能用 os.kill(pid, 0)：Windows 上那是 TerminateProcess，会把被探测的进程
    # （比如占着 profile 的控制台）直接结束掉。详见 procutil。
    return pid_alive(pid)


def cdp_port_for(cfg: AppConfig, account_id: str) -> int:
    """该账号的 CDP 端口。

    多账号必须用不同端口，否则会连到别的账号的浏览器上造成串号。
    以配置端口为基准，按账号在列表中的序号依次 +1。
    """
    base = cfg.ctrip.cdp_port
    if base <= 0:
        return 0
    ids = [a.account_id for a in cfg.ctrip.accounts]
    try:
        offset = ids.index(account_id)
    except ValueError:
        offset = 0
    return base + offset


def keep_alive_status(cfg: AppConfig, account_id: str) -> tuple[bool, str]:
    """常驻浏览器是否还活着。"""
    meta = read_session_meta(cfg, account_id)
    port = int(meta.get("cdp_port") or 0)
    pid = int(meta.get("pid") or 0)
    if port <= 0:
        return False, "未启用常驻浏览器"
    if not _port_open(port):
        return False, f"CDP 端口 {port} 无法连接（浏览器可能已退出）"
    alive = _pid_alive(pid) if pid else True
    if not alive:
        return False, f"记录的进程 {pid} 已不存在"
    since = meta.get("started_at", "?")
    return True, f"常驻浏览器在线（CDP 端口 {port}，pid {pid}，启动于 {since}）"


# =============================================================================
# profile 占用检测
# =============================================================================

def _singleton_lock_files(profile_dir: Path) -> list[Path]:
    # Chromium 在 profile 根下用 SingletonLock/SingletonSocket 标记占用
    return [profile_dir / name for name in ("SingletonLock", "SingletonSocket", "SingletonCookie")]


def profile_in_use(cfg: AppConfig, account_id: str) -> tuple[bool, str]:
    """profile 是否正被另一个进程占用。

    三道判据，缺一不可：

    1. 常驻会话在线 —— 那是**合法**占用，可通过 CDP 复用；
    2. session.json 记录的持有者进程仍存活且不是当前进程；
    3. SingletonLock 指向一个活着的进程。

    为何需要第 2 条：实测发现 **headless 模式下 Chromium 不写
    SingletonLock**（Playwright 用的是 chrome-headless-shell，不走桌面端
    「同一 profile 只允许一个实例」那套逻辑）。若只靠锁文件判断，
    无头场景（也就是定时任务的常态）的并发保护就形同虚设，
    两个进程同时写同一个 profile 会造成 Cookie 数据库损坏。
    """
    alive, detail = keep_alive_status(cfg, account_id)
    if alive:
        return True, f"由常驻浏览器持有：{detail}"

    meta = read_session_meta(cfg, account_id)
    holder = int(meta.get("holder_pid") or 0)
    if holder and holder != os.getpid() and _pid_alive(holder):
        since = meta.get("holder_since", "?")
        return True, f"已被进程 {holder} 持有（自 {since} 起）"

    profile_dir = cfg.browser_profile_dir(account_id)
    for lock in _singleton_lock_files(profile_dir):
        if not lock.is_symlink() and not lock.exists():
            continue
        try:
            # SingletonLock 是符号链接，内容形如 hostname-pid
            target = os.readlink(lock) if lock.is_symlink() else ""
        except OSError:
            target = ""
        pid = 0
        if "-" in target:
            try:
                pid = int(target.rsplit("-", 1)[-1])
            except ValueError:
                pid = 0
        if pid and _pid_alive(pid):
            return True, f"profile 正被进程 {pid} 占用（{lock.name}）"
    return False, "profile 未被占用"


def clear_stale_locks(cfg: AppConfig, account_id: str, log: logging.Logger | None = None) -> int:
    """清理残留的 SingletonLock。

    进程被强杀（如 kill -9、断电）后锁文件会残留，导致下次启动失败。
    仅在确认无活进程持有时清理，避免误删正在使用的锁。
    """
    log = log or logger
    in_use, detail = profile_in_use(cfg, account_id)
    if in_use:
        raise BrowserBusyError(detail)
    profile_dir = cfg.browser_profile_dir(account_id)
    removed = 0
    for lock in _singleton_lock_files(profile_dir):
        if lock.is_symlink() or lock.exists():
            try:
                lock.unlink()
                removed += 1
            except OSError:
                pass
    if removed:
        log.info("[%s] 已清理 %d 个残留浏览器锁文件", account_id, removed)
    return removed


# =============================================================================
# 控制台占用：请控制台让出浏览器，用完再请它打开
# =============================================================================
#
# Web 控制台（webconsole）打开某个账号的浏览器时，在 session.json 里记下
# console_pid / console_url。命令行登录时发现 profile 正被控制台占着，
# 就调控制台的接口让它先关掉浏览器，登录完再让它重新打开 —— 不用人去点。

# 等控制台让出的上限（秒）。页面上有人正在登录时，控制台会等登录结束再关
CONSOLE_YIELD_WAIT = int(os.environ.get("CTRIP_CONSOLE_YIELD_WAIT", "600"))


def console_holder_url(cfg: AppConfig, account_id: str) -> str:
    """profile 正被控制台里开着的浏览器占用时，返回控制台地址；否则返回空串。"""
    meta = read_session_meta(cfg, account_id)
    holder = int(meta.get("holder_pid") or 0)
    console_pid = int(meta.get("console_pid") or 0)
    url = str(meta.get("console_url") or "").rstrip("/")
    if (url and holder and holder == console_pid and holder != os.getpid()
            and _pid_alive(holder)):
        return url
    return ""


def _console_post(url: str, path: str, timeout: float) -> dict[str, Any]:
    import urllib.error
    import urllib.request

    # 控制台在本机或内网，一律直连，不读系统代理（代理没开时会连不上）
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(url + path, data=b"", method="POST")
    try:
        with opener.open(req, timeout=timeout) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        raw = exc.read()
    try:
        return json.loads(raw.decode("utf-8", "replace") or "{}")
    except json.JSONDecodeError:
        return {"ok": False, "message": raw[:200].decode("utf-8", "replace")}


def ask_console_release(cfg: AppConfig, account_id: str,
                        log: logging.Logger | None = None) -> str:
    """请控制台关掉该账号的浏览器。成功关掉返回控制台地址（用完要还），否则空串。"""
    log = log or logger
    url = console_holder_url(cfg, account_id)
    if not url:
        return ""
    log.info("[%s] 浏览器正在控制台里开着，请控制台先关闭它（%s）", account_id, url)
    try:
        body = _console_post(url, f"/api/browser/{account_id}/yield?wait={CONSOLE_YIELD_WAIT}",
                             timeout=CONSOLE_YIELD_WAIT + 60)
    except Exception as exc:  # noqa: BLE001
        log.warning("[%s] 联系不上控制台（%s）：%s", account_id, url, exc)
        return ""
    if not body.get("ok"):
        log.warning("[%s] 控制台没有关闭浏览器：%s", account_id, body.get("message", ""))
        return ""
    if not body.get("closed"):
        return ""
    # 控制台关浏览器后，占用标记要稍等才清掉
    for _ in range(30):
        busy, _detail = profile_in_use(cfg, account_id)
        if not busy:
            break
        time.sleep(0.5)
    log.info("[%s] 控制台已关闭浏览器，登录完会请它重新打开", account_id)
    return url


def ask_console_reopen(url: str, account_id: str,
                       log: logging.Logger | None = None) -> None:
    """请控制台重新打开之前让出来的浏览器。失败只告警：不影响已经完成的登录。"""
    log = log or logger
    try:
        body = _console_post(url, f"/api/browser/{account_id}/start", timeout=60)
    except Exception as exc:  # noqa: BLE001
        log.warning("[%s] 请控制台重新打开浏览器失败：%s", account_id, exc)
        return
    if body.get("ok"):
        log.info("[%s] 已请控制台重新打开浏览器", account_id)
    else:
        log.warning("[%s] 控制台没能重新打开浏览器：%s", account_id, body.get("message", ""))


# =============================================================================
# 打开会话
# =============================================================================

_NOISE_MARKERS = (
    "Task was destroyed but it is pending",
    "Future exception was never retrieved",
    "TargetClosedError",
    "Target page, context or browser has been closed",
)
_noise_filter_installed = False


def _silence_asyncio_teardown_noise() -> None:
    """静默 Playwright 断开 CDP 连接后遗留协程在退出时的告警。

    Playwright 同步 API 内部跑在自己的线程/事件循环里，外部拿不到那个
    loop，因此无法用 ``set_exception_handler`` 拦住；而且告警是在
    解释器退出、asyncio 回收未完成 task 时直接写 stderr 的。

    因此改为在进程退出阶段过滤 stderr：仅丢弃上述确定无害的几行，
    其余输出原样透传，不会掩盖真正的错误。
    """
    global _noise_filter_installed
    if _noise_filter_installed:
        return
    _noise_filter_installed = True

    real_stderr = sys.stderr

    class _FilteredStderr:
        """退出阶段的 stderr 包装，按行丢弃无害噪声。"""

        def __init__(self) -> None:
            self._suppressing = False

        def write(self, text: str) -> int:
            if any(m in text for m in _NOISE_MARKERS):
                # 噪声是多行的（task/future/traceback），进入抑制模式
                self._suppressing = True
                return len(text)
            if self._suppressing:
                # 缩进行与空行视为同一段噪声的延续
                if text.startswith((" ", "\t")) or not text.strip():
                    return len(text)
                if text.startswith(("task:", "future:", "playwright.", "asyncio.")):
                    return len(text)
                self._suppressing = False
            return real_stderr.write(text)

        def flush(self) -> None:
            try:
                real_stderr.flush()
            except Exception:  # noqa: BLE001
                pass

        def __getattr__(self, name: str) -> Any:
            return getattr(real_stderr, name)

    # 立即生效：atexit 钩子的执行时机早于 asyncio 回收未完成 task，
    # 但晚于部分库自己注册的钩子，存在遗漏窗口；
    # 直接包装更可靠，且过滤器只认那几个固定标记，不会影响正常日志。
    sys.stderr = _FilteredStderr()  # type: ignore[assignment]


def _cdp_browser_id(port: int, timeout: float = 3.0) -> str:
    """读取 CDP 端口背后浏览器的唯一标识（webSocketDebuggerUrl）。"""
    import urllib.request

    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version",
                                    timeout=timeout) as resp:
            info = json.loads(resp.read().decode("utf-8", "replace"))
        return str(info.get("webSocketDebuggerUrl") or "")
    except Exception:  # noqa: BLE001
        return ""


def _port_owned_by_us(cfg: AppConfig, account_id: str, port: int,
                      log: logging.Logger) -> bool:
    """端口后面的浏览器是不是本工具为该账号拉起的。

    以 session.json 记录的 pid 与 ws 地址为凭据：
    两者均不匹配时判定为端口冲突，拒绝复用。
    """
    meta = read_session_meta(cfg, account_id)
    recorded_pid = int(meta.get("pid") or 0)
    recorded_ws = str(meta.get("cdp_ws") or "")
    recorded_port = int(meta.get("cdp_port") or 0)

    # 本账号确实有一个存活的常驻进程，且端口一致 → 可信
    if recorded_pid > 0 and recorded_port == port and _pid_alive(recorded_pid):
        if not recorded_ws:
            return True
        current_ws = _cdp_browser_id(port)
        if not current_ws or current_ws == recorded_ws:
            return True
        log.warning("[%s] CDP 端口 %d 背后的浏览器已变更，不再复用", account_id, port)
        return False

    # 端口开着但不是我们的进程 → 端口被外部占用
    log.warning("[%s] CDP 端口 %d 已被其他浏览器占用（非本工具拉起），"
                "为避免误用陌生浏览器身份，改用持久化 profile 模式",
                account_id, port)
    log.warning("[%s] 如需常驻模式，请将配置 cdp_port 改为空闲端口（如 9400）", account_id)
    return False


def _warn_if_fingerprint_drift(context: Any, cfg: AppConfig, account_id: str,
                               log: logging.Logger) -> None:
    """校验复用的 context 指纹是否与预期一致。

    常驻浏览器若不是本工具拉起的（比如用户自己开了个带调试端口的浏览器），
    其 UA / 时区可能与 profile 模式不同，一旦不同就等于换了台设备，
    反而会触发验证。这里只告警不中断，因为采集仍可进行。
    """
    try:
        pages = [pg for pg in context.pages if not pg.is_closed()]
        page = pages[0] if pages else context.new_page()
        ua = page.evaluate("() => navigator.userAgent")
        tz = page.evaluate("() => Intl.DateTimeFormat().resolvedOptions().timeZone")
    except Exception:  # noqa: BLE001
        return
    if "Headless" in ua:
        log.warning("[%s] 常驻浏览器 UA 包含 Headless 标识，极易被风控识别：%s",
                    account_id, ua)
        log.warning("[%s] 建议执行 browser --stop 后重新 browser --start（由本工具拉起才会带伪装参数）",
                    account_id)
    elif ua != cfg.collect.user_agent:
        log.warning("[%s] 常驻浏览器 UA 与配置不一致，可能被当成不同设备", account_id)
    if tz and tz != cfg.run.timezone:
        log.warning("[%s] 常驻浏览器时区为 %s（预期 %s），建议重启常驻浏览器",
                    account_id, tz, cfg.run.timezone)


def _context_kwargs(cfg: AppConfig) -> dict[str, Any]:
    return {
        # 4:3 窗口比例，贴近 PC 端浏览习惯
        "viewport": {"width": 1440, "height": 1080},
        "locale": "zh-CN",
        "timezone_id": cfg.run.timezone,
        "user_agent": cfg.collect.user_agent,
    }


def open_session(cfg: AppConfig, account_id: str, *,
                 headless: bool | None = None,
                 keep_alive: bool = False,
                 yield_console: bool = False,
                 log: logging.Logger | None = None) -> BrowserSession:
    """打开一个浏览器会话，按可用性依次尝试 CDP 复用 → 持久化 profile → 临时。

    ``keep_alive=True`` 时，若尚无常驻浏览器则以独立进程拉起一个并监听
    CDP 端口，本次运行结束后浏览器继续挂在后台。

    ``yield_console=True`` 时，profile 若正被控制台里开着的浏览器占用，
    先请控制台关掉它再打开；会话关闭时再请控制台重新打开（登录用）。
    """
    from playwright.sync_api import sync_playwright

    log = log or logger
    headless = cfg.ctrip.headless if headless is None else headless
    port = cdp_port_for(cfg, account_id)

    # 1) 已有常驻浏览器 → 直接复用（连验证码都不用重过）
    #
    # 先确认端口后面那个浏览器确实是本工具拉起的。
    # 实测过的事故：机器上早已存在一个监听 9222 的无关 chromium，
    # 直接 connect_over_cdp 会连到它——既没有携程登录态，
    # 又会用陆生浏览器的指纹去请求，排查起来极难定位。
    if port > 0 and _port_open(port) and _port_owned_by_us(cfg, account_id, port, log):
        p = sync_playwright().start()
        try:
            browser = p.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
            context = browser.contexts[0] if browser.contexts else browser.new_context(
                **_context_kwargs(cfg))
            context.set_default_timeout(cfg.ctrip.nav_timeout * 1000)
            try:
                context.add_init_script(STEALTH_JS)
            except Exception as exc:  # noqa: BLE001
                # 不能静默吞掉：注入失败意味着 navigator.webdriver 仍为 true，
                # 风控一眼就能认出是自动化
                log.warning("[%s] 常驻浏览器反自动化脚本注入失败（可能已注入过）：%s",
                            account_id, exc)
            _warn_if_fingerprint_drift(context, cfg, account_id, log)
            log.info("[%s] 复用常驻浏览器（CDP 端口 %d），无需重新启动", account_id, port)
            return BrowserSession(context=context, mode="cdp", account_id=account_id,
                                  profile_dir=cfg.browser_profile_dir(account_id),
                                  keep_alive=True, _playwright=p, _browser=browser)
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] 连接常驻浏览器失败，回退为本地启动：%s", account_id, exc)
            try:
                p.stop()
            except Exception:  # noqa: BLE001
                pass

    # 2) 需要常驻但还没起 → 以独立进程拉起，再 CDP 连上
    if keep_alive and port > 0:
        _spawn_keep_alive(cfg, account_id, port, headless=headless, log=log)
        if _port_open(port):
            return open_session(cfg, account_id, headless=headless,
                                keep_alive=False, log=log)
        log.warning("[%s] 常驻浏览器启动后端口仍不可用，回退为持久化 profile 模式", account_id)

    # 3) 持久化 profile（默认路径）
    if cfg.ctrip.persistent_browser:
        in_use, detail = profile_in_use(cfg, account_id)
        console_url = ""
        if in_use and yield_console:
            console_url = ask_console_release(cfg, account_id, log)
            if console_url:
                in_use, detail = profile_in_use(cfg, account_id)
        if in_use:
            if console_url:
                ask_console_reopen(console_url, account_id, log)
            raise BrowserBusyError(_busy_message(cfg, account_id, detail))
        try:
            session = _open_profile(cfg, account_id, headless=headless, log=log)
        except BaseException:
            if console_url:
                ask_console_reopen(console_url, account_id, log)
            raise
        session._console_url = console_url
        return session

    # 4) 兜底：旧的临时 profile 行为
    log.warning("[%s] 持久化浏览器已关闭，使用临时 profile（每次登录都可能触发验证）", account_id)
    p = sync_playwright().start()
    browser = p.chromium.launch(headless=headless, slow_mo=cfg.ctrip.slow_mo,
                                args=list(_LAUNCH_ARGS) + _NO_PROXY_ARGS)
    context = browser.new_context(**_context_kwargs(cfg))
    context.add_init_script(STEALTH_JS)
    context.set_default_timeout(cfg.ctrip.nav_timeout * 1000)
    return BrowserSession(context=context, mode="ephemeral", account_id=account_id,
                          keep_alive=False, _playwright=p, _browser=browser)


def _open_profile(cfg: AppConfig, account_id: str, *, headless: bool,
                  log: logging.Logger) -> BrowserSession:
    """以持久化 profile 启动浏览器（调用方已确认 profile 未被占用）。"""
    from playwright.sync_api import sync_playwright

    profile_dir = cfg.browser_profile_dir(account_id)
    p = sync_playwright().start()
    launch_kwargs: dict[str, Any] = {
        "user_data_dir": str(profile_dir),
        "headless": headless,
        "slow_mo": cfg.ctrip.slow_mo,
        "args": list(_LAUNCH_ARGS) + _NO_PROXY_ARGS,
        # 强制用完整 Chromium（--headless=new）而非精简的 chrome-headless-shell。
        # 实测：headless-shell 不写 Preferences / Login Data / History /
        # Service Worker，也不写 SingletonLock，持久化不完整；
        # 更关键的是它与常驻 CDP 模式（完整 chromium）指纹不一致，
        # 用户切换模式时会被当成新设备而重新触发验证。
        "channel": "chromium",
        **_context_kwargs(cfg),
    }
    try:
        context = p.chromium.launch_persistent_context(**launch_kwargs)
    except Exception as exc:  # noqa: BLE001
        # 未安装 chromium channel 时降级为默认构建，不让采集直接失败
        log.warning("[%s] 完整 Chromium 启动失败，降级为默认构建（持久化完整度略低）：%s",
                    account_id, exc)
        launch_kwargs.pop("channel", None)
        try:
            context = p.chromium.launch_persistent_context(**launch_kwargs)
        except Exception:  # noqa: BLE001
            try:
                p.stop()
            except Exception:  # noqa: BLE001
                pass
            raise
    context.add_init_script(STEALTH_JS)
    context.set_default_timeout(cfg.ctrip.nav_timeout * 1000)
    first_run = not any(profile_dir.glob("Default/**/*"))
    log.info("[%s] 使用持久化浏览器 profile：%s（%s）", account_id, profile_dir,
             "首次创建" if first_run else "复用已有设备指纹与登录态")
    now = datetime.now().isoformat(timespec="seconds")
    # holder_pid 是并发保护的依据（headless 下 Chromium 不写 SingletonLock），
    # 必须在会话结束时清除，否则下次运行会被自己的遗留记录拦住
    write_session_meta(cfg, account_id, mode="profile", pid=os.getpid(),
                       holder_pid=os.getpid(), holder_since=now,
                       profile_dir=str(profile_dir), cdp_port=0,
                       last_open_at=now)
    return BrowserSession(context=context, mode="profile", account_id=account_id,
                          profile_dir=profile_dir, keep_alive=False, _playwright=p,
                          _cfg=cfg)


def _chromium_executable() -> str | None:
    """定位 Playwright 安装的 Chromium 可执行文件。"""
    from playwright.sync_api import sync_playwright
    try:
        with sync_playwright() as p:
            path = p.chromium.executable_path
        if path and Path(path).exists():
            return path
    except Exception:  # noqa: BLE001
        pass
    for name in ("chromium", "chromium-browser", "google-chrome", "chrome"):
        found = shutil.which(name)
        if found:
            return found
    return None


def _spawn_keep_alive(cfg: AppConfig, account_id: str, port: int, *,
                      headless: bool, log: logging.Logger) -> None:
    """以独立进程拉起常驻浏览器（脱离当前进程生命周期）。"""
    exe = _chromium_executable()
    if not exe:
        log.warning("[%s] 未找到 Chromium 可执行文件，无法启动常驻浏览器", account_id)
        return

    try:
        clear_stale_locks(cfg, account_id, log)
    except BrowserBusyError as exc:
        log.info("[%s] %s", account_id, exc)
        return

    profile_dir = cfg.browser_profile_dir(account_id)
    # 关键：常驻浏览器是裸 Chromium 进程，不经过 Playwright 的 context 参数，
    # 必须在命令行上把伪装参数补齐。否则它的 UA 会带 HeadlessChrome、
    # 时区是 UTC、窗口 800x600，比 profile 模式更容易被风控识别，
    # 与「减少验证码」的目标适得其反。
    cmd = [
        exe,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
        "--remote-allow-origins=*",
        f"--user-agent={cfg.collect.user_agent}",
        "--window-size=1440,1080",
        "--lang=zh-CN",
        "--accept-lang=zh-CN,zh",
        *_LAUNCH_ARGS,
        *_NO_PROXY_ARGS,
    ]
    if headless:
        cmd.append("--headless=new")
        # 无头下 screen.width/height 默认 800x600，与 profile 模式的1440x1080 不同；
        # 屏幕尺寸是风控的常用信号，必须强制对齐，否则切换模式就算换设备
        cmd.append("--window-position=0,0")
        cmd.append("--screen-info={1440x1080}")
    cmd.append("about:blank")

    log_dir = cfg.log_dir(account_id)
    out = open(log_dir / "browser_keepalive.log", "ab", buffering=0)
    # Chromium 读系统时区，只能通过 TZ 环境变量传递
    child_env = dict(os.environ)
    child_env["TZ"] = cfg.run.timezone
    kwargs: dict[str, Any] = {"stdout": out, "stderr": out, "stdin": subprocess.DEVNULL,
                              "env": child_env}
    if sys.platform == "win32":
        # 脱离当前控制台，父进程退出后浏览器继续存活
        kwargs["creationflags"] = 0x00000008 | 0x00000200  # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True

    # 端口已被占用时不能直接启：Chromium 会静默改用其他端口或归并到
    # 已有实例，结果看似「启动成功」却并非我们的浏览器
    if _port_open(port):
        log.error("[%s] CDP 端口 %d 已被占用，拒绝启动常驻浏览器（避免误用陌生浏览器）",
                  account_id, port)
        log.error("[%s] 请将配置 cdp_port 改为空闲端口，或先释放该端口", account_id)
        return

    proc = subprocess.Popen(cmd, **kwargs)  # noqa: S603
    log.info("[%s] 正在启动常驻浏览器（pid %d，CDP 端口 %d）…", account_id, proc.pid, port)

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if _port_open(port):
            # 记下 ws 地址作为「这个浏览器是我们的」的凭据，
            # 供下次复用前校验，避开端口被外部占用导致的误连
            write_session_meta(cfg, account_id, mode="cdp", pid=proc.pid, cdp_port=port,
                               cdp_ws=_cdp_browser_id(port),
                               profile_dir=str(profile_dir), headless=headless,
                               started_at=datetime.now().isoformat(timespec="seconds"))
            log.info("[%s] 常驻浏览器已就绪，后续运行将直接复用", account_id)
            return
        if proc.poll() is not None:
            log.error("[%s] 常驻浏览器进程已退出（返回码 %s），详见 %s",
                      account_id, proc.returncode, log_dir / "browser_keepalive.log")
            return
        time.sleep(0.5)
    log.warning("[%s] 等待常驻浏览器就绪超时（30s）", account_id)


def ensure_keep_alive(cfg: AppConfig, account_id: str, *,
                      headless: bool | None = None,
                      log: logging.Logger | None = None) -> tuple[bool, str]:
    """确保该账号的常驻浏览器已在后台运行（已在则直接返回）。

    用于 `run --keep-alive`：登录态仍有效时采集不会打开浏览器，
    但用户既然要求常驻，就应该把浏览器挂起来备用。
    """
    log = log or logger
    port = cdp_port_for(cfg, account_id)
    if port <= 0:
        return False, "未启用常驻浏览器（cdp_port 未配置）"
    alive, detail = keep_alive_status(cfg, account_id)
    if alive:
        log.info("[%s] 常驻浏览器已在运行，无需重开：%s", account_id, detail)
        return True, detail
    try:
        session = open_session(cfg, account_id, headless=headless,
                              keep_alive=True, log=log)
    except BrowserBusyError as exc:
        log.warning("[%s] 无法启动常驻浏览器：%s", account_id, exc)
        return False, str(exc)
    except Exception as exc:  # noqa: BLE001
        log.warning("[%s] 常驻浏览器启动失败：%s", account_id, exc)
        return False, str(exc)
    # 只拉起，不关闭
    session.keep_alive = True
    session.close()
    return keep_alive_status(cfg, account_id)


def stop_keep_alive(cfg: AppConfig, account_id: str,
                    log: logging.Logger | None = None) -> bool:
    """停止常驻浏览器。profile 保留在磁盘上，下次启动仍复用同一设备指纹。"""
    import signal

    log = log or logger
    meta = read_session_meta(cfg, account_id)
    # 只有 cdp 模式的 pid 才是常驻浏览器进程；profile 模式下 pid 是当时的
    # Python 进程，误杀它会把正在采集的任务干掉
    port = int(meta.get("cdp_port") or 0)
    pid = int(meta.get("pid") or 0) if port > 0 else 0
    if pid <= 0 or not _pid_alive(pid):
        # 只清常驻相关字段，保留 profile_dir / holder 等信息：
        # 整体删除会连并发保护所需的占用标记一起丢失
        write_session_meta(cfg, account_id, cdp_port=0, pid=0, mode="profile")
        log.info("[%s] 无常驻浏览器在运行", account_id)
        return False
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as exc:
        log.warning("[%s] 停止常驻浏览器失败：%s", account_id, exc)
        return False
    for _ in range(20):
        if not _pid_alive(pid):
            break
        time.sleep(0.3)
    else:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
    # 主进程退出后，Chromium 的子进程与 SingletonLock 还要几百毫秒才释放。
    # 不等就返回的话，紧接着的下一次运行会看到残留锁而报「profile 被占用」
    for _ in range(20):
        in_use, _detail = profile_in_use(cfg, account_id)
        if not in_use:
            break
        time.sleep(0.3)
    # 残留锁指向的进程已死，但文件仍在；顺手清掉，
    # 避免用户下次运行还要手动跑 browser --clear-locks
    try:
        removed = 0
        for lock in _singleton_lock_files(cfg.browser_profile_dir(account_id)):
            if lock.is_symlink() or lock.exists():
                try:
                    lock.unlink()
                    removed += 1
                except OSError:
                    pass
        if removed:
            log.info("[%s] 已清理停止后残留的 %d 个锁文件", account_id, removed)
    except Exception:  # noqa: BLE001
        pass
    write_session_meta(cfg, account_id, cdp_port=0, pid=0, mode="profile",
                       stopped_at=datetime.now().isoformat(timespec="seconds"))
    log.info("[%s] 常驻浏览器已停止（pid %d，端口 %d），profile 已保留", account_id, pid, port)
    return True
