"""虚拟显示与 VNC 服务管理。

为什么走 VNC
------------
CDP ``Page.startScreencast`` 推的是一张张独立 JPEG，没有帧间压缩：画面一动
带宽就上去，而且静止时不推帧（要靠补截图才有画面，那就退化成截图流了）。

VNC 用的 RFB 协议是**增量矩形更新**——只传变化的那块区域，配合 x11vnc 的
XDAMAGE 事件驱动，是真正的实时流。鼠标键盘也走 X 原生输入栈，拖拽滑块的
手感与本地一致。

附带收益：浏览器可以跑**有头模式**。实测有头下 ``navigator.webdriver``
为 undefined、UA 不含 Headless、``innerHeight`` 与窗口一致（headless 下
Playwright 设 1080 但合成表面只有 941，底部一截看不见也点不到）。

端口分配
--------
每账号一套，按账号在配置中的序号偏移，避免多账号互相顶掉：
    display  :99 + index
    VNC 端口 5900 + index      （只监听 127.0.0.1，由控制台内部桥接转发）
"""

from __future__ import annotations

import os
import platform
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from ctrip.logging_utils import get_logger

logger = get_logger("display")

SCREEN = "1440x1080x24"
BASE_DISPLAY = 99
BASE_VNC_PORT = 5900


class DisplayError(RuntimeError):
    pass


IS_WINDOWS = sys.platform.startswith("win")
IS_MAC = sys.platform == "darwin"
IS_LINUX = sys.platform.startswith("linux")


def check_dependencies() -> tuple[bool, str]:
    """VNC 模式需要 Linux + Xvfb + x11vnc。"""
    if not IS_LINUX:
        return False, (
            f"VNC 模式只能在 Linux 上跑（当前 {platform.system()}）。"
            f"本机开发请用 local 模式：浏览器直接开在桌面上，用你自己的鼠标键盘操作。"
        )
    missing = [b for b in ("Xvfb", "x11vnc") if not shutil.which(b)]
    if missing:
        return False, (
            f"缺少 {', '.join(missing)}。安装：\n"
            f"  Ubuntu/Debian:  sudo apt-get install -y xvfb x11vnc\n"
            f"  Amazon Linux:   sudo yum install -y xorg-x11-server-Xvfb x11vnc"
        )
    return True, "依赖齐全"


def resolve_mode(requested: str = "auto") -> tuple[str, str]:
    """决定用哪种画面模式。

    ``cdp``（默认，全平台）
        无头浏览器 + CDP screencast。合成器在画面变化时**主动推帧**，
        不是定时截图。Windows / Linux 都一样跑，无需任何系统依赖。

    ``vnc``（可选，仅 Linux）
        Xvfb + x11vnc，RFB 增量更新，延迟更低。但**必须有头浏览器**——
        无头没有窗口，x11vnc 抓不到任何东西。只在明确要有头时才用。

    默认走 cdp：既满足「无头」，又能在 Windows 上开发。
    """
    requested = (requested or "auto").lower()
    if requested in ("auto", "", "cdp"):
        return "cdp", f"{platform.system()} · 无头浏览器 + CDP 实时推流"

    if requested == "vnc":
        ok, detail = check_dependencies()
        if not ok:
            return "vnc", detail      # 交给上层报错，不偷偷降级
        return "vnc", "Linux + x11vnc（有头浏览器）· RFB 增量流"
    return requested, f"按 CONSOLE_STREAM={requested} 指定"


def _port_free(port: int) -> bool:
    with socket.socket() as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _wait_port(port: int, timeout: float = 15) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.25)
    return False


@dataclass
class Display:
    """一个账号专属的虚拟显示 + VNC 服务。"""

    account_id: str
    index: int
    log_dir: Path
    xvfb: subprocess.Popen | None = field(default=None, repr=False)
    vnc: subprocess.Popen | None = field(default=None, repr=False)

    @property
    def display(self) -> str:
        return f":{BASE_DISPLAY + self.index}"

    @property
    def vnc_port(self) -> int:
        return BASE_VNC_PORT + self.index

    @property
    def running(self) -> bool:
        return (self.xvfb is not None and self.xvfb.poll() is None
                and self.vnc is not None and self.vnc.poll() is None)

    # ------------------------------------------------------------ 启停
    def start(self) -> None:
        ok, detail = check_dependencies()
        if not ok:
            raise DisplayError(detail)
        if not IS_LINUX:
            raise DisplayError("虚拟显示只在 Linux 上可用")

        self.log_dir.mkdir(parents=True, exist_ok=True)
        lock = Path(f"/tmp/.X{BASE_DISPLAY + self.index}-lock")

        if not self._display_alive():
            # 上次进程被强杀会留下锁文件，X server 会拒绝启动
            if lock.exists() and not self._display_alive():
                try:
                    lock.unlink()
                    logger.info("[%s] 清理残留 X 锁文件 %s", self.account_id, lock)
                except OSError:
                    pass
            out = open(self.log_dir / "xvfb.log", "ab", buffering=0)
            self.xvfb = subprocess.Popen(
                ["Xvfb", self.display, "-screen", "0", SCREEN, "-nolisten", "tcp"],
                stdout=out, stderr=out, stdin=subprocess.DEVNULL,
                start_new_session=(os.name == "posix"),
            )
            for _ in range(40):
                if self._display_alive():
                    break
                if self.xvfb.poll() is not None:
                    raise DisplayError(
                        f"Xvfb 启动失败（退出码 {self.xvfb.returncode}），"
                        f"详见 {self.log_dir / 'xvfb.log'}")
                time.sleep(0.25)
            else:
                raise DisplayError(f"Xvfb 启动超时（{self.display}）")
            logger.info("[%s] 虚拟显示已就绪 %s（%s）",
                        self.account_id, self.display, SCREEN)

        if not _port_free(self.vnc_port):
            # 端口已被占用：可能是上次遗留的 x11vnc，直接复用
            logger.info("[%s] VNC 端口 %d 已在监听，复用", self.account_id, self.vnc_port)
            return

        out = open(self.log_dir / "x11vnc.log", "ab", buffering=0)
        cmd = [
            "x11vnc",
            "-display", self.display,
            "-rfbport", str(self.vnc_port),
            "-localhost",        # 只听本机，外部一律走控制台的 WebSocket 桥接
            "-nopw",             # 因为只听本机，密码由控制台这一层负责
            "-forever",          # 客户端断开后不退出
            "-shared",
            "-xkb",              # 键盘映射更准，中文标点/组合键不易错
            "-cursor", "arrow",
            "-wait", "10",       # 无 XDAMAGE 时的轮询间隔上限（ms）
            "-defer", "10",      # 更新合并窗口（ms），越小越跟手
            "-quiet",
        ]
        self.vnc = subprocess.Popen(cmd, stdout=out, stderr=out,
                                    stdin=subprocess.DEVNULL,
                                    start_new_session=(os.name == "posix"))
        if not _wait_port(self.vnc_port, timeout=15):
            code = self.vnc.poll()
            raise DisplayError(
                f"x11vnc 未能在端口 {self.vnc_port} 上就绪"
                f"{f'（退出码 {code}）' if code is not None else ''}，"
                f"详见 {self.log_dir / 'x11vnc.log'}")
        logger.info("[%s] VNC 已就绪 127.0.0.1:%d", self.account_id, self.vnc_port)

    def _display_alive(self) -> bool:
        """X server 是否可用：socket 存在即认为已在跑。"""
        return Path(f"/tmp/.X11-unix/X{BASE_DISPLAY + self.index}").exists()

    def stop(self) -> None:
        for name, proc in (("x11vnc", self.vnc), ("Xvfb", self.xvfb)):
            if proc is None or proc.poll() is not None:
                continue
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            logger.info("[%s] %s 已停止", self.account_id, name)
        self.vnc = self.xvfb = None

    def env(self) -> dict[str, str]:
        e = dict(os.environ)
        e["DISPLAY"] = self.display
        return e
