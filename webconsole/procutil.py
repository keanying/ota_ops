"""进程工具。存活判断的实现（及为什么不能用 ``os.kill(pid, 0)``）见 ``ctrip/procutil.py``。"""

from __future__ import annotations

from ctrip.procutil import IS_WINDOWS, pid_alive

__all__ = ["IS_WINDOWS", "pid_alive", "describe"]


def describe(pid: int) -> str:
    """给人看的一句话，附上各平台的确认与清理命令。"""
    alive = pid_alive(pid)
    if IS_WINDOWS:
        how = (f"确认：Get-Process -Id {pid}　"
               f"结束：taskkill /PID {pid} /F")
    else:
        how = f"确认：ps -p {pid}　结束：kill -9 {pid}"
    return f"进程 {pid} {'仍在运行' if alive else '已不存在'}。{how}"
