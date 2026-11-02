"""进程存活判断（跨平台，非破坏性）。

为什么不直接用 ``os.kill(pid, 0)``
----------------------------------
POSIX 上 signal 0 是「只检查权限与存在性、不真的发信号」的惯例，所以
``os.kill(pid, 0)`` 是标准的存活探测写法。

**Windows 上没有信号这回事。** CPython 在 Windows 上把 ``os.kill`` 实现成
``OpenProcess`` + ``TerminateProcess(handle, sig)`` —— 也就是说
``os.kill(pid, 0)`` 会尝试**终止**那个进程（退出码 0），而不是探测它；
权限不足时又会抛 OSError 被当成「进程不存在」。两种结果都不对：
要么误杀，要么误判。

这里改用只读的 ``OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION)`` +
``GetExitCodeProcess``：进程还在跑时退出码是 STILL_ACTIVE(259)。
这个权限位任何进程对同用户进程都有，且不会造成任何副作用。
"""

from __future__ import annotations

import os
import sys

IS_WINDOWS = sys.platform.startswith("win")
_STILL_ACTIVE = 259
_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000


def pid_alive(pid: int) -> bool:
    """进程是否还活着。判断不了时返回 False（宁可当它已死，也不要永远卡住）。"""
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False

    if IS_WINDOWS:
        return _pid_alive_windows(pid)

    try:
        os.kill(pid, 0)          # POSIX：只探测，不发信号
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True              # 存在但不属于当前用户
    except OSError:
        return False


def _pid_alive_windows(pid: int) -> bool:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE,
                                            ctypes.POINTER(wintypes.DWORD)]
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]

    handle = kernel32.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return False             # 打不开：多半已退出（也可能无权限，同样当它不占用）
    try:
        code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
            return False
        return code.value == _STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


def describe(pid: int) -> str:
    """给人看的一句话，附上各平台的确认与清理命令。"""
    alive = pid_alive(pid)
    if IS_WINDOWS:
        how = (f"确认：Get-Process -Id {pid}　"
               f"结束：taskkill /PID {pid} /F")
    else:
        how = f"确认：ps -p {pid}　结束：kill -9 {pid}"
    return f"进程 {pid} {'仍在运行' if alive else '已不存在'}。{how}"
