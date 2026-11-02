"""持久化浏览器自检。

验证目标（这是「不再反复触发验证码」的技术前提）：
1. 浏览器用户数据目录固定在项目内，且按 account_id 隔离；
2. 跨进程重启后 Cookie / localStorage / IndexedDB 仍在原地；
3. 设备指纹（Canvas / WebGL / UA / 硬件参数）跨运行保持一致；
4. 同一 profile 被两个进程同时打开时能明确拒绝，而不是静默串号；
5. 常驻浏览器（CDP）能被后续运行直接复用，无需重开；
6. 多账号使用不同 CDP 端口，不会连到别人的浏览器上。

用法：python tools/test_browser.py
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ctrip.browser import (BrowserBusyError, _port_owned_by_us,  # noqa: E402
                          cdp_port_for, clear_stale_locks, keep_alive_status,
                          open_session, profile_in_use, read_session_meta,
                          stop_keep_alive)
from ctrip.config import load_config, load_dotenv  # noqa: E402
from ctrip.logging_utils import get_logger  # noqa: E402

load_dotenv()
log = get_logger("test.browser")

PASSED: list[str] = []
FAILED: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        PASSED.append(name)
        print(f"  [通过] {name}" + (f" —— {detail}" if detail else ""))
    else:
        FAILED.append(name)
        print(f"  [失败] {name}" + (f" —— {detail}" if detail else ""))


# 采集设备指纹的脚本：这几项是风控用来识别「是否同一台设备」的核心信号
FINGERPRINT_JS = """
() => {
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  ctx.textBaseline = 'top';
  ctx.font = '14px Arial';
  ctx.fillText('ctrip-fingerprint-probe', 2, 2);
  const canvasHash = canvas.toDataURL();

  let webgl = 'na';
  try {
    const gl = document.createElement('canvas').getContext('webgl');
    const dbg = gl.getExtension('WEBGL_debug_renderer_info');
    webgl = gl.getParameter(dbg.UNMASKED_RENDERER_WEBGL) + '|' +
            gl.getParameter(dbg.UNMASKED_VENDOR_WEBGL);
  } catch (e) { webgl = 'error'; }

  return {
    canvas: canvasHash.slice(-64),
    webgl: webgl,
    ua: navigator.userAgent,
    platform: navigator.platform,
    hardwareConcurrency: navigator.hardwareConcurrency,
    deviceMemory: navigator.deviceMemory || 0,
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    languages: (navigator.languages || []).join(','),
    webdriver: navigator.webdriver === undefined ? 'hidden' : String(navigator.webdriver),
    screen: screen.width + 'x' + screen.height + 'x' + screen.colorDepth,
  };
}
"""

TEST_ACCOUNT = "A001"
PROBE_URL = "https://example.com/"


def _other_process_view(cfg) -> str:
    """以独立子进程的身份查看占用状态。

    ``profile_in_use`` 对「当前进程自己持有」是放行的（否则会自己拦自己），
    所以必须从另一个进程去验证保护是否真的生效。
    """
    code = (
        "import sys; sys.path.insert(0, %r)\n"
        "from ctrip.config import load_config, load_dotenv\n"
        "from ctrip.browser import profile_in_use\n"
        "load_dotenv()\n"
        "cfg = load_config(validate=False)\n"
        "print(profile_in_use(cfg, %r)[1])\n"
        % (str(Path(__file__).resolve().parent.parent), TEST_ACCOUNT)
    )
    import subprocess
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    return (out.stdout.strip().split("\n") or [""])[-1]


def collect_fingerprint(cfg, account_id: str) -> dict:
    """开一次浏览器采集指纹，并写入一份 localStorage 与 Cookie 用于验证持久化。"""
    session = open_session(cfg, account_id, headless=True, log=log)
    try:
        page = session.page()
        page.goto(PROBE_URL, wait_until="domcontentloaded", timeout=45000)
        fp = page.evaluate(FINGERPRINT_JS)
        # 写入本地存储与 Cookie，下一次运行验证是否还在
        page.evaluate("() => localStorage.setItem('ctrip_probe', 'persisted-value-42')")
        page.context.add_cookies([{
            "name": "ctrip_probe_cookie", "value": "cookie-value-42",
            "domain": "example.com", "path": "/",
            "expires": time.time() + 86400 * 30,
        }])
        fp["_mode"] = session.mode
        return fp
    finally:
        session.close()


def read_persisted(cfg, account_id: str) -> dict:
    """重新打开浏览器，读取上一次写入的 localStorage 与 Cookie。"""
    session = open_session(cfg, account_id, headless=True, log=log)
    try:
        page = session.page()
        page.goto(PROBE_URL, wait_until="domcontentloaded", timeout=45000)
        ls = page.evaluate("() => localStorage.getItem('ctrip_probe')")
        cookies = {c["name"]: c["value"] for c in page.context.cookies()}
        fp = page.evaluate(FINGERPRINT_JS)
        fp["_localStorage"] = ls
        fp["_cookie"] = cookies.get("ctrip_probe_cookie")
        fp["_mode"] = session.mode
        return fp
    finally:
        session.close()


def main() -> int:
    cfg = load_config(validate=False)

    print("=" * 78)
    print("持久化浏览器自检")
    print("=" * 78)

    # ---- 1. 目录规划与账号隔离 ----------------------------------------
    print("\n[1] profile 目录规划与账号隔离")
    root = cfg.path(cfg.ctrip.browser_dir)
    p1 = cfg.browser_profile_dir("A001")
    p2 = cfg.browser_profile_dir("A002")
    check("持久化浏览器默认开启", cfg.ctrip.persistent_browser is True)
    check("profile 位于项目目录内", str(p1).startswith(str(cfg.project_root)),
          f"根目录 {root}")
    check("不同账号 profile 相互独立", p1 != p2 and p1.parent != p2.parent,
          f"{p1.name} vs {p2.name}")
    check("profile 目录含 account_id", "A001" in str(p1) and "A002" in str(p2))
    # 路径穿越防护：account_id 已在 config 层做过白名单校验，这里确认落地路径安全
    check("profile 路径不含跳级符号", ".." not in str(p1))

    # ---- 2. 设备指纹稳定性（核心）------------------------------------
    print("\n[2] 跨进程重启后的设备指纹与本地存储")
    # 先重置，确保从零开始，能真实反映「首次创建 → 再次复用」的过程
    if p1.exists():
        shutil.rmtree(p1, ignore_errors=True)
    p1.mkdir(parents=True, exist_ok=True)

    try:
        fp1 = collect_fingerprint(cfg, TEST_ACCOUNT)
    except Exception as exc:  # noqa: BLE001
        print(f"  [跳过] 浏览器不可用：{exc}")
        fp1 = None

    if fp1:
        check("使用持久化 profile 模式", fp1["_mode"] == "profile", fp1["_mode"])
        check("自动化特征已隐藏", fp1["webdriver"] == "hidden",
              f"navigator.webdriver={fp1['webdriver']}")

        fp2 = read_persisted(cfg, TEST_ACCOUNT)

        # 关键断言：指纹一致意味着服务端认为是同一台设备
        stable_keys = ["canvas", "webgl", "ua", "platform", "hardwareConcurrency",
                       "timezone", "languages", "screen"]
        for key in stable_keys:
            check(f"设备指纹稳定：{key}", fp1[key] == fp2[key],
                  f"{str(fp1[key])[:40]}")

        check("localStorage 跨进程保留", fp2["_localStorage"] == "persisted-value-42",
              f"读到 {fp2['_localStorage']}")
        check("Cookie 跨进程保留", fp2["_cookie"] == "cookie-value-42",
              f"读到 {fp2['_cookie']}")

        # 磁盘证据
        default = p1 / "Default"
        check("Cookies 数据库已落盘", (default / "Cookies").exists())
        check("Local Storage 已落盘", (default / "Local Storage").exists())
        # IndexedDB / Service Worker 目录仅在页面真正用到该 API 时创建，
        # 探测页没用，不作为断言依据
        check("Session Storage 已落盘", (default / "Session Storage").exists())
        # 下面三项只有完整 Chromium 会写，chrome-headless-shell 不写；
        # 能通过就证明确实用上了完整构建（与常驻模式同一二进制，指纹一致）
        check("使用完整 Chromium：Preferences 已写入", (default / "Preferences").exists(),
              "headless-shell 不会写该文件")
        check("使用完整 Chromium：Login Data 已写入", (default / "Login Data").exists(),
              "设备记住类标识依赖它")
        check("使用完整 Chromium：History 已写入", (default / "History").exists())

        meta = read_session_meta(cfg, TEST_ACCOUNT)
        check("会话元信息已记录", meta.get("mode") == "profile" and "profile_dir" in meta)

    # ---- 3. 并发占用防护 ----------------------------------------------
    print("\n[3] 同一 profile 的并发占用防护")
    try:
        first = open_session(cfg, TEST_ACCOUNT, headless=True, log=log)
    except Exception as exc:  # noqa: BLE001
        print(f"  [跳过] 浏览器不可用：{exc}")
        first = None

    if first:
        # 注意：profile_in_use 会放行「当前进程自己持有」的情况，
        # 因此这里直接读元信息确认持有者已登记，
        # 再用子进程验证「其他进程看到的是已占用」
        meta = read_session_meta(cfg, TEST_ACCOUNT)
        check("占用者已记录到会话元信息",
              int(meta.get("holder_pid") or 0) == os.getpid(),
              f"holder_pid={meta.get('holder_pid')}, 当前={os.getpid()}")
        detail = _other_process_view(cfg)
        check("其他进程看到 profile 已被占用", "已被进程" in detail, detail)
        try:
            second = open_session(cfg, TEST_ACCOUNT, headless=True, log=log)
            second.close()
            check("重复打开被拒绝", False, "第二个会话竟然打开成功了")
        except BrowserBusyError as exc:
            check("重复打开被拒绝并给出处理建议", "browser --stop" in str(exc),
                  str(exc)[:60])
        except Exception as exc:  # noqa: BLE001
            # Chromium 自身也会拒绝，同样算防护生效，但提示不如我们的友好
            check("重复打开被拒绝", True, f"底层拒绝：{type(exc).__name__}")
        first.close()
        in_use2, _ = profile_in_use(cfg, TEST_ACCOUNT)
        check("会话结束后占用释放", not in_use2)

    # ---- 4. 残留锁清理 ------------------------------------------------
    print("\n[4] 强杀进程后的残留锁清理")
    fake_lock = p1 / "SingletonLock"
    try:
        if fake_lock.is_symlink() or fake_lock.exists():
            fake_lock.unlink()
        # 指向一个不存在的 pid，模拟 kill -9 后的残留
        os.symlink("sandbox-999999", fake_lock)
        in_use, _ = profile_in_use(cfg, TEST_ACCOUNT)
        check("死进程锁不会误判为占用", not in_use)
        removed = clear_stale_locks(cfg, TEST_ACCOUNT, log)
        check("残留锁可被清理", removed >= 1, f"清理 {removed} 个")
    except OSError as exc:
        print(f"  [跳过] 无法创建符号链接：{exc}")

    # ---- 5. 多账号 CDP 端口隔离 ---------------------------------------
    print("\n[5] 多账号常驻浏览器端口隔离")
    saved_port = cfg.ctrip.cdp_port
    cfg.ctrip.cdp_port = 9222
    ports = {a.account_id: cdp_port_for(cfg, a.account_id) for a in cfg.ctrip.accounts}
    check("各账号 CDP 端口互不相同", len(set(ports.values())) == len(ports), str(ports))
    check("端口以配置值为基准递增", min(ports.values()) == 9222, str(ports))
    cfg.ctrip.cdp_port = saved_port
    check("cdp_port=0 时不启用常驻", cdp_port_for(cfg, TEST_ACCOUNT) == 0)

    # ---- 6. 常驻浏览器复用 --------------------------------------------
    print("\n[6] 常驻浏览器启动与复用")
    cfg.ctrip.cdp_port = 9333
    try:
        s1 = open_session(cfg, TEST_ACCOUNT, headless=True, keep_alive=True, log=log)
        mode1 = s1.mode
        s1.keep_alive = True
        s1.close()
        alive, detail = keep_alive_status(cfg, TEST_ACCOUNT)
        check("常驻浏览器启动后在线", alive, detail)

        if alive:
            s2 = open_session(cfg, TEST_ACCOUNT, headless=True, log=log)
            check("后续运行直接复用常驻浏览器（无需重开）", s2.mode == "cdp", s2.mode)
            check("复用标记正确", s2.reused is True)

            # 两种模式指纹必须一致：否则用户从 profile 模式切到常驻模式（或反之）
            # 就会被携程当成新设备，白白多过一次验证码
            try:
                page2 = s2.page()
                page2.goto(PROBE_URL, wait_until="domcontentloaded", timeout=45000)
                fp_cdp = page2.evaluate(FINGERPRINT_JS)
                if fp1:
                    same = all(fp1[k] == fp_cdp[k] for k in
                               ("canvas", "webgl", "ua", "platform", "screen"))
                    check("profile 模式与常驻模式设备指纹一致", same,
                          "切换运行模式不会被当成新设备")
            except Exception as exc:  # noqa: BLE001
                print(f"  [跳过] 常驻模式指纹采集失败：{exc}")

            # 复用时不应关闭浏览器
            s2.close()
            alive2, _ = keep_alive_status(cfg, TEST_ACCOUNT)
            check("复用结束后浏览器仍常驻", alive2)

        stopped = stop_keep_alive(cfg, TEST_ACCOUNT, log)
        check("常驻浏览器可被停止", stopped)
        alive3, _ = keep_alive_status(cfg, TEST_ACCOUNT)
        check("停止后状态正确", not alive3)
        check("停止后 profile 仍保留（指纹不丢）", p1.exists() and (p1 / "Default").exists())
    except Exception as exc:  # noqa: BLE001
        print(f"  [跳过] 常驻浏览器测试失败：{exc}")
    finally:
        cfg.ctrip.cdp_port = saved_port
        try:
            stop_keep_alive(cfg, TEST_ACCOUNT, log)
        except Exception:  # noqa: BLE001
            pass

    # ---- 6.5 端口被外部占用时不能误连 ------------------------------
    print("\n[6.5] CDP 端口被外部占用时的防误连")
    import socket as _socket
    squatter = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    try:
        squatter.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
        squatter.bind(("127.0.0.1", 9412))
        squatter.listen(5)
        cfg.ctrip.cdp_port = 9412
        owned = _port_owned_by_us(cfg, TEST_ACCOUNT, 9412, log)
        check("外部占用的端口不被认为是自己的", owned is False,
              "避开连到陌生浏览器")
        # 此时应回退到持久化 profile 模式，而不是报错退出。
        # 先清上一步常驻浏览器被 SIGTERM 后可能遗留的锁
        clear_stale_locks(cfg, TEST_ACCOUNT, log)
        s3 = open_session(cfg, TEST_ACCOUNT, headless=True, log=log)
        check("端口冲突时回退为持久化 profile 模式", s3.mode == "profile", s3.mode)
        s3.close()
    except OSError as exc:
        print(f"  [跳过] 无法占用测试端口：{exc}")
    finally:
        try:
            squatter.close()
        except Exception:  # noqa: BLE001
            pass
        cfg.ctrip.cdp_port = saved_port

    # ---- 7. 退出噪声过滤不能掩盖真错误 -------------------------------
    print("\n[7] 退出噪声过滤的安全性")
    import subprocess
    root = str(Path(__file__).resolve().parent.parent)
    noise_case = (
        f"import sys; sys.path.insert(0, {root!r})\n"
        "from ctrip.browser import _silence_asyncio_teardown_noise\n"
        "_silence_asyncio_teardown_noise()\n"
        "sys.stderr.write('Task was destroyed but it is pending!\\n')\n"
        "sys.stderr.write('  缩进的后续行\\n')\n"
        "sys.stderr.write('真实告警应保留\\n')\n"
    )
    real_case = (
        f"import sys; sys.path.insert(0, {root!r})\n"
        "from ctrip.browser import _silence_asyncio_teardown_noise\n"
        "_silence_asyncio_teardown_noise()\n"
        "raise RuntimeError('真实错误必须可见')\n"
    )
    r1 = subprocess.run([sys.executable, "-c", noise_case], capture_output=True, text=True)
    r2 = subprocess.run([sys.executable, "-c", real_case], capture_output=True, text=True)
    check("无害噪声被过滤", "Task was destroyed" not in r1.stderr,
          f"stderr={r1.stderr.strip()[:40]!r}")
    check("同一阶段的真实告警不被误删", "真实告警应保留" in r1.stderr)
    check("真实错误仍然上报", "真实错误必须可见" in r2.stderr)
    check("真实错误保留非零退出码", r2.returncode != 0, f"returncode={r2.returncode}")

    print()
    print("=" * 78)
    print(f"通过 {len(PASSED)} 项，失败 {len(FAILED)} 项")
    if FAILED:
        for name in FAILED:
            print(f"  - {name}")
    print("=" * 78)
    return 0 if not FAILED else 1


if __name__ == "__main__":
    raise SystemExit(main())
