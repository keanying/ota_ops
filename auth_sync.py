#!/usr/bin/env python3
"""登录态搬运：Windows 抽出 → Linux 植入。放在项目根目录（与 ctrip/ 同级）。

搬什么
------
只搬 ``auth/{account_id}/`` 下的两个文件：

  storage_state.json   Playwright 导出的明文 Cookie + localStorage，
                       采集侧 ``build_session()`` 唯一依赖的东西
  meta.json            login_at / login_name / fingerprint.region_ids

``browser/{account_id}/profile/`` **不需要搬，也搬不了**：
它是 Chromium 用户数据目录，Cookie 在 ``Default/Cookies``（SQLite）里，
值用 Windows DPAPI 加密，密钥绑在本机 Windows 用户账户上，Linux 解不开。
而且 Linux 端根本不开浏览器 —— ``ensure_logged_in`` 复用登录态那条路径
走的是 requests，profile 只在 ``login()`` 里用来保设备指纹、少触发滑块。

meta.json 为什么必须一起搬
--------------------------
``ensure_logged_in()`` 先看 ``CookieStore.is_fresh()``，它读 meta 里的
``login_at`` 与 ``cookie_ttl_hours`` 比对。只搬 storage_state 的话
is_fresh 返回 False，Linux 会直接走自动重登；headless 下遇到验证码就
抛 CaptchaRequired，整个采集失败。

用法
----
  # Windows：导出（--refresh 会先打开 profile 重新导一次登录态）
  python auth_sync.py export --account-id A001
  python auth_sync.py export --account-id A001 --refresh -o A001.b64
  python auth_sync.py export --all -o all.b64

  # Linux：植入
  python auth_sync.py import A001.b64
  type A001.b64 | python auth_sync.py import -          # Windows 管道
  cat A001.b64 | python auth_sync.py import -           # Linux 管道

  # 两边都能用：校验（走项目自己的 verify_cookie）
  python auth_sync.py check --account-id A001
"""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ctrip.config import load_config  # noqa: E402

FILES = ("storage_state.json", "meta.json")


# ---------------------------------------------------------------- 工具
def account_ids(cfg, args) -> list[str]:
    if getattr(args, "all", False):
        return [a.account_id for a in cfg.ctrip.accounts]
    if args.account_id:
        return [args.account_id]
    ids = [a.account_id for a in cfg.ctrip.accounts]
    if len(ids) == 1:
        return ids
    sys.exit(f"❌ 配置里有 {len(ids)} 个账号，请用 --account-id 指定，或 --all")


def summarize(state: dict) -> str:
    from collections import Counter

    cookies = state.get("cookies") or []
    dist = Counter(c.get("domain", "?") for c in cookies)
    top = "、".join(f"{d}×{n}" for d, n in dist.most_common(4))
    ls = sum(len(o.get("localStorage") or []) for o in state.get("origins") or [])
    return f"{len(cookies)} 个 Cookie（{top}），localStorage {ls} 项"


def refresh_from_profile(cfg, account_id: str) -> bool:
    """打开该账号的持久化 profile，重新导出 storage_state。

    用于 auth/ 里的登录态已过期、但浏览器 profile 里还是登录态的情况 ——
    直接把 profile 里的最新 Cookie 抽出来，不用重登。
    走的就是项目自己的 open_session()，参数、UA、时区完全一致。
    """
    from ctrip.auth import CookieStore, verify_cookie
    from ctrip.browser import BrowserBusyError, open_session
    from ctrip.logging_utils import get_logger

    log = get_logger(f"sync.{account_id}")
    print(f"  打开 profile 重新导出：{cfg.browser_profile_dir(account_id)}")
    try:
        session = open_session(cfg, account_id, headless=True, log=log)
    except BrowserBusyError as exc:
        print(f"  ❌ {exc}")
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"  ❌ 浏览器启动失败：{exc}")
        return False

    try:
        page = session.page()
        try:
            # 先访问一次后台首页，让业务域 Cookie 刷新后再导出
            page.goto(cfg.ctrip.home_url, wait_until="domcontentloaded",
                      timeout=cfg.ctrip.nav_timeout * 1000)
            page.wait_for_timeout(2500)
            print(f"  当前地址：{page.url}")
            if "login" in page.url.lower():
                print("  ⚠️  profile 里也是未登录状态，导出的登录态不可用")
        except Exception as exc:  # noqa: BLE001
            print(f"  ⚠️  访问首页失败（仍尝试导出）：{exc}")

        cookie_file = cfg.cookie_file(account_id)
        cookie_file.parent.mkdir(parents=True, exist_ok=True)
        session.context.storage_state(path=str(cookie_file))
    finally:
        session.close()

    CookieStore(cfg).save_meta(
        account_id,
        login_at=datetime.now().isoformat(timespec="seconds"),
        refreshed_from_profile=True,
    )
    ok, detail = verify_cookie(cfg, account_id)
    print(f"  {'✅' if ok else '❌'} 重新导出后校验：{detail}")
    return ok


# ---------------------------------------------------------------- export
def cmd_export(args) -> int:
    cfg = load_config(args.config, validate=False)
    ids = account_ids(cfg, args)

    bundle: dict[str, dict] = {}
    for account_id in ids:
        print(f"[{account_id}]")
        if args.refresh and not refresh_from_profile(cfg, account_id):
            if not args.force:
                print("   跳过该账号（加 --force 可强制导出当前文件）\n")
                continue

        payload: dict[str, str] = {}
        for name in FILES:
            p = cfg.cookie_dir(account_id) / name
            if not p.exists():
                if name == "storage_state.json":
                    print(f"  ❌ 缺少 {p}，请先执行 login\n")
                    payload = {}
                    break
                print(f"  ⚠️  缺少 {name}（Linux 端会因 is_fresh 失败而触发重登）")
                continue
            payload[name] = p.read_text(encoding="utf-8-sig")
        if not payload:
            continue

        state = json.loads(payload["storage_state.json"])
        print(f"  {summarize(state)}")

        meta = json.loads(payload.get("meta.json") or "{}")
        if meta.get("login_at"):
            print(f"  登录于 {meta['login_at']}"
                  f"（TTL {cfg.ctrip.cookie_ttl_hours}h）")
        fp = (meta.get("fingerprint") or {}).get("region_count")
        if fp:
            print(f"  指纹：{fp} 个目的地")

        if args.verify:
            from ctrip.auth import verify_cookie
            ok, detail = verify_cookie(cfg, account_id)
            print(f"  {'✅' if ok else '❌'} 校验：{detail}")
            if not ok and not args.force:
                print("   跳过该账号（加 --force 可强制导出）\n")
                continue

        bundle[account_id] = payload
        print()

    if not bundle:
        print("❌ 没有可导出的账号")
        return 1

    raw = json.dumps({"version": 1, "exported_at": datetime.now().isoformat(),
                      "accounts": bundle}, ensure_ascii=False).encode()
    blob = base64.b64encode(gzip.compress(raw, 9)).decode()

    if args.out:
        Path(args.out).write_text(blob, encoding="utf-8")
        print(f"✅ 已导出 {len(bundle)} 个账号 → {args.out}（{len(blob)} 字符）")
        print(f"   传到 Linux 后执行: python auth_sync.py import {Path(args.out).name}")
    else:
        print(f"# {len(bundle)} 个账号，复制下面整行到 Linux 执行 import",
              file=sys.stderr)
        print(blob)
    print("\n⚠️  这份内容等同于登录凭证，走 scp，别发聊天工具，别进 git")
    return 0


# ---------------------------------------------------------------- import
def cmd_import(args) -> int:
    cfg = load_config(args.config, validate=False)

    if args.source == "-":
        blob = sys.stdin.read()
    else:
        p = Path(args.source)
        blob = p.read_text(encoding="utf-8") if p.exists() else args.source
    blob = "".join(blob.split())          # 容忍复制时混入的换行空格

    try:
        data = json.loads(gzip.decompress(base64.b64decode(blob)))
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"❌ 解析失败：{exc}\n   确认复制完整、中间没有截断")

    accounts = data.get("accounts") or {}
    if not accounts:
        sys.exit("❌ 内容里没有账号数据")
    print(f"来源导出于 {data.get('exported_at', '?')}，共 {len(accounts)} 个账号\n")

    wanted = {args.account_id} if args.account_id else set(accounts)
    failed = []
    for account_id, payload in accounts.items():
        if account_id not in wanted:
            continue
        print(f"[{account_id}]")
        target = cfg.cookie_dir(account_id)

        for name in FILES:
            if name not in payload:
                continue
            p = target / name
            if p.exists() and not args.force:
                backup = p.with_suffix(p.suffix + ".bak")
                p.replace(backup)
                print(f"  原文件已备份为 {backup.name}")
            p.write_text(payload[name], encoding="utf-8")
            try:
                os.chmod(p, 0o600)        # 与 CookieStore.save_meta 保持一致
            except OSError:
                pass
            print(f"  ✅ 写入 {p}")

        state = json.loads(payload["storage_state.json"])
        print(f"  {summarize(state)}")

        # 时钟差处理：login_at 是不带时区的本地时间，两台机器时区不同会让
        # is_fresh 误判。--touch 把它改成本机当前时间。
        if args.touch and "meta.json" in payload:
            from ctrip.auth import CookieStore
            CookieStore(cfg).save_meta(
                account_id,
                login_at=datetime.now().isoformat(timespec="seconds"),
                imported_at=datetime.now().isoformat(timespec="seconds"))
            print("  已把 login_at 刷成本机当前时间")

        if not args.no_verify:
            from ctrip.auth import CookieStore, verify_cookie
            ok, detail = verify_cookie(cfg, account_id)
            print(f"  {'✅' if ok else '❌'} 校验：{detail}")
            fresh = CookieStore(cfg).is_fresh(account_id)
            print(f"  {'✅' if fresh else '⚠️'} is_fresh={fresh}"
                  f"（决定采集时是否直接复用，False 会触发重登）")
            if not fresh:
                print("     两台机器时区/时钟不一致时，重跑一次并加 --touch")
            if not ok:
                failed.append(account_id)
        print()

    if failed:
        print(f"❌ 校验未通过：{failed}")
        print("   多半是源端登录态本就失效，或本机出口 IP 触发了风控")
        return 1
    print("✅ 全部完成，可以直接跑 run_collect.sh")
    return 0


# ---------------------------------------------------------------- check
def cmd_check(args) -> int:
    from ctrip.auth import CookieStore, verify_cookie

    cfg = load_config(args.config, validate=False)
    store = CookieStore(cfg)
    bad = 0
    for account_id in account_ids(cfg, args):
        print(f"[{account_id}]")
        if not store.exists(account_id):
            print("  ❌ 没有登录态文件\n")
            bad += 1
            continue
        meta = store.load_meta(account_id)
        print(f"  登录于 {meta.get('login_at', '?')}　"
              f"最近校验 {meta.get('last_check_at', '?')}")
        print(f"  is_fresh={store.is_fresh(account_id)}"
              f"（TTL {cfg.ctrip.cookie_ttl_hours}h）")
        fp = meta.get("fingerprint") or {}
        if fp:
            print(f"  指纹：{fp.get('region_count')} 个目的地")
        ok, detail = verify_cookie(cfg, account_id)
        print(f"  {'✅' if ok else '❌'} {detail}\n")
        if not ok:
            bad += 1
    return 1 if bad else 0


# ---------------------------------------------------------------- CLI
def main() -> int:
    p = argparse.ArgumentParser(
        description="ctrip_collector 登录态搬运（Windows 抽出 → Linux 植入）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="典型流程:\n"
               "  Windows> python auth_sync.py export --account-id A001 -o A001.b64\n"
               "  scp A001.b64 user@linux:~/ctrip_collector-1.0.0/\n"
               "  Linux>   python auth_sync.py import A001.b64\n"
               "  Linux>   bash run_collect.sh",
    )
    p.add_argument("-c", "--config", default=None, help="配置文件路径")
    sub = p.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("export", help="导出登录态为一行 base64")
    e.add_argument("--account-id", default=None)
    e.add_argument("--all", action="store_true", help="导出配置里所有账号")
    e.add_argument("--refresh", action="store_true",
                   help="先打开浏览器 profile 重新导出登录态（不重登）")
    e.add_argument("--verify", action="store_true", help="导出前先校验有效性")
    e.add_argument("--force", action="store_true", help="校验失败也照样导出")
    e.add_argument("-o", "--out", default=None, help="写入文件；不给则打印到标准输出")
    e.set_defaults(func=cmd_export)

    i = sub.add_parser("import", help="把导出内容植入 auth/")
    i.add_argument("source", help="base64 文件路径，或 - 从标准输入读，或直接粘贴内容")
    i.add_argument("--account-id", default=None, help="只植入其中某个账号")
    i.add_argument("--touch", action="store_true",
                   help="把 login_at 改成本机当前时间（两台机器时钟不一致时用）")
    i.add_argument("--force", action="store_true", help="覆盖已有文件且不备份")
    i.add_argument("--no-verify", action="store_true", help="植入后不校验")
    i.set_defaults(func=cmd_import)

    c = sub.add_parser("check", help="校验登录态（走项目自己的 verify_cookie）")
    c.add_argument("--account-id", default=None)
    c.add_argument("--all", action="store_true")
    c.set_defaults(func=cmd_check)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())