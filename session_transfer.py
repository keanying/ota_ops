#!/usr/bin/env python3
"""携程登录态搬运工具 —— 独立脚本，零依赖（纯标准库），不需要装项目。

要搬的东西就是 Playwright 的 storage_state 文件（Cookie + localStorage 的
纯 JSON），与操作系统无关。采集侧走 HTTP 请求不开浏览器，不存在浏览器指纹。

用法
----
  # Windows：导出成一行 base64（gzip 压缩过，方便直接复制粘贴，不用 scp）
  python session_transfer.py dump
  python session_transfer.py dump -f auth/ctrip_auth_default.json -o state.txt

  # Linux：粘贴那行 base64 写回文件
  python session_transfer.py load state.txt -f auth/ctrip_auth_A001.json
  echo "H4sIA..." | python session_transfer.py load - -f auth/ctrip_auth.json

  # 两边都能用：验证登录态还有效吗（打真实接口）
  python session_transfer.py check -f auth/ctrip_auth_default.json

  # 看看里面有什么（不打印 Cookie 值）
  python session_transfer.py info -f auth/ctrip_auth_default.json

注意
----
storage_state 等同于密码。传输别发聊天工具，别进 git。
搬到新机器后先 check 一次：文件本身跨系统没问题，但 IP 变了可能触发风控。
"""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_FILE = "auth/ctrip_auth_default.json"
PROBE_URL = "https://m.ctrip.com/restapi/soa2/13807/getVendorDstList"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


# ---------------------------------------------------------------- 读写
def read_state(path: Path) -> dict:
    if not path.exists():
        sys.exit(f"❌ 文件不存在: {path}")
    try:
        # utf-8-sig 容忍 Windows 记事本加的 BOM
        state = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as e:
        sys.exit(f"❌ 不是合法 JSON: {e}\n"
                 f"   传输时请用二进制方式，别用记事本另存为 ANSI")
    if not isinstance(state.get("cookies"), list) or not state["cookies"]:
        sys.exit("❌ 文件里没有 cookies，可能不是 Playwright 的 storage_state")
    return state


def cookie_header(state: dict) -> str:
    """把 storage_state 里的 ctrip 域 Cookie 拼成请求头。"""
    pairs = []
    seen = set()
    for c in state.get("cookies", []):
        domain = (c.get("domain") or "").lstrip(".")
        if not domain.endswith("ctrip.com"):
            continue
        name = c.get("name")
        if name and name not in seen:
            seen.add(name)
            pairs.append(f"{name}={c.get('value', '')}")
    return "; ".join(pairs)


# ---------------------------------------------------------------- 命令
def cmd_dump(args) -> int:
    state = read_state(Path(args.file))
    raw = json.dumps(state, ensure_ascii=False, separators=(",", ":")).encode()
    blob = base64.b64encode(gzip.compress(raw, 9)).decode()

    if args.out:
        Path(args.out).write_text(blob, encoding="utf-8")
        print(f"✅ 已导出 {args.out}"
              f"（{len(state['cookies'])} 个 Cookie，{len(blob)} 字符）")
        print("   把这个文件内容复制到目标机器，用 load 写回")
    else:
        print(f"# {len(state['cookies'])} 个 Cookie，"
              f"复制下面整行到目标机器执行 load", file=sys.stderr)
        print(blob)
    return 0


def cmd_load(args) -> int:
    if args.source == "-":
        blob = sys.stdin.read().strip()
    else:
        p = Path(args.source)
        blob = p.read_text(encoding="utf-8").strip() if p.exists() \
            else args.source.strip()

    blob = "".join(blob.split())          # 容忍复制时混入的换行和空格
    try:
        raw = gzip.decompress(base64.b64decode(blob))
        state = json.loads(raw)
    except Exception as e:
        sys.exit(f"❌ 解析失败: {e}\n   确认复制完整，中间没有截断")

    if not isinstance(state.get("cookies"), list) or not state["cookies"]:
        sys.exit("❌ 内容里没有 cookies")

    out = Path(args.file)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists() and not args.force:
        sys.exit(f"❌ 目标已存在: {out}\n   确认要覆盖请加 --force")
    out.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    print(f"✅ 已写入 {out}（{len(state['cookies'])} 个 Cookie）")
    print(f"   验证一下: python {Path(sys.argv[0]).name} check -f {out}")
    return 0


def cmd_check(args) -> int:
    state = read_state(Path(args.file))
    cookies = cookie_header(state)
    if not cookies:
        sys.exit("❌ 没有 ctrip.com 域的 Cookie")

    req = urllib.request.Request(
        PROBE_URL, data=b"{}", method="POST",
        headers={
            "Content-Type": "application/json;charset=UTF-8",
            "appname": "vbkbusiness",
            "Origin": "https://vbooking.ctrip.com",
            "Referer": "https://vbooking.ctrip.com/",
            "User-Agent": UA,
            "Cookie": cookies,
        },
    )
    print(f"探测 {PROBE_URL}")
    print(f"  携带 {len(cookies.split(';'))} 个 Cookie")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", "replace")
            status = resp.status
            final = resp.url
    except urllib.error.HTTPError as e:
        print(f"  ❌ HTTP {e.code}")
        print(f"     {e.read()[:200].decode('utf-8', 'replace')}")
        return 1
    except Exception as e:
        print(f"  ❌ 请求失败: {e}")
        return 1

    print(f"  状态码 {status}　最终地址 {final}")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        print(f"  ❌ 返回的不是 JSON（多半被登录页拦截了）")
        print(f"     {body[:200]}")
        return 1

    ack = data.get("ResponseStatus", {}).get("Ack")
    dst = data.get("dstList", []) or []
    if ack == "Success":
        print(f"  ✅ 登录态有效（Ack=Success，{len(dst)} 个目的地）")
        for d in dst[:5]:
            print(f"     {d.get('regionId')} {d.get('regionName')}")
        return 0

    print(f"  ❌ 登录态无效（Ack={ack}）")
    print(f"     {body[:300]}")
    return 1


def cmd_info(args) -> int:
    state = read_state(Path(args.file))
    cookies = state.get("cookies", [])
    print(f"文件: {args.file}")
    print(f"Cookie 数量: {len(cookies)}")

    domains: dict[str, int] = {}
    soonest = None
    for c in cookies:
        d = c.get("domain", "?")
        domains[d] = domains.get(d, 0) + 1
        exp = c.get("expires")
        if isinstance(exp, (int, float)) and exp > 0:
            soonest = exp if soonest is None else min(soonest, exp)

    print("按域名分布:")
    for d, n in sorted(domains.items(), key=lambda kv: -kv[1]):
        print(f"  {d:<28} {n}")

    if soonest:
        t = datetime.fromtimestamp(soonest, tz=timezone.utc).astimezone()
        left = t - datetime.now(t.tzinfo)
        print(f"最早过期的 Cookie: {t:%Y-%m-%d %H:%M:%S}"
              f"（还剩 {left.days} 天 {left.seconds // 3600} 小时）")
        print("  注意：这只是浏览器侧的过期时间，服务端可能更早让它失效")

    origins = state.get("origins", []) or []
    if origins:
        print(f"localStorage 来源: {len(origins)} 个"
              f"（采集走 HTTP 用不到，仅 Playwright 复用时有意义）")
    print("\n（不打印 Cookie 值：它等同于密码）")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(
        description="携程登录态搬运（导出 / 导入 / 验证）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="示例:\n"
               "  python session_transfer.py dump -o state.txt\n"
               "  python session_transfer.py load state.txt -f auth/ctrip_auth_A001.json\n"
               "  python session_transfer.py check -f auth/ctrip_auth_A001.json",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_file(sp):
        sp.add_argument("-f", "--file", default=DEFAULT_FILE,
                        help=f"storage_state 路径（默认 {DEFAULT_FILE}）")

    d = sub.add_parser("dump", help="导出为一行 base64")
    add_file(d)
    d.add_argument("-o", "--out", default=None, help="写入文件；不给则打印到标准输出")
    d.set_defaults(func=cmd_dump)

    l = sub.add_parser("load", help="把 base64 写回 storage_state 文件")
    l.add_argument("source", help="base64 文件路径，或 - 从标准输入读，或直接粘贴内容")
    add_file(l)
    l.add_argument("--force", action="store_true", help="覆盖已存在的目标文件")
    l.set_defaults(func=cmd_load)

    c = sub.add_parser("check", help="打真实接口验证登录态是否有效")
    add_file(c)
    c.set_defaults(func=cmd_check)

    i = sub.add_parser("info", help="查看 Cookie 数量、域名分布、过期时间")
    add_file(i)
    i.set_defaults(func=cmd_info)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())