#!/usr/bin/env python3
"""WebSocket 命令行客户端 —— 不开浏览器页面也能试调用。

FastAPI 的 /docs 只列 HTTP 路由，WebSocket 不在 OpenAPI 里，所以用这个来试。

用法
----
    # 看协议说明（也可以浏览器打开 http://host:8800/api/ws-spec）
    python -m webconsole.ws_client spec

    # 跑一次完整登录：自动填表 → 卡验证时提示你 → 短信码在终端输入
    python -m webconsole.ws_client login --account-id vbk_569202
    python -m webconsole.ws_client login --account-id vbk_569202 \\
        --account vbk_569202 --password 'xxx'

    # 标准测试流程：连通性 → 启动浏览器 → 收帧 → 导航 → 输入 → 登录通道
    python -m webconsole.ws_client smoke --account-id vbk_569202
    python -m webconsole.ws_client smoke --account-id vbk_569202 --login --stop

    # 只观察主通道：画面帧、状态、采集日志，不发任何指令
    python -m webconsole.ws_client watch --account-id vbk_569202

    # 手动发一条指令到主通道（导航、输入文本、重取画面…）
    python -m webconsole.ws_client send --account-id vbk_569202 \\
        --json '{"type":"nav","action":"goto"}'

默认连 http://127.0.0.1:8800，用 --host / --port 改。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

try:
    import websockets
except ImportError:  # pragma: no cover
    sys.exit("缺少依赖：pip install websockets")


# ---------------------------------------------------------------- 协议说明
WS_SPEC = {
    "endpoints": [
        {
            "path": "/api/onboard",
            "用途": "新商户上线（推荐，简化版）：建号或更新 → 登录 → 需要时人工验证 → 成功 → 服务端关闭连接。"
                    "成功后账号自动进入每日定时采集，浏览器自动关闭。",
            "客户端发送": [
                {"action": "start", "scenic_id": "PFTSCA01018088", "scenic_name": "九龙潭",
                 "account": "vbk_569202", "password": "…",
                 "说明": "连上后的第一条消息。账号、密码必填；同一个账号再发一次会更新密码 / 景区，不会报已存在"},
                {"action": "code", "code": "123456", "说明": "收到 need_code 后回传短信验证码，后端自动填入并登录"},
                {"action": "cancel", "说明": "取消本次登录"},
            ],
            "服务端推送": [
                {"event": "accepted", "account_id": "vbk_569202", "created": True,
                 "说明": "账号已写入（created=false 表示已有账号，已更新）"},
                {"event": "progress", "step": "browser|filling|submitted|notified|code", "message": "…",
                 "说明": "过程播报，前端显示即可"},
                {"event": "need_code", "phone": "138****8888",
                 "说明": "需要短信验证码：已自动点过「获取验证码」并发了飞书通知，回传 code 即可"},
                {"event": "need_captcha", "link": "http://…/?account=vbk_569202",
                 "说明": "需要人工拖滑块：已发飞书通知，打开 link 在控制台里完成，完成后自动继续"},
                {"event": "success", "account_id": "vbk_569202", "scheduled": True,
                 "schedule": "已加入定时采集：每天 00:10 采交易数据；…",
                 "说明": "登录成功、登录态已保存。随后服务端关闭连接"},
                {"event": "failed", "message": "…", "说明": "失败或取消。随后服务端关闭连接"},
            ],
        },
        {
            "path": "/api/login/{account_id}",
            "用途": "跑一次完整登录。登录成功或客户端断开即关闭连接。",
            "客户端发送": [
                {"action": "login", "account": "vbk_569202", "password": "…",
                 "timeout": 600, "sms_wait": 300, "persist": True,
                 "说明": "account/password 可省略，省略则用 config.yaml 里的；"
                         "persist 默认 true —— 登录成功后把凭据写回 config.yaml，"
                         "失败的凭据不会保存"},
                {"action": "sms_code", "code": "123456",
                 "说明": "收到 stage=need_sms 后回传"},
                {"action": "create_login", "name": "九龙潭商户",
                 "account": "vbk_888888", "password": "…", "remark": "九龙潭",
                 "scenic_id": "PFTSCA01018088", "persist": True,
                 "collect_after_login": {"job": "day", "years": 2},
                 "说明": "建账号 + 登录 + 首采，一条连接走完。"
                         "account_id 自动取登录账号；连接地址用 /api/login/new 即可，"
                         "建完会切到新账号"},
                {"action": "abort", "说明": "中止本次登录"},
            ],
            "服务端推送": [
                {"type": "login", "stage": "creating", "说明": "正在建账号（仅 create_login）"},
                {"type": "login", "stage": "account_created", "account_id": "vbk_888888",
                 "说明": "账号已写入 config.yaml"},
                {"type": "login", "stage": "starting", "说明": "浏览器就绪中"},
                {"type": "login", "stage": "filling", "说明": "正在填账号密码"},
                {"type": "login", "stage": "submitted", "说明": "已提交，识别页面状态"},
                {"type": "login", "stage": "need_captcha",
                 "说明": "需要人工拖滑块，同时会发飞书通知"},
                {"type": "login", "stage": "need_sms", "phone": "138****8888",
                 "说明": "已自动点过获取验证码，等你回传"},
                {"type": "login", "stage": "sms_filling", "说明": "正在填入验证码并提交"},
                {"type": "login", "stage": "success", "verified": True,
                 "persisted": True, "persisted_fields": ["password"],
                 "说明": "登录态已保存并校验；persisted 表示凭据是否写回了配置"},
                {"type": "config_changed", "fields": ["password"],
                 "说明": "配置已更新，服务端已重载"},
                {"type": "login", "stage": "collecting", "label": "日数据 2024-09-08~2026-09-08",
                 "说明": "首次登录成功后自动发起的采集；"
                         "采集的逐行日志走主通道 /ws/{account_id}"},
                {"type": "login", "stage": "failed|aborted"},
                {"type": "notify", "sent": True, "link": "http://…/?account=…",
                 "说明": "飞书通知的发送回执"},
            ],
        },
        {
            "path": "/ws/{account_id}",
            "用途": "主通道：画面帧下行、鼠标键盘上行、采集日志。",
            "客户端发送": [
                {"type": "mouse", "action": "move|down|up", "x": 0, "y": 0,
                 "button": "left", "buttons": 1, "clickCount": 1, "modifiers": 0},
                {"type": "wheel", "x": 0, "y": 0, "deltaX": 0, "deltaY": 600},
                {"type": "key", "action": "down|up", "key": "a", "code": "KeyA",
                 "keyCode": 65, "text": "a"},
                {"type": "text", "text": "中文", "说明": "整段插入，绕开输入法"},
                {"type": "nav", "action": "goto|back|forward|reload", "url": "…"},
                {"type": "fill_login", "说明": "只填表不提交"},
                {"type": "save_state", "说明": "保存登录态并校验"},
                {"type": "refresh_frame", "说明": "补一张当前画面"},
                {"type": "sms_code", "code": "123456",
                 "说明": "不在登录流程中时，直接填进页面的验证码框"},
                {"type": "quality", "value": 60},
            ],
            "服务端推送": [
                {"type": "frame", "data": "<base64 jpeg>", "meta": {}, "keyframe": False},
                {"type": "status", "state": "starting|live|closed|error"},
                {"type": "url", "url": "…", "title": "…"},
                {"type": "task", "level": "info|warn|error|done", "message": "…"},
                {"type": "auth", "ok": True, "detail": "…"},
            ],
        },
        {
            "path": "/vnc/{account_id}",
            "用途": "VNC 模式下的 RFB 二进制桥接，由 noVNC 连接，不是 JSON 协议。",
        },
    ],
    "相关 HTTP 接口": {
        "GET /api/accounts": "账号列表与登录态",
        "POST /api/accounts": "新建账号，account_id 取登录账号本身",
        "POST /api/browser/{id}/start": "启动浏览器（最多同步等 6 秒）",
        "POST /api/browser/{id}/stop": "关闭浏览器",
        "POST /api/collect/{id}/day|month": "发起采集",
        "GET/POST /api/settings": "飞书通知配置",
        "POST /feishu/callback": "飞书事件订阅回调（URL 验证）",
    },
}


# ---------------------------------------------------------------- 工具
def ws_url(args, path: str) -> str:
    scheme = "wss" if args.tls else "ws"
    return f"{scheme}://{args.host}:{args.port}{path}"


def show(msg: dict) -> None:
    t = msg.get("type")
    if t == "frame":
        n = len(msg.get("data") or "")
        print(f"  [帧] {n // 1024} KB{' 关键帧' if msg.get('keyframe') else ''}")
    elif t == "login":
        print(f"  [登录:{msg.get('stage')}] {msg.get('message', '')}"
              + (f"  手机 {msg['phone']}" if msg.get("phone") else ""))
    elif t == "notify":
        print(f"  [飞书] {'已发送' if msg.get('sent') else '失败'}"
              f"  {msg.get('link') or msg.get('detail', '')}")
    elif t == "url":
        print(f"  [url] {msg.get('title') or ''}  {msg.get('url', '')}".rstrip())
    elif t == "status":
        print(f"  [状态:{msg.get('state')}] {msg.get('message', '')}")
    else:
        print(f"  [{t}] {msg.get('message') or json.dumps(msg, ensure_ascii=False)[:120]}")


# ---------------------------------------------------------------- 命令
async def cmd_login(args) -> int:
    url = ws_url(args, f"/api/login/{args.account_id}")
    print(f"连接 {url}")
    async with websockets.connect(url, max_size=None) as ws:
        payload = {"action": "login", "timeout": args.timeout,
                   "sms_wait": args.sms_wait}
        if args.account:
            payload["account"] = args.account
        if args.password:
            payload["password"] = args.password
        await ws.send(json.dumps(payload))
        print(f"→ {json.dumps({**payload, 'password': '***'}, ensure_ascii=False)}")

        loop = asyncio.get_running_loop()
        while True:
            raw = await ws.recv()
            msg = json.loads(raw)
            show(msg)

            if msg.get("type") != "login":
                continue
            stage = msg.get("stage")
            if stage == "need_captcha":
                print("  ⚠ 需要人工拖滑块：打开控制台页面完成，这里会自动继续")
            elif stage == "need_sms":
                # 阻塞式 input 会卡住事件循环，放到线程里读
                code = await loop.run_in_executor(
                    None, input, "  请输入收到的短信验证码（直接回车跳过）: ")
                code = (code or "").strip()
                if code:
                    await ws.send(json.dumps({"action": "sms_code", "code": code}))
                    print(f"→ 已提交验证码 {code}")
            elif stage in ("success", "failed", "aborted"):
                print(f"\n结束：{stage}")
                return 0 if stage == "success" else 1


async def cmd_onboard(args) -> int:
    """新商户上线：/api/onboard 的命令行版本，也是最小的调用示例。"""
    url = ws_url(args, "/api/onboard")
    print(f"连接 {url}")
    async with websockets.connect(url, max_size=None) as ws:
        await ws.send(json.dumps({"action": "start", "scenic_id": args.scenic_id,
                                  "scenic_name": args.scenic_name, "account": args.account,
                                  "password": args.password}, ensure_ascii=False))
        loop = asyncio.get_running_loop()
        async for raw in ws:                      # 服务端发完 success / failed 会主动关闭
            msg = json.loads(raw)
            ev = msg.get("event")
            print(f"  [{ev}] {msg.get('message', '')}"
                  + (f"  {msg['schedule']}" if msg.get("schedule") else "")
                  + (f"  {msg['link']}" if msg.get("link") else ""))
            if ev == "need_code":
                code = await loop.run_in_executor(None, input, "  请输入短信验证码: ")
                await ws.send(json.dumps({"action": "code", "code": code.strip()}))
            elif ev in ("success", "failed"):
                return 0 if ev == "success" else 1
    return 1


async def cmd_watch(args) -> int:
    url = ws_url(args, f"/ws/{args.account_id}")
    print(f"连接 {url}（Ctrl-C 退出）")
    frames = 0
    async with websockets.connect(url, max_size=None) as ws:
        while True:
            msg = json.loads(await ws.recv())
            if msg.get("type") == "frame":
                frames += 1
                if args.frames:
                    show(msg)
                elif frames % 20 == 1:
                    print(f"  [帧] 已收到 {frames} 帧")
                continue
            show(msg)


async def cmd_send(args) -> int:
    try:
        payload = json.loads(args.json)
    except json.JSONDecodeError as exc:
        sys.exit(f"--json 不是合法 JSON：{exc}")
    url = ws_url(args, f"/ws/{args.account_id}")
    async with websockets.connect(url, max_size=None) as ws:
        await ws.send(json.dumps(payload))
        print(f"→ {json.dumps(payload, ensure_ascii=False)}")
        end = asyncio.get_running_loop().time() + args.wait
        while asyncio.get_running_loop().time() < end:
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=0.5))
            except asyncio.TimeoutError:
                continue
            if msg.get("type") == "frame" and not args.frames:
                continue
            show(msg)
    return 0


async def cmd_smoke(args) -> int:
    """标准 WebSocket 测试流程：按真实使用顺序把每一步都走一遍。

    不需要真的登录携程 —— 默认只验证协议链路（能不能连、能不能收帧、
    指令有没有生效）。加 --login 才会真跑一次登录。
    """
    import requests

    base = f"{'https' if args.tls else 'http'}://{args.host}:{args.port}"
    aid = args.account_id
    results: list[tuple[str, bool, str]] = []

    def step(name: str, ok: bool, detail: str = "") -> None:
        results.append((name, ok, detail))
        print(f"  {'✅' if ok else '❌'} {name}{'  ' + detail if detail else ''}")

    print(f"\n目标 {base}　账号 {aid}\n" + "─" * 56)

    # ---- 1. HTTP 侧 ----
    try:
        accounts = requests.get(f"{base}/api/accounts", timeout=10).json()
        ids = [a["account_id"] for a in accounts["accounts"]]
        step("HTTP /api/accounts", aid in ids,
             f"模式 {accounts.get('stream')}　账号 {ids}")
        if aid not in ids:
            print(f"\n账号 {aid} 不在列表里，先创建或换一个 --account-id")
            return 1
    except Exception as exc:  # noqa: BLE001
        step("HTTP /api/accounts", False, str(exc))
        return 1

    try:
        spec = requests.get(f"{base}/api/ws-spec", timeout=10).json()
        step("HTTP /api/ws-spec", len(spec.get("endpoints", [])) >= 2)
    except Exception as exc:  # noqa: BLE001
        step("HTTP /api/ws-spec", False, str(exc))

    # ---- 2. 启动浏览器 ----
    t0 = time.time()
    try:
        r = requests.post(f"{base}/api/browser/{aid}/start", timeout=180).json()
        step("启动浏览器", bool(r.get("ok")),
             f"{time.time() - t0:.1f}s　ready={r.get('ready')}　模式 {r.get('mode')}")
    except Exception as exc:  # noqa: BLE001
        step("启动浏览器", False, str(exc))
        return 1

    # ---- 3. 主通道 ----
    frames = titles = 0
    got_live = False
    url = ws_url(args, f"/ws/{aid}")
    try:
        async with websockets.connect(url, max_size=None) as ws:
            step("连上主通道 /ws", True, url)

            async def drain(sec: float) -> None:
                nonlocal frames, titles, got_live
                end = time.time() + sec
                while time.time() < end:
                    try:
                        m = json.loads(await asyncio.wait_for(ws.recv(), timeout=0.3))
                    except asyncio.TimeoutError:
                        continue
                    t = m.get("type")
                    if t == "frame":
                        frames += 1
                    elif t == "url":
                        titles += 1
                    elif t == "status" and m.get("state") == "live":
                        got_live = True

            await ws.send(json.dumps({"type": "refresh_frame"}))
            await drain(6)
            step("收到画面帧", frames > 0, f"{frames} 帧")

            before = frames
            await ws.send(json.dumps({"type": "nav", "action": "goto"}))
            await drain(8)
            step("导航指令生效", titles > 0 or frames > before,
                 f"新增 {frames - before} 帧")

            before = frames
            await ws.send(json.dumps({"type": "mouse", "action": "move",
                                      "x": 400, "y": 300, "button": "none",
                                      "buttons": 0}))
            await ws.send(json.dumps({"type": "wheel", "x": 400, "y": 300,
                                      "deltaX": 0, "deltaY": 300}))
            await drain(4)
            step("鼠标/滚轮被接受", True, f"滚动后新增 {frames - before} 帧")

            idle_before = frames
            await drain(3)
            step("静止时不推帧（实时流而非轮询）", frames - idle_before <= 1,
                 f"静置 3 秒新增 {frames - idle_before} 帧")
    except Exception as exc:  # noqa: BLE001
        step("主通道", False, repr(exc))

    # ---- 4. 登录通道 ----
    if args.login:
        print("\n  开始真实登录（卡验证时按提示操作）")
        rc = await cmd_login(args)
        step("登录流程", rc == 0)
    else:
        try:
            async with websockets.connect(ws_url(args, f"/api/login/{aid}")) as lw:
                await lw.send(json.dumps({"action": "abort"}))
                step("登录通道可连接", True, "（未真正登录，加 --login 才跑）")
        except Exception as exc:  # noqa: BLE001
            step("登录通道可连接", False, repr(exc))

    # ---- 5. 收尾 ----
    if args.stop:
        try:
            requests.post(f"{base}/api/browser/{aid}/stop", timeout=60)
            step("关闭浏览器", True)
        except Exception as exc:  # noqa: BLE001
            step("关闭浏览器", False, str(exc))

    bad = [n for n, ok, _ in results if not ok]
    print("─" * 56)
    print(f"  通过 {len(results) - len(bad)}/{len(results)}"
          + (f"　失败：{bad}" if bad else "　全部正常"))
    return 1 if bad else 0


EXAMPLES = """
════════ 1. 已有账号，登录一次 ════════

  import asyncio, json, websockets

  async def login(account_id, account=None, password=None):
      url = f"ws://127.0.0.1:8800/api/login/{account_id}"
      async with websockets.connect(url) as ws:
          await ws.send(json.dumps({
              "action": "login",
              "account": account, "password": password,   # 省略则用配置里的
              "persist": True,          # 登录成功后把凭据写回 config.yaml
          }))
          async for raw in ws:
              m = json.loads(raw)
              if m.get("type") != "login":
                  continue
              print(m["stage"], m.get("message", ""))
              if m["stage"] == "need_sms":
                  code = input("短信验证码: ")
                  await ws.send(json.dumps({"action": "sms_code", "code": code}))
              elif m["stage"] == "need_captcha":
                  print("去控制台页面把滑块拖完，流程会自己继续")
              elif m["stage"] in ("success", "failed", "aborted"):
                  return m["stage"] == "success"

  asyncio.run(login("vbk_569202"))

════════ 2. 新账号：建号 + 登录 + 首采近两年 ════════

  连接地址用 /api/login/new —— 账号还不存在，建完服务端会自动切过去。

  async def create_and_login():
      async with websockets.connect("ws://127.0.0.1:8800/api/login/new") as ws:
          await ws.send(json.dumps({
              "action": "create_login",
              "name": "九龙潭商户", "account": "vbk_888888",
              "password": "pw@123", "remark": "九龙潭",
              "scenic_id": "PFTSCA01018088",
              "persist": True,
              "collect_after_login": {"job": "day", "years": 2},
          }))
          async for raw in ws:
              m = json.loads(raw)
              if m.get("type") != "login":
                  continue
              print(m["stage"], m.get("message", ""))
              if m["stage"] == "account_created":
                  print("新账号标识:", m["account_id"])   # = 登录账号
              elif m["stage"] == "need_sms":
                  await ws.send(json.dumps({
                      "action": "sms_code", "code": input("短信验证码: ")}))
              elif m["stage"] == "collecting":
                  print("首采已启动:", m["label"])
              elif m["stage"] in ("success", "failed"):
                  break

════════ 3. 看采集进度 ════════

  登录通道在成功后就关了，采集的逐行日志走主通道：

  async def watch(account_id):
      async with websockets.connect(f"ws://127.0.0.1:8800/ws/{account_id}") as ws:
          async for raw in ws:
              m = json.loads(raw)
              if m["type"] == "task":
                  print(m["level"], m["message"])

  也可以查库：SELECT * FROM ads_service_ota_ctrip_run_log ORDER BY id DESC LIMIT 20;

════════ 4. 远程操作画面（拖滑块） ════════

  await ws.send(json.dumps({"type":"mouse","action":"down","x":95,"y":215,
                            "button":"left","buttons":1,"clickCount":1}))
  for x in range(95, 800, 30):
      await ws.send(json.dumps({"type":"mouse","action":"move","x":x,"y":215,
                                "button":"left","buttons":1}))
      await asyncio.sleep(0.05)
  await ws.send(json.dumps({"type":"mouse","action":"up","x":795,"y":215,
                            "button":"left","buttons":0,"clickCount":1}))

  帧从同一条连接下行：{"type":"frame","data":"<base64 jpeg>"}

════════ 5. 命令行等价操作 ════════

  python -m webconsole.ws_client login --account-id vbk_569202
  python -m webconsole.ws_client watch --account-id vbk_569202
  python -m webconsole.ws_client smoke --account-id vbk_569202
"""


def cmd_example(args) -> int:
    print(EXAMPLES)
    return 0


def cmd_spec(args) -> int:
    print(json.dumps(WS_SPEC, ensure_ascii=False, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m webconsole.ws_client",
        description="控制台 WebSocket API 命令行客户端",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("用法\n----\n")[-1])
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8800)
    p.add_argument("--tls", action="store_true", help="用 wss")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("spec", help="打印协议说明")
    s.set_defaults(func=cmd_spec, is_async=False)

    ex = sub.add_parser("example", help="打印可直接抄的调用样例")
    ex.set_defaults(func=cmd_example, is_async=False)

    lg = sub.add_parser("login", help="跑一次完整登录")
    lg.add_argument("--account-id", required=True)
    lg.add_argument("--account", default="")
    lg.add_argument("--password", default="")
    lg.add_argument("--timeout", type=int, default=600)
    lg.add_argument("--sms-wait", type=int, default=300)
    lg.set_defaults(func=cmd_login, is_async=True)

    ob = sub.add_parser("onboard", help="新商户上线：建号 + 登录 + 进定时采集（简化版）")
    ob.add_argument("--scenic-id", default="")
    ob.add_argument("--scenic-name", default="")
    ob.add_argument("--account", required=True)
    ob.add_argument("--password", required=True)
    ob.set_defaults(func=cmd_onboard, is_async=True)

    w = sub.add_parser("watch", help="观察主通道")
    w.add_argument("--account-id", required=True)
    w.add_argument("--frames", action="store_true", help="逐帧打印")
    w.set_defaults(func=cmd_watch, is_async=True)

    sd = sub.add_parser("send", help="发一条指令到主通道")
    sd.add_argument("--account-id", required=True)
    sd.add_argument("--json", required=True)
    sd.add_argument("--wait", type=float, default=3, help="发完等几秒收回执")
    sd.add_argument("--frames", action="store_true")
    sd.set_defaults(func=cmd_send, is_async=True)

    sm = sub.add_parser("smoke", help="标准测试流程：按真实顺序把每步走一遍")
    sm.add_argument("--account-id", required=True)
    sm.add_argument("--login", action="store_true", help="连真实登录一起跑")
    sm.add_argument("--account", default="")
    sm.add_argument("--password", default="")
    sm.add_argument("--timeout", type=int, default=600)
    sm.add_argument("--sms-wait", type=int, default=300)
    sm.add_argument("--stop", action="store_true", help="跑完关掉浏览器")
    sm.set_defaults(func=cmd_smoke, is_async=True)

    args = p.parse_args(argv)
    if not args.is_async:
        return args.func(args)
    try:
        return asyncio.run(args.func(args))
    except KeyboardInterrupt:
        print("\n已断开")
        return 0
    except OSError as exc:
        sys.exit(f"连不上 {args.host}:{args.port} —— 控制台起来了吗？（{exc}）")


if __name__ == "__main__":
    sys.exit(main())
