#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验证「采集中途登录态失效 → 自动重登 → 继续采集」这条路径。

真实登录要拉起浏览器并需要有效凭据，无法在自动化测试里跑通，
因此这里把 ``ctrip.pipeline.ensure_logged_in`` 替换为桩函数：
它不做浏览器操作，只标记「已重登」并返回成功。

配合 mock 服务的 ``MOCK_EXPIRE_AFTER``（第 N 次业务接口调用后返回 401，
但登录态校验接口始终正常），即可完整验证：

1. 采集过程中出现 401 会被识别为 ``SessionExpired``（而不是被采集器的
   容错逻辑降级成普通警告）；
2. 调度层捕获后触发自动重登；
3. 重登后重建会话、重验指纹，并重跑该任务；
4. 最终该任务状态为成功，数据完整落地。

运行::

    python tools/test_relogin.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

MOCK_PORT = 18099
MOCK_BASE = f"http://127.0.0.1:{MOCK_PORT}"

FAILURES: list[str] = []


def check(cond: bool, label: str) -> None:
    print(("  [OK]   " if cond else "  [FAIL] ") + label)
    if not cond:
        FAILURES.append(label)


def main() -> int:
    # 1) 启动带「第 4 次业务调用后失效」的 mock 服务
    env = dict(os.environ)
    env["MOCK_EXPIRE_AFTER"] = "4"
    mock = subprocess.Popen(
        [sys.executable, str(ROOT / "tools" / "mock_ctrip_server.py"),
         "--port", str(MOCK_PORT)],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        time.sleep(2.5)

        os.environ["CTRIP__CTRIP__API_BASE"] = f"{MOCK_BASE}/restapi/soa2/13807"
        os.environ["CTRIP__CTRIP__HOME_URL"] = f"{MOCK_BASE}/micro/tour-bi-vendor-new/"
        os.environ["CTRIP__COLLECT__REQUEST_INTERVAL"] = "0"

        from ctrip import pipeline as pipeline_mod
        from ctrip.accounts import resolve_accounts
        from ctrip.auth import LoginResult
        from ctrip.config import load_config

        cfg = load_config(None)
        accounts = resolve_accounts(cfg, ["A001"], log=pipeline_mod.logger)
        assert accounts, "测试需要配置中存在账号 A001"

        # 2) 用桩替换真实登录：只记录调用次数，不碰浏览器
        calls: list[str] = []

        def fake_ensure_logged_in(cfg_, account, log=None, *,
                                  force_relogin=False, headless=None,
                                  keep_alive=False):
            calls.append(f"force={force_relogin}")
            return LoginResult(account.account_id, True, "桩：模拟重登成功")

        pipeline_mod.ensure_logged_in = fake_ensure_logged_in

        print("=" * 70)
        print("验证：采集中途登录态失效 → 自动重登 → 继续采集")
        print("=" * 70)

        pipe = pipeline_mod.Pipeline(cfg, "csv", "overwrite")
        reports = pipe.run(accounts, ["kylin", "rating", "score"])

        report = reports[0]
        jobs = {j.job: j for j in report.jobs}

        # 3) 断言
        print("\n[1] 登录桩被调用情况")
        check(len(calls) >= 2,
              f"至少发生一次自动重登（登录调用 {len(calls)} 次：{calls}）")
        check(any(c == "force=True" for c in calls),
              "重登使用 force_relogin=True（强制重新登录而非复用旧态）")

        print("\n[2] 任务最终状态")
        for name in ("kylin", "rating", "score"):
            jr = jobs.get(name)
            check(jr is not None and jr.status in ("success", "partial"),
                  f"任务 {name} 最终成功（状态={jr.status if jr else '缺失'}）")

        print("\n[3] 重登痕迹写入告警，便于事后排查")
        relogin_notes = [w for j in report.jobs for w in j.warnings
                         if "自动重登" in w]
        check(bool(relogin_notes),
              f"报告中包含重登说明（{len(relogin_notes)} 条）")
        if relogin_notes:
            print(f"         示例：{relogin_notes[0][:70]}")

        print("\n[4] 数据完整落地")
        total = sum(j.rows_fetched for j in report.jobs)
        check(total > 100, f"重登后数据完整采集（共 {total} 行）")

        print("\n" + "=" * 70)
        if FAILURES:
            print(f"未通过 {len(FAILURES)} 项：")
            for f in FAILURES:
                print(f"  - {f}")
            return 1
        print("全部通过")
        return 0
    finally:
        mock.terminate()
        mock.wait(timeout=5)


if __name__ == "__main__":
    sys.exit(main())
