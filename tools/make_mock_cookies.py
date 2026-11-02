#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""为本地验证生成各账号的模拟登录态（Playwright storage_state 结构）。

仅用于对着 ``tools/mock_ctrip_server.py`` 跑通端到端流程；
生产环境请用 ``python -m ctrip.cli login`` 走真实登录。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ctrip.config import load_config  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", default="127.0.0.1", help="mock 服务域名/IP")
    parser.add_argument("--account-id", action="append", dest="account_ids")
    args = parser.parse_args()

    cfg = load_config(validate=False)
    account_ids = args.account_ids or [a.account_id for a in cfg.ctrip.accounts]
    expires = int(time.time()) + 86400

    for account_id in account_ids:
        state = {
            "cookies": [
                {"name": "mock_account", "value": account_id, "domain": args.domain,
                 "path": "/", "expires": expires, "httpOnly": False,
                 "secure": False, "sameSite": "Lax"},
                {"name": "cticket", "value": f"mock-ticket-{account_id}",
                 "domain": args.domain, "path": "/", "expires": expires,
                 "httpOnly": True, "secure": False, "sameSite": "Lax"},
            ],
            "origins": [],
        }
        cookie_file = cfg.cookie_file(account_id)
        cookie_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        meta = {
            "account_id": account_id,
            "login_at": datetime.now().isoformat(timespec="seconds"),
            "login_name": f"mock-{account_id}",
            "mock": True,
        }
        cfg.cookie_meta_file(account_id).write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"已生成模拟登录态：{cookie_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
