# -*- coding: utf-8 -*-
"""账号来源：支持配置文件、MySQL 账号表、账号管理 API 三种。

由 ``ctrip.account_source`` 选择：

* ``config``（默认）：读 YAML + ``.env``；
* ``mysql``：从账号表读（适合账号统一存库管理的场景）；
* ``api``：从账号管理服务拉取；
* ``auto``：依次尝试 api -> mysql -> config，取第一个拿到账号的源。


约定的 API 响应格式（与方案文档一致）::

    {"code": 0, "data": [{"account_id": "A001", "account": "u1", "password": "p1",
                          "name": "账号1", "status": "active",
                          "scenic_id": "PFTSCA01018088"}]}

``scenic_id`` 为景区 ID（账号级绑定，一个账号一个景区），也兼容 ``scenicId`` 写法。
若 API 未下发该字段，会保留本地配置中已有的景区绑定，不会被空值覆盖。

安全约束
--------
* 账号列表只在内存中流转，不写入 CSV，也不写入数据库的业务表；
  数据库仅保存**不含密码**的账号维表（``ads_service_ota_ctrip_account``）。
* ``account_id`` 会被用于拼接目录名和表名，因此加载后立即做白名单校验。
"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

from .config import AccountConfig, AppConfig, ConfigError
from .logging_utils import get_logger
from .schema import TABLES

logger = get_logger("accounts")


def fetch_from_api(cfg: AppConfig, log: logging.Logger | None = None) -> list[AccountConfig]:
    """从账号管理 API 拉取账号列表。未配置或失败时返回空列表。"""
    log = log or logger
    url = (cfg.ctrip.account_api_url or "").strip()
    if not url:
        return []

    headers = {"Accept": "application/json"}
    if cfg.ctrip.account_api_token:
        headers["Authorization"] = f"Bearer {cfg.ctrip.account_api_token}"
    try:
        resp = requests.get(url, headers=headers, timeout=cfg.ctrip.account_api_timeout)
        resp.raise_for_status()
        payload: Any = resp.json()
    except (requests.RequestException, ValueError) as exc:
        log.warning("账号管理 API 调用失败（将回落到本地配置）：%s", exc)
        return []

    items = _extract_items(payload)
    if not items:
        log.warning("账号管理 API 未返回账号数据（将回落到本地配置）")
        return []

    accounts: list[AccountConfig] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        account = AccountConfig(
            account_id=str(item.get("account_id") or item.get("accountId") or "").strip(),
            account=str(item.get("account") or item.get("login_name") or "").strip(),
            password=str(item.get("password") or ""),
            name=str(item.get("name") or "").strip(),
            status=str(item.get("status") or "active").strip(),
            remark=str(item.get("remark") or "").strip(),
            scenic_id=str(item.get("scenic_id") or item.get("scenicId") or "").strip(),
        )
        try:
            account.validate()
        except ConfigError as exc:
            log.warning("忽略非法账号记录：%s", exc)
            continue
        accounts.append(account)

    log.info("账号管理 API 返回 %d 个可用账号", len(accounts))
    return accounts


def fetch_from_mysql(cfg: AppConfig, log: logging.Logger | None = None) -> list[AccountConfig]:
    """从 MySQL 账号表读取账号（含密码）。

    密码列名由 ``ctrip.account_password_column`` 配置（默认 ``password``）。
    若表中不存在该列，不报错而是降级为仅读取账号元信息，密码
    仍从环境变量取——因为本工具自己写入的维表是刻意不存密码的，
    不能假定表里一定有密码列。
    """
    log = log or logger
    if not cfg.mysql.enabled:
        log.warning("account_source 涉及 MySQL 但 mysql.enabled=false，跳过数据库账号源")
        return []

    try:
        import pymysql  # noqa: PLC0415
    except ImportError:
        log.warning("未安装 pymysql，无法从数据库读取账号")
        return []

    table = TABLES["account_dim"].name
    pwd_col = (cfg.ctrip.account_password_column or "").strip()

    try:
        conn = pymysql.connect(
            host=cfg.mysql.host, port=cfg.mysql.port, user=cfg.mysql.user,
            password=cfg.mysql.password, database=cfg.mysql.database,
            charset=cfg.mysql.charset, connect_timeout=cfg.mysql.connect_timeout,
            cursorclass=pymysql.cursors.DictCursor,
        )
    except Exception as exc:  # noqa: BLE001 - 连不上库时要回落，不能直接崩
        log.warning("连接数据库读取账号失败（将回落其他源）：%s", exc)
        return []

    try:
        with conn.cursor() as cur:
            # 先探测实际列，避免 SELECT 不存在的密码列直接报错
            cur.execute(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
                (cfg.mysql.database, table),
            )
            columns = {row["COLUMN_NAME"] for row in cur.fetchall()}
            if not columns:
                log.warning("账号表 %s.%s 不存在，请先执行 init-db",
                            cfg.mysql.database, table)
                return []

            has_pwd = bool(pwd_col) and pwd_col in columns
            if pwd_col and not has_pwd:
                log.warning(
                    "账号表 %s 中无密码列 %r，将仅从数据库读取账号元信息，"
                    "密码仍从 .env / 环境变量读取", table, pwd_col,
                )

            select_cols = ["account_id", "scenic_id", "account_name",
                           "login_name", "status", "remark"]
            select_cols = [c for c in select_cols if c in columns]
            if has_pwd:
                select_cols.append(pwd_col)
            # 列名来自 information_schema 实测结果，不是用户输入，拼接安全
            cur.execute(
                f"SELECT {', '.join('`%s`' % c for c in select_cols)} FROM `{table}`"
            )
            rows = cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        log.warning("从数据库读取账号失败（将回落其他源）：%s", exc)
        return []
    finally:
        conn.close()

    accounts: list[AccountConfig] = []
    for row in rows:
        account_id = str(row.get("account_id") or "").strip()
        password = str(row.get(pwd_col) or "") if has_pwd else ""
        if not password:
            # 库里没密码时回落到环境变量 CTRIP_<ID>_PASSWORD
            password = os.environ.get(f"CTRIP_{account_id.upper()}_PASSWORD", "")
        account = AccountConfig(
            account_id=account_id,
            account=str(row.get("login_name") or "").strip(),
            password=password,
            name=str(row.get("account_name") or "").strip(),
            status=str(row.get("status") or "active").strip(),
            remark=str(row.get("remark") or "").strip(),
            scenic_id=str(row.get("scenic_id") or "").strip(),
        )
        try:
            # 只校验已启用账号的凭据完整性；停用账号允许不填
            account.validate(require_credentials=account.is_active)
        except ConfigError as exc:
            log.warning("忽略数据库中的非法账号记录：%s", str(exc).splitlines()[0])
            continue
        accounts.append(account)

    log.info("从数据库账号表 %s 读到 %d 个账号%s",
             table, len(accounts), "（含密码）" if has_pwd else "（不含密码）")
    return accounts


def _extract_items(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "accounts", "list", "result", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict) and isinstance(value.get("list"), list):
                return value["list"]
    return []


def _merge_into_config(cfg: AppConfig, external: list[AccountConfig]) -> None:
    """外部源账号合并回配置：同 ID 以外部为准，本地独有的账号保留。

    合并时对两个字段做了「空值不覆盖」保护：scenic_id 与 password。
    否则当外部源（如不存密码的账号表）未提供密码时，
    会把 .env 里已配置好的密码洗成空值。
    """
    merged = {a.account_id: a for a in cfg.ctrip.accounts}
    for item in external:
        local = merged.get(item.account_id)
        if local:
            if not item.scenic_id and local.scenic_id:
                item.scenic_id = local.scenic_id
            if not item.password and local.password:
                item.password = local.password
            if not item.account and local.account:
                item.account = local.account
        merged[item.account_id] = item
    cfg.ctrip.accounts = list(merged.values())


def load_accounts(cfg: AppConfig, log: logging.Logger | None = None) -> str:
    """根据 ``ctrip.account_source`` 加载账号，返回实际生效的源名。"""
    log = log or logger
    source = (cfg.ctrip.account_source or "config").strip().lower()

    if source == "config":
        log.info("账号源：配置文件 / .env（共 %d 个）", len(cfg.ctrip.accounts))
        return "config"

    if source == "api":
        accounts = fetch_from_api(cfg, log)
        if accounts:
            _merge_into_config(cfg, accounts)
            return "api"
        log.warning("账号 API 未返回数据，回落到配置文件")
        return "config"

    if source == "mysql":
        accounts = fetch_from_mysql(cfg, log)
        if accounts:
            _merge_into_config(cfg, accounts)
            return "mysql"
        log.warning("数据库账号表未返回数据，回落到配置文件")
        return "config"

    # auto：api -> mysql -> config
    for name, fetcher in (("api", fetch_from_api), ("mysql", fetch_from_mysql)):
        accounts = fetcher(cfg, log)
        if accounts:
            _merge_into_config(cfg, accounts)
            log.info("账号源（auto）命中：%s", name)
            return name
    log.info("账号源（auto）回落到配置文件（共 %d 个）", len(cfg.ctrip.accounts))
    return "config"


def resolve_accounts(cfg: AppConfig, account_ids: list[str] | None = None,
                     *, include_disabled: bool = False,
                     log: logging.Logger | None = None) -> list[AccountConfig]:
    """得到本次要处理的账号列表。

    先按 ``account_source`` 加载账号（可能来自数据库或 API），
    再按 ID 筛选，最后对选中账号做一次严格的凭据校验——
    因为这一刻无论账号从哪里来，都必须已经拿到用户名与密码。
    """
    log = log or logger
    load_accounts(cfg, log)

    selected = cfg.select_accounts(account_ids, include_disabled=include_disabled)
    for account in selected:
        if account.is_active:
            account.validate(require_credentials=True)
    log.info("本次目标账号：%s", ", ".join(a.account_id for a in selected))
    return selected


def to_dim_rows(accounts: list[AccountConfig]) -> list[dict[str, Any]]:
    """转成账号维表行（**不含密码**）。

    注意：这里不输出密码列，而 UPSERT 只更新它列出的列，
    因此即使你在账号表里自行加了 password 列，同步维表也不会把它洗掉。
    """
    return [
        {
            "account_id": a.account_id,
            "scenic_id": a.scenic_id or None,
            "account_name": a.name or a.account_id,
            "login_name": a.account,
            "status": a.status,
            "remark": a.remark,
        }
        for a in accounts
    ]
