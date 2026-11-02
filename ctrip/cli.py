# -*- coding: utf-8 -*-
"""命令行入口。

常用命令::

    # 登录（首次或 Cookie 失效时；服务器无图形界面用默认 headless）
    python -m ctrip.cli login                      # 全部启用账号
    python -m ctrip.cli login --account-id A001 --no-headless   # 需人工过验证码

    # 检查登录态
    python -m ctrip.cli check

    # 采集（默认写 CSV）
    python -m ctrip.cli collect --job all
    python -m ctrip.cli collect --job kylin --days 7 --account-id A001
    python -m ctrip.cli collect --job score --month 2026-07

    # 写入 MySQL（需先配置 mysql.enabled=true）
    python -m ctrip.cli init-db
    python -m ctrip.cli collect --job all --sink both

    # 表结构维护
    python -m ctrip.cli show-ddl --write           # 由 schema.py 生成 sql/ddl.sql
    python -m ctrip.cli show-schema                # 查看字段与上游映射
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

from .accounts import resolve_accounts, to_dim_rows
from .auth import CookieStore, ensure_logged_in, login, verify_cookie
from .collectors import ALL_JOBS
from .collectors.kylin_daily import ALL_PERIODS
from .config import ConfigError, load_config
from .daterange import (parse_date, parse_month, resolve_date_range,
                        resolve_month_range)
from .logging_utils import account_logger, get_logger, setup_root
from .pipeline import Pipeline, exit_code, render_reports
from .schema import TABLES, all_ddl
from .timeutil import get_timezone

logger = get_logger("cli")


# =============================================================================
# 参数
# =============================================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ctrip-collector",
        description="携程商家后台多账号自动登录与数据采集（默认落 CSV，可切换 MySQL）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-c", "--config", help="配置文件路径（默认 config/config.yaml）")
    parser.add_argument("--log-level", help="日志级别（DEBUG/INFO/WARNING/ERROR）")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_account_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--account-id", action="append", dest="account_ids",
                       help="指定账号 ID，可重复传入；不传则处理全部启用账号")
        p.add_argument("--account-ids", dest="account_ids_csv",
                       help="逗号分隔的账号 ID 列表")
        p.add_argument("--include-disabled", action="store_true",
                       help="同时处理 status=disabled 的账号")

    # login
    p_login = sub.add_parser("login", help="登录并保存各账号独立的登录态")
    add_account_args(p_login)
    p_login.add_argument("--force", action="store_true", help="忽略现有登录态强制重新登录")
    p_login.add_argument("--keep-alive", action="store_true",
                         help="登录后让浏览器常驻后台（需配置 ctrip.cdp_port），"
                              "后续运行直接复用已打开的浏览器")
    headless_group = p_login.add_mutually_exclusive_group()
    headless_group.add_argument("--headless", dest="headless", action="store_true",
                                default=None, help="无头模式（默认，按配置）")
    headless_group.add_argument("--no-headless", dest="headless", action="store_false",
                                help="显示浏览器窗口，用于人工处理验证码")

    # check
    p_check = sub.add_parser("check", help="校验各账号登录态是否有效")
    add_account_args(p_check)

    # collect
    p_collect = sub.add_parser("collect", help="采集数据并落地")
    add_account_args(p_collect)
    p_collect.add_argument("--job", action="append", dest="jobs",
                           choices=list(ALL_JOBS) + ["all"],
                           help=f"任务名，可重复；可选 {', '.join(ALL_JOBS)} 或 all（默认 all）")
    p_collect.add_argument("--sink", choices=["csv", "mysql", "both"],
                           help="落地方式，默认取配置 run.sink（csv）")
    # 日粒度（交易卡片 kylin）
    p_collect.add_argument("--start-date",
                           help="交易卡片开始日期，如 2025-01-01；"
                                "指定后自动按 --batch-days（默认 30 天）分批采集")
    p_collect.add_argument("--end-date",
                           help="交易卡片截止日期（默认昨天，即 T+1 口径）")
    p_collect.add_argument("--days", type=int,
                           help="交易卡片回溯天数（与 --start-date 二选一，默认取配置 30）")
    p_collect.add_argument("--batch-days", type=int,
                           help="日粒度单批天数（默认 30），长区间会自动切成多批")
    p_collect.add_argument("--period", action="append", dest="periods",
                           choices=["1d", "7d", "30d", "all"],
                           help="交易卡片周期口径，可重复：1d 逐日明细（受区间参数影响）；"
                                "7d/30d 仅采当前窗口各 1 行。"
                                "默认只采 1d，传 all 则三种全采")
    # 月粒度（目的地评级 rating / 供应商评级 score）
    p_collect.add_argument("--start-month",
                           help="月度任务起始月，如 2025-01；默认到 --end-month（不填则到上个月）")
    p_collect.add_argument("--end-month", help="月度任务结束月（默认上个月）")
    p_collect.add_argument("--month", help="只采单个月份 YYYY-MM（默认上个月）")
    p_collect.add_argument("--months", help="指定多个非连续月份，逗号分隔，如 2025-03,2025-07")
    p_collect.add_argument("--per-region", action="store_true",
                           help="目的地评级分析逐目的地采集（默认仅账号汇总）")
    p_collect.add_argument("--write-mode", choices=["overwrite", "upsert"],
                           help="overwrite（默认）同日期/月份旧数据先清理再写入；"
                                "upsert 仅按唯一键更新不删除")
    p_collect.add_argument("--workers", type=int, help="账号级并发数（默认 1，串行）")
    p_collect.add_argument("--scenic-id",
                           help="临时指定景区ID（如 PFTSCA01018088），覆盖配置；"
                                "仅在只处理单个账号时允许，避免多账号误标同一景区")
    p_collect.add_argument("--skip-login", action="store_true",
                           help="跳过登录态检查（假定 Cookie 可用，用于调试）")
    p_collect.add_argument("--force-relogin", action="store_true", help="强制重新登录")

    # init-db
    p_init = sub.add_parser("init-db", help="按 schema.py 创建数据库与表")
    add_account_args(p_init)
    p_init.add_argument("--drop-create", action="store_true",
                        help="先 DROP 再建表（危险，仅开发环境）")
    p_init.add_argument("--no-create-database", action="store_true",
                        help="不执行 CREATE DATABASE")
    p_init.add_argument("--seed-accounts", action="store_true",
                        help="同时写入账号维表（不含密码）")

    # show-ddl
    p_ddl = sub.add_parser("show-ddl", help="输出建表语句（表结构单一来源：ctrip/schema.py）")
    add_account_args(p_ddl)
    p_ddl.add_argument("--write", action="store_true", help="同时写入 sql/ddl.sql")

    # show-schema
    sub.add_parser("show-schema", help="查看各表字段、注释与上游字段映射")

    # show-config
    sub.add_parser("show-config", help="查看生效配置（密码脱敏）")

    # doctor
    sub.add_parser("doctor", help="环境体检：.env、账号凭据、MySQL、浏览器")

    # browser
    p_browser = sub.add_parser("browser", help="管理持久化浏览器（查看状态 / 启动常驻 / 停止 / 重置）")
    add_account_args(p_browser)
    browser_action = p_browser.add_mutually_exclusive_group()
    browser_action.add_argument("--status", action="store_true",
                                help="查看各账号 profile 与常驻浏览器状态（默认）")
    browser_action.add_argument("--start", action="store_true",
                                help="启动常驻浏览器并保持登录挂在后台")
    browser_action.add_argument("--stop", action="store_true",
                                help="停止常驻浏览器（profile 保留，下次仍复用同一设备指纹）")
    browser_action.add_argument("--reset", action="store_true",
                                help="删除 profile 重建（仅在 profile 损坏时使用，下次登录可能重新触发验证）")
    browser_action.add_argument("--clear-locks", action="store_true",
                                help="清理进程被强杀后残留的浏览器锁文件")
    p_browser.add_argument("--no-headless", dest="headless", action="store_false",
                           default=None, help="常驻浏览器显示窗口（便于人工过验证码）")

    # run：一体化入口
    p_run = sub.add_parser(
        "run",
        help="一键全流程：体检 → 建表 → 登录 → 采集 → 写入",
        description=(
            "一个命令跑完整条链路，适合日常定时任务与首次建仓：\n"
            "  1) 环境体检（凭据、浏览器、时区、MySQL 连通性）\n"
            "  2) 自动建表（表不存在时，--sink 含 mysql 时生效）\n"
            "  3) 逐账号确保登录态（持久化浏览器，失效自动重登）\n"
            "  4) 按指定区间与批次采集三类数据\n"
            "  5) 写入 CSV / MySQL 并输出汇总报告"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_account_args(p_run)
    p_run.add_argument("--job", action="append", dest="jobs",
                       choices=list(ALL_JOBS) + ["all"],
                       help=f"任务名，可重复；可选 {', '.join(ALL_JOBS)} 或 all（默认 all）")
    p_run.add_argument("--sink", choices=["csv", "mysql", "both"],
                       help="落地方式，默认取配置 run.sink")
    p_run.add_argument("--start-date",
                       help="交易卡片开始日期，如 2025-01-01；自动按 --batch-days 分批")
    p_run.add_argument("--end-date", help="交易卡片截止日期（默认昨天）")
    p_run.add_argument("--days", type=int, help="交易卡片回溯天数（与 --start-date 二选一）")
    p_run.add_argument("--batch-days", type=int, help="日粒度单批天数（默认 30）")
    p_run.add_argument("--period", action="append", dest="periods",
                       choices=["1d", "7d", "30d", "all"],
                       help="交易卡片周期口径，可重复（1d/7d/30d，默认只采 1d）")
    p_run.add_argument("--start-month", help="月度任务起始月，如 2025-01")
    p_run.add_argument("--end-month", help="月度任务结束月（默认上个月）")
    p_run.add_argument("--month", help="只采单个月份 YYYY-MM")
    p_run.add_argument("--months", help="指定多个非连续月份，逗号分隔")
    p_run.add_argument("--per-region", action="store_true",
                       help="目的地评级分析逐目的地采集")
    p_run.add_argument("--write-mode", choices=["overwrite", "upsert"],
                       help="overwrite（默认）同日期/月份旧数据先清理再写入")
    p_run.add_argument("--workers", type=int, help="账号级并发数（默认 1，串行）")
    p_run.add_argument("--scenic-id", help="临时指定景区ID（仅单账号时允许）")
    p_run.add_argument("--force-relogin", action="store_true", help="强制重新登录")
    p_run.add_argument("--keep-alive", action="store_true",
                       help="采集结束后让浏览器继续挂在后台（需配置 ctrip.cdp_port）")
    p_run.add_argument("--no-headless", dest="headless", action="store_false",
                       default=None, help="显示浏览器窗口（首次登录或验证码时用）")
    p_run.add_argument("--no-init-db", action="store_true",
                       help="跳过自动建表（默认会在表不存在时建表）")
    p_run.add_argument("--no-check", action="store_true", help="跳过前置环境体检")

    return parser


def _account_ids(args: argparse.Namespace) -> list[str] | None:
    ids: list[str] = list(getattr(args, "account_ids", None) or [])
    csv_value = getattr(args, "account_ids_csv", None)
    if csv_value:
        ids.extend(x.strip() for x in csv_value.split(",") if x.strip())
    return ids or None


def _jobs(args: argparse.Namespace) -> list[str]:
    jobs = getattr(args, "jobs", None) or ["all"]
    if "all" in jobs:
        return list(ALL_JOBS)
    seen: list[str] = []
    for job in jobs:
        if job not in seen:
            seen.append(job)
    return seen


# =============================================================================
# 子命令实现
# =============================================================================

def cmd_login(cfg, args) -> int:
    accounts = resolve_accounts(cfg, _account_ids(args),
                               include_disabled=args.include_disabled, log=logger)
    failed = 0
    for account in accounts:
        log = account_logger(account.account_id, cfg.log_dir(account.account_id),
                             cfg.run.log_level)
        keep_alive = bool(getattr(args, "keep_alive", False))
        if args.force:
            result = login(cfg, account, log, headless=args.headless, keep_alive=keep_alive)
        else:
            result = ensure_logged_in(cfg, account, log, headless=args.headless,
                                      keep_alive=keep_alive)
        flag = "成功" if result.success else "失败"
        logger.info("[%s] 登录%s：%s", account.account_id, flag, result.message)
        if not result.success:
            failed += 1
    logger.info("登录完成：成功 %d，失败 %d", len(accounts) - failed, failed)
    return 0 if failed == 0 else (1 if failed < len(accounts) else 2)


def cmd_check(cfg, args) -> int:
    accounts = resolve_accounts(cfg, _account_ids(args),
                               include_disabled=args.include_disabled, log=logger)
    store = CookieStore(cfg)
    invalid = 0
    print(f"{'账号':<10}{'景区ID':<18}{'登录态':<10}{'最近登录':<22}说明")
    print("-" * 104)
    for account in accounts:
        log = account_logger(account.account_id, cfg.log_dir(account.account_id),
                             cfg.run.log_level)
        meta = store.load_meta(account.account_id)
        ok, detail = verify_cookie(cfg, account.account_id, log)
        if not ok:
            invalid += 1
        print(f"{account.account_id:<10}{(account.scenic_id or '-'):<18}"
              f"{'有效' if ok else '无效':<10}"
              f"{str(meta.get('login_at', '-')):<22}{detail}")
    return 0 if invalid == 0 else (1 if invalid < len(accounts) else 2)


def cmd_collect(cfg, args) -> int:
    return _collect_core(cfg, args)


def _collect_core(cfg, args, *, keep_alive: bool = False) -> int:
    """采集主流程。``collect`` 与 ``run`` 共用这一份实现。"""
    if args.workers:
        cfg.run.workers = args.workers
    sink_mode = args.sink or cfg.run.sink
    if sink_mode in ("mysql", "both") and not cfg.mysql.enabled:
        raise ConfigError(
            f"--sink {sink_mode} 需要先在配置中启用 MySQL（mysql.enabled: true）。"
            "默认的 CSV 落地无需任何数据库配置。"
        )

    accounts = resolve_accounts(cfg, _account_ids(args),
                               include_disabled=args.include_disabled, log=logger)
    jobs = _jobs(args)

    # --scenic-id 临时覆盖：景区绑定是账号级一对一关系，
    # 若允许一次覆盖多个账号，就会把不同账号的数据错标成同一个景区。
    if args.scenic_id:
        if len(accounts) != 1:
            raise ConfigError(
                f"--scenic-id 仅允许在处理单个账号时使用（当前匹配到 {len(accounts)} 个账号）；"
                "请配合 --account-id 指定单个账号，或直接在 config/config.yaml 中配置 scenic_id"
            )
        accounts[0].scenic_id = args.scenic_id.strip()
        accounts[0].validate()
        logger.info("[%s] 景区ID 已由命令行临时覆盖为 %s",
                    accounts[0].account_id, accounts[0].scenic_id)

    # 日期与月份参数统一交由 ctrip.daterange 解析，兼容 20250101 / 2025-01-01 / 2025/01/01
    start_date = parse_date(args.start_date, field="--start-date") if args.start_date else None
    end_date = parse_date(args.end_date, field="--end-date") if args.end_date else None
    start_month = parse_month(args.start_month, field="--start-month") if args.start_month else None
    end_month = parse_month(args.end_month, field="--end-month") if args.end_month else None
    month = parse_month(args.month, field="--month") if args.month else None
    months = None
    if args.months:
        months = [parse_month(m.strip(), field="--months")
                  for m in args.months.split(",") if m.strip()]

    # 周期口径：默认（不传）走配置，而配置默认只有 1d。
    # 传 all 时显式展开为三种全采，而不是回退成配置值，
    # 否则用户写了 --period all 却只采到 1d，与字面含义相反。
    periods = getattr(args, "periods", None)
    if periods:
        periods = (ALL_PERIODS if "all" in periods
                   else list(dict.fromkeys(periods)))

    # 提前校验区间参数：区间写错时应立即报错，
    # 而不是先去登录、每个账号都跑一遍才在采集阶段报同一个错
    today = datetime.now(get_timezone(cfg.run.timezone, log=logger)).date()
    if "kylin" in jobs:
        resolve_date_range(today=today, start_date=start_date, end_date=end_date,
                           days=args.days, default_days=cfg.collect.kylin_days)
    if {"rating", "score"} & set(jobs):
        resolve_month_range(today=today, start_month=start_month,
                            end_month=end_month, month=month, months=months)

    write_mode = (args.write_mode or cfg.run.write_mode).lower()
    pipeline = Pipeline(cfg, sink_mode, write_mode)
    reports = pipeline.run(
        accounts, jobs,
        force_relogin=args.force_relogin,
        headless=getattr(args, "headless", None),
        skip_login=getattr(args, "skip_login", False),
        keep_alive=keep_alive,
        days=args.days,
        start_date=start_date,
        end_date=end_date,
        batch_days=args.batch_days,
        periods=periods,
        month=month,
        months=months,
        start_month=start_month,
        end_month=end_month,
        per_region=args.per_region or None,
    )
    print(render_reports(reports, sink_mode))
    return exit_code(reports)


def cmd_init_db(cfg, args) -> int:
    from .sinks import MySQLSink

    if not cfg.mysql.enabled:
        raise ConfigError(
            "mysql.enabled=false。请先在 config/config.yaml 配置数据库连接并置为 true，"
            "或设置环境变量 CTRIP__MYSQL__ENABLED=true"
        )
    account_ids = None
    if cfg.mysql.table_strategy == "per_account":
        accounts = resolve_accounts(cfg, _account_ids(args), include_disabled=True, log=logger)
        account_ids = [a.account_id for a in accounts]

    with MySQLSink(cfg, logger) as sink:
        tables = sink.init_database(
            create_database=not args.no_create_database,
            account_ids=account_ids,
            drop_create=args.drop_create,
        )
        version = sink.ping()
        print(f"MySQL {version} @ {cfg.mysql.dsn_safe()}")
        print(f"已初始化 {len(tables)} 张表（策略：{cfg.mysql.table_strategy}）：")
        for name in tables:
            print(f"  - {name}")
        if args.seed_accounts:
            accounts = resolve_accounts(cfg, _account_ids(args), include_disabled=True, log=logger)
            count = sink.upsert_accounts(to_dim_rows(accounts))
            print(f"已写入账号维表 {count} 条（不含密码）")
    return 0


def cmd_show_ddl(cfg, args) -> int:
    account_ids = None
    if cfg.mysql.table_strategy == "per_account":
        try:
            accounts = resolve_accounts(cfg, _account_ids(args), include_disabled=True, log=logger)
            account_ids = [a.account_id for a in accounts]
        except ConfigError:
            account_ids = None
    ddl = all_ddl(cfg.mysql.table_prefix, account_ids, cfg.mysql.table_strategy)
    print(ddl)
    if args.write:
        path = cfg.path("sql/ddl.sql")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(ddl, encoding="utf-8")
        print(f"-- 已写入 {path}", file=sys.stderr)
    return 0


def cmd_show_schema(cfg, args) -> int:
    for key, table in TABLES.items():
        print("=" * 92)
        print(f"[{key}] 表 {table.table_name(cfg.mysql.table_prefix)} —— {table.comment}")
        print(f"唯一键：({', '.join(table.unique_key)})")
        print("-" * 92)
        print(f"{'列名':<26}{'类型':<18}{'上游字段':<42}说明")
        for f in table.fields:
            print(f"{f.name:<26}{f.type:<18}{(f.source or '-'):<42}{f.comment}")
        print()
    print("提示：以上内容由 ctrip/schema.py 生成；改字段只改该文件，"
          "CSV 表头、建表 DDL、UPSERT 语句会自动同步。")
    return 0


def cmd_show_config(cfg, args) -> int:
    print(f"配置文件：{cfg.config_path}")
    from .timeutil import describe_timezone
    print(f"时区：{describe_timezone(cfg.run.timezone)}")
    print(f"落地方式：{cfg.run.sink}   并发：{cfg.run.workers}")
    print(f"CSV 输出目录：{cfg.path(cfg.run.output_dir)}（按 account_id 分子目录）")
    print(f"登录态目录：{cfg.path(cfg.ctrip.cookie_dir)}（按 account_id 分子目录）")
    print(f"MySQL：enabled={cfg.mysql.enabled} {cfg.mysql.dsn_safe()} "
          f"策略={cfg.mysql.table_strategy} 前缀='{cfg.mysql.table_prefix}'")
    print(f"账号源：{cfg.ctrip.account_source}"
          + (f"（密码列：{cfg.ctrip.account_password_column}）"
             if cfg.ctrip.account_source in ("mysql", "auto") else ""))
    print(f"账号管理 API：{cfg.ctrip.account_api_url or '（未配置）'}")
    print(f"账号数量：{len(cfg.ctrip.accounts)}")
    print(f"{'account_id':<12}{'状态':<10}{'登录名':<20}"
          f"{'密码':<8}{'景区ID(scenic_id)':<22}备注")
    for a in cfg.ctrip.accounts:
        print(f"{a.account_id:<12}{a.status:<10}{(a.account or '-'):<20}"
              f"{'已配置' if a.password else '缺失':<8}"
              f"{(a.scenic_id or '未绑定'):<22}{a.name or a.remark}")
    unbound = [a.account_id for a in cfg.ctrip.accounts if a.is_active and not a.scenic_id]
    if unbound:
        print(f"\n注意：以下启用账号尚未绑定 scenic_id，采集出的景区ID 将为空："
              f"{', '.join(unbound)}")
    return 0


def cmd_doctor(cfg, args) -> int:
    """环境体检：把「为何拿不到账号密码」这类问题一次性查清楚。

    之前的报错只说「缺少登录用户名」，在 Windows 上特别容易让人迷惑：
    实际原因往往是 .env 根本没被加载。这个命令直接把每一环的真实状态列出来。
    """
    import os
    import platform

    from .config import DEFAULT_ENV_PATH, PROJECT_ROOT, load_dotenv
    from .timeutil import describe_timezone

    print("=" * 78)
    print("环境体检（doctor）")
    print("=" * 78)
    print(f"操作系统：{platform.system()} {platform.release()}")
    print(f"Python：{platform.python_version()}")
    print(f"时区：{describe_timezone(cfg.run.timezone)}")
    print(f"项目根目录：{PROJECT_ROOT}")
    print(f"当前工作目录：{Path.cwd()}")
    print()

    problems: list[str] = []

    # 1) .env
    print("-" * 78)
    print("[1/6] .env 文件")
    if DEFAULT_ENV_PATH.exists():
        injected = load_dotenv(DEFAULT_ENV_PATH)
        print(f"  位置：{DEFAULT_ENV_PATH}  ✓ 已找到并自动加载")
        raw = DEFAULT_ENV_PATH.read_bytes()
        if raw.startswith(b"\xef\xbb\xbf"):
            print("  编码：UTF-8 with BOM（已自动处理，不影响使用）")
        if b"\r\n" in raw:
            print("  换行：CRLF（Windows，已自动处理）")
        keys = [k for k in os.environ if k.startswith(("CTRIP_", "MYSQL_"))]
        print(f"  本次新注入 {len(injected)} 项，当前相关环境变量共 {len(keys)} 项")
    else:
        print(f"  位置：{DEFAULT_ENV_PATH}  ✗ 不存在")
        problems.append(
            "未找到 .env。Windows: Copy-Item .env.example .env；"
            "Linux/macOS: cp .env.example .env"
        )
    print()

    # 2) 配置文件
    print("-" * 78)
    print("[2/6] 配置文件")
    print(f"  实际使用：{cfg.config_path}")
    if cfg.config_path and cfg.config_path.name == "config.example.yaml":
        print("  注意：当前用的是示例配置（config.yaml 不存在）")
        problems.append(
            "建议创建自己的配置：Windows: "
            "Copy-Item config\\config.example.yaml config\\config.yaml"
        )
    print(f"  账号源：{cfg.ctrip.account_source}")
    print()

    # 3) 账号凭据
    print("-" * 78)
    print("[3/6] 账号凭据")
    if not cfg.ctrip.accounts:
        print("  ✗ 配置中没有任何账号")
        problems.append("在 config.yaml 的 ctrip.accounts 下配置至少一个账号")
    else:
        print(f"  {'account_id':<12}{'状态':<10}{'用户名':<18}"
              f"{'密码':<8}{'景区ID':<20}")
        for a in cfg.ctrip.accounts:
            mark = "✓" if (a.account and a.password) else "✗"
            print(f"  {a.account_id:<12}{a.status:<10}{(a.account or '-'):<18}"
                  f"{('已配置' if a.password else '缺失'):<8}"
                  f"{(a.scenic_id or '未绑定'):<20}{mark}")
            if a.is_active and not (a.account and a.password):
                env_a = f"CTRIP_{a.account_id.upper()}_ACCOUNT"
                env_p = f"CTRIP_{a.account_id.upper()}_PASSWORD"
                problems.append(
                    f"账号 {a.account_id} 凭据不完整：请在 .env 中填写 "
                    f"{env_a} 与 {env_p}"
                )
    print()

    # 4) MySQL
    print("-" * 78)
    print("[4/6] MySQL 连接")
    if not cfg.mysql.enabled:
        print("  enabled=false（仅写 CSV，无需数据库）")
    else:
        print(f"  {cfg.mysql.dsn_safe()}")
        try:
            import pymysql  # noqa: PLC0415
            conn = pymysql.connect(
                host=cfg.mysql.host, port=cfg.mysql.port, user=cfg.mysql.user,
                password=cfg.mysql.password, database=cfg.mysql.database,
                charset=cfg.mysql.charset, connect_timeout=cfg.mysql.connect_timeout,
            )
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION()")
                version = cur.fetchone()[0]
                cur.execute(
                    "SELECT TABLE_NAME FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA=%s", (cfg.mysql.database,))
                existing = {r[0] for r in cur.fetchall()}
            conn.close()
            print(f"  ✓ 连接成功，MySQL {version}")
            expected = {t.name for t in TABLES.values()}
            missing = sorted(expected - existing)
            if missing:
                print(f"  ✗ 缺少 {len(missing)} 张表：{', '.join(missing)}")
                problems.append("执行 python -m ctrip.cli init-db 创建缺失的表")
            else:
                print(f"  ✓ {len(expected)} 张表均已存在")
        except Exception as exc:  # noqa: BLE001
            print(f"  ✗ 连接失败：{exc}")
            problems.append("检查 .env 中的 MYSQL_HOST / MYSQL_USER / "
                            "MYSQL_PASSWORD / MYSQL_DATABASE")
    print()

    # 5) Playwright 浏览器
    print("-" * 78)
    print("[5/6] Playwright 浏览器")
    # 直接查本地缓存目录，不启动 Playwright 进程——
    # 体检只需知道浏览器装了没有，没必要为此拉起子进程并产生收尾警告
    try:
        import playwright  # noqa: PLC0415,F401
    except ImportError:
        print("  ✗ 未安装 playwright 包")
        problems.append("执行 uv pip install -e . 安装依赖")
    else:
        cache_root = Path(
            os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
            or (Path.home() / "AppData/Local/ms-playwright"
                if platform.system() == "Windows"
                else Path.home() / ".cache/ms-playwright")
        )
        # 优先找完整版 chromium-*；chromium_headless_shell-* 不能用于 --no-headless 人工过验证码
        full = sorted(cache_root.glob("chromium-*")) if cache_root.exists() else []
        shell_only = sorted(cache_root.glob("chromium_headless_shell-*")) if cache_root.exists() else []
        if full:
            print(f"  ✓ Chromium 已安装：{full[-1]}")
        elif shell_only:
            print(f"  ⚠ 仅找到无头精简版：{shell_only[-1]}")
            problems.append(
                "无头精简版无法用于 --no-headless 人工过验证码，"
                "建议执行 playwright install chromium 安装完整版"
            )
        else:
            print(f"  ✗ 未在 {cache_root} 找到 Chromium")
            problems.append("执行 playwright install chromium")
    print()

    # 6) 时区数据库（Windows 常见坑）
    print("-" * 78)
    print("[6/6] 时区数据库")
    try:
        from zoneinfo import ZoneInfo  # noqa: PLC0415
        ZoneInfo(cfg.run.timezone)
        print(f"  ✓ 可解析 {cfg.run.timezone}")
    except Exception as exc:  # noqa: BLE001
        print(f"  ⚠ 无法解析 {cfg.run.timezone}：{exc}")
        print("    程序会自动降级为固定偏移 UTC+08:00，采集不受影响，但建议补装")
        problems.append("执行 uv pip install tzdata（Windows 不自带系统时区数据库）")
    print()

    print("=" * 78)
    if problems:
        print(f"发现 {len(problems)} 个需处理的问题：")
        for i, item in enumerate(problems, 1):
            print(f"  {i}. {item}")
    else:
        print("体检通过，可以执行：python -m ctrip.cli login")
    print("=" * 78)
    return 0 if not problems else 1


def cmd_browser(cfg, args) -> int:
    """持久化浏览器管理。"""
    import shutil

    from .browser import (BrowserBusyError, clear_stale_locks, keep_alive_status,
                         cdp_port_for, open_session, profile_in_use, stop_keep_alive)

    accounts = resolve_accounts(cfg, _account_ids(args),
                               include_disabled=args.include_disabled, log=logger)

    if args.stop:
        stopped = 0
        for account in accounts:
            if stop_keep_alive(cfg, account.account_id, logger):
                stopped += 1
        print(f"已停止 {stopped} 个常驻浏览器（profile 已保留，下次启动仍复用同一设备指纹）")
        return 0

    if args.clear_locks:
        total = 0
        for account in accounts:
            try:
                total += clear_stale_locks(cfg, account.account_id, logger)
            except BrowserBusyError as exc:
                print(f"[{account.account_id}] 跳过：{exc}")
        print(f"共清理 {total} 个残留锁文件")
        return 0

    if args.reset:
        # profile 重建意味着设备指纹变更，下次登录很可能重新触发验证，
        # 因此先确认无进程占用，避免删到正在使用的目录
        for account in accounts:
            in_use, detail = profile_in_use(cfg, account.account_id)
            if in_use:
                print(f"[{account.account_id}] 拒绝重置：{detail}")
                continue
            profile = cfg.browser_profile_dir(account.account_id)
            shutil.rmtree(profile, ignore_errors=True)
            profile.mkdir(parents=True, exist_ok=True)
            print(f"[{account.account_id}] 已重置 profile：{profile}")
            print(f"[{account.account_id}] 提醒：设备指纹已变更，下次登录可能需要重新过验证码")
        return 0

    if args.start:
        if cfg.ctrip.cdp_port <= 0:
            raise ConfigError(
                "启动常驻浏览器需要先配置 CDP 端口：在 config/config.yaml 的 ctrip 段下设置 "
                "cdp_port: 9222（多账号会自动递增端口），"
                "或设置环境变量 CTRIP__CTRIP__CDP_PORT=9222"
            )
        failed = 0
        for account in accounts:
            log = account_logger(account.account_id, cfg.log_dir(account.account_id),
                                 cfg.run.log_level)
            try:
                session = open_session(cfg, account.account_id, headless=args.headless,
                                       keep_alive=True, log=log)
            except BrowserBusyError as exc:
                print(f"[{account.account_id}] {exc}")
                failed += 1
                continue
            except Exception as exc:  # noqa: BLE001
                print(f"[{account.account_id}] 启动失败：{exc}")
                failed += 1
                continue
            # 只负责拉起常驻浏览器，不关闭它
            session.keep_alive = True
            session.close()
            alive, detail = keep_alive_status(cfg, account.account_id)
            print(f"[{account.account_id}] {'已就绪' if alive else '未就绪'}：{detail}")
            if not alive:
                failed += 1
        if failed == 0:
            print("\n常驻浏览器已启动。接下来执行登录（会直接复用已打开的浏览器）：")
            print("  python -m ctrip.cli login")
        return 0 if failed == 0 else 1

    # 默认：状态表
    print(f"持久化浏览器：{'开启' if cfg.ctrip.persistent_browser else '关闭（每次临时 profile）'}"
          f"，根目录 {cfg.path(cfg.ctrip.browser_dir)}")
    print(f"CDP 常驻端口：{cfg.ctrip.cdp_port or '未配置（不启用常驻浏览器）'}")
    print()
    print(f"{'账号':<10}{'profile':<10}{'大小':<10}{'端口':<8}{'常驻':<8}说明")
    print("-" * 96)
    for account in accounts:
        aid = account.account_id
        profile = cfg.browser_profile_dir(aid)
        has_profile = any(profile.glob("Default/**/*"))
        size_mb = sum(f.stat().st_size for f in profile.rglob("*") if f.is_file()) / 1048576
        alive, detail = keep_alive_status(cfg, aid)
        port = cdp_port_for(cfg, aid)
        print(f"{aid:<10}{('已建立' if has_profile else '未创建'):<10}"
              f"{f'{size_mb:.1f}MB':<10}{(str(port) if port else '-'):<8}"
              f"{('在线' if alive else '离线'):<8}{detail}")
    print()
    print("profile 已建立即意味着设备指纹与本地存储已保留，后续登录不会被当成陌生设备。")
    return 0


def cmd_run(cfg, args) -> int:
    """一体化入口：体检 → 建表 → 登录 → 采集 → 写入。

    这个命令不重复实现任何采集逻辑，而是把已有的体检、建表、采集
    三个环节串成一条链路，并在每一步失败时给出可操作的提示。
    """
    args.skip_login = False
    sink_mode = args.sink or cfg.run.sink
    need_mysql = sink_mode in ("mysql", "both")

    print("=" * 78)
    print("携程数据采集全流程")
    print("=" * 78)

    # 步骤 1：环境体检。放在最前面是为了避免采到一半才发现
    # MySQL 连不上、缺 tzdata 或浏览器未安装这类可提前发现的问题
    if not args.no_check:
        print("\n[步骤 1/4] 环境体检")
        print("-" * 78)
        problems = _preflight(cfg, need_mysql=need_mysql)
        if problems:
            print(f"\n体检发现 {len(problems)} 个阻塞问题，已停止执行：")
            for i, item in enumerate(problems, 1):
                print(f"  {i}. {item}")
            print("\n详细诊断请执行：python -m ctrip.cli doctor")
            return 2
        print("体检通过")
    else:
        print("\n[步骤 1/4] 环境体检（已跳过）")

    # 步骤 2：自动建表。幂等，表已存在时 CREATE TABLE IF NOT EXISTS 不会改动存量
    if need_mysql and not args.no_init_db:
        print("\n[步骤 2/4] 确认数据库与表结构")
        print("-" * 78)
        from .sinks import MySQLSink
        account_ids = None
        if cfg.mysql.table_strategy == "per_account":
            all_accounts = resolve_accounts(cfg, _account_ids(args), include_disabled=True,
                                            log=logger)
            account_ids = [a.account_id for a in all_accounts]
        with MySQLSink(cfg, logger) as sink:
            tables = sink.init_database(create_database=True, account_ids=account_ids)
            print(f"MySQL {sink.ping()} @ {cfg.mysql.dsn_safe()}")
            print(f"表结构已就绪（{len(tables)} 张，策略：{cfg.mysql.table_strategy}）")
    else:
        reason = "已跳过" if args.no_init_db else "仅写 CSV，无需建表"
        print(f"\n[步骤 2/4] 确认表结构（{reason}）")

    # 步骤 3 与 4：登录 + 采集 + 写入（由 Pipeline 一体完成）
    #
    # 带 --keep-alive 时先把常驻浏览器拉起来。
    # 否则当登录态仍有效时，采集根本不会打开浏览器（直接用 Cookie 请求），
    # 用户传了 --keep-alive 却发现浏览器并未常驻，与预期不符
    if args.keep_alive and cfg.ctrip.cdp_port > 0:
        from .browser import ensure_keep_alive
        print("\n[步骤 3/4] 启动常驻浏览器并采集")
        print("-" * 78)
        for account in resolve_accounts(cfg, _account_ids(args),
                                       include_disabled=args.include_disabled, log=logger):
            ensure_keep_alive(cfg, account.account_id,
                              headless=args.headless,
                              log=account_logger(account.account_id,
                                                 cfg.log_dir(account.account_id),
                                                 cfg.run.log_level))
    else:
        print("\n[步骤 3/4] 登录与采集")
        print("-" * 78)
    code = _collect_core(cfg, args, keep_alive=bool(args.keep_alive))

    print("\n[步骤 4/4] 完成")
    print("-" * 78)
    status = {0: "全部成功", 1: "部分成功（详见上方报告与日志）",
              2: "全部失败"}.get(code, f"退出码 {code}")
    print(f"运行结果：{status}")
    if args.keep_alive:
        from .browser import keep_alive_status
        for account in resolve_accounts(cfg, _account_ids(args),
                                        include_disabled=args.include_disabled, log=logger):
            alive, detail = keep_alive_status(cfg, account.account_id)
            print(f"  [{account.account_id}] 浏览器{'仍在后台' if alive else '未常驻'}：{detail}")
    return code


def _preflight(cfg, *, need_mysql: bool) -> list[str]:
    """轻量级前置体检，只报会阻塞采集的问题。

    与 ``doctor`` 的区别：doctor 面向人工排障，会把所有细节列出来；
    这里面向自动化执行，只关心「能不能跑」，且不因非致命问题中断。
    """
    from .timeutil import get_timezone

    problems: list[str] = []

    # 账号凭据
    try:
        accounts = resolve_accounts(cfg, None, include_disabled=False, log=logger)
    except Exception as exc:  # noqa: BLE001
        problems.append(f"账号解析失败：{exc}")
        accounts = []
    if not accounts:
        problems.append("没有可用的启用账号（检查 config/config.yaml 与 .env）")
    else:
        missing = [a.account_id for a in accounts if not a.account or not a.password]
        if missing:
            problems.append(f"账号 {', '.join(missing)} 缺少用户名或密码"
                            f"（在 .env 中配置，执行 doctor 查看详情）")
        print(f"账号凭据：{len(accounts)} 个启用账号"
              f"（{', '.join(a.account_id for a in accounts)}）")

    # 时区
    tz = get_timezone(cfg.run.timezone, log=logger)
    print(f"时区：{cfg.run.timezone} → {tz}")

    # 浏览器
    try:
        from playwright.sync_api import sync_playwright

        from .browser import _silence_asyncio_teardown_noise
        with sync_playwright() as p:
            exe = p.chromium.executable_path
        # 同样会遗留 asyncio 噪声，预先静默
        _silence_asyncio_teardown_noise()
        if exe and Path(exe).exists():
            print(f"浏览器：Chromium 已安装（持久化 profile 根目录 "
                  f"{cfg.path(cfg.ctrip.browser_dir)}）")
        else:
            problems.append("Chromium 未安装：执行 playwright install chromium")
    except Exception as exc:  # noqa: BLE001
        problems.append(f"Playwright 不可用：{exc}")

    # MySQL
    if need_mysql:
        if not cfg.mysql.enabled:
            problems.append("落地方式含 MySQL 但 mysql.enabled=false"
                            "（在 config/config.yaml 开启，或设 CTRIP__MYSQL__ENABLED=true）")
        else:
            try:
                from .sinks import MySQLSink
                with MySQLSink(cfg, logger) as sink:
                    print(f"MySQL：{sink.ping()} @ {cfg.mysql.dsn_safe()} 连接正常")
            except Exception as exc:  # noqa: BLE001
                problems.append(f"MySQL 连接失败：{exc}")
    return problems


COMMANDS = {
    "login": cmd_login,
    "doctor": cmd_doctor,
    "check": cmd_check,
    "collect": cmd_collect,
    "run": cmd_run,
    "browser": cmd_browser,
    "init-db": cmd_init_db,
    "show-ddl": cmd_show_ddl,
    "show-schema": cmd_show_schema,
    "show-config": cmd_show_config,
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # doctor 的职责就是诊断配置问题，因此不能先被配置校验拦住
    validate = args.command not in ("show-config", "show-ddl", "show-schema", "doctor")
    try:
        cfg = load_config(args.config, validate=validate)
    except ConfigError as exc:
        print(f"配置错误：{exc}", file=sys.stderr)
        return 2

    if args.log_level:
        cfg.run.log_level = args.log_level
    setup_root(cfg.run.log_level, cfg.path(cfg.run.log_dir))

    handler = COMMANDS[args.command]
    try:
        return handler(cfg, args)
    except ConfigError as exc:
        logger.error("配置错误：%s", exc)
        return 2
    except ValueError as exc:
        # 参数语义错误（如日期格式、区间冲突）是用户输入问题，
        # 直接给一行可读提示，不要把堆栈扑到用户脸上
        logger.error("参数错误：%s", exc)
        return 2
    except KeyboardInterrupt:
        logger.warning("已被用户中断")
        return 130
    except Exception as exc:  # noqa: BLE001
        logger.exception("执行失败：%s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
