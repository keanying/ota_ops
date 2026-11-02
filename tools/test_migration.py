#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""迁移脚本回归测试：在模拟的旧版存量库上执行 migration_to_new_schema.sql，
验证「表重命名 + 字段重命名 + 补唯一键 + 新增审计字段」全部生效且历史数据不丢。

运行::

    python tools/test_migration.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ctrip.config import load_dotenv  # noqa: E402

# 与主程序保持一致：自动读取 .env，不需要先手动 source（Windows 上尤其必要）
load_dotenv()

DB = "ctrip_legacy_test"
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_USER = os.getenv("MYSQL_USER", "ctrip")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

# 旧版表结构（本次改名前的样子），仅建迁移脚本会触碰的列
LEGACY_DDL = """
DROP DATABASE IF EXISTS {db};
CREATE DATABASE {db} DEFAULT CHARSET utf8mb4;
USE {db};

CREATE TABLE `ctrip_kylin_daily` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `account_id` VARCHAR(50) NOT NULL,
  `scenic_id` VARCHAR(50) NULL,
  `record_date` DATE NOT NULL,
  `signing_code` VARCHAR(20) NOT NULL,
  `signing_type` VARCHAR(50) NOT NULL DEFAULT '',
  `vbk_bu_type` VARCHAR(50) NOT NULL DEFAULT '',
  `date_type` VARCHAR(20) NOT NULL,
  `income` DECIMAL(18,2) NULL,
  `income_mom_rate` DECIMAL(12,6) NULL,
  `order_count` INT NULL,
  `order_count_mom_rate` DECIMAL(12,6) NULL,
  `ticket_qty` INT NULL,
  `ticket_qty_mom_rate` DECIMAL(12,6) NULL,
  `verify_qty` INT NULL,
  `verify_qty_mom_rate` DECIMAL(12,6) NULL,
  `refund_rate` DECIMAL(12,6) NULL,
  `refund_rate_mom_rate` DECIMAL(12,6) NULL,
  `collected_at` DATETIME NULL,
  PRIMARY KEY (`id`),
  KEY `idx_kylin_date` (`record_date`),
  KEY `idx_kylin_account_date` (`account_id`, `record_date`),
  KEY `idx_kylin_scenic_date` (`scenic_id`, `record_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `ctrip_destination_rating` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `account_id` VARCHAR(50) NOT NULL,
  `scenic_id` VARCHAR(50) NULL,
  `stat_month` VARCHAR(7) NOT NULL,
  `region_id` VARCHAR(20) NOT NULL DEFAULT 'ALL',
  `region_name` VARCHAR(50) NOT NULL DEFAULT '',
  `region_type` VARCHAR(10) NULL,
  `message` MEDIUMTEXT NULL,
  `message_length` INT NULL,
  `collected_at` DATETIME NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ctrip_destination_rating` (`account_id`, `stat_month`, `region_id`),
  KEY `idx_rating_month` (`stat_month`),
  KEY `idx_rating_scenic` (`scenic_id`, `stat_month`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `ctrip_supplier_rate_score` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `account_id` VARCHAR(50) NOT NULL,
  `scenic_id` VARCHAR(50) NULL,
  `stat_month` VARCHAR(7) NOT NULL,
  `region_id` VARCHAR(20) NOT NULL,
  `region_name` VARCHAR(50) NOT NULL DEFAULT '',
  `region_type` VARCHAR(10) NULL,
  `service_score` DECIMAL(8,2) NULL,
  `order_confirm_rate` DECIMAL(8,2) NULL,
  `fulfillment_defect_rate` DECIMAL(8,2) NULL,
  `cs_intervention_rate` DECIMAL(8,2) NULL,
  `reply_timeliness_rate` DECIMAL(8,2) NULL,
  `im_negative_rate` DECIMAL(8,2) NULL,
  `collected_at` DATETIME NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ctrip_supplier_rate_score` (`account_id`, `stat_month`, `region_id`),
  KEY `idx_score_month` (`stat_month`),
  KEY `idx_score_region` (`region_id`),
  KEY `idx_score_scenic` (`scenic_id`, `stat_month`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `ctrip_account` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `account_id` VARCHAR(50) NOT NULL,
  `scenic_id` VARCHAR(50) NULL,
  `account_name` VARCHAR(100) NOT NULL DEFAULT '',
  `login_name` VARCHAR(100) NOT NULL DEFAULT '',
  `vendor_id` VARCHAR(50) NULL,
  `status` VARCHAR(20) NOT NULL DEFAULT 'active',
  `last_login_at` DATETIME NULL,
  `remark` VARCHAR(255) NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ctrip_account` (`account_id`),
  KEY `idx_account_status` (`status`),
  KEY `idx_account_scenic` (`scenic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `ctrip_collect_run_log` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `account_id` VARCHAR(50) NOT NULL,
  `scenic_id` VARCHAR(50) NULL,
  `job` VARCHAR(30) NOT NULL,
  `run_key` VARCHAR(60) NOT NULL DEFAULT '',
  `started_at` DATETIME NULL,
  `finished_at` DATETIME NULL,
  `duration_ms` INT NULL,
  `status` VARCHAR(20) NOT NULL DEFAULT '',
  `rows_fetched` INT NULL,
  `rows_written` INT NULL,
  `sink` VARCHAR(30) NULL,
  `error` VARCHAR(1000) NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ctrip_collect_run_log` (`account_id`, `job`, `run_key`),
  KEY `idx_log_started` (`started_at`),
  KEY `idx_log_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 存量数据
INSERT INTO `ctrip_kylin_daily`
  (`account_id`,`scenic_id`,`record_date`,`signing_code`,`signing_type`,
   `vbk_bu_type`,`date_type`,`income`,`order_count`,`ticket_qty`,
   `verify_qty`,`refund_rate`,`collected_at`)
VALUES
  ('A001','PFTSCA01018088','2026-08-01','1','跟团游','跟团游','使用日期',
   1234.56,12,50,48,0.125,'2026-08-02 10:00:00'),
  ('A001','PFTSCA01018088','2026-08-02','1','跟团游','跟团游','使用日期',
   2345.67,20,60,55,0.100,'2026-08-03 10:00:00'),
  ('A002',NULL,'2026-08-01','7','景点玩乐','门票','预订日期',
   999.99,5,10,9,0.200,'2026-08-02 10:00:00');

INSERT INTO `ctrip_supplier_rate_score`
  (`account_id`,`scenic_id`,`stat_month`,`region_id`,`region_name`,`region_type`,
   `service_score`,`order_confirm_rate`,`fulfillment_defect_rate`,
   `cs_intervention_rate`,`reply_timeliness_rate`,`im_negative_rate`,`collected_at`)
VALUES
  ('A001','PFTSCA01018088','2026-07','200001','吉林','2',
   3.30,2.00,3.50,3.20,2.90,2.90,'2026-08-02 10:00:00');

INSERT INTO `ctrip_destination_rating`
  (`account_id`,`scenic_id`,`stat_month`,`region_id`,`region_name`,
   `region_type`,`message`,`message_length`,`collected_at`)
VALUES
  ('A001','PFTSCA01018088','2026-07','ALL','账号汇总','0',
   '### 现状分析\\n历史分析文本',20,'2026-08-02 10:00:00');

INSERT INTO `ctrip_account`
  (`account_id`,`scenic_id`,`account_name`,`login_name`,`status`)
VALUES
  ('A001','PFTSCA01018088','主账号','vbk_569202','active'),
  ('A002','PFTSCA01029177','第二账号','vbk_778001','active');
""".format(db=DB)

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    results.append((PASS if cond else FAIL, name, detail))


def mysql(sql: str, database: str | None = None) -> str:
    cmd = ["mysql", f"-h{MYSQL_HOST}", f"-u{MYSQL_USER}"]
    if MYSQL_PASSWORD:
        cmd.append(f"-p{MYSQL_PASSWORD}")
    if database:
        cmd.append(database)
    cmd += ["-N", "-B", "-e", sql]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        stderr = "\n".join(
            line for line in proc.stderr.splitlines() if "Warning" not in line
        )
        raise RuntimeError(stderr.strip() or "mysql 执行失败")
    return proc.stdout.strip()


def mysql_file(path: Path, database: str | None = None) -> None:
    cmd = ["mysql", f"-h{MYSQL_HOST}", f"-u{MYSQL_USER}"]
    if MYSQL_PASSWORD:
        cmd.append(f"-p{MYSQL_PASSWORD}")
    if database:
        cmd.append(database)
    with path.open(encoding="utf-8") as fh:
        proc = subprocess.run(cmd, stdin=fh, capture_output=True, text=True)
    if proc.returncode != 0:
        stderr = "\n".join(
            line for line in proc.stderr.splitlines() if "Warning" not in line
        )
        raise RuntimeError(stderr.strip() or "迁移脚本执行失败")


def main() -> int:
    print("=" * 90)
    print("迁移脚本回归测试：旧版存量库 -> 新 DDL")
    print("=" * 90)

    # 1) 构造旧版存量库
    legacy_sql = ROOT / "sql" / "_legacy_fixture.sql"
    legacy_sql.write_text(LEGACY_DDL, encoding="utf-8")
    try:
        mysql_file(legacy_sql)
        print(f"已构造模拟旧版库 {DB}（含存量数据）")
    except RuntimeError as exc:
        print(f"[FAIL] 构造旧版库失败：{exc}")
        return 1
    finally:
        legacy_sql.unlink(missing_ok=True)

    before_trade = int(mysql("SELECT COUNT(*) FROM ctrip_kylin_daily;", DB))
    before_income = mysql(
        "SELECT income FROM ctrip_kylin_daily "
        "WHERE account_id='A001' AND record_date='2026-08-01';", DB)

    # 2) 执行迁移脚本
    try:
        mysql_file(ROOT / "sql" / "migration_to_new_schema.sql", DB)
        check("迁移脚本执行成功", True)
    except RuntimeError as exc:
        check("迁移脚本执行成功", False, str(exc)[:120])
        _report()
        return 1

    # 3) 表名已重命名
    tables = set(mysql("SHOW TABLES;", DB).split("\n"))
    for expect in ("ads_sup_ota_ctrip_trade_di",
                   "ads_sup_ota_ctrip_destination_rating_di",
                   "ads_sup_ota_ctrip_score_di",
                   "ads_service_ota_ctrip_account",
                   "ads_service_ota_ctrip_run_log"):
        check(f"表 {expect} 存在", expect in tables)
    for old in ("ctrip_kylin_daily", "ctrip_supplier_rate_score"):
        check(f"旧表 {old} 已不存在", old not in tables)

    # 4) 字段已重命名，且历史数据未丢
    cols = set(mysql(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA='{DB}' AND TABLE_NAME='ads_sup_ota_ctrip_trade_di';").split("\n"))
    for expect in ("transaction_date", "biz_type", "turnover", "turnover_mom",
                   "order_count_mom", "sold_ticket_cnt", "sold_ticket_mom",
                   "validated_ticket_volume", "validated_ticket_mom",
                   "refund_rate_mom", "collect_time", "create_time", "update_time"):
        check(f"交易表含新字段 {expect}", expect in cols)
    for old in ("record_date", "vbk_bu_type", "income", "ticket_qty",
                "verify_qty", "collected_at"):
        check(f"交易表旧字段 {old} 已消失", old not in cols)

    after_trade = int(mysql("SELECT COUNT(*) FROM ads_sup_ota_ctrip_trade_di;", DB))
    check("交易表历史行数未丢失", after_trade == before_trade,
          f"{before_trade} -> {after_trade}")
    after_income = mysql(
        "SELECT turnover FROM ads_sup_ota_ctrip_trade_di "
        "WHERE account_id='A001' AND transaction_date='2026-08-01';", DB)
    check("重命名后历史数值保持一致", after_income == before_income,
          f"{before_income} -> {after_income}")

    # 5) 计数字段已提升为 BIGINT
    types = dict(
        line.split("\t") for line in mysql(
            "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
            f"WHERE TABLE_SCHEMA='{DB}' AND TABLE_NAME='ads_sup_ota_ctrip_trade_di';"
        ).split("\n"))
    for col in ("order_count", "sold_ticket_cnt", "validated_ticket_volume"):
        check(f"{col} 已提升为 BIGINT", types.get(col) == "bigint", types.get(col, ""))

    # 6) 唯一键与索引
    uk = mysql(
        "SELECT INDEX_NAME FROM information_schema.STATISTICS "
        f"WHERE TABLE_SCHEMA='{DB}' AND TABLE_NAME='ads_sup_ota_ctrip_trade_di' "
        "AND NON_UNIQUE=0 AND INDEX_NAME<>'PRIMARY' GROUP BY INDEX_NAME;")
    check("交易表已补唯一键", uk == "uk_ads_sup_ota_ctrip_trade_di", uk)

    score_uk = mysql(
        "SELECT INDEX_NAME FROM information_schema.STATISTICS "
        f"WHERE TABLE_SCHEMA='{DB}' AND TABLE_NAME='ads_sup_ota_ctrip_score_di' "
        "AND NON_UNIQUE=0 AND INDEX_NAME<>'PRIMARY' GROUP BY INDEX_NAME;")
    check("评分表唯一键已改名为 uk_ 前缀",
          score_uk == "uk_ads_sup_ota_ctrip_score_di", score_uk)

    idx = set(mysql(
        "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS "
        f"WHERE TABLE_SCHEMA='{DB}' AND TABLE_NAME='ads_sup_ota_ctrip_trade_di';").split("\n"))
    check("交易表索引已跟随字段改名重建",
          {"idx_kylin_date", "idx_kylin_account_date", "idx_kylin_scenic_date"} <= idx)

    # 7) 唯一键真正生效：重复插入应被 UPSERT 吸收
    mysql(
        "INSERT INTO ads_sup_ota_ctrip_trade_di "
        "(account_id,scenic_id,transaction_date,signing_code,signing_type,"
        " biz_type,date_type,turnover,collect_time) "
        "VALUES ('A001','PFTSCA01018088','2026-08-01','1','跟团游','跟团游',"
        "'使用日期',8888.88,NOW()) "
        "ON DUPLICATE KEY UPDATE turnover=VALUES(turnover);", DB)
    dup_count = int(mysql("SELECT COUNT(*) FROM ads_sup_ota_ctrip_trade_di;", DB))
    check("唯一键生效：重复插入未新增行", dup_count == after_trade,
          f"{after_trade} -> {dup_count}")
    updated = mysql(
        "SELECT turnover FROM ads_sup_ota_ctrip_trade_di "
        "WHERE account_id='A001' AND transaction_date='2026-08-01';", DB)
    check("重复插入触发原地更新", updated.startswith("8888.88"), updated)

    # 8) update_time 自动维护
    extra = mysql(
        "SELECT EXTRA FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA='{DB}' AND TABLE_NAME='ads_sup_ota_ctrip_trade_di' "
        "AND COLUMN_NAME='update_time';")
    check("update_time 带 ON UPDATE CURRENT_TIMESTAMP",
          "on update" in extra.lower(), extra)

    # 9) 其余两张业务表字段
    score_cols = set(mysql(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA='{DB}' AND TABLE_NAME='ads_sup_ota_ctrip_score_di';").split("\n"))
    for expect in ("evaluation_month", "overall_service_score", "ocr_score",
                   "fdr_score", "manual_intervention_rate_score",
                   "timely_response_20s_score", "im_neg_rate_score",
                   "collect_time", "create_time", "update_time"):
        check(f"评分表含新字段 {expect}", expect in score_cols)

    rating_cols = set(mysql(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA='{DB}' "
        "AND TABLE_NAME='ads_sup_ota_ctrip_destination_rating_di';").split("\n"))
    for expect in ("evaluation_month", "analysis_text", "analysis_text_len",
                   "collect_time", "create_time", "update_time"):
        check(f"评级表含新字段 {expect}", expect in rating_cols)

    # 10) 迁移后结构与 schema.py 生成的 DDL 列一致
    from ctrip.schema import TABLES  # noqa: PLC0415

    for key in ("kylin_daily", "destination_rating", "supplier_rate_score",
                "account_dim", "collect_run_log"):
        table = TABLES[key]
        actual = mysql(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            f"WHERE TABLE_SCHEMA='{DB}' AND TABLE_NAME='{table.name}' "
            "ORDER BY ORDINAL_POSITION;").split("\n")
        expected = ["id"] + table.all_column_names()
        check(f"{table.name} 迁移后列与 schema.py 完全一致",
              actual == expected,
              "" if actual == expected else f"差异={set(expected) ^ set(actual)}")

    # 清理
    mysql(f"DROP DATABASE IF EXISTS {DB};")
    print(f"已清理测试库 {DB}\n")
    return _report()


def _report() -> int:
    width = max(len(n) for _, n, _ in results) + 2
    failed = sum(1 for s, _, _ in results if s == FAIL)
    for status, name, detail in results:
        print(f"[{status}] {name:<{width}}{detail}")
    print("-" * 90)
    print(f"共 {len(results)} 项，失败 {failed} 项")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
