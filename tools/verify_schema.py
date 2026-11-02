#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""表结构单一源自检：验证「改一处，全链路同步」以及脏数据清洗能力。

运行::

    python tools/verify_schema.py
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ctrip.schema import (ACCOUNT_DIM, DESTINATION_RATING,  # noqa: E402
                          KYLIN_DAILY, SUPPLIER_RATE_SCORE, TABLES, Field,
                          TableSchema, account_field, all_ddl)

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    results.append((PASS if condition else FAIL, name, detail))


def main() -> int:
    # 1) 上游脏字段名 -> 合法列名映射
    raw = {
        "date_start": "2026/08/05",
        "code": 7,
        "signing_type": "景点玩乐",
        "vbk_bu_type": "门票",
        "date_type": "使用日期",
        "ttd_suc_nocoef_income": "1,234.56",
        "ttd_suc_ord_cnt": "12笔",
        "ttd_suc_nocoef_qty": 50.0,
        "ttd_chk_qty": "",
        "ttd_arfd_ord_cnt|ttd_pay_ord_cnt|/": "12.5%",
        "ttd_arfd_ord_cnt|ttd_pay_ord_cnt|/_MOMRate": "-",
    }
    row = KYLIN_DAILY.build_row(raw, {"account_id": "A001",
                                      "collect_time": "2026-08-13 10:00:00"})
    check("含 | 和 / 的指标名映射为合法列名",
          "refund_rate" in row and row["refund_rate"] == Decimal("0.125"),
          f"refund_rate={row['refund_rate']}")
    check("日期格式 2026/08/05 归一化",
          str(row["transaction_date"]) == "2026-08-05", str(row["transaction_date"]))
    check("千分位金额清洗", row["turnover"] == Decimal("1234.56"), str(row["turnover"]))
    check("带单位整数清洗", row["order_count"] == 12, str(row["order_count"]))
    check("浮点转整数", row["sold_ticket_cnt"] == 50, str(row["sold_ticket_cnt"]))
    check("空串转 NULL", row["validated_ticket_volume"] is None,
          str(row["validated_ticket_volume"]))
    check("占位符 '-' 转 NULL", row["refund_rate_mom"] is None,
          str(row["refund_rate_mom"]))
    check("account_id 强制注入", row["account_id"] == "A001", row["account_id"])
    check("业务类型映射 vbk_bu_type -> biz_type",
          row["biz_type"] == "门票", str(row["biz_type"]))
    check("采集时间写入 collect_time",
          row["collect_time"] == "2026-08-13 10:00:00", str(row["collect_time"]))

    # 2) 唯一键必须以 account_id 开头（多账号隔离约束）
    for table in (KYLIN_DAILY, SUPPLIER_RATE_SCORE, DESTINATION_RATING):
        check(f"{table.name} 唯一键以 account_id 开头",
              table.unique_key[0] == "account_id", str(table.unique_key))

    # 3) 违反隔离约束的表定义必须报错
    try:
        TableSchema(
            name="bad_table", comment="错误示例",
            fields=[account_field(), Field("evaluation_month", "VARCHAR(7)", "月份",
                                           nullable=False)],
            unique_key=["evaluation_month"],  # 未以 account_id 开头
        )
        check("拒绝唯一键未含 account_id 的表定义", False, "未抛异常")
    except ValueError as exc:
        check("拒绝唯一键未含 account_id 的表定义", True, str(exc)[:40])

    # 4) 非法列名必须报错
    try:
        Field("bad name!", "VARCHAR(10)", "非法")
        check("拒绝非法列名", False, "未抛异常")
    except ValueError:
        check("拒绝非法列名", True)

    # 5) CSV 表头 == DDL 列 == UPSERT 列（三处同步）
    csv_cols = KYLIN_DAILY.csv_columns()
    upsert = KYLIN_DAILY.upsert_sql()
    ddl = KYLIN_DAILY.ddl()
    check("CSV 列与 UPSERT 列一致",
          all(f"`{c}`" in upsert for c in csv_cols), f"{len(csv_cols)} 列")
    check("CSV 列与 DDL 列一致",
          all(f"`{c}`" in ddl for c in csv_cols))
    check("唯一键列不参与 UPDATE",
          "`transaction_date` = VALUES(`transaction_date`)" not in upsert)
    check("UPSERT 全参数化（无字面量拼接）",
          upsert.count("%s") == len(KYLIN_DAILY.column_names()))

    # 6) 新增字段后自动出现在三处产物中
    original = list(KYLIN_DAILY.fields)
    try:
        KYLIN_DAILY.fields = original + [
            Field("gmv_per_order", "DECIMAL(18,2)", "客单价（演示新增字段）",
                  source="ttd_avg_price", cast="decimal")
        ]
        check("新增字段自动进入 CSV 表头", "gmv_per_order" in KYLIN_DAILY.csv_columns())
        check("新增字段自动进入 DDL", "`gmv_per_order`" in KYLIN_DAILY.ddl())
        check("新增字段自动进入 UPSERT",
              "`gmv_per_order` = VALUES(`gmv_per_order`)" in KYLIN_DAILY.upsert_sql())
        new_row = KYLIN_DAILY.build_row({**raw, "ttd_avg_price": "88.80"},
                                        {"account_id": "A001"})
        check("新增字段自动完成上游映射与清洗",
              new_row["gmv_per_order"] == Decimal("88.80"), str(new_row["gmv_per_order"]))
    finally:
        KYLIN_DAILY.fields = original

    # 7) per_account 策略生成分表 DDL
    ddl_split = all_ddl("", ["A001", "A002"], "per_account")
    check("per_account 策略生成按账号分表 DDL",
          "`ads_sup_ota_ctrip_trade_di_a001`" in ddl_split
          and "`ads_sup_ota_ctrip_trade_di_a002`" in ddl_split)
    check("维表与日志表在 per_account 下保持共享",
          "`ads_service_ota_ctrip_account`" in ddl_split
          and "`ads_service_ota_ctrip_account_a001`" not in ddl_split)

    # 8) 必填校验
    bad = KYLIN_DAILY.build_row({"code": 7}, {"account_id": "A001"})
    problems = KYLIN_DAILY.validate_row(bad)
    check("缺少必填字段被拦截", bool(problems), f"{len(problems)} 个问题")

    # 9) scenic_id（景区ID）约束
    for _key, table in TABLES.items():
        names = table.column_names()
        check(f"{table.name} 含 scenic_id 字段", "scenic_id" in names)
        if "scenic_id" in names:
            check(f"{table.name} scenic_id 紧跟 account_id",
                  names.index("scenic_id") == 1, f"index={names.index('scenic_id')}")
            check(f"{table.name} scenic_id 不在唯一键中",
                  "scenic_id" not in table.unique_key, str(table.unique_key))
            fld = table.field_map()["scenic_id"]
            check(f"{table.name} scenic_id 参与 UPSERT 更新（可回填）", fld.upsert)

    scenic_row = KYLIN_DAILY.build_row(raw, {"account_id": "A001",
                                             "scenic_id": "PFTSCA01018088"})
    check("scenic_id 正常写入行", scenic_row["scenic_id"] == "PFTSCA01018088",
          str(scenic_row["scenic_id"]))
    empty_row = KYLIN_DAILY.build_row(raw, {"account_id": "A001", "scenic_id": ""})
    check("未绑定时 scenic_id 为 NULL 而非空串",
          empty_row["scenic_id"] is None, str(empty_row["scenic_id"]))
    check("scenic_id 参与 UPSERT 回填语句",
          "`scenic_id` = VALUES(`scenic_id`)" in KYLIN_DAILY.upsert_sql())
    check("scenic_id 进入 CSV 表头且位于第二列",
          KYLIN_DAILY.csv_columns()[1] == "scenic_id")
    check("三张业务表均含 scenic_id",
          all("scenic_id" in t.column_names()
              for t in (KYLIN_DAILY, DESTINATION_RATING, SUPPLIER_RATE_SCORE)))

    # 10) 表名与唯一键索引名符合更新后的 DDL
    expected_tables = {
        "kylin_daily": "ads_sup_ota_ctrip_trade_di",
        "destination_rating": "ads_sup_ota_ctrip_destination_rating_di",
        "supplier_rate_score": "ads_sup_ota_ctrip_score_di",
        "account_dim": "ads_service_ota_ctrip_account",
        "collect_run_log": "ads_service_ota_ctrip_run_log",
    }
    for key, expect in expected_tables.items():
        check(f"表名 {key} = {expect}", TABLES[key].name == expect, TABLES[key].name)
    check("交易表唯一键覆盖账号+日期+周期+签约+业务+日期类型",
          KYLIN_DAILY.unique_key == ["account_id", "transaction_date", "period_type",
                                     "signing_code", "biz_type", "date_type"],
          str(KYLIN_DAILY.unique_key))
    check("评分表唯一键索引名不与表名重复",
          SUPPLIER_RATE_SCORE.uk_name() == "uk_ads_sup_ota_ctrip_score_di",
          SUPPLIER_RATE_SCORE.uk_name())
    check("目的地评级唯一键索引名沿用 uk_ctrip_destination_rating",
          DESTINATION_RATING.uk_name() == "uk_ctrip_destination_rating")

    # 11) create_time / update_time 由数据库自动维护
    for key, table in TABLES.items():
        all_cols = table.all_column_names()
        write_cols = table.column_names()
        check(f"{table.name} DDL 含 create_time/update_time",
              "create_time" in all_cols and "update_time" in all_cols)
        check(f"{table.name} 写入列排除自动维护字段",
              "create_time" not in write_cols and "update_time" not in write_cols)
        check(f"{table.name} CSV 表头排除自动维护字段",
              "create_time" not in table.csv_columns()
              and "update_time" not in table.csv_columns())
    ddl_kylin = KYLIN_DAILY.ddl()
    check("update_time 带 ON UPDATE CURRENT_TIMESTAMP",
          "`update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP "
          "ON UPDATE CURRENT_TIMESTAMP" in ddl_kylin)
    check("INSERT 列不含 create_time",
          "`create_time`" not in KYLIN_DAILY.insert_sql())
    check("UPSERT 显式刷新 update_time",
          "`update_time` = CURRENT_TIMESTAMP" in KYLIN_DAILY.upsert_sql())
    check("build_row 不产出自动维护字段",
          "create_time" not in row and "update_time" not in row)
    check("collect_time 仍由程序写入", "collect_time" in KYLIN_DAILY.column_names())
    check("账号维表也带审计时间字段",
          "create_time" in ACCOUNT_DIM.all_column_names())

    # ---------------------------------------------------------------
    # 覆盖写入（同日期/月份先清理再写入）所需的分区列
    # ---------------------------------------------------------------
    check("交易表分区列为 transaction_date",
          KYLIN_DAILY.partition_column == "transaction_date",
          str(KYLIN_DAILY.partition_column))
    check("目的地评级表分区列为 evaluation_month",
          DESTINATION_RATING.partition_column == "evaluation_month")
    check("供应商评级表分区列为 evaluation_month",
          SUPPLIER_RATE_SCORE.partition_column == "evaluation_month")
    for table in (KYLIN_DAILY, DESTINATION_RATING, SUPPLIER_RATE_SCORE):
        check(f"{table.name} 支持覆盖写入", table.supports_overwrite())
        check(f"{table.name} 分区列属于唯一键",
              table.partition_column in table.unique_key,
              f"uk={table.unique_key}")
    check("账号维表不参与分区覆盖", not ACCOUNT_DIM.supports_overwrite(),
          "账号维表是维度表，整表 UPSERT 即可")
    # 分区值提取：去重且忽略空值
    part_rows = [
        {"transaction_date": "2025-01-01"},
        {"transaction_date": "2025-01-01"},
        {"transaction_date": "2025-01-02"},
        {"transaction_date": None},
        {},
    ]
    parts = KYLIN_DAILY.partition_values(part_rows)
    check("分区值提取去重且排除空值",
          sorted(parts) == ["2025-01-01", "2025-01-02"], str(sorted(parts)))

    # ---------------------------------------------------------------
    # 时间区间解析与切片
    # ---------------------------------------------------------------
    from datetime import date as _date  # noqa: PLC0415

    from ctrip.daterange import (month_list, parse_date,  # noqa: PLC0415
                                 parse_month, resolve_date_range,
                                 resolve_month_range, slice_dates)

    today = _date(2026, 8, 13)
    start, end = resolve_date_range(today=today, default_days=30)
    check("日粒度默认口径为 T+1（到昨天）",
          end == _date(2026, 8, 12), str(end))
    check("日粒度默认回溯 30 天", (end - start).days + 1 == 30)

    start, end = resolve_date_range(today=today,
                                    start_date=_date(2025, 1, 1),
                                    end_date=_date(2025, 3, 31))
    batches = slice_dates(start, end, 30)
    check("90 天按 30 天/批切为 3 批", len(batches) == 3, f"{len(batches)} 批")
    check("分批从早到晚且首批起点为区间起点",
          batches[0].start == _date(2025, 1, 1))
    check("末批终点为区间终点", batches[-1].end == _date(2025, 3, 31))
    check("分批无重叠无遗漏",
          sum(b.days for b in batches) == 90, str(sum(b.days for b in batches)))
    covered: list[_date] = []
    for b in batches:
        covered.extend([b.start, b.end])
    check("相邻批次首尾相接（不差一天）",
          all((batches[i + 1].start - batches[i].end).days == 1
              for i in range(len(batches) - 1)))

    conflict = False
    try:
        resolve_date_range(today=today, start_date=_date(2025, 1, 1), days=7)
    except ValueError:
        conflict = True
    check("--start-date 与 --days 冲突时报错而不默默取其一", conflict)

    future = False
    try:
        resolve_date_range(today=today, end_date=_date(2027, 1, 1))
    except ValueError:
        future = True
    check("截止日期晚于今天时报错", future)

    check("月粒度默认采上个月",
          resolve_month_range(today=today) == ["2026-07"],
          str(resolve_month_range(today=today)))
    check("月区间展开正确（含边界）",
          resolve_month_range(today=today, start_month="2025-01",
                              end_month="2025-04")
          == ["2025-01", "2025-02", "2025-03", "2025-04"])
    check("月份跨年递增正确",
          month_list("2025-11", "2026-02")
          == ["2025-11", "2025-12", "2026-01", "2026-02"])
    check("非连续月份去重升序",
          resolve_month_range(today=today,
                              months=["2025-07", "2025-03", "2025-07"])
          == ["2025-03", "2025-07"])
    check("日期容错格式 20250801 / 2025/08/01",
          parse_date("20250801") == parse_date("2025/08/01") == _date(2025, 8, 1))
    check("月份容错格式 202508 / 2025/08",
          parse_month("202508") == parse_month("2025/08") == "2025-08")

    # ---------------------------------------------------------------
    # 周期口径 period_type 与成交资源数
    # ---------------------------------------------------------------
    from ctrip.collectors.kylin_daily import (  # noqa: PLC0415
        PERIOD_DAILY, PERIOD_WINDOWS, QUERY_ID, RESOURCE_METRIC,
        RESOURCE_QUERY_ID)

    kf = KYLIN_DAILY.field_map()
    check("交易表含 period_type 字段", "period_type" in kf)
    check("period_type 必须进唯一键（否则 1d/7d/30d 互相覆盖）",
          "period_type" in KYLIN_DAILY.unique_key, str(KYLIN_DAILY.unique_key))
    check("交易表含成交资源数及环比字段",
          "deal_resource_count" in kf and "deal_resource_count_mom" in kf)
    check("成交资源数映射到上游 suc_resourceid",
          kf["deal_resource_count"].source == RESOURCE_METRIC,
          str(kf["deal_resource_count"].source))
    check("成交资源数环比映射到 _MOMRate 子树",
          kf["deal_resource_count_mom"].source == f"{RESOURCE_METRIC}_MOMRate")
    check("资源数使用独立 queryId=354（与主指标 353 不同）",
          RESOURCE_QUERY_ID == 354 and QUERY_ID == 353,
          f"resource={RESOURCE_QUERY_ID}, main={QUERY_ID}")
    check("周期口径定义为 1d/7d/30d",
          PERIOD_DAILY == "1d" and set(PERIOD_WINDOWS) == {"7d", "30d"},
          f"{PERIOD_DAILY} + {sorted(PERIOD_WINDOWS)}")
    check("7d/30d 窗口天数正确",
          PERIOD_WINDOWS["7d"] == 7 and PERIOD_WINDOWS["30d"] == 30)

    period_row = KYLIN_DAILY.build_row(
        {**raw, "period_type": "7d", "suc_resourceid": "12",
         "suc_resourceid_MOMRate": "0.25"},
        {"account_id": "A001"})
    check("period_type 正常写入行", period_row["period_type"] == "7d",
          str(period_row["period_type"]))
    check("成交资源数清洗为整数", period_row["deal_resource_count"] == 12,
          str(period_row["deal_resource_count"]))
    check("成交资源数环比清洗为小数",
          period_row["deal_resource_count_mom"] == Decimal("0.25"),
          str(period_row["deal_resource_count_mom"]))

    # 覆盖写入必须同时限定 period_type：默认 kylin_days=30 时，
    # 30d 窗口起始日必然落在 1d 区间内，不限定就会误删同日其他口径的行
    check("交易表覆盖范围附加限定 period_type",
          KYLIN_DAILY.partition_extra == ("period_type",),
          str(KYLIN_DAILY.partition_extra))
    check("附加分区维度必须属于唯一键",
          all(x in KYLIN_DAILY.unique_key for x in KYLIN_DAILY.partition_extra))
    only_1d = [{"period_type": "1d", "transaction_date": "2026-08-01"},
               {"period_type": "1d", "transaction_date": "2026-08-02"}]
    check("只采 1d 时覆盖范围仅含 1d",
          KYLIN_DAILY.partition_extra_values(only_1d) == {"period_type": ["1d"]},
          str(KYLIN_DAILY.partition_extra_values(only_1d)))
    check("月粒度表无附加分区维度",
          SUPPLIER_RATE_SCORE.partition_extra == ()
          and DESTINATION_RATING.partition_extra == ())

    bad_extra = False
    try:
        TableSchema(
            name="bad_extra", comment="附加维度未进唯一键",
            fields=[account_field(),
                    Field("evaluation_month", "VARCHAR(7)", "月份", nullable=False),
                    Field("period_type", "VARCHAR(10)", "周期")],
            unique_key=["account_id", "evaluation_month"],
            partition_column="evaluation_month",
            partition_extra=("period_type",),   # 不在唯一键中
        )
    except ValueError:
        bad_extra = True
    check("拒绝不在唯一键中的附加分区维度", bad_extra)

    # 输出
    width = max(len(name) for _, name, _ in results) + 2
    failed = 0
    print("=" * (width + 30))
    print("表结构单一源自检")
    print("=" * (width + 30))
    for status, name, detail in results:
        if status == FAIL:
            failed += 1
        print(f"[{status}] {name:<{width}}{detail}")
    print("-" * (width + 30))
    print(f"共 {len(results)} 项，失败 {failed} 项")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
