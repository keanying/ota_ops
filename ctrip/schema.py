# -*- coding: utf-8 -*-
"""表结构单一维护点（Single Source of Truth）。

后期需要增删改字段时，**只改本文件**即可，以下产物全部自动同步：

* MySQL 建表 DDL（``TableSchema.ddl()``）
* CSV 表头与列顺序（``TableSchema.column_names()`` / ``column_labels()``）
* MySQL UPSERT 语句（``TableSchema.upsert_sql()``）
* 上游 API 字段 -> 落库字段的映射与类型清洗（``TableSchema.build_row()``）

字段定义说明
------------
``Field.source`` 填写上游接口返回的原始 key（携程指标名可能包含 ``|`` 与 ``/``，
不能直接作为列名，因此这里做一层映射）。若该字段由采集器代码直接计算，
``source`` 留空并在采集器里显式赋值。

``Field.cast`` 决定值清洗方式，取值见 ``CASTERS``。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field as dc_field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable

# =============================================================================
# 值清洗器
# =============================================================================

_NUM_RE = re.compile(r"-?\d+(?:\.\d+)?")


def _blank_to_none(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and value.strip() in ("", "-", "--", "null", "None", "N/A"):
        return None
    return value


def cast_str(value: Any) -> str | None:
    value = _blank_to_none(value)
    if value is None:
        return None
    return str(value).strip()


def cast_int(value: Any) -> int | None:
    """整数清洗：兼容 "1,234"、"1234.0"、"1234张" 等脏格式。"""
    value = _blank_to_none(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int,)):
        return int(value)
    if isinstance(value, float):
        return int(round(value))
    text = str(value).replace(",", "").strip()
    match = _NUM_RE.search(text)
    if not match:
        return None
    return int(round(float(match.group())))


def cast_decimal(value: Any) -> Decimal | None:
    """小数清洗：兼容 "1,234.56"、"56.7%"（百分号按原值/100 折算）。"""
    value = _blank_to_none(value)
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = str(value).replace(",", "").strip()
    is_percent = text.endswith("%")
    match = _NUM_RE.search(text)
    if not match:
        return None
    try:
        num = Decimal(match.group())
    except InvalidOperation:
        return None
    if is_percent:
        num = num / Decimal("100")
    return num


def cast_date(value: Any) -> date | None:
    """日期清洗：接受 date/datetime/"YYYY-MM-DD"/"YYYYMMDD"/"YYYY/MM/DD"。"""
    value = _blank_to_none(value)
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip().replace("/", "-")
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def cast_month(value: Any) -> str | None:
    """月份清洗：统一为 ``YYYY-MM``。"""
    value = _blank_to_none(value)
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.strftime("%Y-%m")
    text = str(value).strip().replace("/", "-")
    m = re.match(r"^(\d{4})-?(\d{1,2})", text)
    if not m:
        return None
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}"


def cast_text(value: Any) -> str | None:
    value = _blank_to_none(value)
    if value is None:
        return None
    return str(value)


def cast_json(value: Any) -> str | None:
    """把 dict/list 序列化为 JSON 字符串存 MySQL JSON / TEXT 列。"""
    import json

    value = _blank_to_none(value)
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


CASTERS: dict[str, Callable[[Any], Any]] = {
    "str": cast_str,
    "int": cast_int,
    "decimal": cast_decimal,
    "date": cast_date,
    "month": cast_month,
    "text": cast_text,
    "json": cast_json,
}


# =============================================================================
# 字段与表定义
# =============================================================================

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Field:
    """单个字段定义。

    :param name:    落库/CSV 列名（必须是合法 SQL 标识符）
    :param type:    MySQL 类型片段，如 ``DECIMAL(15,2)``
    :param comment: 中文注释，同时用作 CSV 中文表头
    :param source:  上游 API 原始字段名；None 表示由采集器直接赋值
    :param cast:    值清洗器名，见 ``CASTERS``
    :param nullable: 是否允许为空
    :param default:  DDL 默认值片段（如 ``''``、``0``），None 表示不写默认值
    :param in_csv:   是否输出到 CSV
    :param upsert:   UPSERT 时是否参与 UPDATE（唯一键列自动排除）
    :param db_managed: 是否由数据库自动维护（如 create_time / update_time）。
        为 True 时只出现在建表 DDL 里，不参与 INSERT 列清单、不进 CSV，
        也不参与行构造，避免程序覆盖数据库的时间戳语义。
    :param extra: 附加在列定义末尾的 DDL 片段，如
        ``ON UPDATE CURRENT_TIMESTAMP``。
    """

    name: str
    type: str
    comment: str = ""
    source: str | None = None
    cast: str = "str"
    nullable: bool = True
    default: str | None = None
    in_csv: bool = True
    upsert: bool = True
    db_managed: bool = False
    extra: str = ""

    def __post_init__(self) -> None:
        if not _IDENT_RE.match(self.name):
            raise ValueError(f"非法字段名: {self.name!r}")
        if self.cast not in CASTERS:
            raise ValueError(f"未知 cast: {self.cast!r}（字段 {self.name}）")

    # -- DDL ---------------------------------------------------------------
    def ddl(self) -> str:
        parts = [f"`{self.name}`", self.type]
        parts.append("NULL" if self.nullable else "NOT NULL")
        if self.default is not None:
            parts.append(f"DEFAULT {self.default}")
        if self.extra:
            parts.append(self.extra)
        if self.comment:
            parts.append(f"COMMENT '{self.comment}'")
        return " ".join(parts)

    def clean(self, value: Any) -> Any:
        return CASTERS[self.cast](value)


@dataclass
class TableSchema:
    """一张表的完整定义。"""

    name: str
    comment: str
    fields: list[Field]
    unique_key: list[str]
    indexes: list[tuple[str, list[str]]] = dc_field(default_factory=list)
    engine: str = "InnoDB"
    charset: str = "utf8mb4"
    # 唯一键索引名。置空时自动为 ``uk_<表名>``。
    unique_key_name: str = ""
    # 时间分区列：覆盖写入（先删后插）时的删除范围依据。
    # 删除条件始终附加 account_id，避免误删其他账号的同期数据。
    # 置空表示该表不按时间覆盖（如账号维表、审计表）。
    partition_column: str = ""
    # 覆盖写入的**附加限定维度**（除 account_id 与分区列之外）。
    #
    # 为何必需：交易表同一个 transaction_date 下共存 1d/7d/30d 三种口径。
    # 若删除只看日期，那么「只采 1d」会把同日的 7d/30d 行一并删掉，
    # 而本次又不会重写它们，造成数据凭空消失（默认 kylin_days=30，
    # 30d 窗口起始日必然落在 1d 区间内，这个冲突是必发而非偶发）。
    partition_extra: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        names = [f.name for f in self.fields]
        dup = {n for n in names if names.count(n) > 1}
        if dup:
            raise ValueError(f"表 {self.name} 存在重复字段: {dup}")
        for key in self.unique_key:
            if key not in names:
                raise ValueError(f"表 {self.name} 唯一键字段 {key} 未定义")
        if names and names[0] != "account_id" and "account_id" in names:
            raise ValueError(f"表 {self.name}: account_id 必须是第一个业务字段以保证多账号隔离语义")
        if "account_id" in names and self.unique_key and self.unique_key[0] != "account_id":
            raise ValueError(f"表 {self.name}: 唯一键必须以 account_id 开头，确保账号间数据隔离")
        if self.partition_column and self.partition_column not in names:
            raise ValueError(f"表 {self.name}: 分区列 {self.partition_column} 未定义")
        for extra in self.partition_extra:
            if extra not in names:
                raise ValueError(f"表 {self.name}: 附加分区维度 {extra} 未定义")
            # 附加维度必须属于唯一键，否则它区分不了行，
            # 用作删除条件只会遗漏本应清理的旧数据
            if self.unique_key and extra not in self.unique_key:
                raise ValueError(
                    f"表 {self.name}: 附加分区维度 {extra} 必须包含在唯一键中"
                )

    # -- 元信息 ------------------------------------------------------------
    def field_map(self) -> dict[str, Field]:
        return {f.name: f for f in self.fields}

    def column_names(self) -> list[str]:
        """参与写入的列（不含数据库自动维护的列）。"""
        return [f.name for f in self.fields if not f.db_managed]

    def all_column_names(self) -> list[str]:
        """包含数据库自动维护列的完整列表（仅用于 DDL 与校验）。"""
        return [f.name for f in self.fields]

    def writable_fields(self) -> list[Field]:
        return [f for f in self.fields if not f.db_managed]

    def csv_columns(self) -> list[str]:
        return [f.name for f in self.fields if f.in_csv and not f.db_managed]

    def csv_labels(self) -> list[str]:
        return [f.comment or f.name for f in self.fields
                if f.in_csv and not f.db_managed]

    def source_map(self) -> dict[str, str]:
        """上游字段名 -> 列名。"""
        return {f.source: f.name for f in self.fields if f.source}

    def uk_name(self) -> str:
        return self.unique_key_name or f"uk_{self.name}"

    def table_name(self, prefix: str = "", account_id: str | None = None) -> str:
        """物理表名。``account_id`` 非空时返回按账号分表的表名。"""
        base = f"{prefix}{self.name}"
        if account_id:
            safe = re.sub(r"[^A-Za-z0-9_]", "_", account_id).lower()
            return f"{base}_{safe}"
        return base

    # -- DDL ---------------------------------------------------------------
    def ddl(self, prefix: str = "", account_id: str | None = None,
            if_not_exists: bool = True) -> str:
        table = self.table_name(prefix, account_id)
        lines = ["  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键'"]
        lines += [f"  {f.ddl()}" for f in self.fields]
        lines.append("  PRIMARY KEY (`id`)")
        if self.unique_key:
            cols = ", ".join(f"`{c}`" for c in self.unique_key)
            lines.append(f"  UNIQUE KEY `{self.uk_name()}` ({cols})")
        for idx_name, cols in self.indexes:
            col_sql = ", ".join(f"`{c}`" for c in cols)
            lines.append(f"  KEY `{idx_name}` ({col_sql})")
        exists = "IF NOT EXISTS " if if_not_exists else ""
        body = ",\n".join(lines)
        return (
            f"CREATE TABLE {exists}`{table}` (\n{body}\n) "
            f"ENGINE={self.engine} DEFAULT CHARSET={self.charset} "
            f"COMMENT='{self.comment}';"
        )

    # -- DML ---------------------------------------------------------------
    def insert_sql(self, prefix: str = "", account_id: str | None = None) -> str:
        table = self.table_name(prefix, account_id)
        cols = self.column_names()
        col_sql = ", ".join(f"`{c}`" for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        return f"INSERT INTO `{table}` ({col_sql}) VALUES ({placeholders})"

    def upsert_sql(self, prefix: str = "", account_id: str | None = None) -> str:
        """INSERT ... ON DUPLICATE KEY UPDATE，参数化，天然防注入。"""
        table = self.table_name(prefix, account_id)
        cols = self.column_names()
        col_sql = ", ".join(f"`{c}`" for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        updatable = [
            f.name for f in self.writable_fields()
            if f.upsert and f.name not in self.unique_key
        ]
        # update_time 已由 DDL 的 ON UPDATE CURRENT_TIMESTAMP 自动维护，
        # 但仅当行内容真的发生变化时才会触发；这里显式赋值以保证
        # 「每次 UPSERT 命中都能看到最新处理时间」。
        auto_update = [f.name for f in self.fields
                       if f.db_managed and "ON UPDATE" in f.extra.upper()]
        clauses = [f"`{c}` = VALUES(`{c}`)" for c in updatable]
        clauses += [f"`{c}` = CURRENT_TIMESTAMP" for c in auto_update]
        if not clauses:
            # 退化写法：保证 ON DUPLICATE KEY UPDATE 子句非空
            first = self.unique_key[0] if self.unique_key else self.column_names()[0]
            clauses = [f"`{first}` = `{first}`"]
        return (
            f"INSERT INTO `{table}` ({col_sql}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {', '.join(clauses)}"
        )

    # -- 行构造 ------------------------------------------------------------
    def build_row(self, raw: dict[str, Any], extra: dict[str, Any] | None = None) -> dict[str, Any]:
        """把上游原始 dict 转成符合本表结构的一行。

        取值优先级：``extra[列名]`` > ``raw[列名]`` > ``raw[source]``。
        所有值都会经过对应 ``cast`` 清洗。
        """
        extra = extra or {}
        row: dict[str, Any] = {}
        for f in self.writable_fields():
            if f.name in extra:
                value = extra[f.name]
            elif f.name in raw:
                value = raw[f.name]
            elif f.source is not None and f.source in raw:
                value = raw[f.source]
            else:
                value = None
            row[f.name] = f.clean(value)
        return row

    def validate_row(self, row: dict[str, Any]) -> list[str]:
        """返回校验问题列表（空列表表示通过）。"""
        problems: list[str] = []
        for f in self.writable_fields():
            if not f.nullable and row.get(f.name) in (None, ""):
                problems.append(f"字段 {f.name}({f.comment}) 不允许为空")
        for key in self.unique_key:
            if row.get(key) in (None, ""):
                problems.append(f"唯一键字段 {key} 为空，无法保证幂等")
        return problems

    def row_values(self, row: dict[str, Any]) -> list[Any]:
        return [row.get(c) for c in self.column_names()]

    def row_key(self, row: dict[str, Any]) -> tuple:
        return tuple(str(row.get(k)) for k in self.unique_key)

    # -- 覆盖写入辅助 --------------------------------------------------
    def supports_overwrite(self) -> bool:
        """是否可按时间分区覆盖。"""
        return bool(self.partition_column) and "account_id" in self.field_map()

    def partition_values(self, rows: Iterable[dict[str, Any]]) -> list[Any]:
        """从待写入的行中提取本次涉及的分区值（去重升序）。

        覆盖范围按【实际采到的数据】而非【请求区间】确定：
        若某天本次没采到（接口失败或上游无数据），就不动它的旧数据，
        否则会出现「旧数据删了、新数据又没来」的数据空洞。
        """
        if not self.partition_column:
            return []
        seen: set[Any] = set()
        for row in rows:
            value = row.get(self.partition_column)
            if value is not None and value != "":
                seen.add(value)
        return sorted(seen, key=lambda v: str(v))

    def partition_extra_values(
            self, rows: Iterable[dict[str, Any]]) -> dict[str, list[Any]]:
        """提取附加分区维度本次实际出现的值（各自去重升序）。

        与 ``partition_values`` 同理，只限定本次真实采到的值：
        只采 1d 时删除条件仅含 ``period_type='1d'``，
        同日的 7d/30d 旧数据不会被误删。
        """
        if not self.partition_extra:
            return {}
        out: dict[str, list[Any]] = {}
        for column in self.partition_extra:
            seen: set[Any] = set()
            for row in rows:
                value = row.get(column)
                if value is not None and value != "":
                    seen.add(value)
            if seen:
                out[column] = sorted(seen, key=lambda v: str(v))
        return out


# =============================================================================
# 公共字段：多账号隔离基石
# =============================================================================

def account_field() -> Field:
    """所有业务表的第一个字段，且必须位于唯一键首位。"""
    return Field(
        name="account_id",
        type="VARCHAR(50)",
        comment="账号标识（多账号隔离键）",
        cast="str",
        nullable=False,
        upsert=False,
    )


def collect_time_field() -> Field:
    """采集时间。

    虽然 DDL 里带有 ``DEFAULT CURRENT_TIMESTAMP``，但它仍由程序显式写入，
    因为它表达的是「本次采集发生的时刻」这个业务语义，
    需要与 CSV 输出保持一致，不能交给数据库的入库时间代替。
    """
    return Field(
        name="collect_time",
        type="DATETIME",
        comment="采集时间",
        cast="str",
        nullable=False,
        default="CURRENT_TIMESTAMP",
    )


def audit_fields() -> list[Field]:
    """数据库自动维护的审计时间字段。

    标记为 ``db_managed=True``，因此：只出现在建表 DDL；不进 INSERT 列清单；
    不进 CSV 表头；不参与行构造。这样程序不会覆盖数据库的时间戳语义。
    """
    return [
        Field(name="create_time", type="DATETIME", comment="创建时间",
              nullable=False, default="CURRENT_TIMESTAMP",
              db_managed=True, in_csv=False, upsert=False),
        Field(name="update_time", type="DATETIME", comment="更新时间",
              nullable=False, default="CURRENT_TIMESTAMP",
              extra="ON UPDATE CURRENT_TIMESTAMP",
              db_managed=True, in_csv=False, upsert=False),
    ]


def scenic_field() -> Field:
    """景区 ID（如 PFTSCA01018088）。

    绑定粒度为**账号级**：一个采集账号对应一个景区 ID，
    由 ``AccountConfig.scenic_id`` 配置，采集时由 ``BaseCollector`` 统一注入每一行。

    不纳入唯一键：账号与景区是一对一关系，``account_id`` 已能唯一定位记录；
    若景区 ID 进唯一键，日后修正绑定关系时会产生重复行而不是原地更新。
    它参与 UPSERT UPDATE，因此改完配置重跑即可回填历史数据。
    """
    return Field(
        name="scenic_id",
        type="VARCHAR(50)",
        comment="景区ID（如 PFTSCA01018088，账号级绑定）",
        cast="str",
        nullable=True,
    )


# =============================================================================
# 业务表定义
# =============================================================================

KYLIN_DAILY = TableSchema(
    name="ads_sup_ota_ctrip_trade_di",
    comment="携程交易卡片每日数据",
    # period_type 必须进唯一键：同一 transaction_date 下 1d/7d/30d 三种周期各是一条独立记录，
    # 不进唯一键会使三种周期互相覆盖，最终只剩下最后写入的那一条。
    unique_key=["account_id", "transaction_date", "period_type",
                "signing_code", "biz_type", "date_type"],
    partition_column="transaction_date",   # 覆盖写入按交易日期分区
    # 同一日期下共存 1d/7d/30d 三种口径，覆盖时必须同时限定 period_type，
    # 否则只采其中一种会把同日其他口径的行误删
    partition_extra=("period_type",),
    indexes=[
        ("idx_kylin_date", ["transaction_date"]),
        ("idx_kylin_account_date", ["account_id", "transaction_date"]),
        ("idx_kylin_scenic_date", ["scenic_id", "transaction_date"]),
        ("idx_kylin_period", ["account_id", "period_type", "transaction_date"]),
    ],
    fields=[
        account_field(),
        scenic_field(),
        Field("transaction_date", "DATE", "交易日期", source="date_start", cast="date",
              nullable=False, upsert=False),
        Field("signing_code", "VARCHAR(20)", "签约类型编码", source="code", cast="str",
              nullable=False, upsert=False),
        Field("signing_type", "VARCHAR(50)", "签约类型名称", source="signing_type", cast="str",
              nullable=False, default="''"),
        Field("biz_type", "VARCHAR(50)", "业务类型", source="vbk_bu_type", cast="str",
              nullable=False, default="''", upsert=False),
        Field("date_type", "VARCHAR(20)", "日期类型（使用日期/预订日期）", source="date_type",
              cast="str", nullable=False, upsert=False),
        # 周期类型：1d=单日明细；7d=近 7 日聚合；30d=近 30 日聚合。
        # 7d/30d 的 transaction_date 存区间【起始日】（与携程接口返回的 key 一致），
        # 必须配合 period_type 才能唯一定位一个聚合区间。
        Field("period_type", "VARCHAR(10)", "周期类型（1d:昨日 7d:近7日 30d:近30日）",
              source="period_type", cast="str", nullable=False, default="'1d'", upsert=False),
        Field("turnover", "DECIMAL(18,2)", "成交销售额（元）",
              source="ttd_suc_nocoef_income", cast="decimal"),
        Field("turnover_mom", "DECIMAL(12,6)", "成交销售额环比",
              source="ttd_suc_nocoef_income_MOMRate", cast="decimal"),
        Field("order_count", "BIGINT", "订单数（笔）",
              source="ttd_suc_ord_cnt", cast="int"),
        Field("order_count_mom", "DECIMAL(12,6)", "订单数环比",
              source="ttd_suc_ord_cnt_MOMRate", cast="decimal"),
        Field("sold_ticket_cnt", "BIGINT", "成交票量（张）",
              source="ttd_suc_nocoef_qty", cast="int"),
        Field("sold_ticket_mom", "DECIMAL(12,6)", "成交票量环比",
              source="ttd_suc_nocoef_qty_MOMRate", cast="decimal"),
        Field("validated_ticket_volume", "BIGINT", "核验票量（张）",
              source="ttd_chk_qty", cast="int"),
        Field("validated_ticket_mom", "DECIMAL(12,6)", "核验票量环比",
              source="ttd_chk_qty_MOMRate", cast="decimal"),
        Field("refund_rate", "DECIMAL(12,6)", "退订率",
              source="ttd_arfd_ord_cnt|ttd_pay_ord_cnt|/", cast="decimal"),
        Field("refund_rate_mom", "DECIMAL(12,6)", "退订率环比",
              source="ttd_arfd_ord_cnt|ttd_pay_ord_cnt|/_MOMRate", cast="decimal"),
        # 成交资源数：与上面 5 项不同一个请求（queryId=354 + aggregateType=distinct），
        # 因此采集器需单独发一轮，详见 collectors/kylin_daily.py 的 RESOURCE_METRIC。
        Field("deal_resource_count", "BIGINT", "成交资源数（去重资源ID数）",
              source="suc_resourceid", cast="int"),
        Field("deal_resource_count_mom", "DECIMAL(12,6)", "成交资源数环比",
              source="suc_resourceid_MOMRate", cast="decimal"),
        collect_time_field(),
        *audit_fields(),
    ],
)


DESTINATION_RATING = TableSchema(
    name="ads_sup_ota_ctrip_destination_rating_di",
    comment="携程目的地评级",
    unique_key=["account_id", "evaluation_month", "region_id"],
    unique_key_name="uk_ctrip_destination_rating",
    partition_column="evaluation_month",   # 覆盖写入按评价月份分区
    indexes=[
        ("idx_rating_month", ["evaluation_month"]),
        ("idx_rating_scenic", ["scenic_id", "evaluation_month"]),
    ],
    fields=[
        account_field(),
        scenic_field(),
        Field("evaluation_month", "VARCHAR(7)", "考核月份（YYYY-MM）", source="month",
              cast="month", nullable=False, upsert=False),
        Field("region_id", "VARCHAR(20)", "目的地ID（账号级汇总时为 ALL）", source="region_id",
              cast="str", nullable=False, default="'ALL'", upsert=False),
        Field("region_name", "VARCHAR(50)", "目的地名称", source="region_name", cast="str",
              nullable=False, default="''"),
        Field("region_type", "VARCHAR(10)", "目的地类型", source="region_type", cast="str"),
        Field("analysis_text", "MEDIUMTEXT", "AI 分析原文", source="message", cast="text"),
        Field("analysis_text_len", "INT", "分析原文字符数", cast="int"),
        collect_time_field(),
        *audit_fields(),
    ],
)


SUPPLIER_RATE_SCORE = TableSchema(
    name="ads_sup_ota_ctrip_score_di",
    comment="携程供应商评级雷达图数据",
    unique_key=["account_id", "evaluation_month", "region_id"],
    unique_key_name="uk_ads_sup_ota_ctrip_score_di",
    partition_column="evaluation_month",   # 覆盖写入按评价月份分区
    indexes=[
        ("idx_score_month", ["evaluation_month"]),
        ("idx_score_region", ["region_id"]),
        ("idx_score_scenic", ["scenic_id", "evaluation_month"]),
    ],
    fields=[
        account_field(),
        scenic_field(),
        Field("evaluation_month", "VARCHAR(7)", "月份（YYYY-MM）", source="month", cast="month",
              nullable=False, upsert=False),
        Field("region_id", "VARCHAR(20)", "目的地ID", source="regionId", cast="str",
              nullable=False, upsert=False),
        Field("region_name", "VARCHAR(50)", "目的地名称", source="regionName", cast="str",
              nullable=False, default="''"),
        Field("region_type", "VARCHAR(10)", "目的地类型", source="regionType", cast="str"),
        Field("overall_service_score", "DECIMAL(8,2)", "综合服务分",
              source="serviceScore", cast="decimal"),
        Field("ocr_score", "DECIMAL(8,2)", "订单确认率评分",
              source="orderConfirmationRate", cast="decimal"),
        Field("fdr_score", "DECIMAL(8,2)", "履约缺陷率评分",
              source="fulfillmentDefectRate", cast="decimal"),
        Field("manual_intervention_rate_score", "DECIMAL(8,2)", "客服人工介入率评分",
              source="customerServiceManualInterventionRate", cast="decimal"),
        Field("timely_response_20s_score", "DECIMAL(8,2)", "20秒回复及时率评分",
              source="20secondsResponseTimelinessRate", cast="decimal"),
        Field("im_neg_rate_score", "DECIMAL(8,2)", "IM差评率评分",
              source="IMNegativeReviewRate", cast="decimal"),
        collect_time_field(),
        *audit_fields(),
    ],
)


ACCOUNT_DIM = TableSchema(
    name="ads_service_ota_ctrip_account",
    comment="携程采集账号维表（不存储密码）",
    unique_key=["account_id"],
    unique_key_name="uk_ctrip_account",
    indexes=[
        ("idx_account_status", ["status"]),
        ("idx_account_scenic", ["scenic_id"]),
    ],
    fields=[
        account_field(),
        scenic_field(),
        Field("account_name", "VARCHAR(100)", "账号备注名", source="name", cast="str",
              nullable=False, default="''"),
        Field("login_name", "VARCHAR(100)", "登录用户名", source="account", cast="str",
              nullable=False, default="''"),
        Field("vendor_id", "VARCHAR(50)", "携程侧供应商ID（身份指纹）", cast="str"),
        Field("status", "VARCHAR(20)", "账号状态（active/disabled）", cast="str",
              nullable=False, default="'active'"),
        Field("last_login_at", "DATETIME", "最近登录成功时间", cast="str"),
        Field("remark", "VARCHAR(255)", "备注", cast="str"),
        *audit_fields(),
    ],
)


COLLECT_RUN_LOG = TableSchema(
    name="ads_service_ota_ctrip_run_log",
    comment="采集运行审计日志（按账号+任务+批次）",
    unique_key=["account_id", "job", "run_key"],
    unique_key_name="uk_ctrip_collect_run_log",
    indexes=[
        ("idx_log_started", ["started_at"]),
        ("idx_log_status", ["status"]),
    ],
    fields=[
        account_field(),
        scenic_field(),
        Field("job", "VARCHAR(30)", "任务名（kylin/rating/score）", cast="str",
              nullable=False, upsert=False),
        Field("run_key", "VARCHAR(60)", "批次标识（日期或月份区间）", cast="str",
              nullable=False, default="''", upsert=False),
        Field("started_at", "DATETIME", "开始时间", cast="str"),
        Field("finished_at", "DATETIME", "结束时间", cast="str"),
        Field("duration_ms", "INT", "耗时（毫秒）", cast="int"),
        Field("status", "VARCHAR(20)", "状态（success/partial/failed）", cast="str",
              nullable=False, default="''"),
        Field("rows_fetched", "INT", "采集条数", cast="int"),
        Field("rows_written", "INT", "写入条数", cast="int"),
        Field("sink", "VARCHAR(30)", "落地方式（csv/mysql/both）", cast="str"),
        Field("error", "VARCHAR(1000)", "错误摘要", cast="str"),
        *audit_fields(),
    ],
)


# 表注册中心：新增表只需在此登记
TABLES: dict[str, TableSchema] = {
    "kylin_daily": KYLIN_DAILY,
    "destination_rating": DESTINATION_RATING,
    "supplier_rate_score": SUPPLIER_RATE_SCORE,
    "account_dim": ACCOUNT_DIM,
    "collect_run_log": COLLECT_RUN_LOG,
}

# 业务数据表（会按账号分表策略处理），维表/日志表始终共享
BUSINESS_TABLES = ("kylin_daily", "destination_rating", "supplier_rate_score")
SHARED_ONLY_TABLES = ("account_dim", "collect_run_log")


def get_table(key: str) -> TableSchema:
    if key not in TABLES:
        raise KeyError(f"未注册的表: {key}，可用: {sorted(TABLES)}")
    return TABLES[key]


def all_ddl(prefix: str = "", account_ids: Iterable[str] | None = None,
            table_strategy: str = "shared", if_not_exists: bool = True) -> str:
    """生成全部建表语句。

    :param table_strategy: ``shared`` 共享表（推荐）；``per_account`` 按账号分表
    """
    chunks: list[str] = [
        "-- 本文件由 ctrip/schema.py 自动生成，请勿手工修改；",
        "-- 修改字段请编辑 ctrip/schema.py 后执行: python -m ctrip.cli show-ddl --write",
        "",
    ]
    for key, table in TABLES.items():
        if table_strategy == "per_account" and key in BUSINESS_TABLES and account_ids:
            for account_id in account_ids:
                chunks.append(table.ddl(prefix, account_id, if_not_exists))
                chunks.append("")
        else:
            chunks.append(table.ddl(prefix, None, if_not_exists))
            chunks.append("")
    return "\n".join(chunks)
