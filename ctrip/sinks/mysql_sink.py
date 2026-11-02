# -*- coding: utf-8 -*-
"""MySQL 落地（配置后启用）。

要点
----
* 全参数化 ``INSERT ... ON DUPLICATE KEY UPDATE``，杜绝字符串拼 SQL 的注入风险
  （原 ``scenic_supplier.py`` 就是拼接式，存在注入与转义隐患）。
* 唯一键以 ``account_id`` 开头，因此 UPSERT 在账号维度天然隔离，
  不同账号的同一天数据不会互相覆盖。
* 支持 ``table_strategy=per_account``：每个账号写独立物理表，强隔离场景可用。
* 建表 DDL 与 UPSERT 语句都来自 ``ctrip/schema.py``，改字段只需改一处。
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from ..config import AppConfig
from ..logging_utils import get_logger
from ..schema import (BUSINESS_TABLES, SHARED_ONLY_TABLES, TABLES, TableSchema)
from .base import Sink, WriteResult

logger = get_logger("sink.mysql")

BATCH_SIZE = 500


class MySQLSink(Sink):
    name = "mysql"

    def __init__(self, cfg: AppConfig, log: logging.Logger | None = None) -> None:
        self.cfg = cfg
        self.log = log or logger
        self._conn = None

    # -- 连接管理 ----------------------------------------------------------
    def _connect(self, *, with_database: bool = True):
        import pymysql

        mc = self.cfg.mysql
        kwargs: dict[str, Any] = dict(
            host=mc.host, port=mc.port, user=mc.user, password=mc.password,
            charset=mc.charset, connect_timeout=mc.connect_timeout, autocommit=False,
        )
        if with_database:
            kwargs["database"] = mc.database
        return pymysql.connect(**kwargs)

    @property
    def conn(self):
        if self._conn is None:
            self._conn = self._connect()
            self.log.info("MySQL 已连接：%s", self.cfg.mysql.dsn_safe())
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def ping(self) -> str:
        with self.conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            version = cur.fetchone()[0]
        return version

    # -- 表名解析（账号隔离策略） -------------------------------------------
    def _account_for_table(self, key: str, account_id: str) -> str | None:
        if self.cfg.mysql.table_strategy != "per_account":
            return None
        return None if key in SHARED_ONLY_TABLES else account_id

    def _table_key(self, table: TableSchema) -> str:
        for key, value in TABLES.items():
            if value is table or value.name == table.name:
                return key
        return table.name

    def physical_table(self, table: TableSchema, account_id: str) -> str:
        key = self._table_key(table)
        return table.table_name(self.cfg.mysql.table_prefix,
                                self._account_for_table(key, account_id))

    # -- 初始化 ------------------------------------------------------------
    def init_database(self, *, create_database: bool = True,
                      account_ids: Iterable[str] | None = None,
                      drop_create: bool = False) -> list[str]:
        """建库建表。返回已执行的表名列表。"""
        mc = self.cfg.mysql
        executed: list[str] = []

        if create_database:
            conn = self._connect(with_database=False)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"CREATE DATABASE IF NOT EXISTS `{mc.database}` "
                        f"DEFAULT CHARACTER SET {mc.charset} COLLATE {mc.charset}_general_ci"
                    )
                conn.commit()
                self.log.info("数据库已就绪：%s", mc.database)
            finally:
                conn.close()

        account_ids = list(account_ids or [])
        with self.conn.cursor() as cur:
            for key, table in TABLES.items():
                targets: list[str | None]
                if mc.table_strategy == "per_account" and key in BUSINESS_TABLES and account_ids:
                    targets = list(account_ids)
                else:
                    targets = [None]
                for account_id in targets:
                    physical = table.table_name(mc.table_prefix, account_id)
                    if drop_create:
                        cur.execute(f"DROP TABLE IF EXISTS `{physical}`")
                    cur.execute(table.ddl(mc.table_prefix, account_id, if_not_exists=True))
                    executed.append(physical)
                    self.log.info("表已就绪：%s", physical)
        self.conn.commit()
        return executed

    # -- 写入 --------------------------------------------------------------
    def write(self, table: TableSchema, account_id: str, rows: list[dict[str, Any]],
              *, run_key: str = "", write_mode: str = "") -> WriteResult:
        physical = self.physical_table(table, account_id)
        result = WriteResult(sink=self.name, table=table.name, account_id=account_id,
                            rows_in=len(rows),
                            target=f"{self.cfg.mysql.database}.{physical}")
        if not rows:
            return result

        seen: set[tuple] = set()
        clean_rows: list[dict[str, Any]] = []
        payload: list[list[Any]] = []
        for row in rows:
            if "account_id" in table.field_map():
                row = {**row, "account_id": account_id}
            issues = table.validate_row(row)
            if issues:
                result.rows_skipped += 1
                if len(result.problems) < 10:
                    result.problems.append("；".join(issues))
                continue
            key = table.row_key(row)
            if key in seen:
                result.rows_skipped += 1
                continue
            seen.add(key)
            clean_rows.append(row)
            payload.append(table.row_values(row))

        if not payload:
            return result

        mode = (write_mode or self.cfg.run.write_mode or "overwrite").lower()
        suffix = self._account_for_table(self._table_key(table), account_id)
        sql = table.upsert_sql(self.cfg.mysql.table_prefix, suffix)

        # 覆盖模式：先删除本次采到的分区旧数据，再插入。
        # 删、插在同一事务里，中途失败会整体回滚，
        # 不会出现「旧数据已删、新数据未写」的中间态。
        partitions: list[Any] = []
        extra_scope: dict[str, list[Any]] = {}
        if mode == "overwrite" and table.supports_overwrite():
            partitions = table.partition_values(clean_rows)
            extra_scope = table.partition_extra_values(clean_rows)
            result.partitions = len(partitions)

        try:
            with self.conn.cursor() as cur:
                if partitions:
                    result.rows_deleted = self._delete_partitions(
                        cur, table, account_id, partitions, suffix, extra_scope)
                for i in range(0, len(payload), BATCH_SIZE):
                    cur.executemany(sql, payload[i:i + BATCH_SIZE])
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        result.rows_written = len(payload)
        if partitions:
            self.log.info("[%s] MySQL 覆盖写入：清理 %d 行旧数据（%s 共 %d 个），"
                          "写入 %d 行（跳过 %d）→ %s",
                          account_id, result.rows_deleted, table.partition_column,
                          len(partitions), result.rows_written, result.rows_skipped,
                          result.target)
        else:
            self.log.info("[%s] MySQL UPSERT %d 行（跳过 %d）→ %s",
                          account_id, result.rows_written, result.rows_skipped, result.target)
        return result

    def _delete_partitions(self, cur, table: TableSchema, account_id: str,
                           partitions: list[Any], suffix: str | None,
                           extra_scope: dict[str, list[Any]] | None = None) -> int:
        """删除指定账号在指定分区上的旧数据。

        删除范围由三部分共同限定，缺一不可：

        1. ``account_id``：否则清理 A001 的某个月会把 A002 的同月数据
           一并删掉，这是多账号场景下最危险的错误；
        2. 分区列（如 ``transaction_date``）；
        3. ``partition_extra`` 附加维度（如 ``period_type``）：同一日期下
           共存 1d/7d/30d 三种口径，不限定就会在只采一种时
           把其他口径的行误删而且不会补回。
        """
        physical = table.table_name(self.cfg.mysql.table_prefix, suffix)
        column = table.partition_column
        deleted = 0

        extra_sql = ""
        extra_params: list[Any] = []
        for name, values in (extra_scope or {}).items():
            placeholders = ", ".join(["%s"] * len(values))
            extra_sql += f" AND `{name}` IN ({placeholders})"
            extra_params.extend(values)

        # IN 列表可能很长（回溯两年会有 700+ 个日期），分块避免单条 SQL 过大
        for i in range(0, len(partitions), BATCH_SIZE):
            chunk = partitions[i:i + BATCH_SIZE]
            placeholders = ", ".join(["%s"] * len(chunk))
            sql = (f"DELETE FROM `{physical}` WHERE `account_id` = %s "
                   f"AND `{column}` IN ({placeholders}){extra_sql}")
            cur.execute(sql, [account_id, *chunk, *extra_params])
            deleted += cur.rowcount or 0
        return deleted

    # -- 审计日志 ----------------------------------------------------------
    def write_run_log(self, rows: list[dict[str, Any]]) -> int:
        if not self.cfg.mysql.write_run_log or not rows:
            return 0
        table = TABLES["collect_run_log"]
        total = 0
        for row in rows:
            account_id = str(row.get("account_id") or "")
            built = table.build_row(row)
            total += self.write(table, account_id, [built]).rows_written
        return total

    def upsert_accounts(self, rows: list[dict[str, Any]]) -> int:
        """写入账号维表（不含密码）。"""
        table = TABLES["account_dim"]
        total = 0
        for row in rows:
            account_id = str(row.get("account_id") or "")
            total += self.write(table, account_id, [table.build_row(row)]).rows_written
        return total

    # -- 查询辅助（用于回验展示） -------------------------------------------
    def query(self, sql: str, params: tuple | None = None) -> list[tuple]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
            return list(cur.fetchall())
