# -*- coding: utf-8 -*-
"""CSV 落地（默认方式）。

多账号隔离
----------
文件路径为 ``data/output/{account_id}/{table}_{run_key}.csv``，
账号之间物理分目录；同时 CSV 内部第一列仍是 ``account_id``，
即使把多个账号的文件合并，也能区分来源。

与原实现的差异
--------------
* 原 ``fetch_daily_kylin.py`` 用追加模式写入，重复运行会不断累积重复行；
  这里默认按批次覆盖写（``run.csv_overwrite``），并在写入前按唯一键去重。
* 原实现写「中文行 + 英文行」双表头，导致文件无法被 pandas / LOAD DATA 直接读取；
  这里默认单行英文表头（可选 ``run.csv_with_label_row`` 追加中文说明行）。
"""

from __future__ import annotations

import csv
import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..config import AppConfig
from ..logging_utils import get_logger
from ..schema import TableSchema
from .base import Sink, WriteResult

logger = get_logger("sink.csv")


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


class CsvSink(Sink):
    name = "csv"

    def __init__(self, cfg: AppConfig, log: logging.Logger | None = None) -> None:
        self.cfg = cfg
        self.log = log or logger

    def target_path(self, table: TableSchema, account_id: str, run_key: str = "") -> Path:
        """CSV 文件路径。

        默认单文件（``{表名}.csv``），使「同一日期重采则覆盖」在 CSV 侧
        也能成立；若开启 ``run.csv_split_by_run_key``，则每个采集区间单独成文件
        （旧行为，适合临时导出给人看）。
        """
        suffix = f"_{run_key}" if (run_key and self.cfg.run.csv_split_by_run_key) else ""
        return self.cfg.output_dir(account_id) / f"{table.name}{suffix}.csv"

    def write(self, table: TableSchema, account_id: str, rows: list[dict[str, Any]],
              *, run_key: str = "", write_mode: str = "") -> WriteResult:
        path = self.target_path(table, account_id, run_key)
        result = WriteResult(sink=self.name, table=table.name, account_id=account_id,
                            rows_in=len(rows), target=str(path))
        columns = table.csv_columns()

        clean_rows, skipped, problems = _prepare(table, account_id, rows)
        result.rows_skipped = skipped
        result.problems = problems

        mode = (write_mode or self.cfg.run.write_mode or "overwrite").lower()
        overwrite_partitions = (
            mode == "overwrite" and table.supports_overwrite()
            and not self.cfg.run.csv_split_by_run_key
        )

        path.parent.mkdir(parents=True, exist_ok=True)
        if overwrite_partitions:
            kept, dropped = self._read_kept_rows(path, table, columns,
                                                 account_id, clean_rows)
            result.rows_deleted = dropped
            result.partitions = len(table.partition_values(clean_rows))
            out_rows = kept + [[_fmt(r.get(c)) for c in columns] for r in clean_rows]
            self._rewrite(path, table, columns, out_rows)
            self.log.info("[%s] CSV 覆盖写入：清理 %d 行旧数据（%s 共 %d 个），"
                          "写入 %d 行（跳过 %d）→ %s",
                          account_id, result.rows_deleted, table.partition_column,
                          result.partitions, len(clean_rows), result.rows_skipped, path)
        else:
            append = not self.cfg.run.csv_overwrite
            need_header = not append or not path.exists() or path.stat().st_size == 0
            with open(path, "a" if append else "w", newline="",
                      encoding=self.cfg.run.csv_encoding) as f:
                writer = csv.writer(f)
                if need_header:
                    if self.cfg.run.csv_with_label_row:
                        writer.writerow(table.csv_labels())
                    writer.writerow(columns)
                for row in clean_rows:
                    writer.writerow([_fmt(row.get(c)) for c in columns])
            self.log.info("[%s] CSV 写入 %d 行（跳过 %d）→ %s",
                          account_id, len(clean_rows), result.rows_skipped, path)

        result.rows_written = len(clean_rows)
        return result

    # -- 分区覆盖辅助 --------------------------------------------------
    def _read_kept_rows(self, path: Path, table: TableSchema, columns: list[str],
                        account_id: str,
                        new_rows: list[dict[str, Any]]) -> tuple[list[list[str]], int]:
        """读出现有文件中【不属于本次覆盖范围】的行。

        判定条件与 MySQL 完全一致（必须一致，否则 CSV 与数据库会出现差异）：
        同一 ``account_id``、分区值在本次采到的集合里、
        且附加维度（如 ``period_type``）也在本次范围内的旧行才会被剔除；
        其他账号、其他日期、其他周期口径的行原样保留。
        """
        if not path.exists() or path.stat().st_size == 0:
            return [], 0

        column = table.partition_column
        targets = {str(v) for v in table.partition_values(new_rows)}
        extra_targets = {
            name: {str(v) for v in values}
            for name, values in table.partition_extra_values(new_rows).items()
        }
        kept: list[list[str]] = []
        dropped = 0

        with open(path, newline="", encoding=self.cfg.run.csv_encoding) as f:
            reader = csv.reader(f)
            header: list[str] | None = None
            for raw in reader:
                if not raw:
                    continue
                if header is None:
                    # 跳过表头（可能包含一行中文说明行）
                    if raw == columns:
                        header = raw
                        continue
                    if raw == table.csv_labels():
                        continue
                    # 表头与当前结构不一致（旧版本字段），无法可靠对齐，
                    # 直接舍弃旧内容重建，否则会造成列错位。
                    self.log.warning("CSV 表头与当前表结构不一致，将重建文件：%s", path)
                    return [], 0
                record = dict(zip(columns, raw))
                same_account = str(record.get("account_id", "")) == str(account_id)
                in_scope = str(record.get(column, "")) in targets
                in_extra = all(
                    str(record.get(name, "")) in values
                    for name, values in extra_targets.items()
                )
                if same_account and in_scope and in_extra:
                    dropped += 1
                    continue
                kept.append(raw)
        return kept, dropped

    def _rewrite(self, path: Path, table: TableSchema, columns: list[str],
                 body: list[list[str]]) -> None:
        with open(path, "w", newline="", encoding=self.cfg.run.csv_encoding) as f:
            writer = csv.writer(f)
            if self.cfg.run.csv_with_label_row:
                writer.writerow(table.csv_labels())
            writer.writerow(columns)
            writer.writerows(body)


def _prepare(table: TableSchema, account_id: str,
             rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, list[str]]:
    """强制 account_id 一致 + 按唯一键去重 + 必填校验。"""
    seen: set[tuple] = set()
    clean: list[dict[str, Any]] = []
    skipped = 0
    problems: list[str] = []

    for row in rows:
        # 兜底保证隔离键正确，防止采集器漏填导致数据串账号
        if "account_id" in table.field_map():
            row = {**row, "account_id": account_id}
        issues = table.validate_row(row)
        if issues:
            skipped += 1
            if len(problems) < 10:
                problems.append("；".join(issues))
            continue
        key = table.row_key(row)
        if key in seen:
            skipped += 1
            continue
        seen.add(key)
        clean.append(row)
    return clean, skipped, problems
