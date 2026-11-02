# -*- coding: utf-8 -*-
"""数据落地抽象层。

调用方（采集器）只面向 ``Sink`` 接口，不关心最终写 CSV 还是 MySQL。
新增落地方式（如 Kafka、Parquet）只需实现本接口并在 ``sinks/__init__.py`` 注册。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from ..schema import TableSchema


@dataclass
class WriteResult:
    sink: str
    table: str
    account_id: str
    rows_in: int = 0
    rows_written: int = 0
    rows_skipped: int = 0
    rows_deleted: int = 0          # 覆盖模式下清理的旧数据行数
    partitions: int = 0            # 本次覆盖涉及的分区（日期/月份）个数
    target: str = ""
    problems: list[str] = field(default_factory=list)

    def merge(self, other: "WriteResult") -> "WriteResult":
        return WriteResult(
            sink=f"{self.sink}+{other.sink}",
            table=self.table,
            account_id=self.account_id,
            rows_in=max(self.rows_in, other.rows_in),
            rows_written=self.rows_written + other.rows_written,
            rows_skipped=self.rows_skipped + other.rows_skipped,
            rows_deleted=self.rows_deleted + other.rows_deleted,
            partitions=max(self.partitions, other.partitions),
            target="; ".join(x for x in (self.target, other.target) if x),
            problems=self.problems + other.problems,
        )


class Sink(ABC):
    """落地目标接口。

    ``write_mode`` 控制同一时间分区已有数据时的处理方式：

    * ``overwrite``（默认）：先删除本次采到的分区旧数据，再写入新数据。
      能真正做到「以本次采集结果为准」，连上游下线的维度也会被清掉。
    * ``upsert``：仅按唯一键原地更新，不删除任何行。

    两种模式都是幂等的；区别在于能否清除「新数据里已不存在的旧行」。
    """

    name: str = "base"

    @abstractmethod
    def write(self, table: TableSchema, account_id: str, rows: list[dict[str, Any]],
              *, run_key: str = "", write_mode: str = "") -> WriteResult:
        """写入一批已经过 ``TableSchema.build_row`` 清洗的行。"""

    def close(self) -> None:  # pragma: no cover - 默认无需清理
        return None

    def __enter__(self) -> "Sink":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class MultiSink(Sink):
    """把同一批数据写入多个目标（sink=both 时使用）。"""

    name = "multi"

    def __init__(self, sinks: list[Sink]) -> None:
        self.sinks = sinks
        self.name = "+".join(s.name for s in sinks) or "none"

    def write(self, table: TableSchema, account_id: str, rows: list[dict[str, Any]],
              *, run_key: str = "", write_mode: str = "") -> WriteResult:
        result: WriteResult | None = None
        for sink in self.sinks:
            current = sink.write(table, account_id, rows,
                                 run_key=run_key, write_mode=write_mode)
            result = current if result is None else result.merge(current)
        return result or WriteResult(sink="none", table=table.name, account_id=account_id)

    def close(self) -> None:
        for sink in self.sinks:
            sink.close()
