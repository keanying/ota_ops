# -*- coding: utf-8 -*-
"""采集器基类。

每个采集器实例只服务**一个账号**，持有该账号独占的 API 客户端与 logger，
产出的每一行都会被强制打上该账号的 ``account_id``，这是多账号隔离的最后一道保险。

同时会注入该账号绑定的 ``scenic_id``（景区 ID，如 PFTSCA01018088）。
绑定粒度为账号级，因此与 ``account_id`` 一同在此统一处理，
各采集器无需关心景区字段，日后新增采集任务也会自动带上。
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from ..api import CtripApi
from ..config import AppConfig
from ..daterange import resolve_month_range
from ..schema import TableSchema
from ..timeutil import get_timezone


@dataclass
class CollectResult:
    job: str
    account_id: str
    table: TableSchema
    rows: list[dict[str, Any]] = field(default_factory=list)
    run_key: str = ""
    warnings: list[str] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.rows)


class BaseCollector(ABC):
    """采集器基类。"""

    job: str = "base"
    table_key: str = ""

    def __init__(self, cfg: AppConfig, api: CtripApi, account_id: str,
                 log: logging.Logger, scenic_id: str = "") -> None:
        self.cfg = cfg
        self.api = api
        self.account_id = account_id
        self.scenic_id = scenic_id or ""
        self.log = log
        self.tz = get_timezone(cfg.run.timezone, log=log)

    # -- 时间工具（统一时区，避免服务器 UTC 差一天） -----------------------
    def today(self) -> date:
        return datetime.now(self.tz).date()

    def now_str(self) -> str:
        return datetime.now(self.tz).strftime("%Y-%m-%d %H:%M:%S")

    def last_month(self) -> str:
        today = self.today()
        year, month = today.year, today.month - 1
        if month <= 0:
            month, year = 12, year - 1
        return f"{year:04d}-{month:02d}"

    def months_to_collect(self, month: str | None = None,
                          months: list[str] | None = None,
                          start_month: str | None = None,
                          end_month: str | None = None) -> list[str]:
        """统一推导月粒度任务要采的月份列表（升序去重，默认上个月）。"""
        return resolve_month_range(today=self.today(), start_month=start_month,
                                   end_month=end_month, month=month, months=months)

    # -- 行构造：强制注入 account_id 与 scenic_id ------------------------------
    def build_row(self, table: TableSchema, raw: dict[str, Any],
                  extra: dict[str, Any] | None = None) -> dict[str, Any]:
        merged: dict[str, Any] = {
            "account_id": self.account_id,
            "collect_time": self.now_str(),
        }
        # 只在表结构声明了 scenic_id 时注入，避免污染不含该字段的表
        if "scenic_id" in table.field_map():
            merged["scenic_id"] = self.scenic_id or None
        if extra:
            merged.update(extra)
        return table.build_row(raw, merged)

    @abstractmethod
    def collect(self, **kwargs: Any) -> CollectResult:
        """执行采集，返回已清洗好的行。"""
