# -*- coding: utf-8 -*-
"""采集器注册中心。新增采集任务只需实现 BaseCollector 并在此登记。"""

from __future__ import annotations

from .base import BaseCollector, CollectResult
from .destination_rating import DestinationRatingCollector
from .kylin_daily import KylinDailyCollector
from .supplier_rate_score import SupplierRateScoreCollector

COLLECTORS: dict[str, type[BaseCollector]] = {
    KylinDailyCollector.job: KylinDailyCollector,
    DestinationRatingCollector.job: DestinationRatingCollector,
    SupplierRateScoreCollector.job: SupplierRateScoreCollector,
}

ALL_JOBS = tuple(COLLECTORS)

__all__ = [
    "BaseCollector", "CollectResult", "COLLECTORS", "ALL_JOBS",
    "KylinDailyCollector", "DestinationRatingCollector", "SupplierRateScoreCollector",
]


def get_collector(job: str) -> type[BaseCollector]:
    if job not in COLLECTORS:
        raise KeyError(f"未知任务：{job}，可选 {', '.join(ALL_JOBS)}")
    return COLLECTORS[job]
