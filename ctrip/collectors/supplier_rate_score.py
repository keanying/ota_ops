# -*- coding: utf-8 -*-
"""供应商评级雷达图采集（服务分 + 5 项子指标）。

对应原 ``fetch_supplier_rate_score.py``，主要修复：

* 原 CSV 用中文表头，无法直接入库、也与数据库列名不一致；
  现在统一由 schema 提供英文列名（中文注释仍在 schema 与文档里维护）。
* 原脚本丢弃了 ``region_id``，只写目的地名称，导致唯一键无法稳定；
  现在同时保留 ``region_id`` 与 ``region_name``，唯一键为
  ``(account_id, evaluation_month, region_id)``。
* 支持采集多个月份（``--months 2026-06,2026-07``），便于历史回补。
"""

from __future__ import annotations

from typing import Any

from ..api import ApiError, SessionExpired
from ..schema import SUPPLIER_RATE_SCORE
from .base import BaseCollector, CollectResult

RADAR_METRICS = (
    "orderConfirmationRate",
    "fulfillmentDefectRate",
    "customerServiceManualInterventionRate",
    "20secondsResponseTimelinessRate",
    "IMNegativeReviewRate",
)


class SupplierRateScoreCollector(BaseCollector):
    job = "score"
    table_key = "supplier_rate_score"

    def collect(self, month: str | None = None, months: list[str] | None = None,
                start_month: str | None = None, end_month: str | None = None,
                **_: Any) -> CollectResult:
        """采集供应商评级得分。

        月份四种指定方式（优先级从高到低）：``months`` 显式列表 >
        ``start_month`` [+ ``end_month``] 区间 > 单个 ``month`` > 默认上个月。
        """
        month_list = self.months_to_collect(month=month, months=months,
                                           start_month=start_month, end_month=end_month)
        run_key = "_".join(m.replace("-", "") for m in month_list)
        result = CollectResult(job=self.job, account_id=self.account_id,
                               table=SUPPLIER_RATE_SCORE, run_key=run_key)

        try:
            dst_list = self.api.get_destination_list()
        except SessionExpired:
            raise
        except ApiError as exc:
            result.warnings.append(f"获取目的地列表失败：{exc}")
            self.log.error("[%s] %s", self.account_id, result.warnings[-1])
            return result

        if not dst_list:
            result.warnings.append("该账号下没有目的地，跳过评级采集")
            self.log.warning("[%s] %s", self.account_id, result.warnings[-1])
            return result

        self.log.info("[%s] 供应商评级采集：%d 个目的地 × %d 个月（%s ~ %s）",
                      self.account_id, len(dst_list), len(month_list),
                      month_list[0], month_list[-1])

        for evaluation_month in month_list:
            for index, dst in enumerate(dst_list, start=1):
                region_id = str(dst.get("regionId") or "")
                region_name = str(dst.get("regionName") or "")
                region_type = str(dst.get("regionType") or "2")
                if not region_id:
                    continue
                dst_info = {"regionType": region_type, "regionId": region_id,
                            "regionName": region_name}
                try:
                    data = self.api.get_classify_score_metric(
                        {"dstInfo": dst_info, "month": evaluation_month}
                    )
                except SessionExpired:
                    # 同理：登录态失效属全局故障，不当作单个目的地的失败处理
                    raise
                except ApiError as exc:
                    msg = f"{evaluation_month} {region_name}({region_id}) 评分采集失败：{exc}"
                    result.warnings.append(msg)
                    self.log.error("[%s] %s", self.account_id, msg)
                    continue

                metric = self._parse_score(data)
                if metric is None:
                    self.log.warning("[%s] [%d/%d] %s %s(%s) 无评分数据",
                                     self.account_id, index, len(dst_list),
                                     evaluation_month, region_name, region_id)
                    continue

                raw = {
                    "month": evaluation_month,
                    "regionId": region_id,
                    "regionName": region_name,
                    "regionType": region_type,
                    **metric,
                }
                result.rows.append(self.build_row(SUPPLIER_RATE_SCORE, raw))
                self.log.info("[%s] [%d/%d] %s %s(%s) 服务分=%s",
                              self.account_id, index, len(dst_list), evaluation_month,
                              region_name, region_id, metric.get("serviceScore"))

        self.log.info("[%s] 供应商评级共采集 %d 行", self.account_id, result.count)
        return result

    @staticmethod
    def _parse_score(data: dict[str, Any]) -> dict[str, Any] | None:
        card = data.get("serviceScoreMetricCardInfo") or {}
        if not card:
            return None
        values = card.get("values") or {}
        atomic = ((card.get("calculationFormula") or {}).get("atomicMetrics")) or []
        metric_map = {
            str(m.get("metricName")): m.get("score")
            for m in atomic if isinstance(m, dict)
        }
        parsed: dict[str, Any] = {"serviceScore": values.get("score")}
        for name in RADAR_METRICS:
            parsed[name] = metric_map.get(name)
        if parsed["serviceScore"] in (None, "") and all(
            parsed[name] in (None, "") for name in RADAR_METRICS
        ):
            return None
        return parsed
