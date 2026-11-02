# -*- coding: utf-8 -*-
"""目的地评级 AI 分析采集。

对应原 ``fetch_destination_analysis.py``，主要修复：

* 原脚本输出 JSON 文件、与另两个脚本产出格式不统一，也无法直接入库；
  现在统一走 schema + Sink，CSV/MySQL 一致。
* 原参数解析用「是否以 / 开头」判断输出路径，会把月份误判成路径；现改为标准参数。
* 无数据时原脚本仍写文件；这里区分「接口成功但无内容」与「接口失败」，
  前者返回 0 行并告警，不会污染数据表。
"""

from __future__ import annotations

from typing import Any

from ..api import ApiError, SessionExpired
from ..schema import DESTINATION_RATING
from .base import BaseCollector, CollectResult


class DestinationRatingCollector(BaseCollector):
    job = "rating"
    table_key = "destination_rating"

    def collect(self, month: str | None = None, per_region: bool = False,
                months: list[str] | None = None, start_month: str | None = None,
                end_month: str | None = None, **_: Any) -> CollectResult:
        """采集目的地评级 AI 分析。

        月份四种指定方式（优先级从高到低）：``months`` 显式列表 >
        ``start_month`` [+ ``end_month``] 区间 > 单个 ``month`` > 默认上个月。

        原先只支持单月，无法批量补月；现在与供应商评级保持一致的多月语义。
        """
        month_list = self.months_to_collect(month=month, months=months,
                                           start_month=start_month, end_month=end_month)
        run_key = "_".join(m.replace("-", "") for m in month_list)
        result = CollectResult(job=self.job, account_id=self.account_id,
                               table=DESTINATION_RATING, run_key=run_key)
        self.log.info("[%s] 目的地评级分析采集 %d 个月（%s ~ %s，逐目的地=%s）",
                      self.account_id, len(month_list), month_list[0], month_list[-1],
                      per_region)

        targets: list[dict[str, Any]] = [
            {"regionType": "0", "regionId": "ALL", "regionName": "账号汇总"}
        ]
        if per_region:
            try:
                for dst in self.api.get_destination_list():
                    targets.append({
                        "regionType": str(dst.get("regionType") or "2"),
                        "regionId": str(dst.get("regionId") or ""),
                        "regionName": str(dst.get("regionName") or ""),
                    })
            except SessionExpired:
                raise
            except ApiError as exc:
                result.warnings.append(f"获取目的地列表失败，仅采集账号汇总：{exc}")
                self.log.warning("[%s] %s", self.account_id, result.warnings[-1])

        total = len(month_list)
        for index, month_value in enumerate(month_list, start=1):
            for target in targets:
                self._collect_one(result, month_value, target, index, total)

        self.log.info("[%s] 目的地评级分析共采集 %d 行", self.account_id, result.count)
        return result

    def _collect_one(self, result: CollectResult, month: str,
                     target: dict[str, Any], index: int, total: int) -> None:
        region_id = target["regionId"]
        payload_dst = {"regionType": target["regionType"]}
        if region_id != "ALL":
            payload_dst["regionId"] = region_id
            payload_dst["regionName"] = target["regionName"]
        try:
            data = self.api.get_classify_ai_analysis({"dstInfo": payload_dst, "month": month})
        except SessionExpired:
            # 登录态失效是全局故障，必须穿透「单片失败不中断」的容错网：
            # SessionExpired 继承自 ApiError，若不先行捕获并上抛，
            # 它会被下面的 except ApiError 降级为警告，
            # 导致调度层无法触发自动重登，后续分片也会全部白跑。
            raise
        except ApiError as exc:
            msg = f"{month} {target['regionName']} 分析采集失败：{exc}"
            result.warnings.append(msg)
            self.log.error("[%s] %s", self.account_id, msg)
            return

        message = (data.get("message") or "").strip()
        if not message:
            msg = f"{month} {target['regionName']} 无分析内容（该接口通常仅近期月份有数据）"
            result.warnings.append(msg)
            self.log.warning("[%s] %s", self.account_id, msg)
            return

        raw = {
            "month": month,
            "region_id": region_id,
            "region_name": target["regionName"],
            "region_type": target["regionType"],
            "message": message,
        }
        result.rows.append(self.build_row(
            DESTINATION_RATING, raw, {"analysis_text_len": len(message)}
        ))
        self.log.info("[%s] [%d/%d] %s %s 获取分析文本 %d 字",
                      self.account_id, index, total, month,
                      target["regionName"], len(message))
