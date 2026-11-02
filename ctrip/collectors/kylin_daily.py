# -*- coding: utf-8 -*-
"""交易卡片数据采集（Kylin）。

对应原 ``fetch_daily_kylin.py``，主要修复：

* 指标名含 ``|`` ``/``，无法作为列名 —— 现在由 ``schema.py`` 的 ``source`` 映射到
  ``turnover`` / ``refund_rate`` 等合法列名，CSV 与数据库列名完全一致。
* 原脚本边采边追加写 CSV，重复运行会累积重复数据；现在先采集成内存行，
  由 Sink 统一按唯一键去重后覆盖写 / UPSERT。
* 签约类型解析仅依赖一条脆弱正则；这里改为「正则 + 兜底关键字扫描」，
  且未识别的类型会明确告警而不是静默丢弃。
* 时间统一用配置时区（``Asia/Shanghai``），避免服务器 UTC 导致少采一天。
* **补齐成交资源数**：原脚本的 5 项指标走 ``queryId=353``，而成交资源数是
  ``queryId=354`` + ``aggregateType=distinct``（资源 ID 去重计数），
  走 ``getKylinDataWithSublineV2`` 接口，必须单独发请求，
  塞进原 ``metricList`` 是采不到的。
  而且该接口固定聚合，日口径下必须 **逐日** 发（``startdate == enddate``），
  一次传区间只会得到首日一个聚合值。

三种周期口径（``period_type``）
------------------------------
=========  ==================================  =========================
period     区间                                 采集方式
=========  ==================================  =========================
``1d``     用户指定区间内的**每一天**            TrendLine 逐日明细，
                                                一次请求拿回整批的每日值
``7d``     截止日往前 7 天（**仅当前窗口**）     Subline 区间聚合，1 行
``30d``    截止日往前 30 天（**仅当前窗口**）    Subline 区间聚合，1 行
=========  ==================================  =========================

``7d`` / ``30d`` 是「当前快照」，不随 ``--start-date`` 做历史滚动
（否则回溯两年会额外产生上千次请求，且携程页面本身也只展示当前窗口）。
其 ``transaction_date`` 存区间**起始日**，与携程返回的 key 一致，
配合 ``period_type`` 唯一定位一个聚合区间。

**默认只采 ``1d``**，``7d`` / ``30d`` 默认关闭；需要时用
``--period 7d,30d`` 或在 ``config.yaml`` 的 ``collect.periods`` 里开启。
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any

from ..api import ApiError, SessionExpired
from ..daterange import resolve_date_range, slice_dates
from ..schema import KYLIN_DAILY
from .base import BaseCollector, CollectResult

# -----------------------------------------------------------------------------
# 指标定义
# -----------------------------------------------------------------------------

# 交易卡片 5 项指标（queryId=353，原样传给携程接口）
METRICS = [
    "ttd_suc_nocoef_income",               # 成交销售额
    "ttd_suc_ord_cnt",                     # 订单数
    "ttd_suc_nocoef_qty",                  # 成交票量
    "ttd_chk_qty",                         # 核验票量
    "ttd_arfd_ord_cnt|ttd_pay_ord_cnt|/",  # 退订率
]

QUERY_ID = 353

# 成交资源数：与上面 5 项**不是同一个请求**，三处都不同，缺一不可：
#   1. queryId 是 354 而非 353
#   2. 必须带 aggregateType=distinct（对资源 ID 去重计数）
#   3. 走 getKylinDataWithSublineV2 聚合接口
# 早期版本照搬 5 项指标的请求形态，因此这个指标一直采不到。
RESOURCE_METRIC = "suc_resourceid"
RESOURCE_QUERY_ID = 354

# 周期口径 -> 天数。1d 由用户指定的区间逐日展开，不在此表内。
PERIOD_WINDOWS: dict[str, int] = {"7d": 7, "30d": 30}

PERIOD_DAILY = "1d"

# 全部可选口径，固定 1d -> 7d -> 30d 的顺序（CLI 的 --period all 也用它）
ALL_PERIODS: list[str] = [PERIOD_DAILY, *PERIOD_WINDOWS]

# 页面下拉框结构（class 名会随构建变化，故同时保留宽松模式）
_SIGNING_PATTERNS = [
    r"<span>([^<]{2,12})</span>\s*<span[^>]*class=\"[^\"]*gpznbmm[^\"]*\"[^>]*>"
    r"\s*<span[^>]*class=\"[^\"]*gpznbmn[^\"]*\"[^>]*>\s*([^<]{2,8})\s*</span>",
    r"<span>([^<]{2,12})</span>\s*<span[^>]*>\s*<span[^>]*>\s*(已签约|未签约)\s*</span>",
]


class KylinDailyCollector(BaseCollector):
    job = "kylin"
    table_key = "kylin_daily"

    def collect(self, days: int | None = None, end_date: date | None = None,
                start_date: date | None = None, batch_days: int | None = None,
                periods: list[str] | None = None,
                **_: Any) -> CollectResult:
        """采集交易卡片数据。

        区间三种指定方式（优先级从高到低，仅作用于 ``1d``）：

        * ``start_date`` [+ ``end_date``]：显式区间，回溯历史用这个
        * ``days`` [+ ``end_date``]：从截止日往前推 N 天
        * 全不传：默认 T+1（到昨天）往前 ``collect.kylin_days`` 天

        ``periods`` 控制采哪些周期口径，**默认只采 ``1d``**；
        ``7d`` / ``30d`` 默认关闭，需显式传入或在配置里开启。
        ``1d`` 按 ``batch_days``（默认 30 天）切批，单批失败不影响其他批次。
        """
        start, end = resolve_date_range(
            today=self.today(), start_date=start_date, end_date=end_date,
            days=days, default_days=self.cfg.collect.kylin_days,
        )
        wanted = self._resolve_periods(periods)
        size = max(1, int(batch_days or self.cfg.collect.batch_days))
        slices = slice_dates(start, end, size)
        total_days = (end - start).days + 1

        run_key = f"{start:%Y%m%d}_{end:%Y%m%d}"
        result = CollectResult(job=self.job, account_id=self.account_id,
                               table=KYLIN_DAILY, run_key=run_key)

        self.log.info("[%s] 交易卡片采集口径 %s", self.account_id, "/".join(wanted))
        if PERIOD_DAILY in wanted:
            self.log.info("[%s] 1d 区间 %s ~ %s（共 %d 天，按 %d 天/批切为 %d 批）",
                          self.account_id, start, end, total_days, size, len(slices))

        signings = self.discover_signings()
        if not signings:
            result.warnings.append("未发现任何已签约类型，无法采集交易卡片数据")
            self.log.warning("[%s] %s", self.account_id, result.warnings[-1])
            return result

        failed = 0
        attempted = 0
        for signing in signings:
            for date_type in self.cfg.collect.date_types:
                # ---- 1d：逐日明细，按批次请求 ----
                if PERIOD_DAILY in wanted:
                    for batch in slices:
                        attempted += 1
                        try:
                            rows = self.fetch_daily(batch.start, batch.end,
                                                    date_type, signing)
                            result.rows.extend(rows)
                            self.log.info("[%s] 1d 批次 %s | %s | %s/%s → %d 行",
                                          self.account_id, batch, date_type,
                                          signing["signing_type"],
                                          signing["vbk_bu_type"], len(rows))
                        except SessionExpired:
                            # 登录态失效后继续跑剩下几十个批次没有意义，
                            # 只会白白消耗请求并加重风控；直接上抛由调度层重登
                            raise
                        except ApiError as exc:
                            failed += 1
                            msg = (f"1d {signing['signing_type']}/{signing['vbk_bu_type']} "
                                   f"{batch.start}~{batch.end} {date_type} 采集失败：{exc}")
                            result.warnings.append(msg)
                            self.log.error("[%s] %s", self.account_id, msg)

                # ---- 7d / 30d：仅当前窗口，各 1 行 ----
                for period in (p for p in wanted if p != PERIOD_DAILY):
                    attempted += 1
                    window_days = PERIOD_WINDOWS[period]
                    win_start = end - timedelta(days=window_days - 1)
                    try:
                        rows = self.fetch_window(win_start, end, period,
                                                 date_type, signing)
                        result.rows.extend(rows)
                        self.log.info("[%s] %s 窗口 %s ~ %s | %s | %s/%s → %d 行",
                                      self.account_id, period, win_start, end, date_type,
                                      signing["signing_type"],
                                      signing["vbk_bu_type"], len(rows))
                    except SessionExpired:
                        raise
                    except ApiError as exc:
                        failed += 1
                        msg = (f"{period} {signing['signing_type']}/{signing['vbk_bu_type']} "
                               f"{win_start}~{end} {date_type} 采集失败：{exc}")
                        result.warnings.append(msg)
                        self.log.error("[%s] %s", self.account_id, msg)

        self.log.info("[%s] 交易卡片共采集 %d 行（请求 %d/%d 成功）",
                      self.account_id, result.count, attempted - failed, attempted)
        return result

    # -- 周期口径解析 ------------------------------------------------------
    def _resolve_periods(self, periods: list[str] | None) -> list[str]:
        """校验并规范化周期口径，保持 1d -> 7d -> 30d 的稳定顺序。"""
        allowed = ALL_PERIODS
        if not periods:
            # 未指定时读配置；配置也缺失时只采 1d。
            # 这里故意不回退成「全采」：7d/30d 会额外发请求，
            # 默认行为应当是最保守的那个。
            configured = list(
                getattr(self.cfg.collect, "periods", None) or [PERIOD_DAILY]
            )
        else:
            configured = list(periods)

        normalized: list[str] = []
        for item in configured:
            key = str(item).strip().lower()
            if key not in allowed:
                raise ValueError(
                    f"未知周期口径「{item}」，可选：{', '.join(allowed)}"
                )
            if key not in normalized:
                normalized.append(key)
        if not normalized:
            raise ValueError("周期口径不能为空")
        return [p for p in allowed if p in normalized]

    # -- Step 1: 发现已签约类型 --------------------------------------------
    def discover_signings(self) -> list[dict[str, Any]]:
        """解析后台页面得到已签约类型，再查询各自的业务子类型。"""
        try:
            html = self.api.get_home_html()
        except Exception as exc:  # noqa: BLE001
            self.log.error("[%s] 获取后台页面失败：%s", self.account_id, exc)
            return []

        pairs = self._parse_signing_status(html)
        if not pairs:
            self.log.warning("[%s] 未从页面解析出签约状态，回退为按配置全量尝试",
                             self.account_id)
            pairs = [(name, "已签约") for name in self.cfg.collect.signing_name_to_code]

        signed: list[tuple[int, str]] = []
        for name, status in pairs:
            code = self.cfg.collect.signing_name_to_code.get(name)
            if code is None:
                self.log.warning("[%s] 未知签约类型「%s」，请在配置 "
                                 "collect.signing_name_to_code 中补充映射", self.account_id, name)
                continue
            if "已签约" in status:
                signed.append((code, name))
                self.log.info("[%s] 已签约：%s（code=%s）", self.account_id, name, code)
            else:
                self.log.debug("[%s] 未签约：%s", self.account_id, name)

        result: list[dict[str, Any]] = []
        for code, name in signed:
            try:
                bu_list = self.api.get_vbk_bu_types(code)
            except SessionExpired:
                raise
            except ApiError as exc:
                self.log.warning("[%s] code=%s 查询业务子类型失败：%s", self.account_id, code, exc)
                bu_list = []
            if bu_list:
                for item in bu_list:
                    result.append({
                        "code": code,
                        "signing_type": name,
                        "vbk_bu_type": str(item.get("vbkBuType") or ""),
                    })
            else:
                result.append({"code": code, "signing_type": name, "vbk_bu_type": ""})
        return result

    @staticmethod
    def _parse_signing_status(html: str) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        seen: set[str] = set()
        for pattern in _SIGNING_PATTERNS:
            for name, status in re.findall(pattern, html):
                name = name.strip()
                if name and name not in seen:
                    seen.add(name)
                    pairs.append((name, status.strip()))
            if pairs:
                break
        return pairs

    # -- Step 2a: 1d 逐日明细 ---------------------------------------------
    def fetch_daily(self, start: date, end: date, date_type: str,
                    signing: dict[str, Any]) -> list[dict[str, Any]]:
        """逐日口径：一次请求拿回区间内每天的值。"""
        payload = {
            "queryId": [QUERY_ID],
            "needAggregate": False,
            "groupTagList": [],
            "calculator": "sum",
            "startdate": start.isoformat(),
            "enddate": end.isoformat(),
            "tagMap": {"date_type": date_type, "d": self.today().isoformat()},
            "metricList": [{"metric": m, "sublineTypeList": ["MOMRate"]} for m in METRICS],
            "calculate": {"unit": "d", "value": 1},
        }
        data = self.api.get_kylin_trendline(payload)
        metrics = self._extract_metrics(data)

        self._merge_daily_resource(metrics, start, end, date_type)

        return self._rows_from_metrics(metrics, PERIOD_DAILY, date_type, signing)

    def _merge_daily_resource(self, metrics: dict[str, dict[str, Any]],
                              start: date, end: date, date_type: str) -> None:
        """逐日补齐成交资源数（每天一个请求）。

        这里必须逐日发而不能一次传整个区间：该接口固定
        ``needAggregate=true``，传区间会把整段聚合成**一个**值（key 为区间
        起始日），结果只有首日有资源数、其余天全部为 NULL。
        真实拓包也证实页面在日口径下发的是 ``startdate == enddate`` 的单日
        请求（``value=1``）。

        资源数是去重计数，**天粒度不可由区间值拆分**，因此多发请求是
        唯一途径。单天失败只影响那一天（留 NULL），不拖累主指标与其他天。
        """
        total = (end - start).days + 1
        ok = 0
        bad: list[str] = []
        day = start
        while day <= end:
            try:
                res_data = self.api.get_kylin_subline(
                    self._resource_payload(day, day, date_type, value=1)
                )
                extra = self._extract_metrics(res_data)
                # 单日请求的返回 key 就是该日，但不依赖这一点：若上游返回了
                # 其他日期键，强制归一到当前循环日，避免写错行
                key = day.isoformat()
                merged = {}
                for values in extra.values():
                    merged.update(values)
                if merged:
                    self._merge_metrics(metrics, {key: merged})
                    ok += 1
            except SessionExpired:
                raise
            except ApiError as exc:
                bad.append(f"{day}({exc})")
            day += timedelta(days=1)

        if bad:
            # 汇总输出而不是每天一条：一个 30 天批次失败时会刷出 30 行日志
            self.log.warning("[%s] 1d 成交资源数 %s~%s %s：%d/%d 成功，"
                             "失败日期 %s（其余指标不受影响，重跑可回填）",
                             self.account_id, start, end, date_type,
                             ok, total, ", ".join(bad[:5])
                             + (f" 等 {len(bad)} 天" if len(bad) > 5 else ""))
        else:
            self.log.debug("[%s] 1d 成交资源数 %s~%s %s：%d/%d 成功",
                           self.account_id, start, end, date_type, ok, total)

    # -- Step 2b: 7d / 30d 区间聚合 ---------------------------------------
    def fetch_window(self, start: date, end: date, period: str, date_type: str,
                     signing: dict[str, Any]) -> list[dict[str, Any]]:
        """聚合口径：整个窗口聚合成一个值，返回单行。

        携程返回的 ``valueDateMap`` 只含一个 key（窗口起始日），
        因此这里天然只会产出 1 行。
        """
        window_days = PERIOD_WINDOWS[period]
        payload = {
            "queryId": [QUERY_ID],
            "needAggregate": True,
            "groupTagList": [],
            "calculator": "sum",
            "startdate": start.isoformat(),
            "enddate": end.isoformat(),
            "tagMap": {"date_type": date_type, "d": self.today().isoformat()},
            "metricList": [{"metric": m, "sublineTypeList": ["MOMRate"]} for m in METRICS],
            "calculate": {"unit": "d", "value": window_days},
        }
        data = self.api.get_kylin_subline(payload)
        metrics = self._extract_metrics(data)

        try:
            res_data = self.api.get_kylin_subline(
                self._resource_payload(start, end, date_type, value=window_days)
            )
            self._merge_metrics(metrics, self._extract_metrics(res_data))
        except SessionExpired:
            raise
        except ApiError as exc:
            self.log.warning("[%s] %s 成交资源数采集失败（其余指标不受影响）"
                             "%s~%s %s：%s", self.account_id, period, start, end, date_type, exc)

        rows = self._rows_from_metrics(metrics, period, date_type, signing)
        if len(rows) > 1:
            # 聚合口径理论上只应返回窗口起始日一个 key；若携程改了行为，
            # 这里显式告警，避免多行悄悄写进库里污染 7d/30d 口径。
            self.log.warning("[%s] %s 聚合口径返回 %d 个日期（预期 1 个），"
                             "已全部保留，请核对上游是否改变了聚合语义",
                             self.account_id, period, len(rows))
        return rows

    def _resource_payload(self, start: date, end: date, date_type: str,
                          value: int) -> dict[str, Any]:
        """成交资源数请求体（queryId=354 + distinct 去重）。"""
        return {
            "queryId": [RESOURCE_QUERY_ID],
            "needAggregate": True,
            "groupTagList": [],
            "calculator": "sum",
            "startdate": start.isoformat(),
            "enddate": end.isoformat(),
            "tagMap": {"date_type": date_type, "d": self.today().isoformat()},
            "metricList": [{
                "metric": RESOURCE_METRIC,
                "aggregateType": "distinct",
                "sublineTypeList": ["MOMRate"],
            }],
            "calculate": {"unit": "d", "value": value},
        }

    # -- 返回解析 ----------------------------------------------------------
    @staticmethod
    def _extract_metrics(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """把携程返回摊平成 ``{日期: {指标名: 值}}``。

        主指标与 ``MOMRate`` 环比在返回里是两棵子树，这里合并到同一天上，
        环比键统一加 ``_MOMRate`` 后缀，与 ``schema.py`` 的 ``source`` 对齐。
        """
        out: dict[str, dict[str, Any]] = {}
        for item in data.get("resultList") or []:
            metric = item.get("metric")
            if not metric:
                continue
            for dvm in item.get("dateValueMapList") or []:
                for day, value in (dvm.get("valueDateMap") or {}).items():
                    out.setdefault(day, {})[metric] = value
            sub = (item.get("sublineDataMap") or {}).get("MOMRate") or {}
            for dl in sub.get("dataList") or []:
                for day, value in (dl.get("valueDateMap") or {}).items():
                    out.setdefault(day, {})[f"{metric}_MOMRate"] = value
        return out

    @staticmethod
    def _merge_metrics(target: dict[str, dict[str, Any]],
                       extra: dict[str, dict[str, Any]]) -> None:
        """把额外一轮请求的指标合并进主结果。

        资源数是独立请求，日期键理论上与主指标一致；若上游给了主指标没有的
        日期（例如主指标当天无成交但仍有资源），也会补成新的一天，
        避免静默丢数据。
        """
        for day, values in extra.items():
            target.setdefault(day, {}).update(values)

    def _rows_from_metrics(self, metrics: dict[str, dict[str, Any]], period: str,
                           date_type: str, signing: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for day in sorted(metrics):
            raw: dict[str, Any] = {
                "date_start": day,
                "code": signing["code"],
                "signing_type": signing["signing_type"],
                "vbk_bu_type": signing.get("vbk_bu_type") or "",
                "date_type": date_type,
                "period_type": period,
            }
            raw.update(metrics[day])
            rows.append(self.build_row(KYLIN_DAILY, raw))
        return rows
