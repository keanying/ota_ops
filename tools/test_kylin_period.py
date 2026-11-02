#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""交易卡片周期口径与成交资源数自检（不依赖真实携程，也不依赖 MySQL）。

验证重点在于「请求形态」——成交资源数之所以长期采不到，
根因就是请求形态错了（queryId/接口/聚合方式三处），
因此这里用假的 API 客户端拦截并断言每一次请求体，
而不是只看最终有没有数值。

运行::

    python tools/test_kylin_period.py
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ctrip.collectors.kylin_daily import (  # noqa: E402
    ALL_PERIODS, METRICS, PERIOD_DAILY, PERIOD_WINDOWS, QUERY_ID,
    RESOURCE_METRIC, RESOURCE_QUERY_ID, KylinDailyCollector)
from ctrip.config import load_config  # noqa: E402
from ctrip.logging_utils import account_logger  # noqa: E402

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    results.append((PASS if condition else FAIL, name, detail))


class FakeApi:
    """记录所有请求体的假客户端。"""

    def __init__(self) -> None:
        self.trendline_calls: list[dict[str, Any]] = []
        self.subline_calls: list[dict[str, Any]] = []

    def get_home_html(self) -> str:
        return ('<div><span>景点玩乐</span><span class="tag gpznbmm">'
                '<span class="txt gpznbmn">已签约</span></span></div>')

    def get_vbk_bu_types(self, code: int) -> list[dict[str, Any]]:
        return [{"vbkBuType": "门票"}]

    @staticmethod
    def _payload_metrics(payload: dict[str, Any]) -> list[str]:
        return [m["metric"] for m in payload.get("metricList", [])]

    def _respond(self, payload: dict[str, Any], per_day: bool) -> dict[str, Any]:
        start = date.fromisoformat(payload["startdate"])
        end = date.fromisoformat(payload["enddate"])
        span = (end - start).days + 1 if per_day else 1
        out = []
        for metric in self._payload_metrics(payload):
            vm = {}
            mm = {}
            for i in range(span):
                day = (start + timedelta(days=i)).isoformat()
                vm[day] = 7 if metric == RESOURCE_METRIC else 100 + i
                mm[day] = 0.5
            out.append({
                "metric": metric,
                "dateValueMapList": [{"valueDateMap": vm}],
                "sublineDataMap": {"MOMRate": {"dataList": [{"valueDateMap": mm}]}},
            })
        return {"resultList": out}

    def get_kylin_trendline(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.trendline_calls.append(payload)
        return self._respond(payload, per_day=True)

    def get_kylin_subline(self, payload: dict[str, Any]) -> dict[str, Any]:
        """严格模拟真实聚合接口：**永远只返回区间起始日一个 key**。

        早期版本这里按 ``calculate.value <= 1`` 就逐日返回，恰好把
        「传个区间就能拿到每日资源数」的错误假设掩盖了，
        导致自检通过但真实环境下只有首日有值。
        """
        self.subline_calls.append(payload)
        return self._respond(payload, per_day=False)


def build(cfg, api: FakeApi) -> KylinDailyCollector:
    return KylinDailyCollector(
        cfg=cfg, account_id="A001", scenic_id="PFTSCA01018088",
        api=api, log=account_logger(cfg, "A001"),
    )


def main() -> int:
    cfg = load_config()
    today = date(2026, 8, 13)

    # ---- 三种口径全采 ----
    api = FakeApi()
    col = build(cfg, api)
    col.today = lambda: today          # 固定"今天"，避免测试随日期漂移
    # 默认（不传 periods）应只采 1d：7d/30d 会额外发请求，不应默认开启
    res_default = col.collect(days=3)
    check("默认只采 1d（7d/30d 默认关闭）",
          {r["period_type"] for r in res_default.rows} == {"1d"},
          str(sorted({r["period_type"] for r in res_default.rows})))
    check("默认不发窗口聚合主指标请求",
          all(RESOURCE_METRIC in FakeApi._payload_metrics(p)
              for p in api.subline_calls),
          f"{len(api.subline_calls)} 次 subline")
    check("配置默认值也是仅 1d",
          list(cfg.collect.periods) == ["1d"], str(cfg.collect.periods))

    # 显式全采（等同 CLI 的 --period all）
    api = FakeApi()
    col = build(cfg, api)
    col.today = lambda: today
    res = col.collect(days=3, periods=ALL_PERIODS)

    periods = {r["period_type"] for r in res.rows}
    check("显式全采时三种口径都在", periods == {"1d", "7d", "30d"},
          str(sorted(periods)))
    check("ALL_PERIODS 顺序为 1d/7d/30d",
          ALL_PERIODS == ["1d", "7d", "30d"], str(ALL_PERIODS))

    by_period: dict[str, list[dict[str, Any]]] = {}
    for row in res.rows:
        by_period.setdefault(row["period_type"], []).append(row)

    # 本例只有 1 个签约组合（景点玩乐/门票），
    # 但默认会采使用日期与预订日期两种 date_type，故行数要乘 2
    n_types = len(cfg.collect.date_types)
    check("1d 每天一行（3 天 × 日期类型数）",
          len(by_period["1d"]) == 3 * n_types,
          f"{len(by_period['1d'])} 行（date_types={n_types}）")
    check("7d 每个组合仅 1 行（区间聚合）",
          len(by_period["7d"]) == n_types, f"{len(by_period['7d'])} 行")
    check("30d 每个组合仅 1 行（区间聚合）",
          len(by_period["30d"]) == n_types, f"{len(by_period['30d'])} 行")
    check("7d/30d 行数不随区间长度增长（快照语义）",
          len(by_period["7d"]) == len(by_period["30d"]) == n_types)

    check("7d 的 transaction_date 为窗口起始日",
          str(by_period["7d"][0]["transaction_date"]) == "2026-08-06",
          str(by_period["7d"][0]["transaction_date"]))
    check("30d 的 transaction_date 为窗口起始日",
          str(by_period["30d"][0]["transaction_date"]) == "2026-07-14",
          str(by_period["30d"][0]["transaction_date"]))
    check("1d 截止昨天（T+1）",
          str(max(r["transaction_date"] for r in by_period["1d"])) == "2026-08-12")

    # ---- 成交资源数确实落到了每一行 ----
    missing = [r for r in res.rows if r.get("deal_resource_count") is None]
    check("所有行都有成交资源数", not missing, f"{len(missing)} 行缺失")
    check("成交资源数环比也已写入",
          all(r.get("deal_resource_count_mom") is not None for r in res.rows))

    # ---- 请求形态：这是资源数长期采不到的根因所在 ----
    res_calls = [p for p in api.subline_calls
                 if RESOURCE_METRIC in FakeApi._payload_metrics(p)]
    check("资源数确实单独发了请求", bool(res_calls), f"{len(res_calls)} 次")
    check("资源数请求使用 queryId=354",
          all(p["queryId"] == [RESOURCE_QUERY_ID] for p in res_calls))
    check("资源数请求带 aggregateType=distinct",
          all(p["metricList"][0].get("aggregateType") == "distinct"
              for p in res_calls))
    check("资源数走 SublineV2 聚合接口而非 TrendLine",
          all(RESOURCE_METRIC not in FakeApi._payload_metrics(p)
              for p in api.trendline_calls))
    check("资源数未被混入主指标 metricList（混入则采不到）",
          all(len(p["metricList"]) == 1 for p in res_calls))
    check("主指标仍是 5 项且 queryId=353",
          all(len(FakeApi._payload_metrics(p)) == len(METRICS)
              and p["queryId"] == [QUERY_ID] for p in api.trendline_calls))

    # 1d 的资源数按天返回（unit value=1），7d/30d 按窗口聚合
    res_units = sorted({int(p["calculate"]["value"]) for p in res_calls})
    check("资源数请求覆盖 1/7/30 三种聚合单位",
          res_units == [1, 7, 30], str(res_units))

    # ---- 资源数在 1d 口径下必须逐日单发（本轮修正的根因）----
    # 聚合接口传区间只返回首日一个值，因此只能一天一个请求
    daily_res = [p for p in res_calls
                 if int(p["calculate"]["value"]) == 1]
    check("1d 资源数请求 startdate == enddate（单日）",
          bool(daily_res) and all(p["startdate"] == p["enddate"]
                                  for p in daily_res),
          f"{len(daily_res)} 次单日请求")
    # 3 天 × date_type 数，每天一个请求
    check("1d 资源数每天一个请求（3 天 × 日期类型数）",
          len(daily_res) == 3 * n_types,
          f"{len(daily_res)} 次（期望 {3 * n_types}）")
    daily_dates = sorted({p["startdate"] for p in daily_res})
    check("1d 资源数请求覆盖区间内每一天（无遗漏）",
          daily_dates == ["2026-08-10", "2026-08-11", "2026-08-12"],
          str(daily_dates))

    # 最关键的一条：每一天都要有资源数，而不是只有首日
    d1_rows = by_period["1d"]
    check("1d 每一行都有成交资源数（不是只首日有）",
          all(r.get("deal_resource_count") is not None for r in d1_rows),
          f"{sum(1 for r in d1_rows if r.get('deal_resource_count') is None)}"
          f"/{len(d1_rows)} 行缺失")
    check("1d 每一行都有资源数环比",
          all(r.get("deal_resource_count_mom") is not None for r in d1_rows))
    # 7d/30d 仍然是区间聚合单请求，不该被改成逐日
    win_res = [p for p in res_calls if int(p["calculate"]["value"]) > 1]
    check("7d/30d 资源数仍为区间单请求（startdate != enddate）",
          bool(win_res) and all(p["startdate"] != p["enddate"] for p in win_res),
          f"{len(win_res)} 次")
    check("7d 资源数请求区间与窗口一致",
          any(p["startdate"] == "2026-08-06" and p["enddate"] == "2026-08-12"
              for p in win_res))
    check("30d 资源数请求区间与窗口一致",
          any(p["startdate"] == "2026-07-14" and p["enddate"] == "2026-08-12"
              for p in win_res))

    win_calls = [p for p in api.subline_calls
                 if RESOURCE_METRIC not in FakeApi._payload_metrics(p)]
    check("7d/30d 主指标走聚合接口且 needAggregate=True",
          bool(win_calls) and all(p["needAggregate"] for p in win_calls))
    check("1d 主指标 needAggregate=False（逐日明细）",
          all(not p["needAggregate"] for p in api.trendline_calls))

    # ---- 只采单一口径 ----
    api2 = FakeApi()
    col2 = build(cfg, api2)
    col2.today = lambda: today
    res2 = col2.collect(days=3, periods=["1d"])
    check("--period 1d 只产出 1d 行",
          {r["period_type"] for r in res2.rows} == {"1d"})
    check("只采 1d 时不请求窗口聚合主指标",
          all(RESOURCE_METRIC in FakeApi._payload_metrics(p)
              for p in api2.subline_calls))

    api3 = FakeApi()
    col3 = build(cfg, api3)
    col3.today = lambda: today
    res3 = col3.collect(days=3, periods=["7d", "30d"])
    check("--period 7d,30d 只产出这两种口径",
          {r["period_type"] for r in res3.rows} == {"7d", "30d"})
    check("只采 7d/30d 时完全不调 TrendLine（省请求）",
          not api3.trendline_calls, f"{len(api3.trendline_calls)} 次")

    # ---- 非法口径必须报错 ----
    bad = False
    try:
        col3.collect(days=3, periods=["90d"])
    except ValueError:
        bad = True
    check("拒绝未知周期口径", bad)

    api5 = FakeApi()
    col5 = build(cfg, api5)
    col5.today = lambda: today
    empty = False
    try:
        res5 = col5.collect(days=3, periods=[])
    except ValueError:
        empty = True
    # 空列表按「未指定」处理，走配置默认（仅 1d），不应报错
    check("空口径列表回退为配置默认而非报错", not empty)
    check("空口径列表回退后也只有 1d",
          not empty and {r["period_type"] for r in res5.rows} == {"1d"})

    # ---- 顺序稳定：便于日志与人工核对 ----
    api4 = FakeApi()
    col4 = build(cfg, api4)
    col4.today = lambda: today
    res4 = col4.collect(days=1, periods=["30d", "1d", "7d", "1d"])
    order = []
    for row in res4.rows:
        if row["period_type"] not in order:
            order.append(row["period_type"])
    check("口径顺序恒为 1d→7d→30d（去重且忽略传入顺序）",
          order == [PERIOD_DAILY, "7d", "30d"], str(order))
    check("窗口天数映射正确",
          PERIOD_WINDOWS == {"7d": 7, "30d": 30}, str(PERIOD_WINDOWS))

    width = max(len(name) for _, name, _ in results) + 2
    failed = 0
    print("=" * (width + 26))
    print("交易卡片周期口径与成交资源数自检")
    print("=" * (width + 26))
    for status, name, detail in results:
        if status == FAIL:
            failed += 1
        print(f"[{status}] {name:<{width}}{detail}")
    print("-" * (width + 26))
    print(f"共 {len(results)} 项，失败 {failed} 项")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
