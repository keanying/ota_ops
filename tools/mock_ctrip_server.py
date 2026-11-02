#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""携程接口模拟服务（仅用于本地端到端验证，不参与生产）。

为什么需要它：真实携程后台需要有效账号与人工验证码，无法在 CI/沙箱里跑通；
这个 mock 严格按真实接口的响应结构返回数据，并且**不同账号返回不同数据**，
以便验证多账号隔离、字段映射、去重、UPSERT 幂等等逻辑是否正确。

账号识别：通过 Cookie 中的 ``mock_account`` 值区分账号。

启动::

    python tools/mock_ctrip_server.py --port 18080
"""

from __future__ import annotations

import argparse
import json
import os
import random
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

SUCCESS = {"Ack": "Success", "Errors": [], "Timestamp": "2026-08-13T00:00:00"}

# 每个账号的画像：签约类型、目的地、数据量级都不同，便于验证隔离
ACCOUNT_PROFILES: dict[str, dict] = {
    "A001": {
        "signings": [("跟团游", "已签约"), ("私家团", "未签约"), ("景点玩乐", "已签约")],
        "bu_types": {1: ["跟团游"], 7: ["门票", "玩乐"]},
        "regions": [
            {"regionId": "100031", "regionName": "吉林", "regionType": "2"},
            {"regionId": "100032", "regionName": "长白山", "regionType": "2"},
            {"regionId": "100033", "regionName": "延边", "regionType": "2"},
        ],
        "income_base": 5200.0,
        "score_base": 3.0,
    },
    "A002": {
        "signings": [("跟团游", "未签约"), ("私家团", "已签约"), ("景点玩乐", "已签约")],
        "bu_types": {6: ["私家团"], 7: ["门票"]},
        "regions": [
            {"regionId": "200011", "regionName": "三亚", "regionType": "2"},
            {"regionId": "200012", "regionName": "海口", "regionType": "2"},
        ],
        "income_base": 18800.0,
        "score_base": 4.2,
    },
    "A003": {
        "signings": [("景点玩乐", "已签约")],
        "bu_types": {7: ["门票"]},
        "regions": [{"regionId": "300001", "regionName": "成都", "regionType": "2"}],
        "income_base": 990.0,
        "score_base": 2.4,
    },
}

RADAR_METRICS = [
    "orderConfirmationRate",
    "fulfillmentDefectRate",
    "customerServiceManualInterventionRate",
    "20secondsResponseTimelinessRate",
    "IMNegativeReviewRate",
]


def profile(account_id: str) -> dict:
    return ACCOUNT_PROFILES.get(account_id, ACCOUNT_PROFILES["A001"])


def home_html(account_id: str) -> str:
    """模拟后台页面：包含签约状态下拉框结构。"""
    items = []
    for name, status in profile(account_id)["signings"]:
        items.append(
            f'<div class="item"><span>{name}</span>'
            f'<span class="tag gpznbmm"><span class="txt gpznbmn">{status}</span></span></div>'
        )
    filler = "<!-- tour-bi-vendor mock page -->" + ("padding " * 800)
    return (
        "<!DOCTYPE html><html><head><title>数据参谋 tour-bi-vendor</title></head><body>"
        f'<div id="app" data-account="{account_id}">{"".join(items)}</div>{filler}'
        "</body></html>"
    )


# 模拟上游历史数据窗口：早于此日期返回空结果（用于验证回溯脚本的探测能力）。
# 现实中携程对历史数据的保留期限未知，因此验证时必须能模拟出「部分月份没数据」的情形。
HISTORY_START = os.getenv("MOCK_HISTORY_START", "")  # 如 2025-06-01，空则不限制

# 测试用：模拟「采集进行到一半登录态失效」。
# MOCK_EXPIRE_AFTER=N 表示第 N 次业务接口调用之后开始返回 401；
# 登录态校验接口 getVendorDstList 不受限制，否则重登后的校验也会失败，
# 就无法验证「重登成功后继续采集」这条路径。
_EXPIRE_AFTER = int(os.getenv("MOCK_EXPIRE_AFTER", "0") or 0)
_CALL_COUNT: dict[str, int] = {}
_RELOGGED: dict[str, bool] = {}


def _out_of_window(end: date) -> bool:
    if not HISTORY_START:
        return False
    try:
        return end < date.fromisoformat(HISTORY_START)
    except ValueError:
        return False


def _month_out_of_window(month: str) -> bool:
    """月份类接口的窗口判定：取该月末日与窗口起点比较。"""
    if not HISTORY_START or not month:
        return False
    try:
        y, m = (int(x) for x in month.split("-")[:2])
        last = date(y + 1, 1, 1) - timedelta(days=1) if m == 12 \
            else date(y, m + 1, 1) - timedelta(days=1)
        return _out_of_window(last)
    except (ValueError, TypeError):
        return False


def trendline(account_id: str, payload: dict) -> dict:
    prof = profile(account_id)
    start = date.fromisoformat(payload["startdate"])
    end = date.fromisoformat(payload["enddate"])
    days = (end - start).days + 1

    # 超出历史窗口：上游通常返回空列表而不是报错
    if _out_of_window(end):
        return {"ResponseStatus": SUCCESS, "resultList": []}
    metrics = [m["metric"] for m in payload.get("metricList", [])]
    date_type = (payload.get("tagMap") or {}).get("date_type", "使用日期")
    seed = f"{account_id}:{date_type}:{payload['startdate']}"
    rng = random.Random(seed)

    result_list = []
    for metric in metrics:
        value_map: dict[str, float] = {}
        mom_map: dict[str, float] = {}
        for i in range(days):
            day = (start + timedelta(days=i)).isoformat()
            if "income" in metric:
                value = round(prof["income_base"] * rng.uniform(0.5, 1.6), 2)
            elif metric.endswith("|/"):
                value = round(rng.uniform(0.0, 0.25), 4)
            else:
                value = rng.randint(0, 80)
            value_map[day] = value
            mom_map[day] = round(rng.uniform(-0.5, 1.2), 4)
        result_list.append({
            "metric": metric,
            "dateValueMapList": [{"valueDateMap": value_map}],
            "sublineDataMap": {"MOMRate": {"dataList": [{"valueDateMap": mom_map}]}},
        })
    return {"ResponseStatus": SUCCESS, "resultList": result_list}


def subline(account_id: str, payload: dict) -> dict:
    """聚合接口 ``getKylinDataWithSublineV2``（needAggregate=True）。

    与 ``trendline`` 的关键区别：``valueDateMap`` 只返回**一个 key**
    （区间起始日），值是整个区间的聚合结果。真实携程就是这个行为（已由
    拓包证实：7d 请求 08-06~08-12 仅返回 ``{"2026-08-06": 5.0}``），
    **无论 ``calculate.value`` 是多少都一样**。

    因此日口径的资源数必须逐日发单日请求（``startdate == enddate``）；
    模拟接口也严格按这个语义实现，否则会把「传区间就能拿到每日值」这种
    错误假设掩盖掉。
    """
    prof = profile(account_id)
    start = date.fromisoformat(payload["startdate"])
    end = date.fromisoformat(payload["enddate"])

    if _out_of_window(end):
        return {"ResponseStatus": SUCCESS, "resultList": []}

    date_type = (payload.get("tagMap") or {}).get("date_type", "使用日期")
    unit_value = int((payload.get("calculate") or {}).get("value") or 1)
    days = (end - start).days + 1
    # 聚合接口永远只返回区间起始日一个 key
    single = start.isoformat()

    result_list = []
    for item in payload.get("metricList", []):
        metric = item["metric"]
        aggregate = item.get("aggregateType", "")
        # 种子里带上起止日，使单日请求逐天得到不同值，
        # 且同一天重复请求结果稳定（便于验证幂等）
        rng = random.Random(f"{account_id}:{date_type}:{metric}:{aggregate}:"
                            f"{payload['startdate']}:{payload['enddate']}:{unit_value}")
        value_map: dict[str, float] = {}
        mom_map: dict[str, float] = {}
        if aggregate == "distinct":
            # 成交资源数：去重计数，区间越长值越大
            scale = 1 if days <= 1 else max(1, days // 3)
            value_map[single] = float(rng.randint(0, 6) * scale)
        elif "income" in metric:
            value_map[single] = round(
                prof["income_base"] * rng.uniform(0.5, 1.6) * days, 2)
        elif metric.endswith("|/"):
            value_map[single] = round(rng.uniform(0.0, 0.25), 4)
        else:
            value_map[single] = float(rng.randint(0, 80) * days)
        mom_map[single] = round(rng.uniform(-0.5, 1.2), 4)
        result_list.append({
            "metric": metric,
            "dateValueMapList": [{"valueDateMap": value_map}],
            "sublineDataMap": {"MOMRate": {"dataList": [{"valueDateMap": mom_map}]}},
        })
    return {"ResponseStatus": SUCCESS, "resultList": result_list}


def score_metric(account_id: str, payload: dict) -> dict:
    prof = profile(account_id)
    region_id = (payload.get("dstInfo") or {}).get("regionId", "")
    month = payload.get("month", "")
    if _month_out_of_window(month):
        # 超出窗口：返回空卡片（采集器会视为无数据）
        return {"ResponseStatus": SUCCESS, "serviceScoreMetricCardInfo": None}
    rng = random.Random(f"{account_id}:{region_id}:{month}")
    base = prof["score_base"]
    atomic = [
        {"metricName": name, "score": round(min(5.0, max(1.0, base + rng.uniform(-1.2, 1.2))), 1)}
        for name in RADAR_METRICS
    ]
    return {
        "ResponseStatus": SUCCESS,
        "serviceScoreMetricCardInfo": {
            "values": {"score": round(min(5.0, max(1.0, base + rng.uniform(-0.5, 0.5))), 1)},
            "calculationFormula": {"atomicMetrics": atomic},
        },
    }


def ai_analysis(account_id: str, payload: dict) -> dict:
    month = payload.get("month", "")
    if _month_out_of_window(month):
        return {"ResponseStatus": SUCCESS, "message": ""}
    prof = profile(account_id)
    region_count = len(prof["regions"])
    message = (
        f"### 现状分析\n\n在您运营的 {region_count} 个目的地中，{month} 综合服务分均值约 "
        f"{prof['score_base']:.1f} 分。其中履约缺陷率对总分影响最大（权重 0.5），"
        f"建议优先治理该项指标。\n\n### 改进建议\n\n"
        f"1. 提升订单确认时效，将 24 小时未确认订单比例压降至 3% 以内；\n"
        f"2. 加强 IM 20 秒回复及时率的排班保障；\n"
        f"3. 对高退订率产品做库存与价格复核。\n\n"
        f"（本段文本由模拟服务生成，账号 {account_id}）"
    )
    return {"ResponseStatus": SUCCESS, "message": message}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:  # 降噪
        pass

    # -- 工具 --------------------------------------------------------------
    def _account(self) -> str:
        cookie = self.headers.get("Cookie", "") or ""
        for chunk in cookie.split(";"):
            name, _, value = chunk.strip().partition("=")
            if name == "mock_account":
                return value
        return "A001"

    def _send(self, body: bytes, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, data: dict, status: int = 200) -> None:
        self._send(json.dumps(data, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8", status)

    # -- 路由 --------------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        account_id = self._account()
        if path.startswith("/micro/"):
            self._send(home_html(account_id).encode("utf-8"), "text/html; charset=utf-8")
            return
        if path == "/api/v1/ctrip/accounts":
            self._json({"code": 0, "data": [
                {"account_id": "A001", "account": "vbk_569202", "password": "demo_pwd_A001",
                 "name": "主账号(API)", "status": "active",
                 "scenic_id": "PFTSCA01018088"},
                {"account_id": "A002", "account": "vbk_778001", "password": "demo_pwd_A002",
                 "name": "第二账号(API)", "status": "active"},
            ]})
            return
        self._json({"ResponseStatus": {"Ack": "Failure", "Errors": [{"Message": "not found"}]}}, 404)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        account_id = self._account()
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            payload = {}

        endpoint = path.rstrip("/").rsplit("/", 1)[-1]
        prof = profile(account_id)

        if _EXPIRE_AFTER and endpoint != "getVendorDstList":
            _CALL_COUNT[account_id] = _CALL_COUNT.get(account_id, 0) + 1
            if _CALL_COUNT[account_id] > _EXPIRE_AFTER and not _RELOGGED.get(account_id):
                self._json({"ResponseStatus": {"Ack": "Failure",
                                               "Errors": [{"Message": "session expired"}]}},
                           401)
                return

        if endpoint == "getVendorDstList":
            # 登录态校验能跑通就视为已完成重登，后续业务接口恢复正常
            if _EXPIRE_AFTER and _CALL_COUNT.get(account_id, 0) > _EXPIRE_AFTER:
                _RELOGGED[account_id] = True
            self._json({"ResponseStatus": SUCCESS, "dstList": prof["regions"]})
        elif endpoint == "getVbkBuTypeMenuLinkage":
            code = payload.get("code")
            bu = prof["bu_types"].get(code, [])
            self._json({"ResponseStatus": SUCCESS,
                        "innerVbkBuTypeBaseInfoList": [{"vbkBuType": b} for b in bu]})
        elif endpoint == "getKylinTrendLineData":
            self._json(trendline(account_id, payload))
        elif endpoint == "getKylinDataWithSublineV2":
            self._json(subline(account_id, payload))
        elif endpoint == "getGetVendorClassifyScoreMetric":
            self._json(score_metric(account_id, payload))
        elif endpoint == "getVendorClassifyAIAnalysisData":
            self._json(ai_analysis(account_id, payload))
        else:
            self._json({"ResponseStatus": {"Ack": "Failure",
                                           "Errors": [{"Message": f"unknown endpoint {endpoint}"}]}},
                       404)


def main() -> None:
    parser = argparse.ArgumentParser(description="携程接口模拟服务（本地验证用）")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"mock ctrip server on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
