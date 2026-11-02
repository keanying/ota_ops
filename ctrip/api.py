# -*- coding: utf-8 -*-
"""携程内部接口调用封装。

相比原代码的改进
----------------
* 业务层失败（``ResponseStatus.Ack != Success``）也纳入重试，
  原代码遇到业务失败直接返回空，数据静默丢失且无告警。
* 统一超时、指数退避、结构化日志。
* 会话有效性校验（``verify_session``）同时看 HTTP 状态、跳转目标和真实接口返回，
  不再只用「HTML 长度 > 5000」这种脆弱判据。
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import requests

from .session import get_html


class ApiError(RuntimeError):
    """接口调用失败（含重试后仍失败）。"""


class SessionExpired(ApiError):
    """登录态失效，需要重新登录。"""


class NetworkError(ApiError):
    """网络 / 代理不通。**不是**登录态问题，重新登录也解决不了。

    之前网络错误一律被当成「登录态失效」，于是触发自动重登：
    既白白占用浏览器 profile，又把真正的原因（比如本机代理没开）埋在一堆登录失败日志里。
    """


def _network_hint(exc: Exception) -> str:
    text = str(exc)
    if "ProxyError" in text or "proxy" in text.lower():
        return ("连不上代理服务器。系统或环境变量里配置了代理（如 Clash / v2ray 的 127.0.0.1 端口），"
                "但代理程序没在运行。打开代理程序，或在 config.yaml 的 ctrip.proxy 设为 none 直连")
    if isinstance(exc, requests.Timeout):
        return "请求超时，检查本机网络"
    return "网络不通，检查本机网络 / DNS"


class CtripApi:
    """一个账号对应一个实例，内部持有该账号独占的 Session。"""

    def __init__(self, session: requests.Session, *, api_base: str, home_url: str,
                 account_id: str, logger: logging.Logger,
                 timeout: int = 30, max_retries: int = 3, retry_backoff: float = 5.0,
                 request_interval: float = 1.5) -> None:
        self.session = session
        self.api_base = api_base.rstrip("/")
        self.home_url = home_url
        self.account_id = account_id
        self.log = logger
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.request_interval = request_interval
        self._last_call_at = 0.0

    # -- 基础调用 ----------------------------------------------------------
    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_call_at
        wait = self.request_interval - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_call_at = time.monotonic()

    def post(self, endpoint: str, payload: dict[str, Any], *,
             expect_success: bool = True) -> dict[str, Any]:
        """POST 接口调用，带重试 + 业务状态校验。"""
        url = endpoint if endpoint.startswith("http") else f"{self.api_base}/{endpoint}"
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            try:
                resp = self.session.post(url, json=payload, timeout=self.timeout)
                if resp.status_code in (401, 403):
                    raise SessionExpired(
                        f"[{self.account_id}] 接口 {endpoint} 返回 {resp.status_code}，登录态可能已失效"
                    )
                resp.raise_for_status()
                data = resp.json()
            except SessionExpired:
                raise
            except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
                last_error = exc
                if isinstance(exc, requests.exceptions.ProxyError):
                    # 代理连不上，重试几次结果都一样，别再干等指数退避
                    raise NetworkError(f"[{self.account_id}] 接口 {endpoint}：{_network_hint(exc)}"
                                       f"（{exc}）") from exc
                self.log.warning("[%s] %s 请求异常（第 %d/%d 次）：%s",
                                 self.account_id, endpoint, attempt, self.max_retries, exc)
            else:
                ack = (data.get("ResponseStatus") or {}).get("Ack")
                if not expect_success or ack == "Success":
                    return data
                errors = (data.get("ResponseStatus") or {}).get("Errors") or []
                message = errors[0].get("Message") if errors and isinstance(errors[0], dict) else ""
                last_error = ApiError(f"业务返回 Ack={ack} {message}".strip())
                if _looks_like_auth_error(ack, message):
                    raise SessionExpired(
                        f"[{self.account_id}] 接口 {endpoint} 提示未登录/无权限：{message or ack}"
                    )
                self.log.warning("[%s] %s 业务失败（第 %d/%d 次）：Ack=%s %s",
                                 self.account_id, endpoint, attempt, self.max_retries, ack, message)

            if attempt < self.max_retries:
                wait = self.retry_backoff * (2 ** (attempt - 1))
                self.log.info("[%s] %.0fs 后重试 %s", self.account_id, wait, endpoint)
                time.sleep(wait)

        if isinstance(last_error, (requests.ConnectionError, requests.Timeout)):
            raise NetworkError(f"[{self.account_id}] 接口 {endpoint} 重试 {self.max_retries} 次仍失败："
                               f"{_network_hint(last_error)}（{last_error}）")
        raise ApiError(f"[{self.account_id}] 接口 {endpoint} 重试 {self.max_retries} 次仍失败：{last_error}")

    # -- 会话校验 ----------------------------------------------------------
    def verify_session(self) -> tuple[bool, str]:
        """校验登录态是否有效。返回 ``(是否有效, 说明)``。

        判据（比原实现严格得多）：
        1. 首页 HTTP 200 且未被重定向到登录页；
        2. 真实业务接口 ``getVendorDstList`` 能返回 Success。

        网络 / 代理不通时抛 ``NetworkError``，而不是返回「无效」——
        调用方据此区分「该重新登录」和「该去修网络」。
        """
        try:
            resp = get_html(self.session, self.home_url, timeout=self.timeout)
        except (requests.ConnectionError, requests.Timeout) as exc:
            raise NetworkError(f"访问携程失败：{_network_hint(exc)}（{exc}）") from exc
        except requests.RequestException as exc:
            return False, f"首页请求失败：{exc}"

        final_url = (resp.url or "").lower()
        if resp.status_code != 200:
            return False, f"首页返回状态码 {resp.status_code}"
        if any(k in final_url for k in ("login", "signin", "sign_in", "passport")):
            return False, f"被重定向到登录页：{resp.url}"

        try:
            data = self.post("getVendorDstList", {}, expect_success=True)
        except SessionExpired as exc:
            return False, str(exc)
        except NetworkError:
            raise
        except ApiError as exc:
            return False, f"业务接口校验失败：{exc}"

        dst_count = len(data.get("dstList") or [])
        return True, f"登录态有效（可访问 {dst_count} 个目的地）"

    # -- 业务接口 ----------------------------------------------------------
    def get_home_html(self) -> str:
        resp = get_html(self.session, self.home_url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text

    def get_destination_list(self) -> list[dict[str, Any]]:
        data = self.post("getVendorDstList", {})
        return list(data.get("dstList") or [])

    def get_vbk_bu_types(self, code: int) -> list[dict[str, Any]]:
        data = self.post("getVbkBuTypeMenuLinkage", {"code": code, "isOffline": False},
                         expect_success=False)
        return list(data.get("innerVbkBuTypeBaseInfoList") or [])

    def get_kylin_trendline(self, payload: dict[str, Any]) -> dict[str, Any]:
        """逐日趋势接口（needAggregate=False），一次拉回整个区间的单日明细。"""
        return self.post("getKylinTrendLineData", payload)

    def get_kylin_subline(self, payload: dict[str, Any]) -> dict[str, Any]:
        """区间聚合接口（needAggregate=True）。

        与 ``getKylinTrendLineData`` 的关系：

        * 本接口把整个区间聚合成一个值，返回的 ``valueDateMap``
          只有一个 key（区间起始日），用于近 7 日 / 近 30 日口径；
        * 成交资源数（``suc_resourceid``，queryId=354）只能走本接口，
          因为它需要 ``aggregateType=distinct`` 做资源 ID 去重计数。
        """
        return self.post("getKylinDataWithSublineV2", payload)

    def get_classify_ai_analysis(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.post("getVendorClassifyAIAnalysisData", payload)

    def get_classify_score_metric(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.post("getGetVendorClassifyScoreMetric", payload)

    def get_vendor_profile(self) -> dict[str, Any]:
        """获取账号身份信息，用于账号指纹校验（防 Cookie 串号）。"""
        return self.post("getVendorDstList", {}, expect_success=False)


_AUTH_KEYWORDS = ("未登录", "登录", "无权限", "权限不足", "NotLogin", "Unauthorized", "auth")


def _looks_like_auth_error(ack: Any, message: str) -> bool:
    text = f"{ack} {message}"
    return any(k.lower() in text.lower() for k in _AUTH_KEYWORDS)
