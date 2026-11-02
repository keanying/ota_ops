# -*- coding: utf-8 -*-
"""统一的 ``requests.Session`` 构建。

修复原代码的三处关键缺陷
------------------------
1. **Cookie 域名被破坏**：原 `fetch_daily_kylin.py` 用 ``domain.lstrip(".")``
   去掉了前导点，导致 ``.ctrip.com`` 的 Cookie 不会发往 ``m.ctrip.com``；
   另两个脚本又把 ``vbooking.ctrip.com`` 强行改写成 ``.ctrip.com``，
   把站点专属 Cookie 泄漏成父域 Cookie，语义错误。
   这里改为**原样保留** Playwright 保存的 domain/path，由 requests 自行按域匹配。
2. **请求头不一致**：统一补齐 UA / Origin / Referer / appname，降低风控命中率。
3. **每账号独立 Session**：Session 与 account_id 绑定，绝不共享 Cookie Jar。
"""

from __future__ import annotations

import json
import socket
import threading
from dataclasses import dataclass
from http.cookiejar import Cookie
from urllib.parse import urlparse
from pathlib import Path
from typing import Any

import requests

from .logging_utils import get_logger

logger = get_logger("session")

DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


class SessionError(RuntimeError):
    """会话构建失败。"""


# =============================================================================
# 代理
# =============================================================================

CTRIP_PROBE_URL = "https://vbooking.ctrip.com/"


@dataclass(frozen=True)
class ProxyPlan:
    """这次访问携程走什么路。接口请求与浏览器用同一个决定，避免一边通一边不通。

    kind: system（交给系统/环境变量）| direct（直连）| explicit（指定代理 url）
    """
    kind: str
    url: str = ""
    note: str = ""


_plan_cache: dict[str, ProxyPlan] = {}
_plan_lock = threading.Lock()


def _system_proxy_for(url: str) -> str:
    """系统/环境变量里给这个地址配的代理（Windows 上含注册表里的系统代理）。"""
    proxies = requests.utils.get_environ_proxies(url) or {}
    scheme = urlparse(url).scheme or "https"
    return proxies.get(scheme) or proxies.get("all") or ""


def _reachable(proxy_url: str, timeout: float = 1.5) -> bool:
    parsed = urlparse(proxy_url if "://" in proxy_url else f"http://{proxy_url}")
    host = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme == "https" else 1080 if parsed.scheme.startswith("socks") else 80)
    if not host:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def resolve_proxy(mode: str | None, *, url: str = CTRIP_PROBE_URL) -> ProxyPlan:
    """按配置 ``ctrip.proxy`` 决定访问携程的代理。结果在进程内缓存（只探测一次）。"""
    mode = (mode or "auto").strip()
    key = mode.lower()
    if key in ("none", "direct", "off", "false", "no"):
        return ProxyPlan("direct", note="已配置直连（ctrip.proxy=none）")
    if "://" in mode:
        return ProxyPlan("explicit", mode, f"使用指定代理 {mode}")
    if key == "system":
        return ProxyPlan("system", _system_proxy_for(url), "跟随系统代理")

    with _plan_lock:
        if "auto" in _plan_cache:
            return _plan_cache["auto"]
        sys_proxy = _system_proxy_for(url)
        if not sys_proxy:
            plan = ProxyPlan("system", note="系统未配置代理，直连")
        elif _reachable(sys_proxy):
            plan = ProxyPlan("system", sys_proxy, f"使用系统代理 {sys_proxy}")
        else:
            plan = ProxyPlan("direct", sys_proxy,
                             f"系统代理 {sys_proxy} 连不上（代理程序没开？），已改为直连。"
                             "确实需要代理请先打开代理程序；想固定直连可在 config.yaml 设 ctrip.proxy: none")
            logger.warning(plan.note)
        _plan_cache["auto"] = plan
        return plan


def apply_proxy(session: requests.Session, plan: ProxyPlan) -> None:
    """把代理决定落到 Session 上。

    注意 requests 的合并顺序：``trust_env=True`` 时环境/系统代理会**覆盖**
    ``session.proxies``，所以直连和指定代理都必须关掉 trust_env。
    """
    if plan.kind == "direct":
        session.trust_env = False
        session.proxies.clear()
    elif plan.kind == "explicit":
        session.trust_env = False
        session.proxies.update({"http": plan.url, "https": plan.url})


def _make_cookie(name: str, value: str, domain: str, path: str,
                 secure: bool = False, expires: float | None = None) -> Cookie:
    """构造标准 http.cookiejar.Cookie，正确处理前导点（domain_dot）。"""
    domain = domain or ""
    initial_dot = domain.startswith(".")
    return Cookie(
        version=0,
        name=name,
        value=value,
        port=None,
        port_specified=False,
        domain=domain,
        domain_specified=bool(domain),
        domain_initial_dot=initial_dot,
        path=path or "/",
        path_specified=True,
        secure=bool(secure),
        expires=int(expires) if expires and expires > 0 else None,
        discard=False,
        comment=None,
        comment_url=None,
        rest={"HttpOnly": None},
        rfc2109=False,
    )


def load_storage_state(cookie_file: str | Path) -> dict[str, Any]:
    """读取 Playwright storage_state JSON。"""
    cookie_file = Path(cookie_file)
    if not cookie_file.exists():
        raise SessionError(
            f"登录态文件不存在: {cookie_file}\n"
            f"请先执行: python -m ctrip.cli login --account-id <ID>"
        )
    try:
        with open(cookie_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise SessionError(f"登录态文件不是合法 JSON: {cookie_file}（{exc}）") from exc
    if not isinstance(data, dict) or "cookies" not in data:
        raise SessionError(f"登录态文件缺少 cookies 字段: {cookie_file}")
    return data


def build_session(cookie_file: str | Path, *, user_agent: str = DEFAULT_UA,
                  origin: str = "https://vbooking.ctrip.com",
                  referer: str = "https://vbooking.ctrip.com/",
                  account_id: str = "-",
                  proxy: str = "auto") -> requests.Session:
    """从 storage_state 构建带完整请求头的 Session。"""
    data = load_storage_state(cookie_file)
    cookies = data.get("cookies") or []
    if not cookies:
        raise SessionError(f"登录态文件中没有任何 Cookie: {cookie_file}")

    session = requests.Session()
    apply_proxy(session, resolve_proxy(proxy))
    for c in cookies:
        name, value = c.get("name"), c.get("value")
        if not name or value is None:
            continue
        session.cookies.set_cookie(_make_cookie(
            name=name,
            value=value,
            domain=c.get("domain", ""),
            path=c.get("path", "/"),
            secure=c.get("secure", False),
            expires=c.get("expires"),
        ))

    session.headers.update({
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Content-Type": "application/json;charset=UTF-8",
        "appname": "vbkbusiness",
        "Origin": origin,
        "Referer": referer,
    })
    logger.debug("[%s] Session 构建完成，注入 %d 个 Cookie", account_id, len(cookies))
    return session


def get_html(session: requests.Session, url: str, timeout: int = 20) -> requests.Response:
    """GET 页面 HTML（临时去掉 JSON Content-Type，避免服务端误判）。"""
    headers = {"Accept": "text/html,application/xhtml+xml,*/*;q=0.8", "Content-Type": None}
    merged = {k: v for k, v in headers.items() if v is not None}
    original_ct = session.headers.pop("Content-Type", None)
    try:
        return session.get(url, headers=merged, timeout=timeout, allow_redirects=True)
    finally:
        if original_ct is not None:
            session.headers["Content-Type"] = original_ct
