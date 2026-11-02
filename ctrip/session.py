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
import os
from http.cookiejar import Cookie
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


def use_direct_connection(session: requests.Session) -> None:
    """一律直连，不读系统 / 环境变量里的代理。

    本项目访问携程、推送下游都不需要代理。但 requests 默认会读系统代理
    （Windows 注册表里的那个）：电脑上装过 Clash / v2ray 之类的软件，
    它没开时系统代理设置还在，所有请求就会撞到 127.0.0.1:xxxx 上被拒绝。
    """
    session.trust_env = False
    session.proxies.clear()
    # trust_env=False 会连带忽略 REQUESTS_CA_BUNDLE（自定义根证书），这里手动保留
    ca = os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("CURL_CA_BUNDLE")
    if ca:
        session.verify = ca


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
                  account_id: str = "-") -> requests.Session:
    """从 storage_state 构建带完整请求头的 Session。"""
    data = load_storage_state(cookie_file)
    cookies = data.get("cookies") or []
    if not cookies:
        raise SessionError(f"登录态文件中没有任何 Cookie: {cookie_file}")

    session = requests.Session()
    use_direct_connection(session)
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
