# -*- coding: utf-8 -*-
"""登录与登录态管理（每账号完全隔离）。

修复原 ``scraper.py`` 的问题
---------------------------
1. 账号密码不再硬编码，改由配置/环境变量/账号 API 提供。
2. 每账号独立 ``auth/{account_id}/storage_state.json`` + ``meta.json``。
3. ``is_visible(timeout=...)`` 是错误用法（该方法不接受 timeout），
   改用 ``wait_for(state="visible", timeout=...)``。
4. 弹窗处理放进等待跳转的轮询循环内，避免弹窗挡住跳转判定导致误判超时。
5. 登录成功判定为「URL 到达后台 + 真实业务接口可用」双重校验，
   而不是只看 URL 里有没有 ``dashboard``。
6. 失败时自动落盘截图与 HTML，便于排障。
7. 支持 headless 运行（注入反自动化检测脚本）；识别到需要人工验证码时
   给出明确提示并落盘证据，而不是静默超时。
8. 无交互式 ``input()``，可安全用于定时任务；需要人工介入时用 ``--no-headless``。
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .config import AccountConfig, AppConfig
from .logging_utils import get_logger

logger = get_logger("auth")

# 反自动化检测脚本已移至 browser.py（与浏览器启动参数放在一起维护）

_USERNAME_SELECTORS = [
    'input[name="userName"]', 'input[name="username"]', 'input[name="account"]',
    'input[name="loginName"]', 'input[placeholder*="账号"]', 'input[placeholder*="用户名"]',
    'input[id*="account"]', 'input[id*="user"]',
    'form input[type="text"]:visible', 'input[type="text"]',
]
_PASSWORD_SELECTORS = [
    'input[name="password"]', 'input[name="passWord"]',
    'input[placeholder*="密码"]', 'input[type="password"]',
]
# 「登 录」中间可能有空格，用正则匹配更稳
_LOGIN_BUTTON_PATTERNS = [r"^登\s*录$", r"^立即登录$", r"^登录$"]
# 登录后可能出现的干扰弹窗按钮
_DISMISS_BUTTON_TEXTS = ["暂不修改", "以后再说", "稍后处理", "我知道了", "知道了", "取消", "关闭"]
_CAPTCHA_HINTS = ["请拖动滑块", "验证码", "安全验证", "滑动验证", "请完成验证"]


class LoginError(RuntimeError):
    """登录失败。"""


class CaptchaRequired(LoginError):
    """需要人工完成验证码/滑块。"""


@dataclass
class LoginResult:
    account_id: str
    success: bool
    message: str
    cookie_file: Path | None = None
    evidence: list[Path] | None = None


# =============================================================================
# 登录态存储（接口化，后续可换 Redis / DB 而不影响调用方）
# =============================================================================

class CookieStore:
    """基于本地文件的登录态存储，按 account_id 分目录，天然隔离。"""

    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg

    def path(self, account_id: str) -> Path:
        return self.cfg.cookie_file(account_id)

    def meta_path(self, account_id: str) -> Path:
        return self.cfg.cookie_meta_file(account_id)

    def exists(self, account_id: str) -> bool:
        return self.path(account_id).exists()

    def load_meta(self, account_id: str) -> dict[str, Any]:
        p = self.meta_path(account_id)
        if not p.exists():
            return {}
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}

    def save_meta(self, account_id: str, **kwargs: Any) -> None:
        meta = self.load_meta(account_id)
        meta.update(kwargs)
        meta["account_id"] = account_id
        meta["updated_at"] = datetime.now().isoformat(timespec="seconds")
        with open(self.meta_path(account_id), "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        # 登录态含敏感凭据，限制文件权限
        for p in (self.path(account_id), self.meta_path(account_id)):
            if p.exists():
                try:
                    os.chmod(p, 0o600)
                except OSError:
                    pass

    def is_fresh(self, account_id: str) -> bool:
        """登录态是否在 TTL 内（仅做快速判断，真实有效性仍需接口校验）。"""
        if not self.exists(account_id):
            return False
        meta = self.load_meta(account_id)
        stamp = meta.get("login_at") or meta.get("updated_at")
        if not stamp:
            return False
        try:
            login_at = datetime.fromisoformat(stamp)
        except ValueError:
            return False
        ttl = timedelta(hours=self.cfg.ctrip.cookie_ttl_hours)
        return datetime.now() - login_at <= ttl

    def delete(self, account_id: str) -> None:
        for p in (self.path(account_id), self.meta_path(account_id)):
            if p.exists():
                p.unlink()


# =============================================================================
# Playwright 登录
# =============================================================================

def _first_visible(page, selectors: list[str], timeout: int = 2000):
    """返回第一个可见元素定位器，找不到返回 None。"""
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            locator.wait_for(state="visible", timeout=timeout)
            return locator
        except Exception:  # noqa: BLE001 - Playwright 抛的是 TimeoutError 家族
            continue
    return None


def _dismiss_popups(page, log: logging.Logger) -> None:
    """关闭「密码修改提示」等干扰弹窗。在轮询循环里反复调用。"""
    import re as _re

    for text in _DISMISS_BUTTON_TEXTS:
        try:
            btn = page.get_by_role("button", name=_re.compile(f"^{text}$")).first
            btn.wait_for(state="visible", timeout=600)
            btn.click()
            log.info("已关闭弹窗按钮：%s", text)
            page.wait_for_timeout(500)
        except Exception:  # noqa: BLE001
            continue


def _detect_captcha(page) -> str | None:
    try:
        body_text = page.inner_text("body", timeout=3000)
    except Exception:  # noqa: BLE001
        return None
    for hint in _CAPTCHA_HINTS:
        if hint in body_text:
            return hint
    return None


def _dump_evidence(page, out_dir: Path, tag: str, log: logging.Logger) -> list[Path]:
    """落盘截图 + HTML，便于事后排查登录失败原因。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    files: list[Path] = []
    shot = out_dir / f"{tag}_{stamp}.png"
    html = out_dir / f"{tag}_{stamp}.html"
    try:
        page.screenshot(path=str(shot), full_page=True)
        files.append(shot)
    except Exception as exc:  # noqa: BLE001
        log.debug("截图失败：%s", exc)
    try:
        html.write_text(page.content(), encoding="utf-8")
        files.append(html)
    except Exception as exc:  # noqa: BLE001
        log.debug("保存 HTML 失败：%s", exc)
    if files:
        log.info("已保存现场证据：%s", ", ".join(str(f) for f in files))
    return files


def _fill_credentials(page, account: AccountConfig, log: logging.Logger) -> bool:
    """填写账号密码并勾选协议。"""
    user_input = _first_visible(page, _USERNAME_SELECTORS, timeout=4000)
    if user_input is None:
        log.error("未找到账号输入框")
        return False
    user_input.fill(account.account)
    log.info("已填入登录账号")

    pwd_input = _first_visible(page, _PASSWORD_SELECTORS, timeout=4000)
    if pwd_input is None:
        log.error("未找到密码输入框")
        return False
    pwd_input.fill(account.password)
    log.info("已填入密码")

    # 勾选「阅读并同意」：优先点 label（携程用自定义样式，直接 check input 常常无效）
    try:
        checkbox = page.locator('input[type="checkbox"]').first
        checkbox.wait_for(state="attached", timeout=1500)
        if not checkbox.is_checked():
            try:
                checkbox.check(force=True, timeout=2000)
            except Exception:  # noqa: BLE001
                page.locator('label:has(input[type="checkbox"])').first.click(timeout=2000)
            log.info("已勾选用户协议")
    except Exception:  # noqa: BLE001
        log.debug("未发现协议勾选框，跳过")
    return True


def _click_login(page, log: logging.Logger) -> bool:
    import re as _re

    for pattern in _LOGIN_BUTTON_PATTERNS:
        try:
            btn = page.get_by_role("button", name=_re.compile(pattern)).first
            btn.wait_for(state="visible", timeout=2500)
            btn.click()
            log.info("已点击登录按钮（%s）", pattern)
            return True
        except Exception:  # noqa: BLE001
            continue
    # 兜底：表单回车提交
    try:
        page.keyboard.press("Enter")
        log.info("未匹配到登录按钮，改用回车提交")
        return True
    except Exception as exc:  # noqa: BLE001
        log.error("提交登录表单失败：%s", exc)
        return False


def _is_logged_in_url(url: str) -> bool:
    url = (url or "").lower()
    if any(k in url for k in ("login", "signin", "sign_in", "passport")):
        return False
    return "vbooking.ctrip.com" in url and ("ivbk" in url or "dashboard" in url or "micro" in url)


def login(cfg: AppConfig, account: AccountConfig, log: logging.Logger | None = None,
          *, headless: bool | None = None, keep_alive: bool = False) -> LoginResult:
    """执行一次登录并保存该账号独立的登录态。

    浏览器使用项目内的持久化 profile（``browser/{account_id}/profile``），
    保留设备指纹与本地存储，因此后续登录不会被当成「陌生设备」
    而反复触发滑块验证。若 profile 里已是登录态，会直接跳过填表单环节。
    """
    from .browser import BrowserBusyError, open_session

    log = log or logger
    if not account.account or not account.password:
        return LoginResult(account.account_id, False,
                           "账号或密码为空：请通过环境变量注入（如 CTRIP_A001_PASSWORD）")

    headless = cfg.ctrip.headless if headless is None else headless
    cookie_store = CookieStore(cfg)
    cookie_file = cookie_store.path(account.account_id)
    evidence_dir = cfg.log_dir(account.account_id) / "login"
    deadline = time.monotonic() + cfg.ctrip.login_timeout
    evidence: list[Path] = []

    log.info("开始登录账号 %s（headless=%s，超时 %ds）",
             account.account_id, headless, cfg.ctrip.login_timeout)

    try:
        session = open_session(cfg, account.account_id, headless=headless,
                               keep_alive=keep_alive, log=log)
    except BrowserBusyError as exc:
        return LoginResult(account.account_id, False, str(exc))
    except Exception as exc:  # noqa: BLE001
        return LoginResult(account.account_id, False, f"浏览器启动失败：{exc}")

    context = session.context
    page = session.page()

    try:
        page.goto(cfg.ctrip.login_url, wait_until="domcontentloaded",
                  timeout=cfg.ctrip.nav_timeout * 1000)
        page.wait_for_timeout(2500)
        _dismiss_popups(page, log)

        # profile 里已是登录态（持久化浏览器的主要收益）则跳过填表单
        if _is_logged_in_url(page.url) and _first_visible(page, _PASSWORD_SELECTORS, 1500) is None:
            log.info("检测到浏览器侧已处于登录态，直接保存")
        else:
            if not _fill_credentials(page, account, log):
                evidence = _dump_evidence(page, evidence_dir, "fill_failed", log)
                return LoginResult(account.account_id, False,
                                   "未能定位登录表单（页面结构可能已变更，请查看落盘证据）",
                                   evidence=evidence)
            if not _click_login(page, log):
                evidence = _dump_evidence(page, evidence_dir, "submit_failed", log)
                return LoginResult(account.account_id, False, "登录表单提交失败",
                                   evidence=evidence)

            # 轮询等待跳转，期间持续清理弹窗、检测验证码
            logged_in = False
            captcha_hint: str | None = None
            while time.monotonic() < deadline:
                page.wait_for_timeout(1500)
                _dismiss_popups(page, log)
                if _is_logged_in_url(page.url):
                    logged_in = True
                    break
                hint = _detect_captcha(page)
                if hint and hint != captcha_hint:
                    captcha_hint = hint
                    log.warning("检测到人工验证环节：%s", hint)
                    if headless:
                        evidence = _dump_evidence(page, evidence_dir, "captcha", log)
                        raise CaptchaRequired(
                            f"需要人工完成「{hint}」验证。请在有图形界面的环境执行："
                            f"python -m ctrip.cli login --account-id {account.account_id} --no-headless"
                        )
                    log.warning("请在浏览器窗口中手动完成验证，剩余 %.0fs",
                                deadline - time.monotonic())
            if not logged_in:
                evidence = _dump_evidence(page, evidence_dir, "timeout", log)
                return LoginResult(account.account_id, False,
                                   f"等待登录跳转超时，当前地址：{page.url}",
                                   evidence=evidence)

        # 进入后台首页，确保业务域 Cookie 也被写入
        try:
            page.goto(cfg.ctrip.home_url, wait_until="domcontentloaded",
                      timeout=cfg.ctrip.nav_timeout * 1000)
            page.wait_for_timeout(2500)
            _dismiss_popups(page, log)
        except Exception as exc:  # noqa: BLE001
            log.warning("访问数据参谋首页失败（不影响登录态保存）：%s", exc)

        cookie_file.parent.mkdir(parents=True, exist_ok=True)
        context.storage_state(path=str(cookie_file))
        cookie_store.save_meta(
            account.account_id,
            login_at=datetime.now().isoformat(timespec="seconds"),
            login_name=account.account,
            headless=headless,
        )
        log.info("登录态已保存：%s", cookie_file)

    except CaptchaRequired as exc:
        return LoginResult(account.account_id, False, str(exc), evidence=evidence)
    except Exception as exc:  # noqa: BLE001
        evidence = _dump_evidence(page, evidence_dir, "error", log)
        return LoginResult(account.account_id, False, f"登录异常：{exc}", evidence=evidence)
    finally:
        session.close()

    # 用 requests 复验登录态（真正的成功判据）
    ok, detail = verify_cookie(cfg, account.account_id, log)
    if not ok:
        return LoginResult(account.account_id, False, f"登录态保存后校验未通过：{detail}",
                           cookie_file=cookie_file)
    return LoginResult(account.account_id, True, detail, cookie_file=cookie_file)


# =============================================================================
# 登录态校验 / 自动续期
# =============================================================================

def build_api(cfg: AppConfig, account_id: str, log: logging.Logger):
    """为某账号构建 API 客户端（独占 Session）。"""
    from .api import CtripApi
    from .session import build_session

    session = build_session(
        cfg.cookie_file(account_id),
        user_agent=cfg.collect.user_agent,
        origin="https://vbooking.ctrip.com",
        referer="https://vbooking.ctrip.com/",
        account_id=account_id,
    )
    return CtripApi(
        session,
        api_base=cfg.ctrip.api_base,
        home_url=cfg.ctrip.home_url,
        account_id=account_id,
        logger=log,
        timeout=cfg.collect.timeout,
        max_retries=cfg.collect.max_retries,
        retry_backoff=cfg.collect.retry_backoff,
        request_interval=cfg.collect.request_interval,
    )


def verify_cookie(cfg: AppConfig, account_id: str,
                  log: logging.Logger | None = None) -> tuple[bool, str]:
    """校验某账号登录态是否可用，并顺手更新账号指纹。"""
    log = log or logger
    store = CookieStore(cfg)
    if not store.exists(account_id):
        return False, "登录态文件不存在"
    try:
        api = build_api(cfg, account_id, log)
    except Exception as exc:  # noqa: BLE001
        return False, f"登录态不可用：{exc}"

    ok, detail = api.verify_session()
    if ok:
        # 只更新校验时间；**不在此处写指纹**。
        # 指纹是防“Cookie 串号”的基准，一旦在每次校验时无条件覆写，
        # 就会把串号后的新身份认作基准，导致 check_fingerprint 永远“一致”。
        store.save_meta(account_id,
                        last_check_at=datetime.now().isoformat(timespec="seconds"))
    return ok, detail


def _account_fingerprint(api) -> dict[str, Any]:
    """用目的地列表作为账号身份指纹，用于发现 Cookie 串号。"""
    dst_list = api.get_destination_list()
    region_ids = sorted(str(d.get("regionId")) for d in dst_list if d.get("regionId"))
    return {"region_count": len(region_ids), "region_ids": region_ids[:50]}


def check_fingerprint(cfg: AppConfig, account_id: str, api,
                      log: logging.Logger) -> tuple[bool, str]:
    """比对当前登录态的身份指纹与历史记录，防止不同账号 Cookie 混用导致数据串号。"""
    store = CookieStore(cfg)
    meta = store.load_meta(account_id)
    old = meta.get("fingerprint") or {}
    try:
        new = _account_fingerprint(api)
    except Exception as exc:  # noqa: BLE001
        return True, f"指纹获取失败，跳过校验：{exc}"

    old_ids = set(old.get("region_ids") or [])
    new_ids = set(new.get("region_ids") or [])

    if not old_ids:
        store.save_meta(account_id, fingerprint=new)
        return True, f"首次记录账号指纹（{len(new_ids)} 个目的地）"
    if not new_ids:
        return True, "当前未获取到目的地，跳过指纹校验"

    if old_ids & new_ids:
        # 身份吻合：取并集作为新基准（应对正常的目的地新增/下线），
        # 但不直接用新值覆盖，避免基准被一次异常登录洗掉。
        merged = sorted(old_ids | new_ids)[:50]
        store.save_meta(account_id, fingerprint={"region_count": len(merged),
                                                 "region_ids": merged})
        return True, f"账号指纹一致（共同目的地 {len(old_ids & new_ids)} 个）"

    return False, (
        f"账号指纹不一致！历史目的地 {sorted(old_ids)[:5]}，"
        f"当前 {sorted(new_ids)[:5]}，无任何交集。很可能是登录态串号，"
        f"已拒绝本次采集以保护账号数据隔离；"
        f"如确实是账号权限变更，请删除 auth/{account_id}/meta.json 中的 fingerprint 后重跑"
    )


def ensure_logged_in(cfg: AppConfig, account: AccountConfig,
                     log: logging.Logger | None = None,
                     *, force_relogin: bool = False,
                     headless: bool | None = None,
                     keep_alive: bool = False) -> LoginResult:
    """确保账号处于可用登录态：优先复用，失效则自动重登（带重试）。"""
    log = log or logger
    account_id = account.account_id
    store = CookieStore(cfg)

    if not force_relogin and store.exists(account_id):
        if store.is_fresh(account_id):
            ok, detail = verify_cookie(cfg, account_id, log)
            if ok:
                log.info("[%s] 复用已有登录态：%s", account_id, detail)
                return LoginResult(account_id, True, detail, cookie_file=store.path(account_id))
            log.warning("[%s] 已有登录态失效：%s", account_id, detail)
        else:
            log.info("[%s] 登录态超过 %dh 未刷新，重新登录",
                     account_id, cfg.ctrip.cookie_ttl_hours)

    attempts = max(1, cfg.ctrip.login_retry)
    last: LoginResult | None = None
    for attempt in range(1, attempts + 1):
        log.info("[%s] 自动登录第 %d/%d 次", account_id, attempt, attempts)
        last = login(cfg, account, log, headless=headless, keep_alive=keep_alive)
        if last.success:
            return last
        log.warning("[%s] 登录失败：%s", account_id, last.message)
        if isinstance(last.message, str) and "人工完成" in last.message:
            break  # 需要人工验证码，重试无意义
        if attempt < attempts:
            time.sleep(5)
    return last or LoginResult(account_id, False, "登录未执行")
