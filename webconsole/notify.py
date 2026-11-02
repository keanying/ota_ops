"""飞书机器人通知 + 事件回调 URL 验证。

两件事
------
1. **发通知**：自定义机器人 Webhook。支持「签名校验」（时间戳 + HMAC-SHA256），
   也支持不开签名。
2. **URL 验证**：把本服务的 ``/feishu/callback`` 填进飞书「事件订阅」时，
   飞书会先发一个 ``url_verification`` 请求，要求原样回 ``challenge``。
   开了 Encrypt Key 的话请求体是 ``{"encrypt": "..."}``，需要先解密。
   两种情况这里都处理了。

配置存在 ``webconsole/settings.json``，可在控制台页面上改，不用动代码。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import requests

from ctrip.logging_utils import get_logger

logger = get_logger("notify")

SETTINGS_FILE = Path(__file__).resolve().parent / "settings.json"
_lock = threading.Lock()


# =============================================================================
# 配置
# =============================================================================

@dataclass
class FeishuConfig:
    enabled: bool = False
    webhook: str = ""            # 自定义机器人 Webhook 地址
    secret: str = ""             # 签名校验密钥（机器人安全设置里开启后才有）
    verify_token: str = ""       # 事件订阅 Verification Token
    encrypt_key: str = ""        # 事件订阅 Encrypt Key（开了才填）
    console_url: str = ""        # 通知里「进入此链接验证」的地址前缀
    platform: str = "携程 VBooking"


@dataclass
class LoginSelectors:
    """登录流程里用到的选择器，页面改版时在页面上改，不用发版。"""

    sms_send_texts: list[str] = field(default_factory=lambda: [
        "获取验证码", "发送验证码", "获取短信验证码", "免费获取验证码", "重新发送",
    ])
    sms_input_selectors: list[str] = field(default_factory=lambda: [
        'input[placeholder*="验证码"]', 'input[name*="code"]', 'input[id*="code"]',
        'input[name*="captcha"]', 'input[maxlength="6"]',
    ])
    sms_page_hints: list[str] = field(default_factory=lambda: [
        "短信验证", "手机验证", "验证码已发送", "接收验证码", "安全验证",
    ])
    captcha_hints: list[str] = field(default_factory=lambda: [
        "请拖动滑块", "滑动验证", "请完成验证", "拖动下方滑块", "点击按钮开始验证",
    ])
    captcha_selectors: list[str] = field(default_factory=lambda: [
        'iframe[src*="captcha"]', '[class*="slider"]', '[class*="captcha"]',
        '[id*="captcha"]', 'canvas[class*="verify"]',
    ])


@dataclass
class Settings:
    feishu: FeishuConfig = field(default_factory=FeishuConfig)
    selectors: LoginSelectors = field(default_factory=LoginSelectors)
    sync: dict = field(default_factory=dict)      # 数据同步，结构见 datasync.SyncConfig
    collect: dict = field(default_factory=dict)   # 定时采集，结构见 collect_schedule

    @classmethod
    def load(cls) -> "Settings":
        if not SETTINGS_FILE.exists():
            return cls()
        try:
            raw = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("读取 settings.json 失败，使用默认配置：%s", exc)
            return cls()
        return cls(
            feishu=FeishuConfig(**{**asdict(FeishuConfig()), **(raw.get("feishu") or {})}),
            selectors=LoginSelectors(
                **{**asdict(LoginSelectors()), **(raw.get("selectors") or {})}),
            sync=dict(raw.get("sync") or {}),
            collect=dict(raw.get("collect") or {}),
        )

    def save(self) -> None:
        with _lock:
            SETTINGS_FILE.write_text(
                json.dumps({"feishu": asdict(self.feishu),
                            "selectors": asdict(self.selectors),
                            "sync": self.sync,
                            "collect": self.collect},
                           ensure_ascii=False, indent=2),
                encoding="utf-8")
        try:
            SETTINGS_FILE.chmod(0o600)   # 里面有 secret，别让同机其他用户读到
        except OSError:
            pass

    def masked(self) -> dict:
        """给前端看的版本：密钥只留首尾，避免页面上明文回显。"""
        def mask(v: str) -> str:
            if not v:
                return ""
            return v[:4] + "…" + v[-4:] if len(v) > 12 else "…" * 3

        f = asdict(self.feishu)
        for k in ("secret", "verify_token", "encrypt_key"):
            f[k] = mask(f[k])
        f["webhook_set"] = bool(self.feishu.webhook)
        f["webhook"] = mask(self.feishu.webhook) if self.feishu.webhook else ""
        return {"feishu": f, "selectors": asdict(self.selectors)}


def update_settings(patch: dict) -> Settings:
    """局部更新。前端回传的掩码值（含 …）视为「未修改」，不覆盖原值。"""
    cur = Settings.load()
    fp = patch.get("feishu") or {}
    for k, v in fp.items():
        if not hasattr(cur.feishu, k):
            continue
        if isinstance(v, str) and "…" in v:
            continue                      # 掩码回传，保持原值
        setattr(cur.feishu, k, v)
    sp = patch.get("selectors") or {}
    for k, v in sp.items():
        if hasattr(cur.selectors, k) and isinstance(v, list):
            setattr(cur.selectors, k, [str(x) for x in v if str(x).strip()])
    if "collect" in patch and isinstance(patch["collect"], dict):
        cur.collect = {**cur.collect, **patch["collect"]}
    if "sync" in patch and isinstance(patch["sync"], dict):
        merged = dict(cur.sync)
        for k, v in patch["sync"].items():
            # 掩码回传视为未修改，与飞书密钥同样处理
            if k == "key" and isinstance(v, str) and "…" in v:
                continue
            merged[k] = v
        cur.sync = merged
    cur.save()
    return cur


# =============================================================================
# 发送
# =============================================================================

def _sign(secret: str, timestamp: int) -> str:
    """飞书签名：以 "{timestamp}\\n{secret}" 为密钥、空串为消息做 HMAC-SHA256。"""
    key = f"{timestamp}\n{secret}".encode("utf-8")
    return base64.b64encode(hmac.new(key, b"", hashlib.sha256).digest()).decode()


def build_verify_text(account_id: str, account: str, platform: str,
                      link: str, reason: str = "") -> str:
    lines = [
        "OTA需验证：",
        f"账号:{account or account_id}",
        f"平台:{platform}",
        f"进入此链接验证:{link}",
    ]
    if reason:
        lines.insert(3, f"验证类型:{reason}")
    return "\n".join(lines)


def send_text(text: str, cfg: FeishuConfig | None = None,
              timeout: int = 10) -> tuple[bool, str]:
    cfg = cfg or Settings.load().feishu
    if not cfg.enabled:
        return False, "飞书通知未启用"
    if not cfg.webhook:
        return False, "未配置 Webhook 地址"

    body: dict[str, Any] = {"msg_type": "text", "content": {"text": text}}
    if cfg.secret:
        ts = int(time.time())
        body["timestamp"] = str(ts)
        body["sign"] = _sign(cfg.secret, ts)

    try:
        resp = requests.post(cfg.webhook, json=body, timeout=timeout)
        data = resp.json() if resp.content else {}
    except requests.exceptions.RequestException as exc:
        logger.warning("飞书通知发送失败：%s", exc)
        return False, f"请求失败：{exc}"
    except json.JSONDecodeError:
        return False, f"返回不是 JSON（HTTP {resp.status_code}）"

    # 自定义机器人成功时 code=0；失败时 code/msg 里有原因
    code = data.get("code", data.get("StatusCode", 0))
    if code in (0, None):
        return True, "已发送"
    msg = data.get("msg") or data.get("StatusMessage") or str(data)
    logger.warning("飞书返回错误：%s", msg)
    return False, f"飞书返回 code={code} {msg}"


def notify_verify_needed(account_id: str, account: str, link: str,
                         reason: str = "") -> tuple[bool, str]:
    cfg = Settings.load().feishu
    text = build_verify_text(account_id, account, cfg.platform, link, reason)
    return send_text(text, cfg)


# =============================================================================
# 事件回调 URL 验证
# =============================================================================

def decrypt_event(encrypt: str, encrypt_key: str) -> dict:
    """飞书事件加密：AES-256-CBC，key=sha256(encrypt_key)，IV 是密文前 16 字节。"""
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    raw = base64.b64decode(encrypt)
    key = hashlib.sha256(encrypt_key.encode("utf-8")).digest()
    iv, body = raw[:16], raw[16:]
    decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
    plain = decryptor.update(body) + decryptor.finalize()
    plain = plain[:-plain[-1]]                     # 去 PKCS#7 填充
    return json.loads(plain.decode("utf-8"))


def handle_callback(payload: dict) -> tuple[dict, int]:
    """处理飞书事件回调。返回 (响应体, HTTP 状态码)。

    只实现 URL 验证：把回调地址填进飞书后台时会用到。业务事件暂不处理，
    一律回 200，避免飞书因为收不到 200 而重复投递。
    """
    cfg = Settings.load().feishu

    if "encrypt" in payload:
        if not cfg.encrypt_key:
            return {"error": "收到加密事件，但未配置 Encrypt Key"}, 400
        try:
            payload = decrypt_event(payload["encrypt"], cfg.encrypt_key)
        except Exception as exc:  # noqa: BLE001
            logger.warning("飞书事件解密失败：%s", exc)
            return {"error": f"解密失败：{exc}"}, 400

    if payload.get("type") == "url_verification":
        token = payload.get("token", "")
        if cfg.verify_token and token != cfg.verify_token:
            logger.warning("飞书 URL 验证 token 不匹配")
            return {"error": "token 不匹配"}, 403
        logger.info("飞书 URL 验证通过")
        return {"challenge": payload.get("challenge", "")}, 200

    return {"code": 0}, 200
