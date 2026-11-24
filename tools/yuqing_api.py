#!/usr/bin/env python3
"""舆情平台第三方接口客户端。

调用顺序（main 里就是按这个顺序跑的）
--------------------------------------
1. 登录             /api/third/login                   拿 token + userId（后面所有接口都要）
2. 专题列表         /api/third/special/list            拿专题 id，文章列表可按专题过滤
3. 文章列表         /api/third/special/queryDocList    按时间窗口分页拉文章
4. 文章详情         /api/third/article/detail          用第 3 步的 id / actileId 批量取正文
5. 热点数据         /api/third/special/hotTopN         时间窗口内热度前 N
6. 专题预警         /api/third/special/warning         分页拉预警
7. 融媒体数据       /api/third/fusionMedia/queryList   按时间窗口 + 关键词等分页拉

怎么用
------
* 公共参数（域名、imei、账号密码、签名密钥等）在下面「公共参数」一节里改，
  也可以用同名环境变量（YUQING_BASE_URL 等）注入，免得把密码写进代码；
* 每次运行的动态参数（时间窗口、媒体类型、关键词、跑哪几步…）在 ``main()`` 开头改；
* 运行：``python tools/yuqing_api.py``；结果按步骤存成 JSON，放在 ``OUTPUT_DIR`` 下；
* 只算签名不发请求（拿对方给的示例核对加密方式）::

      python tools/yuqing_api.py sign 123123 222222 1604664285307

几个刻意的处理
--------------
* **登录一天只能 50 次**：token 落盘（``STATE_FILE``）复用，只有接口说 token 失效
  或超过 ``TOKEN_MAX_AGE_HOURS`` 才重新登录；
* **每分钟不超过 10 次**：所有请求共用一个 60 秒滑动窗口限速，到上限就等；
* **每日次数**：按接口记数并落盘，快到 ``DAILY_LIMITS`` 时直接报错，不去撞对方的封禁；
  专题列表一天只能调 10 次，当天的结果会缓存起来复用；
* **签名的加密细节文档没写全**（3DES 的模式、填充、输出编码），按 Java DESede 的默认值
  实现，全部做成参数；对方给的示例对不上时只改「签名加密方式」那几行即可。
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import sys
import time
from collections import deque
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# =============================================================================
# 公共参数（在这里改；也可用同名环境变量注入）
# =============================================================================

# 接口域名，带协议、不带结尾的 /，例如 "https://yq.example.com"
BASE_URL = os.environ.get("YUQING_BASE_URL", "")
# 接口提供方分配的 imei，所有请求都带在请求头里
IMEI = os.environ.get("YUQING_IMEI", "")
# 登录账号；密码填明文，代码里自动做 md5（已经是 md5 的话把 PASSWORD_IS_MD5 改成 True）
LOGIN_NAME = os.environ.get("YUQING_LOGIN_NAME", "")
PASSWORD = os.environ.get("YUQING_PASSWORD", "")
PASSWORD_IS_MD5 = False
PASSWORD_MD5_UPPER = False          # md5 结果用大写十六进制
# 签名密钥（对方提供）
SIGN_KEY = os.environ.get("YUQING_SIGN_KEY", "")

# ---- 签名加密方式（文档只写了「des3 加密」，以下按 Java DESede 默认值）----
SIGN_KEY_FORMAT = "utf8"            # utf8：密钥字符串按 UTF-8 取前 24 字节；hex：按十六进制解码
SIGN_CIPHER_MODE = "ECB"            # ECB | CBC
SIGN_IV = b"\0" * 8                 # 仅 CBC 用，8 字节
SIGN_OUTPUT = "hex"                 # hex（小写）| HEX（大写）| base64

# ---- 请求 ----
HTTP_METHOD = "POST"                # 文档没写，按 POST + JSON 请求体
TIMEOUT = 30                        # 单次请求超时（秒）
REQUEST_PER_MINUTE = 10             # 文档：1 分钟不超过 10 次
TOKEN_MAX_AGE_HOURS = 24            # token 有效期文档没写；超过这个时间主动重新登录
MAX_PAGE_SIZE = 200                 # 文档：列表 / 详情一次最多 200 条

# 每日调用上限（文档「接口调用限制」）；没列出的接口不限
DAILY_LIMITS = {
    "login": 50,
    "doc_list": 10000,
    "article_detail": 10000,
    "special_list": 10,
}

# token、每日计数、专题缓存（含 token，已在 .gitignore 的 auth/ 下）
STATE_FILE = PROJECT_ROOT / "auth" / "yuqing_state.json"
# 拉下来的数据（已在 .gitignore 的 data/output/ 下）
OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "yuqing"

# 接口路径
PATHS = {
    "login": "/api/third/login",
    "doc_list": "/api/third/special/queryDocList",
    "article_detail": "/api/third/article/detail",
    "special_list": "/api/third/special/list",
    "hot_top_n": "/api/third/special/hotTopN",
    "warning": "/api/third/special/warning",
    "fusion_media": "/api/third/fusionMedia/queryList",
}

# 媒体类型（groupId / mediaType）
MEDIA_TYPES = {
    10: "网站", 17: "论坛", 14: "博客", 19: "微博", 23: "微信", 24: "客户端",
    21: "视频", 25: "贴吧", 250: "电子报", 99: "其他", 251: "融媒体",
    252: "电视台", 253: "短视频", 254: "评论",
}

TIME_FMT = "%Y-%m-%d %H:%M:%S"

logger = logging.getLogger("yuqing")


class YuqingError(RuntimeError):
    """接口返回失败（code != 200）、网络错误或超出调用上限。"""


class TokenExpired(YuqingError):
    """token 失效，需要重新登录。"""


# =============================================================================
# 签名
# =============================================================================

def md5_hex(text: str, upper: bool = False) -> str:
    h = hashlib.md5(text.encode("utf-8")).hexdigest()
    return h.upper() if upper else h


def _triple_des():
    # cryptography 43 起把 TripleDES 挪到了 decrepit 模块
    try:
        from cryptography.hazmat.decrepit.ciphers.algorithms import TripleDES
    except ImportError:  # 旧版本
        from cryptography.hazmat.primitives.ciphers.algorithms import TripleDES
    return TripleDES


def _sign_key_bytes(key: str) -> bytes:
    if SIGN_KEY_FORMAT == "hex":
        raw = bytes.fromhex(key.replace(" ", ""))
    else:
        raw = key.encode("utf-8")
    if len(raw) >= 24:
        return raw[:24]                  # 与 Java DESedeKeySpec 一致：取前 24 字节
    if len(raw) == 16:
        return raw + raw[:8]             # 双倍长密钥 K1K2 → K1K2K1
    raise YuqingError(f"签名密钥长度不对：{len(raw)} 字节（需要 16 或 ≥24）")


def des3_encrypt(text: str, key: str) -> str:
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, modes

    padder = padding.PKCS7(64).padder()  # 8 字节块的 PKCS7 即 PKCS5
    data = padder.update(text.encode("utf-8")) + padder.finalize()
    mode = modes.CBC(SIGN_IV) if SIGN_CIPHER_MODE.upper() == "CBC" else modes.ECB()
    enc = Cipher(_triple_des()(_sign_key_bytes(key)), mode).encryptor()
    out = enc.update(data) + enc.finalize()
    if SIGN_OUTPUT == "base64":
        return base64.b64encode(out).decode("ascii")
    return out.hex().upper() if SIGN_OUTPUT == "HEX" else out.hex()


def sign_plaintext(data: dict[str, Any], timestamp: int) -> str:
    """待签名串：按 data 字段顺序拼「字段名+值」（值为 None 拼 null），再拼 #时间戳。

    例：loginName123123passWord222222#1604664285307
    """
    parts = [f"{k}{'null' if v is None else v}" for k, v in data.items()]
    return "".join(parts) + f"#{timestamp}"


def make_sign(data: dict[str, Any], timestamp: int, key: str | None = None) -> str:
    return des3_encrypt(sign_plaintext(data, timestamp), key or SIGN_KEY)


# =============================================================================
# 客户端
# =============================================================================

class YuqingClient:
    """按文档封装各接口。登录态、限速、每日计数都在这里处理，调用方只管调方法。"""

    def __init__(self, base_url: str = BASE_URL, imei: str = IMEI,
                 login_name: str = LOGIN_NAME, password: str = PASSWORD,
                 sign_key: str = SIGN_KEY, state_file: Path = STATE_FILE) -> None:
        missing = [n for n, v in (("BASE_URL", base_url), ("IMEI", imei),
                                  ("LOGIN_NAME", login_name), ("PASSWORD", password),
                                  ("SIGN_KEY", sign_key)) if not v]
        if missing:
            raise YuqingError("公共参数没填：" + "、".join(missing)
                              + "（在 tools/yuqing_api.py 顶部或环境变量 YUQING_* 里设置）")
        self.base_url = base_url.rstrip("/")
        self.imei = imei
        self.login_name = login_name
        self.password = password
        self.sign_key = sign_key
        self.state_file = state_file
        self.http = requests.Session()
        self.http.trust_env = False      # 直连，不读系统代理（代理没开时会连不上）
        self._calls: deque[float] = deque()
        self.state = self._load_state()

    # ------------------------------------------------------------ 状态落盘
    def _load_state(self) -> dict[str, Any]:
        try:
            return json.loads(self.state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _save_state(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.state_file)
        try:
            os.chmod(self.state_file, 0o600)   # 含 token
        except OSError:
            pass

    @property
    def token(self) -> str:
        return str(self.state.get("token") or "")

    @property
    def user_id(self) -> int | None:
        uid = self.state.get("user_id")
        return int(uid) if uid not in (None, "") else None

    # ------------------------------------------------------------ 限速与计数
    def _throttle(self) -> None:
        """60 秒滑动窗口内最多 REQUEST_PER_MINUTE 次，满了就等最早那次滑出窗口。"""
        while True:
            now = time.monotonic()
            while self._calls and now - self._calls[0] >= 60:
                self._calls.popleft()
            if len(self._calls) < REQUEST_PER_MINUTE:
                self._calls.append(now)
                return
            wait = 60 - (now - self._calls[0]) + 0.1
            logger.info("已达每分钟 %d 次上限，等待 %.0f 秒", REQUEST_PER_MINUTE, wait)
            time.sleep(wait)

    def _count(self, name: str) -> None:
        """今日调用计数 +1；到上限直接报错。"""
        today = date.today().isoformat()
        counts = self.state.setdefault("counts", {})
        if counts.get("date") != today:
            counts.clear()
            counts["date"] = today
        used = int(counts.get(name, 0))
        limit = DAILY_LIMITS.get(name)
        if limit is not None and used >= limit:
            raise YuqingError(f"{name} 今日已调用 {used} 次，达到上限 {limit} 次，明天再试")
        counts[name] = used + 1
        self._save_state()

    def daily_usage(self) -> dict[str, Any]:
        counts = dict(self.state.get("counts") or {})
        return counts if counts.get("date") == date.today().isoformat() else {}

    # ------------------------------------------------------------ 底层请求
    def _request(self, name: str, body: dict[str, Any], *, auth: bool = True,
                 relogin: bool = True) -> Any:
        """发一次请求，返回响应里的 data。token 失效时自动重新登录并重试一次。"""
        if auth and not self.token:
            self.login()
        headers = {"imei": self.imei, "Content-Type": "application/json;charset=UTF-8"}
        if auth:
            headers["Authorization"] = f"Bearer {self.token}"
        url = self.base_url + PATHS[name]

        self._count(name)
        self._throttle()
        try:
            if HTTP_METHOD.upper() == "GET":
                resp = self.http.get(url, params=body, headers=headers, timeout=TIMEOUT)
            else:
                resp = self.http.request(HTTP_METHOD.upper(), url, json=body,
                                         headers=headers, timeout=TIMEOUT)
        except requests.RequestException as exc:
            raise YuqingError(f"{name} 请求失败：{exc}") from exc

        try:
            payload = resp.json()
        except ValueError:
            payload = None

        try:
            if resp.status_code in (401, 403):
                raise TokenExpired(f"{name} HTTP {resp.status_code}")
            if payload is None:
                raise YuqingError(f"{name} 返回的不是 JSON（HTTP {resp.status_code}）："
                                  f"{resp.text[:200]}")
            code = str(payload.get("code", ""))
            msg = str(payload.get("msg") or "")
            if code in ("401", "403") or (code != "200" and _looks_like_token_error(msg)):
                raise TokenExpired(f"{name} code={code} {msg}")
            if code != "200":
                raise YuqingError(f"{name} 调用失败：code={code} {msg}")
            return payload.get("data")
        except TokenExpired:
            if not (auth and relogin):
                raise
            logger.warning("token 失效，重新登录后重试 %s", name)
            self.login(force=True)
            return self._request(name, body, auth=auth, relogin=False)

    # ------------------------------------------------------------ 1. 登录
    def login(self, force: bool = False) -> tuple[str, int | None]:
        """登录拿 token / userId。一天只能 50 次，所以优先复用落盘的 token。"""
        if not force and self.token and self.user_id is not None:
            at = self.state.get("login_at")
            try:
                age = datetime.now() - datetime.fromisoformat(at)
            except (TypeError, ValueError):
                age = timedelta(hours=TOKEN_MAX_AGE_HOURS + 1)
            if age < timedelta(hours=TOKEN_MAX_AGE_HOURS):
                logger.info("复用已保存的 token（%s 登录，userId=%s）", at, self.user_id)
                return self.token, self.user_id

        pwd = self.password if PASSWORD_IS_MD5 else md5_hex(self.password, PASSWORD_MD5_UPPER)
        data = {"loginName": self.login_name, "passWord": pwd}   # 顺序即签名顺序
        ts = int(time.time() * 1000)
        body = {"sign": make_sign(data, ts, self.sign_key), "timestamp": ts, "data": data}
        logger.info("登录 %s", self.login_name)
        result = self._request("login", body, auth=False) or {}
        token = result.get("token")
        if not token:
            raise YuqingError(f"登录成功但没返回 token：{result}")
        self.state.update(token=token, user_id=result.get("userId"),
                          login_at=datetime.now().isoformat(timespec="seconds"))
        self._save_state()
        logger.info("登录成功，userId=%s", self.user_id)
        return self.token, self.user_id

    def _uid(self) -> int:
        if self.user_id is None:
            self.login()
        assert self.user_id is not None
        return self.user_id

    # ------------------------------------------------------------ 2. 专题列表
    def special_list(self, use_cache: bool = True) -> list[dict[str, Any]]:
        """专题列表 [{id, name}]。一天只能调 10 次，当天结果缓存复用。"""
        cache = self.state.get("specials") or {}
        if use_cache and cache.get("date") == date.today().isoformat():
            logger.info("专题列表用今天的缓存（%d 个）", len(cache.get("items") or []))
            return list(cache.get("items") or [])
        items = _rows(self._request("special_list", {"userId": self._uid()}))
        self.state["specials"] = {"date": date.today().isoformat(), "items": items}
        self._save_state()
        return items

    # ------------------------------------------------------------ 3. 文章列表
    def doc_list_page(self, begin: str, end: str, page_no: int = 1,
                      page_size: int = 15, **filters: Any) -> list[dict[str, Any]]:
        """文章列表一页。filters：groupId / duplicate / orderFlag / specialId / sen /
        readFlag / ignoreLocation / closeKeyLineHight（None 的不传）。"""
        body = {"userId": self._uid(), "begin": begin, "end": end,
                "pageNo": page_no, "pageSize": min(page_size, MAX_PAGE_SIZE),
                **_compact(filters)}
        return _rows(self._request("doc_list", body))

    def iter_doc_list(self, begin: str, end: str, page_size: int = MAX_PAGE_SIZE,
                      max_pages: int | None = None, **filters: Any) -> Iterator[dict[str, Any]]:
        yield from _paginate(lambda no, size: self.doc_list_page(begin, end, no, size, **filters),
                             page_size, max_pages, "文章列表")

    # ------------------------------------------------------------ 4. 文章详情
    def article_detail(self, ids: Iterable[str], actile_ids: Iterable[str]) -> list[dict[str, Any]]:
        """一次最多 200 篇；ids 与 actileIds 一一对应。"""
        ids, actile_ids = list(ids), list(actile_ids)
        if len(ids) > MAX_PAGE_SIZE:
            raise YuqingError(f"文章详情一次最多 {MAX_PAGE_SIZE} 篇，收到 {len(ids)} 篇")
        body = {"userId": self._uid(), "ids": ",".join(map(str, ids)),
                "actileIds": ",".join(map(str, actile_ids))}
        return _rows(self._request("article_detail", body))

    def article_details_for(self, docs: list[dict[str, Any]],
                            batch: int = MAX_PAGE_SIZE) -> list[dict[str, Any]]:
        """给文章列表的结果补详情，按 batch 篇一批。"""
        out: list[dict[str, Any]] = []
        docs = [d for d in docs if d.get("id") or d.get("actileId")]
        for i in range(0, len(docs), batch):
            chunk = docs[i:i + batch]
            logger.info("文章详情 %d-%d / %d", i + 1, i + len(chunk), len(docs))
            out += self.article_detail([d.get("id", "") for d in chunk],
                                       [d.get("actileId", "") for d in chunk])
        return out

    # ------------------------------------------------------------ 5. 热点数据
    def hot_top_n(self, begin_time: str, end_time: str, top_n: int | None = None) -> list[dict[str, Any]]:
        body = {"userId": self._uid(), "beginTime": begin_time, "endTime": end_time}
        if top_n is not None:
            body["topN"] = min(int(top_n), 200)
        return _rows(self._request("hot_top_n", body))

    # ------------------------------------------------------------ 6. 专题预警
    def warning_page(self, page_no: int = 1, page_size: int = 15) -> list[dict[str, Any]]:
        body = {"userId": self._uid(), "pageNo": page_no,
                "pageSize": min(page_size, MAX_PAGE_SIZE)}
        return _rows(self._request("warning", body))

    def iter_warnings(self, page_size: int = 50,
                      max_pages: int | None = None) -> Iterator[dict[str, Any]]:
        yield from _paginate(self.warning_page, page_size, max_pages, "专题预警")

    # ------------------------------------------------------------ 7. 融媒体
    def fusion_media_page(self, begin: str, end: str, page_no: int = 1,
                          page_size: int = 15, **filters: Any) -> list[dict[str, Any]]:
        """融媒体一页。filters：groupId / source / keywordsGroupList（list[str]）/
        excludeWords / areaCode / title / content（None 的不传）。"""
        body = {"userId": self._uid(), "begin": begin, "end": end,
                "pageNo": page_no, "pageSize": min(page_size, MAX_PAGE_SIZE),
                **_compact(filters)}
        return _rows(self._request("fusion_media", body))

    def iter_fusion_media(self, begin: str, end: str, page_size: int = MAX_PAGE_SIZE,
                          max_pages: int | None = None, **filters: Any) -> Iterator[dict[str, Any]]:
        yield from _paginate(
            lambda no, size: self.fusion_media_page(begin, end, no, size, **filters),
            page_size, max_pages, "融媒体")


# =============================================================================
# 工具函数
# =============================================================================

def _looks_like_token_error(msg: str) -> bool:
    m = msg.lower()
    return any(w in m for w in ("token", "未登录", "登录过期", "登录失效", "unauthorized", "认证"))


def _compact(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


def _rows(data: Any) -> list[dict[str, Any]]:
    """data 可能直接是列表，也可能是 {result: [...]} / {records: [...]} 之类。"""
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("result", "records", "list", "rows", "data", "items"):
            if isinstance(data.get(key), list):
                return data[key]
        return [data] if data else []
    return []


def _paginate(fetch, page_size: int, max_pages: int | None, label: str) -> Iterator[dict[str, Any]]:
    """逐页拉，直到某页不满一页（或空）或到 max_pages。文档没给总条数字段，只能这样判断。"""
    page_size = min(page_size, MAX_PAGE_SIZE)
    page_no = 1
    while True:
        rows = fetch(page_no, page_size)
        logger.info("%s 第 %d 页：%d 条", label, page_no, len(rows))
        yield from rows
        if len(rows) < page_size:
            return
        if max_pages is not None and page_no >= max_pages:
            logger.warning("%s 到达 max_pages=%d，后面可能还有数据", label, max_pages)
            return
        page_no += 1


def save_json(name: str, rows: Any, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.json"
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# =============================================================================
# 主流程：按调用顺序跑一遍
# =============================================================================

def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # ---- 本次运行的动态参数（按需修改）--------------------------------------
    end_dt = datetime.now().replace(microsecond=0)
    begin_dt = end_dt - timedelta(days=1)          # 时间窗口：最近 1 天
    begin, end = begin_dt.strftime(TIME_FMT), end_dt.strftime(TIME_FMT)

    # 要跑的步骤（登录总是先跑）；不需要的删掉
    steps = ["specials", "docs", "details", "hot", "warnings", "fusion"]

    # 文章列表（queryDocList）
    doc_filters = {
        "groupId": None,           # 媒体类型，如 "19"（微博）；见 MEDIA_TYPES
        "specialId": None,         # 专题 id，多个用逗号；None 表示不限
        "sen": None,               # 倾向：1 正面 / -1 负面 / 0 中性
        "duplicate": None,         # 1 去重 / 0 不去重（默认 0）
        "orderFlag": None,         # 0 发布时间 / 1 采集时间
    }
    doc_page_size = 200
    doc_max_pages = 50             # 防止窗口太大时把每日额度一次用光；None 不限

    # 热点数据（hotTopN）
    hot_top_n = 50                 # 前 N 条，不超过 200

    # 专题预警（warning）
    warning_page_size = 50
    warning_max_pages = 10

    # 融媒体（fusionMedia/queryList）
    fusion_filters = {
        "groupId": None,           # 媒体类型，多个用逗号
        "source": None,            # 媒体来源，多个用逗号
        "keywordsGroupList": None, # 如 ["测试,疫情", "起火,消防"]：组内或、组间且
        "excludeWords": None,      # 排除词，多个用逗号
        "areaCode": None,          # 地域编码，如 "35"（含下级）
        "title": None,             # 标题匹配
        "content": None,           # 正文匹配
    }
    fusion_page_size = 200
    fusion_max_pages = 50

    out_dir = OUTPUT_DIR / end_dt.strftime("%Y%m%d_%H%M%S")
    # -------------------------------------------------------------------------

    client = YuqingClient()
    summary: dict[str, Any] = {"window": [begin, end]}

    # 1. 登录（后面所有接口都要 token + userId）
    client.login()

    # 2. 专题列表
    if "specials" in steps:
        specials = client.special_list()
        save_json("specials", specials, out_dir)
        summary["specials"] = len(specials)

    # 3. 文章列表
    docs: list[dict[str, Any]] = []
    if "docs" in steps or "details" in steps:
        docs = list(client.iter_doc_list(begin, end, page_size=doc_page_size,
                                         max_pages=doc_max_pages, **doc_filters))
        save_json("docs", docs, out_dir)
        summary["docs"] = len(docs)

    # 4. 文章详情（用第 3 步的 id / actileId）
    if "details" in steps and docs:
        details = client.article_details_for(docs)
        save_json("details", details, out_dir)
        summary["details"] = len(details)

    # 5. 热点数据
    if "hot" in steps:
        hot = client.hot_top_n(begin, end, hot_top_n)
        save_json("hot", hot, out_dir)
        summary["hot"] = len(hot)

    # 6. 专题预警
    if "warnings" in steps:
        warnings = list(client.iter_warnings(warning_page_size, warning_max_pages))
        save_json("warnings", warnings, out_dir)
        summary["warnings"] = len(warnings)

    # 7. 融媒体
    if "fusion" in steps:
        fusion = list(client.iter_fusion_media(begin, end, page_size=fusion_page_size,
                                               max_pages=fusion_max_pages, **fusion_filters))
        save_json("fusion", fusion, out_dir)
        summary["fusion"] = len(fusion)

    summary["daily_usage"] = client.daily_usage()
    save_json("summary", summary, out_dir)
    logger.info("完成：%s", json.dumps(summary, ensure_ascii=False))
    logger.info("结果目录：%s", out_dir)
    return 0


def _sign_cli(argv: list[str]) -> int:
    """python tools/yuqing_api.py sign <loginName> <passWord> <timestamp>
    按当前配置算签名（passWord 原样参与，不做 md5），用来和对方给的示例核对。"""
    if len(argv) != 3:
        print(_sign_cli.__doc__)
        return 2
    name, pwd, ts = argv
    data = {"loginName": name, "passWord": pwd}
    print("待签名串：", sign_plaintext(data, int(ts)))
    print("sign    ：", make_sign(data, int(ts)))
    return 0


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "sign":
            sys.exit(_sign_cli(sys.argv[2:]))
        sys.exit(main())
    except YuqingError as exc:
        logging.getLogger("yuqing").error("%s", exc)
        sys.exit(2)
