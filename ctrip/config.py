# -*- coding: utf-8 -*-
"""配置加载：.env + YAML + ``${ENV_VAR}`` 占位符 + 环境变量覆盖。

设计要点
--------
1. 账号密码等敏感信息**不写在源码**，也建议不写在 YAML 明文里，
   而是在 YAML 中写 ``${CTRIP_A001_PASSWORD}``，由环境变量注入。
2. 项目根目录的 ``.env`` 会被**自动加载**，无需手动 ``source .env``。
   这一点对 Windows 尤其重要：PowerShell / CMD 里没有 ``source`` 命令，
   若依赖手动导入，程序会因读不到环境变量而报「缺少登录用户名」。
   已存在的真实环境变量优先于 ``.env``，不会被文件覆盖。
3. 任意配置项都可用环境变量覆盖，命名规则 ``CTRIP__<段>__<键>``，
   例如 ``CTRIP__MYSQL__HOST=10.0.0.5``、``CTRIP__RUN__SINK=both``。
4. MySQL 默认关闭；只有显式 ``mysql.enabled=true``（或通过环境变量打开）
   才会尝试连接数据库，保证「默认写 CSV」的语义。
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
EXAMPLE_CONFIG_PATH = PROJECT_ROOT / "config" / "config.example.yaml"
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"

ENV_PREFIX = "CTRIP__"
_PLACEHOLDER_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_ACCOUNT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,50}$")
# 景区 ID（如 PFTSCA01018088）：字母、数字、下划线、连字符，长度 1-50
_SCENIC_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,50}$")


class ConfigError(RuntimeError):
    """配置错误。"""


def _missing_credential_hint(account_id: str, field_name: str) -> str:
    """拼接可操作的报错信息。

    它会实际检查 ``.env`` 是否存在、相关环境变量是否已注入，
    然后给出对应场景的具体建议——而不是只说一句「缺少 xxx」让人自己猜。
    最典型的误区是已经填好了 ``.env`` 但文件放错了位置。
    """
    zh = {"account": "登录用户名", "password": "登录密码"}[field_name]
    env_var = f"CTRIP_{account_id.upper()}_{field_name.upper()}"
    lines = [f"账号 {account_id} 缺少{zh}（{field_name}）。"]

    if not DEFAULT_ENV_PATH.exists():
        lines += [
            f"原因：未找到 .env 文件（期望路径：{DEFAULT_ENV_PATH}）。",
            "处理：先复制模板再填写真实值：",
            "  Windows PowerShell:  Copy-Item .env.example .env",
            "  Linux / macOS:       cp .env.example .env",
        ]
    elif not os.environ.get(env_var):
        lines += [
            f"原因：.env 已找到（{DEFAULT_ENV_PATH}），但其中的 {env_var} 为空或未定义。",
            f"处理：在 .env 里填写 {env_var}=<真实值>（注意等号两边不要加空格）。",
            "若账号信息统一存在数据库，也可改用数据库账号源："
            "在 config.yaml 中设置 ctrip.account_source: mysql。",
        ]
    else:
        lines += [
            f"异常：环境变量 {env_var} 已有值，但未能注入到该账号。",
            "请检查 config.yaml 中该账号的 "
            f"{field_name} 是否写成了 ${{{env_var}}} 占位符（大小写需完全一致）。",
        ]

    lines.append("诊断命令：python -m ctrip.cli doctor")
    return "\n".join(lines)


# =============================================================================
# 数据类
# =============================================================================

@dataclass
class MySQLConfig:
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "ctrip_data"
    charset: str = "utf8mb4"
    table_prefix: str = ""
    # shared: 所有账号写同一批表（推荐，唯一键含 account_id 天然隔离）
    # per_account: 每个账号一套物理表 ads_sup_ota_ctrip_trade_di_{account_id}
    table_strategy: str = "shared"
    connect_timeout: int = 10
    write_run_log: bool = True

    def dsn_safe(self) -> str:
        return f"mysql://{self.user}@{self.host}:{self.port}/{self.database}"

    def validate(self) -> None:
        if self.table_strategy not in ("shared", "per_account"):
            raise ConfigError(f"mysql.table_strategy 只能为 shared/per_account，当前 {self.table_strategy!r}")
        if self.enabled and not self.database:
            raise ConfigError("mysql.enabled=true 时必须配置 mysql.database")


@dataclass
class RunConfig:
    timezone: str = "Asia/Shanghai"
    sink: str = "csv"                 # csv | mysql | both
    output_dir: str = "data/output"
    log_dir: str = "logs"
    log_level: str = "INFO"
    csv_encoding: str = "utf-8-sig"   # 带 BOM，Excel 直接打开不乱码
    csv_with_label_row: bool = False  # True 时在英文表头上方追加一行中文表头
    csv_overwrite: bool = True        # 同一批次重复运行覆盖，避免重复累积
    csv_split_by_run_key: bool = False  # True 时每个采集区间单独成文件
    # 落地写入模式：
    #   overwrite（默认）同一日期/月份的旧数据先清理再写入，
    #     能清除上游已下线维度遗留的陈旧行；
    #   upsert 仅按唯一键原地更新，不删除任何行。
    write_mode: str = "overwrite"
    workers: int = 1                  # 账号级并发数，默认串行更风控友好

    def validate(self) -> None:
        if self.sink not in ("csv", "mysql", "both"):
            raise ConfigError(f"run.sink 只能为 csv/mysql/both，当前 {self.sink!r}")
        if self.write_mode not in ("overwrite", "upsert"):
            raise ConfigError(
                f"run.write_mode 只能为 overwrite/upsert，当前 {self.write_mode!r}"
            )
        if self.workers < 1:
            raise ConfigError("run.workers 必须 >= 1")


@dataclass
class AccountConfig:
    account_id: str
    account: str = ""
    password: str = ""
    name: str = ""
    status: str = "active"
    remark: str = ""
    # 景区 ID（如 PFTSCA01018088），账号级绑定，一个账号对应一个景区。
    # 采集时会自动写入三张业务表的每一行，便于与内部景区维表关联。
    scenic_id: str = ""

    def validate(self, *, require_credentials: bool = True) -> None:
        if not _ACCOUNT_ID_RE.match(self.account_id or ""):
            raise ConfigError(
                f"非法 account_id: {self.account_id!r}；"
                "仅允许字母、数字、下划线、连字符，长度 1-50（用于拼接目录与表名，必须安全）"
            )
        if require_credentials and self.status == "active":
            if not self.account:
                raise ConfigError(_missing_credential_hint(self.account_id, "account"))
            if not self.password:
                raise ConfigError(_missing_credential_hint(self.account_id, "password"))
        if self.scenic_id and not _SCENIC_ID_RE.match(self.scenic_id):
            raise ConfigError(
                f"账号 {self.account_id} 的 scenic_id 非法: {self.scenic_id!r}；"
                "仅允许字母、数字、下划线、连字符，长度 1-50（如 PFTSCA01018088）"
            )

    @property
    def is_active(self) -> bool:
        return self.status == "active"


@dataclass
class CtripConfig:
    login_url: str = "https://vbooking.ctrip.com/micro/ivbk/accountV2/dashboard"
    home_url: str = "https://vbooking.ctrip.com/micro/tour-bi-vendor-new/"
    api_base: str = "https://m.ctrip.com/restapi/soa2/13807"
    headless: bool = True
    slow_mo: int = 0
    login_timeout: int = 180          # 秒，人工处理验证码留足时间
    nav_timeout: int = 45             # 秒
    cookie_dir: str = "auth"
    # 浏览器持久化目录（每账号一个子目录）。
    # 保留 Chromium 的 profile（cookie/localStorage/缓存/设备指纹），
    # 让携程风控看到的是「同一台设备重复登录」而非「陌生设备」，
    # 从而大幅降低滑块/短信验证的触发概率。
    browser_dir: str = "browser"
    # 是否使用持久化浏览器（强烈建议开启）。
    # 关闭后退回旧行为：每次 launch() 全新临时 profile
    persistent_browser: bool = True
    # 常驻浏览器的 CDP 远程调试端口。
    # >0 时：login --keep-alive 会拉起常驻浏览器并监听该端口，
    # 后续运行优先 connect_over_cdp 复用已打开的浏览器（连验证码都不用重过）
    cdp_port: int = 0
    # 账号来源：config（YAML/.env，默认）| mysql（从账号表读）| api（账号服务）
    # auto 表示依次尝试 api -> mysql -> config，取第一个能拿到账号的源
    account_source: str = "config"
    # 账号表中存密码的列名。与审计维表分开配置，是因为本工具
    # 写入的维表刻意不含密码；若你自己的表里存了密码，在此指定列名即可
    account_password_column: str = "password"
    account_api_url: str = ""
    account_api_token: str = ""
    account_api_timeout: int = 10
    login_retry: int = 2
    cookie_ttl_hours: int = 12        # 超过该时长强制复验
    accounts: list[AccountConfig] = field(default_factory=list)


_ACCOUNT_SOURCES = ("config", "mysql", "api", "auto")


@dataclass
class CollectConfig:
    kylin_days: int = 30
    batch_days: int = 30
    request_interval: float = 1.5
    max_retries: int = 3
    retry_backoff: float = 5.0
    timeout: int = 30
    date_types: list[str] = field(default_factory=lambda: ["使用日期", "预订日期"])
    # 交易卡片周期口径：
    #   1d  按指定区间逐日采（历史回溯只影响这一种）
    #   7d  近 7 日聚合，仅当前窗口 1 行
    #   30d 近 30 日聚合，仅当前窗口 1 行
    #
    # 默认**只采 1d**：7d/30d 是当前快照，日常增量采集时每跑一次就会
    # 多发一轮请求，而近 7 日 / 近 30 日也可由 1d 明细在下游聚合得出，
    # 因此不默认开启；需要时用 --period 7d,30d 或在此处配置显式打开。
    periods: list[str] = field(default_factory=lambda: ["1d"])
    signing_name_to_code: dict[str, int] = field(
        default_factory=lambda: {"跟团游": 1, "私家团": 6, "景点玩乐": 7}
    )
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    def validate(self) -> None:
        allowed = ("1d", "7d", "30d")
        if not self.periods:
            raise ConfigError("collect.periods 不能为空，可选：1d/7d/30d")
        unknown = [p for p in self.periods if str(p).strip().lower() not in allowed]
        if unknown:
            raise ConfigError(
                f"collect.periods 存在未知口径 {unknown}，只能为 {'/'.join(allowed)}"
            )


@dataclass
class AppConfig:
    run: RunConfig = field(default_factory=RunConfig)
    mysql: MySQLConfig = field(default_factory=MySQLConfig)
    ctrip: CtripConfig = field(default_factory=CtripConfig)
    collect: CollectConfig = field(default_factory=CollectConfig)
    project_root: Path = PROJECT_ROOT
    config_path: Path | None = None

    # -- 路径工具（全部带账号隔离） -------------------------------------
    def path(self, relative: str | Path) -> Path:
        p = Path(relative)
        return p if p.is_absolute() else self.project_root / p

    def output_dir(self, account_id: str) -> Path:
        d = self.path(self.run.output_dir) / _safe_seg(account_id)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def log_dir(self, account_id: str | None = None) -> Path:
        d = self.path(self.run.log_dir)
        if account_id:
            d = d / _safe_seg(account_id)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def cookie_dir(self, account_id: str) -> Path:
        d = self.path(self.ctrip.cookie_dir) / _safe_seg(account_id)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def cookie_file(self, account_id: str) -> Path:
        return self.cookie_dir(account_id) / "storage_state.json"

    def cookie_meta_file(self, account_id: str) -> Path:
        return self.cookie_dir(account_id) / "meta.json"

    def browser_dir(self, account_id: str) -> Path:
        """该账号的浏览器持久化根目录。"""
        d = self.path(self.ctrip.browser_dir) / _safe_seg(account_id)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def browser_profile_dir(self, account_id: str) -> Path:
        """Chromium 用户数据目录。

        每账号一个独立 profile，而不是共用一个。共用会导致后登录的
        账号顶掉前一个的会话（同域 cookie 互相覆盖），多账号场景必然串号。
        """
        d = self.browser_dir(account_id) / "profile"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def browser_lock_file(self, account_id: str) -> Path:
        """常驻会话元信息（pid / CDP 端口 / 启动时间）。"""
        return self.browser_dir(account_id) / "session.json"

    # -- 账号 ------------------------------------------------------------
    def account_map(self) -> dict[str, AccountConfig]:
        return {a.account_id: a for a in self.ctrip.accounts}

    def select_accounts(self, account_ids: list[str] | None = None,
                        include_disabled: bool = False) -> list[AccountConfig]:
        """按 ID 过滤账号；未指定则返回全部启用账号。"""
        accounts = self.ctrip.accounts
        if not accounts:
            raise ConfigError(
                "未配置任何账号。请在 config/config.yaml 的 ctrip.accounts 中配置，"
                "或设置 ctrip.account_api_url 从账号服务拉取。"
                "（为避免数据串账号，本工具不提供匿名默认账号）"
            )
        if account_ids:
            mapping = self.account_map()
            missing = [i for i in account_ids if i not in mapping]
            if missing:
                raise ConfigError(f"配置中不存在账号: {missing}；可用: {sorted(mapping)}")
            selected = [mapping[i] for i in account_ids]
        else:
            selected = list(accounts)
        if not include_disabled:
            selected = [a for a in selected if a.is_active]
        if not selected:
            raise ConfigError("筛选后没有可用账号（可能全部 status != active）")
        return selected

    def validate(self) -> None:
        self.run.validate()
        self.mysql.validate()
        self.collect.validate()
        if self.ctrip.account_source not in _ACCOUNT_SOURCES:
            raise ConfigError(
                f"ctrip.account_source 只能为 {'/'.join(_ACCOUNT_SOURCES)}，"
                f"当前 {self.ctrip.account_source!r}"
            )
        if self.ctrip.account_source in ("mysql", "auto") and not self.mysql.enabled:
            raise ConfigError(
                f"ctrip.account_source={self.ctrip.account_source} 需要 mysql.enabled=true，"
                "否则无法从账号表读取账号"
            )
        # 账号从外部源（数据库/API）加载时，配置里允许暂无凭据，
        # 否则会在还没去读数据库之前就报「缺少登录用户名」。
        require_credentials = self.ctrip.account_source == "config"
        seen: set[str] = set()
        for account in self.ctrip.accounts:
            account.validate(require_credentials=require_credentials)
            if account.account_id in seen:
                raise ConfigError(f"account_id 重复: {account.account_id}")
            seen.add(account.account_id)
        if self.run.sink in ("mysql", "both") and not self.mysql.enabled:
            raise ConfigError(
                f"run.sink={self.run.sink} 需要 mysql.enabled=true；"
                "未配置数据库时请使用 sink=csv（默认）"
            )


def _safe_seg(value: str) -> str:
    """路径片段消毒，防止 account_id 造成目录穿越。"""
    if not _ACCOUNT_ID_RE.match(value or ""):
        raise ConfigError(f"非法 account_id 用于路径拼接: {value!r}")
    return value


# =============================================================================
# 加载逻辑
# =============================================================================

def load_dotenv(path: str | Path | None = None, *, override: bool = False) -> list[str]:
    """加载 ``.env`` 到 ``os.environ``，返回本次注入的变量名。

    自己实现而不依赖 python-dotenv，是为了少一个依赖、并且能精确控制
    几个容易出问题的细节：

    * BOM：Windows 记事本 / 部分编辑器保存的 UTF-8 带 BOM，不处理会导致
      第一个变量名变成 ``\ufeffMYSQL_HOST`` 而永远取不到；
    * CRLF：Windows 换行符会在值末尾留下 ``\r``，使密码多一个不可见字符；
    * ``export`` 前缀：从 Linux 习惯拷过来的 ``.env`` 常带 ``export``；
    * 引号：成对的引号剥除，但不误伤密码中间的引号。

    :param override: 默认 False，已存在的环境变量优先（便于 CI 与临时覆盖）。
    """
    env_path = Path(path) if path else DEFAULT_ENV_PATH
    if not env_path.exists():
        return []

    injected: list[str] = []
    # utf-8-sig 自动吃掉 BOM；遇非 UTF-8 编码（如 Windows GBK）回退到 gbk
    try:
        text = env_path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        text = env_path.read_text(encoding="gbk", errors="replace")

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key or not key.replace("_", "").isalnum():
            continue
        value = value.strip()
        # 仅剥除成对的首尾引号，避免把密码里的引号弄掉
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if not override and key in os.environ:
            continue
        os.environ[key] = value
        injected.append(key)
    return injected


def _expand_placeholders(value: Any) -> Any:
    """递归展开 ``${ENV}`` / ``${ENV:-默认值}``。"""
    if isinstance(value, str):
        def repl(m: re.Match) -> str:
            env_name, default = m.group(1), m.group(2)
            env_value = os.environ.get(env_name)
            if env_value is None:
                if default is not None:
                    return default
                # 未设置且无默认值：保留空串，交由后续校验报错，避免把字面量写进数据
                return ""
            return env_value
        return _PLACEHOLDER_RE.sub(repl, value)
    if isinstance(value, dict):
        return {k: _expand_placeholders(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_placeholders(v) for v in value]
    return value


def _coerce(current: Any, raw: str) -> Any:
    """按现有默认值类型转换环境变量字符串。"""
    if isinstance(current, bool):
        return raw.strip().lower() in ("1", "true", "yes", "on", "y")
    if isinstance(current, int) and not isinstance(current, bool):
        return int(raw)
    if isinstance(current, float):
        return float(raw)
    if isinstance(current, list):
        return [x.strip() for x in raw.split(",") if x.strip()]
    return raw


def _apply_env_overrides(cfg: AppConfig) -> list[str]:
    """应用 ``CTRIP__SECTION__KEY`` 形式的环境变量覆盖，返回生效项列表。"""
    applied: list[str] = []
    sections = {"run": cfg.run, "mysql": cfg.mysql, "ctrip": cfg.ctrip, "collect": cfg.collect}
    for env_key, raw in os.environ.items():
        if not env_key.startswith(ENV_PREFIX):
            continue
        parts = env_key[len(ENV_PREFIX):].split("__")
        if len(parts) != 2:
            continue
        section_name, key = parts[0].lower(), parts[1].lower()
        section = sections.get(section_name)
        if section is None or not hasattr(section, key):
            continue
        try:
            setattr(section, key, _coerce(getattr(section, key), raw))
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"环境变量 {env_key} 值非法: {raw!r}（{exc}）") from exc
        applied.append(f"{section_name}.{key}")
    return applied


def _build_accounts(items: Any) -> list[AccountConfig]:
    accounts: list[AccountConfig] = []
    for item in items or []:
        if not isinstance(item, dict):
            raise ConfigError(f"ctrip.accounts 元素必须是字典，当前: {item!r}")
        accounts.append(AccountConfig(
            account_id=str(item.get("account_id", "")).strip(),
            account=str(item.get("account", "") or "").strip(),
            password=str(item.get("password", "") or ""),
            name=str(item.get("name", "") or "").strip(),
            status=str(item.get("status", "active") or "active").strip(),
            remark=str(item.get("remark", "") or "").strip(),
            scenic_id=str(item.get("scenic_id", "") or "").strip(),
        ))
    return accounts


def _fill(target: Any, data: dict[str, Any], skip: tuple[str, ...] = ()) -> None:
    for key, value in (data or {}).items():
        if key in skip:
            continue
        if hasattr(target, key):
            setattr(target, key, value)


def load_config(path: str | Path | None = None, *, validate: bool = True,
                env_file: str | Path | None = None) -> AppConfig:
    """加载配置。缺失配置文件时回落到示例配置 + 环境变量。

    会先自动加载项目根目录的 ``.env``，因此 Windows 上无需任何手动导入。
    """
    load_dotenv(env_file)
    cfg = AppConfig()
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not config_path.exists() and EXAMPLE_CONFIG_PATH.exists():
        config_path = EXAMPLE_CONFIG_PATH

    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        raw = _expand_placeholders(raw)
        _fill(cfg.run, raw.get("run", {}))
        _fill(cfg.mysql, raw.get("mysql", {}))
        _fill(cfg.collect, raw.get("collect", {}))
        ctrip_raw = raw.get("ctrip", {}) or {}
        _fill(cfg.ctrip, ctrip_raw, skip=("accounts",))
        cfg.ctrip.accounts = _build_accounts(ctrip_raw.get("accounts"))
        cfg.config_path = config_path

    _apply_env_overrides(cfg)

    # 环境变量直传账号（适合容器/CI）：CTRIP_ACCOUNTS="A001:user1:pwd1,A002:user2:pwd2"
    inline = os.environ.get("CTRIP_ACCOUNTS", "").strip()
    if inline:
        parsed: list[AccountConfig] = []
        for chunk in inline.split(","):
            bits = chunk.split(":")
            if len(bits) < 3:
                raise ConfigError(
                    "CTRIP_ACCOUNTS 片段格式应为 account_id:account:password[:scenic_id]，"
                    f"当前: {chunk!r}"
                )
            # 四段形式时最后一段为 scenic_id；三段形式时密码可含冒号
            scenic_id = ""
            if len(bits) >= 4:
                scenic_id = bits[-1].strip()
                password = ":".join(bits[2:-1])
            else:
                password = ":".join(bits[2:])
            parsed.append(AccountConfig(
                account_id=bits[0].strip(), account=bits[1].strip(),
                password=password, scenic_id=scenic_id,
            ))
        cfg.ctrip.accounts = parsed

    if validate:
        cfg.validate()
    return cfg
