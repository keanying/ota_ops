"""账号管理：新建账号并写回 ``config/config.yaml``。

为什么不用 PyYAML 重写整个文件
------------------------------
你的 config.yaml 里写满了注释（每个字段都有说明）。``yaml.safe_dump`` 会把
注释全部丢掉，等于把文档毁了。所以这里做**定点插入**：只在 ``ctrip.accounts``
列表末尾追加一段，其余字节不动。

写入前备份、写入后用项目自己的 ``load_config`` 复核；解析不过就回滚，
绝不留下一个坏掉的配置文件。
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from ctrip.config import ConfigError, load_config

ACCOUNT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,50}$")
SCENIC_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,50}$")
SERIAL_RE = re.compile(r"^A(\d+)$")


class AccountAdminError(RuntimeError):
    pass


@dataclass
class NewAccount:
    name: str
    account: str
    password: str
    remark: str = ""
    scenic_id: str = ""
    status: str = "active"
    account_id: str = ""      # 留空则自动生成


# ---------------------------------------------------------------- 生成 ID
def id_from_account(account: str) -> str:
    """用登录账号本身作为 account_id。

    account_id 会成为目录名（``browser/{id}/profile``、``auth/{id}/``）和
    数据库里的账号标识，所以只保留字母数字下划线连字符，其余字符换成下划线。
    携程的账号形如 ``vbk_569202``，本来就合规，转换是给异常输入兜底的。
    """
    cleaned = re.sub(r"[^A-Za-z0-9_-]", "_", (account or "").strip()).strip("_")
    return cleaned[:50]


def next_account_id(existing: list[str]) -> str:
    """在已有 A001、A002… 之后取下一个序号。

    没有任何符合 A+数字 形式的历史账号时从 A001 开始；
    有非序号形式的（比如 prod-a）不影响，跳过即可。
    """
    used = set(existing)
    top = 0
    for aid in existing:
        m = SERIAL_RE.match(aid or "")
        if m:
            top = max(top, int(m.group(1)))
    n = top + 1
    while f"A{n:03d}" in used:
        n += 1
    return f"A{n:03d}"


# ---------------------------------------------------------------- 校验
def validate(new: NewAccount, existing: list[str]) -> None:
    """existing 传 account_id 列表。"""
    if not new.account.strip():
        raise AccountAdminError("登录账号不能为空")
    if not new.password:
        raise AccountAdminError("登录密码不能为空")
    if not ACCOUNT_ID_RE.match(new.account_id or ""):
        raise AccountAdminError(
            f"非法 account_id: {new.account_id!r}（只能用字母、数字、下划线、连字符）")
    if new.account_id in existing:
        raise AccountAdminError(
            f"账号已存在：{new.account_id}。若要改密码请直接编辑 config.yaml，"
            f"不要重复创建")
    dup = [x for x in existing if x.lower() == new.account_id.lower()]
    if dup:
        raise AccountAdminError(f"已有仅大小写不同的账号 {dup[0]}，会共用同一个目录，请改用别的标识")
    if new.scenic_id and not SCENIC_ID_RE.match(new.scenic_id):
        raise AccountAdminError(f"景区ID 格式不对：{new.scenic_id!r}")
    if new.status not in ("active", "disabled"):
        raise AccountAdminError("状态只能是 active 或 disabled")


# ---------------------------------------------------------------- YAML 插入
def _quote(v: str) -> str:
    """YAML 标量安全引用：密码里常有 @ : # 等字符，不引会解析错。"""
    v = str(v or "")
    if v == "":
        return '""'
    if re.search(r'[:#\'"{}\[\],&*?|<>=!%@`\\]|^\s|\s$|^[-?]', v):
        return '"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return v


def render_block(a: NewAccount, indent: int) -> list[str]:
    pad = " " * indent
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"{pad}- account_id: {a.account_id}"
        f"{'':<{max(1, 22 - len(a.account_id))}}# 控制台创建于 {stamp}",
        f"{pad}  name: {_quote(a.name)}",
        f"{pad}  account: {_quote(a.account)}",
        f"{pad}  password: {_quote(a.password)}",
    ]
    if a.scenic_id:
        lines.append(f"{pad}  scenic_id: {_quote(a.scenic_id)}")
    lines.append(f"{pad}  status: {a.status}")
    if a.remark:
        lines.append(f"{pad}  remark: {_quote(a.remark)}")
    return lines


def _locate_accounts_block(lines: list[str]) -> tuple[int, int, int]:
    """定位 ctrip.accounts 列表。返回 (accounts 行号, 插入位置, 条目缩进)。"""
    acc_idx = None
    acc_indent = 0
    for i, ln in enumerate(lines):
        m = re.match(r"^(\s*)accounts:\s*(#.*)?$", ln)
        if m:
            acc_idx = i
            acc_indent = len(m.group(1))
            break
    if acc_idx is None:
        raise AccountAdminError(
            "config.yaml 里没找到 ctrip.accounts 段，无法自动写入。请手动添加账号")

    # 条目缩进：取第一个 "- " 行；没有条目时按 accounts 缩进 +2
    item_indent = acc_indent + 2
    for ln in lines[acc_idx + 1:]:
        m = re.match(r"^(\s*)-\s", ln)
        if m and len(m.group(1)) > acc_indent:
            item_indent = len(m.group(1))
            break
        if ln.strip() and not ln.strip().startswith("#") and \
                len(ln) - len(ln.lstrip()) <= acc_indent:
            break

    # 插入点：块内最后一行**有效条目**之后（跳过尾部注释与空行，
    # 免得插到被注释掉的示例账号后面，看起来像注释的一部分）
    insert = acc_idx + 1
    for i in range(acc_idx + 1, len(lines)):
        ln = lines[i]
        if not ln.strip():
            continue
        indent = len(ln) - len(ln.lstrip())
        if indent <= acc_indent and not ln.lstrip().startswith("#"):
            break                       # 出了 accounts 块
        if not ln.lstrip().startswith("#"):
            insert = i + 1              # 记住最后一条真实条目
    return acc_idx, insert, item_indent


def append_to_yaml(config_path: Path, a: NewAccount) -> Path:
    """把账号追加进 config.yaml，返回备份文件路径。"""
    text = config_path.read_text(encoding="utf-8")
    newline = "\r\n" if "\r\n" in text else "\n"
    lines = text.splitlines()

    _, insert, indent = _locate_accounts_block(lines)
    block = render_block(a, indent)
    merged = lines[:insert] + block + lines[insert:]

    backup = config_path.with_suffix(
        config_path.suffix + f".bak-{datetime.now():%Y%m%d%H%M%S}")
    shutil.copy2(config_path, backup)
    config_path.write_text(newline.join(merged) + newline, encoding="utf-8")
    return backup


# ---------------------------------------------------------------- 就地更新
UPDATABLE = ("account", "password", "name", "remark", "scenic_id", "status")


def _entry_span(lines: list[str], account_id: str) -> tuple[int, int, int]:
    """定位某个账号在 accounts 列表里的行区间，返回 (起, 止, 条目缩进)。"""
    _, _, item_indent = _locate_accounts_block(lines)
    start = None
    for i, ln in enumerate(lines):
        m = re.match(r"^(\s*)-\s+account_id:\s*[\"']?([^\s#\"']+)", ln)
        if m and m.group(2) == account_id:
            start, item_indent = i, len(m.group(1))
            break
    if start is None:
        raise AccountAdminError(f"config.yaml 里没找到账号 {account_id}")

    end = len(lines)
    for i in range(start + 1, len(lines)):
        ln = lines[i]
        if not ln.strip():
            continue
        indent = len(ln) - len(ln.lstrip())
        # 遇到同级的下一个条目，或缩进退回到 accounts 之外，就是本条目的结束
        if (re.match(r"^\s*-\s", ln) and indent <= item_indent) or indent < item_indent:
            end = i
            break
    return start, end, item_indent


def update_account(config_path: Path, account_id: str, fields: dict) -> Path:
    """就地更新某个账号的字段，返回备份路径。

    只改指定的那几行，条目里其他行（含注释）原样保留。
    字段不存在就插到 account_id 那行后面。
    """
    fields = {k: v for k, v in fields.items() if k in UPDATABLE and v is not None}
    if not fields:
        raise AccountAdminError("没有可更新的字段")

    text = config_path.read_text(encoding="utf-8")
    newline = "\r\n" if "\r\n" in text else "\n"
    lines = text.splitlines()
    start, end, indent = _entry_span(lines, account_id)
    pad = " " * (indent + 2)

    block = lines[start:end]
    for key, value in fields.items():
        rendered = f"{pad}{key}: {_quote(str(value))}"
        for j, ln in enumerate(block):
            m = re.match(rf"^(\s*){key}:\s*(.*)$", ln)
            if m and len(m.group(1)) > indent:
                # 保留行尾注释，只换值
                tail = m.group(2)
                comment = ""
                if "#" in tail:
                    quoted = tail.count('"') % 2
                    if not quoted:
                        comment = "  " + tail[tail.index("#"):].strip()
                block[j] = rendered + comment
                break
        else:
            block.insert(1, rendered)     # 紧跟在 - account_id 那行后面

    merged = lines[:start] + block + lines[end:]
    backup = config_path.with_suffix(
        config_path.suffix + f".bak-{datetime.now():%Y%m%d%H%M%S}")
    shutil.copy2(config_path, backup)
    config_path.write_text(newline.join(merged) + newline, encoding="utf-8")
    return backup


def save_credentials(cfg, account_id: str, account: str = "", password: str = ""):
    """把登录成功的凭据写回 config.yaml。

    **只在登录成功后调用** —— 存一个登不进去的密码没有意义，
    还会覆盖掉原本可能正确的那个。

    写入后复核并在失败时回滚，与 create_account 一致。
    返回 (新的 cfg, 实际改了哪些字段)；没有变化时返回 (cfg, {})。
    """
    current = {x.account_id: x for x in cfg.ctrip.accounts}.get(account_id)
    if current is None:
        raise AccountAdminError(f"配置里没有账号 {account_id}")

    fields = {}
    if account and account != current.account:
        fields["account"] = account
    if password and password != current.password:
        fields["password"] = password
    if not fields:
        return cfg, {}

    config_path = Path(cfg.config_path or "")
    if not config_path.exists():
        raise AccountAdminError("找不到 config.yaml，无法保存凭据")

    backup = update_account(config_path, account_id, fields)
    try:
        fresh = load_config(str(config_path), validate=False)
        got = {x.account_id: x for x in fresh.ctrip.accounts}.get(account_id)
        if got is None:
            raise AccountAdminError("写入后读不回该账号")
        for k, v in fields.items():
            if getattr(got, k) != v:
                raise AccountAdminError(f"写入后 {k} 与预期不一致")
    except Exception as exc:  # noqa: BLE001
        shutil.copy2(backup, config_path)
        raise AccountAdminError(f"保存凭据失败已回滚：{exc}") from exc

    try:
        config_path.chmod(0o600)
    except OSError:
        pass
    return fresh, fields


# ---------------------------------------------------------------- 对外
def create_account(cfg, new: NewAccount):
    """新建账号：生成 ID → 校验 → 写配置 → 复核 → 返回新的 cfg 与账号。

    写入后用项目自己的 load_config 复核；解析失败或读不到新账号就回滚，
    宁可这次创建失败，也不能留下一个坏掉的 config.yaml。
    """
    existing = [x.account_id for x in cfg.ctrip.accounts]
    new.name = (new.name or "").strip()
    new.account = (new.account or "").strip()
    if not new.account_id:
        # 默认用登录账号本身当标识：目录名、日志、数据库里一眼能对上是哪个号，
        # 比 A002 这种流水号直观。account 里有非法字符时才退回流水号。
        new.account_id = id_from_account(new.account) or next_account_id(existing)
    new.remark = (new.remark or "").strip()
    new.scenic_id = (new.scenic_id or "").strip()
    validate(new, existing)

    config_path = cfg.config_path
    if not config_path or not Path(config_path).exists():
        raise AccountAdminError(
            "找不到 config.yaml，无法写入账号（检查 CTRIP_CONFIG 或工作目录）")
    config_path = Path(config_path)

    backup = append_to_yaml(config_path, new)
    try:
        fresh = load_config(str(config_path), validate=False)
        got = {x.account_id: x for x in fresh.ctrip.accounts}.get(new.account_id)
        if got is None:
            raise AccountAdminError("写入后没能读回新账号，可能 accounts 段结构特殊")
        if got.account != new.account or got.password != new.password:
            raise AccountAdminError("写入后读回的账号密码与提交的不一致")
        got.validate(require_credentials=True)
    except (ConfigError, AccountAdminError, Exception) as exc:
        shutil.copy2(backup, config_path)      # 回滚
        raise AccountAdminError(f"写入配置失败已回滚：{exc}") from exc

    try:
        config_path.chmod(0o600)               # 里面有明文密码
    except OSError:
        pass
    return fresh, got, backup


# ---------------------------------------------------------------- 删除
def delete_account(cfg, account_id: str):
    """从 config.yaml 删掉一个账号条目，返回 (新的 cfg, 备份路径)。

    只删这一段：条目尾部紧跟的注释和空行（比如被注释掉的示例账号）保留。
    写入后复核读不到该账号、且其余账号一个不少，否则回滚。
    """
    config_path = Path(cfg.config_path or "")
    if not cfg.config_path or not config_path.exists():
        raise AccountAdminError("找不到 config.yaml，无法删除账号")
    before = [x.account_id for x in cfg.ctrip.accounts]

    text = config_path.read_text(encoding="utf-8")
    newline = "\r\n" if "\r\n" in text else "\n"
    lines = text.splitlines()
    try:
        start, end, _ = _entry_span(lines, account_id)
    except AccountAdminError:
        raise AccountAdminError(
            f"config.yaml 里没有账号 {account_id} 的条目（可能来自环境变量 CTRIP_ACCOUNTS、"
            "账号表或账号服务），需要到来源处删除")
    while end > start + 1 and (not lines[end - 1].strip()
                               or lines[end - 1].lstrip().startswith("#")):
        end -= 1
    merged = lines[:start] + lines[end:]

    backup = config_path.with_suffix(
        config_path.suffix + f".bak-{datetime.now():%Y%m%d%H%M%S}")
    shutil.copy2(config_path, backup)
    config_path.write_text(newline.join(merged) + newline, encoding="utf-8")
    try:
        fresh = load_config(str(config_path), validate=False)
        after = [x.account_id for x in fresh.ctrip.accounts]
        if account_id in after:
            raise AccountAdminError("删除后仍能读到该账号")
        if sorted(after) != sorted(a for a in before if a != account_id):
            raise AccountAdminError("删除后其他账号有变化，已放弃")
    except Exception as exc:  # noqa: BLE001
        shutil.copy2(backup, config_path)
        raise AccountAdminError(f"删除失败已回滚：{exc}") from exc
    return fresh, backup
