"""数据同步：把已落库的采集数据按日/月定时推给下游 API。

做什么
------
从项目自己的 MySQL 表里按时间窗口取数，分批 POST 给下游的
``crossDomainSync`` 接口，连接地址、鉴权 key、定时规则、每张表的
``pkId`` 全部在控制台页面上配，不用改代码。

几个刻意的选择
--------------
* **pkId 按表单独配置**。下游要的主键与表自身的唯一键并不一致
  （交易表唯一键含 period_type/signing_code/biz_type，下游只要
  account_id/scenic_id/transaction_date/date_type），所以不能拿
  ``TableSchema.unique_key`` 直接当 pkId 用。
* **时间窗口用表自己的 partition_column**（交易表是 transaction_date，
  评级两表是 evaluation_month），与采集侧的分区口径保持一致。
* **推送失败不静默**。每次运行都留一条记录（条数、耗时、失败原因），
  页面上能看到最近若干次的结果。
* **只读下游不改本地**。同步是纯读取 + 上报，任何失败都不会动本地数据。
"""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from ctrip.logging_utils import get_logger
from ctrip.schema import BUSINESS_TABLES, TABLES
from ctrip.timeutil import get_timezone

logger = get_logger("datasync")

# 下游默认要的主键。与表自身的唯一键不同，见模块文档。
DEFAULT_PK = {
    "kylin_daily": ["account_id", "scenic_id", "transaction_date", "date_type"],
    "destination_rating": ["account_id", "scenic_id", "evaluation_month", "region_id"],
    "supplier_rate_score": ["account_id", "scenic_id", "evaluation_month", "region_id"],
}

# 哪些表归日同步、哪些归月同步
DAILY_TABLES = ("kylin_daily",)
MONTHLY_TABLES = ("destination_rating", "supplier_rate_score")


# =============================================================================
# 配置
# =============================================================================

@dataclass
class TableSync:
    enabled: bool = True
    remote_table: str = ""     # 下游的 tableName，留空则用本地表名
    pk: list[str] = field(default_factory=list)
    lookback: int = 7          # 日表回溯天数 / 月表回溯月数


@dataclass
class Schedule:
    daily_enabled: bool = False
    daily_at: str = "03:30"        # HH:MM，本机时区
    monthly_enabled: bool = False
    monthly_day: int = 5           # 每月几号
    monthly_at: str = "04:00"


@dataclass
class SyncConfig:
    enabled: bool = False
    url: str = ""
    header_name: str = "xpftkey"
    key: str = ""
    extra_headers: dict = field(default_factory=dict)
    batch_size: int = 200
    timeout: int = 30
    retries: int = 2
    after_collect: bool = True   # 采集完成后立刻推一次（与定时互不影响）
    tables: dict = field(default_factory=dict)      # table_key -> TableSync
    schedule: Schedule = field(default_factory=Schedule)

    @classmethod
    def from_dict(cls, raw: dict | None) -> "SyncConfig":
        raw = raw or {}
        tables = {}
        for key in BUSINESS_TABLES:
            t = (raw.get("tables") or {}).get(key) or {}
            tables[key] = TableSync(
                enabled=bool(t.get("enabled", True)),
                remote_table=(t.get("remote_table") or "").strip(),
                pk=list(t.get("pk") or DEFAULT_PK.get(key, [])),
                lookback=int(t.get("lookback") or (7 if key in DAILY_TABLES else 2)),
            )
        sc = raw.get("schedule") or {}
        return cls(
            enabled=bool(raw.get("enabled", False)),
            url=raw.get("url", ""),
            header_name=raw.get("header_name") or "xpftkey",
            key=raw.get("key", ""),
            extra_headers=dict(raw.get("extra_headers") or {}),
            batch_size=max(1, int(raw.get("batch_size") or 200)),
            timeout=int(raw.get("timeout") or 30),
            retries=int(raw.get("retries") or 2),
            after_collect=bool(raw.get("after_collect", True)),
            tables=tables,
            schedule=Schedule(
                daily_enabled=bool(sc.get("daily_enabled", False)),
                daily_at=sc.get("daily_at") or "03:30",
                monthly_enabled=bool(sc.get("monthly_enabled", False)),
                monthly_day=max(1, min(28, int(sc.get("monthly_day") or 5))),
                monthly_at=sc.get("monthly_at") or "04:00",
            ),
        )

    def to_dict(self) -> dict:
        d = asdict(self)
        d["tables"] = {k: asdict(v) if not isinstance(v, dict) else v
                       for k, v in self.tables.items()}
        return d

    def masked(self) -> dict:
        d = self.to_dict()
        k = self.key
        d["key"] = (k[:4] + "…" + k[-4:]) if len(k) > 12 else ("…" * 3 if k else "")
        d["key_set"] = bool(k)
        return d


# =============================================================================
# 取数
# =============================================================================

def _jsonable(v: Any) -> Any:
    """转成下游能收的类型。

    下游示例里数值也是字符串（"312.00"），这里保持 Decimal 的原始标度，
    不做 float 转换 —— float 会把 0.500000 变成 0.5，金额还可能掉精度。
    """
    if v is None:
        return None
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "replace")
    if isinstance(v, (int, float, str, bool)):
        return v
    return str(v)


def today_of(cfg=None) -> date:
    """业务口径的「今天」。

    采集 CLI 按 ``run.timezone``（默认 Asia/Shanghai）推导昨天 / 上个月，
    控制台这边也必须用同一个时区，否则服务器跑在 UTC 时，
    零点前后两边对「昨天」「上个月」的理解会差一天 / 一个月，
    推送窗口就和实际采集的区间对不上。
    """
    tz_name = getattr(getattr(cfg, "run", None), "timezone", "") or "Asia/Shanghai"
    return datetime.now(get_timezone(tz_name, log=logger)).date()


def last_month(today: date) -> str:
    """上个月 YYYY-MM —— 评级类数据只有上月及更早。"""
    return (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")


def window_for(table_key: str, lookback: int, today: date | None = None) -> tuple[str, str]:
    """返回 (下界, 上界)，都是闭区间，格式与表里的时间列一致。"""
    today = today or date.today()
    if table_key in DAILY_TABLES:
        end = today - timedelta(days=1)          # 采到昨天为止
        start = end - timedelta(days=max(0, lookback - 1))
        return start.isoformat(), end.isoformat()
    # 月表：从**上个月**往前推 lookback 个月。
    # 不能从当月算起：评级数据只有上月及更早，当月永远是空的；
    # 之前「含当月」的算法在 lookback=1 时窗口只有当月，定时补推永远取到 0 行。
    y, m = (int(x) for x in last_month(today).split("-"))
    months = []
    for _ in range(max(1, lookback)):
        months.append(f"{y}-{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return min(months), max(months)


def _connect(cfg):
    import pymysql

    mc = cfg.mysql
    return pymysql.connect(
        host=mc.host, port=mc.port, user=mc.user, password=mc.password,
        database=mc.database, charset=mc.charset,
        connect_timeout=mc.connect_timeout,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _physical_table(cfg, table_key: str) -> str:
    """本地物理表名，带上配置的表前缀（与采集写入时一致）。"""
    return TABLES[table_key].table_name(getattr(cfg.mysql, "table_prefix", "") or "")


def month_coverage(cfg, month: str, account_ids: list[str],
                   table_keys: tuple[str, ...] = MONTHLY_TABLES) -> dict[str, dict[str, int]]:
    """各账号在某个月、各月表里有多少行。用来判断评级数据补齐了没有。

    返回 {account_id: {table_key: 行数}}；没有行的账号/表也会出现，值为 0。
    """
    out = {aid: {k: 0 for k in table_keys} for aid in account_ids}
    if not account_ids:
        return out
    conn = _connect(cfg)
    try:
        with conn.cursor() as cur:
            for key in table_keys:
                col = TABLES[key].partition_column
                marks = ",".join(["%s"] * len(account_ids))
                cur.execute(
                    f"SELECT `account_id`, COUNT(*) AS n FROM `{_physical_table(cfg, key)}` "
                    f"WHERE `{col}` = %s AND `account_id` IN ({marks}) GROUP BY `account_id`",
                    [month, *account_ids])
                for r in cur.fetchall() or []:
                    if r["account_id"] in out:
                        out[r["account_id"]][key] = int(r["n"] or 0)
    finally:
        conn.close()
    return out


def fetch_rows(cfg, table_key: str, lookback: int,
               account_ids: list[str] | None = None,
               limit: int | None = None,
               window: tuple[str, str] | None = None) -> tuple[list[dict], str, str]:
    """从 MySQL 读一张表的窗口数据。返回 (行, 窗口下界, 窗口上界)。

    window 显式给定时用它 —— 采集完立刻推的场景要按那一次采集的区间推，
    而不是按配置里的回溯天数，否则会把无关的历史数据一起重推一遍。
    """
    schema = TABLES[table_key]
    col = schema.partition_column
    low, high = window or window_for(table_key, lookback, today_of(cfg))

    where = [f"`{col}` >= %s", f"`{col}` <= %s"]
    args: list[Any] = [low, high]
    if account_ids:
        where.append("`account_id` IN (" + ",".join(["%s"] * len(account_ids)) + ")")
        args.extend(account_ids)
    sql = f"SELECT * FROM `{_physical_table(cfg, table_key)}` WHERE {' AND '.join(where)} ORDER BY `{col}`"
    if limit:
        sql += f" LIMIT {int(limit)}"

    conn = _connect(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            raw = cur.fetchall() or []
    finally:
        conn.close()

    rows = []
    for r in raw:
        row = {k: _jsonable(v) for k, v in r.items()}
        row.pop("id", None)          # 本地自增主键对下游没有意义
        rows.append(row)
    return rows, low, high


# =============================================================================
# 推送
# =============================================================================

class SyncError(RuntimeError):
    pass


def post_batch(sc: SyncConfig, table_name: str, pk: list[str],
               rows: list[dict]) -> dict:
    """推一批。只用标准库，少一个依赖少一份装不上的风险。"""
    payload = json.dumps({"tableName": table_name, "pkId": pk, "data": rows},
                         ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json", **(sc.extra_headers or {})}
    if sc.key:
        headers[sc.header_name] = sc.key

    last: Exception | None = None
    for attempt in range(sc.retries + 1):
        req = urllib.request.Request(sc.url, data=payload, headers=headers,
                                     method="POST")
        try:
            with urllib.request.urlopen(req, timeout=sc.timeout) as resp:
                body = resp.read().decode("utf-8", "replace")
            data = json.loads(body) if body else {}
            # 下游约定 code=0 / status=true 为成功
            if data.get("code") not in (0, None) or data.get("status") is False:
                raise SyncError(f"下游返回失败：{data.get('msg') or body[:200]}")
            return data.get("data") or {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:200]
            last = SyncError(f"HTTP {exc.code}：{detail}")
            if 400 <= exc.code < 500:
                break        # 报文或鉴权错，重试多少次都一样
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError,
                SyncError) as exc:
            last = exc if isinstance(exc, SyncError) else SyncError(str(exc))
        if attempt < sc.retries:
            time.sleep(1.5 * (attempt + 1))
    raise last or SyncError("推送失败")


# =============================================================================
# 运行
# =============================================================================

HISTORY: deque = deque(maxlen=30)
_running = threading.Lock()


def run_sync(cfg, sc: SyncConfig, job: str = "daily",
             account_ids: list[str] | None = None,
             dry_run: bool = False, on_log=None,
             window: tuple[str, str] | None = None,
             reason: str = "manual") -> dict:
    """跑一次同步。job = daily | monthly | all。

    window 给定时按该区间取数（采集完立刻推用），否则按各表配置的回溯窗口。
    """
    def emit(msg: str, level: str = "info") -> None:
        logger.info(msg) if level == "info" else logger.warning(msg)
        if on_log:
            on_log(level, msg)

    if not sc.url and not dry_run:
        raise SyncError("未配置同步地址")

    keys = {"daily": DAILY_TABLES, "monthly": MONTHLY_TABLES,
            "all": tuple(BUSINESS_TABLES)}.get(job)
    if keys is None:
        raise SyncError(f"未知的同步任务：{job}")

    started = time.time()
    result = {"job": job, "dry_run": dry_run, "tables": {}, "reason": reason,
              "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    ok_all = True

    if not _running.acquire(blocking=False):
        raise SyncError("已有同步任务在跑，稍后再试")
    try:
        for key in keys:
            ts = sc.tables.get(key)
            if isinstance(ts, dict):
                ts = TableSync(**ts)
            if ts is None or not ts.enabled:
                continue
            schema = TABLES[key]
            pk = ts.pk or DEFAULT_PK.get(key) or schema.unique_key
            # 下游表名可与本地表名不同（对方库里叫什么由对方定），留空才用本地表名
            remote = ts.remote_table or schema.name
            item = {"table": schema.name, "remote_table": remote,
                    "pk": pk, "rows": 0, "batches": 0,
                    "inserted": 0, "updated": 0, "ok": True, "error": ""}
            try:
                rows, low, high = fetch_rows(cfg, key, ts.lookback, account_ids,
                                             window=window)
                item.update(rows=len(rows), window=f"{low} ~ {high}")
                emit(f"[{schema.name} → {remote}] 窗口 {low} ~ {high}，"
                     f"取到 {len(rows)} 行")
                if not rows:
                    result["tables"][key] = item
                    continue
                if dry_run:
                    item["batches"] = (len(rows) + sc.batch_size - 1) // sc.batch_size
                    emit(f"[{schema.name}] 试运行，不实际推送"
                         f"（将分 {item['batches']} 批）")
                    result["tables"][key] = item
                    continue
                for i in range(0, len(rows), sc.batch_size):
                    chunk = rows[i:i + sc.batch_size]
                    data = post_batch(sc, remote, pk, chunk)
                    item["batches"] += 1
                    item["inserted"] += int(data.get("inserted") or 0)
                    item["updated"] += int(data.get("updated") or 0)
                emit(f"[{remote}] 推送完成：{item['batches']} 批，"
                     f"新增 {item['inserted']}，更新 {item['updated']}")
            except Exception as exc:  # noqa: BLE001
                ok_all = False
                item.update(ok=False, error=str(exc))
                emit(f"[{schema.name}] 失败：{exc}", "warn")
            result["tables"][key] = item
    finally:
        _running.release()

    result["ok"] = ok_all
    result["elapsed"] = round(time.time() - started, 1)
    HISTORY.appendleft(result)
    return result


# =============================================================================
# 定时
# =============================================================================

def _parse_hhmm(text: str, fallback: tuple[int, int] = (3, 30)) -> tuple[int, int]:
    try:
        h, m = text.split(":")
        return max(0, min(23, int(h))), max(0, min(59, int(m)))
    except Exception:  # noqa: BLE001
        return fallback


def next_daily(sc: Schedule, now: datetime | None = None) -> datetime | None:
    if not sc.daily_enabled:
        return None
    now = now or datetime.now()
    h, m = _parse_hhmm(sc.daily_at)
    nxt = now.replace(hour=h, minute=m, second=0, microsecond=0)
    return nxt if nxt > now else nxt + timedelta(days=1)


def next_monthly(sc: Schedule, now: datetime | None = None) -> datetime | None:
    if not sc.monthly_enabled:
        return None
    now = now or datetime.now()
    h, m = _parse_hhmm(sc.monthly_at, (4, 0))
    day = max(1, min(28, sc.monthly_day))     # 限制在 28 内，免得 2 月踩空
    nxt = now.replace(day=day, hour=h, minute=m, second=0, microsecond=0)
    if nxt > now:
        return nxt
    y, mo = (now.year + 1, 1) if now.month == 12 else (now.year, now.month + 1)
    return nxt.replace(year=y, month=mo)


@dataclass
class JobSpec:
    """一个定时任务。采集与同步共用同一套时间计算与触发逻辑。"""

    name: str                  # daily / monthly，用于日志
    kind: str                  # collect / sync
    get_schedule: Any          # () -> Schedule（每轮重新取，改配置立刻生效）
    run: Any                   # () -> None


def prev_daily(sc: Schedule, now: datetime | None = None) -> datetime | None:
    """最近一次**应该**触发的时刻（<= now）。没启用返回 None。"""
    if not sc.daily_enabled:
        return None
    now = now or datetime.now()
    h, m = _parse_hhmm(sc.daily_at)
    today = now.replace(hour=h, minute=m, second=0, microsecond=0)
    return today if today <= now else today - timedelta(days=1)


def prev_monthly(sc: Schedule, now: datetime | None = None) -> datetime | None:
    if not sc.monthly_enabled:
        return None
    now = now or datetime.now()
    h, m = _parse_hhmm(sc.monthly_at, (4, 0))
    day = max(1, min(28, sc.monthly_day))
    this = now.replace(day=day, hour=h, minute=m, second=0, microsecond=0)
    if this <= now:
        return this
    y, mo = (now.year - 1, 12) if now.month == 1 else (now.year, now.month - 1)
    return this.replace(year=y, month=mo)


class Scheduler(threading.Thread):
    """统一调度：采集与同步都挂在这一个线程上。

    每轮重新读配置计算下次时间 —— 页面上改完定时立刻生效，不用重启服务。
    到点误差控制在一分钟内（每 20 秒检查一次，命中窗口 30 秒）。
    """

    def __init__(self, jobs: list[JobSpec] | None = None,
                 catch_up_seconds: int = 1800) -> None:
        super().__init__(name="scheduler", daemon=True)
        self.jobs = jobs or []
        # 启动时，早于这个窗口的错过时刻不补跑 —— 服务停了两天再起来，
        # 没必要把前天的任务翻出来跑；半小时内的则补上，覆盖「重启一下」的情况。
        self.catch_up_seconds = catch_up_seconds
        self._stopping = threading.Event()
        self.next_runs: dict[str, datetime | None] = {}
        self._fired: dict[str, datetime] = {}
        self._booted_at = datetime.now()

    def stop(self) -> None:
        self._stopping.set()

    def next_of(self, kind: str, name: str) -> datetime | None:
        return self.next_runs.get(f"{kind}.{name}")

    def last_fired(self, kind: str, name: str) -> datetime | None:
        return self._fired.get(f"{kind}.{name}")

    def _due(self, job: JobSpec, sched: Schedule, now: datetime) -> datetime | None:
        """这一轮该不该跑。返回应触发的时刻，不该跑返回 None。

        判据是「到点或已过点，且这个时刻还没跑过」，而不是「距离下次不到 N 秒」。
        后者只能在目标时刻**之前**的一小段窗口里命中：到点当秒 next_* 就跳到
        下一周期，窗口一旦被别的任务占住（采集动辄几分钟）当天就整个跳过。
        """
        prev = (prev_daily(sched, now) if job.name == "daily"
                else prev_monthly(sched, now))
        if prev is None:
            return None
        key = f"{job.kind}.{job.name}"
        fired = self._fired.get(key)
        if fired is not None and prev <= fired:
            return None                     # 这个时刻已经跑过
        if fired is None:
            # 本次启动后的第一轮：只补跑刚刚错过的，太久以前的不管
            if (now - prev).total_seconds() > self.catch_up_seconds:
                self._fired[key] = prev     # 记下但不跑，避免下轮又判成待跑
                return None
        return prev

    def run(self) -> None:
        while not self._stopping.is_set():
            try:
                now = datetime.now()
                for job in self.jobs:
                    key = f"{job.kind}.{job.name}"
                    try:
                        sched = job.get_schedule()
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("读取 %s 定时配置失败：%s", key, exc)
                        continue

                    self.next_runs[key] = (next_daily(sched, now) if job.name == "daily"
                                           else next_monthly(sched, now))
                    due = self._due(job, sched, now)
                    if due is None:
                        continue

                    self._fired[key] = due   # 先记后跑：任务失败也不要反复重跑
                    logger.info("定时触发 %s（应于 %s）", key, due.strftime("%Y-%m-%d %H:%M"))
                    try:
                        job.run()
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("定时任务 %s 失败：%s", key, exc)
            except Exception as exc:  # noqa: BLE001
                logger.warning("调度线程异常（已忽略，继续运行）：%s", exc)
            self._stopping.wait(20)
