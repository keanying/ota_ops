"""定时采集。

与定时同步共用 ``datasync.Scheduler``，配置也用同一套 ``Schedule`` 结构，
所以「每天几点 / 每月几号几点」的语义两边完全一致。

一条链路：**定时到点 → 采集（写本地 MySQL）→ 采集完成回调 → 推送下游**。
所以配了定时采集之后，同步那边的定时就只是兜底（补推没推成功的），
不必两边都开。

几个刻意的选择
--------------
* **登录态失效就跳过，并发飞书通知。** 采集靠 Cookie，登录态过期时硬跑
  只会得到一堆失败请求；不如直接叫人来登一次。
* **账号之间串行。** 采集是子进程，同时跑多个会同时压 MySQL 和对方接口；
  而且同一账号已有任务在跑时直接跳过，不排队堆积。
* **只管发起，不等结果。** 每个账号的采集结果由 CollectTask 自己写进
  run_log 和页面日志，这里只负责按时把它拉起来。
* **评级没出数就逐日补采。** 携程的月度评级（尤其目的地评级）月初往往
  还没产出上月数据，1 号跑一次常常拿到 0 行。月任务之后的
  ``monthly_retry_days`` 天里，每天的定时会顺带查一下本地库：
  上月哪张评级表还没数据，就只补采那一张，补齐即停。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, timedelta

from ctrip.auth import CookieStore
from ctrip.logging_utils import get_logger

from .datasync import Schedule

logger = get_logger("collect-schedule")


@dataclass
class CollectSchedule:
    """字段名与 datasync.Schedule 保持一致，好复用同一套 next_* 计算。"""

    enabled: bool = False
    daily_enabled: bool = False
    daily_at: str = "02:30"
    daily_days: int = 3          # 每次回补最近几天，覆盖延迟产出的数据
    monthly_enabled: bool = False
    monthly_day: int = 6         # 评级类接口只对上月有数据，月初几天后跑
    monthly_at: str = "03:00"
    monthly_retry_days: int = 7  # 月任务后若干天内，评级仍缺数则每天补采；0=不补
    account_ids: list[str] = field(default_factory=list)   # 空=全部启用的账号
    skip_stale_login: bool = True
    # 到点先打开浏览器刷新登录态：profile 里还登着就直接保存；掉了就用配置里的账号密码自动登录，
    # 要滑块 / 短信时发飞书通知等人处理。每天只跑一次、浏览器平时是关的，不刷新的话登录态基本都过期了
    refresh_login: bool = True
    # 定时采哪几张表：kylin（交易，日任务）/ rating / score（评级，月任务）。
    # 页面上按表勾选；日 / 月任务开不开由这里推出来，不再单独维护两个开关。
    jobs: list[str] = field(default_factory=lambda: list(ALL_JOBS))

    @property
    def month_jobs(self) -> list[str]:
        return [j for j in self.jobs if j in ("rating", "score")]

    @classmethod
    def from_dict(cls, raw: dict | None) -> "CollectSchedule":
        raw = raw or {}
        if "jobs" in raw:
            jobs = [j for j in ALL_JOBS if j in (raw.get("jobs") or [])]
            daily_on = "kylin" in jobs
            monthly_on = any(j in jobs for j in ("rating", "score"))
        else:
            # 老配置没有 jobs：按原来的日 / 月开关推出来
            daily_on = bool(raw.get("daily_enabled", False))
            monthly_on = bool(raw.get("monthly_enabled", False))
            jobs = (["kylin"] if daily_on else []) + (["rating", "score"] if monthly_on else [])
        return cls(
            jobs=jobs,
            enabled=bool(raw.get("enabled", False)),
            daily_enabled=daily_on,
            daily_at=raw.get("daily_at") or "02:30",
            daily_days=max(1, int(raw.get("daily_days") or 3)),
            monthly_enabled=monthly_on,
            monthly_day=max(1, min(28, int(raw.get("monthly_day") or 6))),
            monthly_at=raw.get("monthly_at") or "03:00",
            monthly_retry_days=max(0, min(27, int(raw.get("monthly_retry_days", 7) or 0))),
            account_ids=[str(x) for x in (raw.get("account_ids") or []) if str(x).strip()],
            skip_stale_login=bool(raw.get("skip_stale_login", True)),
            refresh_login=bool(raw.get("refresh_login", True)),
        )

    def to_dict(self) -> dict:
        return asdict(self)

    def as_schedule(self) -> Schedule:
        """转成调度器认识的形状；enabled 关掉时两个开关一并关掉。"""
        return Schedule(
            daily_enabled=self.enabled and self.daily_enabled,
            daily_at=self.daily_at,
            monthly_enabled=self.enabled and self.monthly_enabled,
            monthly_day=self.monthly_day,
            monthly_at=self.monthly_at,
        )

    def as_catchup_schedule(self) -> Schedule:
        """评级补采：每天在月任务的时刻检查一次（是否在补采期由任务自己判断）。"""
        return Schedule(
            daily_enabled=(self.enabled and self.monthly_enabled
                           and self.monthly_retry_days > 0),
            daily_at=self.monthly_at,
        )


def target_accounts(cfg, cs: CollectSchedule) -> list:
    """本次要跑哪些账号。"""
    accounts = [a for a in cfg.ctrip.accounts if a.status == "active"]
    if cs.account_ids:
        wanted = set(cs.account_ids)
        accounts = [a for a in accounts if a.account_id in wanted]
    return accounts


ALL_JOBS = ("kylin", "rating", "score")

# 月表 → CLI 的 --job 名
MONTH_JOBS = {"destination_rating": "rating", "supplier_rate_score": "score"}
JOB_LABELS = {"rating": "目的地评级", "score": "供应商评级"}


def month_param(today: date | None = None) -> str:
    """上个月 —— 评级类接口只对上月有数据。"""
    today = today or date.today()
    first = today.replace(day=1)
    return (first - timedelta(days=1)).strftime("%Y-%m")


def in_retry_window(cs: CollectSchedule, today: date) -> bool:
    """今天是否处在「月任务之后的补采期」内（不含月任务当天）。"""
    if not (cs.enabled and cs.monthly_enabled and cs.monthly_retry_days > 0):
        return False
    return cs.monthly_day < today.day <= cs.monthly_day + cs.monthly_retry_days


def login_issue(store: CookieStore, account_id: str) -> str:
    """登录态现在有什么问题（只看本地文件和时间，不发请求）。"""
    if not store.exists(account_id):
        return "从未登录"
    if not store.is_fresh(account_id):
        return "登录态已过期"
    return ""


def _login_skip(store: CookieStore, cs: CollectSchedule, account_id: str) -> str:
    # 开了「到点先刷新登录态」就不在这里跳过：真正跑的时候先刷新，刷新失败才跳过
    if not cs.skip_stale_login or cs.refresh_login:
        return ""
    return login_issue(store, account_id)


def plan(cfg, cs: CollectSchedule, job: str, today: date | None = None,
         coverage: dict[str, dict[str, int]] | None = None) -> list[dict]:
    """算出这次要发起哪些采集。返回每个账号的参数，附带跳过原因。

    job = daily | monthly | catchup。catchup 是评级补采：需要传入 ``coverage``
    （``datasync.month_coverage`` 的结果），只为上月还缺数据的表发起采集。

    只做计算不做副作用，方便在页面上「预览」和在测试里断言。
    """
    today = today or date.today()
    month = month_param(today)
    store = CookieStore(cfg)
    out = []
    for a in target_accounts(cfg, cs):
        item = {"account_id": a.account_id, "job": "day" if job == "daily" else "month",
                "kind": job, "skip": "", "params": {}}
        if job == "catchup":
            have = (coverage or {}).get(a.account_id, {})
            # 只补定时里勾了的评级表
            missing = [j for k, j in MONTH_JOBS.items() if j in cs.month_jobs and not have.get(k)]
            if not missing:
                continue                      # 上月已齐，不打扰
            item["missing"] = missing
        item["login"] = login_issue(store, a.account_id)
        item["refresh"] = cs.refresh_login
        item["skip"] = _login_skip(store, cs, a.account_id)
        if not item["skip"]:
            if job == "daily":
                item["params"] = {"days": cs.daily_days, "jobs": ["kylin"]}
            elif job == "catchup":
                item["params"] = {"month": month, "jobs": item["missing"]}
            else:
                item["params"] = {"month": month, "jobs": cs.month_jobs}
        out.append(item)
    return out
