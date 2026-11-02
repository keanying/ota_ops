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
    account_ids: list[str] = field(default_factory=list)   # 空=全部启用的账号
    skip_stale_login: bool = True

    @classmethod
    def from_dict(cls, raw: dict | None) -> "CollectSchedule":
        raw = raw or {}
        return cls(
            enabled=bool(raw.get("enabled", False)),
            daily_enabled=bool(raw.get("daily_enabled", False)),
            daily_at=raw.get("daily_at") or "02:30",
            daily_days=max(1, int(raw.get("daily_days") or 3)),
            monthly_enabled=bool(raw.get("monthly_enabled", False)),
            monthly_day=max(1, min(28, int(raw.get("monthly_day") or 6))),
            monthly_at=raw.get("monthly_at") or "03:00",
            account_ids=[str(x) for x in (raw.get("account_ids") or []) if str(x).strip()],
            skip_stale_login=bool(raw.get("skip_stale_login", True)),
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


def target_accounts(cfg, cs: CollectSchedule) -> list:
    """本次要跑哪些账号。"""
    accounts = [a for a in cfg.ctrip.accounts if a.status == "active"]
    if cs.account_ids:
        wanted = set(cs.account_ids)
        accounts = [a for a in accounts if a.account_id in wanted]
    return accounts


def month_param(today: date | None = None) -> str:
    """上个月 —— 评级类接口只对上月有数据。"""
    today = today or date.today()
    first = today.replace(day=1)
    return (first - timedelta(days=1)).strftime("%Y-%m")


def plan(cfg, cs: CollectSchedule, job: str) -> list[dict]:
    """算出这次要发起哪些采集。返回每个账号的参数，附带跳过原因。

    只做计算不做副作用，方便在页面上「预览」和在测试里断言。
    """
    store = CookieStore(cfg)
    out = []
    for a in target_accounts(cfg, cs):
        item = {"account_id": a.account_id, "job": "day" if job == "daily" else "month",
                "skip": "", "params": {}}
        if cs.skip_stale_login:
            if not store.exists(a.account_id):
                item["skip"] = "从未登录"
            elif not store.is_fresh(a.account_id):
                item["skip"] = "登录态已过期"
        if not item["skip"]:
            item["params"] = ({"days": cs.daily_days} if job == "daily"
                              else {"month": month_param()})
        out.append(item)
    return out
