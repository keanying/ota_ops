# -*- coding: utf-8 -*-
"""时间区间解析与切片。

采集有两种时间粒度，各自的默认口径不同：

* **日粒度**（交易卡片）默认采 T+1，即到「昨天」为止。
  当天数据在携程侧尚未跑完，采当天会拿到不完整的数字。
* **月粒度**（目的地评级、供应商评级）默认采「上个月」，
  当月未结束，评级分数仍在变动。

区间一旦拉长（例如补两年历史），必须切片请求：一次性传 700 天
携程侧会超时或截断，且失败后整段都要重来。切片大小默认 30 天，
与配置 ``collect.batch_days`` 一致。
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timedelta

__all__ = [
    "DateSlice", "MonthSlice",
    "parse_date", "parse_month", "month_add", "month_end",
    "resolve_date_range", "slice_dates",
    "resolve_month_range", "month_list",
]


# =============================================================================
# 解析
# =============================================================================

def parse_date(text: str, *, field: str = "日期") -> date:
    """解析 ``YYYY-MM-DD``，同时容忍 ``YYYYMMDD`` 与 ``YYYY/MM/DD``。"""
    raw = (text or "").strip()
    if not raw:
        raise ValueError(f"{field}不能为空")
    normalized = raw.replace("/", "-")
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(normalized, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"{field}格式应为 YYYY-MM-DD，收到「{raw}」")


def parse_month(text: str, *, field: str = "月份") -> str:
    """解析 ``YYYY-MM``，同时容忍 ``YYYYMM`` 与 ``YYYY/MM``，返回规范化字符串。"""
    raw = (text or "").strip()
    if not raw:
        raise ValueError(f"{field}不能为空")
    normalized = raw.replace("/", "-")
    for fmt in ("%Y-%m", "%Y%m"):
        try:
            parsed = datetime.strptime(normalized, fmt)
            return f"{parsed.year:04d}-{parsed.month:02d}"
        except ValueError:
            continue
    raise ValueError(f"{field}格式应为 YYYY-MM，收到「{raw}」")


def month_add(month: str, delta: int) -> str:
    """月份加减，自动跨年。"""
    year, mon = (int(x) for x in parse_month(month).split("-"))
    total = year * 12 + (mon - 1) + delta
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


def month_end(month: str) -> date:
    """该月最后一天（自动处理闰年 2 月）。"""
    year, mon = (int(x) for x in parse_month(month).split("-"))
    return date(year, mon, calendar.monthrange(year, mon)[1])


def month_of(day: date) -> str:
    return f"{day.year:04d}-{day.month:02d}"


# =============================================================================
# 日粒度
# =============================================================================

@dataclass(frozen=True)
class DateSlice:
    """一个日期批次。``index`` / ``total`` 从 1 开始，仅用于日志与进度显示。"""
    start: date
    end: date
    index: int = 1
    total: int = 1

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1

    @property
    def key(self) -> str:
        return f"{self.start:%Y%m%d}_{self.end:%Y%m%d}"

    def __str__(self) -> str:
        return f"[{self.index}/{self.total}] {self.start} ~ {self.end}（{self.days} 天）"


def resolve_date_range(*, today: date,
                       start_date: date | None = None,
                       end_date: date | None = None,
                       days: int | None = None,
                       default_days: int = 30) -> tuple[date, date]:
    """推导日粒度区间，返回 ``(start, end)``，两端均含。

    优先级：显式 ``start_date`` > ``days`` 推算 > ``default_days`` 推算。
    ``end_date`` 缺省为昨天（T+1 口径）。

    ``start_date`` 与 ``days`` 同时给出会冲突（一个定起点、一个定长度，
    若二者算出的起点不一致，无法判断用户真实意图），因此直接报错而不是
    默默取其一。
    """
    end = end_date or (today - timedelta(days=1))

    if start_date is not None and days is not None:
        implied = end - timedelta(days=days - 1)
        if implied != start_date:
            raise ValueError(
                f"--start-date（{start_date}）与 --days（{days}）冲突："
                f"按 --days 从截止日 {end} 往前推得到的起点是 {implied}。"
                "请只用其中一个：指定起点用 --start-date，指定长度用 --days"
            )

    if start_date is not None:
        start = start_date
    else:
        span = days if days is not None else default_days
        if span < 1:
            raise ValueError(f"采集天数必须大于 0，收到 {span}")
        start = end - timedelta(days=span - 1)

    if start > end:
        raise ValueError(f"开始日期 {start} 晚于截止日期 {end}")
    if end > today:
        raise ValueError(
            f"截止日期 {end} 晚于今天 {today}；"
            "携程当天数据尚未结算，默认口径为采到昨天（T+1）"
        )
    return start, end


def slice_dates(start: date, end: date, batch_days: int = 30) -> list[DateSlice]:
    """把区间切成若干批次，**从早到晚**顺序返回。

    顺序很重要：历史数据从早往晚采，中途失败时已落地的部分是连续的，
    便于人工判断补到了哪一天。
    """
    if start > end:
        raise ValueError(f"开始日期 {start} 晚于截止日期 {end}")
    size = max(1, int(batch_days))

    spans: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        batch_end = min(cursor + timedelta(days=size - 1), end)
        spans.append((cursor, batch_end))
        cursor = batch_end + timedelta(days=1)

    total = len(spans)
    return [DateSlice(s, e, i, total) for i, (s, e) in enumerate(spans, start=1)]


# =============================================================================
# 月粒度
# =============================================================================

@dataclass(frozen=True)
class MonthSlice:
    month: str
    index: int = 1
    total: int = 1

    def __str__(self) -> str:
        return f"[{self.index}/{self.total}] {self.month}"


def resolve_month_range(*, today: date,
                        start_month: str | None = None,
                        end_month: str | None = None,
                        month: str | None = None,
                        months: list[str] | None = None) -> list[str]:
    """推导月粒度的月份列表，升序去重。

    四种入参的优先级（从高到低）：显式 ``months`` 列表 > ``start_month``
    区间 > 单个 ``month`` > 默认上个月。
    """
    default_month = month_add(month_of(today), -1)  # 上个月

    if months:
        resolved = [parse_month(m) for m in months]
    elif start_month:
        start = parse_month(start_month, field="起始月份")
        end = parse_month(end_month, field="截止月份") if end_month else default_month
        if start > end:
            raise ValueError(f"起始月份 {start} 晚于截止月份 {end}")
        resolved = month_list(start, end)
    elif month:
        resolved = [parse_month(month)]
    else:
        resolved = [default_month]

    current = month_of(today)
    resolved = sorted(set(resolved))
    if resolved and resolved[-1] > current:
        raise ValueError(f"月份 {resolved[-1]} 超过当前月 {current}")
    return resolved


def month_list(start: str, end: str) -> list[str]:
    """闭区间月份列表。"""
    start, end = parse_month(start), parse_month(end)
    out: list[str] = []
    cursor = start
    while cursor <= end:
        out.append(cursor)
        cursor = month_add(cursor, 1)
    return out


def to_month_slices(months: list[str]) -> list[MonthSlice]:
    total = len(months)
    return [MonthSlice(m, i, total) for i, m in enumerate(months, start=1)]
