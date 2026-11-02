#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""历史数据批量回溯采集。

用途
----
一次性把某个历史区间（如 2025-01 至今，约 20 个月）的数据全部采下来。
日常增量采集用 ``python -m ctrip.cli collect`` 即可，本脚本专为首次建仓、
补历史数据这类"跑一次就够"的场景设计。

为什么不直接用 ``collect --days 730``
--------------------------------------
一次性请求两年会带来几个现实问题，本脚本逐一处理：

1. **上游窗口未知**：携程后台对历史数据的可查范围有限制，且不同账号、
   不同签约类型可能不同。直接跑 730 天，若上游只给 1 年，你会得到一堆
   空结果却不知道边界在哪。本脚本先做**窗口探测**，明确告诉你最早能取到
   哪个月，再决定实际采集区间。
2. **风控**：交易卡片是「签约类型 × 日期类型 × 日期批次」三重循环，两年
   意味着数百次请求连续打出去。本脚本按批次分段、批次之间强制间隔，
   并支持随机抖动。
3. **中断成本**：跑到一半网络断了或登录态过期，重头再来代价太大。
   本脚本把每个切片的完成状态写入进度文件，**断点续跑**只补未完成的部分。
4. **失败定位**：哪个月失败了、失败原因是什么，需要能一眼看清而不是
   埋在几千行日志里。结束时输出分片级汇总表。

用法
----
探测上游数据窗口（只读，不落库，建议先跑）::

    python tools/backfill.py --probe

回溯 2025-01 至今的全部数据（三类任务）::

    python tools/backfill.py --start 2025-01 --sink both

只回溯交易数据，中断后继续::

    python tools/backfill.py --start 2025-01 --job kylin --resume

重新跑某个失败的月份::

    python tools/backfill.py --start 2025-06 --end 2025-06 --force
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ctrip.accounts import resolve_accounts  # noqa: E402
from ctrip.api import ApiError, SessionExpired  # noqa: E402
from ctrip.auth import build_api, ensure_logged_in  # noqa: E402
from ctrip.collectors import get_collector  # noqa: E402
from ctrip.config import AccountConfig, AppConfig, load_config  # noqa: E402
from ctrip.logging_utils import account_logger, get_logger  # noqa: E402
from ctrip.schema import (  # noqa: E402
    DESTINATION_RATING, KYLIN_DAILY, SUPPLIER_RATE_SCORE,
)
from ctrip.sinks import build_sink  # noqa: E402
from ctrip.timeutil import get_timezone  # noqa: E402

logger = get_logger("backfill")

PROGRESS_FILE = "data/backfill_progress.json"


# =============================================================================
# 月份工具
# =============================================================================

def parse_month(text: str) -> tuple[int, int]:
    """解析 ``YYYY-MM``，返回 ``(year, month)``。"""
    text = text.strip()
    try:
        dt = datetime.strptime(text, "%Y-%m")
    except ValueError:
        try:
            dt = datetime.strptime(text, "%Y%m")
        except ValueError as exc:
            raise ValueError(f"月份格式应为 YYYY-MM，收到 {text!r}") from exc
    return dt.year, dt.month


def month_str(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def month_range(start: str, end: str) -> list[str]:
    """生成 ``[start, end]`` 闭区间内的月份列表（升序）。"""
    sy, sm = parse_month(start)
    ey, em = parse_month(end)
    if (sy, sm) > (ey, em):
        raise ValueError(f"起始月份 {start} 晚于结束月份 {end}")
    out: list[str] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        out.append(month_str(y, m))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def month_bounds(month: str) -> tuple[date, date]:
    """返回该月的首日与末日。"""
    y, m = parse_month(month)
    first = date(y, m, 1)
    last = date(y + 1, 1, 1) - timedelta(days=1) if m == 12 \
        else date(y, m + 1, 1) - timedelta(days=1)
    return first, last


# =============================================================================
# 进度管理（断点续跑）
# =============================================================================

@dataclass
class Progress:
    """分片级进度。键为 ``account_id|job|slice_key``。"""

    path: Path
    done: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> Progress:
        if path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                return cls(path=path, done=raw.get("done", {}))
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("进度文件损坏，将重新开始：%s", exc)
        return cls(path=path)

    @staticmethod
    def key(account_id: str, job: str, slice_key: str) -> str:
        return f"{account_id}|{job}|{slice_key}"

    def is_done(self, account_id: str, job: str, slice_key: str) -> bool:
        entry = self.done.get(self.key(account_id, job, slice_key))
        return bool(entry) and entry.get("status") == "success"

    def entry_of(self, account_id: str, job: str,
                 slice_key: str) -> dict[str, Any]:
        """返回历史记录（无记录时为空字典）。"""
        return self.done.get(self.key(account_id, job, slice_key)) or {}

    def empty_streak(self, account_id: str, job: str, slice_key: str) -> int:
        """该分片连续采到空的次数。"""
        return int(self.entry_of(account_id, job, slice_key)
                   .get("empty_streak", 0) or 0)

    def should_skip_empty(self, account_id: str, job: str, slice_key: str,
                          max_empty: int) -> bool:
        """连续多次采到空的分片视为窗口外，不再白跑。

        上游可能迟到补数据，所以单次 empty 不能就当作永久无数据；
        但连续 ``max_empty`` 次都是空，基本可以判定超出携程的历史窗口。
        再每次重跑都重试它们只是白耗请求、抬高风控概率。
        """
        if max_empty <= 0:
            return False
        return self.empty_streak(account_id, job, slice_key) >= max_empty

    def mark(self, account_id: str, job: str, slice_key: str,
             status: str, rows: int, error: str = "") -> None:
        key = self.key(account_id, job, slice_key)
        prev = self.done.get(key) or {}
        streak = int(prev.get("empty_streak", 0) or 0)
        streak = streak + 1 if status == "empty" else 0
        self.done[key] = {
            "status": status,
            "rows": rows,
            "error": error[:300],
            "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "empty_streak": streak,
        }
        self.save()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                   "done": self.done}
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                       encoding="utf-8")
        tmp.replace(self.path)

    def reset(self) -> None:
        self.done.clear()
        if self.path.exists():
            self.path.unlink()


# =============================================================================
# 分片定义
# =============================================================================

@dataclass
class Slice:
    """一个采集切片。"""

    job: str
    key: str                      # 分片标识，用于进度记录
    label: str                    # 人类可读描述
    kwargs: dict[str, Any]        # 传给 collector.collect() 的参数
    partial: bool = False         # 区间被截断（当月只采到昨天）
    skip_reason: str = ""         # 非空则该片无需执行（如整月尚在未来）


def build_slices(job: str, months: list[str], cfg: AppConfig,
                 today: date | None = None) -> list[Slice]:
    """把月份区间展开为采集切片。

    交易卡片按「每月一片」切分。虽然 collector 内部还会按 ``batch_days``
    进一步切批次，但在这一层按月切有两个好处：进度记录的粒度是月，
    失败后只需重跑那一个月；而且每片的 ``run_key`` 天然对应月份，
    落库时不会与其他片互相覆盖。

    当月分片必须把截止日限到昨天：月末日期（如 08-31）晚于今天会被
    ``resolve_date_range`` 直接拒绝，导致整个当月分片失败、本月已结算的
    那几天也跟着采不到。``limited=True`` 会在报告里标注该片属于部分区间，
    提醒使用者下月初补一次。
    """
    slices: list[Slice] = []
    yesterday = (today - timedelta(days=1)) if today else None
    for month in months:
        if job == "kylin":
            first, last = month_bounds(month)
            limited = False
            if yesterday and last > yesterday:
                if first > yesterday:
                    # 整月都在未来（或今天正是 1 号，本月尚无已结算数据）
                    slices.append(Slice(
                        job=job, key=month,
                        label=f"{month}（尚无已结算数据）",
                        kwargs={}, skip_reason=(
                            f"{month} 尚无已结算数据（默认口径采到昨天"
                            f" {yesterday}），无需采集"),
                    ))
                    continue
                last = yesterday
                limited = True
            days = (last - first).days + 1
            tail = "，本月至昨日" if limited else ""
            slices.append(Slice(
                job=job,
                key=month,
                label=f"{month}（{first} ~ {last}，{days} 天{tail}）",
                # 用显式起止日期而不是 days 倒推：后者在跳月时容易算偏一天，
                # 且起止日期写进日志后排查时能直接对应到数据库里的日期区间
                # periods 锁定 1d：7d/30d 是「当前窗口快照」，对历史月份没有意义。
                # 若不锁定，回溯 24 个月就会重复采 24 次同一个当前窗口，
                # 白耗请求且每次都写同一行。
                kwargs={"start_date": first, "end_date": last,
                        "periods": ["1d"]},
                partial=limited,
            ))
        elif job in ("rating", "score"):
            slices.append(Slice(job=job, key=month, label=month,
                                kwargs={"month": month}))
        else:
            raise ValueError(f"未知任务：{job}")
    return slices


# =============================================================================
# 窗口探测
# =============================================================================

def probe_window(cfg: AppConfig, account: AccountConfig, jobs: list[str],
                 earliest: str, latest: str,
                 today: date | None = None) -> dict[str, str | None]:
    """探测上游对各任务实际能返回数据的最早月份。

    采用**从远到近逐月试探**而非二分：上游的可用区间未必连续（中间某个月
    恰好没有业务数据也会返回空），二分法遇到空洞会误判边界。逐月试探虽然
    多花几次请求，但结果可靠，而且探测本身只跑到第一个有数据的月份为止。
    """
    log = account_logger(account.account_id, cfg.log_dir(account.account_id),
                         cfg.run.log_level)
    result: dict[str, str | None] = {}

    login = ensure_logged_in(cfg, account, log)
    if not login.success:
        logger.error("[%s] 登录失败，无法探测：%s", account.account_id, login.message)
        return {job: None for job in jobs}

    api = build_api(cfg, account.account_id, log)
    all_months = month_range(earliest, latest)

    for job in jobs:
        collector = get_collector(job)(cfg, api, account.account_id, log,
                                       account.scenic_id)
        found: str | None = None
        logger.info("[%s] 探测 %s 的历史窗口（%s ~ %s，从最早月份开始试）",
                    account.account_id, job, earliest, latest)

        for month in all_months:
            slices = build_slices(job, [month], cfg, today)
            if slices[0].skip_reason:
                logger.info("[%s] %s：%s 尚无已结算数据，跳过探测",
                            account.account_id, job, month)
                continue
            try:
                res = collector.collect(**slices[0].kwargs)
                if res.count > 0:
                    found = month
                    logger.info("[%s] %s：%s 有数据（%d 行）→ 最早可用月份",
                                account.account_id, job, month, res.count)
                    break
                logger.info("[%s] %s：%s 无数据，继续往后试",
                            account.account_id, job, month)
            except SessionExpired as exc:
                logger.error("[%s] 登录态失效，探测中断：%s", account.account_id, exc)
                break
            except (ApiError, Exception) as exc:  # noqa: BLE001
                logger.warning("[%s] %s：%s 探测异常（视为不可用）：%s",
                               account.account_id, job, month, exc)
            time.sleep(max(0.5, cfg.collect.request_interval))

        result[job] = found
        if found is None:
            logger.warning("[%s] %s：在 %s ~ %s 区间内未找到任何数据",
                           account.account_id, job, earliest, latest)
    return result


# =============================================================================
# 回溯执行
# =============================================================================

@dataclass
class SliceReport:
    account_id: str
    job: str
    slice_key: str
    # success | empty | failed | blocked（登录失败）
    # | skipped（历史已成功）| skipped_empty（连续多次空，判定窗口外）
    # | not_ready（区间尚在未来，无需采集）
    status: str
    rows_fetched: int = 0
    rows_written: int = 0
    error: str = ""
    history_rows: int = 0   # 历史已采行数（skipped 时从进度文件读回）
    history_at: str = ""    # 历史成功时间
    partial: bool = False   # 当月分片，区间被截断到昨天


def run_backfill(cfg: AppConfig, accounts: list[AccountConfig], jobs: list[str],
                 months: list[str], sink_mode: str, progress: Progress,
                 *, interval: float, jitter: float, force: bool,
                 stop_after_failures: int,
                 write_mode: str = "overwrite",
                 today: date | None = None,
                 max_empty_retry: int = 3) -> list[SliceReport]:
    reports: list[SliceReport] = []

    for account in accounts:
        account_id = account.account_id
        log = account_logger(account_id, cfg.log_dir(account_id), cfg.run.log_level)

        # 整个账号只登录一次：回溯动辄几十个分片，若每分片都试登录，
        # 一旦凭据有问题就会反复拉起浏览器，既浪费时间又容易触发风控。
        login = ensure_logged_in(cfg, account, log)
        if not login.success:
            logger.error("[%s] 登录失败，该账号全部分片均不执行：%s",
                         account_id, login.message)
            # 登录失败不往进度文件写 failed：这不是分片自身的问题，
            # 否则修好凭据后 --resume 会误以为这些分片已试过而给出冗余提示。
            for job in jobs:
                for sl in build_slices(job, months, cfg, today):
                    reports.append(SliceReport(account_id, job, sl.key, "blocked",
                                               error=f"登录失败：{login.message}"))
            continue

        api = build_api(cfg, account_id, log)
        sink = build_sink(cfg, sink_mode, log)
        consecutive_failures = 0

        try:
            for job in jobs:
                collector = get_collector(job)(cfg, api, account_id, log,
                                               account.scenic_id)
                slices = build_slices(job, months, cfg, today)
                logger.info("[%s] 任务 %s：共 %d 个分片待处理",
                            account_id, job, len(slices))

                for index, sl in enumerate(slices, start=1):
                    if sl.skip_reason:
                        logger.info("[%s] %s %s 无需采集：%s",
                                    account_id, job, sl.key, sl.skip_reason)
                        reports.append(SliceReport(account_id, job, sl.key,
                                                   "not_ready",
                                                   error=sl.skip_reason))
                        continue

                    if not force and progress.is_done(account_id, job, sl.key):
                        hist = progress.entry_of(account_id, job, sl.key)
                        rows = int(hist.get("rows", 0) or 0)
                        logger.info("[%s] %s %s 历史已采 %d 行（%s），跳过；"
                                    "如需重采加 --force",
                                    account_id, job, sl.key, rows,
                                    hist.get("at", "时间未记录"))
                        reports.append(SliceReport(
                            account_id, job, sl.key, "skipped",
                            history_rows=rows, history_at=hist.get("at", "")))
                        continue

                    if not force and progress.should_skip_empty(
                            account_id, job, sl.key, max_empty_retry):
                        streak = progress.empty_streak(account_id, job, sl.key)
                        logger.info("[%s] %s %s 连续 %d 次采到空，判定超出历史窗口，"
                                    "不再重试（--force 或 --max-empty-retry 0 可强制）",
                                    account_id, job, sl.key, streak)
                        reports.append(SliceReport(
                            account_id, job, sl.key, "skipped_empty",
                            error=f"连续 {streak} 次无数据，视为超出上游历史窗口"))
                        continue

                    logger.info("[%s] %s 分片 %d/%d：%s",
                                account_id, job, index, len(slices), sl.label)
                    try:
                        res = collector.collect(**sl.kwargs)
                        written = 0
                        if res.rows:
                            wr = sink.write(res.table, account_id, res.rows,
                                            run_key=res.run_key,
                                            write_mode=write_mode)
                            written = wr.rows_written
                        status = "success" if res.count else "empty"
                        progress.mark(account_id, job, sl.key, status,
                                      res.count,
                                      "; ".join(res.warnings[:2]))
                        reports.append(SliceReport(account_id, job, sl.key, status,
                                                   res.count, written,
                                                   "; ".join(res.warnings[:2]),
                                                   partial=sl.partial))
                        consecutive_failures = 0
                        logger.info("[%s] %s %s → 采集 %d 行，写入 %d 行",
                                    account_id, job, sl.key, res.count, written)
                    except SessionExpired as exc:
                        msg = f"登录态失效：{exc}"
                        logger.error("[%s] %s %s %s，中止该账号剩余分片",
                                     account_id, job, sl.key, msg)
                        progress.mark(account_id, job, sl.key, "failed", 0, msg)
                        reports.append(SliceReport(account_id, job, sl.key,
                                                   "failed", error=msg))
                        raise
                    except Exception as exc:  # noqa: BLE001
                        msg = str(exc)
                        logger.error("[%s] %s %s 失败：%s",
                                     account_id, job, sl.key, msg)
                        progress.mark(account_id, job, sl.key, "failed", 0, msg)
                        reports.append(SliceReport(account_id, job, sl.key,
                                                   "failed", error=msg))
                        consecutive_failures += 1
                        if 0 < stop_after_failures <= consecutive_failures:
                            logger.error(
                                "[%s] 连续失败 %d 次，中止该账号（可能已触发风控或"
                                "登录态异常）。修复后加 --resume 继续",
                                account_id, consecutive_failures)
                            raise RuntimeError("连续失败过多，主动中止") from exc

                    # 分片之间限流：固定间隔 + 随机抖动，降低风控概率
                    if index < len(slices):
                        delay = interval + random.uniform(0, jitter)
                        logger.debug("限流等待 %.1f 秒", delay)
                        time.sleep(delay)
        except (SessionExpired, RuntimeError):
            logger.warning("[%s] 该账号提前结束，其余账号继续", account_id)
        finally:
            sink.close()

    return reports


# =============================================================================
# 报告
# =============================================================================

_STATUS_CN = {
    "success": "成功",
    "empty": "无数据",
    "failed": "失败",
    "blocked": "未执行",
    "skipped": "已采过",
    "skipped_empty": "窗口外",
    "not_ready": "无需采",
}


# 任务 -> 目标表。表名仍从 schema 取，改表名时这里自动跟随，
# 不需要在报告里维护一份字符串副本。
_JOB_TABLE = {
    "kylin": KYLIN_DAILY,
    "rating": DESTINATION_RATING,
    "score": SUPPLIER_RATE_SCORE,
}


def _table_of(job: str) -> str:
    """任务对应的目标表名。"""
    table = _JOB_TABLE.get(job)
    return table.name if table else job


def _render_by_table(reports: list[SliceReport]) -> list[str]:
    """按目标表拆分行数。

    三张表的数据粒度相差很大：交易表是「日 × 签约组合 × 日期类型」，
    一个月就有上百行；目的地分析表是账号级分析文本，每账号每月只有一行；
    供应商评级是「目的地 × 月」，每月几行。只看一个总行数，很容易误以为
    后两张表没采到，所以这里把每张表分开列出。
    """
    by_table: dict[str, dict[str, int]] = {}
    for r in reports:
        if r.status == "not_ready":
            continue
        slot = by_table.setdefault(
            _table_of(r.job),
            {"fetched": 0, "history": 0, "slices": 0},
        )
        slot["fetched"] += r.rows_fetched
        slot["history"] += r.history_rows
        slot["slices"] += 1
    if not by_table:
        return []
    lines = ["", "按目标表（三张表粒度不同，行数相差大是正常的）："]
    for table, slot in sorted(by_table.items()):
        total = slot["fetched"] + slot["history"]
        tail = ""
        if total == 0:
            tail = "  ← 无数据，看上方分片状态判断是空数据还是失败"
        lines.append(f"  {table:<42}{total:>7} 行"
                     f"（本次 {slot['fetched']} + 历史 {slot['history']}，"
                     f"{slot['slices']} 分片）{tail}")
    return lines


def render_report(reports: list[SliceReport], months: list[str]) -> str:
    lines: list[str] = []
    lines.append("=" * 92)
    lines.append(f"回溯采集汇总（区间 {months[0]} ~ {months[-1]}，共 {len(months)} 个月）")
    lines.append("=" * 92)
    lines.append(f"{'账号':<8}{'任务':<8}{'分片':<10}{'状态':<12}"
                 f"{'本次采集':>10}{'写入':>8}{'历史已采':>10}  说明")
    lines.append("-" * 92)

    total_fetched = total_written = total_history = 0
    counts: dict[str, int] = {}
    for r in reports:
        total_fetched += r.rows_fetched
        total_written += r.rows_written
        total_history += r.history_rows
        counts[r.status] = counts.get(r.status, 0) + 1
        note = r.error[:30] if r.error else ""
        if r.partial and not note:
            note = "当月只采到昨日，下月初补一次"
        elif r.status == "skipped" and r.history_at:
            note = f"上次成功 {r.history_at[:10]}"
        label = f"{r.status}({_STATUS_CN.get(r.status, '')})" \
            if r.status in _STATUS_CN else r.status
        hist = str(r.history_rows) if r.history_rows else "-"
        lines.append(f"{r.account_id:<8}{r.job:<8}{r.slice_key:<10}{label:<12}"
                     f"{r.rows_fetched:>10}{r.rows_written:>8}{hist:>10}  {note}")

    lines.append("-" * 92)
    summary = "  ".join(
        f"{k}({_STATUS_CN.get(k, '')})={v}" for k, v in sorted(counts.items()))
    lines.append(f"分片状态：{summary}")
    lines.append(f"本次新采：{total_fetched} 行，写入 {total_written} 行")
    if total_history:
        # 分开报「本次新采」与「历史已采」：断点续跑下大部分分片已在之前
        # 几轮采完，若只报本次行数，使用者会误以为什么都没采到。
        lines.append(f"历史已采：{total_history} 行（之前已成功入库，本次跳过未重复请求）")
        lines.append(f"区间累计：{total_fetched + total_history} 行")

    lines.extend(_render_by_table(reports))

    blocked = [r for r in reports if r.status == "blocked"]
    if blocked:
        blocked_accounts = sorted({r.account_id for r in blocked})
        lines.append("")
        lines.append(f"因登录失败而未执行的账号：{', '.join(blocked_accounts)}")
        lines.append(f"  原因：{blocked[0].error[:70]}")
        lines.append("  处理：先跑 python -m ctrip.cli login --no-headless 完成一次人工登录，"
                     "再重新执行本命令")

    failed = [r for r in reports if r.status == "failed"]
    if failed:
        lines.append("")
        lines.append(f"失败分片 {len(failed)} 个（修复后重跑相同命令只补这些）：")
        for r in failed[:20]:
            lines.append(f"  [{r.account_id}/{r.job}/{r.slice_key}] {r.error[:60]}")
        if len(failed) > 20:
            lines.append(f"  ... 另有 {len(failed) - 20} 个，详见进度文件")

    empty = [r for r in reports if r.status == "empty"]
    if empty:
        lines.append("")
        lines.append(f"空数据分片 {len(empty)} 个（上游本次无数据，下次仍会重试）：")
        keys = sorted({f"{r.job}/{r.slice_key}" for r in empty})
        lines.append("  " + ", ".join(keys[:30]))

    skipped_empty = [r for r in reports if r.status == "skipped_empty"]
    if skipped_empty:
        lines.append("")
        lines.append(f"已判定超出历史窗口的分片 {len(skipped_empty)} 个（连续多次无数据，"
                     f"不再重试以节省请求）：")
        keys = sorted({f"{r.job}/{r.slice_key}" for r in skipped_empty})
        lines.append("  " + ", ".join(keys[:30]))
        lines.append("  如确认上游已补数据，用 --force 或 --max-empty-retry 0 强制重试")

    not_ready = [r for r in reports if r.status == "not_ready"]
    if not_ready:
        lines.append("")
        keys = sorted({f"{r.job}/{r.slice_key}" for r in not_ready})
        lines.append(f"尚无已结算数据的分片 {len(not_ready)} 个：" + ", ".join(keys[:20]))

    partial = [r for r in reports if r.partial and r.status in ("success", "empty")]
    if partial:
        lines.append("")
        keys = sorted({f"{r.job}/{r.slice_key}" for r in partial})
        lines.append(f"部分区间分片 {len(partial)} 个（当月只采到昨日）："
                     + ", ".join(keys[:20]))
        lines.append("  下月初加 --force 重跑这个月份即可补齐剩余天数")

    skipped = [r for r in reports if r.status == "skipped"]
    if skipped and not any(r.status in ("success", "failed") for r in reports):
        # 全部分片都是历史已采时特别提示，避免误认为采集失败
        lines.append("")
        lines.append(f"本次无新增采集：{len(skipped)} 个分片之前已成功入库（共 "
                     f"{sum(r.history_rows for r in skipped)} 行），属正常断点续跑行为。")
        lines.append("  确认库里数据：SELECT MIN(transaction_date), MAX(transaction_date), "
                     "COUNT(*) FROM ads_sup_ota_ctrip_trade_di WHERE account_id='xxx';")
        lines.append("  如需强制重采：同样命令加 --force")

    lines.append("=" * 92)
    return "\n".join(lines)


# =============================================================================
# 入口
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="历史数据批量回溯采集（按月分片，支持窗口探测与断点续跑）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("-c", "--config", help="配置文件路径")
    parser.add_argument("--start", help="起始月份 YYYY-MM，如 2025-01")
    parser.add_argument("--end", help="结束月份 YYYY-MM（默认当前月）")
    parser.add_argument("--job", action="append", dest="jobs",
                        choices=["kylin", "rating", "score", "all"],
                        help="任务，可重复；默认 all")
    parser.add_argument("--sink", choices=["csv", "mysql", "both"],
                        help="落地方式（默认取配置）")
    parser.add_argument("--write-mode", choices=["overwrite", "upsert"],
                        help="overwrite（默认）同日期/月份旧数据先清理再写入；"
                             "upsert 仅按唯一键更新")
    parser.add_argument("--account-id", action="append", dest="account_ids",
                        help="只处理指定账号，可重复")
    parser.add_argument("--probe", action="store_true",
                        help="只探测上游历史数据窗口，不落库")
    parser.add_argument("--probe-from", default="2024-01",
                        help="探测起始月份（默认 2024-01）")
    parser.add_argument("--resume", action="store_true",
                        help="断点续跑，跳过已成功的分片（默认行为）")
    parser.add_argument("--force", action="store_true",
                        help="忽略进度记录，强制重跑所有分片")
    parser.add_argument("--reset-progress", action="store_true",
                        help="清空进度文件后重新开始")
    parser.add_argument("--interval", type=float, default=3.0,
                        help="分片之间的间隔秒数（默认 3.0，降低风控概率）")
    parser.add_argument("--jitter", type=float, default=2.0,
                        help="间隔的随机抖动上限秒数（默认 2.0）")
    parser.add_argument("--stop-after-failures", type=int, default=5,
                        help="同一账号连续失败多少次后中止（默认 5，0 表示不限）")
    parser.add_argument("--max-empty-retry", type=int, default=3,
                        help="分片连续采到空多少次后不再重试（默认 3，"
                             "避免每次重跑都白跑超出历史窗口的月份；0 表示总是重试）")
    parser.add_argument("--dry-run", action="store_true",
                        help="只打印将要执行的分片计划，不实际采集")
    parser.add_argument("--log-level", help="日志级别")
    args = parser.parse_args()

    cfg = load_config(args.config)
    if args.log_level:
        cfg.run.log_level = args.log_level.upper()
    tz = get_timezone(cfg.run.timezone)
    today = datetime.now(tz).date()

    # 账号（resolve_accounts 已包含 ID 筛选、启用状态过滤与凭据校验）
    try:
        accounts = resolve_accounts(cfg, args.account_ids, log=logger)
    except Exception as exc:  # noqa: BLE001
        logger.error("获取账号失败：%s", exc)
        return 2
    if not accounts:
        logger.error("没有可用账号（检查 status 是否为 active、凭据是否配置）")
        return 2

    # 任务
    jobs = args.jobs or ["all"]
    if "all" in jobs:
        jobs = ["kylin", "rating", "score"]
    jobs = list(dict.fromkeys(jobs))

    # 探测模式
    if args.probe:
        latest = month_str(today.year, today.month)
        print("=" * 86)
        print(f"上游历史数据窗口探测（试探区间 {args.probe_from} ~ {latest}）")
        print("=" * 86)
        print("说明：从最早月份开始逐月试探，找到第一个有数据的月份即为窗口边界。")
        print()
        earliest_found: list[str] = []
        for account in accounts:
            found = probe_window(cfg, account, jobs, args.probe_from, latest, today)
            print(f"账号 {account.account_id}（景区 {account.scenic_id or '未绑定'}）：")
            for job, month in found.items():
                if month:
                    print(f"  {job:<8} 最早可用月份：{month}")
                    earliest_found.append(month)
                else:
                    print(f"  {job:<8} 在试探区间内未找到数据"
                          f"（可能上游不保留这么久，或该账号无此业务）")
            print()
        print("=" * 86)
        if earliest_found:
            suggest = min(earliest_found)
            print("下一步：用探测到的最早月份执行回溯")
            print(f"  python tools/backfill.py --start {suggest} --sink both")
            if args.probe_from < suggest:
                print()
                print(f"注意：{args.probe_from} ~ {suggest} 之间上游无数据，"
                      f"从更早月份开始只会白跑。")
        else:
            print("未探测到任何可用数据。请确认登录态正常（python -m ctrip.cli check），"
                  "并确认该账号在携程后台确实有业务数据。")
        print("=" * 86)
        return 0

    # 回溯区间
    if not args.start:
        logger.error("请用 --start 指定起始月份（如 --start 2025-01），"
                     "或先用 --probe 探测上游可用窗口")
        return 2
    end = args.end or month_str(today.year, today.month)
    try:
        months = month_range(args.start, end)
    except ValueError as exc:
        logger.error("月份区间无效：%s", exc)
        return 2

    sink_mode = (args.sink or cfg.run.sink or "csv").lower()
    progress_path = cfg.path(PROGRESS_FILE)
    progress = Progress.load(progress_path)
    if args.reset_progress:
        progress.reset()
        logger.info("进度文件已清空")

    # 计划预览
    all_slices = {j: build_slices(j, months, cfg, today) for j in jobs}
    total_slices = sum(len(v) for v in all_slices.values()) * len(accounts)
    done_slices = 0
    empty_skips = 0
    not_ready = 0
    history_rows = 0
    for a in accounts:
        for j, sls in all_slices.items():
            for sl in sls:
                if sl.skip_reason:
                    not_ready += 1
                elif progress.is_done(a.account_id, j, sl.key):
                    done_slices += 1
                    history_rows += int(
                        progress.entry_of(a.account_id, j, sl.key)
                        .get("rows", 0) or 0)
                elif progress.should_skip_empty(a.account_id, j, sl.key,
                                               args.max_empty_retry):
                    empty_skips += 1
    pending = total_slices if args.force else \
        total_slices - done_slices - empty_skips - not_ready

    print("=" * 92)
    print("回溯计划")
    print("=" * 92)
    print(f"账号：{', '.join(a.account_id for a in accounts)}")
    print(f"任务：{', '.join(jobs)}")
    # 把目标表写清楚：不传 --job 时默认就是三张表全采，
    # 但仅看「kylin, rating, score」看不出到底写哪几张表。
    print("目标表：" + "、".join(_table_of(j) for j in jobs))
    print(f"区间：{months[0]} ~ {months[-1]}（{len(months)} 个月，数据口径采到 "
          f"{today - timedelta(days=1)}）")
    detail = [f"待采 {pending}"]
    if not args.force:
        if done_slices:
            detail.append(f"历史已采 {done_slices}（约 {history_rows} 行，跳过）")
        if empty_skips:
            detail.append(f"已判定窗口外 {empty_skips}（跳过）")
    if not_ready:
        detail.append(f"尚无数据 {not_ready}")
    print(f"分片：共 {total_slices} 个（" + "，".join(detail) + "）")
    if done_slices and not args.force:
        print("      历史已采的分片不会重复请求，如需强制重采请加 --force")
    print(f"落地：{sink_mode}")
    print(f"限流：分片间隔 {args.interval}s + 抖动 0~{args.jitter}s")
    print(f"进度文件：{progress_path}")
    est_min = max(0, pending) * (args.interval + args.jitter / 2) / 60
    print(f"预计仅限流耗时约 {est_min:.0f} 分钟（不含接口响应时间）")
    print("=" * 92)

    if args.dry_run:
        for job in jobs:
            print(f"\n任务 {job} 的分片 → {_table_of(job)}：")
            for sl in all_slices[job]:
                mark = "  [无需采集]" if sl.skip_reason else \
                    ("  [部分区间]" if sl.partial else "")
                print(f"  {sl.key}  {sl.label}{mark}")
        return 0

    started = time.time()
    reports = run_backfill(
        cfg, accounts, jobs, months, sink_mode, progress,
        interval=args.interval, jitter=args.jitter, force=args.force,
        stop_after_failures=args.stop_after_failures,
        write_mode=(args.write_mode or cfg.run.write_mode).lower(),
        today=today, max_empty_retry=args.max_empty_retry,
    )
    elapsed = time.time() - started

    print()
    print(render_report(reports, months))
    print(f"耗时 {elapsed / 60:.1f} 分钟")

    if any(r.status in ("failed", "blocked") for r in reports):
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n已中断。进度已保存，重新执行相同命令即可继续（默认跳过已完成分片）。")
        sys.exit(130)
