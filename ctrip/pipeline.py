# -*- coding: utf-8 -*-
"""多账号采集调度编排。

隔离与容错原则
--------------
1. 一个账号一套上下文：logger、Session、CSV 目录、登录态目录彼此独立。
2. 单账号失败（登录失败 / 接口失败 / 落库失败）只影响该账号，
   其余账号继续执行，最终汇总为一张报告。
3. 采集前做账号指纹校验，一旦发现登录态与历史身份不符（可能串号），
   立即拒绝该账号本次采集，宁可少采也不写错数据。
4. 每个「账号 × 任务」都会产生一条审计记录（可写入 ``ads_service_ota_ctrip_run_log``）。
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .accounts import to_dim_rows
from .api import SessionExpired
from .auth import build_api, check_fingerprint, ensure_logged_in
from .collectors import ALL_JOBS, get_collector
from .config import AccountConfig, AppConfig
from .logging_utils import account_logger, get_logger
from .sinks import MySQLSink, build_sink
from .sinks.base import MultiSink, Sink
from .timeutil import get_timezone

logger = get_logger("pipeline")


@dataclass
class JobReport:
    account_id: str
    job: str
    scenic_id: str = ""              # 该账号绑定的景区ID（如 PFTSCA01018088）
    status: str = "pending"          # success | partial | failed | skipped
    run_key: str = ""
    rows_fetched: int = 0
    rows_written: int = 0
    rows_skipped: int = 0
    rows_deleted: int = 0            # 覆盖模式下清理的旧数据行数
    partitions: int = 0              # 本次覆盖涉及的日期/月份个数
    # 仅用于调度层判定是否需要自动重登，不写入审计表
    session_expired: bool = False
    target: str = ""
    error: str = ""
    warnings: list[str] = field(default_factory=list)
    started_at: str = ""
    finished_at: str = ""
    duration_ms: int = 0

    def to_log_row(self, sink_name: str) -> dict[str, Any]:
        error_text = self.error or ("; ".join(self.warnings[:3]) if self.warnings else "")
        return {
            "account_id": self.account_id,
            "scenic_id": self.scenic_id or None,
            "job": self.job,
            # 不能用 "-" 占位：cast_str 会把 "-" 归一为 NULL，
            # 导致唯一键为空而被校验拦下，登录失败/串号等跳过类审计记录会丢失。
            "run_key": self.run_key or "NA",
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "rows_fetched": self.rows_fetched,
            "rows_written": self.rows_written,
            "sink": sink_name,
            "error": error_text[:1000],
        }


@dataclass
class AccountReport:
    account_id: str
    login_ok: bool = False
    login_message: str = ""
    jobs: list[JobReport] = field(default_factory=list)

    @property
    def status(self) -> str:
        if not self.login_ok:
            return "failed"
        statuses = {j.status for j in self.jobs}
        if statuses == {"success"}:
            return "success"
        if "success" in statuses:
            return "partial"
        return "failed"

    @property
    def rows_written(self) -> int:
        return sum(j.rows_written for j in self.jobs)


class Pipeline:
    """采集调度器。"""

    def __init__(self, cfg: AppConfig, sink_mode: str | None = None,
                 write_mode: str | None = None) -> None:
        self.cfg = cfg
        self.sink_mode = (sink_mode or cfg.run.sink or "csv").lower()
        self.write_mode = (write_mode or cfg.run.write_mode or "overwrite").lower()
        # 用带降级的解析器：Windows 缺少 tzdata 时不应让整批采集失败
        self.tz = get_timezone(cfg.run.timezone, log=logger)

    # -- 对外入口 ----------------------------------------------------------
    def run(self, accounts: list[AccountConfig], jobs: list[str] | None = None,
            *, force_relogin: bool = False, headless: bool | None = None,
            skip_login: bool = False, keep_alive: bool = False,
            **job_kwargs: Any) -> list[AccountReport]:
        jobs = jobs or list(ALL_JOBS)
        for job in jobs:
            get_collector(job)  # 提前校验任务名

        mode_zh = "覆盖（同日期/月份先清理再写入）" if self.write_mode == "overwrite" \
            else "增量（仅按唯一键更新）"
        logger.info("开始采集：%d 个账号 × 任务 %s，落地方式=%s，写入模式=%s，并发=%d",
                    len(accounts), ",".join(jobs), self.sink_mode, mode_zh,
                    self.cfg.run.workers)

        self._seed_account_dim(accounts)

        workers = max(1, min(self.cfg.run.workers, len(accounts)))
        reports: list[AccountReport] = []
        if workers == 1:
            for account in accounts:
                reports.append(self._run_account(account, jobs, force_relogin=force_relogin,
                                                 headless=headless, skip_login=skip_login,
                                                 keep_alive=keep_alive, **job_kwargs))
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(self._run_account, account, jobs,
                                force_relogin=force_relogin, headless=headless,
                                skip_login=skip_login, keep_alive=keep_alive,
                                **job_kwargs): account
                    for account in accounts
                }
                for future in as_completed(futures):
                    account = futures[future]
                    try:
                        reports.append(future.result())
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("[%s] 账号处理出现未捕获异常：%s",
                                         account.account_id, exc)
                        report = AccountReport(account.account_id, login_ok=False,
                                              login_message=f"未捕获异常：{exc}")
                        reports.append(report)
            order = {a.account_id: i for i, a in enumerate(accounts)}
            reports.sort(key=lambda r: order.get(r.account_id, 999))
        return reports

    # -- 单账号执行 --------------------------------------------------------
    def _run_account(self, account: AccountConfig, jobs: list[str], *,
                     force_relogin: bool, headless: bool | None,
                     skip_login: bool, keep_alive: bool = False,
                     **job_kwargs: Any) -> AccountReport:
        account_id = account.account_id
        log = account_logger(account_id, self.cfg.log_dir(account_id), self.cfg.run.log_level)
        report = AccountReport(account_id)

        # 1) 登录态
        if skip_login:
            report.login_ok, report.login_message = True, "按要求跳过登录检查"
        else:
            login_result = ensure_logged_in(self.cfg, account, log,
                                            force_relogin=force_relogin, headless=headless,
                                            keep_alive=keep_alive)
            report.login_ok = login_result.success
            report.login_message = login_result.message
        if not report.login_ok:
            logger.error("[%s] 登录失败，跳过该账号：%s", account_id, report.login_message)
            report.jobs = [JobReport(account_id, job, scenic_id=account.scenic_id,
                                     status="skipped",
                                     error=report.login_message) for job in jobs]
            self._flush_run_log(report)
            return report

        # 2) 构建该账号独占的 API 客户端
        try:
            api = build_api(self.cfg, account_id, log)
        except Exception as exc:  # noqa: BLE001
            report.login_ok = False
            report.login_message = f"构建会话失败：{exc}"
            report.jobs = [JobReport(account_id, job, scenic_id=account.scenic_id,
                                     status="skipped",
                                     error=report.login_message) for job in jobs]
            logger.error("[%s] %s", account_id, report.login_message)
            self._flush_run_log(report)
            return report

        # 3) 账号指纹校验：防止 Cookie 串号造成跨账号数据污染
        fingerprint_ok, fingerprint_msg = check_fingerprint(self.cfg, account_id, api, log)
        if not fingerprint_ok:
            logger.error("[%s] %s", account_id, fingerprint_msg)
            report.login_ok = False
            report.login_message = fingerprint_msg
            report.jobs = [JobReport(account_id, job, scenic_id=account.scenic_id,
                                     status="skipped",
                                     error=fingerprint_msg) for job in jobs]
            self._flush_run_log(report)
            return report
        log.info("账号指纹校验：%s", fingerprint_msg)

        # 4) 逐任务采集 + 落地（该账号独占 Sink 句柄，MySQL 连接线程独立）
        if not account.scenic_id:
            log.warning("[%s] 未绑定 scenic_id（景区ID），本次采集的 scenic_id 将为空；"
                        "请在 config/config.yaml 的对应账号下配置 scenic_id", account_id)
        else:
            log.info("[%s] 绑定景区 scenic_id=%s", account_id, account.scenic_id)

        sink = build_sink(self.cfg, self.sink_mode, log)
        try:
            for job in jobs:
                jr = self._run_job(account_id, job, api, sink, log,
                                   scenic_id=account.scenic_id, **job_kwargs)
                # 采集中途登录态失效（长区间回溯很常见）：自动重登并重试本任务，
                # 否则开头那次登录检查早已通过，剩下任务会连锁全部失败。
                if jr.status == "failed" and jr.session_expired and not skip_login:
                    new_api, relogin_note = self._recover_session(account, log, headless,
                                                                  keep_alive=keep_alive)
                    jr.warnings.append(relogin_note)
                    if new_api is not None:
                        api = new_api
                        retry = self._run_job(account_id, job, api, sink, log,
                                              scenic_id=account.scenic_id, **job_kwargs)
                        retry.warnings.insert(0, f"中途登录态失效，已自动重登并重试：{relogin_note}")
                        jr = retry
                report.jobs.append(jr)
        finally:
            sink.close()

        self._flush_run_log(report)
        return report

    def _recover_session(self, account: AccountConfig, log: logging.Logger,
                         headless: bool | None, *, keep_alive: bool = False):
        """中途登录态失效后的恢复：强制重登 + 重建会话 + 重验指纹。

        只尝试一次：若账号已被风控，反复登录只会加重封锁。
        重登后必须重新校验指纹：万一期间账号绑定被改过，
        拿到别人的登录态会造成跳账号数据污染。
        """
        account_id = account.account_id
        log.warning("[%s] 检测到采集过程中登录态失效，开始自动重登", account_id)
        result = ensure_logged_in(self.cfg, account, log,
                                  force_relogin=True, headless=headless,
                                  keep_alive=keep_alive)
        if not result.success:
            return None, f"自动重登失败：{result.message}"
        try:
            api = build_api(self.cfg, account_id, log)
        except Exception as exc:  # noqa: BLE001
            return None, f"重登成功但重建会话失败：{exc}"
        ok, msg = check_fingerprint(self.cfg, account_id, api, log)
        if not ok:
            return None, f"重登后指纹校验未通过，为避免数据污染已停止：{msg}"
        return api, f"已自动重登并通过指纹校验（{msg}）"

    def _run_job(self, account_id: str, job: str, api, sink: Sink,
                 log: logging.Logger, scenic_id: str = "", **job_kwargs: Any) -> JobReport:
        started = time.monotonic()
        jr = JobReport(account_id=account_id, job=job, scenic_id=scenic_id,
                       started_at=datetime.now(self.tz).strftime("%Y-%m-%d %H:%M:%S"))
        collector_cls = get_collector(job)
        collector = collector_cls(self.cfg, api, account_id, log, scenic_id)

        try:
            kwargs = _filter_kwargs(job, job_kwargs)
            result = collector.collect(**kwargs)
            jr.run_key = result.run_key
            jr.rows_fetched = result.count
            jr.warnings = list(result.warnings)

            if result.rows:
                write = sink.write(result.table, account_id, result.rows,
                                   run_key=result.run_key, write_mode=self.write_mode)
                jr.rows_written = write.rows_written
                jr.rows_skipped = write.rows_skipped
                jr.rows_deleted = write.rows_deleted
                jr.partitions = write.partitions
                jr.target = write.target
                jr.warnings.extend(write.problems)
                jr.status = "success" if not result.warnings else "partial"
            else:
                jr.status = "partial" if result.warnings else "success"
                log.warning("[%s] 任务 %s 未采集到数据", account_id, job)
        except SessionExpired as exc:
            jr.status = "failed"
            jr.session_expired = True
            jr.error = f"登录态失效：{exc}"
            log.error("[%s] 任务 %s 失败：%s", account_id, job, jr.error)
        except Exception as exc:  # noqa: BLE001
            jr.status = "failed"
            jr.error = str(exc)
            log.exception("[%s] 任务 %s 异常：%s", account_id, job, exc)

        jr.finished_at = datetime.now(self.tz).strftime("%Y-%m-%d %H:%M:%S")
        jr.duration_ms = int((time.monotonic() - started) * 1000)
        return jr

    # -- 辅助：账号维表与审计日志 -------------------------------------------
    def _seed_account_dim(self, accounts: list[AccountConfig]) -> None:
        if self.sink_mode not in ("mysql", "both") or not self.cfg.mysql.enabled:
            return
        try:
            with MySQLSink(self.cfg, logger) as sink:
                count = sink.upsert_accounts(to_dim_rows(accounts))
            logger.info("账号维表已同步 %d 条（不含密码）", count)
        except Exception as exc:  # noqa: BLE001
            logger.warning("同步账号维表失败（不影响采集）：%s", exc)

    def _flush_run_log(self, report: AccountReport) -> None:
        if self.sink_mode not in ("mysql", "both") or not self.cfg.mysql.enabled:
            return
        if not self.cfg.mysql.write_run_log:
            return
        rows = [jr.to_log_row(self.sink_mode) for jr in report.jobs]
        if not rows:
            return
        try:
            with MySQLSink(self.cfg, logger) as sink:
                sink.write_run_log(rows)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] 写入采集审计日志失败：%s", report.account_id, exc)


# 逐任务的参数白名单。新增采集参数必须在此登记，
# 否则会被 _filter_kwargs 静默丢弃（参数似乎生效但实际无效）。
_JOB_KWARGS = {
    "kylin": ("days", "end_date", "start_date", "batch_days", "periods"),
    "rating": ("month", "months", "start_month", "end_month", "per_region"),
    "score": ("month", "months", "start_month", "end_month"),
}


def _filter_kwargs(job: str, kwargs: dict[str, Any]) -> dict[str, Any]:
    allowed = _JOB_KWARGS.get(job, ())
    return {k: v for k, v in kwargs.items() if k in allowed and v is not None}


# =============================================================================
# 报告渲染
# =============================================================================

def render_reports(reports: list[AccountReport], sink_mode: str) -> str:
    lines: list[str] = []
    lines.append("=" * 78)
    lines.append(f"采集汇总（落地方式：{sink_mode}）")
    lines.append("=" * 78)
    header = (f"{'账号':<8}{'景区ID':<18}{'任务':<8}{'状态':<9}"
              f"{'采集':>6}{'写入':>6}{'清理':>6}{'跳过':>6}  目标")
    lines.append(header)
    lines.append("-" * 78)
    total_fetched = total_written = total_deleted = 0
    for report in reports:
        if not report.jobs:
            lines.append(f"{report.account_id:<8}{'-':<18}{'-':<8}{report.status:<9}"
                         f"{0:>6}{0:>6}{0:>6}{0:>6}  {report.login_message}")
        for jr in report.jobs:
            total_fetched += jr.rows_fetched
            total_written += jr.rows_written
            total_deleted += jr.rows_deleted
            target = jr.target or jr.error or ""
            lines.append(f"{jr.account_id:<8}{(jr.scenic_id or '-'):<18}{jr.job:<8}"
                         f"{jr.status:<9}"
                         f"{jr.rows_fetched:>6}{jr.rows_written:>6}"
                         f"{jr.rows_deleted:>6}{jr.rows_skipped:>6}  "
                         f"{target[:40]}")
    lines.append("-" * 78)
    lines.append(f"合计：采集 {total_fetched} 行，写入 {total_written} 行，"
                 f"清理旧数据 {total_deleted} 行，账号 {len(reports)} 个")
    problem_lines: list[str] = []
    for report in reports:
        if not report.login_ok:
            problem_lines.append(f"  [{report.account_id}] 登录：{report.login_message}")
        for jr in report.jobs:
            if jr.error:
                problem_lines.append(f"  [{jr.account_id}/{jr.job}] 错误：{jr.error}")
            for warn in jr.warnings[:3]:
                problem_lines.append(f"  [{jr.account_id}/{jr.job}] 警告：{warn}")
    if problem_lines:
        lines.append("")
        lines.append("需关注：")
        lines.extend(problem_lines)
    lines.append("=" * 78)
    return "\n".join(lines)


def exit_code(reports: list[AccountReport]) -> int:
    """0 全部成功；1 部分成功；2 全部失败。"""
    statuses = [r.status for r in reports]
    if not statuses:
        return 2
    if all(s == "success" for s in statuses):
        return 0
    if any(s in ("success", "partial") for s in statuses):
        return 1
    return 2
