"""Web 控制台服务端。

启动：
    python -m webconsole            # 或 bash run_console.sh
然后浏览器打开 http://<服务器IP>:8800

提供三件事：
  1. 账号列表与登录态状态（读项目的 config / CookieStore）
  2. 实时可交互的远程浏览器（CDP 连续帧 + 真实输入事件派发）
  3. 采集日数据 / 月数据，日志实时回流页面

安全提示：这个控制台等同于「谁能打开页面，谁就能操作已登录的携程后台」。
只监听内网地址，或放在跳板机 / SSH 隧道后面，不要直接暴露公网。
"""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager
import json
import os
import platform
import queue
import re
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import date, datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from ctrip.auth import CookieStore
from ctrip.logging_utils import get_logger
from ctrip.config import load_config

from . import collect_schedule as csched
from . import datasync
from .display import Display, check_dependencies, resolve_mode
from . import notify
from .worker import VIEWPORT, BrowserWorker

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

@asynccontextmanager
async def lifespan(_app: FastAPI):
    """启动/关闭钩子。用 lifespan 而不是 on_event —— 后者已被 FastAPI 弃用。"""
    global _scheduler
    if not _past:
        _load_past()
    if _scheduler is None:
        _scheduler = _build_scheduler()
        _scheduler.start()
    try:
        yield
    finally:
        if _scheduler:
            _scheduler.stop()


app = FastAPI(
    lifespan=lifespan,
    title="携程采集控制台",
    description=(
        "HTTP 接口见下方列表。**WebSocket 不在 OpenAPI 里**，"
        "协议说明看 [/api/ws-spec](/api/ws-spec)，"
        "命令行试调用：`python -m webconsole.ws_client spec`"
    ),
)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

_cfg = load_config(os.environ.get("CTRIP_CONFIG") or None, validate=False)
_scheduler: "datasync.Scheduler | None" = None
_workers: dict[str, BrowserWorker] = {}
_displays: dict[str, Display] = {}
_tasks: dict[str, "CollectTask"] = {}
_lock = threading.Lock()

# 画面模式：
#   cdp（默认，全平台）无头浏览器 + CDP screencast 实时推流
#   vnc（可选，仅 Linux）Xvfb + x11vnc，必须有头浏览器
STREAM_MODE, MODE_REASON = resolve_mode(os.environ.get("CONSOLE_STREAM", "auto"))

# 常驻浏览器（独立进程 + CDP 端口）默认**不开**：沿用项目原本的持久化 profile
# 方式，行为与命令行一致。开着的账号直接复用，靠的是控制台自己持有 worker，
# 不需要把浏览器拆成独立进程。
# 需要多进程共享同一个浏览器时才打开：CONSOLE_KEEP_ALIVE=1（可配 CONSOLE_CDP_PORT）。
KEEP_ALIVE = os.environ.get("CONSOLE_KEEP_ALIVE", "0") in ("1", "true", "True")
CONSOLE_CDP_PORT = int(os.environ.get("CONSOLE_CDP_PORT", "9300"))

if KEEP_ALIVE and getattr(_cfg.ctrip, "cdp_port", 0) <= 0:
    _cfg.ctrip.cdp_port = CONSOLE_CDP_PORT

# 启动浏览器时同步等待的上限（秒）。超过就先返回，让页面显示「启动中」，
# 别把用户晾在一个没有任何反馈的请求里。
START_ACK_WAIT = float(os.environ.get("CONSOLE_START_ACK", "6"))
# 登录前确保浏览器就绪的上限（秒）。新账号首次要初始化 profile，给足时间。
LOGIN_BROWSER_WAIT = float(os.environ.get("CONSOLE_LOGIN_WAIT", "120"))


def _account_index(account_id: str) -> int:
    ids = [a.account_id for a in _cfg.ctrip.accounts]
    return ids.index(account_id) if account_id in ids else 0


# =============================================================================
# 采集任务：起子进程跑项目自己的 CLI，stdout 实时回流
# =============================================================================

def rc_text(rc: int | None) -> str:
    """CLI 退出码 → 人话。0 全部成功；1 部分成功（如某个任务上游还没出数）；2 全部失败。

    退出码 1 不是失败：已采到的数据都已入库，照常推送下游。
    之前把 1 也显示成「失败」，且推送只认 0 —— 评级没出数时，
    同一次已经入库的供应商评级也被连带卡住推不出去。
    """
    if rc == 0:
        return "完成"
    if rc == 1:
        return "部分完成"
    if rc is None:
        return "运行中"
    return f"失败（退出码 {rc}）"


_EMPTY_JOB_RE = re.compile(r"任务 (\w+) 未采集到数据")

# =============================================================================
# 运行记录：每次采集一条，日志落盘，控制台重启后历史仍在
# =============================================================================

RUNS_DIR = PROJECT_ROOT / "logs" / "console_runs"
RUNS_INDEX = RUNS_DIR / "index.jsonl"
SOURCE_TEXT = {"manual": "手动", "schedule": "定时", "catchup": "补采", "login": "首采"}
STATUS_TEXT = {"queued": "排队中", "running": "运行中", "success": "完成",
               "partial": "部分完成", "failed": "失败", "stopped": "已停止",
               "skipped": "跳过"}

_runs: "dict[str, CollectTask]" = {}       # 本进程发起的（含进行中），按插入顺序
_past: list[dict] = []                     # 启动时从 index.jsonl 读回的历史摘要
_runs_lock = threading.Lock()


def _new_run_id(account_id: str) -> str:
    import secrets
    return f"{datetime.now():%Y%m%d-%H%M%S}-{account_id}-{secrets.token_hex(2)}"


def _append_index(summary: dict) -> None:
    try:
        RUNS_DIR.mkdir(parents=True, exist_ok=True)
        with RUNS_INDEX.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(summary, ensure_ascii=False) + "\n")
    except OSError as exc:
        print(f"[runs] 写运行记录失败：{exc}")


def _load_past(limit: int = 300) -> None:
    """启动时读回最近的运行摘要。文件只追加，读尾部即可。"""
    try:
        lines = RUNS_INDEX.read_text(encoding="utf-8").splitlines()[-limit:]
    except OSError:
        return
    for ln in lines:
        try:
            _past.append(json.loads(ln))
        except json.JSONDecodeError:
            continue


def _record_collect(account_id: str, label: str, source: str, reason: str) -> None:
    """记一条「没跑」的记录（登录态失效、已有任务在跑、发起失败）。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary = {"run_id": _new_run_id(account_id), "account_id": account_id,
               "label": label, "source": source, "status": "skipped",
               "status_text": STATUS_TEXT["skipped"], "detail": reason, "rc": None,
               "created_at": now, "started_at": "", "finished_at": now,
               "duration": 0, "lines": 0, "has_log": False}
    with _runs_lock:
        _past.append(summary)
        del _past[:-300]
    _append_index(summary)


def list_runs(limit: int = 100, account_id: str = "") -> list[dict]:
    with _runs_lock:
        live = [t.summary() for t in _runs.values()]
        seen = {r["run_id"] for r in live}
        rows = live + [r for r in _past if r["run_id"] not in seen]
    if account_id:
        rows = [r for r in rows if r["account_id"] == account_id]
    rows.sort(key=lambda r: r.get("created_at", ""), reverse=True)
    return rows[:limit]


def _register(task: "CollectTask") -> None:
    with _runs_lock:
        _runs[task.run_id] = task
        # 只保留最近 200 个在内存里；更早的已落盘，查日志走文件
        finished = [k for k, t in _runs.items() if t.finished]
        for k in finished[:max(0, len(_runs) - 200)]:
            _past.append(_runs.pop(k).summary())


class CollectTask:
    """一次采集任务。用子进程而非直接 import，避免与控制台共享日志/事件循环。

    生命周期：queued（建好未启动，批量任务里排队）→ running → 结束。
    日志同时写内存和 ``logs/console_runs/<run_id>.log``，结束时把摘要追加到
    ``index.jsonl`` —— 页面上的运行列表和日志弹窗在控制台重启后依然能看。
    """

    MAX_LINES = 20000

    def __init__(self, account_id: str, label: str, argv: list[str],
                 on_done=None, source: str = "manual") -> None:
        self.run_id = _new_run_id(account_id)
        self.account_id = account_id
        self.label = label
        self.argv = argv
        self.on_done = on_done
        self.source = source             # manual / schedule / catchup / login
        self.empty_jobs: list[str] = []  # 跑了但上游没数据的任务（如 rating 还没出）
        self.job = "day"
        self.sync_window: tuple[str, str] | None = None
        self.entries: list[dict] = []
        # 浏览器画面通道会顺带转发采集日志；没人连时丢弃，不无限堆积
        self.queue: queue.Queue[dict] = queue.Queue(maxsize=2000)
        self.proc: subprocess.Popen | None = None
        self.rc: int | None = None
        self.stopped = False
        self.skip_reason = ""
        self.created_at = datetime.now()
        self.started_at: datetime | None = None
        self.finished_at: datetime | None = None
        self.done = threading.Event()
        self._log_fh = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    # ------------------------------------------------------------ 状态
    @property
    def running(self) -> bool:
        return self.started_at is not None and not self.done.is_set()

    @property
    def finished(self) -> bool:
        return self.done.is_set()

    @property
    def status(self) -> str:
        if self.skip_reason:
            return "skipped"
        if self.started_at is None:
            return "queued"
        if not self.done.is_set():
            return "running"
        if self.stopped:
            return "stopped"
        return {0: "success", 1: "partial"}.get(self.rc, "failed")

    def summary(self) -> dict:
        end = self.finished_at or (datetime.now() if self.started_at else None)
        fmt = "%Y-%m-%d %H:%M:%S"
        return {
            "run_id": self.run_id, "account_id": self.account_id,
            "label": self.label, "source": self.source,
            "status": self.status, "status_text": STATUS_TEXT[self.status],
            "detail": self.skip_reason or self.empty_detail().strip("（）"),
            "rc": self.rc,
            "created_at": self.created_at.strftime(fmt),
            "started_at": self.started_at.strftime(fmt) if self.started_at else "",
            "finished_at": self.finished_at.strftime(fmt) if self.finished_at else "",
            "duration": (round((end - self.started_at).total_seconds())
                         if self.started_at and end else 0),
            "lines": len(self.entries), "has_log": self.started_at is not None,
        }

    # ------------------------------------------------------------ 运行
    def start(self) -> None:
        self.started_at = datetime.now()
        self._thread.start()

    def skip(self, reason: str) -> None:
        """排队中的任务轮到时发现不能跑（如同账号已有任务）。"""
        self.skip_reason = reason
        self.finished_at = datetime.now()
        self.done.set()
        _append_index(self.summary())

    def _open_log(self) -> None:
        try:
            RUNS_DIR.mkdir(parents=True, exist_ok=True)
            self._log_fh = (RUNS_DIR / f"{self.run_id}.log").open(
                "a", encoding="utf-8", buffering=1)
        except OSError as exc:
            print(f"[runs] 打开日志文件失败：{exc}")

    def _run(self) -> None:
        self._open_log()
        try:
            self._run_proc()
        finally:
            self.finished_at = datetime.now()
            self.done.set()
            _append_index(self.summary())
            if self._log_fh:
                with contextlib.suppress(OSError):
                    self._log_fh.close()
                self._log_fh = None

    def _run_proc(self) -> None:
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        self._push("info", f"$ {' '.join(self.argv)}")
        try:
            self.proc = subprocess.Popen(
                self.argv, cwd=str(PROJECT_ROOT), env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace", bufsize=1,
            )
        except Exception as exc:  # noqa: BLE001
            self.rc = -1
            self._push("error", f"启动失败：{exc}")
            self._push("done", f"{self.label} 未能启动", rc=-1)
            return

        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            line = line.rstrip()
            if line:
                hit = _EMPTY_JOB_RE.search(line)
                if hit and hit.group(1) not in self.empty_jobs:
                    self.empty_jobs.append(hit.group(1))
                level = "error" if (" ERROR " in line or "❌" in line) else (
                    "warn" if (" WARNING " in line or "⚠" in line) else "info")
                self._push(level, line)
        self.rc = self.proc.wait()
        text = "已停止" if self.stopped else rc_text(self.rc)
        self._push("done", f"{self.label} {text}{self.empty_detail()}", rc=self.rc)
        if self.on_done and not self.stopped:
            # 采集完成的后续动作（推 API）单独兜异常：
            # 推送失败不该让这次采集看起来像失败了，数据已经在本地库里
            try:
                self.on_done(self)
            except Exception as exc:  # noqa: BLE001
                self._push("warn", f"采集后置动作异常：{exc}")

    def _push(self, level: str, message: str, **extra) -> None:
        at = datetime.now().strftime("%H:%M:%S")
        if len(self.entries) < self.MAX_LINES:
            self.entries.append({"at": at, "level": level, "message": message})
        if self._log_fh:
            with contextlib.suppress(OSError):
                self._log_fh.write(f"{at}\t{level}\t{message}\n")
        item = {"type": "task", "level": level, "message": message,
                "label": self.label, "run_id": self.run_id, "at": at, **extra}
        with contextlib.suppress(queue.Full):
            self.queue.put_nowait(item)

    def empty_detail(self) -> str:
        if not self.empty_jobs:
            return ""
        names = "、".join(csched.JOB_LABELS.get(j, j) for j in self.empty_jobs)
        hint = ("，月初评级常常还没出上月数据，开启「缺数补采」后定时会逐日自动补"
                if set(self.empty_jobs) & set(csched.JOB_LABELS) else "")
        return f"（{names} 上游暂无数据{hint}）"

    def stop(self) -> None:
        if self.proc and self.rc is None:
            self.stopped = True
            self.proc.terminate()
            self._push("warn", "已请求终止任务")


def read_run_log(run_id: str, after: int = 0) -> tuple[dict | None, list[dict]]:
    """取一次运行的日志（从第 after 行起）。内存里有就用内存，否则读落盘文件。"""
    with _runs_lock:
        task = _runs.get(run_id)
        summary = task.summary() if task else next(
            (r for r in _past if r["run_id"] == run_id), None)
    if task is not None:
        return summary, task.entries[after:]
    if summary is None or not re.fullmatch(r"[\w.-]+", run_id):
        return None, []
    lines: list[dict] = []
    try:
        with (RUNS_DIR / f"{run_id}.log").open(encoding="utf-8") as fh:
            for i, ln in enumerate(fh):
                if i < after:
                    continue
                parts = ln.rstrip("\n").split("\t", 2)
                if len(parts) == 3:
                    lines.append({"at": parts[0], "level": parts[1], "message": parts[2]})
                else:
                    lines.append({"at": "", "level": "info", "message": ln.rstrip("\n")})
    except OSError:
        pass
    return summary, lines


def launch_collect(account_id: str, mode: str, params: dict, source: str = "manual",
                   start: bool = True) -> "CollectTask":
    """建一个采集任务并登记。start=False 时只排队（批量任务用）。"""
    label, argv = build_collect_argv(account_id, mode, params)
    task = CollectTask(account_id, label, argv, on_done=_after_collect_push, source=source)
    task.job = mode
    task.sync_window = _sync_window_of(mode, params)
    _register(task)
    if start:
        _tasks[account_id] = task
        task.start()
    return task


def run_batch(tasks: list["CollectTask"]) -> None:
    """按顺序逐个跑已排队的任务（阻塞）。账号之间串行，同账号有任务在跑就跳过。"""
    for task in tasks:
        if task.finished:
            continue
        busy = _tasks.get(task.account_id)
        if busy and busy is not task and busy.running:
            task.skip(f"该账号已有任务在跑：{busy.label}")
            continue
        _tasks[task.account_id] = task
        task.start()
        task.done.wait()


def collect_params_recent_years(years: int = 2) -> dict:
    """近 N 年的日数据区间。默认两年 —— 首次登录成功后把历史一次性拉齐。"""
    end = datasync.today_of(_cfg) - timedelta(days=1)
    start = end - timedelta(days=365 * years)
    return {"start_date": start.isoformat(), "end_date": end.isoformat()}


def _after_collect_push(task: "CollectTask") -> None:
    """采集完成后立刻把这一批数据推给下游。

    采集子进程已经把数据直写本地 MySQL；这里只负责把同一个窗口的数据
    读出来推给 API。两件事分开做，API 不通不会影响入库。
    """
    # 1 = 部分成功：已采到的部分已经入库，照样推。只有完全失败才不推。
    if task.rc not in (0, 1):
        return
    sc = _sync_cfg()
    if not (sc.enabled and sc.after_collect and sc.url):
        return

    job = "daily" if task.job == "day" else "monthly"
    window = task.sync_window
    task._push("info", f"开始推送到下游接口（{job}{'，窗口 ' + '~'.join(window) if window else ''}）")
    try:
        result = datasync.run_sync(_cfg, sc, job=job,
                                   account_ids=[task.account_id],
                                   window=window, reason="after_collect",
                                   on_log=lambda lv, m: task._push(lv, m))
        for item in result.get("tables", {}).values():
            if item["ok"]:
                task._push("info", f"→ {item['remote_table']}：{item['rows']} 行，"
                                   f"新增 {item['inserted']}，更新 {item['updated']}")
            else:
                # 推送失败时必须说清楚数据已经落在本地库里 —— 否则看到红字的人
                # 第一反应是这次采集白跑了，会去重采一遍
                task._push("error",
                           f"→ {item['remote_table']} 推送失败：{item['error']}"
                           f"（数据已入库，可在「同步」标签手动重推）")
    except Exception as exc:  # noqa: BLE001
        task._push("error", f"推送下游失败（数据已入库，可稍后重推）：{exc}")


def _python() -> str:
    return sys.executable or "python"


def build_collect_argv(account_id: str, mode: str, params: dict) -> tuple[str, list[str]]:
    """把页面参数翻译成项目 CLI 的命令行。"""
    argv = [_python(), "-m", "ctrip.cli", "collect", "--account-id", account_id]
    # 落地固定走 MySQL：本地直写；推给下游由控制台在采集完成后单独调 API。
    # CSV 不再作为选项——它既不是落库也不是上报，留着只会让人选错。
    argv += ["--sink", "mysql"]
    if params.get("write_mode"):
        argv += ["--write-mode", params["write_mode"]]

    if mode == "day":
        argv += ["--job", "kylin"]
        if params.get("start_date") and params.get("end_date"):
            argv += ["--start-date", params["start_date"],
                     "--end-date", params["end_date"]]
            label = f"日数据 {params['start_date']}~{params['end_date']}"
        elif params.get("end_date"):
            argv += ["--end-date", params["end_date"]]
            label = f"日数据 至 {params['end_date']}"
        else:
            days = int(params.get("days") or 1)
            argv += ["--days", str(days)]
            label = f"日数据 近 {days} 天"
    elif mode == "month":
        # jobs 可只采其中一类（评级补采时只补缺的那张表）
        jobs = [j for j in (params.get("jobs") or ["rating", "score"])
                if j in ("rating", "score")] or ["rating", "score"]
        for j in jobs:
            argv += ["--job", j]
        only = ("" if len(jobs) == 2
                else "（" + "、".join(csched.JOB_LABELS[j] for j in jobs) + "）")
        if params.get("start_month") and params.get("end_month"):
            argv += ["--start-month", params["start_month"],
                     "--end-month", params["end_month"]]
            label = f"月数据 {params['start_month']}~{params['end_month']}{only}"
        elif params.get("month"):
            argv += ["--month", params["month"]]
            label = f"月数据 {params['month']}{only}"
        else:
            label = f"月数据 上个月{only}"
    else:
        raise HTTPException(400, f"未知采集类型：{mode}")

    if params.get("skip_login"):
        argv.append("--skip-login")
    return label, argv


def _sync_window_of(mode: str, params: dict) -> tuple[str, str] | None:
    """这次采集覆盖的时间区间，用于采集后只推这一段。"""
    if mode == "day":
        if params.get("start_date") and params.get("end_date"):
            return params["start_date"], params["end_date"]
        end = datasync.today_of(_cfg) - timedelta(days=1)
        if params.get("end_date"):
            end = date.fromisoformat(params["end_date"])
        days = max(1, int(params.get("days") or 1))
        return (end - timedelta(days=days - 1)).isoformat(), end.isoformat()
    if params.get("start_month") and params.get("end_month"):
        return params["start_month"], params["end_month"]
    month = params.get("month") or datasync.last_month(datasync.today_of(_cfg))
    return month, month


# =============================================================================
# REST
# =============================================================================

@app.get("/")
def index() -> FileResponse:
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.get("/api/accounts")
def api_accounts() -> JSONResponse:
    store = CookieStore(_cfg)
    out = []
    for a in _cfg.ctrip.accounts:
        meta = store.load_meta(a.account_id)
        fp = meta.get("fingerprint") or {}
        worker = _workers.get(a.account_id)
        out.append({
            "account_id": a.account_id,
            "account": a.account,
            "name": getattr(a, "name", "") or "",
            "remark": getattr(a, "remark", "") or "",
            "status": getattr(a, "status", "active") or "active",
            "password_set": bool(getattr(a, "password", "")),
            "scenic_id": getattr(a, "scenic_id", "") or "",
            "has_state": store.exists(a.account_id),
            "is_fresh": store.is_fresh(a.account_id),
            "login_at": meta.get("login_at", ""),
            "last_check_at": meta.get("last_check_at", ""),
            "region_count": fp.get("region_count", 0),
            "browser": ("live" if worker and worker.alive else "off"),
            "task": (_tasks[a.account_id].label
                     if a.account_id in _tasks and _tasks[a.account_id].running else ""),
            "task_run_id": (_tasks[a.account_id].run_id
                            if a.account_id in _tasks and _tasks[a.account_id].running else ""),
        })
    vnc_ok, vnc_detail = check_dependencies()
    fs = notify.Settings.load().feishu
    return JSONResponse({
        "feishu_enabled": fs.enabled and bool(fs.webhook),
        "accounts": out,
        "stream": STREAM_MODE,
        "mode_reason": MODE_REASON,
        "platform": platform.system(),
        "vnc_ready": vnc_ok,
        "vnc_detail": vnc_detail,
        "viewport": VIEWPORT,
        "ttl_hours": _cfg.ctrip.cookie_ttl_hours,
        "sink": getattr(_cfg.run, "sink", "csv"),
        "today": date.today().isoformat(),
    })


@app.post("/api/accounts")
async def api_account_create(payload: dict) -> JSONResponse:
    """新建账号。account_id 自动生成，其余字段由页面提交。

    写进 config/config.yaml 的 ctrip.accounts，采集子进程读的是同一份配置，
    所以新账号建完立刻就能用。
    """
    global _cfg
    from .accounts_admin import AccountAdminError, NewAccount, create_account

    payload = payload or {}
    new = NewAccount(
        name=payload.get("name", ""),
        account=payload.get("account", ""),
        password=payload.get("password", ""),
        remark=payload.get("remark", ""),
        scenic_id=payload.get("scenic_id", ""),
        status=payload.get("status", "active") or "active",
        account_id=(payload.get("account_id") or "").strip(),
    )
    with _lock:
        try:
            fresh, got, backup = create_account(_cfg, new)
        except AccountAdminError as exc:
            raise HTTPException(400, str(exc))
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(500, f"创建账号失败：{exc}")
        _cfg = fresh
    return JSONResponse({
        "ok": True, "account_id": got.account_id, "name": got.name,
        "account": got.account, "scenic_id": got.scenic_id,
        "backup": str(backup),
        "message": f"账号已创建：{got.account_id}",
    })


@app.patch("/api/accounts/{account_id}")
async def api_account_update(account_id: str, payload: dict) -> JSONResponse:
    """修改已有账号。已登录的账号在页面上只开放改密码这一项。

    注意：改密码不会让当前登录态失效——登录态是 Cookie，与密码无关。
    下次需要重新登录时才会用到新密码。
    """
    global _cfg
    from .accounts_admin import AccountAdminError, update_account

    payload = payload or {}
    fields = {k: v for k, v in payload.items()
              if k in ("password", "name", "remark", "scenic_id", "status") and v}
    if not fields:
        raise HTTPException(400, "没有可修改的字段（可改：password/name/remark/scenic_id/status）")

    with _lock:
        current = {a.account_id: a for a in _cfg.ctrip.accounts}.get(account_id)
        if current is None:
            raise HTTPException(404, f"账号不存在：{account_id}")
        try:
            backup = update_account(Path(_cfg.config_path), account_id, fields)
            fresh = load_config(str(_cfg.config_path), validate=False)
            got = {a.account_id: a for a in fresh.ctrip.accounts}[account_id]
            for k, v in fields.items():
                if getattr(got, k) != v:
                    raise AccountAdminError(f"写入后 {k} 与预期不一致")
        except Exception as exc:  # noqa: BLE001
            import shutil
            if "backup" in dir():
                shutil.copy2(backup, _cfg.config_path)
            raise HTTPException(500, f"修改失败已回滚：{exc}")
        _cfg = fresh
    return JSONResponse({"ok": True, "account_id": account_id,
                         "fields": list(fields),
                         "message": f"已更新：{'、'.join(fields)}"})


@app.post("/api/accounts/{account_id}/verify")
def api_verify(account_id: str) -> JSONResponse:
    from ctrip.auth import verify_cookie

    store = CookieStore(_cfg)
    if not store.exists(account_id):
        return JSONResponse({"ok": False, "detail": "还没有登录态，先点开启浏览器登录"})
    ok, detail = verify_cookie(_cfg, account_id)
    return JSONResponse({"ok": ok, "detail": detail,
                         "is_fresh": store.is_fresh(account_id)})


def _heal_stale_holder(account_id: str) -> str:
    """清掉「进程已经没了、占用记录还在」的残留。

    控制台或命令行被强杀时，session.json 里的 holder_pid 留着，
    profile 就会被永久判定为占用。这里用 procutil 的非破坏性判断
    （不能用 os.kill(pid,0)，Windows 上那是 TerminateProcess）确认进程
    确实不在了，再清记录。进程还活着就不动它。
    """
    from ctrip.browser import read_session_meta, write_session_meta

    from .procutil import pid_alive

    try:
        meta = read_session_meta(_cfg, account_id)
        holder = int(meta.get("holder_pid") or 0)
        if holder and holder != os.getpid() and not pid_alive(holder):
            write_session_meta(_cfg, account_id, holder_pid=0, holder_since="")
            return f"已清除过期占用记录（进程 {holder} 已不存在）"
    except Exception:  # noqa: BLE001
        pass
    try:
        from ctrip.browser import clear_stale_locks

        n = clear_stale_locks(_cfg, account_id, get_logger("console"))
        if n:
            return f"已清理残留锁文件 {n} 个"
    except Exception:  # noqa: BLE001
        pass
    return ""


def _ensure_display(account_id: str) -> tuple[Display | None, JSONResponse | None]:
    """vnc 模式下准备虚拟显示；其余模式返回 (None, None)。"""
    if STREAM_MODE != "vnc":
        return None, None
    ok, detail = check_dependencies()
    if not ok:
        return None, JSONResponse(
            {"ok": False, "message": detail,
             "hint": "Linux 上装好 xvfb/x11vnc 后重启控制台；"
                     "其他平台用默认的 cdp 模式"}, status_code=503)
    display = _displays.get(account_id) or Display(
        account_id=account_id, index=_account_index(account_id),
        log_dir=_cfg.log_dir(account_id))
    try:
        display.start()
    except Exception as exc:  # noqa: BLE001
        return None, JSONResponse({"ok": False, "message": str(exc)}, status_code=500)
    _displays[account_id] = display
    return display, None


@app.post("/api/browser/{account_id}/start")
def api_browser_start(account_id: str) -> JSONResponse:
    """打开这个账号的浏览器。**幂等**：已经开着就直接用，没开就先开再用。

    整段加锁：vnc 模式要临时改进程级的 DISPLAY 环境变量，
    两个账号同时启动会互相串到对方的显示上。
    """
    with _lock:
        worker = _workers.get(account_id)
        if worker and worker.alive:
            return JSONResponse({"ok": True, "ready": True, "reused": True,
                                 "mode": worker.mode,
                                 "message": "浏览器已在运行，直接使用",
                                 "vnc_port": worker.display.vnc_port
                                 if worker.display else 0})

        display, err = _ensure_display(account_id)
        if err is not None:
            return err

        healed = _heal_stale_holder(account_id)

        worker = BrowserWorker(_cfg, account_id, mode=STREAM_MODE,
                               display=display, keep_alive=KEEP_ALIVE)
        _workers[account_id] = worker
        worker.start()
        # 只等一小会儿。启动慢（新账号要初始化 profile）时先把响应还给页面，
        # 让画面区立刻显示「启动中」，就绪与否由 WebSocket 的 status 事件驱动。
        ready = worker.wait_ready(START_ACK_WAIT)

    if worker.error:
        return JSONResponse({
            "ok": False, "busy": "占用" in worker.error, "message": worker.error,
            "healed": healed,
            "hint": "点「释放 profile 占用」；那里会告诉你占用者是谁、是否还活着",
        }, status_code=409)

    reused = bool(getattr(worker, "_session", None)
                  and getattr(worker._session, "reused", False))
    return JSONResponse({
        "ok": True, "ready": ready, "mode": STREAM_MODE, "reused": reused,
        "healed": healed,
        "message": ("已连上该账号正在运行的浏览器" if reused else
                    ("浏览器已就绪" if ready else "浏览器启动中，画面稍后出现")),
        "vnc_port": worker.display.vnc_port if worker.display else 0,
    })


@app.post("/api/browser/{account_id}/release")
def api_browser_release(account_id: str, force: bool = False) -> JSONResponse:
    """释放 profile 占用。

    占用可能来自三处，处理方式不同：

    1. **控制台自己的会话** —— 直接关掉。
    2. **常驻浏览器** —— 那是合法占用（会被 CDP 复用），只有 force 才停。
    3. **session.json 里记的 holder_pid** —— 进程若已不在，就是过期记录，
       清掉即可；还在跑则给出确认与结束命令，由人决定。

    第 3 条要自己判断进程存活，不能依赖 ``ctrip.browser._pid_alive``：
    它用 ``os.kill(pid, 0)``，这在 Windows 上是 TerminateProcess 而不是探测，
    既可能误杀也可能误判，导致占用永远解不开（实际踩到过）。
    """
    from ctrip.browser import (BrowserBusyError, clear_stale_locks,
                               keep_alive_status, profile_in_use,
                               read_session_meta, stop_keep_alive,
                               write_session_meta)

    from .procutil import describe, pid_alive

    steps = []
    log = get_logger("console")

    # 1) 先停掉本进程自己的 worker，不然它就是占用者
    worker = _workers.pop(account_id, None)
    if worker:
        worker.shutdown()
        worker.join(timeout=20)
        steps.append("已断开控制台自己的浏览器会话")
    display = _displays.pop(account_id, None)
    if display:
        display.stop()

    # 2) 常驻浏览器：合法占用，只有明确要求才停
    alive, ka = keep_alive_status(_cfg, account_id)
    if alive:
        if force:
            try:
                stop_keep_alive(_cfg, account_id, log)
                steps.append(f"已停止常驻浏览器（{ka}）")
            except Exception as exc:  # noqa: BLE001
                steps.append(f"停止常驻浏览器失败：{exc}")
        else:
            steps.append(f"常驻浏览器在线且会被复用，未停止（{ka}）")

    # 3) holder_pid：进程没了就是过期记录，直接清
    meta = read_session_meta(_cfg, account_id)
    holder = int(meta.get("holder_pid") or 0)
    holder_note = ""
    if holder and holder != os.getpid():
        holder_alive = pid_alive(holder)
        holder_note = describe(holder)
        if not holder_alive:
            write_session_meta(_cfg, account_id, holder_pid=0, holder_since="")
            steps.append(f"清除过期占用记录（进程 {holder} 已不存在）")
        elif force:
            write_session_meta(_cfg, account_id, holder_pid=0, holder_since="")
            steps.append(f"已强制清除占用记录（进程 {holder} 仍在运行，"
                         f"若它确实还在用同一 profile，请先结束它）")
        else:
            steps.append(f"占用记录保留：{holder_note}")

    # 4) 残留锁文件
    try:
        n = clear_stale_locks(_cfg, account_id, log)
        steps.append(f"清理残留锁文件 {n} 个")
    except BrowserBusyError as exc:
        steps.append(f"锁文件未清理：{exc}")
    except Exception as exc:  # noqa: BLE001
        steps.append(f"清理锁文件失败：{exc}")

    busy, detail = profile_in_use(_cfg, account_id)
    return JSONResponse({
        "ok": not busy, "busy": busy,
        "detail": detail if busy else "profile 已释放，可以重新启动浏览器",
        "holder_pid": holder, "holder_note": holder_note, "steps": steps,
        "hint": ("仍被占用。" + (holder_note or "") +
                 "　确认那是残留进程后，可再点一次并选择强制释放")
        if busy else "",
    })


@app.post("/api/browser/{account_id}/stop")
def api_browser_stop(account_id: str) -> JSONResponse:
    worker = _workers.pop(account_id, None)
    display = _displays.pop(account_id, None)
    if not worker:
        if display:
            display.stop()
        return JSONResponse({"ok": True, "message": "浏览器本来就没开"})
    worker.shutdown()
    worker.join(timeout=20)
    if display:
        display.stop()
    return JSONResponse({"ok": True, "message": "浏览器已关闭"})


@app.post("/api/collect/{account_id}/stop")
def api_collect_stop(account_id: str) -> JSONResponse:
    """注意：必须注册在 /{mode} 之前，否则 stop 会被当成 mode 匹配走。"""
    task = _tasks.get(account_id)
    if task and task.running:
        task.stop()
        return JSONResponse({"ok": True})
    return JSONResponse({"ok": True, "message": "没有正在运行的任务"})


@app.post("/api/collect/{account_id}/{mode}")
async def api_collect(account_id: str, mode: str, params: dict | None = None) -> JSONResponse:
    params = params or {}
    existing = _tasks.get(account_id)
    if existing and existing.running:
        raise HTTPException(409, f"该账号已有任务在跑：{existing.label}")
    task = launch_collect(account_id, mode, params, source="manual")
    return JSONResponse({"ok": True, "label": task.label, "argv": task.argv,
                         "run_id": task.run_id, "sync_window": task.sync_window})


# =============================================================================
# 任务管理：新建采集任务（可多账号）、运行列表、运行日志
# =============================================================================

@app.get("/api/runs")
def api_runs(limit: int = 100, account_id: str = "") -> JSONResponse:
    return JSONResponse({"runs": list_runs(max(1, min(500, limit)), account_id)})


@app.post("/api/runs")
async def api_runs_create(payload: dict) -> JSONResponse:
    """新建采集任务并立即运行。多个账号按顺序串行，排队中的也会出现在列表里。

    payload: {"account_ids": [...], "mode": "day"|"month", "params": {...}}
    """
    payload = payload or {}
    mode = payload.get("mode") or "day"
    if mode not in ("day", "month"):
        raise HTTPException(400, f"未知采集类型：{mode}")
    params = dict(payload.get("params") or {})
    known = {a.account_id for a in _cfg.ctrip.accounts}
    ids = [str(x) for x in (payload.get("account_ids") or []) if str(x) in known]
    if not ids:
        raise HTTPException(400, "至少选一个账号")
    busy = [a for a in ids if _tasks.get(a) and _tasks[a].running]
    tasks = []
    try:
        for aid in ids:
            tasks.append(launch_collect(aid, mode, params, source="manual", start=False))
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"参数有误：{exc}")
    threading.Thread(target=run_batch, args=(tasks,), name="collect-batch",
                     daemon=True).start()
    msg = f"已创建 {len(tasks)} 个采集任务" + (
        f"，其中 {len(busy)} 个账号已有任务在跑，轮到时会跳过" if busy else "")
    return JSONResponse({"ok": True, "message": msg,
                         "runs": [t.summary() for t in tasks]})


@app.get("/api/runs/{run_id}/log")
def api_run_log(run_id: str, after: int = 0) -> JSONResponse:
    summary, lines = read_run_log(run_id, max(0, after))
    if summary is None:
        raise HTTPException(404, "运行记录不存在")
    return JSONResponse({"run": summary, "lines": lines,
                         "next": max(0, after) + len(lines)})


@app.post("/api/runs/{run_id}/stop")
def api_run_stop(run_id: str) -> JSONResponse:
    task = _runs.get(run_id)
    if task is None:
        raise HTTPException(404, "运行记录不存在或已结束")
    if task.status == "queued":
        task.skip("已手动取消")
        return JSONResponse({"ok": True, "message": "已取消排队"})
    if task.running:
        task.stop()
        return JSONResponse({"ok": True, "message": "已请求停止"})
    return JSONResponse({"ok": True, "message": "任务已结束"})


# =============================================================================
# 定时采集
# =============================================================================

def _collect_sched() -> csched.CollectSchedule:
    return csched.CollectSchedule.from_dict(notify.Settings.load().collect)


def _catchup_items(cs: csched.CollectSchedule, force: bool = False) -> tuple[list[dict], str]:
    """评级补采：查本地库上月各账号缺哪张表。返回 (计划, 不补的原因)。"""
    today = datasync.today_of(_cfg)
    if not force and not csched.in_retry_window(cs, today):
        return [], "不在补采期内"
    if not _cfg.mysql.enabled:
        return [], "未启用 MySQL，无法判断是否已采齐"
    accounts = [a.account_id for a in csched.target_accounts(_cfg, cs)]
    month = csched.month_param(today)
    coverage = datasync.month_coverage(_cfg, month, accounts)
    return csched.plan(_cfg, cs, "catchup", today=today, coverage=coverage), ""


def _run_scheduled_collect(job: str) -> None:
    """定时到点：逐个账号发起采集。job = daily | monthly | catchup。

    账号之间串行 —— 同时跑多个子进程会同时压 MySQL 和对方接口；
    同一账号已有任务在跑时直接跳过，不排队堆积。
    采集完成后由 CollectTask 的回调推送下游，不用这里操心。
    """
    cs = _collect_sched()
    title = {"daily": "日数据", "monthly": "月数据", "catchup": "评级补采"}.get(job, job)
    if job == "catchup":
        items, why = _catchup_items(cs)
        if why:
            return
        if not items:
            _sync_log("info", f"评级补采：上月（{csched.month_param(datasync.today_of(_cfg))}）"
                              "各账号评级已齐，无需补采")
            return
    else:
        items = csched.plan(_cfg, cs, job, today=datasync.today_of(_cfg))
    if not items:
        _sync_log("warn", f"定时采集（{title}）：没有符合条件的账号")
        return

    source = "catchup" if job == "catchup" else "schedule"
    tasks = _queue_items(items, title, source)
    if tasks:
        _sync_log("info", f"定时{title}：{len(tasks)} 个账号排队，逐个执行")
        run_batch(tasks)
        for t in tasks:
            sm = t.summary()
            _sync_log({"success": "info", "partial": "warn"}.get(sm["status"], "error"),
                      f"{t.account_id} {t.label} {sm['status_text']}"
                      f"{('（' + sm['detail'] + '）') if sm['detail'] else ''}")


def _queue_items(items: list[dict], title: str, source: str) -> list["CollectTask"]:
    """把计划变成排队中的任务；登录态有问题的记一条跳过并发通知。"""
    tasks = []
    for item in items:
        aid = item["account_id"]
        if item["skip"]:
            _sync_log("warn", f"定时{title}跳过 {aid}：{item['skip']}")
            _record_collect(aid, title, source, item["skip"])
            # 登录态问题是要人来处理的，光记日志没人看得见
            if "登录" in item["skip"]:
                try:
                    notify.notify_verify_needed(
                        aid, _account_name(aid), _verify_link(aid),
                        f"定时采集前检查：{item['skip']}")
                except Exception:  # noqa: BLE001
                    pass
            continue
        try:
            tasks.append(launch_collect(aid, item["job"], item["params"],
                                        source=source, start=False))
        except Exception as exc:  # noqa: BLE001
            _sync_log("error", f"定时采集 {aid} 发起失败：{exc}")
            _record_collect(aid, title, source, f"发起失败：{exc}")
    return tasks


def _catchup_window_text(cs: csched.CollectSchedule) -> str:
    if not (cs.enabled and cs.monthly_enabled and cs.monthly_retry_days > 0):
        return ""
    last = cs.monthly_day + cs.monthly_retry_days
    return (f"每月 {cs.monthly_day + 1}～{min(last, 31)} 号 {cs.monthly_at}，"
            "上月评级仍缺数据的账号自动补采")


@app.get("/api/collect/schedule")
def api_collect_schedule() -> JSONResponse:
    cs = _collect_sched()
    return JSONResponse({
        **cs.to_dict(),
        "accounts": [{"account_id": a.account_id, "account": a.account}
                     for a in _cfg.ctrip.accounts if a.status == "active"],
        "next_daily": _fmt_dt(_scheduler.next_of("collect", "daily") if _scheduler else None),
        "next_monthly": _fmt_dt(_scheduler.next_of("collect", "monthly") if _scheduler else None),
        "catchup": _catchup_window_text(cs),
        "target_month": csched.month_param(datasync.today_of(_cfg)),
    })


@app.post("/api/collect/schedule")
async def api_collect_schedule_set(patch: dict) -> JSONResponse:
    notify.update_settings({"collect": patch or {}})
    cs = _collect_sched()
    sched = cs.as_schedule()
    return JSONResponse({"ok": True, **cs.to_dict(),
                         "next_daily": _fmt_dt(datasync.next_daily(sched)),
                         "next_monthly": _fmt_dt(datasync.next_monthly(sched)),
                         "catchup": _catchup_window_text(cs)})


@app.get("/api/collect/schedule/preview")
def api_collect_preview(job: str = "daily") -> JSONResponse:
    """预览这次定时会跑哪些账号、跳过哪些、为什么跳过。

    job=catchup 时不看今天是否在补采期内，直接按本地库现状算出「现在补会补哪些」。
    """
    cs = _collect_sched()
    if job == "catchup":
        try:
            items, why = _catchup_items(cs, force=True)
        except Exception as exc:  # noqa: BLE001
            return JSONResponse({"job": job, "items": [], "reason": f"查库失败：{exc}"})
        return JSONResponse({"job": job, "items": items, "reason": why,
                             "month": csched.month_param(datasync.today_of(_cfg))})
    return JSONResponse({"job": job, "items": csched.plan(
        _cfg, cs, job, today=datasync.today_of(_cfg))})


@app.post("/api/collect/schedule/catchup")
async def api_collect_catchup_now() -> JSONResponse:
    """立即补采一次上月缺失的评级（不受补采期限制）。后台跑，结果看「最近运行」。"""
    cs = _collect_sched()
    try:
        items, why = await asyncio.to_thread(_catchup_items, cs, True)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, f"查库失败：{exc}")
    if why:
        raise HTTPException(400, why)
    todo = [i for i in items if not i["skip"]]
    if not items:
        return JSONResponse({"ok": True, "message": "上月评级各账号都已采齐，无需补采"})
    tasks = _queue_items(items, "评级补采", "catchup")
    threading.Thread(target=run_batch, args=(tasks,), name="catchup-now",
                     daemon=True).start()
    return JSONResponse({"ok": True, "message":
                         f"已开始补采 {len(todo)} 个账号，"
                         f"{len(items) - len(todo)} 个因登录态跳过",
                         "items": items, "runs": [t.summary() for t in tasks]})


# =============================================================================
# 数据同步
# =============================================================================

_sync_logs: deque = deque(maxlen=300)


def _sync_cfg() -> datasync.SyncConfig:
    return datasync.SyncConfig.from_dict(notify.Settings.load().sync)


def _sync_log(level: str, message: str) -> None:
    _sync_logs.appendleft({"level": level, "message": message,
                           "at": datetime.now().strftime("%m-%d %H:%M:%S")})


@app.get("/api/sync/config")
def api_sync_config() -> JSONResponse:
    sc = _sync_cfg()
    data = sc.masked()
    data["default_pk"] = datasync.DEFAULT_PK
    data["tables_meta"] = {
        k: {"name": datasync.TABLES[k].name,
            "comment": datasync.TABLES[k].comment,
            "time_column": datasync.TABLES[k].partition_column,
            "period": "daily" if k in datasync.DAILY_TABLES else "monthly",
            "unique_key": list(datasync.TABLES[k].unique_key),
            "columns": [{"name": f.name, "type": f.type, "comment": f.comment}
                        for f in datasync.TABLES[k].fields]}
        for k in datasync.BUSINESS_TABLES
    }
    data["next_daily"] = _fmt_dt(_scheduler.next_of("sync", "daily") if _scheduler else None)
    data["next_monthly"] = _fmt_dt(_scheduler.next_of("sync", "monthly") if _scheduler else None)
    return JSONResponse(data)


@app.post("/api/sync/config")
async def api_sync_config_set(patch: dict) -> JSONResponse:
    notify.update_settings({"sync": patch or {}})
    sc = _sync_cfg()
    return JSONResponse({"ok": True, **sc.masked(),
                         "next_daily": _fmt_dt(datasync.next_daily(sc.schedule)),
                         "next_monthly": _fmt_dt(datasync.next_monthly(sc.schedule))})


@app.post("/api/sync/run")
async def api_sync_run(payload: dict | None = None) -> JSONResponse:
    """立即跑一次。dry_run=true 只取数不推送，用来验证配置。"""
    payload = payload or {}
    job = payload.get("job") or "daily"
    dry = bool(payload.get("dry_run"))
    try:
        result = await asyncio.to_thread(
            datasync.run_sync, _cfg, _sync_cfg(), job,
            payload.get("account_ids") or None, dry, _sync_log)
    except datasync.SyncError as exc:
        raise HTTPException(400, str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, f"同步失败：{exc}")
    return JSONResponse(result)


@app.get("/api/sync/status")
def api_sync_status() -> JSONResponse:
    return JSONResponse({
        "history": list(datasync.HISTORY)[:10],
        "logs": list(_sync_logs)[:80],
        "next_daily": _fmt_dt(_scheduler.next_of("sync", "daily") if _scheduler else None),
        "next_monthly": _fmt_dt(_scheduler.next_of("sync", "monthly") if _scheduler else None),
        "next_collect_daily": _fmt_dt(_scheduler.next_of("collect", "daily") if _scheduler else None),
        "next_collect_monthly": _fmt_dt(_scheduler.next_of("collect", "monthly") if _scheduler else None),
        "scheduler_alive": bool(_scheduler and _scheduler.is_alive()),
        "collect_history": list_runs(20),
    })


def _fmt_dt(dt) -> str:
    return dt.strftime("%Y-%m-%d %H:%M") if dt else ""


# =============================================================================
# 设置 / 飞书
# =============================================================================

@app.get("/api/ws-spec", tags=["WebSocket"])
def api_ws_spec() -> JSONResponse:
    """WebSocket 协议说明。

    /docs 只列 HTTP 路由，WebSocket 不在 OpenAPI 规范里，所以单独开这个接口，
    浏览器直接打开就能看到完整的收发格式。
    命令行试调用：python -m webconsole.ws_client login --account-id <id>
    """
    from .ws_client import WS_SPEC

    return JSONResponse(WS_SPEC)


@app.get("/api/settings")
def api_settings_get() -> JSONResponse:
    st = notify.Settings.load()
    data = st.masked()
    data["callback_url"] = "/feishu/callback"
    return JSONResponse(data)


@app.post("/api/settings")
async def api_settings_set(patch: dict) -> JSONResponse:
    st = notify.update_settings(patch or {})
    return JSONResponse({"ok": True, **st.masked()})


@app.post("/api/settings/feishu/test")
def api_settings_test(account_id: str = "A001") -> JSONResponse:
    """按真实通知格式发一条测试消息，确认机器人配好了。"""
    ok, detail = notify.notify_verify_needed(
        account_id, _account_name(account_id), _verify_link(account_id), "连通性测试")
    return JSONResponse({"ok": ok, "detail": detail})


@app.post("/feishu/callback")
async def feishu_callback(request: Request) -> JSONResponse:
    """飞书事件订阅回调。填地址时飞书会先做 URL 验证，这里原样回 challenge。"""
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse({"error": "请求体不是 JSON"}, status_code=400)
    body, code = notify.handle_callback(payload)
    return JSONResponse(body, status_code=code)


def _account_name(account_id: str) -> str:
    for a in _cfg.ctrip.accounts:
        if a.account_id == account_id:
            return a.account or account_id
    return account_id


def _verify_link(account_id: str) -> str:
    base = (notify.Settings.load().feishu.console_url or "").rstrip("/")
    if not base:
        base = f"http://{os.environ.get('CONSOLE_HOST', '127.0.0.1')}:" \
               f"{os.environ.get('CONSOLE_PORT', '8800')}"
    return f"{base}/?account={account_id}"


# =============================================================================
# 登录 WebSocket：接账号密码与短信验证码，回推登录状态
# =============================================================================

def _create_account_from_ws(msg: dict) -> tuple[str, str]:
    """建号（create_login 用）。返回 (account_id, 错误信息)，成功时错误为空。

    account_id 取登录账号本身，所以建完要把后续流程切到新 id 上。
    """
    global _cfg
    from .accounts_admin import AccountAdminError, NewAccount, create_account

    try:
        with _lock:
            fresh, got, _backup = create_account(_cfg, NewAccount(
                name=msg.get("name", ""), account=msg.get("account", ""),
                password=msg.get("password", ""), remark=msg.get("remark", ""),
                scenic_id=msg.get("scenic_id", "")))
            _cfg = fresh
        return got.account_id, ""
    except AccountAdminError as exc:
        return "", f"创建账号失败：{exc}"
    except Exception as exc:  # noqa: BLE001
        return "", f"创建账号异常：{exc}"


def _start_first_collect(account_id: str, spec: dict) -> tuple[str, list[str], str]:
    """登录成功后的首采。返回 (标签, 命令, 错误)。"""
    job = spec.get("job", "day")
    years = int(spec.get("years") or 2)
    params = {"sink": spec.get("sink") or "mysql"}
    if job == "day":
        params.update(collect_params_recent_years(years))
    else:
        params["months"] = years * 12
    try:
        task = launch_collect(account_id, job, params, source="login")
        return task.label, task.argv, ""
    except Exception as exc:  # noqa: BLE001
        return "", [], str(exc)


@app.websocket("/api/login/{account_id}")
async def login_ws(ws: WebSocket, account_id: str) -> None:
    """一条连接跑完整登录。

    客户端 → 服务端
        {"action":"login","account":"...","password":"...","timeout":600}
        {"action":"sms_code","code":"123456"}
        {"action":"abort"}
    服务端 → 客户端
        {"type":"login","stage":"submitted|need_captcha|need_sms|success|failed",...}
        {"type":"log",...}
    登录成功或客户端断开即关闭连接。
    """
    await ws.accept()
    stop = asyncio.Event()
    finished = asyncio.Event()
    pending_collect: dict = {}
    # create_login 会在建号后把账号切成新建出来的那个（account_id 取登录账号）。
    # 事件泵必须跟着切 —— 若把 account_id 按值传进去，泵会一直轮询旧 ID 的
    # worker，表现为「建号成功后整个流程静默卡死」。
    ctx = {"account_id": account_id}

    async def _emit(sock, stage: str, message: str, **extra) -> None:
        with contextlib.suppress(Exception):
            await sock.send_text(json.dumps(
                {"type": "login", "stage": stage, "message": message, **extra}))

    async def pump() -> None:
        """把 worker 的事件转发给客户端；需要人工验证时同时发飞书。

        同样要兜住意外异常：登录 WS 静默失效的话，客户端会一直等一个
        永远不来的 need_sms，比直接报错难查得多。
        """
        try:
            await _pump_login(ws, ctx, stop, finished)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[login {account_id}] 事件通道异常：{exc!r}")
            with contextlib.suppress(Exception):
                await ws.send_text(json.dumps({
                    "type": "login", "stage": "failed",
                    "message": f"服务端事件通道异常：{exc}"}))
            finished.set()
            stop.set()


    async def _pump_login(ws, ctx, stop, finished) -> None:
        while not stop.is_set():
            account_id = ctx["account_id"]
            worker = _workers.get(account_id)
            if not worker:
                await asyncio.sleep(0.2)
                continue
            try:
                evt = await asyncio.to_thread(worker.events.get, True, 0.3)
            except queue.Empty:
                continue
            except (WebSocketDisconnect, RuntimeError):
                stop.set()
                return

            if evt.get("type") == "config_changed":
                # 凭据写回了 config.yaml，把进程里的配置对象同步刷新，
                # 否则后续操作还在用旧密码
                global _cfg
                try:
                    _cfg = load_config(os.environ.get("CTRIP_CONFIG") or None,
                                       validate=False)
                except Exception as exc:  # noqa: BLE001
                    print(f"[login {account_id}] 重载配置失败：{exc}")
                with contextlib.suppress(Exception):
                    await ws.send_text(json.dumps(evt))
                continue

            if evt.get("type") in ("notify_captcha", "notify_sms"):
                link = _verify_link(account_id)
                if evt["type"] == "notify_sms":
                    phone = evt.get("phone", "")
                    reason = f"手机短信验证码{f'（{phone}）' if phone else ''}"
                else:
                    reason = evt.get("hint") or "滑块/图形验证"
                ok, detail = await asyncio.to_thread(
                    notify.notify_verify_needed, account_id,
                    _account_name(account_id), link, reason)
                try:
                    await ws.send_text(json.dumps({
                        "type": "notify", "sent": ok, "detail": detail,
                        "link": link, "at": evt.get("at", "")}))
                except (WebSocketDisconnect, RuntimeError):
                    stop.set()
                    return
                continue

            try:
                await ws.send_text(json.dumps(evt))
            except (WebSocketDisconnect, RuntimeError):
                stop.set()
                return

            if (evt.get("type") == "login" and evt.get("stage") == "success"
                    and pending_collect):
                spec = dict(pending_collect)
                pending_collect.clear()
                label, argv, err = await asyncio.to_thread(
                    _start_first_collect, account_id, spec)
                if err:
                    await _emit(ws, "collect_failed", f"启动采集失败：{err}")
                else:
                    await _emit(ws, "collecting", f"首次登录成功，开始采集：{label}",
                                label=label, argv=argv)

            if evt.get("type") == "login" and evt.get("stage") in (
                    "success", "failed", "aborted"):
                finished.set()

    pump_task = asyncio.create_task(pump())
    try:
        while not finished.is_set():
            recv = asyncio.create_task(ws.receive_text())
            done, _ = await asyncio.wait(
                {recv, asyncio.create_task(finished.wait())},
                return_when=asyncio.FIRST_COMPLETED)
            if recv not in done:
                recv.cancel()
                break
            try:
                msg = json.loads(recv.result())
            except json.JSONDecodeError:
                continue

            action = msg.get("action")

            if action == "create_login":
                await _emit(ws, "creating",
                            f"正在创建账号 {msg.get('account', '')}")
                new_id, err = await asyncio.to_thread(_create_account_from_ws, msg)
                if err:
                    await _emit(ws, "failed", err)
                    break
                await _emit(ws, "account_created",
                            f"账号已创建：{new_id}（写入 config.yaml）",
                            account_id=new_id, account=msg.get("account", ""))
                # account_id 取的是登录账号，与连接时用的占位 id 不同，
                # 事件泵要跟着切过去
                account_id = new_id
                ctx["account_id"] = new_id
                action = "login"

            if action == "login":
                started = await asyncio.to_thread(_ensure_browser, account_id)
                if not started[0]:
                    await ws.send_text(json.dumps({
                        "type": "login", "stage": "failed", "message": started[1]}))
                    break
                worker = _workers.get(account_id)
                if worker is None:
                    # 理论上 _ensure_browser 成功就一定有 worker，但真出现不一致时
                    # 直接下标访问会 KeyError 崩掉整个处理器、客户端什么都收不到
                    await _emit(ws, "failed",
                                f"浏览器实例丢失（账号 {account_id}），请重试")
                    break
                worker.submit({
                    "type": "login_start",
                    "account": msg.get("account", ""),
                    "password": msg.get("password", ""),
                    "timeout": msg.get("timeout", 600),
                    "sms_wait": msg.get("sms_wait", 300),
                    "persist": msg.get("persist", True),
                })
                await ws.send_text(json.dumps({
                    "type": "login", "stage": "starting",
                    "message": started[1]}))
                # 记下首采意图，登录成功时触发
                collect_after = msg.get("collect_after_login")
                if collect_after:
                    pending_collect.update(
                        collect_after if isinstance(collect_after, dict) else {})
                    pending_collect.setdefault("job", "day")
                    pending_collect.setdefault("years", 2)
            elif action == "sms_code":
                w = _workers.get(account_id)
                if w:
                    w.submit({"type": "sms_code", "code": msg.get("code", "")})
            elif action == "abort":
                w = _workers.get(account_id)
                if w:
                    w.submit({"type": "login_abort"})
                break
    except WebSocketDisconnect:
        # 客户端主动断开：中止登录，别让浏览器停在半路
        w = _workers.get(account_id)
        if w:
            w.submit({"type": "login_abort"})
    finally:
        stop.set()
        pump_task.cancel()
        try:
            await ws.close()
        except RuntimeError:
            pass


def _ensure_browser(account_id: str) -> tuple[bool, str]:
    """登录前确保浏览器已起（沿用 /api/browser/start 的全部逻辑）。

    start 接口为了不让页面干等，可能在浏览器还没就绪时就返回；
    这里必须真正等到 ready，否则 login_start 命令会排在队列里，
    而客户端看不出到底是在启动还是卡住了。等待期间 status 事件照常回推。
    """
    worker = _workers.get(account_id)
    if worker and worker.alive and worker._ready.is_set():
        return True, "浏览器已在运行"

    resp = api_browser_start(account_id)
    body = json.loads(bytes(resp.body).decode("utf-8"))
    if not body.get("ok"):
        return False, body.get("message", "浏览器启动失败")

    worker = _workers.get(account_id)
    if worker is None:
        return False, "浏览器实例丢失"
    if not worker.wait_ready(LOGIN_BROWSER_WAIT):
        return False, f"浏览器启动超时（{LOGIN_BROWSER_WAIT:.0f}s）"
    if worker.error:
        return False, worker.error
    return True, "浏览器已就绪"


# =============================================================================
# VNC 桥接：浏览器 WebSocket ←→ x11vnc TCP
# =============================================================================

@app.websocket("/vnc/{account_id}")
async def vnc_bridge(ws: WebSocket, account_id: str) -> None:
    """把 noVNC 的 WebSocket 直接接到 x11vnc 的 TCP 端口上。

    自己做桥接而不用 websockify：少一个进程、少一个依赖，而且能复用
    控制台已有的鉴权与端口（对外只暴露一个端口）。
    RFB 是二进制协议，两个方向都必须原样透传，不能做任何编解码。
    """
    # noVNC 会带 binary 子协议（老版本必需，新版本可选）
    sub = ws.headers.get("sec-websocket-protocol", "")
    offered = [p.strip() for p in sub.split(",") if p.strip()]
    await ws.accept(subprotocol="binary" if "binary" in offered else None)

    display = _displays.get(account_id)
    worker = _workers.get(account_id)
    if display is None or not display.running:
        await ws.close(code=1011, reason="VNC 未启动，请先开启浏览器")
        return

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", display.vnc_port)
    except OSError as exc:
        await ws.close(code=1011, reason=f"连接 VNC 失败：{exc}")
        return

    if worker:
        worker.emit("log", level="info", message="VNC 画面已连接")

    async def tcp_to_ws() -> None:
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                await ws.send_bytes(data)
        except (WebSocketDisconnect, RuntimeError, ConnectionError):
            pass

    async def ws_to_tcp() -> None:
        try:
            while True:
                msg = await ws.receive()
                if msg["type"] == "websocket.disconnect":
                    break
                data = msg.get("bytes")
                if data is None and msg.get("text") is not None:
                    data = msg["text"].encode("latin-1", "ignore")
                if data:
                    writer.write(data)
                    await writer.drain()
        except (WebSocketDisconnect, RuntimeError, ConnectionError, KeyError):
            pass

    up = asyncio.create_task(tcp_to_ws())
    down = asyncio.create_task(ws_to_tcp())
    try:
        await asyncio.wait({up, down}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in (up, down):
            t.cancel()
        writer.close()
        try:
            await writer.wait_closed()
        except (OSError, ConnectionError):
            pass
        if worker:
            worker.emit("log", level="info", message="VNC 画面已断开")


# =============================================================================
# WebSocket：帧下行 + 输入上行 + 日志
# =============================================================================

@app.websocket("/ws/{account_id}")
async def ws_endpoint(ws: WebSocket, account_id: str) -> None:
    await ws.accept()
    stop = asyncio.Event()

    async def pump_frames() -> None:
        """帧与事件下行。队列是线程队列，用 to_thread 取，避免阻塞事件循环。

        这里必须兜住所有异常：任务一旦静默死掉，客户端就永远收不到帧，
        画面停在最后一张、也没有任何报错，排查起来毫无线索。
        出意外就关掉连接，让前端的重连逻辑接管。
        """
        try:
            while not stop.is_set():
                worker = _workers.get(account_id)
                if not worker or not worker.alive:
                    await asyncio.sleep(0.2)
                    continue
                sent = False
                try:
                    frame = await asyncio.to_thread(worker.frames.get, True, 0.05)
                    await ws.send_text(json.dumps(frame))
                    sent = True
                except queue.Empty:
                    pass
                except (WebSocketDisconnect, RuntimeError):
                    stop.set()
                    return
                try:
                    while True:
                        evt = worker.events.get_nowait()
                        await ws.send_text(json.dumps(evt))
                        sent = True
                except queue.Empty:
                    pass
                except (WebSocketDisconnect, RuntimeError):
                    stop.set()
                    return
                if not sent:
                    await asyncio.sleep(0.02)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[ws {account_id}] 下行通道异常，已断开让前端重连：{exc!r}")
            stop.set()
            with contextlib.suppress(Exception):
                await ws.close(code=1011, reason="stream error")

    async def pump_task_logs() -> None:
        try:
            while not stop.is_set():
                task = _tasks.get(account_id)
                if not task:
                    await asyncio.sleep(0.3)
                    continue
                try:
                    item = await asyncio.to_thread(task.queue.get, True, 0.2)
                    await ws.send_text(json.dumps(item))
                except queue.Empty:
                    continue
                except (WebSocketDisconnect, RuntimeError):
                    stop.set()
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[ws {account_id}] 采集日志通道异常：{exc!r}")
            stop.set()

    frames_task = asyncio.create_task(pump_frames())
    logs_task = asyncio.create_task(pump_task_logs())

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            worker = _workers.get(account_id)
            if worker and worker.alive:
                worker.submit(msg)
    except WebSocketDisconnect:
        pass
    finally:
        stop.set()
        frames_task.cancel()
        logs_task.cancel()


def _build_scheduler() -> datasync.Scheduler:
    """采集在前、同步在后：到点先采，采完由回调推送下游；
    同步自己的定时是兜底，补推之前没推成功的。"""
    return datasync.Scheduler([
        datasync.JobSpec("daily", "collect", lambda: _collect_sched().as_schedule(),
                         lambda: _run_scheduled_collect("daily")),
        datasync.JobSpec("monthly", "collect", lambda: _collect_sched().as_schedule(),
                         lambda: _run_scheduled_collect("monthly")),
        # 评级补采：每天在月任务的时刻检查一次，是否在补采期由任务自己判断
        datasync.JobSpec("daily", "catchup", lambda: _collect_sched().as_catchup_schedule(),
                         lambda: _run_scheduled_collect("catchup")),
        datasync.JobSpec("daily", "sync", lambda: _sync_cfg().schedule,
                         lambda: datasync.run_sync(_cfg, _sync_cfg(), "daily",
                                                   on_log=_sync_log, reason="schedule")),
        datasync.JobSpec("monthly", "sync", lambda: _sync_cfg().schedule,
                         lambda: datasync.run_sync(_cfg, _sync_cfg(), "monthly",
                                                   on_log=_sync_log, reason="schedule")),
    ])


def main() -> None:
    import uvicorn

    host = os.environ.get("CONSOLE_HOST", "0.0.0.0")
    port = int(os.environ.get("CONSOLE_PORT", "8800"))
    label = {"vnc": "VNC 增量流（有头浏览器）",
             "cdp": "CDP 实时推流（无头浏览器）"}.get(STREAM_MODE, STREAM_MODE)
    print(f"控制台启动：http://{host}:{port}")
    print(f"接口文档：  http://{host}:{port}/docs"
          f"　WebSocket 协议：http://{host}:{port}/api/ws-spec")
    print(f"运行平台：{platform.system()} {platform.release()}")
    print(f"画面模式：{STREAM_MODE} —— {label}")
    print(f"          {MODE_REASON}")
    if STREAM_MODE == "vnc" and not check_dependencies()[0]:
        print(f"⚠️  {MODE_REASON}")
    print("⚠️  能打开这个页面就能操作已登录的携程后台，请只在内网或 SSH 隧道后使用")
    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
