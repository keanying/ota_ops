#!/usr/bin/env bash
# =============================================================================
# OTAOPS 控制台运维脚本（Linux 服务器）：启动 / 停止 / 重启 / 检测 / 看日志
#
#   bash otaops.sh start            后台启动，等到接口可访问才算成功
#   bash otaops.sh stop [-f]        停止（有采集在跑会先确认，-f 不确认）
#   bash otaops.sh restart [-f]     重启
#   bash otaops.sh status           详细状态：进程、端口、接口、调度、正在跑的任务
#   bash otaops.sh check [--restart] 健康检测，正常退出码 0、异常 1；
#                                   加 --restart 时异常自动重启（给 crontab 当看门狗用）
#   bash otaops.sh logs [-f] [N]    看控制台输出日志（默认最后 100 行，-f 持续跟踪）
#
# 配置（都可不配）：项目根目录放一个 console.env，或者直接用环境变量
#   CONSOLE_HOST=127.0.0.1   监听地址，默认只听本机（SSH 隧道访问）；内网访问改 0.0.0.0
#   CONSOLE_PORT=8800        端口
#   CONSOLE_STREAM=auto      画面模式 auto / vnc / cdp
#   PYTHON=/path/to/python   指定解释器；不配时依次找 .venv/、venv/、python3
#
# 看门狗示例（每 5 分钟检测一次，挂了自动拉起）：
#   */5 * * * * cd /path/to/ota_ops && bash otaops.sh check --restart >> logs/watchdog.log 2>&1
# =============================================================================
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

[[ -f "$ROOT/console.env" ]] && { set -a; . "$ROOT/console.env"; set +a; }

export CONSOLE_HOST="${CONSOLE_HOST:-127.0.0.1}"
export CONSOLE_PORT="${CONSOLE_PORT:-8008}"
START_WAIT="${START_WAIT:-40}"      # 启动后最多等多少秒接口可用
STOP_WAIT="${STOP_WAIT:-20}"        # 停止时最多等多少秒正常退出，超时强杀

LOG_DIR="$ROOT/logs"
PID_FILE="$LOG_DIR/console.pid"
OUT_LOG="$LOG_DIR/console.out"
mkdir -p "$LOG_DIR"

# ---------------------------------------------------------------- 输出
if [[ -t 1 ]]; then
  C_OK=$'\e[32m'; C_BAD=$'\e[31m'; C_WARN=$'\e[33m'; C_DIM=$'\e[2m'; C_B=$'\e[1m'; C_0=$'\e[0m'
else
  C_OK=""; C_BAD=""; C_WARN=""; C_DIM=""; C_B=""; C_0=""
fi
ts()   { date '+%Y-%m-%d %H:%M:%S'; }
ok()   { echo "${C_OK}✔${C_0} $*"; }
bad()  { echo "${C_BAD}✘${C_0} $*"; }
warn() { echo "${C_WARN}!${C_0} $*"; }
info() { echo "${C_DIM}·${C_0} $*"; }

# ---------------------------------------------------------------- 解释器
find_python() {
  if [[ -n "${PYTHON:-}" ]]; then echo "$PYTHON"; return; fi
  local c
  for c in "$ROOT/.venv/bin/python" "$ROOT/venv/bin/python"; do
    [[ -x "$c" ]] && { echo "$c"; return; }
  done
  command -v python3 || command -v python
}
PY="$(find_python)"
[[ -z "$PY" ]] && { bad "找不到 Python，设置 PYTHON=/path/to/python 后再试"; exit 2; }

# 本机访问地址：监听 0.0.0.0 时也走 127.0.0.1
probe_host() { [[ "$CONSOLE_HOST" == "0.0.0.0" || "$CONSOLE_HOST" == "::" ]] && echo 127.0.0.1 || echo "$CONSOLE_HOST"; }
BASE_URL="http://$(probe_host):${CONSOLE_PORT}"

# ---------------------------------------------------------------- 进程
read_pid() { [[ -f "$PID_FILE" ]] && tr -dc '0-9' < "$PID_FILE" || true; }

# PID 文件里的进程还活着，并且确实是本项目的控制台（防止 PID 被别的进程复用）
is_ours() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null || return 1
  # 参数逐个比对：必须是 python … -m webconsole（整段命令行里只是「包含」这串字的 shell 不算）
  local cwd args=() i
  cwd="$(readlink "/proc/$pid/cwd" 2>/dev/null)"
  [[ "$cwd" == "$ROOT" ]] || return 1
  mapfile -d '' -t args < "/proc/$pid/cmdline" 2>/dev/null || return 1
  [[ "${args[0]:-}" == *python* ]] || return 1
  for ((i = 1; i < ${#args[@]} - 1; i++)); do
    [[ "${args[i]}" == "-m" && "${args[i+1]}" == "webconsole" ]] && return 0
  done
  return 1
}

running_pid() { local p; p="$(read_pid)"; is_ours "$p" && echo "$p"; }

# 没有 PID 文件时（比如之前用 run_console.sh 前台起的），按命令行和工作目录找
find_stray() {
  local p
  for p in $(pgrep -f -- "-m webconsole" 2>/dev/null); do
    [[ "$p" == "$$" ]] && continue
    is_ours "$p" && { echo "$p"; return; }
  done
}

port_owner() {
  if command -v ss >/dev/null 2>&1; then
    ss -ltnpH "sport = :${CONSOLE_PORT}" 2>/dev/null | head -1
  elif command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"${CONSOLE_PORT}" -sTCP:LISTEN 2>/dev/null | sed -n 2p
  fi
}

uptime_of() { ps -o etime= -p "$1" 2>/dev/null | tr -d ' '; }
mem_of()    { ps -o rss= -p "$1" 2>/dev/null | awk '{printf "%.0f MB", $1/1024}'; }

# ---------------------------------------------------------------- 接口探测（用项目的 Python，不依赖 curl / jq）
# 输出一行 key=value，失败时 ok=0 err=…
probe() {
  "$PY" - "$BASE_URL" <<'PYEOF' 2>/dev/null || echo "ok=0 err=探测脚本出错"
import json, sys, urllib.request
base = sys.argv[1]
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))   # 本机探测不走代理
def get(path):
    with opener.open(base + path, timeout=5) as r:
        return json.loads(r.read().decode("utf-8"))
try:
    s = get("/api/system")
except Exception as e:
    print("ok=0 err=" + (str(getattr(e, "reason", "")) or type(e).__name__).replace(" ", "_")); sys.exit(0)
out = {"ok": 1, "version": s.get("version") or "-", "sched": int(bool(s.get("scheduler_alive"))),
       "stream": (s.get("browser") or {}).get("stream", "-")}
try:
    st = get("/api/sync/status")
    out["next_collect"] = (st.get("next_collect_daily") or "-").replace(" ", "_")
    out["next_sync"] = (st.get("next_daily") or "-").replace(" ", "_")
except Exception:
    pass
try:
    runs = get("/api/runs?limit=50").get("runs", [])
    out["running"] = sum(1 for r in runs if r.get("status") == "running")
    out["queued"] = sum(1 for r in runs if r.get("status") == "queued")
except Exception:
    out["running"] = out["queued"] = 0
print(" ".join(f"{k}={v}" for k, v in out.items()))
PYEOF
}
field() { sed -n "s/.*\b$1=\([^ ]*\).*/\1/p" <<< "$2"; }

# ---------------------------------------------------------------- 命令
cmd_start() {
  local pid; pid="$(running_pid)"
  [[ -z "$pid" ]] && pid="$(find_stray)"
  if [[ -n "$pid" ]]; then
    echo "$pid" > "$PID_FILE"
    warn "控制台已在运行（PID $pid），无需启动；要重启用：bash otaops.sh restart"
    return 0
  fi
  local owner; owner="$(port_owner)"
  if [[ -n "$owner" ]]; then
    bad "端口 ${CONSOLE_PORT} 已被占用：$owner"
    info "换端口：CONSOLE_PORT=8801 bash otaops.sh start"
    return 1
  fi

  info "Python：$PY"
  info "监听：${CONSOLE_HOST}:${CONSOLE_PORT}　日志：${OUT_LOG}"
  echo "===== $(ts) start =====" >> "$OUT_LOG"
  # setsid：自成一个进程组，停止时连同浏览器、Xvfb、采集子进程一起收掉；不受 SSH 断开影响
  setsid nohup "$PY" -u -m webconsole >> "$OUT_LOG" 2>&1 < /dev/null &
  pid=$!
  echo "$pid" > "$PID_FILE"

  local i r
  for ((i = 1; i <= START_WAIT; i++)); do
    sleep 1
    if ! kill -0 "$pid" 2>/dev/null; then
      bad "启动失败：进程已退出。最近的日志："
      tail -n 30 "$OUT_LOG" | sed 's/^/    /'
      rm -f "$PID_FILE"
      return 1
    fi
    r="$(probe)"
    if [[ "$(field ok "$r")" == "1" ]]; then
      ok "已启动（PID $pid，用时 ${i}s）  ${BASE_URL}"
      [[ "$(field sched "$r")" == "1" ]] && ok "调度线程运行中，下次采集 $(field next_collect "$r" | tr _ ' ')" \
                                         || warn "调度线程未运行，定时采集不会执行"
      [[ "$CONSOLE_HOST" == "127.0.0.1" ]] && \
        info "本地访问先开隧道：ssh -N -L ${CONSOLE_PORT}:127.0.0.1:${CONSOLE_PORT} <user>@<服务器>"
      return 0
    fi
  done
  warn "进程在跑（PID $pid），但 ${START_WAIT}s 内接口没有响应。看日志：bash otaops.sh logs"
  return 1
}

cmd_stop() {
  local force="${1:-}" pid
  pid="$(running_pid)"; [[ -z "$pid" ]] && pid="$(find_stray)"
  if [[ -z "$pid" ]]; then
    rm -f "$PID_FILE"
    info "控制台没有在运行"
    return 0
  fi

  # 有采集在跑：停止会中断它，先确认
  local r n; r="$(probe)"; n=$(( $(field running "$r" || echo 0) + 0 ))
  if (( n > 0 )) && [[ "$force" != "-f" && "$force" != "--force" ]]; then
    warn "有 ${n} 个采集任务正在运行，停止会中断它们（可以之后在「任务管理」里重跑）"
    if [[ -t 0 ]]; then
      read -r -p "仍要停止？[y/N] " a
      [[ "$a" =~ ^[Yy]$ ]] || { info "已取消"; return 1; }
    else
      bad "非交互环境下不停止；确认要停加 -f"
      return 1
    fi
  fi

  echo "===== $(ts) stop =====" >> "$OUT_LOG"
  local pgid; pgid="$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ')"
  # 进程组是本控制台自己的（setsid 启动）才按组发信号，否则只发给进程本身
  if [[ -n "$pgid" && "$pgid" == "$pid" ]]; then kill -TERM -- "-$pgid" 2>/dev/null; else kill -TERM "$pid" 2>/dev/null; fi

  local i
  for ((i = 1; i <= STOP_WAIT; i++)); do
    kill -0 "$pid" 2>/dev/null || { rm -f "$PID_FILE"; ok "已停止（PID $pid，用时 ${i}s）"; return 0; }
    sleep 1
  done
  warn "${STOP_WAIT}s 内没有退出，强制结束"
  if [[ -n "$pgid" && "$pgid" == "$pid" ]]; then kill -KILL -- "-$pgid" 2>/dev/null; else kill -KILL "$pid" 2>/dev/null; fi
  sleep 1
  rm -f "$PID_FILE"
  kill -0 "$pid" 2>/dev/null && { bad "无法结束 PID $pid"; return 1; }
  ok "已强制停止（PID $pid）"
}

cmd_restart() {
  cmd_stop "${1:-}" || return 1
  sleep 1
  cmd_start
}

cmd_status() {
  local pid r rc=0
  pid="$(running_pid)"; [[ -z "$pid" ]] && pid="$(find_stray)"
  echo "${C_B}OTAOPS 控制台${C_0}  ${C_DIM}${ROOT}${C_0}"
  if [[ -n "$pid" ]]; then
    ok "进程：运行中  PID $pid · 已运行 $(uptime_of "$pid") · 内存 $(mem_of "$pid")"
    [[ "$(read_pid)" != "$pid" ]] && { echo "$pid" > "$PID_FILE"; info "PID 文件已补写（之前不是用本脚本启动的）"; }
  else
    bad "进程：未运行"; rc=1
  fi

  local owner; owner="$(port_owner)"
  if [[ -z "$owner" ]]; then
    bad "端口：${CONSOLE_PORT} 未监听"; rc=1
  elif [[ -n "$pid" ]]; then
    ok "端口：${CONSOLE_HOST}:${CONSOLE_PORT} 监听中"
  else
    warn "端口：${CONSOLE_PORT} 被其他进程占用：$owner"; rc=1
  fi

  r="$(probe)"
  if [[ "$(field ok "$r")" == "1" ]]; then
    ok "接口：${BASE_URL} 正常  版本 $(field version "$r") · 画面模式 $(field stream "$r")"
    if [[ "$(field sched "$r")" == "1" ]]; then
      ok "调度：运行中  下次采集 $(field next_collect "$r" | tr _ ' ') · 下次补推 $(field next_sync "$r" | tr _ ' ')"
    else
      bad "调度：未运行（定时采集不会执行）"; rc=1
    fi
    local run q; run="$(field running "$r")"; q="$(field queued "$r")"
    info "任务：运行中 ${run:-0} · 排队 ${q:-0}"
  else
    bad "接口：${BASE_URL} 无响应（$(field err "$r" | tr _ ' ')）"; rc=1
  fi

  if [[ -f "$OUT_LOG" ]]; then
    local errs; errs="$(tail -n 300 "$OUT_LOG" | grep -ciE 'traceback|error|exception' || true)"
    info "日志：${OUT_LOG}（最近 300 行里有 ${errs} 行含 error / traceback）"
  fi
  (( rc == 0 )) && ok "${C_B}整体：健康${C_0}" || bad "${C_B}整体：异常${C_0}"
  return $rc
}

cmd_check() {
  local pid r
  pid="$(running_pid)"; [[ -z "$pid" ]] && pid="$(find_stray)"
  r="$(probe)"
  if [[ -n "$pid" && "$(field ok "$r")" == "1" && "$(field sched "$r")" == "1" ]]; then
    [[ -t 1 ]] && ok "健康（PID $pid）"
    return 0
  fi
  local why
  if [[ -z "$pid" ]]; then why="进程未运行"
  elif [[ "$(field ok "$r")" != "1" ]]; then why="接口无响应（$(field err "$r" | tr _ ' ')）"
  else why="调度线程未运行"; fi
  echo "$(ts) 异常：$why"
  if [[ "${1:-}" == "--restart" ]]; then
    echo "$(ts) 自动重启"
    # 看门狗场景：进程在但接口卡死 / 调度挂了，直接强制重启
    [[ -n "$pid" ]] && cmd_stop -f
    cmd_start && return 0
  fi
  return 1
}

cmd_logs() {
  local follow="" n=100 a
  for a in "$@"; do
    case "$a" in -f|--follow) follow=1 ;; *[!0-9]*) ;; *) n="$a" ;; esac
  done
  [[ -f "$OUT_LOG" ]] || { info "还没有日志：$OUT_LOG"; return 0; }
  if [[ -n "$follow" ]]; then tail -n "$n" -F "$OUT_LOG"; else tail -n "$n" "$OUT_LOG"; fi
}

usage() {
  sed -n '3,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

case "${1:-}" in
  start)   cmd_start ;;
  stop)    cmd_stop "${2:-}" ;;
  restart) cmd_restart "${2:-}" ;;
  status)  cmd_status ;;
  check)   cmd_check "${2:-}" ;;
  logs)    shift; cmd_logs "$@" ;;
  -h|--help|help|"") usage ;;
  *) bad "未知命令：$1"; usage; exit 2 ;;
esac
