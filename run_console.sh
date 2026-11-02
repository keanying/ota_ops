#!/usr/bin/env bash
# 启动 Web 控制台。放在项目根目录（与 ctrip/、webconsole/ 同级）执行。
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# 若用 venv，取消注释
# source .venv/bin/activate

export CONSOLE_HOST="${CONSOLE_HOST:-127.0.0.1}"   # 默认只听本机，用 SSH 隧道访问
export CONSOLE_PORT="${CONSOLE_PORT:-8800}"

echo "控制台地址：http://${CONSOLE_HOST}:${CONSOLE_PORT}"
echo "从本地访问服务器上的控制台，先开隧道："
echo "  ssh -N -L ${CONSOLE_PORT}:127.0.0.1:${CONSOLE_PORT} <user>@<server>"
exec python -m webconsole
