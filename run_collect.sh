#!/bin/bash

# 切换到项目目录
cd /home/bi_normal_user/coleta/app/ctrip_collector/ctrip_collector-1.0.0 || {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 错误：无法切换到项目目录"
    exit 1
}

# 获取 T-1 日期
T_1_DATE=$(date -d "yesterday" +%Y-%m-%d)

# 定义 Python 路径（可以使用相对路径了）
PYTHON_PATH="./venv/bin/python"
COMMAND="$PYTHON_PATH -m ctrip.cli collect --job kylin --start-date $T_1_DATE --sink mysql"

# 执行命令
echo "$(date '+%Y-%m-%d %H:%M:%S') - 当前目录: $(pwd)"
echo "$(date '+%Y-%m-%d %H:%M:%S') - 开始执行，日期: $T_1_DATE"
$COMMAND

# 检查执行结果
if [ $? -eq 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 执行成功"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 执行失败"
    exit 1
fi