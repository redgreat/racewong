#!/usr/bin/env bash
# RaceBox 数据同步脚本 (Linux/macOS)
# 用法:  ./run.sh              # 默认下载
#        ./run.sh status       # 查询状态
#        ./run.sh full         # 下载+清除

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
PYTHON_EXE="$PROJECT_ROOT/.venv/bin/python"

if [ ! -f "$PYTHON_EXE" ]; then
    echo "未找到 .venv，请先执行: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
    exit 1
fi

exec "$PYTHON_EXE" "$PROJECT_ROOT/src/racebox.py" "$@"
