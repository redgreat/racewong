#!/usr/bin/env pwsh
# RaceBox 数据同步脚本 (Windows PowerShell)
# 用法:  .\run.ps1              # 默认下载
#        .\run.ps1 status       # 查询状态
#        .\run.ps1 full         # 下载+清除

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $PythonExe)) {
    Write-Host "未找到 .venv，请先执行: python -m venv .venv && .venv\Scripts\pip install -r requirements.txt" -ForegroundColor Red
    exit 1
}

& $PythonExe (Join-Path $ProjectRoot "src\racebox.py") @args
