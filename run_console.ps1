# Windows PowerShell 启动脚本（开发用）
Set-Location -Path $PSScriptRoot

# 若用 venv，取消注释
# .\.venv\Scripts\Activate.ps1

if (-not $env:CONSOLE_STREAM) { $env:CONSOLE_STREAM = "local" }
if (-not $env:CONSOLE_HOST)   { $env:CONSOLE_HOST   = "127.0.0.1" }
if (-not $env:CONSOLE_PORT)   { $env:CONSOLE_PORT   = "8800" }

Write-Host "控制台地址: http://$($env:CONSOLE_HOST):$($env:CONSOLE_PORT)"
Write-Host "画面模式:   $($env:CONSOLE_STREAM)"
python -m webconsole
