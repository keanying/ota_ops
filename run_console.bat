@echo off
REM Windows 下启动控制台（开发用）。放在项目根目录，与 ctrip\ 同级。
setlocal
cd /d "%~dp0"

REM 若用 venv，取消注释
REM call .venv\Scripts\activate.bat

REM local 模式：浏览器直接开在你的桌面上，用自己的鼠标键盘操作。
REM 想在页面里看内嵌画面就改成 cdp（注意那不是实时流）。
if "%CONSOLE_STREAM%"=="" set CONSOLE_STREAM=local
if "%CONSOLE_HOST%"==""   set CONSOLE_HOST=127.0.0.1
if "%CONSOLE_PORT%"==""   set CONSOLE_PORT=8800

echo 控制台地址: http://%CONSOLE_HOST%:%CONSOLE_PORT%
echo 画面模式:   %CONSOLE_STREAM%
python -m webconsole
endlocal
