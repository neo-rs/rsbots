@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Explorer double-click closes the window on exit.
REM Re-launch once inside `cmd /k` so the console stays open.
if not defined ORACLE_GUI_RUNNER_WRAPPED (
  set "ORACLE_GUI_RUNNER_WRAPPED=1"
  start "Chromerrunner Oracle GUI Runner" cmd /k ""%~f0" --wrapped %*"
  exit /b 0
)
if /I "%~1"=="--wrapped" shift

set "DEBUG="
if /I "%~1"=="--debug" (
  set "DEBUG=1"
  shift
)
if defined DEBUG (
  @echo on
)
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"
title Chromerrunner - Oracle GUI/CDP runner

REM This launcher:
REM - Opens SSH tunnels for noVNC (6080) and CDP (9222)
REM - Starts Oracle Chrome with CDP enabled (headed if GUI exists)
REM - Runs generic_product_checker.py in CDP mode with manual checkpoint (ENTER)

REM Resolve Oracle connection info via repo helpers (oraclekeys/servers.json).
pushd "%SCRIPT_DIR%\.."
set "ORA_USER="
set "ORA_HOST="
set "ORA_KEY="
set "ORA_ROOT="
where py >nul 2>&1
if errorlevel 1 (
  echo ERROR: `py` launcher not found. Install Python or run from a terminal where `py -3` works.
  echo Tip: open PowerShell in repo root and run: py -3 --version
  pause
  exit /b 1
)
for /f "usebackq tokens=1-4 delims=|" %%A in (`py -3 -c "from mirror_world_config import load_oracle_servers, resolve_oracle_ssh_key_path; from pathlib import Path; root=Path('.').resolve(); servers,_=load_oracle_servers(root); s=servers[0]; key=resolve_oracle_ssh_key_path(s['key'], root); print(f\"{s['user']}|{s['host']}|{key}|{s.get('remote_root','/home/rsadmin/bots/mirror-world')}\")" 2^>^&1`) do (
  set "ORA_USER=%%A"
  set "ORA_HOST=%%B"
  set "ORA_KEY=%%C"
  set "ORA_ROOT=%%D"
)
popd

if "%ORA_USER%"=="" (
  echo ERROR: Could not load Oracle server info (oraclekeys/servers.json).
  echo - Make sure you run this from the repo (py -3 must work)
  echo - Repo root should contain oraclekeys\servers.json
  echo - Tip: run this as: oracle_gui_runner.bat --debug
  pause
  exit /b 1
)

echo ==============================================================================
echo Chromerrunner Oracle GUI/CDP Runner
echo Server: %ORA_USER%@%ORA_HOST%
echo Key:    %ORA_KEY%
echo Root:   %ORA_ROOT%
echo ==============================================================================
echo.

:MENU
echo Choose:
echo   1) Start/verify noVNC on Oracle (port 6080)
echo   2) Open tunnels (noVNC 6080 + CDP 9222)
echo   3) Start Oracle Chrome CDP (HEADED)  [uses noVNC GUI]
echo   4) Start Oracle Chrome CDP (HEADLESS)
echo   5) Run Generic Checker (CDP + MANUAL ENTER)   [paste URL]
echo   6) One-shot: start noVNC + tunnels + headed chrome + run checker
echo   0) Exit
echo.
set /p CH=Selection:
if "%CH%"=="0" exit /b 0

if "%CH%"=="1" goto :NOVNC
if "%CH%"=="2" goto :TUNNELS
if "%CH%"=="3" goto :START_HEADED
if "%CH%"=="4" goto :START_HEADLESS
if "%CH%"=="5" goto :RUN_CHECKER
if "%CH%"=="6" goto :ONE_SHOT

goto :MENU

:NOVNC
echo.
echo Starting/verifying noVNC on Oracle (localhost:6080)...
echo If it needs packages, Oracle may prompt for sudo (first-time only).
echo.
"%WINDIR%\System32\OpenSSH\ssh.exe" -i "%ORA_KEY%" -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ConnectTimeout=60 %ORA_USER%@%ORA_HOST% "bash -lc 'cd %ORA_ROOT%/Chromerrunner && chmod +x start_oracle_novnc.sh && bash start_oracle_novnc.sh'"
if errorlevel 1 goto :FAIL
echo.
echo If noVNC is running, open tunnel option next, then visit:
echo   http://127.0.0.1:6080/vnc.html
echo.
pause
goto :MENU

:TUNNELS
echo.
echo Opening tunnels in a new window...
echo - noVNC: http://127.0.0.1:6080/vnc.html
echo - CDP:   http://127.0.0.1:9222/json/version
echo.
start "Oracle tunnels (keep open)" "%WINDIR%\System32\OpenSSH\ssh.exe" -i "%ORA_KEY%" -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ConnectTimeout=60 -L 6080:127.0.0.1:6080 -L 9222:127.0.0.1:9222 %ORA_USER%@%ORA_HOST%
goto :MENU

:START_HEADED
echo.
echo Starting Oracle Chrome (HEADED) with CDP...
echo Note: this requires a GUI session (noVNC/X11). If you don't have it, use HEADLESS.
echo.
"%WINDIR%\System32\OpenSSH\ssh.exe" -i "%ORA_KEY%" -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ConnectTimeout=60 %ORA_USER%@%ORA_HOST% "bash -lc 'cd %ORA_ROOT%/Chromerrunner && chmod +x start_chrome_oracle_cdp.sh && nohup bash start_chrome_oracle_cdp.sh --headed >/tmp/chromerrunner_cdp_chrome.log 2>&1 & sleep 1; curl -s http://127.0.0.1:9222/json/version | head -c 200; echo'"
if errorlevel 1 goto :FAIL
goto :MENU

:START_HEADLESS
echo.
echo Starting Oracle Chrome (HEADLESS) with CDP...
echo.
"%WINDIR%\System32\OpenSSH\ssh.exe" -i "%ORA_KEY%" -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ConnectTimeout=60 %ORA_USER%@%ORA_HOST% "bash -lc 'cd %ORA_ROOT%/Chromerrunner && chmod +x start_chrome_oracle_cdp.sh && nohup bash start_chrome_oracle_cdp.sh >/tmp/chromerrunner_cdp_chrome.log 2>&1 & sleep 1; curl -s http://127.0.0.1:9222/json/version | head -c 200; echo'"
if errorlevel 1 goto :FAIL
goto :MENU

:RUN_CHECKER
set "URL="
echo.
set /p URL=Paste product URL:
if "%URL%"=="" goto :MENU

REM Escape single quotes for bash single-quoted string: ' -> '"'"'
set "URL_BASH=%URL:'=\"'\"'\"'%"

echo.
echo Running generic checker on Oracle (CDP + manual checkpoint)...
echo - Chrome tab will open in the Oracle Chrome session.
echo - Solve any challenge in noVNC GUI, then press ENTER here when ready.
echo.
"%WINDIR%\System32\OpenSSH\ssh.exe" -i "%ORA_KEY%" -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ConnectTimeout=60 %ORA_USER%@%ORA_HOST% "bash -lc 'cd %ORA_ROOT%/Chromerrunner && source .venv/bin/activate && python generic_product_checker.py --url ''%URL_BASH%'' --connect-cdp --cdp-url http://127.0.0.1:9222 --manual'"
if errorlevel 1 goto :FAIL
echo.
pause
goto :MENU

:ONE_SHOT
call :NOVNC
call :TUNNELS
call :START_HEADED
call :RUN_CHECKER
goto :MENU

:FAIL
echo.
echo ERROR: step failed (exit=%ERRORLEVEL%)
echo Tip: re-run with: oracle_gui_runner.bat --debug
echo.
pause
goto :MENU

