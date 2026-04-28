@echo off
rem Default: choose Interactive (type channel + message) or JSON (no prompts).
rem Skip menu: run_manual_batch_send.bat interactive   OR   ... json
rem Double-click friendly: pause at end so the window stays open.
setlocal
cd /d "%~dp0"

py -3 --version >nul 2>&1
if errorlevel 1 (
  echo.
  echo [ERROR] Could not run "py -3". Install Python from https://www.python.org/downloads/
  echo         and enable the launcher, then try again.
  echo.
  pause
  exit /b 1
)

if /i "%~1"=="interactive" goto :arg_interactive
if /i "%~1"=="json" goto :arg_json
goto :menu

:arg_interactive
shift
py -3 manual_batch_send.py --interactive %*
goto :after_run

:arg_json
shift
py -3 manual_batch_send.py %*
goto :after_run

:menu
echo.
echo  DailyScheduleReminder - manual send to Discord
echo  -------------------------------------------------
echo   1  Interactive  You type Channel ID and Message ^(asks before each send^)
echo.
echo   2  JSON file    Sends EVERYTHING in manual_send_payload.json at once
echo                   ^(no typing - edit the file first, or you will spam example text^)
echo.
choice /c 12 /n /m "Press 1 or 2: "
if errorlevel 2 goto :run_json
if errorlevel 1 goto :run_interactive
goto :menu

:run_interactive
py -3 manual_batch_send.py --interactive
goto :after_run

:run_json
py -3 manual_batch_send.py
goto :after_run

:after_run
set "EXITCODE=%ERRORLEVEL%"

echo.
echo ----------------------------------------
if %EXITCODE% equ 0 (
  echo Finished OK. Exit code %EXITCODE%.
) else (
  echo Finished with errors. Exit code %EXITCODE%.
)
echo Press any key to close this window.
echo ----------------------------------------
pause >nul
exit /b %EXITCODE%
