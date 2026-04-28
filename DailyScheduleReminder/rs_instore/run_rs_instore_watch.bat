@echo off
setlocal
cd /d "%~dp0"
title RS In-Store watch mode
echo.
echo  Watching breakdown channel and auto-processing new posts.
echo  To dry-run (no posting/editing), add: --no-send
echo.
py -3 "%~dp0rs_instore_lead_flow.py" --watch
set EX=%ERRORLEVEL%
echo.
if not %EX%==0 pause
endlocal
exit /b %EX%

