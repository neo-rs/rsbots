@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo === Deploy Telnyx Discord SMS Bridge to Oracle ===
echo.
echo This uploads code from this PC and runs install_oracle.sh on the server.
echo For git-based deploy after push, omit --from-local:
echo   py -3 scripts\run_oracle_deploy_telnyx_bridge.py
echo.

py -3 scripts\run_oracle_deploy_telnyx_bridge.py --from-local %*
set "EC=%ERRORLEVEL%"

echo.
pause
exit /b %EC%
