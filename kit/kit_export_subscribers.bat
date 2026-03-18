@echo off
setlocal

REM Export Kit subscribers to CSV (name, email, tags) in this folder.
REM Output: subscribers_export_YYYYMMDD_HHMMSS.csv

cd /d "%~dp0"
py -3 kit_export_subscribers.py
echo.
pause
exit /b %errorlevel%
