@echo off
setlocal

REM Kit API: unsubscribe from Kit by email or CSV.
REM Double-click: runs test_emails.csv in this folder.
REM Or: kit_unsubscribe.bat test_emails.csv   or   kit_unsubscribe.bat other.csv

cd /d "%~dp0"

if "%~1"=="" (
  py -3 kit_unsubscribe.py test_emails.csv
  echo.
  pause
) else (
  py -3 kit_unsubscribe.py %*
)
exit /b %errorlevel%
