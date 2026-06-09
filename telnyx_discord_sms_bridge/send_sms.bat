@echo off
setlocal
cd /d "%~dp0"

if "%~1"=="" (
  echo Usage: send_sms.bat +15551234567 "message text" [optional_from_number]
  exit /b 1
)

if "%~2"=="" (
  echo Usage: send_sms.bat +15551234567 "message text" [optional_from_number]
  exit /b 1
)

if "%~3"=="" (
  .venv\Scripts\python.exe send_sms.py --to "%~1" --text "%~2"
) else (
  .venv\Scripts\python.exe send_sms.py --to "%~1" --text "%~2" --from "%~3"
)
