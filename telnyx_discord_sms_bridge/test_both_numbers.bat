@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [test] Missing virtual environment. Run setup_windows.bat first.
  exit /b 1
)

echo [test] Sending SMS both directions between configured Telnyx numbers...
.venv\Scripts\python.exe tools_test_bidirectional.py %*
