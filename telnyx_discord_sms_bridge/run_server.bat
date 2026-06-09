@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [run] Missing virtual environment. Run setup_windows.bat first.
  exit /b 1
)

echo [run] Starting Telnyx Discord SMS Bridge...
.venv\Scripts\python.exe -m app.main
