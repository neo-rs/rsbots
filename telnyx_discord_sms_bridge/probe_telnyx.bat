@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [probe] Missing virtual environment. Run setup_windows.bat first.
  exit /b 1
)

.venv\Scripts\python.exe tools_probe_telnyx.py
