@echo off
setlocal
cd /d "%~dp0"

echo [setup] Creating Python virtual environment...
py -3 -m venv .venv
if errorlevel 1 (
  echo [setup] Failed to create virtual environment.
  exit /b 1
)

echo [setup] Installing requirements...
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 (
  echo [setup] Failed to install requirements.
  exit /b 1
)

if not exist ".env" (
  copy ".env.example" ".env" >nul
  echo [setup] Created .env from .env.example. Edit it before running the server.
) else (
  echo [setup] .env already exists. Leaving it untouched.
)

echo [setup] Done.
pause
