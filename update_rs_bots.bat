@echo off
setlocal EnableExtensions

cd /d "%~dp0"

py -3 --version >nul 2>&1
if errorlevel 1 (
  where python >nul 2>&1
  if errorlevel 0 (
    python scripts\run_oracle_update_bots.py --group rs %*
    exit /b %errorlevel%
  )
  echo ERROR: `py -3` is not available and `python` was not found.
  exit /b 1
) else (
  py -3 scripts\run_oracle_update_bots.py --group rs %*
)

