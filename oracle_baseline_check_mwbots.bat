@echo off
setlocal

REM Oracle baseline check (MWBots): local workspace vs latest downloaded Oracle snapshot.
REM One-shot:
REM   oracle_baseline_check_mwbots.bat --download

cd /d "%~dp0"

set "PY_EXE="
set "PY_ARGS="

python --version >nul 2>&1
if not errorlevel 1 (
  for /f "tokens=1,* delims= " %%a in ('python --version 2^>^&1') do (
    if /i "%%a"=="Python" (
      set "PY_EXE=python"
      set "PY_ARGS="
    )
  )
)

if not defined PY_EXE (
  py -3 --version >nul 2>&1
  if not errorlevel 1 (
    set "PY_EXE=py"
    set "PY_ARGS=-3"
  )
)

if not defined PY_EXE (
  if exist ".venv\\Scripts\\python.exe" (
    ".venv\\Scripts\\python.exe" --version >nul 2>&1
    if not errorlevel 1 (
      set "PY_EXE=.venv\\Scripts\\python.exe"
      set "PY_ARGS="
    )
  )
)

if not defined PY_EXE (
  echo ERROR: No working Python found.
  exit /b 1
)

"%PY_EXE%" %PY_ARGS% scripts\\oracle_baseline_check_mwbots.py %*
exit /b %errorlevel%

