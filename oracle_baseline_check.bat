@echo off
setlocal

REM Oracle baseline check (local workspace vs latest downloaded Oracle snapshot).
REM This does NOT deploy anything. It only generates manifests + prints diffs.
REM
REM Typical usage:
REM   download_oracle_snapshot.bat
REM   oracle_baseline_check.bat
REM
REM One-shot (download + check):
REM   oracle_baseline_check.bat --download

cd /d "%~dp0"

REM Pick a Python interpreter:
REM - Prefer `python` if it's a real Python (not the Windows Store alias)
REM - Otherwise use the Windows launcher `py -3`
REM - As a last resort, try a local venv at .venv\Scripts\python.exe
set "PY_EXE="
set "PY_ARGS="

python --version >nul 2>&1
if not errorlevel 1 (
  REM Windows Store alias prints "Python was not found..." even though the command exists.
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
  echo - Install Python 3.x, OR enable the `py` launcher, OR create .venv.
  echo - If you see "Python was not found; run without arguments to install from the Microsoft Store",
  echo   disable the Windows Store python alias: Settings ^> Apps ^> Advanced app settings ^> App execution aliases.
  exit /b 1
)

"%PY_EXE%" %PY_ARGS% scripts\\oracle_baseline_check.py %*
exit /b %errorlevel%

