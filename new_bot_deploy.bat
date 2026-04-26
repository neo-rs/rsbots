@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

REM -----------------------------------------------------------------------------
REM new_bot_deploy.bat
REM - CLI mode: pass args through to scripts\new_bot_deploy.py
REM - Interactive mode: run with no args -> python prompts
REM -----------------------------------------------------------------------------

py -3 --version >nul 2>&1
if errorlevel 1 (
  where python >nul 2>&1
  if errorlevel 0 (
    python scripts\new_bot_deploy.py %*
    exit /b %ERRORLEVEL%
  )
  echo ERROR: `py -3` is not available and `python` was not found.
  exit /b 1
)

py -3 scripts\new_bot_deploy.py %*
exit /b %ERRORLEVEL%

