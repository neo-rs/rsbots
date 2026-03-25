@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo [RSCashoutBot] Working directory: %cd%
echo.

py -3 --version >nul 2>&1
if errorlevel 1 (
  where python >nul 2>&1
  if errorlevel 1 (
    echo ERROR: Neither `py -3` nor `python` was found on PATH.
    echo Install Python 3 or enable the py launcher, then run this again.
    pause
    exit /b 1
  )
  echo [RSCashoutBot] Using: python
  python bot.py %*
) else (
  echo [RSCashoutBot] Using: py -3
  py -3 bot.py %*
)

set "EC=%ERRORLEVEL%"
if not "%EC%"=="0" (
  echo.
  echo [RSCashoutBot] Exited with code %EC%
  pause
)
exit /b %EC%
