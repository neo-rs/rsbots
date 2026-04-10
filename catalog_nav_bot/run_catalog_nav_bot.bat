@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo [catalog_nav_bot] Working directory: %cd%
echo.

if not exist "config.json" (
  echo ERROR: config.json not found in this folder.
  echo Copy config.example.json to config.json and fill in token, ids, and menu_message_url.
  pause
  exit /b 1
)

py -3 --version >nul 2>&1
if errorlevel 1 (
  where python >nul 2>&1
  if errorlevel 1 (
    echo ERROR: Neither `py -3` nor `python` was found on PATH.
    echo Install Python 3 or enable the py launcher, then run this again.
    pause
    exit /b 1
  )
  echo [catalog_nav_bot] Using: python
  python navigation_bot.py %*
) else (
  echo [catalog_nav_bot] Using: py -3
  py -3 navigation_bot.py %*
)

set "EC=%ERRORLEVEL%"
if not "%EC%"=="0" (
  echo.
  echo [catalog_nav_bot] Exited with code %EC%
  pause
)
exit /b %EC%
