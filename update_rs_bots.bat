@echo off
setlocal EnableExtensions

cd /d "%~dp0"
set "EC=0"

py -3 --version >nul 2>&1
if errorlevel 1 (
  where python >nul 2>&1
  if errorlevel 0 (
    python scripts\run_oracle_update_bots.py --group rs %*
    set "EC=%ERRORLEVEL%"
    goto :pause_exit
  )
  echo ERROR: `py -3` is not available and `python` was not found.
  set EC=1
  goto :pause_exit
) else (
  py -3 scripts\run_oracle_update_bots.py --group rs %*
  set "EC=%ERRORLEVEL%"
)

:pause_exit
echo.
pause
exit /b %EC%
