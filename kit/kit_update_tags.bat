@echo off
setlocal enabledelayedexpansion

REM Update Kit tags from CSV. Writes results snapshot + update_tag_failed.csv for retries.
cd /d "%~dp0"

echo.
echo   1) Run full (update_tag.csv - all rows)
echo   2) Retry failed only (update_tag_failed.csv from last run)
echo.
set /p choice="Choose 1 or 2: "

if "!choice!"=="2" (
  if not exist "update_tag_failed.csv" (
    echo No update_tag_failed.csv from last run. Run option 1 first.
    echo.
    pause
    exit /b 1
  )
  echo Running retry with update_tag_failed.csv...
  py -3 kit_update_tags.py update_tag_failed.csv --delay 0.5
) else (
  echo Running full with update_tag.csv...
  py -3 kit_update_tags.py --delay 0.5
)

echo.
pause
exit /b %errorlevel%
