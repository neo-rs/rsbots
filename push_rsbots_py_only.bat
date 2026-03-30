@echo off
setlocal enabledelayedexpansion

REM Push RS bots python-only updates to GitHub.
REM - Assumes this repo is already configured with origin pointing to neo-rs/rsbots
REM - The repo .gitignore enforces python-only tracking for RS bot folders

cd /d "%~dp0"
set "EC=0"

REM Basic sanity checks
git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
  echo ERROR: Not inside a git repository. Run this from the mirror-world repo root.
  set EC=1
  goto :pause_exit
)

REM Make sure we're on main (best effort; do not fail if already correct)
git branch -M main >nul 2>&1

echo.
echo === RS BOTS PUSH (tracked files) ===
echo Repo: %cd%
echo.

REM Stage all changes (only allow-listed files should be tracked)
REM If a previous git command crashed, an index.lock can remain and break future pushes.
del /f /q ".git\index.lock" >nul 2>&1

REM Update tracked files (faster + avoids scanning for new untracked files).
REM NOTE: git add -u does NOT stage brand-new untracked files — add explicit lines below for new modules.
git add -u
REM New bot folders / modules (git add -u skips untracked paths).
git add RSCashoutBot >nul 2>&1
git add RSCheckerbot\rschecker_journal.py >nul 2>&1
git add RSForwarder\mavely_link_resolve.py >nul 2>&1
REM Also stage RSForwarder manual override json (was previously git-ignored).
git add RSForwarder\rs_fs_manual_overrides.json >nul 2>&1
if errorlevel 1 (
  echo ERROR: git add failed.
  set EC=1
  goto :pause_exit
)
REM Never push runtime data (server-owned)
git reset HEAD -- RSCheckerbot/member_history.json 2>nul

REM If nothing is staged, exit cleanly
git diff --cached --quiet
if not errorlevel 1 (
  echo No tracked changes staged. Nothing to push.
  set EC=0
  goto :pause_exit
)

echo.
echo === STAGED CHANGES (what will be committed) ===
echo --- files (name-status)
git --no-pager diff --cached --name-status
echo.
echo --- diffstat
git --no-pager diff --cached --stat
echo.

echo.
echo === CONFIRM ===
echo Review the staged changes above.
choice /C YN /N /M "Proceed to COMMIT + PUSH these staged changes? [Y/N] "
if errorlevel 2 (
  echo.
  echo Aborted. Nothing committed or pushed.
  set EC=0
  goto :pause_exit
)

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set TS=%%i

echo.
echo Changes staged. Committing...
git commit -m "rsbots py update: %TS%"
if errorlevel 1 (
  echo ERROR: git commit failed.
  set EC=1
  goto :pause_exit
)

for /f %%i in ('git rev-parse --short HEAD') do set SHA=%%i
echo.
echo === COMMIT CREATED ===
echo Commit: %SHA%
echo --- commit summary (name-status + stat)
git --no-pager show -1 --name-status --stat --pretty=oneline
echo.

echo.
echo Pushing to origin/main...
git push
if errorlevel 1 (
  echo ERROR: git push failed.
  set EC=1
  goto :pause_exit
)

echo.
echo === PUSH COMPLETE ===
echo Pushed commit: %SHA% to origin/main

echo.
echo DONE.
set EC=0
goto :pause_exit

:pause_exit
echo.
pause
exit /b %EC%

