@echo off
setlocal enabledelayedexpansion

REM Push RS bots python-only updates to GitHub.
REM - Assumes this repo is already configured with origin pointing to neo-rs/rsbots
REM - The repo .gitignore enforces python-only tracking for RS bot folders

cd /d "%~dp0"

REM Basic sanity checks
git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
  echo ERROR: Not inside a git repository. Run this from the mirror-world repo root.
  exit /b 1
)

REM Make sure we're on main (best effort; do not fail if already correct)
git branch -M main >nul 2>&1

echo.
echo === RS BOTS PUSH (tracked files) ===
echo Repo: %cd%
echo.

REM Stage all changes (only allow-listed files should be tracked)
git add -A
if errorlevel 1 (
  echo ERROR: git add failed.
  exit /b 1
)

REM If nothing is staged, exit cleanly
git diff --cached --quiet
if not errorlevel 1 (
  echo No tracked changes staged. Nothing to push.
  exit /b 0
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
  exit /b 0
)

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set TS=%%i

echo.
echo Changes staged. Committing...
git commit -m "rsbots py update: %TS%"
if errorlevel 1 (
  echo ERROR: git commit failed.
  exit /b 1
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
  exit /b 1
)

echo.
echo === PUSH COMPLETE ===
echo Pushed commit: %SHA% to origin/main

echo.
echo DONE.
exit /b 0


