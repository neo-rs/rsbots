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
echo === RS BOTS PY-ONLY PUSH ===
echo Repo: %cd%
echo.

REM Stage all changes (only whitelisted python files should be tracked)
git add -A
if errorlevel 1 (
  echo ERROR: git add failed.
  exit /b 1
)

REM If nothing is staged, exit cleanly
git diff --cached --quiet
if not errorlevel 1 (
  echo No python changes staged. Nothing to push.
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

echo.
echo Pushing to origin/main...
git push
if errorlevel 1 (
  echo ERROR: git push failed.
  exit /b 1
)

echo.
echo DONE.
exit /b 0


