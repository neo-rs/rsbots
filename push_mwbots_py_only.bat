@echo off
setlocal enabledelayedexpansion

REM Push MW bots updates to GitHub (MWBots repo).
REM - Runs inside the ./MWBots folder (separate repo).
REM - Assumes MWBots repo has origin pointing to neo-rs/MWBots
REM - MWBots/.gitignore must exclude secrets + runtime files

cd /d "%~dp0MWBots"
set "EC=0"
REM Basic sanity checks
git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
  echo ERROR: Not inside a git repository. Expected: %~dp0MWBots
  set EC=1
  goto :pause_exit
)

REM Make sure we're on main (best effort; do not fail if already correct)
git branch -M main >nul 2>&1
echo.
echo === MW BOTS PUSH (tracked files) ===
echo Repo: %cd%
echo.

REM Stage all tracked changes (only allow-listed files should be tracked)
echo.
echo === STAGE SAFETY (python-only) ===
echo Staging only safe tracked files (skip secrets/env/playwright caches).
powershell -NoProfile -Command "$ErrorActionPreference='Stop'; $safe=(git ls-files | Where-Object {($_ -match '\.(py|md|json|txt)$') -or ($_ -eq 'requirements.txt')}); $safe=$safe | Where-Object { $_ -notmatch '(^|/)config\.secrets\.json$' -and $_ -notmatch '(^|/)tokens\.env$' -and $_ -notmatch '\.env$' -and $_ -notmatch 'member_history\.json$' -and $_ -notmatch 'playwright_profile/' -and $_ -notmatch '(^|/)\.playwright/' -and $_ -notmatch 'api-token\.env$' -and $_ -notmatch 'mavely_(cookies|refresh_token|auth_token|id_token)\.txt$' }; git reset > $null; if($safe.Count -gt 0){ git add -u -- $safe > $null }"
if errorlevel 1 (
  echo ERROR: staging safe files failed.
  set EC=1
  goto :pause_exit
)

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

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set TS=%%i
echo.
echo Changes staged. Committing...
git commit -m "mwbots py update: %TS%"
if errorlevel 1 (
  echo ERROR: git commit failed.
  echo NOTE: If this is your first commit on this machine, set git user.name/user.email.
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
echo Pushed commit %SHA% to GitHub (origin/main)
echo NOTE: This pushes to GitHub only. To deploy to Oracle: use RSAdminBot /mwupdate or /botsync.
echo.
echo DONE.
set EC=0
goto :pause_exit

:pause_exit
echo.
pause
exit /b %EC%
