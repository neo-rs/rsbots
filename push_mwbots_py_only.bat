@echo off

setlocal enabledelayedexpansion



REM Push MW bots updates to GitHub (MWBots repo).

REM - Runs inside the ./MWBots folder (separate repo).

REM - Assumes MWBots repo has origin pointing to neo-rs/MWBots

REM - Stages safe tracked files plus MWDiscumBot runtime JSON (fetchall_mappings.runtime.json, settings.runtime.json).

REM - Note: git add -u only updates ALREADY TRACKED files. New files must be git add once (see explicit line below).



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

echo === MW BOTS PUSH (safe files + MWDiscumBot runtime JSON) ===

echo Repo: %cd%

echo.



REM Stage allow-listed tracked files, then always stage MWDiscumBot runtime JSON when present.

echo.

echo === STAGE SAFETY (python-only) ===

echo Staging safe tracked files ^(skip secrets/env/playwright caches^) plus MWDiscumBot runtime JSON.

powershell -NoProfile -Command "$ErrorActionPreference='Stop'; $safe=(git ls-files | Where-Object {($_ -match '\.(py|md|json|txt)$') -or ($_ -eq 'requirements.txt')}); $safe=$safe | Where-Object { $_ -notmatch '(^|/)config\.secrets\.json$' -and $_ -notmatch '(^|/)tokens\.env$' -and $_ -notmatch '\.env$' -and $_ -notmatch 'member_history\.json$' -and $_ -notmatch 'playwright_profile/' -and $_ -notmatch '(^|/)\.playwright/' -and $_ -notmatch 'api-token\.env$' -and $_ -notmatch 'mavely_(cookies|refresh_token|auth_token|id_token)\.txt$' }; git reset > $null; if($safe.Count -gt 0){ git add -u -- $safe > $null }; $rt=@('MWDiscumBot/config/fetchall_mappings.runtime.json','MWDiscumBot/config/settings.runtime.json'); foreach($p in $rt){ if(Test-Path -LiteralPath $p){ git add -- $p } }"

if errorlevel 1 (

  echo ERROR: staging safe files failed.

  set EC=1

  goto :pause_exit

)



REM Untracked files are NOT included in "git add -u". Stage explicit new paths here when you add files to the tree.

if exist "Instorebotforwarder\retail_product_link_listener.py" (

  git add -- "Instorebotforwarder/retail_product_link_listener.py"

)



REM If nothing is staged, exit cleanly (reminders still print at :pause_exit)

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

echo DONE.

set EC=0

goto :pause_exit



REM Always shown at end of script (with or without a commit).

:mwbots_push_reminders

echo.

echo === MW BOTS PUSH reminders ===

echo - git add -u only updates files Git already tracks. New .py files: add one explicit git add line in this .bat ^(see Instorebotforwarder example^) or run git add once by hand.

echo - MWDiscumBot config/fetchall_mappings.runtime.json and config/settings.runtime.json are staged when present

echo   ^(tracked in MWBots so deploy matches your tree^). Do not put secrets or tokens in those files.

echo - Git push does not run on Oracle. Deploy: update_mw_bots.bat from mirror-world root, or RSAdminBot mwupdate /mwupdate/botsync.

echo - Optional: fetchall_runtime_mappings_reset_on_startup in settings.json clears fetchall_mappings.runtime.json on bot startup.

exit /b 0



:pause_exit

call :mwbots_push_reminders

echo.

pause

exit /b %EC%

