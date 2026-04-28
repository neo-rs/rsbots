@echo off
rem Pick store -> paste start Mirror World link -> send !m to RS -> wait for monitor -> react on MW.
rem Uses the same token chain as mirror_message_to_m_lead / manual_batch_send.
rem
rem Double-click: interactive + random 1-2 min pause between completed forwards (see --delay-random-minutes).
rem Or pass args, e.g.:
rem   run_mirror_forward_queue.bat --store walmart --url "https://discord.com/channels/..." --yes --max 25
rem   --max 0 = unlimited messages (default in mirror_forward_queue.py)
rem   All clearance stores (checkpoint resume each, then next):  --all-stores --yes
rem     (or interactive: run the .bat with no args, enter 0 at the store list)
rem     (see forward_all_stores_order in m_lead_routes.json; --max counts across all stores)
rem Fixed pause instead:  run_mirror_forward_queue.bat --delay 5
rem Optional: --no-wait-confirm  (skip monitor wait; see m_lead_routes post_confirmation)
rem Optional: --no-checkpoint   (no resume file / no mirror_forward_checkpoint.json)
setlocal
cd /d "%~dp0"

py -3 --version >nul 2>&1
if errorlevel 1 (
  echo.
  echo [ERROR] Could not run py -3. Install Python and try again.
  echo.
  pause
  exit /b 1
)

:runagain
if "%~1"=="" (
  echo.
  echo  Mirror forward queue  ^(store + start link -^> RS sends + check reactions^)
  echo  ---------------------------------------------------------------------------
  echo.
  py -3 mirror_forward_queue.py --delay-random-minutes 1-2
) else (
  py -3 mirror_forward_queue.py %*
)
set "EXITCODE=%ERRORLEVEL%"

echo.
echo ----------------------------------------
if %EXITCODE% equ 0 (
  echo Finished OK. Exit code %EXITCODE%.
) else (
  echo Finished with errors. Exit code %EXITCODE%.
)
echo ----------------------------------------
echo.
choice /c YN /n /m "Run again from the start?  Y = yes   N = no : "
if errorlevel 2 goto :done
goto :runagain

:done
echo.
echo Press any key to close this window.
pause >nul
exit /b %EXITCODE%
