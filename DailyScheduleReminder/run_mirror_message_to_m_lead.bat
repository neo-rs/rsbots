@echo off
rem Discord jump link -> copy/paste !m lead line for RS (DailyScheduleReminder user token).
rem
rem Double-click: prompts for message link + destination (same as --interactive).
rem After each run, choose Y to go again or N to exit.
rem Diagnose: run_mirror_message_to_m_lead.bat --diagnose "PASTE_MESSAGE_LINK"
rem Command line: run_mirror_message_to_m_lead.bat "https://..." --dest 1263736465012293754
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
  echo  Mirror link -^> !m lead   ^(interactive^)
  echo  -----------------------------------------
  echo.
  py -3 mirror_message_to_m_lead.py --interactive
) else (
  py -3 mirror_message_to_m_lead.py %*
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
choice /c YN /n /m "Run again?  Y = yes   N = no : "
if errorlevel 2 goto :done
goto :runagain

:done
echo.
echo Press any key to close this window.
pause >nul
exit /b %EXITCODE%
