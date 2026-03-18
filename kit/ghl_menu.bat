@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM GHL (GoHighLevel) Menu - run from kit folder, repo root is parent
REM - Export all contacts to CSV
REM - Export invalid emails only (billing audit)
REM Add new GHL functions here as new menu options.
REM
REM Scripts: kit\ghl_export_contacts.py (and future scripts in kit)
REM Credentials: set GHL_BEARER_TOKEN (Private Integration Bearer) before or in Settings.

set "KIT_DIR=%~dp0"
for %%I in ("%KIT_DIR%..") do set "ROOT=%%~fI"
cd /d "%ROOT%"

REM Load bearer token from kit\ghl_token.txt if not already set (file is gitignored)
set "GHL_TOKEN_SOURCE="
if exist "%KIT_DIR%ghl_token.txt" if not defined GHL_BEARER_TOKEN (
  set "GHL_TOKEN_SOURCE=file"
  for /f "usebackq delims=" %%a in ("%KIT_DIR%ghl_token.txt") do set "GHL_BEARER_TOKEN=%%a"
)

REM Pick a Python interpreter (same strategy as oracle_tools_menu_mwbots.bat)
set "PY_EXE="
set "PY_ARGS="

python --version >nul 2>&1
if not errorlevel 1 (
  for /f "tokens=1,* delims= " %%a in ('python --version 2^>^&1') do (
    if /i "%%a"=="Python" (
      set "PY_EXE=python"
      set "PY_ARGS="
    )
  )
)

if not defined PY_EXE (
  py -3 --version >nul 2>&1
  if not errorlevel 1 (
    set "PY_EXE=py"
    set "PY_ARGS=-3"
  )
)

if not defined PY_EXE (
  if exist "%ROOT%\.venv\Scripts\python.exe" (
    "%ROOT%\.venv\Scripts\python.exe" --version >nul 2>&1
    if not errorlevel 1 (
      set "PY_EXE=%ROOT%\.venv\Scripts\python.exe"
      set "PY_ARGS="
    )
  )
)

if not defined PY_EXE (
  echo ERROR: No working Python found.
  echo - Install Python 3.x, OR enable the `py` launcher, OR create .venv in repo root.
  exit /b 1
)

:menu
echo.
echo ============================================================
echo GHL (GoHighLevel) Menu  -  kit
echo ------------------------------------------------------------
if defined GHL_BEARER_TOKEN (
  if "%GHL_TOKEN_SOURCE%"=="file" (echo GHL_BEARER_TOKEN:  set ^(from kit\ghl_token.txt^)) else (echo GHL_BEARER_TOKEN:  set ^(session^))
) else (
  echo GHL_BEARER_TOKEN:  not set - add kit\ghl_token.txt or use [S] Settings
)
echo ============================================================
echo.
echo [1] Export all contacts to CSV
echo [2] Export invalid emails only to CSV ^(billing audit^)
echo [3] Export invalid + verify + DELETE ^(loop: CSV -^> verify -^> delete -^> repeat^)
echo [4] Export invalid + DRY RUN ^(same as 3 but do NOT delete - safe to test^)
echo.
echo [S] Settings ^(set GHL_BEARER_TOKEN for this session^)
echo [Q] Quit
echo.

choice /c 1234SQ /n /m "Choose: "
set "ERR=%errorlevel%"
if "%ERR%"=="1" goto opt1
if "%ERR%"=="2" goto opt2
if "%ERR%"=="3" goto opt3
if "%ERR%"=="4" goto opt4
if "%ERR%"=="5" goto optS
if "%ERR%"=="6" goto done
goto menu

:optS
echo.
set /p GHL_BEARER_TOKEN=Enter GHL_BEARER_TOKEN (Private Integration Bearer, pit-...): 
set "GHL_TOKEN_SOURCE=session"
goto menu

:opt1
echo.
echo Exporting all GHL contacts to CSV...
if not defined GHL_BEARER_TOKEN (
  echo ERROR: Set GHL_BEARER_TOKEN first ^(use [S] Settings or set in environment^).
  pause
  goto menu
)
"%PY_EXE%" %PY_ARGS% "%KIT_DIR%ghl_export_contacts.py"
if errorlevel 1 pause
goto menu

:opt2
echo.
echo Exporting invalid-email contacts only...
if not defined GHL_BEARER_TOKEN (
  echo ERROR: Set GHL_BEARER_TOKEN first ^(use [S] Settings or set in environment^).
  pause
  goto menu
)
"%PY_EXE%" %PY_ARGS% "%KIT_DIR%ghl_export_contacts.py" --invalid-only -o "%KIT_DIR%ghl_invalid_emails.csv"
if errorlevel 1 pause
goto menu

:opt3
echo.
echo Export invalid emails and DELETE them from GHL ^(fetch 10k -^> CSV -^> delete -^> repeat^)...
if not defined GHL_BEARER_TOKEN (
  echo ERROR: Set GHL_BEARER_TOKEN first ^(use [S] Settings or set in environment^).
  pause
  goto menu
)
"%PY_EXE%" %PY_ARGS% "%KIT_DIR%ghl_export_contacts.py" --invalid-only -o "%KIT_DIR%ghl_invalid_emails.csv" --export-and-delete-invalid
if errorlevel 1 pause
goto menu

:opt4
echo.
echo Export invalid emails + DRY RUN ^(no delete from GHL^)...
if not defined GHL_BEARER_TOKEN (
  echo ERROR: Set GHL_BEARER_TOKEN first ^(use [S] Settings or set in environment^).
  pause
  goto menu
)
"%PY_EXE%" %PY_ARGS% "%KIT_DIR%ghl_export_contacts.py" --invalid-only -o "%KIT_DIR%ghl_invalid_emails.csv" --export-and-delete-invalid --dry-run
if errorlevel 1 pause
goto menu

:done
echo.
echo DONE.
exit /b 0
