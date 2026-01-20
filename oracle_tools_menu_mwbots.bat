@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Oracle Tools Menu (MWBots)
REM - Download Oracle snapshot INCLUDING MW bots folders
REM - Baseline check for MW bots (local vs latest snapshot)
REM
REM This is a wrapper around the canonical Python tools in ./scripts:
REM - scripts\download_oracle_snapshot_mwbots.py
REM - scripts\oracle_baseline_check_mwbots.py

cd /d "%~dp0"

REM Pick a Python interpreter (same strategy as oracle_tools_menu.bat)
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
  if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" --version >nul 2>&1
    if not errorlevel 1 (
      set "PY_EXE=.venv\Scripts\python.exe"
      set "PY_ARGS="
    )
  )
)

if not defined PY_EXE (
  echo ERROR: No working Python found.
  echo - Install Python 3.x, OR enable the `py` launcher, OR create .venv.
  exit /b 1
)

set "MW_BOTS=MWDataManagerBot,MWPingBot,MWDiscumBot"
set "DEFAULT_OUT_DIR=%cd%\Oraclserver-files-mwbots"
set "OUT_DIR=%DEFAULT_OUT_DIR%"
set "KEEP_SNAPSHOTS=1"
set "NO_ORACLE_SERVER_DATA=0"
set "SERVER_NAME="

:menu
echo.
echo ============================================================
echo Oracle Tools Menu (MWBots)
echo ------------------------------------------------------------
echo OUT_DIR:         "%OUT_DIR%"
echo KEEP_SNAPSHOTS:  %KEEP_SNAPSHOTS%
echo SERVER_NAME:     "%SERVER_NAME%"
echo NO_ORACLE_DATA:  %NO_ORACLE_SERVER_DATA%
echo BOTS:            %MW_BOTS%
echo ============================================================
echo.
echo [1] MWBots: Download snapshot now
echo [2] MWBots: Baseline check (latest snapshot)
echo [3] MWBots: Baseline check + download first
echo [4] Prune old snapshots only (no download)
echo.
echo [S] Settings
echo [Q] Quit
echo.

choice /c 1234SQ /n /m "Choose: "
set "ERR=%errorlevel%"
if "%ERR%"=="1" goto do_mw_download
if "%ERR%"=="2" goto do_mw_baseline
if "%ERR%"=="3" goto do_mw_baseline_download
if "%ERR%"=="4" goto do_prune
if "%ERR%"=="5" goto settings
if "%ERR%"=="6" goto done
goto menu

:settings
echo.
echo Settings
echo ------------------------------------------------------------
echo [1] Set OUT_DIR
echo [2] Set KEEP_SNAPSHOTS
echo [3] Toggle NO_ORACLE_SERVER_DATA (currently %NO_ORACLE_SERVER_DATA%)
echo [4] Set SERVER_NAME (blank = default from servers.json)
echo [B] Back
echo.
choice /c 1234B /n /m "Choose: "
set "SERR=%errorlevel%"
if "%SERR%"=="1" goto set_out_dir
if "%SERR%"=="2" goto set_keep
if "%SERR%"=="3" goto toggle_no_oracle_data
if "%SERR%"=="4" goto set_server_name
goto menu

:set_out_dir
echo.
set /p OUT_DIR=Enter OUT_DIR (absolute path recommended): 
if not defined OUT_DIR set "OUT_DIR=%DEFAULT_OUT_DIR%"
goto menu

:set_keep
echo.
set /p KEEP_SNAPSHOTS=Enter KEEP_SNAPSHOTS (number, default 1): 
if not defined KEEP_SNAPSHOTS set "KEEP_SNAPSHOTS=1"
goto menu

:toggle_no_oracle_data
if "%NO_ORACLE_SERVER_DATA%"=="1" (set "NO_ORACLE_SERVER_DATA=0") else (set "NO_ORACLE_SERVER_DATA=1")
goto menu

:set_server_name
echo.
set /p SERVER_NAME=Enter SERVER_NAME (exact match in oraclekeys\servers.json, blank for default): 
goto menu

:do_mw_download
echo.
echo Running MWBots download snapshot...
if defined SERVER_NAME (
  "%PY_EXE%" %PY_ARGS% scripts\download_oracle_snapshot_mwbots.py --out-dir "%OUT_DIR%" --keep-snapshots %KEEP_SNAPSHOTS% --server-name "%SERVER_NAME%"
) else (
  "%PY_EXE%" %PY_ARGS% scripts\download_oracle_snapshot_mwbots.py --out-dir "%OUT_DIR%" --keep-snapshots %KEEP_SNAPSHOTS%
)
goto menu

:do_mw_baseline
echo.
echo Running MWBots baseline check (latest snapshot)...
"%PY_EXE%" %PY_ARGS% scripts\oracle_baseline_check_mwbots.py --out-dir "%OUT_DIR%"
goto menu

:do_mw_baseline_download
echo.
echo Running MWBots baseline check + download...
if defined SERVER_NAME (
  "%PY_EXE%" %PY_ARGS% scripts\oracle_baseline_check_mwbots.py --out-dir "%OUT_DIR%" --download --server-name "%SERVER_NAME%"
) else (
  "%PY_EXE%" %PY_ARGS% scripts\oracle_baseline_check_mwbots.py --out-dir "%OUT_DIR%" --download
)
goto menu

:do_prune
echo.
echo Pruning old snapshots only (no download)...
"%PY_EXE%" %PY_ARGS% scripts\download_oracle_snapshot_mwbots.py --out-dir "%OUT_DIR%" --keep-snapshots %KEEP_SNAPSHOTS% --prune-only
goto menu

:done
echo.
echo DONE.
exit /b 0

