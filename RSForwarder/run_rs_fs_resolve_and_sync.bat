@echo off
setlocal enabledelayedexpansion

title RSForwarder RS-FS Tools

set "REPO=%~dp0.."
pushd "%REPO%" >nul 2>&1

:menu
cls
echo ==============================================================================
echo RSForwarder - RS-FS Tools
echo ==============================================================================
echo Repo root: %CD%
echo.
echo Choose an action:
echo   [1] Resolve Current List (History -^> monitor_data -^> optional affiliate) + optional Live sync
echo   [2] Prune Live List incomplete rows (title/url/affiliate missing)
echo   [3] Run BOTH: Resolve/Sync then Prune Live
echo   [4] Exit
echo.
set "CH="
set /p "CH=Select 1-4: "
if "%CH%"=="4" goto :done
if "%CH%"=="2" goto :do_prune
if "%CH%"=="3" goto :do_both
if "%CH%"=="1" goto :do_resolve
echo.
echo Unknown selection: %CH%
pause
goto :menu

:do_resolve
call :_run_resolve
goto :menu

:_run_resolve
echo.
echo ==============================================================================
echo RSForwarder - RS-FS Resolve + Sync
echo ==============================================================================
echo.

set "APPLY="
set /p "APPLY=Write changes to Google Sheet? (y/n, default n): "
if /i "!APPLY!"=="y" (
  set "APPLY_FLAG=--apply"
) else (
  set "APPLY_FLAG="
)

set "SYNC="
set /p "SYNC=Also sync Full Send Current -> Live? (y/n, default y): "
if "!SYNC!"=="" set "SYNC=y"
if /i "!SYNC!"=="y" (
  set "SYNC_FLAG=--sync-live"
) else (
  set "SYNC_FLAG="
)

set "SHOWALL="
set /p "SHOWALL=Show ALL resolved rows + per-row miss reasons? (y/n, default n): "
if /i "!SHOWALL!"=="y" (
  set "SHOWALL_FLAG=--show-all"
) else (
  set "SHOWALL_FLAG="
)

set "FILL_AFF="
set /p "FILL_AFF=Fill Affiliate URL (may be slower; uses affiliate rewrite)? (y/n, default n): "
if /i "!FILL_AFF!"=="y" (
  set "FILL_AFF_FLAG=--fill-affiliate"
) else (
  set "FILL_AFF_FLAG="
)

set "HIST_OFF="
set /p "HIST_OFF=Disable History-first fills? (y/n, default n): "
if /i "!HIST_OFF!"=="y" (
  set "HIST_FLAG=--no-history"
) else (
  set "HIST_FLAG="
)

set "HIST_UPSERT_OFF="
set /p "HIST_UPSERT_OFF=Disable History upsert on apply? (y/n, default n): "
if /i "!HIST_UPSERT_OFF!"=="y" (
  set "HIST_UPSERT_FLAG=--no-history-upsert"
) else (
  set "HIST_UPSERT_FLAG="
)

set "JSON_OUT="
set /p "JSON_OUT=Optional JSON report filename (blank = skip, saved under RSForwarder/): "
if not "!JSON_OUT!"=="" (
  set "JSON_OUT_FLAG=--json-out"
) else (
  set "JSON_OUT_FLAG="
)

echo.
echo Running:
if not "!JSON_OUT!"=="" (
  echo py -3 -m RSForwarder.run_rs_fs_resolve_and_sync !APPLY_FLAG! !SYNC_FLAG! !SHOWALL_FLAG! !FILL_AFF_FLAG! !HIST_FLAG! !HIST_UPSERT_FLAG! !JSON_OUT_FLAG! "!JSON_OUT!"
  echo.
  py -3 -m RSForwarder.run_rs_fs_resolve_and_sync !APPLY_FLAG! !SYNC_FLAG! !SHOWALL_FLAG! !FILL_AFF_FLAG! !HIST_FLAG! !HIST_UPSERT_FLAG! !JSON_OUT_FLAG! "!JSON_OUT!"
) else (
  echo py -3 -m RSForwarder.run_rs_fs_resolve_and_sync !APPLY_FLAG! !SYNC_FLAG! !SHOWALL_FLAG! !FILL_AFF_FLAG! !HIST_FLAG! !HIST_UPSERT_FLAG!
  echo.
  py -3 -m RSForwarder.run_rs_fs_resolve_and_sync !APPLY_FLAG! !SYNC_FLAG! !SHOWALL_FLAG! !FILL_AFF_FLAG! !HIST_FLAG! !HIST_UPSERT_FLAG!
)
set "EC=%ERRORLEVEL%"
echo.
if not "!EC!"=="0" (
  echo ERROR: exited with code !EC!
  pause
  exit /b !EC!
)
echo Done.
pause
exit /b 0

:do_prune
call :_run_prune
goto :menu

:_run_prune
echo.
echo ==============================================================================
echo RSForwarder - Live List prune (incomplete rows)
echo ==============================================================================
echo.

set "P_APPLY="
set /p "P_APPLY=Apply deletes now? (y/n, default n): "
if /i "!P_APPLY!"=="y" (
  set "P_APPLY_FLAG=--apply"
) else (
  set "P_APPLY_FLAG="
)

set "P_MAXLOG="
set /p "P_MAXLOG=Max rows to print (default 50): "
if "!P_MAXLOG!"=="" set "P_MAXLOG=50"

echo.
echo Running:
echo py -3 RSForwarder\prune_rs_fs_live_incomplete.py !P_APPLY_FLAG! --max-log !P_MAXLOG!
echo.
py -3 RSForwarder\prune_rs_fs_live_incomplete.py !P_APPLY_FLAG! --max-log !P_MAXLOG!
echo.
pause
exit /b 0

:do_both
echo.
echo ==============================================================================
echo RSForwarder - Run BOTH
echo ==============================================================================
echo.
call :_run_resolve
set "EC=%ERRORLEVEL%"
if not "!EC!"=="0" (
  echo.
  echo Resolve step failed (exit !EC!). Skipping prune.
  pause
  goto :menu
)
echo.
echo Resolve finished. Starting prune step next...
call :_run_prune
goto :menu

:done
popd >nul 2>&1
exit /b 0

