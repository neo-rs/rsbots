@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Batch wrapper is intentionally minimal.
REM The full interactive runner is implemented in PowerShell (reliable quoting + menus).
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%oracle_gui_runner.ps1" %*
pause

