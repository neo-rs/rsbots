@echo off
REM Upload DailyScheduleReminder config.json and config.secrets.json to Oracle server via SSH.
REM
REM Usage:
REM   DailyScheduleReminder\upload_config_oracle.bat
REM
REM Requires:
REM   - oraclekeys\ssh-key-2025-12-15.key (or key from oraclekeys\servers.json)
REM   - DailyScheduleReminder\config.json (will be uploaded)
REM   - DailyScheduleReminder\config.secrets.json (will be uploaded; NOT synced)

setlocal enabledelayedexpansion

cd /d "%~dp0"
cd /d "%~dp0.."

set "KEY_FILE=oraclekeys\ssh-key-2025-12-15.key"
set "HOST=rsadmin@137.131.14.157"
set "REMOTE_ROOT=/home/rsadmin/bots/mirror-world"
set "BOT_DIR=DailyScheduleReminder"

echo ========================================
echo   Upload DailyScheduleReminder Config
echo ========================================
echo.

REM Check key file exists
if not exist "%KEY_FILE%" (
    echo [ERROR] SSH key not found: %KEY_FILE%
    echo Check oraclekeys\servers.json for the correct key filename.
    pause
    exit /b 1
)

REM Check config files exist
if not exist "%BOT_DIR%\config.json" (
    echo [ERROR] config.json not found: %BOT_DIR%\config.json
    pause
    exit /b 1
)

if not exist "%BOT_DIR%\config.secrets.json" (
    echo [WARNING] config.secrets.json not found: %BOT_DIR%\config.secrets.json
    echo This file will NOT be uploaded (create it first if needed).
    echo.
)

REM Upload config.json (synced file)
echo [1/2] Uploading config.json...
"%WINDIR%\System32\OpenSSH\ssh.exe" -i "%KEY_FILE%" -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ConnectTimeout=60 %HOST% "bash -lc 'mkdir -p %REMOTE_ROOT%/%BOT_DIR% && cat > %REMOTE_ROOT%/%BOT_DIR%/config.json'" < "%BOT_DIR%\config.json"
if errorlevel 1 (
    echo [ERROR] Failed to upload config.json
    pause
    exit /b 1
)
echo [OK] config.json uploaded
echo.

REM Upload config.secrets.json (if exists)
if exist "%BOT_DIR%\config.secrets.json" (
    echo [2/2] Uploading config.secrets.json...
    "%WINDIR%\System32\OpenSSH\ssh.exe" -i "%KEY_FILE%" -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ConnectTimeout=60 %HOST% "bash -lc 'mkdir -p %REMOTE_ROOT%/%BOT_DIR% && cat > %REMOTE_ROOT%/%BOT_DIR%/config.secrets.json && chmod 600 %REMOTE_ROOT%/%BOT_DIR%/config.secrets.json'"
    if errorlevel 1 (
        echo [ERROR] Failed to upload config.secrets.json
        pause
        exit /b 1
    )
    echo [OK] config.secrets.json uploaded (permissions set to 600)
    echo.
) else (
    echo [SKIP] config.secrets.json not found (not uploaded)
    echo.
)

echo ========================================
echo   Upload Complete
echo ========================================
echo.
echo Files uploaded to: %REMOTE_ROOT%/%BOT_DIR%/
echo.
echo Next steps:
echo   1. SSH to server and verify files: ssh -i %KEY_FILE% %HOST%
echo   2. Run setup: bash %REMOTE_ROOT%/DailyScheduleReminder/setup_oracle.sh
echo   3. Or restart service: sudo systemctl restart mirror-world-dailyschedulereminder.service
echo.
pause
