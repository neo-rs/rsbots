@echo off
REM Verify DailyScheduleReminder is running on Oracle server (check PID and status).
REM
REM Usage:
REM   DailyScheduleReminder\verify_oracle.bat

setlocal enabledelayedexpansion

cd /d "%~dp0"
cd /d "%~dp0.."

set "KEY_FILE=oraclekeys\ssh-key-2025-12-15.key"
set "HOST=rsadmin@137.131.14.157"
set "SERVICE=mirror-world-dailyschedulereminder.service"

echo ========================================
echo   Verify DailyScheduleReminder
echo ========================================
echo.

REM Check key file exists
if not exist "%KEY_FILE%" (
    echo [ERROR] SSH key not found: %KEY_FILE%
    pause
    exit /b 1
)

echo [1] Checking service status...
"%WINDIR%\System32\OpenSSH\ssh.exe" -i "%KEY_FILE%" -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ConnectTimeout=60 %HOST% "systemctl is-active %SERVICE% 2>/dev/null || echo inactive"
echo.

echo [2] Getting service PID...
"%WINDIR%\System32\OpenSSH\ssh.exe" -i "%KEY_FILE%" -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ConnectTimeout=60 %HOST% "systemctl show %SERVICE% --property=MainPID --no-pager --value 2>/dev/null || echo 'N/A'"
echo.

echo [3] Checking if process is running...
"%WINDIR%\System32\OpenSSH\ssh.exe" -i "%KEY_FILE%" -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ConnectTimeout=60 %HOST% "PID=$(systemctl show %SERVICE% --property=MainPID --no-pager --value 2>/dev/null); if [ -n \"$PID\" ] && [ \"$PID\" != \"0\" ]; then ps -p \"$PID\" > /dev/null 2>&1 && echo \"Process $PID is running\" || echo \"PID $PID found but process not running\"; else echo \"No PID found\"; fi"
echo.

echo [4] Recent logs (last 10 lines)...
"%WINDIR%\System32\OpenSSH\ssh.exe" -i "%KEY_FILE%" -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ConnectTimeout=60 %HOST% "journalctl -u %SERVICE% -n 10 --no-pager 2>/dev/null || echo 'No logs found'"
echo.

echo ========================================
echo   Verification Complete
echo ========================================
echo.
echo For more details, SSH to server:
echo   ssh -i %KEY_FILE% %HOST%
echo   sudo systemctl status %SERVICE%
echo   sudo journalctl -u %SERVICE% -f
echo.
pause
