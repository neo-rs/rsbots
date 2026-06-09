@echo off
setlocal
cd /d "%~dp0"

set "KEY=C:\Users\apaap\OneDrive\Desktop\mirror-world\oracleserverkeys\ssh-key-2025-12-15.key"
set "HOST=rsadmin@137.131.14.157"
set "REMOTE=/home/rsadmin/bots/mirror-world/telnyx_discord_sms_bridge/.env"

echo Uploading local .env to Oracle (secrets only, not git)...
"%WINDIR%\System32\OpenSSH\scp.exe" -i "%KEY%" -o StrictHostKeyChecking=no ".env" %HOST%:%REMOTE%
if errorlevel 1 (
  echo SCP failed.
  exit /b 1
)

echo Restarting bridge service...
"%WINDIR%\System32\OpenSSH\ssh.exe" -i "%KEY%" -o StrictHostKeyChecking=no %HOST% "bash -lc 'sudo systemctl restart mirror-world-telnyx-discord-sms-bridge.service; sleep 2; systemctl is-active mirror-world-telnyx-discord-sms-bridge.service; curl -s http://127.0.0.1:8787/health'"
pause
