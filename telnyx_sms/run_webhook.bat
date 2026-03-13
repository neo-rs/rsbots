@echo off
cd /d "%~dp0"
echo Inbound webhook server. Set your Telnyx Messaging Profile webhook URL to your public URL.
echo For local testing use: ngrok http 8765
echo.
py -3 webhook_server.py
pause
