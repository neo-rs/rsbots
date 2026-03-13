@echo off
setlocal
cd /d "%~dp0"

if not exist ".env" (
    echo Creating .env from .env.example...
    copy .env.example .env
    echo.
    echo Edit .env and add your TELNYX_API_KEY, then run this again.
    pause
    exit /b 1
)

py -3 send_sms.py --interactive
