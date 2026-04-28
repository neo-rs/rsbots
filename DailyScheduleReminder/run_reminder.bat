@echo off
REM Daily Schedule Reminder Bot Launcher
REM This script launches the Daily Schedule Reminder bot

title Daily Schedule Reminder Bot

cd /d "%~dp0"
cd /d "%~dp0.."

echo ========================================
echo   Daily Schedule Reminder Bot
echo ========================================
echo.

REM Check if venv Python is available
if exist ".venv\Scripts\python.exe" (
    set PYTHON_CMD=.venv\Scripts\python.exe
) else (
    REM Fallback to system Python
    python --version >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] Python is not installed or not in PATH
        echo Please install Python 3.8+ and try again
        pause
        exit /b 1
    )
    set PYTHON_CMD=python
)

echo [INFO] Starting Daily Schedule Reminder Bot...
echo [INFO] Bot will stay on and check every minute for reminders.
echo [INFO] Commands: !reminder ^<#channel^> or !reminder ^<channel_id^>
echo.

REM Run the bot
%PYTHON_CMD% DailyScheduleReminder\reminder_bot.py

REM Check exit code
if errorlevel 1 (
    echo.
    echo [ERROR] Bot exited with an error
    echo.
    pause
    exit /b 1
) else (
    echo.
    echo [INFO] Bot stopped normally
)

pause
