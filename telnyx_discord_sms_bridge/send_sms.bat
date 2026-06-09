@echo off
setlocal EnableExtensions
cd /d "%~dp0"

title Telnyx Send SMS

if not exist ".venv\Scripts\python.exe" (
  echo.
  echo  Virtual environment not found.
  echo  Run setup_windows.bat once, then edit .env with your Telnyx keys.
  echo.
  pause
  exit /b 1
)

if not exist ".env" (
  echo.
  echo  Missing .env — copy from .env.example and fill in your keys.
  echo.
  pause
  exit /b 1
)

set "TO=%~1"
set "TEXT=%~2"
set "FROM=%~3"

if "%TO%"=="" (
  echo.
  echo  === Send SMS via Telnyx ===
  echo  Default sender: +15419202540 ^(local — best for coupons/replies^)
  echo  Alt sender:     +18334882119 ^(toll-free^)
  echo.
  set /p TO="To number (E.164, e.g. +1888222 or +15551234567): "
)

if "%TEXT%"=="" (
  set /p TEXT="Message text: "
)

if "%TO%"=="" (
  echo.
  echo  Cancelled — no destination number.
  pause
  exit /b 1
)

if "%TEXT%"=="" (
  echo.
  echo  Cancelled — no message text.
  pause
  exit /b 1
)

echo.
echo  Sending...
echo  To:   %TO%
echo  Text: %TEXT%
if not "%FROM%"=="" echo  From: %FROM%
echo.

if "%FROM%"=="" (
  .venv\Scripts\python.exe send_sms.py --to "%TO%" --text "%TEXT%"
) else (
  .venv\Scripts\python.exe send_sms.py --to "%TO%" --text "%TEXT%" --from "%FROM%"
)

set "RC=%ERRORLEVEL%"
echo.
if "%RC%"=="0" (
  echo  Done. Check Discord for the outbound log.
  echo  Replies/coupons arrive on the line you sent FROM ^(use +15419202540 for Chipotle^).
) else (
  echo  Send failed. See error above.
)
echo.
pause
exit /b %RC%
