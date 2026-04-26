@echo off
setlocal

set "CHROME_EXE="

if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" (
  set "CHROME_EXE=%ProgramFiles%\Google\Chrome\Application\chrome.exe"
)

if exist "%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" (
  set "CHROME_EXE=%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"
)

if exist "%LocalAppData%\Google\Chrome\Application\chrome.exe" (
  set "CHROME_EXE=%LocalAppData%\Google\Chrome\Application\chrome.exe"
)

if "%CHROME_EXE%"=="" (
  echo Could not find Chrome automatically.
  echo Edit this file and set CHROME_EXE to your chrome.exe path.
  pause
  exit /b 1
)

echo Using Chrome:
echo %CHROME_EXE%
echo.
echo Opening Chrome with remote debugging on port 9222...
echo Keep this Chrome window open while running the checker.
echo.

start "" "%CHROME_EXE%" --remote-debugging-port=9222 --user-data-dir="%~dp0target_real_chrome_profile" --no-first-run --no-default-browser-check "https://www.target.com/"

pause
