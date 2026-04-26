@echo off
setlocal
cd /d "%~dp0"

echo Installing requirements...
python -m pip install -r requirements.txt

echo.
echo Example:
echo   run_generic.bat https://www.walmart.com/ip/...
echo   run_generic.bat "https://www.bestbuy.com/site/..." --headless
echo   run_generic.bat "https://www.homedepot.com/p/..." --connect-cdp --manual
echo.

set "FLAGS=%*"

if not "%~1"=="" (
  set "URL=%~1"
  shift
  set "FLAGS=%*"
  goto :RUN_ONCE
)

rem Default to strict-retailer-friendly mode: use real Chrome over CDP + manual checkpoint.
rem If the user explicitly passes --headless, do not add CDP/manual flags.
rem If the user explicitly passes --connect-cdp, do not duplicate it (but still add --manual if missing).
set "ALLARGS=%FLAGS%"
set "EXTRA_ARGS=--connect-cdp --manual"

echo %ALLARGS% | findstr /I /C:"--headless" >nul
if %ERRORLEVEL%==0 (
  set "EXTRA_ARGS="
) else (
  echo %ALLARGS% | findstr /I /C:"--connect-cdp" >nul
  if %ERRORLEVEL%==0 (
    set "EXTRA_ARGS=--manual"
  )
)

echo.
echo Enter product URLs one at a time. Type q to quit.

:LOOP
set "URL="
set /p URL=Enter product URL:
if /I "%URL%"=="q" goto :END
if "%URL%"=="" goto :END

echo.
echo Starting Generic Product Checker...
python generic_product_checker.py --url "%URL%" %EXTRA_ARGS% %FLAGS%

echo.
goto :LOOP

:RUN_ONCE
set "ALLARGS=%FLAGS%"
set "EXTRA_ARGS=--connect-cdp --manual"
echo %ALLARGS% | findstr /I /C:"--headless" >nul
if %ERRORLEVEL%==0 (
  set "EXTRA_ARGS="
) else (
  echo %ALLARGS% | findstr /I /C:"--connect-cdp" >nul
  if %ERRORLEVEL%==0 (
    set "EXTRA_ARGS=--manual"
  )
)

echo.
echo Starting Generic Product Checker...
python generic_product_checker.py --url "%URL%" %EXTRA_ARGS% %FLAGS%
echo.
pause

:END
echo Done.
pause

