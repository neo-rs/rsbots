@echo off
setlocal
cd /d "%~dp0"
title RS In-Store lead flow (interactive)
echo.
echo  RS In-Store lead flow
echo  - paste a breakdown Discord message link
echo  - resolves ID -> URL -> title/price/image via resolver channels
echo  - posts !m lead commands
echo  - edits the breakdown message to fill "Go here to check stock -> <jump link>"
echo.
py -3 "%~dp0rs_instore_lead_flow.py"
set EX=%ERRORLEVEL%
echo.
if not %EX%==0 pause
endlocal
exit /b %EX%

