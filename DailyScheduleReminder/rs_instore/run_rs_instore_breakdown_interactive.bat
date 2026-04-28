@echo off
setlocal
cd /d "%~dp0"
title RS In-Store breakdown - message link
echo.
echo  Fetches the message text via the same Discord token chain as mirror_message_to_m_lead.
echo  After preview, you can confirm a real send (one fetch only^).
echo.
py -3 "%~dp0rs_instore_breakdown_sender.py"
set EX=%ERRORLEVEL%
echo.
if not %EX%==0 pause
endlocal
exit /b %EX%
