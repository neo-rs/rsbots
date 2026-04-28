@echo off
rem Same as run_manual_batch_send.bat option 1 (no JSON menu).
setlocal
cd /d "%~dp0"
call "%~dp0run_manual_batch_send.bat" interactive %*
exit /b %ERRORLEVEL%
