@echo off
cd /d "%~dp0"
py -3 check_status.py %*
pause
