@echo off
cd /d "%~dp0"
py -3 -m pip install -r requirements.txt -q
py -3 -u relay_bot.py
pause
