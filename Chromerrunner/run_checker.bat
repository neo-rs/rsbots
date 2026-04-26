@echo off
setlocal
cd /d "%~dp0"

echo Installing requirements...
python -m pip install -r requirements.txt

echo Starting Target Checker V4...
python target_checker_v4_network.py

pause
