@echo off
setlocal
cd /d "%~dp0"

.venv\Scripts\python.exe tools_test_discord.py
