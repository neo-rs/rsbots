@echo off
setlocal
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

cd /d "%~dp0"

py -3 -m pip install -r requirements.txt
py -3 -m playwright install chromium

py -3 amazon_asin_promo_checker.py

endlocal
@echo off
cd /d %~dp0
py -m pip install -r requirements.txt
py -m playwright install chromium
py amazon_asin_promo_checker.py
pause
