@echo off
cd /d "%~dp0"
python fb_ads_tool.py set-token-local
if errorlevel 1 goto END
python fb_ads_tool.py run-once --mode today
:END
pause
