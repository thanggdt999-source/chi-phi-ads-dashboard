@echo off
cd /d "%~dp0"
python fb_ads_tool.py run-once --mode yesterday
pause
