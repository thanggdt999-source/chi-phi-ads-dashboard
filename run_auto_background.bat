@echo off
setlocal EnableExtensions
cd /d "%~dp0"

if not exist "storage\logs" mkdir "storage\logs"

set "PATH=%PATH%;%LocalAppData%\Programs\Python\Python313;%LocalAppData%\Programs\Python\Python313\Scripts"

python fb_ads_tool.py run-auto >> "storage\logs\auto_fill.log" 2>&1

endlocal