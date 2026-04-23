@echo off
setlocal EnableExtensions
cd /d "%~dp0"

set "PYTHON_EXE=python"
where python >nul 2>nul
if errorlevel 1 (
  set "PATH=%PATH%;%LocalAppData%\Programs\Python\Python313;%LocalAppData%\Programs\Python\Python313\Scripts"
)

echo ==========================================
echo        FB ADS TOOL - CHI PHI ADS
echo ==========================================
echo 1. Nhap token local
echo 2. Test token voi 1 account
echo 3. Fill hom nay
echo 4. Fill hom qua
echo 5. Chay tu dong
echo 6. Thoat
echo ==========================================
set /p CHOICE=Chon so: 

if "%CHOICE%"=="1" goto SET_TOKEN
if "%CHOICE%"=="2" goto VALIDATE
if "%CHOICE%"=="3" goto RUN_TODAY
if "%CHOICE%"=="4" goto RUN_YESTERDAY
if "%CHOICE%"=="5" goto RUN_AUTO
if "%CHOICE%"=="6" goto END

echo Lua chon khong hop le.
goto END

:SET_TOKEN
python fb_ads_tool.py set-token-local
goto END

:VALIDATE
set /p ACCOUNT_ID=Nhap account ID can test: 
python fb_ads_tool.py validate-token --account-id %ACCOUNT_ID%
goto END

:RUN_TODAY
python fb_ads_tool.py run-once --mode today
goto END

:RUN_YESTERDAY
python fb_ads_tool.py run-once --mode yesterday
goto END

:RUN_AUTO
python fb_ads_tool.py run-auto
goto END

:END
echo.
pause
endlocal
