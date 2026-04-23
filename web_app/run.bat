@echo off
cd /d "%~dp0"
echo Starting Chi Phi Ads Dashboard...
echo.
echo Installing dependencies...
pip install -q -r requirements.txt
echo.
echo Starting Flask server on http://127.0.0.1:5000
echo Press Ctrl+C to stop.
echo.
python app.py
pause
