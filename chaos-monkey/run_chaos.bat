@echo off
REM run_chaos.bat — Run Chaos Killer Bot locally (Windows)
REM Reads config from project root .env file

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
set "ENV_FILE=%PROJECT_ROOT%\.env"

REM Load .env if it exists
if exist "%ENV_FILE%" (
    echo Loading configuration from: %ENV_FILE%
    for /f "delims==" %%a in ('findstr /r "." "%ENV_FILE%"') do (
        set "%%a"
    )
) else (
    echo WARNING: .env not found at %ENV_FILE%
    echo Using environment variables or defaults
)

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found. Please install Python 3.8+
    exit /b 1
)

REM Install dependencies
echo Installing dependencies...
pip install -q -r "%SCRIPT_DIR%requirements.txt"

REM Run the bot
echo.
echo Starting Chaos Killer Bot...
echo Press Ctrl+C to stop
echo.

python "%SCRIPT_DIR%killer_bot.py"
