@echo off
setlocal
set "ENV_FILE=%~dp0.env"

:: 1. Load the project directory from .env
if not exist "%ENV_FILE%" (
    echo Missing .env file. Create one and set PROJECT_DIR.
    pause
    exit /b 1
)

for /f "usebackq tokens=1,* delims==" %%A in ("%ENV_FILE%") do (
    if /i "%%A"=="PROJECT_DIR" set "PROJECT_DIR=%%B"
)

if not defined PROJECT_DIR (
    echo PROJECT_DIR is not set in .env.
    pause
    exit /b 1
)

cd /d "%PROJECT_DIR%"

:: 2. Run streamlit using the python executable inside your venv
".\venv\Scripts\python.exe" -m streamlit run dashboard.py

:: 3. Keep the window open so you can see any errors
pause
