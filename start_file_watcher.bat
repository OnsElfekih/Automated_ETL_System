@echo off
REM File Watcher Startup Script for Windows
REM This script starts the file watcher service to automatically process CSV files

setlocal enabledelayedexpansion

echo.
echo ╔════════════════════════════════════════════════════════════════════╗
echo ║              ETL FILE WATCHER - Phase 1 Service                    ║
echo ║                 24/7 Automatic Processing                          ║
echo ╚════════════════════════════════════════════════════════════════════╝
echo.

REM Get the current directory
cd /d "%~dp0"
echo 📂 Working Directory: %CD%
echo.

REM Check if Python is installed
echo ⏳ Checking Python installation...
python --version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ❌ ERROR: Python is not installed or not in PATH
    echo.
    echo 💡 Please install Python from https://www.python.org
    echo    Make sure to check "Add Python to PATH" during installation
    pause
    exit /b 1
)

echo ✅ Python found
echo.

REM Check if we're in the correct directory
if not exist "src\file_watcher.py" (
    echo ❌ ERROR: src\file_watcher.py not found in current directory
    echo.
    echo 📍 Expected: %CD%\src\file_watcher.py
    echo.
    echo 💡 Solution: Run this script from the project root directory
    pause
    exit /b 1
)

echo ✅ Project structure verified
echo.

REM Create data/raw directory if it doesn't exist
if not exist "data\raw" (
    echo 📁 Creating data\raw directory...
    mkdir data\raw
    echo ✅ Created
)

REM Create logs directory if it doesn't exist
if not exist "logs" (
    echo 📁 Creating logs directory...
    mkdir logs
    echo ✅ Created
)

echo.

REM Check for virtual environment
if exist "venv\Scripts\activate.bat" (
    echo ✅ Virtual environment found
    echo 🔄 Activating virtual environment...
    call venv\Scripts\activate.bat
    echo ✅ Activated
) else (
    echo ⚠️  Virtual environment not found
    echo 🔧 Creating virtual environment...
    python -m venv venv
    echo ✅ Created
    echo 🔄 Activating virtual environment...
    call venv\Scripts\activate.bat
    echo ✅ Activated
)

echo.
echo 📦 Installing/updating dependencies...
python -m pip install -q --upgrade pip
pip install -q -r requirements.txt
echo ✅ Dependencies ready
echo.

echo ╔════════════════════════════════════════════════════════════════════╗
echo ║                    STARTING FILE WATCHER                           ║
echo ╚════════════════════════════════════════════════════════════════════╝
echo.
echo 👀 Monitoring: %CD%\data\raw
echo 📦 Archive: %CD%\data\raw\processed_files
echo 📋 Logs: %CD%\logs\pipeline_logs.json
echo.
echo 💡 Instructions:
echo    1. Drop CSV files in data\raw\ directory
echo    2. File Watcher will detect them automatically
echo    3. Full ETL pipeline runs for each file
echo    4. Processed files archived with timestamp
echo    5. Results available in Streamlit dashboard
echo.
echo 🛑 To stop: Press Ctrl+C
echo.

REM Run the file watcher
python src/file_watcher.py

REM If Python exits, pause to show any errors
if %ERRORLEVEL% neq 0 (
    echo.
    echo ❌ ERROR: File watcher exited with error code %ERRORLEVEL%
    echo.
    pause
)

exit /b %ERRORLEVEL%
