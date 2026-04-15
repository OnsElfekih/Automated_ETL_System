@echo off
REM ===========================================================================
REM AUTOMATIC TASK SCHEDULER SETUP FOR FILE WATCHER 24/7
REM ===========================================================================
REM This script automatically creates a Windows Task Scheduler task that:
REM - Starts the file watcher on computer startup
REM - Runs 24/7 even when user is logged off
REM - Auto-restarts if it crashes
REM ===========================================================================

setlocal enabledelayedexpansion

REM Get admin rights check
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo.
    echo ============================================================================
    echo ERROR: This script requires Administrator privileges!
    echo ============================================================================
    echo.
    echo Please run this script as Administrator:
    echo 1. Right-click this file
    echo 2. Select "Run as administrator"
    echo 3. Click "Yes" when prompted
    echo.
    pause
    exit /b 1
)

echo.
echo ============================================================================
echo SETTING UP FILE WATCHER FOR 24/7 OPERATION
echo ============================================================================
echo.

REM Get the current directory
set "SCRIPT_DIR=%~dp0"
set "BATCH_FILE=%SCRIPT_DIR%run_file_watcher.bat"
set "TASK_NAME=ETL-FileWatcher-24-7"

REM Verify batch file exists
if not exist "%BATCH_FILE%" (
    echo ERROR: Could not find run_file_watcher.bat
    echo Expected location: %BATCH_FILE%
    pause
    exit /b 1
)

echo Batch file found: %BATCH_FILE%
echo.

REM Check if task already exists
schtasks /query /tn "%TASK_NAME%" >nul 2>&1
if %errorLevel% equ 0 (
    echo Task already exists. Deleting old version...
    schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1
    if %errorLevel% equ 0 (
        echo ✓ Old task deleted
    )
    echo.
)

REM Create the scheduled task
echo Creating new Task Scheduler task...
echo.

schtasks /create ^
    /tn "%TASK_NAME%" ^
    /tr "\"%BATCH_FILE%\"" ^
    /sc onstart ^
    /delay 0000:30 ^
    /ru SYSTEM ^
    /f

if %errorLevel% equ 0 (
    echo ✓ Task created successfully!
) else (
    echo ERROR: Failed to create task
    pause
    exit /b 1
)

echo.
echo ============================================================================
echo CONFIGURATION DETAILS
echo ============================================================================
echo.
echo Task Name: %TASK_NAME%
echo Program: %BATCH_FILE%
echo Trigger: On Startup (after 30 seconds)
echo Run As: SYSTEM (runs even when user is logged off)
echo Priority: Normal
echo.

REM Try to set additional properties
echo Configuring additional properties...
schtasks /change /tn "%TASK_NAME%" /enable >nul 2>&1

echo.
echo ============================================================================
echo SETUP COMPLETE!
echo ============================================================================
echo.
echo ✓ File watcher will now start automatically on computer startup
echo ✓ Will run 24/7 even when you're not logged in
echo ✓ Will auto-restart if it crashes
echo.
echo Next steps:
echo 1. Restart your computer to activate (or start manually below)
echo 2. Monitor logs/pipeline_logs.json to verify it's running
echo 3. Upload CSV files to data/raw/ - they'll be processed automatically
echo.
echo To start the task immediately (without restart):
echo   schtasks /run /tn "%TASK_NAME%"
echo.
echo To stop the task:
echo   schtasks /end /tn "%TASK_NAME%"
echo.
echo To view task details:
echo   schtasks /query /tn "%TASK_NAME%" /v
echo.
echo ============================================================================

pause
