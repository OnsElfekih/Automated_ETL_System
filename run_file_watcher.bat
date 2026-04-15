@echo off
REM ===========================================================================
REM FILE WATCHER SERVICE - 24/7 AUTO-RESTART
REM ===========================================================================
REM This batch file runs the file watcher and automatically restarts it
REM if it crashes or stops for any reason.

setlocal enabledelayedexpansion

REM Set window title
title File Watcher Service - Running 24/7

REM Get the directory where this script is located
cd /d "%~dp0"

REM Counter for restart attempts
set restart_count=0
set max_restarts=999

REM Color codes
for /F %%A in ('echo prompt $H^| cmd') do set "BS=%%A"

:loop
set /a restart_count=!restart_count!+1

echo.
echo ============================================================================
echo  FILE WATCHER SERVICE - Attempt !restart_count!
echo ============================================================================
echo  Time: %date% %time%
echo  Status: Running...
echo ============================================================================
echo.

REM Run the file watcher with Python
python src\file_watcher.py

REM If we reach here, the script exited (either normally or due to error)
set exit_code=!errorlevel!

echo.
echo ============================================================================
echo  FILE WATCHER STOPPED
echo ============================================================================
echo  Exit Code: !exit_code!
echo  Time: %date% %time%
echo ============================================================================
echo.

REM Check if we should restart
if !restart_count! lss !max_restarts! (
    echo Restarting in 5 seconds (Attempt !restart_count! of unlimited)...
    timeout /t 5 /nobreak
    goto loop
) else (
    echo Maximum restart attempts reached.
    pause
    exit /b 1
)
