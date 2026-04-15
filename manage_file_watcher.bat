@echo off
REM ===========================================================================
REM FILE WATCHER TASK MANAGEMENT
REM ===========================================================================
REM Manage the file watcher scheduled task

setlocal enabledelayedexpansion

:menu
cls
echo.
echo ============================================================================
echo FILE WATCHER 24/7 TASK MANAGEMENT
echo ============================================================================
echo.
echo 1. Start the file watcher task now
echo 2. Stop the file watcher task
echo 3. View task status
echo 4. View task details
echo 5. Disable auto-start (but keep installed)
echo 6. Enable auto-start on startup
echo 7. Uninstall the task completely
echo 8. Exit
echo.
set /p choice="Enter your choice (1-8): "

if "%choice%"=="1" goto start_task
if "%choice%"=="2" goto stop_task
if "%choice%"=="3" goto status_task
if "%choice%"=="4" goto details_task
if "%choice%"=="5" goto disable_auto
if "%choice%"=="6" goto enable_auto
if "%choice%"=="7" goto uninstall_task
if "%choice%"=="8" goto exit_menu

echo Invalid choice. Please try again.
timeout /t 2 >nul
goto menu

:start_task
echo.
echo Starting file watcher task...
schtasks /run /tn "ETL-FileWatcher-24-7"
if %errorLevel% equ 0 (
    echo ✓ Task started successfully!
) else (
    echo ERROR: Failed to start task
    echo Make sure you're running as Administrator
)
timeout /t 3 >nul
goto menu

:stop_task
echo.
echo Stopping file watcher task...
schtasks /end /tn "ETL-FileWatcher-24-7" /f
if %errorLevel% equ 0 (
    echo ✓ Task stopped successfully!
) else (
    echo ERROR: Failed to stop task
)
timeout /t 3 >nul
goto menu

:status_task
echo.
echo Checking task status...
schtasks /query /tn "ETL-FileWatcher-24-7" | findstr "ETL-FileWatcher-24-7"
if %errorLevel% equ 0 (
    echo ✓ Task exists!
) else (
    echo Task not found!
)
timeout /t 3 >nul
goto menu

:details_task
echo.
echo ============================================================================
echo TASK DETAILS
echo ============================================================================
echo.
schtasks /query /tn "ETL-FileWatcher-24-7" /v /fo list
echo.
echo ============================================================================
echo.
timeout /t 5 >nul
goto menu

:disable_auto
echo.
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: This requires Administrator privileges!
    timeout /t 3 >nul
    goto menu
)
echo Disabling auto-start...
schtasks /change /tn "ETL-FileWatcher-24-7" /disable
if %errorLevel% equ 0 (
    echo ✓ Auto-start disabled!
    echo   Task will not start on startup, but you can start it manually
) else (
    echo ERROR: Failed to disable auto-start
)
timeout /t 3 >nul
goto menu

:enable_auto
echo.
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: This requires Administrator privileges!
    timeout /t 3 >nul
    goto menu
)
echo Enabling auto-start...
schtasks /change /tn "ETL-FileWatcher-24-7" /enable
if %errorLevel% equ 0 (
    echo ✓ Auto-start enabled!
    echo   Task will start automatically on computer startup
) else (
    echo ERROR: Failed to enable auto-start
)
timeout /t 3 >nul
goto menu

:uninstall_task
echo.
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: This requires Administrator privileges!
    timeout /t 3 >nul
    goto menu
)
echo.
echo WARNING: This will uninstall the scheduled task!
set /p confirm="Are you sure? (yes/no): "
if /i not "%confirm%"=="yes" goto menu

echo Uninstalling task...
schtasks /delete /tn "ETL-FileWatcher-24-7" /f
if %errorLevel% equ 0 (
    echo ✓ Task uninstalled successfully!
) else (
    echo ERROR: Failed to uninstall task
)
timeout /t 3 >nul
goto menu

:exit_menu
echo.
echo Goodbye!
exit /b 0
