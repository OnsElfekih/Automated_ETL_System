# 🚀 Quick Setup: File Watcher 24/7 Auto-Start

## One-Time Setup (5 minutes)

### Step 1: Run the Setup Script
1. **Right-click** `setup_file_watcher_auto.bat`
2. Select **"Run as administrator"**
3. Click **"Yes"** when prompted
4. Wait for it to complete - should see:
   ```
   ✓ Task created successfully!
   ✓ Setup Complete!
   ```

### Step 2: Verify It Works (Choose One)

**Option A: Restart your computer**
- File watcher will start automatically
- Check `logs/pipeline_logs.json` to verify

**Option B: Start it immediately without restart**
```bash
# Run this command in PowerShell (as Administrator):
schtasks /run /tn "ETL-FileWatcher-24-7"
```

## That's it! 🎉

The file watcher is now:
- ✅ Running 24/7
- ✅ Auto-starts on computer startup
- ✅ Auto-restarts if it crashes
- ✅ Runs even when you're logged off

---

## How to Use

### Upload Files Automatically Process
1. Open Streamlit dashboard: `streamlit run src/dashboard.py`
2. Upload a CSV file
3. The file watcher automatically processes it
4. Check `data/processed/anomaly_detection.csv` for results

### Monitor File Processing
Check the logs:
```bash
# View live logs
Get-Content logs/pipeline_logs.json -Wait

# Or just check recent events
Get-Content logs/pipeline_logs.json | Select-Object -Last 20
```

---

## Management Commands

### View Status
```bash
# See if task is running
schtasks /query /tn "ETL-FileWatcher-24-7"
```

### Stop the Task
```bash
# Stop it temporarily
schtasks /end /tn "ETL-FileWatcher-24-7" /f

# Or use the management menu
manage_file_watcher.bat
```

### Start the Task
```bash
# Start it manually
schtasks /run /tn "ETL-FileWatcher-24-7"
```

### Disable Auto-Start
```bash
# Keep installed but don't auto-start
schtasks /change /tn "ETL-FileWatcher-24-7" /disable
```

### Re-enable Auto-Start
```bash
# Go back to auto-starting on startup
schtasks /change /tn "ETL-FileWatcher-24-7" /enable
```

### Uninstall Completely
```bash
# Remove the task entirely
schtasks /delete /tn "ETL-FileWatcher-24-7" /f
```

---

## Easy Management Tool

**Use this for a menu-driven interface:**
```bash
manage_file_watcher.bat
```

Shows a menu to:
- Start/Stop the task
- View status
- Enable/Disable auto-start
- Uninstall

---

## Troubleshooting

### Task not starting?
1. Open Task Scheduler (Win + R → `taskschd.msc`)
2. Find "ETL-FileWatcher-24-7" in the list
3. Right-click → "Run"
4. Check if it starts

### Still not working?
```bash
# Re-run the setup as Administrator
Right-click setup_file_watcher_auto.bat → Run as administrator
```

### Check the logs
```bash
# See what's happening
Get-Content logs/pipeline_logs.json -Tail 30

# Search for errors
Select-String "ERROR|CRITICAL" logs/pipeline_logs.json
```

---

## Files Involved

| File | Purpose |
|------|---------|
| `setup_file_watcher_auto.bat` | One-time setup (creates the task) |
| `manage_file_watcher.bat` | Management menu (start/stop/etc) |
| `run_file_watcher.bat` | The actual auto-restart wrapper |
| `src/file_watcher.py` | The file watcher service |

---

## What Happens After Setup?

✅ Computer starts → File watcher auto-starts after 30 seconds  
✅ File watcher crashes → Batch file auto-restarts it  
✅ You upload a file → Automatically processed  
✅ Results saved → To `data/processed/anomaly_detection.csv`  

**Zero manual intervention needed!** 🎯
