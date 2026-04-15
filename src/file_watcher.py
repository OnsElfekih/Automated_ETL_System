"""
File Watcher Module - Phase 1: Automatic CSV Detection & Processing
Monitors data/raw/ directory and automatically triggers the ETL pipeline
when new CSV files are detected.
"""

import os
import time
import shutil
from pathlib import Path
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from main import run_pipeline, ensure_ml_model_trained
from logger import log_event


class CSVFileHandler(FileSystemEventHandler):
    """
    Handles events for CSV file detection in the watched directory.
    """
    
    def __init__(self, archive_dir='data/raw/processed_files'):
        """
        Initialize the handler
        
        Args:
            archive_dir: Directory to archive processed files
        """
        self.archive_dir = archive_dir
        self.processing_files = set()  # Track files being processed
        
        # Create archive directory if it doesn't exist
        Path(self.archive_dir).mkdir(parents=True, exist_ok=True)
    
    def on_created(self, event):
        """Called when a file is created in the watched directory"""
        if event.is_directory:
            return
        
        # Only process CSV files
        if not event.src_path.endswith('.csv'):
            return
        
        filename = os.path.basename(event.src_path)
        
        # Avoid duplicate processing
        if filename in self.processing_files:
            return
        
        print(f"\n🔔 New CSV file detected: {filename}")
        log_event("file_detected", {"filename": filename, "path": event.src_path})
        
        # Wait for file to be fully written (check if size is stable)
        if self._is_file_ready(event.src_path):
            self._process_file(event.src_path)
    
    def _is_file_ready(self, filepath, timeout=5, check_interval=0.5):
        """
        Check if file is fully written by monitoring size stability.
        
        Args:
            filepath: Path to the file
            timeout: Maximum wait time in seconds
            check_interval: Interval between size checks in seconds
            
        Returns:
            True if file size is stable, False if timeout
        """
        print(f"   ⏳ Waiting for file to be fully uploaded...")
        
        start_time = time.time()
        last_size = -1
        stable_count = 0
        required_stable = 3  # Number of checks to confirm stability
        
        while time.time() - start_time < timeout:
            try:
                current_size = os.path.getsize(filepath)
                
                if current_size == last_size and current_size > 0:
                    stable_count += 1
                    if stable_count >= required_stable:
                        print(f"   ✅ File ready ({current_size} bytes)")
                        return True
                else:
                    stable_count = 0
                    last_size = current_size
                
                time.sleep(check_interval)
            except OSError:
                # File might be temporarily locked
                time.sleep(check_interval)
        
        print(f"   ⚠️ File size check timeout - proceeding anyway")
        return True
    
    def _process_file(self, filepath):
        """
        Process the CSV file through the pipeline.
        
        Args:
            filepath: Path to the CSV file to process
        """
        filename = os.path.basename(filepath)
        self.processing_files.add(filename)
        
        try:
            print(f"\n{'='*70}")
            print(f"🚀 PROCESSING: {filename}")
            print(f"⏱️  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*70}\n")
            
            log_event("processing_started", {
                "filename": filename,
                "path": filepath,
                "timestamp": datetime.now().isoformat()
            })
            
            # Run the pipeline
            run_pipeline(filepath)
            
            # Archive the processed file
            self._archive_file(filepath, filename)
            
            print(f"\n{'='*70}")
            print(f"✅ COMPLETED: {filename}")
            print(f"⏱️  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*70}\n")
            
            log_event("processing_completed", {
                "filename": filename,
                "status": "success",
                "timestamp": datetime.now().isoformat()
            })
            
        except Exception as e:
            error_msg = f"Error processing {filename}: {str(e)}"
            print(f"\n{'='*70}")
            print(f"❌ ERROR: {filename}")
            print(f"💥 {error_msg}")
            print(f"{'='*70}\n")
            
            log_event("processing_failed", {
                "filename": filename,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
        
        finally:
            # Remove from processing set
            self.processing_files.discard(filename)
    
    def _archive_file(self, filepath, filename):
        """
        Move processed file to archive directory with timestamp.
        
        Args:
            filepath: Path to the file
            filename: Original filename
        """
        try:
            # Check if file still exists (it might have been moved during processing)
            if not os.path.exists(filepath):
                print(f"   ℹ️  File already moved during processing")
                log_event("file_already_moved", {
                    "filename": filename,
                    "filepath": filepath
                })
                return
            
            # Ensure archive directory exists
            os.makedirs(self.archive_dir, exist_ok=True)
            
            # Create timestamped filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            name, ext = os.path.splitext(filename)
            archived_filename = f"{name}_{timestamp}{ext}"
            archived_path = os.path.join(self.archive_dir, archived_filename)
            
            # Move file to archive with retry logic for locked files
            max_retries = 3
            retry_delay = 0.5
            
            for attempt in range(max_retries):
                try:
                    shutil.move(filepath, archived_path)
                    print(f"   📦 Archived to: {archived_path}")
                    
                    log_event("file_archived", {
                        "original": filename,
                        "archived": archived_filename,
                        "archive_path": archived_path
                    })
                    return
                except PermissionError as pe:
                    if attempt < max_retries - 1:
                        print(f"   ⏳ Retrying file move (attempt {attempt + 1}/{max_retries})...")
                        time.sleep(retry_delay)
                    else:
                        raise
        except Exception as e:
            error_msg = f"Could not move file to archive: {str(e)}"
            print(f"   ❌ ERROR: {error_msg}")
            print(f"      File path: {filepath}")
            print(f"      Archive dir: {self.archive_dir}")
            print(f"      Error type: {type(e).__name__}")
            
            log_event("archive_failed", {
                "filename": filename,
                "filepath": filepath,
                "archive_dir": self.archive_dir,
                "error": str(e),
                "error_type": type(e).__name__
            })
            
            # As a fallback, try to copy instead of move if original move fails
            try:
                if os.path.exists(filepath):
                    print(f"   🔄 Attempting fallback: copying file instead...")
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    name, ext = os.path.splitext(filename)
                    backup_filename = f"{name}_{timestamp}{ext}"
                    backup_path = os.path.join(self.archive_dir, backup_filename)
                    shutil.copy2(filepath, backup_path)
                    os.remove(filepath)  # Delete original after successful copy
                    print(f"   ✅ File backed up successfully via copy method")
                    
                    log_event("file_archived_fallback", {
                        "original": filename,
                        "backup": backup_filename,
                        "backup_path": backup_path
                    })
            except Exception as fallback_error:
                print(f"   ⚠️  Fallback also failed: {str(fallback_error)}")
                log_event("archive_fallback_failed", {
                    "filename": filename,
                    "error": str(fallback_error)
                })


class FileWatcherService:
    """
    Main file watcher service that monitors a directory for new CSV files.
    """
    
    def __init__(self, watch_dir='data/raw', archive_dir='data/raw/processed_files'):
        """
        Initialize the file watcher service.
        
        Args:
            watch_dir: Directory to monitor for new CSV files
            archive_dir: Directory to archive processed files
        """
        self.watch_dir = watch_dir
        self.archive_dir = archive_dir
        self.observer = None
        
        # Create watch directory if it doesn't exist
        Path(self.watch_dir).mkdir(parents=True, exist_ok=True)
        
        print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║                   FILE WATCHER SERVICE - PHASE 1                     ║
╚══════════════════════════════════════════════════════════════════════╝

📂 Watch Directory: {os.path.abspath(self.watch_dir)}
📦 Archive Directory: {os.path.abspath(self.archive_dir)}

🔍 Monitoring for new .csv files...
💡 Tip: Drop CSV files in the watch directory to auto-process them!

""")
    
    def start(self):
        """Start monitoring the directory"""
        # PHASE 2: Ensure ML model is trained before starting file watcher
        ensure_ml_model_trained()
        
        event_handler = CSVFileHandler(self.archive_dir)
        self.observer = Observer()
        self.observer.schedule(event_handler, self.watch_dir, recursive=False)
        self.observer.start()
        
        log_event("file_watcher_started", {
            "watch_dir": os.path.abspath(self.watch_dir),
            "archive_dir": os.path.abspath(self.archive_dir),
            "timestamp": datetime.now().isoformat()
        })
        
        print(f"✅ File watcher service started!")
        print(f"📍 Location: {os.path.abspath(self.watch_dir)}")
        print(f"\n🛑 Press Ctrl+C to stop\n")
    
    def stop(self):
        """Stop monitoring the directory"""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            
            log_event("file_watcher_stopped", {
                "timestamp": datetime.now().isoformat()
            })
            
            print(f"\n✅ File watcher service stopped!")
    
    def run(self):
        """Run the service indefinitely (blocking) with 24/7 robustness"""
        health_check_interval = 30  # Check every 30 seconds
        last_health_check = time.time()
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        try:
            self.start()
            
            print(f"\n⏰ File watcher running in 24/7 mode")
            print(f"   Health checks every {health_check_interval} seconds")
            print(f"   Auto-restart on critical errors\n")
            
            while True:
                try:
                    # Health check: Verify observer is still alive
                    current_time = time.time()
                    if current_time - last_health_check > health_check_interval:
                        if not self._health_check():
                            print(f"\n⚠️  Health check failed - observer may be dead")
                            print(f"   Attempting to restart...\n")
                            
                            consecutive_errors += 1
                            
                            if consecutive_errors > max_consecutive_errors:
                                print(f"\n❌ Too many consecutive errors ({consecutive_errors})")
                                print(f"   Exiting for external restart mechanism\n")
                                log_event("file_watcher_critical_error", {
                                    "reason": "Max consecutive errors exceeded",
                                    "consecutive_errors": consecutive_errors,
                                    "timestamp": datetime.now().isoformat()
                                })
                                break
                            
                            # Try to recover
                            try:
                                self.stop()
                            except:
                                pass
                            
                            time.sleep(2)
                            self.start()
                        else:
                            consecutive_errors = 0  # Reset on successful health check
                        
                        last_health_check = current_time
                    
                    time.sleep(1)
                    
                except Exception as e:
                    error_msg = f"Error during health check: {str(e)}"
                    print(f"\n⚠️  {error_msg}")
                    
                    log_event("file_watcher_error", {
                        "error": error_msg,
                        "error_type": type(e).__name__,
                        "timestamp": datetime.now().isoformat()
                    })
                    
                    # Continue running, just log the error
                    time.sleep(5)
                    
        except KeyboardInterrupt:
            print("\n\n⏹️  Shutting down gracefully...")
            self.stop()
            print("✅ File watcher stopped")
        except Exception as e:
            print(f"\n❌ CRITICAL ERROR: {str(e)}")
            print(f"   Type: {type(e).__name__}")
            print(f"   Exiting for external restart mechanism...\n")
            
            log_event("file_watcher_critical_error", {
                "error": str(e),
                "error_type": type(e).__name__,
                "timestamp": datetime.now().isoformat()
            })
            
            try:
                self.stop()
            except:
                pass
            
            # Exit with error code so batch file can restart
            import sys
            sys.exit(1)
    
    def _health_check(self):
        """
        Check if the observer is still healthy and running.
        
        Returns:
            True if observer is healthy, False otherwise
        """
        try:
            if self.observer is None:
                return False
            
            # Check if observer is still alive
            if not self.observer.is_alive():
                return False
            
            return True
        except:
            return False


# Example usage
if __name__ == "__main__":
    watcher = FileWatcherService(
        watch_dir='data/raw',
        archive_dir='data/raw/processed_files'
    )
    watcher.run()
