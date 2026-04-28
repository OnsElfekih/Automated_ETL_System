import os
from datetime import datetime
from ingest import load_data
from profiling import profile_data
from anomaly import detect_anomalies, get_anomaly_insights
from anomaly_classifier import AnomalyClassifier
from cleaning import clean_rules, repair_detected_anomalies, apply_llm_corrections
from validation import validate
from logger import log_event
from llm_fix import fix_row_with_llm
from train_ml_model import MLModelTrainer

def ensure_ml_model_trained():
    """
    Phase 2 Integration: Ensure ML model is trained before pipeline runs.
    Auto-trains model if:
    1. Model doesn't exist, AND
    2. Training data (anomaly_detection.csv) is available
    
    Returns True if model is available (either already trained or just trained)
    """
    model_path = "models/supervised_anomaly_model.pkl"
    
    if os.path.exists(model_path):
        # Model already trained
        return True
    
    # Check if training data exists
    if not os.path.exists("data/processed/anomaly_detection.csv"):
        print("⚠️  ML model not found and no training data available.")
        print("   Hint: Run the notebook 'Phase_2_ML_Model_Training.ipynb' to train the model.")
        return False
    
    # Auto-train model
    print("\n" + "="*70)
    print("🤖 PHASE 2: AUTO-TRAINING ML MODEL")
    print("="*70)
    print("Training data found. Auto-training model...")
    
    trainer = MLModelTrainer(model_dir="models/")
    success = trainer.train_model(data_path="data/processed/anomaly_detection.csv", force_retrain=False)
    
    if success:
        print("✅ ML Model trained and ready for use!")
        print("="*70 + "\n")
        return True
    else:
        print("❌ Failed to train ML model. Continuing without ML detection...")
        print("="*70 + "\n")
        return False

def run_pipeline(file):
    import time
    import json
    from pathlib import Path
    
    start_time = time.time()
    
    # Create status directory
    Path("data/status").mkdir(parents=True, exist_ok=True)
    status_file = "data/status/processing_status.json"
    
    def update_status(status_dict):
        """Update processing status file with estimated time calculation"""
        status_dict['last_update'] = datetime.now().isoformat()
        
        # Calculate estimated remaining time
        elapsed = status_dict.get('elapsed_seconds', 0)
        pct_complete = status_dict.get('percent_complete', 0)
        
        if pct_complete > 0 and elapsed > 0:
            rate = elapsed / pct_complete
            status_dict['estimated_remaining_seconds'] = rate * (100 - pct_complete)
        else:
            status_dict['estimated_remaining_seconds'] = 0
        
        with open(status_file, 'w') as f:
            json.dump(status_dict, f, indent=2)
    
    # Initialize status
    file_name = os.path.basename(file)
    status = {
        'status': 'processing',
        'file': file_name,
        'percent_complete': 0,
        'elapsed_seconds': 0,
        'estimated_remaining_seconds': 0,
        'rows_per_second': 0
    }
    update_status(status)
    
    # PHASE 2: Ensure ML model is trained
    print(f"\n▶️  PHASE 0: ML Model Check")
    status['current_step'] = 'ML Model Check'
    status['current_tier'] = 'Phase 0'
    status['elapsed_seconds'] = time.time() - start_time
    update_status(status)
    ensure_ml_model_trained()
    
    log_event("pipeline_start", {"file": file})
    
    # ===== STEP 1: Data Loading =====
    print(f"\n▶️  STEP 1: Data Loading")
    status['current_step'] = 'Data Loading'
    status['current_tier'] = 'Step 1/7'
    status['percent_complete'] = 5
    status['elapsed_seconds'] = time.time() - start_time
    update_status(status)
    
    step_start = time.time()
    print(f"   Loading data from {file}...")
    df = load_data(file)
    step_time = time.time() - step_start
    
    total_rows = len(df)
    status['total_rows'] = total_rows
    status['percent_complete'] = 10
    status['elapsed_seconds'] = time.time() - start_time
    print(f"   ✓ Loaded {len(df):,} rows, {len(df.columns)} columns ({step_time:.2f}s)")
    update_status(status)
    
    # ===== STEP 2: Profiling =====
    print(f"\n▶️  STEP 2: Data Profiling")
    status['current_step'] = 'Data Profiling'
    status['current_tier'] = 'Step 2/7'
    status['percent_complete'] = 15
    status['elapsed_seconds'] = time.time() - start_time
    update_status(status)
    
    step_start = time.time()
    profile_info = profile_data(df)
    log_event("profiling", profile_info)
    step_time = time.time() - step_start
    status['elapsed_seconds'] = time.time() - start_time
    print(f"   ✓ Profile complete ({step_time:.2f}s)")
    print(f"      Missing values analyzed, data types detected")
    update_status(status)

    # ===== STEP 3: Data Cleaning =====
    print(f"\n▶️  STEP 3: Data Cleaning & Standardization")
    status['current_step'] = 'Data Cleaning'
    status['current_tier'] = 'Step 3/7'
    status['percent_complete'] = 20
    status['elapsed_seconds'] = time.time() - start_time
    update_status(status)
    
    step_start = time.time()
    df = clean_rules(df)
    step_time = time.time() - step_start
    status['elapsed_seconds'] = time.time() - start_time
    print(f"   ✓ Cleaning rules applied ({step_time:.2f}s)")
    update_status(status)

    # ===== STEP 4: Anomaly Detection =====
    print(f"\n▶️  STEP 4: Anomaly Detection (3-Way Ensemble)")
    status['current_step'] = 'Anomaly Detection'
    status['current_tier'] = 'Step 4/7'
    status['percent_complete'] = 30
    status['elapsed_seconds'] = time.time() - start_time
    update_status(status)
    
    df = detect_anomalies(df)
    
    # Get detailed insights about detected anomalies
    anomaly_insights = get_anomaly_insights(df)
    anomalies_count = (df['anomaly'] == -1).sum()
    
    status['anomalies_found'] = int(anomalies_count)
    status['anomaly_percentage'] = (anomalies_count / total_rows * 100) if total_rows > 0 else 0
    status['percent_complete'] = 50
    status['elapsed_seconds'] = time.time() - start_time
    print(f"\n📊 Anomaly Detection Results:")
    print(f"   Total anomalies found: {anomalies_count:,}")
    print(f"   - High confidence (2/2 signals): {anomaly_insights['by_confidence']['high_confidence (2/2 signals)']}")
    print(f"   - Medium confidence (1/2 signals): {anomaly_insights['by_confidence']['medium_confidence (1/2 signals)']}")
    
    log_event("anomaly_detection", {
        "anomalies_found": int(anomalies_count),
        "insights": anomaly_insights
    })
    update_status(status)

    # ===== STEP 5: Rule-based Cleaning =====
    print(f"\n▶️  STEP 5: Rule-based Cleaning of Detected Anomalies")
    status['current_step'] = 'Rule-based Cleaning'
    status['current_tier'] = 'Step 5/8'
    status['percent_complete'] = 60
    status['elapsed_seconds'] = time.time() - start_time
    update_status(status)
    
    step_start = time.time()
    df, cleaning_report = repair_detected_anomalies(df)
    step_time = time.time() - step_start
    status['rule_based_rows_updated'] = cleaning_report.get('rows_updated', 0)
    status['percent_complete'] = 70
    status['elapsed_seconds'] = time.time() - start_time
    print(f"   ✓ Rule-based cleaning applied ({cleaning_report.get('rows_updated', 0)} rows, {step_time:.2f}s)")
    log_event("rule_based_cleaning", cleaning_report)
    update_status(status)

    # ===== STEP 6: LLM Correction (Optional) =====
    print(f"\n▶️  STEP 6: LLM Correction (Optional, Top 2 Remaining Anomalies)")
    status['current_step'] = 'LLM Correction'
    status['current_tier'] = 'Step 6/8'
    status['percent_complete'] = 75
    status['elapsed_seconds'] = time.time() - start_time
    update_status(status)

    step_start = time.time()
    df, llm_report = apply_llm_corrections(df, max_rows=2)
    step_time = time.time() - step_start
    status['llm_corrections'] = llm_report.get('rows_updated', 0)
    status['percent_complete'] = 82
    status['elapsed_seconds'] = time.time() - start_time
    print(f"   ✓ LLM corrections applied ({llm_report.get('rows_updated', 0)} rows, {step_time:.2f}s)")
    log_event("llm_correction", llm_report)
    update_status(status)

    # ===== STEP 7: Validation =====
    print(f"\n▶️  STEP 7: Final Validation")
    status['current_step'] = 'Final Validation'
    status['current_tier'] = 'Step 7/8'
    status['percent_complete'] = 88
    status['elapsed_seconds'] = time.time() - start_time
    update_status(status)
    
    step_start = time.time()
    errors = validate(df)
    log_event("validation", {"errors": errors})
    step_time = time.time() - step_start
    print(f"   ✓ Validation complete ({step_time:.2f}s)")
    update_status(status)

    # ===== STEP 8: Save Results =====
    print(f"\n▶️  STEP 8: Saving Results")
    status['current_step'] = 'Saving Results'
    status['current_tier'] = 'Step 8/8'
    status['percent_complete'] = 95
    update_status(status)
    
    step_start = time.time()
    output_path = "data/processed/anomaly_detection.csv"
    df.to_csv(output_path, index=False)
    step_time = time.time() - step_start
    print(f"   ✓ Saved to {output_path} ({step_time:.2f}s)")
    
    log_event("pipeline_end", {"output": output_path, "status": "success"})
    
    # ===== CLEANUP: Remove processed raw file =====
    if os.path.exists(file):
        try:
            os.remove(file)
            print(f"🗑️  Cleanup: Removed processed raw file: {file}")
            log_event("file_cleanup", {"removed_file": file})
        except Exception as e:
            print(f"⚠️  Warning: Could not remove {file}: {e}")
            log_event("file_cleanup_error", {"file": file, "error": str(e)})
    
    # ===== FINAL SUMMARY =====
    total_time = time.time() - start_time
    rows_per_sec = total_rows / total_time if total_time > 0 else 0
    
    print(f"\n{'='*70}")
    print(f"✅ PIPELINE COMPLETE")
    print(f"{'='*70}")
    print(f"📊 Summary:")
    print(f"   • Rows processed: {total_rows:,}")
    print(f"   • Anomalies found: {anomalies_count:,} ({anomalies_count/total_rows*100:.1f}%)")
    print(f"   • Rule-based rows updated: {cleaning_report.get('rows_updated', 0)}")
    print(f"   • LLM corrections: {llm_report.get('rows_updated', 0)}")
    print(f"   • Total time: {total_time:.2f}s")
    print(f"   • Speed: {rows_per_sec:,.0f} rows/sec")
    print(f"{'='*70}\n")
    
    # Update final status
    status['status'] = 'completed'
    status['percent_complete'] = 100
    status['total_time'] = total_time
    status['rows_per_second'] = int(rows_per_sec)
    status['output_path'] = output_path
    update_status(status)
    
    print("✅ Done!")

if __name__ == "__main__":
    # Test on sales_dirty_2.csv
    run_pipeline("data/raw/dirty_sales_50_rows.csv")