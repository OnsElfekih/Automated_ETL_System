import os
import time
import json
from pathlib import Path
from datetime import datetime

from ingest import load_data
from profiling import profile_data
from anomaly import detect_anomalies, get_anomaly_insights
from cleaning import clean_rules, repair_detected_anomalies, apply_llm_corrections, apply_fallback_aggressive_cleaning
from validation import validate
from logger import log_event
from train_ml_model import MLModelTrainer


def ensure_ml_model_trained():
    model_path = "models/supervised_anomaly_model.pkl"

    if os.path.exists(model_path):
        return True

    training_path = "data/processed/anomaly_detection.csv"

    if not os.path.exists(training_path):
        print("ML model not found and no training data available.")
        return False

    trainer = MLModelTrainer(model_dir="models/")
    success = trainer.train_model(data_path=training_path, force_retrain=False)

    return success


def run_pipeline(file):
    start_time = time.time()

    Path("data/status").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)

    status_file = "data/status/processing_status.json"

    def update_status(status_dict):
        status_dict["last_update"] = datetime.now().isoformat()

        elapsed = status_dict.get("elapsed_seconds", 0)
        pct = status_dict.get("percent_complete", 0)

        if pct > 0 and elapsed > 0:
            status_dict["estimated_remaining_seconds"] = (elapsed / pct) * (100 - pct)
        else:
            status_dict["estimated_remaining_seconds"] = 0

        with open(status_file, "w", encoding="utf-8") as f:
            json.dump(status_dict, f, indent=2)

    file_name = os.path.basename(file)

    status = {
        "status": "processing",
        "file": file_name,
        "percent_complete": 0,
        "elapsed_seconds": 0,
        "estimated_remaining_seconds": 0,
        "rows_per_second": 0,
        "current_step": "Starting"
    }

    update_status(status)

    print("\nPHASE 0: ML Model Check")
    status["current_step"] = "ML Model Check"
    status["current_tier"] = "Phase 0"
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    ensure_ml_model_trained()
    log_event("pipeline_start", {"file": file})

    print("\nSTEP 1: Data Loading")
    status["current_step"] = "Data Loading"
    status["current_tier"] = "Step 1/8"
    status["percent_complete"] = 5
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    df = load_data(file)
    total_rows = len(df)

    status["total_rows"] = total_rows
    status["percent_complete"] = 10
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    print(f"Loaded {total_rows:,} rows and {len(df.columns)} columns.")

    print("\nSTEP 2: Data Profiling")
    status["current_step"] = "Data Profiling"
    status["current_tier"] = "Step 2/8"
    status["percent_complete"] = 15
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    profile_info = profile_data(df)
    log_event("profiling", profile_info)

    print("\nSTEP 3: Basic Cleaning")
    status["current_step"] = "Basic Cleaning"
    status["current_tier"] = "Step 3/8"
    status["percent_complete"] = 25
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    df = clean_rules(df)

    print("\nSTEP 4: Anomaly Detection")
    status["current_step"] = "Anomaly Detection"
    status["current_tier"] = "Step 4/8"
    status["percent_complete"] = 40
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    df = detect_anomalies(df)

    anomaly_df = df.copy()

    anomaly_path = "data/processed/anomaly_detection.csv"
    anomaly_df.to_csv(anomaly_path, index=False)

    anomaly_insights = get_anomaly_insights(df)
    anomalies_before_cleaning = int((df["anomaly"] == -1).sum())

    status["anomalies_found_before_cleaning"] = anomalies_before_cleaning
    status["percent_complete"] = 50
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    log_event("anomaly_detection", {
        "anomalies_found": anomalies_before_cleaning,
        "insights": anomaly_insights
    })

    print(f"Anomalies detected: {anomalies_before_cleaning:,}")
    print(f"Anomaly file saved: {anomaly_path}")

    print("\nSTEP 5: Rule-based Cleaning")
    status["current_step"] = "Rule-based Cleaning"
    status["current_tier"] = "Step 5/8"
    status["percent_complete"] = 60
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    df, cleaning_report = repair_detected_anomalies(df)
    df, fallback_report = apply_fallback_aggressive_cleaning(df)
    status["rule_based_rows_updated"] = (
    cleaning_report.get("rows_updated", 0)
    + fallback_report.get("rows_updated", 0)
    )
    status["percent_complete"] = 70
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    log_event("rule_based_cleaning", cleaning_report)
    
    print(f"Rows updated by cleaning.py: {cleaning_report.get('rows_updated', 0)}")
    print(f"Rows updated by fallback cleaning: {fallback_report.get('rows_updated', 0)}")    

    print("\nSTEP 6: LLM Correction")
    status["current_step"] = "LLM Correction"
    status["current_tier"] = "Step 6/8"
    status["percent_complete"] = 78
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    llm_report = {"rows_updated": 0, "errors": ["LLM disabled for test"]}

    status["llm_corrections"] = llm_report.get("rows_updated", 0)
    status["percent_complete"] = 84
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    log_event("llm_correction", llm_report)

    print(f"Rows updated by LLM: {llm_report.get('rows_updated', 0)}")

    print("\nSTEP 7: Validation")
    status["current_step"] = "Validation"
    status["current_tier"] = "Step 7/8"
    status["percent_complete"] = 90
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    errors = validate(df)
    log_event("validation", {"errors": errors})

    print("\nSTEP 8: Save Clean Data")
    status["current_step"] = "Saving Clean Data"
    status["current_tier"] = "Step 8/8"
    status["percent_complete"] = 95
    status["elapsed_seconds"] = time.time() - start_time
    update_status(status)

    clean_path = "data/processed/clean_data.csv"

    clean_df = df.copy()
    technical_columns = [
    "has_anomaly",
    "empty_count",
    "anomaly",
    "anomaly_confidence",
    "ml_anomaly_score",
    "clean_action",
    "llm_correction_applied",
    "reject_row",
    "anomaly_types",
    "anomaly_flags",
    "anomaly_severity",
    "anomaly_score",
    "severity_weight",
    "anomaly_method",
    "llm_raw_response"
]

    clean_df = clean_df.drop(
        columns=[col for col in technical_columns if col in clean_df.columns],
        errors="ignore"
    )

    clean_df.to_csv(clean_path, index=False)

    total_time = time.time() - start_time
    rows_per_sec = total_rows / total_time if total_time > 0 else 0

    log_event("pipeline_end", {
        "anomaly_output": anomaly_path,
        "clean_output": clean_path,
        "status": "success"
    })

    status["status"] = "completed"
    status["percent_complete"] = 100
    status["total_time"] = total_time
    status["rows_per_second"] = int(rows_per_sec)
    status["anomaly_output_path"] = anomaly_path
    status["clean_output_path"] = clean_path
    status["elapsed_seconds"] = total_time
    update_status(status)

    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)
    print(f"Rows processed: {total_rows:,}")
    print(f"Anomalies detected: {anomalies_before_cleaning:,}")
    print(f"Rows updated by cleaning.py: {cleaning_report.get('rows_updated', 0)}")
    print(f"Rows updated by LLM: {llm_report.get('rows_updated', 0)}")
    print(f"Anomaly file: {anomaly_path}")
    print(f"Clean file: {clean_path}")
    print(f"Total time: {total_time:.2f}s")
    print("=" * 70)


if __name__ == "__main__":
    print("Run pipeline from dashboard or watcher")