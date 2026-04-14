from ingest import load_data
from profiling import profile_data
from anomaly import detect_anomalies, get_anomaly_insights
from anomaly_classifier import AnomalyClassifier
from cleaning import clean_rules
from validation import validate
from logger import log_event
from llm_fix import fix_row_with_llm

def run_pipeline(file):
    log_event("pipeline_start", {"file": file})
    print(f"Loading data from {file}...")
    df = load_data(file)
    
    print("--- profiling ---")
    profile_info = profile_data(df)
    log_event("profiling", profile_info)
    print(profile_info)

    print("--- cleaning rules ---")
    df = clean_rules(df)

    print("--- anomaly detection (enhanced) ---")
    df = detect_anomalies(df)
    
    # Get detailed insights about detected anomalies
    anomaly_insights = get_anomaly_insights(df)
    anomalies_count = (df['anomaly'] == -1).sum()
    
    print(f"\n📊 Anomaly Detection Results:")
    print(f"   Total anomalies found: {anomalies_count}")
    print(f"   - High confidence (2/2 signals): {anomaly_insights['by_confidence']['high_confidence (2/2 signals)']}")
    print(f"   - Medium confidence (1/2 signals): {anomaly_insights['by_confidence']['medium_confidence (1/2 signals)']}")
    
    log_event("anomaly_detection", {
        "anomalies_found": int(anomalies_count),
        "insights": anomaly_insights
    })

    print("\n--- anomaly finding & LLM correction ---")
    anomalies = df[df['anomaly'] == -1]
    
    # Processus de correction LLM limité à 2 exemples pour éviter les temps d'attente trop longs
    llm_feedback = []
    
    for idx, row in anomalies.head(2).iterrows():
        try:
            print(f"Correction via LLM pour l'anomalie Ligne {idx}...")
            print(f"   Anomaly types detected: {row['anomaly_types']}")
            print(f"   Anomaly confidence: {row['anomaly_confidence']}/2")
            
            corrected_val = fix_row_with_llm(row)
            
            # Sauvegarde des corrections pour alimenter le dataset ML (Feedback Loop)
            llm_feedback.append({
                "index": idx,
                "original": row.to_dict(),
                "anomaly_types_detected": row['anomaly_types'],
                "confidence": int(row['anomaly_confidence']),
                "llm_correction": corrected_val
            })
            
            log_event("llm_correction", {
                "index": idx,
                "status": "success",
                "anomaly_types": row['anomaly_types'],
                "correction": corrected_val
            })
        except Exception as e:
            print(f"Erreur LLM sur l'index {idx} : {str(e)}")
            log_event("llm_correction", {
                "index": idx,
                "status": "failed",
                "anomaly_types": row['anomaly_types'],
                "error": str(e)
            })

    # Enregistrer le feedback LLM dans un fichier JSON pour futur entraînement
    if llm_feedback:
        import json
        with open("data/processed/llm_feedback.json", "a", encoding="utf-8") as fb:
            for item in llm_feedback:
                fb.write(json.dumps(item) + "\n")

    print("\n--- validation ---")
    errors = validate(df)
    log_event("validation", {"errors": errors})
    print(errors)

    output_path = "data/processed/clean.csv"
    print(f"\nSaving to {output_path}...")
    df.to_csv(output_path, index=False)
    log_event("pipeline_end", {"output": output_path, "status": "success"})
    print("✅ Done!")

if __name__ == "__main__":
    # Test on sales_dirty_2.csv
    run_pipeline("data/raw/dirty_sales_50_rows.csv")