import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from anomaly_classifier import AnomalyClassifier
from ml_anomaly_detector import MLAnomalyDetector

def detect_anomalies(df):
    """
    Enhanced anomaly detection combining:
    1. Rule-based classification (AnomalyClassifier)
    2. Statistical/ML detection (IsolationForest)
    3. Weighted ensemble voting
    """
    df = df.copy()

    # STEP 1: Rule-based anomaly classification
    classifier = AnomalyClassifier()
    df = classifier.classify_anomalies(df)
    
    # STEP 2: Extract engineered features from rule violations
    # The anomaly_flags column counts how many rule violations each row has
    rule_based_feature = df['anomaly_flags'].values.reshape(-1, 1)
    
    # STEP 3: Prepare numeric features for IsolationForest
    numeric = df[["price", "quantity"]].apply(
        lambda x: pd.to_numeric(x, errors="coerce")
    ).fillna(0)
    
    # STEP 4: Create combined feature matrix
    # Combine: statistical features + rule-based flags
    combined_features = np.hstack([
        numeric.values,
        rule_based_feature
    ])
    
    # STEP 5: Apply IsolationForest on combined features
    model = IsolationForest(contamination=0.15, random_state=42)
    isolation_pred = model.fit_predict(combined_features)
    
    # STEP 6: Use has_anomaly flag from rule-based detection
    # If has_anomaly is True, it's an anomaly (-1), else normal (1)
    rule_based_pred = np.where(df['has_anomaly'], -1, 1)
    
    # STEP 6.5: ML-based anomaly detection (RandomForest) ⭐ PHASE 2
    ml_detector = MLAnomalyDetector()
    ml_pred = None
    ml_scores = None
    
    if ml_detector.is_loaded:
        ml_pred, ml_scores = ml_detector.predict(df)
        print(f"✅ ML anomaly detector loaded (Phase 2 enabled)")
    else:
        print(f"⚠️  ML anomaly detector not loaded (use train_ml_model.py to train)")
    
    # STEP 7: Ensemble voting with three detection methods
    # Rule 1: If anomaly_types is empty (no rule violations), classify as clean (données propres)
    # Rule 2: Otherwise, use ensemble voting with rule-based, isolation forest, and ML
    
    # Check which rows have no anomaly_types (empty string)
    no_anomaly_types = (df['anomaly_types'] == '')
    
    # Ensemble voting (majority wins)
    # -1 from any source = anomaly
    # Only 1 from all sources = normal
    if ml_pred is not None:
        # 3-way ensemble: rules + isolation forest + RandomForest
        ensemble_pred = np.where(
            (isolation_pred == -1) | (rule_based_pred == -1) | (ml_pred == -1),
            -1,
            1
        )
    else:
        # 2-way ensemble: rules + isolation forest (fallback)
        ensemble_pred = np.where(
            (isolation_pred == -1) | (rule_based_pred == -1),
            -1,
            1
        )
    
    # Apply rule: if no anomaly_types detected, override to clean (1)
    ensemble_pred = np.where(no_anomaly_types, 1, ensemble_pred)
    
    df["anomaly"] = ensemble_pred
    df["anomaly_method"] = "ensemble"  # Track which method flagged it
    
    # Add confidence score (how many signals say it's anomalous)
    # For rows with no anomaly_types, confidence is 0 (données propres)
    if ml_pred is not None:
        # 3-way confidence (0-3 based on agreement of all three methods)
        df["anomaly_confidence"] = np.where(
            no_anomaly_types,
            0,
            (isolation_pred == -1).astype(int) + 
            (rule_based_pred == -1).astype(int) + 
            (ml_pred == -1).astype(int)
        )
        df["ml_anomaly_score"] = np.where(
            ml_scores is not None,
            ml_scores,
            0
        )
    else:
        # 2-way confidence (0-2 based on agreement of rules and isolation forest)
        df["anomaly_confidence"] = np.where(
            no_anomaly_types,
            0,
            (isolation_pred == -1).astype(int) + (rule_based_pred == -1).astype(int)
        )
        df["ml_anomaly_score"] = 0
    
    # SYNC: Ensure has_anomaly is consistent with final anomaly classification
    # If ensemble says it's an anomaly (-1), set has_anomaly to True; else False
    df["has_anomaly"] = (df["anomaly"] == -1)
    
    return df

def get_anomaly_insights(df):
    """
    Return insights about detected anomalies
    """
    anomalies = df[df['anomaly'] == -1]
    
    insights = {
        'total_anomalies': len(anomalies),
        'by_confidence': {
            'high_confidence (2/2 signals)': len(anomalies[anomalies['anomaly_confidence'] == 2]),
            'medium_confidence (1/2 signals)': len(anomalies[anomalies['anomaly_confidence'] == 1]),
        },
        'top_anomaly_types': df[df['has_anomaly']]['anomaly_types'].value_counts().head(5).to_dict(),
    }
    
    return insights