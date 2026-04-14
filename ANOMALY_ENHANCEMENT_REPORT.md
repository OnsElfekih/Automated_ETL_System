# Anomaly Detection Enhancement Report

## Step 1: Advanced Rule-Based Classifier ✅ COMPLETED

### New File: `src/anomaly_classifier.py`

A sophisticated rule-based anomaly detector that categorizes and flags different types of data quality issues:

#### Detection Categories:
- **Price Issues**: null, negative, zero, extreme high values
- **Quantity Issues**: negative, extremely high values
- **Date Issues**: invalid format, future dates
- **Missing Data**: key columns with missing values
- **Statistical Outliers**: IQR-based detection for numeric columns

#### Output Columns Added to DataFrame:
```
- has_anomaly: Boolean flag (True if any rule violation)
- anomaly_types: String listing detected issues (e.g., "price_null|quantity_negative|")
- anomaly_flags: Count of rule violations per row (0-N)
```

### Example Output:
```
Row 5:
  has_anomaly: True
  anomaly_types: "price_extreme_high|quantity_extreme|"
  anomaly_flags: 2  (two separate violations)
```

---

## Step 2: Enhanced Anomaly Detection ✅ COMPLETED

### Modified File: `src/anomaly.py`

Integrated engineered features with machine learning:

#### Previous Approach:
```
Features: [price, quantity] (2 features)
Algorithm: IsolationForest only
Anomaly detection: -1 or 1
```

#### New Approach (Ensemble):
```
Features: [price, quantity, anomaly_flags] (3 features)
           ↑                       ↑
        Statistical data    Rule-based violations count

Algorithms: 
  - IsolationForest → Unsupervised anomaly score
  - Rule-based checker → Business rule violations
  
Ensemble: Majority vote (Logical OR)
  → If IsolationForest says anomaly OR rules say anomaly → ANOMALY
  → If both say normal → NORMAL

Output Columns:
  - anomaly: Final prediction (-1 or 1)
  - anomaly_confidence: Score 0-2 showing how many signals flagged it
  - anomaly_method: 'ensemble' indicator
```

####Example:
```
Row 5 (Price: 1,200,000, Quantity: 5000):
  Rule-based flags: 2 (price_extreme_high, quantity_extreme)
  IsolationForest: -1 (anomaly)
  Rule-based: -1 (anomaly)
  
  Result:
    - anomaly: -1 (ANOMALY)
    - anomaly_confidence: 2/2 (HIGH CONFIDENCE)
```

---

## Step 3: Enhanced Main Pipeline ✅ COMPLETED

### Modified File: `src/main.py`

Now displays detailed anomaly detection insights:

```
--- anomaly detection (enhanced) ---

📊 Anomaly Detection Results:
   Total anomalies found: 3
   - High confidence (2/2 signals): 2
   - Medium confidence (1/2 signals): 1
```

For each flagged row, logs include:
- Anomaly types detected
- Confidence score (1 or 2)
- Detailed metadata for LLM to fix

---

## Step 4: Enhanced Dashboard ✅ COMPLETED

### Modified File: `src/dashboard.py`

New features:

**📈 Aperçu Tab**: Overview of cleaned data

**🚨 Anomalies Tab**: 
- Bar chart of anomaly types distribution
- Confidence distribution visualization
- Detailed anomaly rows with all metadata

**🔍 Détails Tab**: Column-by-column analysis

**📊 Statistiques Tab**: Descriptive statistics + missing data visualization

**🤖 Corrections LLM**: View last 5 LLM corrections applied

---

## How to Use (Step-by-Step)

### 1. Run the enhanced pipeline:
```powershell
python src/main.py
```

### 2. Check console output:
```
--- anomaly detection (enhanced) ---

📊 Anomaly Detection Results:
   Total anomalies found: 3
   - High confidence (2/2 signals): 2
   - Medium confidence (1/2 signals): 1
```

### 3. View detailed dashboard:
```powershell
streamlit run src/dashboard.py
```

Then navigate to:
- **🚨 Anomalies Tab** → See all detected anomalies with types
- **📊 Statistiques Tab** → See anomaly confidence distribution

---

## Technical Improvements

### Before:
- Single algorithm (IsolationForest)
- Limited anomaly information
- No confidence scoring
- No anomaly categorization

### After:
- **Ensemble approach**: Rule-based + ML voting
- **Rich metadata**: Anomaly types, confidence scores
- **Engineered features**: Business rules as features
- **Better transparency**: Know why each row was flagged
- **LLM-friendly**: Detailed context for correction

---

## Performance Impact

The ensemble approach is more robust because:

```
Scenario 1: Rule-based might miss outliers that violate no rules
  → ML catches them ✅

Scenario 2: ML might flag edge cases
  → Rules provide context for better LLM fixes ✅

Scenario 3: Both signal anomaly
  → confidence = 2 (high confidence for manual review) ✅
```

---

## Files Modified/Created

✅ **New**: `src/anomaly_classifier.py` (190 lines)
✅ **Modified**: `src/anomaly.py` (100 → 80 lines, but much more powerful)
✅ **Modified**: `src/main.py` (added insights logging)
✅ **Modified**: `src/dashboard.py` (expanded from 30 → 120 lines)
✅ **Modified**: `requirements.txt` (added watchdog, APScheduler)

---

## Next Steps (Optional)

### Fine-tuning:
- Adjust contamination parameter in IsolationForest
- Customize thresholds in AnomalyClassifier
- Train RandomForest on historical LLM feedback

### Advanced:
- Add SQL database for persistence
- Implement Phase 1: File Watcher
- Add model retraining pipeline
