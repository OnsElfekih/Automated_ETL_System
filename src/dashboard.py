import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime

st.set_page_config(page_title="ETL Quality Dashboard", layout="wide")
st.title("📊 ETL & Data Quality Dashboard")

st.sidebar.header("⚙️ Options")

# ===== CSV UPLOAD SECTION =====
st.sidebar.subheader("📤 Upload CSV for Processing")
uploaded_file = st.sidebar.file_uploader(
    "Sélectionnez un fichier CSV",
    type=['csv'],
    help="Upload a CSV file - it will be automatically processed by the file watcher"
)

if uploaded_file is not None:
    # Create data/raw directory if it doesn't exist
    os.makedirs('data/raw', exist_ok=True)
    
    # Save uploaded file to data/raw/
    file_path = os.path.join('data/raw', uploaded_file.name)
    
    try:
        with open(file_path, 'wb') as f:
            f.write(uploaded_file.getbuffer())
        
        # Show success message with instructions
        st.sidebar.success(f"""
✅ **File uploaded successfully!**

📁 Saved to: `data/raw/{uploaded_file.name}`

🔍 **File Watcher Status:**
- If file watcher is running, processing will start automatically
- Check the file watcher terminal for real-time processing
- Processing typically takes 10-30 seconds

💡 **Next Steps:**
1. Check the file watcher terminal for status
2. Wait for "✅ COMPLETED" message
3. Refresh this dashboard to see results
4. Check "✅ Données Propres" tab for clean data
        """)
        
        # Log the upload
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "event": "csv_uploaded_via_ui",
            "filename": uploaded_file.name,
            "file_size": uploaded_file.size,
            "upload_path": file_path
        }
        
        # Append to pipeline logs
        os.makedirs('logs', exist_ok=True)
        with open('logs/pipeline_logs.json', 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry) + '\n')
        
    except Exception as e:
        st.sidebar.error(f"❌ Error uploading file: {str(e)}")

# Separator
st.sidebar.divider()

# ===== DATA PATH & RELOAD SECTION =====
st.sidebar.subheader("📂 Data Path")
data_path = st.sidebar.text_input("Chemin du fichier propre", "data/processed/clean.csv")

if st.sidebar.button("🔄 Recharger les données"):
    st.rerun()

st.sidebar.divider()

# ===== PROCESSING STATUS INFO =====
st.sidebar.subheader("⏳ Processing Status")

# Check for files in data/raw (being processed)
if os.path.exists('data/raw'):
    raw_files = [f for f in os.listdir('data/raw') if f.endswith('.csv')]
    if raw_files:
        st.sidebar.warning(f"""
🔄 **Processing in Progress**

Files detected in watch directory:
""")
        for f in raw_files[:5]:  # Show first 5
            st.sidebar.markdown(f"• `{f}`")
        
        st.sidebar.info("""
⏱️ Processing typically takes:
- Small files (< 100 rows): 10-20 seconds
- Medium files (100-500 rows): 20-40 seconds
- Large files (500+ rows): 1-2 minutes

💡 Check file watcher terminal for real-time progress!
        """)

# Check for processed files
if os.path.exists('data/raw/processed_files'):
    processed_files = [f for f in os.listdir('data/raw/processed_files') if f.endswith('.csv')]
    if processed_files:
        st.sidebar.success(f"✅ **{len(processed_files)} files processed**")
        
        # Show last processed file
        latest_file = sorted(processed_files)[-1]
        st.sidebar.caption(f"Last: {latest_file}")
        
        # Show processing log snippet
        try:
            with open('logs/pipeline_logs.json', 'r') as f:
                lines = f.readlines()
                if lines:
                    last_event = json.loads(lines[-1])
                    if last_event.get('event') == 'processing_completed':
                        st.sidebar.success(f"✅ Last: {last_event.get('filename', 'N/A')}")
        except:
            pass

st.sidebar.divider()

# ===== STATUS INFO =====
st.sidebar.subheader("📊 Status")
if os.path.exists('data/raw/processed_files'):
    processed_count = len([f for f in os.listdir('data/raw/processed_files') if f.endswith('.csv')])
    st.sidebar.metric("Files Processed", processed_count)

# Check file watcher status
if os.path.exists('logs/pipeline_logs.json'):
    try:
        with open('logs/pipeline_logs.json', 'r') as f:
            lines = f.readlines()
            if lines:
                last_event = json.loads(lines[-1])
                st.sidebar.info(f"Last Event: {last_event.get('event', 'N/A')}")
    except:
        pass

if os.path.exists(data_path):
    df = pd.read_csv(data_path)
    
    # Display key metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("📊 Lignes", len(df))
    col2.metric("📋 Colonnes", len(df.columns))
    if 'anomaly' in df.columns:
        anomalies = (df['anomaly'] == -1).sum()
        col3.metric("⚠️ Anomalies", anomalies)
        col4.metric("✅ Normal", len(df) - anomalies)

    # Tabs for different views
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 Aperçu", "🚨 Anomalies", "✅ Données Propres", "🔍 Détails", "📊 Statistiques"])
    
    with tab1:
        st.subheader("Aperçu des données nettoyées")
        st.dataframe(df.head(20), use_container_width=True)
    
    with tab2:
        st.subheader("Anomalies Détectées")
        if 'anomaly' in df.columns:
            anomaly_rows = df[df['anomaly'] == -1]
            
            if len(anomaly_rows) > 0:
                # Show anomaly types distribution
                if 'anomaly_types' in df.columns:
                    st.write("**Types d'anomalies détectées:**")
                    anomaly_type_counts = {}
                    for types_str in anomaly_rows['anomaly_types']:
                        for atype in str(types_str).split('|'):
                            if atype:
                                anomaly_type_counts[atype] = anomaly_type_counts.get(atype, 0) + 1
                    
                    st.bar_chart(pd.Series(anomaly_type_counts).sort_values(ascending=False))
                
                # Show confidence distribution
                if 'anomaly_confidence' in df.columns:
                    st.write("**Confiance des anomalies détectées:**")
                    confidence_dist = anomaly_rows['anomaly_confidence'].value_counts().sort_index()
                    st.bar_chart(confidence_dist)
                
                # Display anomalous rows
                st.write("**Lignes identifiées comme anomalies:**")
                display_cols = [col for col in anomaly_rows.columns if col not in ['anomaly_method']]
                st.dataframe(anomaly_rows[display_cols], use_container_width=True)
            else:
                st.success("✅ Aucune anomalie détectée!")
    
    with tab3:
        st.subheader("✅ Données Propres (Sans Anomalies)")
        if 'anomaly' in df.columns:
            clean_rows = df[df['anomaly'] == 1]  # Normal rows
            
            if len(clean_rows) > 0:
                st.info(f"📊 {len(clean_rows)} lignes sans anomalies détectées ({(len(clean_rows)/len(df)*100):.1f}%)")
                
                # Show statistics about clean data
                col1, col2, col3 = st.columns(3)
                col1.metric("Lignes Propres", len(clean_rows))
                col2.metric("Taux de Qualité", f"{(len(clean_rows)/len(df)*100):.1f}%")
                if 'price' in clean_rows.columns:
                    col3.metric("Prix Moyen", f"{pd.to_numeric(clean_rows['price'], errors='coerce').mean():.2f}")
                
                # Display clean rows
                st.write("**Aperçu des données propres:**")
                # Show relevant columns only
                display_cols = [col for col in clean_rows.columns if col not in ['anomaly_method', 'anomaly', 'has_anomaly', 'empty_count']]
                st.dataframe(clean_rows[display_cols], use_container_width=True)
                
                # Download clean data
                csv = clean_rows.to_csv(index=False)
                st.download_button(
                    label="📥 Télécharger Données Propres (CSV)",
                    data=csv,
                    file_name="clean_data.csv",
                    mime="text/csv"
                )
            else:
                st.warning("❌ Aucune donnée propre trouvée - tous les lignes contiennent des anomalies!")
    
    with tab4:
        st.subheader("Détails des Colonnes")
        for col in df.columns:
            with st.expander(f"Colonne: {col}"):
                col_data = df[col]
                st.write(f"Type: {col_data.dtype}")
                st.write(f"Non-null: {col_data.notna().sum()}/{len(df)}")
                st.write(f"Unique values: {col_data.nunique()}")
                if col_data.dtype in ['int64', 'float64']:
                    st.write(f"Min: {col_data.min()}, Max: {col_data.max()}, Moyenne: {col_data.mean():.2f}")
    
    with tab5:
        st.subheader("Statistiques Descriptives")
        st.write(df.describe())
        
        st.subheader("Données Manquantes")
        missing_data = df.isnull().sum()
        if missing_data.sum() > 0:
            st.bar_chart(missing_data[missing_data > 0])
        else:
            st.info("✅ Aucune donnée manquante!")
    
    # Show LLM feedback if available
    if os.path.exists("data/processed/llm_feedback.json"):
        st.divider()
        st.subheader("🤖 Corrections LLM")
        with st.expander("Voir les corrections appliquées par le LLM"):
            with open("data/processed/llm_feedback.json", "r") as f:
                feedbacks = [json.loads(line) for line in f if line.strip()]
            if feedbacks:
                for fb in feedbacks[-5:]:  # Show last 5
                    st.json(fb)
            else:
                st.info("Aucune correction LLM disponible")
else:
    st.error(f"Fichier introuvable : {data_path}")
    st.markdown("""
### 🚀 Getting Started with File Watcher

No processed data found yet. Here's how to use the pipeline:

#### **Option 1: Upload via Dashboard (Easy)** 📤
1. Click the **"📤 Upload CSV for Processing"** section on the left
2. Select a CSV file from your computer
3. The file will be saved to `data/raw/`
4. If the **File Watcher is running**, it will automatically process it!
5. Results will appear here after processing

#### **Option 2: Manual File Drop** 📁
1. Copy your CSV file to the `data/raw/` directory
2. If the **File Watcher is running**, it detects it automatically
3. Processing starts immediately
4. Results appear in the dashboard after completion

#### **Option 3: Start File Watcher Service** ⏱️
1. Open a terminal in the project directory
2. **Windows users:**
   ```bash
   start_file_watcher.bat
   ```
3. **Linux/Mac users:**
   ```bash
   chmod +x start_file_watcher.sh
   ./start_file_watcher.sh
   ```
4. The service will monitor `data/raw/` continuously
5. Any new CSV files will be processed automatically!

---

### 📊 Pipeline Architecture
```
📤 Upload File (via Dashboard or File System)
    ↓
🔍 File Watcher Detects New CSV
    ↓
✅ Validates File Upload Complete
    ↓
⚙️ Runs Full ETL Pipeline (automatically)
    ├─ Load & Profile
    ├─ Clean Data
    ├─ Detect Anomalies (12-category classifier)
    ├─ Product-Category Validation
    ├─ LLM Corrections (if available)
    └─ Export Clean Data
    ↓
📦 Archive Processed File
    ↓
🔄 Dashboard Auto-Updates Results
```

### 💡 Key Features
- ✅ **Automatic Detection** - Drop files and they're processed instantly
- ✅ **Real-Time Monitoring** - Watch processing in file watcher terminal
- ✅ **Quality Validation** - 12-category anomaly detection system
- ✅ **Clean Data Export** - Download validated data as CSV
- ✅ **Comprehensive Logging** - All events tracked in logs/pipeline_logs.json

### ❓ Next Steps
1. **Start the File Watcher** (see commands above)
2. **Upload a CSV file** using the dashboard or file system
3. **Check the results** here in the dashboard
4. **Download clean data** from the "✅ Données Propres" tab

---

**Ready to get started?** Use the **📤 Upload CSV for Processing** button on the left sidebar!
""")

