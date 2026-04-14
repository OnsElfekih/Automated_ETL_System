import streamlit as st
import pandas as pd
import os
import json

st.set_page_config(page_title="ETL Quality Dashboard", layout="wide")
st.title("📊 ETL & Data Quality Dashboard")

st.sidebar.header("Options")
data_path = st.sidebar.text_input("Chemin du fichier propre", "data/processed/clean.csv")

if st.sidebar.button("Recharger les données"):
    st.rerun()

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
    st.warning(f"Fichier introuvable : {data_path}. Lancez d'abord pipeline `main.py`.")
