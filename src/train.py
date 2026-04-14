import pandas as pd
import json
import os
from sklearn.ensemble import RandomForestClassifier
import joblib

def train_supervised_model(data_path="data/processed/clean.csv", feedback_path="data/processed/llm_feedback.json", model_dir="models/"):
    """
    Entraîne un modèle supervisé (RandomForest) en utilisant les données historiques
    et les retours consolidés du LLM (Feedback Loop).
    """
    print("Chargement des données pour l'apprentissage...")
    
    if not os.path.exists(data_path):
        print("Aucune donnée nettoyée disponible pour l'entraînement.")
        return

    df = pd.read_csv(data_path)
    
    # Pour un exemple basique, on utilise la colonne 'anomaly' générée par l'IsolationForest
    # Dans la réalité, on utiliserait les corrections humaines/LLM stockées dans llm_feedback.json
    # pour créer une cible (target) fiable (1 = normal, 0 = anomalie vraie).
    
    if 'anomaly' not in df.columns:
        print("Colonne 'anomaly' introuvable. Impossible d'entraîner le modèle supervisé.")
        return

    # Préparation des features
    features = df[["price", "quantity"]].fillna(0)
    target = df["anomaly"] # -1 (anomalie), 1 (normal)

    print("Entraînement du classifieur supervisé (Random Forest)...")
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(features, target)

    model_path = os.path.join(model_dir, "supervised_anomaly_model.pkl")
    joblib.dump(clf, model_path)
    print(f"Modèle sauvegardé : {model_path}")

if __name__ == "__main__":
    train_supervised_model()
