"""
Phase 2: RandomForest Model Training
Trains a supervised ML model using enhanced feature engineering
"""
import pandas as pd
import numpy as np
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
import joblib
import json
from datetime import datetime

class MLModelTrainer:
    def __init__(self, model_dir="models/"):
        self.model_dir = model_dir
        self.model_path = os.path.join(model_dir, "supervised_anomaly_model.pkl")
        self.feature_encoder_path = os.path.join(model_dir, "feature_encoders.pkl")
        self.feature_names_path = os.path.join(model_dir, "feature_names.json")
        
        # Create models directory if not exists
        os.makedirs(model_dir, exist_ok=True)
        
        self.label_encoders = {}
        self.feature_names = []

    def engineer_features(self, df):
        """
        Extract and engineer features from data for RandomForest training.
        Converts mixed data types to numeric features.
        """
        df = df.copy()
        features = pd.DataFrame(index=df.index)
        
        # ===== NUMERIC FEATURES =====
        numeric_cols = ['price', 'quantity', 'discount']
        for col in numeric_cols:
            if col in df.columns:
                features[f'{col}_value'] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                features[f'{col}_is_zero'] = (features[f'{col}_value'] == 0).astype(int)
                features[f'{col}_is_negative'] = (features[f'{col}_value'] < 0).astype(int)
        
        # ===== TEXT/STRING FEATURES =====
        text_cols = ['product_name', 'customer_name', 'description']
        for col in text_cols:
            if col in df.columns:
                features[f'{col}_length'] = df[col].fillna('').astype(str).str.len()
                features[f'{col}_is_empty'] = (df[col].fillna('') == '').astype(int)
                features[f'{col}_has_numbers'] = df[col].fillna('').astype(str).str.contains(r'\d').astype(int)
        
        # ===== CATEGORICAL FEATURES (ENCODED) =====
        categorical_cols = ['category', 'status', 'payment_method']
        for col in categorical_cols:
            if col in df.columns:
                # During training: fit & transform; during prediction: fit_transform
                if col not in self.label_encoders:
                    self.label_encoders[col] = LabelEncoder()
                    features[f'{col}_encoded'] = self.label_encoders[col].fit_transform(
                        df[col].fillna('unknown').astype(str)
                    )
                else:
                    try:
                        features[f'{col}_encoded'] = self.label_encoders[col].transform(
                            df[col].fillna('unknown').astype(str)
                        )
                    except:
                        features[f'{col}_encoded'] = 0
        
        # ===== DERIVED FEATURES =====
        # Price-Quantity interaction
        if 'price_value' in features.columns and 'quantity_value' in features.columns:
            features['price_qty_interaction'] = features['price_value'] * features['quantity_value']
            features['price_per_unit_variance'] = (
                (features['price_value'] - features['price_value'].mean()).abs() / 
                (features['price_value'].std() + 1e-8)
            )
        
        # ===== RULE-BASED FEATURES (if available from anomaly_classifier) =====
        if 'anomaly_flags' in df.columns:
            features['anomaly_flags'] = df['anomaly_flags'].fillna(0)
        
        # ===== DATE FEATURES (if applicable) =====
        for col in ['order_date', 'created_at', 'transaction_date']:
            if col in df.columns:
                try:
                    date_col = pd.to_datetime(df[col], errors='coerce')
                    features[f'{col}_is_valid'] = date_col.notna().astype(int)
                    features[f'{col}_day_of_week'] = date_col.dt.dayofweek.fillna(0)
                    features[f'{col}_month'] = date_col.dt.month.fillna(0)
                except:
                    pass
        
        # Handle missing values
        features = features.fillna(0)
        features = features.replace([np.inf, -np.inf], 0)
        
        # Store feature names
        self.feature_names = list(features.columns)
        
        return features

    def train_model(self, data_path="data/processed/clean.csv", force_retrain=False):
        """
        Train RandomForest model on historical data.
        
        Args:
            data_path: Path to processed CSV with historical data
            force_retrain: If True, retrain even if model exists
        """
        print("\n" + "="*70)
        print("PHASE 2: RANDOMFOREST MODEL TRAINING")
        print("="*70)
        
        # Check if model already exists
        if os.path.exists(self.model_path) and not force_retrain:
            print(f"✅ Model already exists at {self.model_path}")
            print("   Use force_retrain=True to retrain...")
            return True
        
        # Load data
        if not os.path.exists(data_path):
            print(f"❌ Data file not found: {data_path}")
            print("   Cannot train without historical data.")
            return False
        
        print(f"\n📂 Loading data from: {data_path}")
        df = pd.read_csv(data_path)
        print(f"   ✓ Loaded {len(df)} rows, {len(df.columns)} columns")
        
        # Prepare features
        print("\n🔧 Engineering features...")
        features = self.engineer_features(df)
        print(f"   ✓ Generated {len(features.columns)} features")
        print(f"   Features: {self.feature_names}")
        
        # Prepare target
        print("\n🎯 Preparing target variable...")
        if 'anomaly' in df.columns:
            # If anomaly column exists, use it (1=normal, -1=anomaly)
            target = (df['anomaly'] == 1).astype(int)
            print(f"   ✓ Using 'anomaly' column from data")
        elif 'has_anomaly' in df.columns:
            # Alternative: use has_anomaly flag
            target = (~df['has_anomaly']).astype(int)
            print(f"   ✓ Using 'has_anomaly' column from data")
        else:
            print(f"   ⚠ No anomaly label found. Generating synthetic labels...")
            # Fallback: Create synthetic labels based on anomaly_flags
            if 'anomaly_flags' in df.columns:
                target = (df['anomaly_flags'] == 0).astype(int)
            else:
                print("   ❌ Cannot determine target. Aborting.")
                return False
        
        print(f"   ✓ Target distribution: {target.value_counts().to_dict()}")
        
        # Train RandomForest
        print("\n🚀 Training RandomForest classifier...")
        print("   Parameters:")
        print("   - n_estimators: 100")
        print("   - max_depth: 15")
        print("   - min_samples_split: 5")
        print("   - min_samples_leaf: 2")
        
        clf = RandomForestClassifier(
            n_estimators=100,
            max_depth=15,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1,
            verbose=1
        )
        
        clf.fit(features, target)
        print("   ✓ Training complete!")
        
        # Feature importance
        print("\n📊 Top 10 Most Important Features:")
        feature_importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': clf.feature_importances_
        }).sort_values('importance', ascending=False)
        
        for idx, (_, row) in enumerate(feature_importance.head(10).iterrows(), 1):
            print(f"   {idx}. {row['feature']:<30} {row['importance']:.4f}")
        
        # Save model
        print(f"\n💾 Saving model...")
        joblib.dump(clf, self.model_path)
        joblib.dump(self.label_encoders, self.feature_encoder_path)
        with open(self.feature_names_path, 'w') as f:
            json.dump(self.feature_names, f)
        
        print(f"   ✓ Model saved: {self.model_path}")
        print(f"   ✓ Encoders saved: {self.feature_encoder_path}")
        print(f"   ✓ Feature names saved: {self.feature_names_path}")
        
        # Summary
        print("\n" + "="*70)
        print("✅ PHASE 2 TRAINING COMPLETE")
        print("="*70)
        print(f"Model accuracy on training data: {clf.score(features, target):.2%}")
        print("Ready for integration into anomaly detection pipeline!")
        
        return True

if __name__ == "__main__":
    trainer = MLModelTrainer()
    trainer.train_model(force_retrain=False)
