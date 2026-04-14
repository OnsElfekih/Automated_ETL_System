"""
Phase 2: ML Anomaly Detector
Loads trained RandomForest model and makes real-time predictions
"""
import pandas as pd
import numpy as np
import os
import joblib
import json
from sklearn.preprocessing import LabelEncoder

class MLAnomalyDetector:
    """
    ML-based anomaly detection using trained RandomForest model.
    Complements rule-based and isolation forest methods in ensemble voting.
    """
    
    def __init__(self, model_dir="models/"):
        self.model_dir = model_dir
        self.model_path = os.path.join(model_dir, "supervised_anomaly_model.pkl")
        self.feature_encoder_path = os.path.join(model_dir, "feature_encoders.pkl")
        self.feature_names_path = os.path.join(model_dir, "feature_names.json")
        
        self.model = None
        self.label_encoders = {}
        self.feature_names = []
        self.is_loaded = False
        
        # Try to load model on initialization
        self.load_model()
    
    def load_model(self):
        """Load trained model and encoders from disk."""
        if not os.path.exists(self.model_path):
            print(f"⚠️  Model not found at {self.model_path}")
            print("   Run train_ml_model.py to train the model first")
            self.is_loaded = False
            return False
        
        try:
            self.model = joblib.load(self.model_path)
            self.label_encoders = joblib.load(self.feature_encoder_path)
            with open(self.feature_names_path, 'r') as f:
                self.feature_names = json.load(f)
            self.is_loaded = True
            print(f"✅ ML Model loaded successfully!")
            print(f"   Features: {len(self.feature_names)}")
            return True
        except Exception as e:
            print(f"❌ Error loading model: {e}")
            self.is_loaded = False
            return False
    
    def engineer_features(self, df):
        """
        Extract features from dataframe.
        Must match features used during training!
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
            if col in df.columns and col in self.label_encoders:
                try:
                    features[f'{col}_encoded'] = self.label_encoders[col].transform(
                        df[col].fillna('unknown').astype(str)
                    )
                except:
                    # If value not in training set, use 0
                    features[f'{col}_encoded'] = 0
        
        # ===== DERIVED FEATURES =====
        if 'price_value' in features.columns and 'quantity_value' in features.columns:
            features['price_qty_interaction'] = features['price_value'] * features['quantity_value']
            price_mean = features['price_value'].mean()
            price_std = features['price_value'].std() + 1e-8
            features['price_per_unit_variance'] = (
                (features['price_value'] - price_mean).abs() / price_std
            )
        
        # ===== RULE-BASED FEATURES =====
        if 'anomaly_flags' in df.columns:
            features['anomaly_flags'] = df['anomaly_flags'].fillna(0)
        
        # ===== DATE FEATURES =====
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
        
        # Align with training features (fill missing, drop extra)
        for feature in self.feature_names:
            if feature not in features.columns:
                features[feature] = 0
        
        # Keep only features used during training (in same order)
        features = features[self.feature_names]
        
        return features
    
    def predict(self, df):
        """
        Make anomaly predictions on input dataframe.
        
        Args:
            df: Input dataframe
        
        Returns:
            tuple: (predictions, probabilities)
                - predictions: -1 for anomaly, 1 for normal
                - probabilities: Probability of normal class [0, 1]
        """
        if not self.is_loaded:
            print("⚠️  ML Model not loaded. Falling back to rule-based detection only.")
            return None, None
        
        try:
            # Engineer features
            features = self.engineer_features(df)
            
            # Make predictions
            # RandomForest returns [prob_class_0, prob_class_1]
            # where class 0 = anomaly, class 1 = normal
            proba = self.model.predict_proba(features)
            prob_normal = proba[:, 1]  # Probability of normal class
            
            # Convert to sklearn convention: -1 = anomaly, 1 = normal
            # Use 0.5 as threshold
            predictions = np.where(prob_normal >= 0.5, 1, -1)
            
            return predictions, prob_normal
        
        except Exception as e:
            print(f"❌ Prediction error: {e}")
            return None, None
    
    def predict_anomaly_score(self, df):
        """
        Return anomaly likelihood score (0-1) where 1 = high anomaly risk.
        Useful for confidence scoring in ensemble voting.
        """
        _, prob_normal = self.predict(df)
        
        if prob_normal is None:
            return None
        
        # Anomaly score = 1 - probability of being normal
        anomaly_score = 1 - prob_normal
        return anomaly_score

if __name__ == "__main__":
    # Test the detector
    print("Testing ML Anomaly Detector...")
    detector = MLAnomalyDetector()
    
    if detector.is_loaded:
        # Create test dataframe
        test_data = pd.DataFrame({
            'price': [100, 50, 999999],
            'quantity': [5, 2, 1],
            'discount': [0, 10, 0],
            'product_name': ['Widget', 'Gadget', 'Scam Product'],
            'category': ['electronics', 'electronics', 'unknown'],
            'status': ['completed', 'completed', 'pending']
        })
        
        preds, probs = detector.predict(test_data)
        print("\nPredictions:")
        for i, (p, prob) in enumerate(zip(preds, probs)):
            print(f"  Row {i}: {'Anomaly' if p == -1 else 'Normal'} (prob_normal={prob:.3f})")
    else:
        print("Model not available. Train first with train_ml_model.py")
