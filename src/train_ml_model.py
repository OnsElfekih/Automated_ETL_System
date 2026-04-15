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
        Automatically detects available columns.
        """
        df = df.copy()
        features = pd.DataFrame(index=df.index)
        
        # ===== NUMERIC FEATURES (Flexible column detection) =====
        # Map possible column names
        numeric_col_mappings = {
            'price': ['price', 'Price', 'sales', 'Sales', 'Sale'],
            'quantity': ['quantity', 'Quantity', 'qty', 'Qty'],
            'discount': ['discount', 'Discount'],
            'profit': ['profit', 'Profit'],
            'total_spent': ['total_spent', 'Total Spent', 'total', 'Total']
        }
        
        detected_columns = {}
        for feature_name, possible_cols in numeric_col_mappings.items():
            for col in possible_cols:
                if col in df.columns:
                    detected_columns[feature_name] = col
                    break
        
        # Process detected numeric columns
        for feature_name, actual_col in detected_columns.items():
            features[f'{feature_name}_value'] = pd.to_numeric(df[actual_col], errors='coerce').fillna(0)
            features[f'{feature_name}_is_zero'] = (features[f'{feature_name}_value'] == 0).astype(int)
            features[f'{feature_name}_is_negative'] = (features[f'{feature_name}_value'] < 0).astype(int)
        
        # ===== TEXT/STRING FEATURES =====
        text_col_mappings = {
            'product_name': ['product_name', 'Product Name', 'Product', 'name', 'Name'],
            'customer_name': ['customer_name', 'Customer Name', 'Customer'],
            'category': ['category', 'Category'],
            'description': ['description', 'Description']
        }
        
        for feature_name, possible_cols in text_col_mappings.items():
            for col in possible_cols:
                if col in df.columns:
                    features[f'{col}_length'] = df[col].fillna('').astype(str).str.len()
                    features[f'{col}_is_empty'] = (df[col].fillna('') == '').astype(int)
                    features[f'{col}_has_numbers'] = df[col].fillna('').astype(str).str.contains(r'\d').astype(int)
                    break
        
        # ===== CATEGORICAL FEATURES (ENCODED) =====
        categorical_col_mappings = {
            'category': ['category', 'Category'],
            'status': ['status', 'Status'],
            'payment_method': ['payment_method', 'Payment Method', 'Payment'],
            'segment': ['segment', 'Segment'],
            'region': ['region', 'Region']
        }
        
        for feature_name, possible_cols in categorical_col_mappings.items():
            for col in possible_cols:
                if col in df.columns:
                    # During training: fit & transform
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
                    break
        
        # ===== DERIVED FEATURES =====
        # Price-Quantity interaction (use whatever was detected)
        price_col = detected_columns.get('price')
        qty_col = detected_columns.get('quantity')
        
        if price_col and qty_col:
            features['price_qty_interaction'] = features['price_value'] * features['quantity_value']
            features['price_per_unit_variance'] = (
                (features['price_value'] - features['price_value'].mean()).abs() / 
                (features['price_value'].std() + 1e-8)
            )
        
        # Missing data indicators (important for anomaly detection)
        for col in df.columns:
            if col not in ['target_class', 'class_name', 'has_anomaly']:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    features[f'{col}_is_missing'] = df[col].isnull().astype(int)
        
        # Handle missing values
        features = features.fillna(0)
        features = features.replace([np.inf, -np.inf], 0)
        
        # Store feature names
        self.feature_names = list(features.columns)
        
        return features

    def train_on_all_available_data(self, force_retrain=False):
        """
        Train model using ALL available training files combined.
        Better for larger, more diverse datasets.
        
        Args:
            force_retrain: If True, retrain even if model exists
        """
        from training_data_manager import TrainingDataManager
        
        print("\n" + "="*70)
        print("PHASE 2: ENHANCED MULTI-FILE MODEL TRAINING")
        print("="*70)
        
        # Check if model already exists
        if os.path.exists(self.model_path) and not force_retrain:
            print(f"✅ Model already exists at {self.model_path}")
            print("   Use force_retrain=True to retrain with new data...")
            return True
        
        # Collect all training data
        print("\n📂 Step 1: Collecting all available training files...")
        manager = TrainingDataManager()
        df = manager.combine_training_files()
        
        if df is None:
            print("❌ No training data available!")
            return False
        
        print(f"\n✅ Loaded combined dataset: {len(df):,} rows")
        
        # Prepare features
        print("\n🔧 Step 2: Engineering features...")
        features = self.engineer_features(df)
        print(f"   ✓ Generated {len(features.columns)} features")
        
        # Prepare target
        print("\n🎯 Step 3: Preparing target variable...")
        if 'has_anomaly' in df.columns:
            target = df['has_anomaly'].astype(int)
            print(f"   ✓ Using 'has_anomaly' column")
        elif 'anomaly' in df.columns:
            target = (df['anomaly'] == -1).astype(int)
            print(f"   ✓ Using 'anomaly' column")
        else:
            print("   ❌ No anomaly label found!")
            return False
        
        # Show class distribution
        print(f"\n   📊 Class distribution:")
        class_dist = target.value_counts()
        for label, count in class_dist.items():
            label_name = "ANOMALY" if label == 1 else "NORMAL"
            pct = (count / len(target) * 100) if len(target) > 0 else 0
            print(f"      {label_name}: {count:,} ({pct:.1f}%)")
        
        # Train RandomForest with enhanced parameters for larger data
        print("\n🚀 Step 4: Training RandomForest classifier...")
        print("   Enhanced parameters for larger dataset:")
        print("   - n_estimators: 200 (was 100)")
        print("   - max_depth: 20 (was 15)")
        print("   - min_samples_split: 5")
        print("   - min_samples_leaf: 2")
        
        clf = RandomForestClassifier(
            n_estimators=200,
            max_depth=20,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1,
            verbose=1
        )
        
        clf.fit(features, target)
        print("   ✓ Training complete!")
        
        # Feature importance
        print("\n📊 Top 15 Most Important Features:")
        feature_importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': clf.feature_importances_
        }).sort_values('importance', ascending=False)
        
        for idx, (_, row) in enumerate(feature_importance.head(15).iterrows(), 1):
            print(f"   {idx:2d}. {row['feature']:<30} {row['importance']:.4f}")
        
        # Save model
        print(f"\n💾 Saving enhanced model...")
        joblib.dump(clf, self.model_path)
        joblib.dump(self.label_encoders, self.feature_encoder_path)
        with open(self.feature_names_path, 'w') as f:
            json.dump(self.feature_names, f)
        
        # Save metadata
        metadata = {
            'training_rows': len(df),
            'features_count': len(features.columns),
            'training_date': datetime.now().isoformat(),
            'model_type': 'RandomForest',
            'n_estimators': 200,
            'max_depth': 20,
            'training_accuracy': float(clf.score(features, target)),
            'class_distribution': {
                'anomaly': int(target.sum()),
                'normal': int(len(target) - target.sum())
            }
        }
        
        metadata_path = os.path.join(self.model_dir, "enhanced_model_metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"   ✓ Model saved: {self.model_path}")
        print(f"   ✓ Encoders saved: {self.feature_encoder_path}")
        print(f"   ✓ Feature names saved: {self.feature_names_path}")
        print(f"   ✓ Metadata saved: {metadata_path}")
        
        # Summary
        print("\n" + "="*70)
        print("✅ PHASE 2 ENHANCED TRAINING COMPLETE")
        print("="*70)
        print(f"Training rows: {len(df):,}")
        print(f"Features engineered: {len(features.columns)}")
        print(f"Model accuracy: {clf.score(features, target):.2%}")
        print(f"Trees: 200 | Max Depth: 20")
        print("\n🎯 Ready for production use!")
        print("="*70 + "\n")
        
        return True

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
