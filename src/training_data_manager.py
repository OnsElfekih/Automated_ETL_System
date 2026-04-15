"""
Training Data Manager for ML Model Improvement
Helps collect, combine, and manage larger training datasets
"""

import os
import glob
import pandas as pd
from pathlib import Path

class TrainingDataManager:
    def __init__(self, training_dir='data/trainingFiles', processed_dir='data/processed'):
        self.training_dir = training_dir
        self.processed_dir = processed_dir
        os.makedirs(training_dir, exist_ok=True)
        
    def collect_all_training_files(self):
        """Collect all available CSV files for training"""
        print("\n" + "="*70)
        print("📊 TRAINING DATA COLLECTION REPORT")
        print("="*70)
        
        files_info = []
        
        # Get all CSV files from training directory
        csv_files = glob.glob(os.path.join(self.training_dir, '*.csv'))
        
        print(f"\n📂 Training Files Directory: {self.training_dir}")
        print(f"   Found: {len(csv_files)} CSV files\n")
        
        total_rows = 0
        for file_path in csv_files:
            try:
                df = pd.read_csv(file_path, nrows=1000)  # Quick load for stats
                file_size = os.path.getsize(file_path) / (1024*1024)
                actual_rows = len(pd.read_csv(file_path))
                
                info = {
                    'file': Path(file_path).name,
                    'path': file_path,
                    'rows': actual_rows,
                    'columns': len(df.columns),
                    'size_mb': file_size
                }
                files_info.append(info)
                total_rows += actual_rows
                
                print(f"   ✅ {info['file']}")
                print(f"      Rows: {actual_rows:,} | Cols: {info['columns']} | Size: {file_size:.1f} MB")
                
            except Exception as e:
                print(f"   ❌ {Path(file_path).name}: {str(e)}")
        
        # Check for processed anomaly detection file
        anomaly_file = os.path.join(self.processed_dir, 'anomaly_detection.csv')
        if os.path.exists(anomaly_file):
            try:
                file_size = os.path.getsize(anomaly_file) / (1024*1024)
                df_anomaly = pd.read_csv(anomaly_file, nrows=5000)
                anomaly_rows = len(pd.read_csv(anomaly_file))
                
                print(f"\n   ✅ anomaly_detection.csv (LARGE DATASET)")
                print(f"      Rows: {anomaly_rows:,} | Cols: {len(df_anomaly.columns)} | Size: {file_size:.1f} MB")
                
                total_rows += anomaly_rows
                
                info = {
                    'file': 'anomaly_detection.csv',
                    'path': anomaly_file,
                    'rows': anomaly_rows,
                    'columns': len(df_anomaly.columns),
                    'size_mb': file_size
                }
                files_info.insert(0, info)  # Prioritize large file
                
            except Exception as e:
                print(f"   ❌ anomaly_detection.csv: {str(e)}")
        
        print(f"\n📈 SUMMARY")
        print(f"   Total training files: {len(files_info)}")
        print(f"   Total rows available: {total_rows:,}")
        
        print("="*70 + "\n")
        
        return files_info, total_rows
    
    def combine_training_files(self, filter_anomalies=True):
        """Combine all training files into one dataset"""
        print("\n🔄 Combining all training files...")
        
        files_info, total_rows = self.collect_all_training_files()
        
        all_dfs = []
        
        for info in files_info:
            try:
                print(f"   Loading {info['file']}...")
                df = pd.read_csv(info['path'], on_bad_lines='skip')
                
                # Ensure anomaly column exists
                if 'has_anomaly' not in df.columns:
                    if 'anomaly' in df.columns:
                        df['has_anomaly'] = (df['anomaly'] == -1)
                    else:
                        df['has_anomaly'] = False
                
                all_dfs.append(df)
                print(f"      ✓ Loaded {len(df):,} rows")
                
            except Exception as e:
                print(f"      ❌ Error: {str(e)}")
                continue
        
        if not all_dfs:
            print("❌ No files could be loaded!")
            return None
        
        # Combine
        df_combined = pd.concat(all_dfs, ignore_index=True)
        df_combined = df_combined.drop_duplicates()
        
        print(f"\n✅ Combined dataset: {len(df_combined):,} rows")
        
        # Statistics
        if 'has_anomaly' in df_combined.columns:
            anomalies = df_combined['has_anomaly'].sum()
            pct = (anomalies / len(df_combined) * 100) if len(df_combined) > 0 else 0
            print(f"   Anomalies: {anomalies:,} ({pct:.1f}%)")
            print(f"   Normal: {len(df_combined) - anomalies:,}")
        
        return df_combined
    
    def get_training_stats(self):
        """Get summary statistics about available training data"""
        files_info, total_rows = self.collect_all_training_files()
        
        stats = {
            'total_files': len(files_info),
            'total_rows': total_rows,
            'files': files_info,
            'largest_file': max(files_info, key=lambda x: x['rows']) if files_info else None,
            'total_size_mb': sum(f['size_mb'] for f in files_info)
        }
        
        print(f"\n📊 TRAINING DATA STATISTICS")
        print(f"   Total files: {stats['total_files']}")
        print(f"   Total rows: {stats['total_rows']:,}")
        print(f"   Total size: {stats['total_size_mb']:.1f} MB")
        if stats['largest_file']:
            print(f"   Largest file: {stats['largest_file']['file']} ({stats['largest_file']['rows']:,} rows)")
        print()
        
        return stats

# Example usage
if __name__ == "__main__":
    manager = TrainingDataManager()
    
    # Get statistics
    stats = manager.get_training_stats()
    
    # Combine all training files
    df_combined = manager.combine_training_files()
    
    if df_combined is not None:
        print("\n✨ Combined training data ready for model training!")
        print(f"   Shape: {df_combined.shape}")
