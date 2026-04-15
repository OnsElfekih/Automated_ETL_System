import json

# Load notebook
with open('Phase_2_ML_Model_Training.ipynb', 'r', encoding='utf-8') as f:
    nb = json.load(f)

# Find and fix BOTH data loading cells
count = 0
for i, cell in enumerate(nb['cells']):
    source_text = ''.join(cell.get('source', []))
    if "data_path = 'data/processed/anomaly_detection.csv'" in source_text:
        count += 1
        print(f"✅ Found data loading cell #{count} at index {i}")
        if count == 1:
            # First cell - replace with TrainingDataManager
            cell['source'] = [
                "# Load combined balanced training data (50% anomalies, 50% normal)\n",
                "from training_data_manager import TrainingDataManager\n",
                "\n",
                'print(f"📂 Loading combined balanced training data...")\n',
                "manager = TrainingDataManager()\n",
                "df = manager.combine_training_files()\n",
                "\n",
                'print(f"✅ Data loaded successfully!")\n',
                'print(f"\\nDataset shape: {df.shape}")\n',
                'print(f"Rows: {len(df)}")\n',
                'print(f"Columns: {len(df.columns)}")\n',
                "\n",
                '# Display first few rows\n',
                'print("\\n📋 First 5 rows:")\n',
                "display(df.head())\n",
                "\n",
                '# Display column info\n',
                'print("\\n📊 Column information:")\n',
                "print(df.info())\n"
            ]
        else:
            # Second cell - skip it
            cell['source'] = [
                '# Data already loaded from TrainingDataManager above\n',
                'print("ℹ️  Data already loaded - using df from combined training files")\n'
            ]
        cell['execution_count'] = None
        cell['outputs'] = []

# Save updated notebook  
with open('Phase_2_ML_Model_Training.ipynb', 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)

print(f"✅ Notebook updated successfully! Fixed {count} data loading cells")

