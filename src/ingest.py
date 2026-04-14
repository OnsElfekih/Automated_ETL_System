import pandas as pd
import chardet

def load_data(path):
    """
    Load CSV file with robust encoding detection.
    Tries multiple encodings if the detected one fails.
    """
    encodings = []
    
    # First, try to detect encoding
    try:
        with open(path, 'rb') as f:
            detected = chardet.detect(f.read())
            if detected['encoding']:
                encodings.append(detected['encoding'])
    except Exception as e:
        print(f"   ⚠️  Encoding detection failed: {str(e)}")
    
    # Add fallback encodings
    encodings.extend(['utf-8', 'iso-8859-1', 'latin-1', 'cp1252', 'ascii'])
    
    # Try each encoding until one works
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc, sep=None, engine='python')
            print(f"   ✅ Successfully loaded with encoding: {enc}")
            return df
        except Exception as e:
            continue
    
    # If all encodings fail, raise error
    raise ValueError(f"Could not load CSV with any encoding. Tried: {encodings}")

# Test
# df = load_data("data/raw/sales_dirty_1.csv")
# print(df.head())