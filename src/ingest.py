import pandas as pd
import chardet

def load_data(path):
    with open(path, 'rb') as f:
        enc = chardet.detect(f.read())['encoding']

    df = pd.read_csv(path, encoding=enc, sep=None, engine='python')
    return df

# Test
# df = load_data("data/raw/sales_dirty_1.csv")
# print(df.head())