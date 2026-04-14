import pandas as pd

def clean_rules(df):
    df = df.copy()

    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce").abs()
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    return df