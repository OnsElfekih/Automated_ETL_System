def profile_data(df):
    report = {
        "rows": len(df),
        "missing": df.isnull().sum().to_dict(),
        "types": df.dtypes.astype(str).to_dict()
    }
    return report

# Test
# print(profile_data(df))