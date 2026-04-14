def validate(df):
    errors = []

    if df["price"].isnull().sum() > 0:
        errors.append("price has nulls")

    if (df["price"] < 0).sum() > 0:
        errors.append("negative price exists")

    return errors