import json

import pandas as pd


def _normalize_columns(df):
    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]
    return df


def _to_numeric(series):
    return pd.to_numeric(
        series.astype(str)
        .str.replace(r"[$,]", "", regex=True)
        .str.replace(r"\s+", "", regex=True)
        .str.strip(),
        errors="coerce",
    )


def _safe_positive_median(series, default_value):
    numeric = pd.to_numeric(series, errors="coerce")
    positive_values = numeric[numeric > 0]
    if not positive_values.empty:
        return float(positive_values.median())
    return default_value


def clean_rules(df):
    """Basic standardization before anomaly detection."""
    df = _normalize_columns(df)

    if "price" in df.columns:
        df["price"] = _to_numeric(df["price"])
    if "quantity" in df.columns:
        df["quantity"] = _to_numeric(df["quantity"])
    if "discount" in df.columns:
        df["discount"] = _to_numeric(df["discount"])
    if "total_price" in df.columns:
        df["total_price"] = _to_numeric(df["total_price"])

    for date_col in ["date", "order_date", "shipping_date"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=False)

    return df


def repair_detected_anomalies(df):
    """Apply deterministic fixes based on anomaly labels emitted by the detector."""
    df = df.copy()

    if "anomaly_types" not in df.columns:
        return df, {"rows_updated": 0, "fixes": {}}

    price_fallback = _safe_positive_median(df["price"], 1.0) if "price" in df.columns else 1.0
    quantity_fallback = _safe_positive_median(df["quantity"], 1.0) if "quantity" in df.columns else 1.0
    discount_fallback = 0.0

    fix_counts = {}
    rows_updated = 0

    for idx, row in df.iterrows():
        anomaly_types = {item for item in str(row.get("anomaly_types", "")).split("|") if item}
        row_changed = False

        if "price" in df.columns and pd.notna(row.get("price")):
            price_value = pd.to_numeric(row["price"], errors="coerce")
            if pd.notna(price_value):
                if "price_negative" in anomaly_types:
                    df.at[idx, "price"] = abs(price_value)
                    fix_counts["price_negative"] = fix_counts.get("price_negative", 0) + 1
                    row_changed = True
                if "price_zero" in anomaly_types and price_value == 0:
                    df.at[idx, "price"] = price_fallback
                    fix_counts["price_zero"] = fix_counts.get("price_zero", 0) + 1
                    row_changed = True
                if "price_too_many_decimals" in anomaly_types:
                    df.at[idx, "price"] = round(float(price_value), 2)
                    fix_counts["price_too_many_decimals"] = fix_counts.get("price_too_many_decimals", 0) + 1
                    row_changed = True
                if "price_extreme_high" in anomaly_types:
                    df.at[idx, "price"] = min(float(price_value), price_fallback * 10)
                    fix_counts["price_extreme_high"] = fix_counts.get("price_extreme_high", 0) + 1
                    row_changed = True

        if "quantity" in df.columns and pd.notna(row.get("quantity")):
            quantity_value = pd.to_numeric(row["quantity"], errors="coerce")
            if pd.notna(quantity_value):
                if "quantity_negative" in anomaly_types:
                    df.at[idx, "quantity"] = abs(quantity_value)
                    fix_counts["quantity_negative"] = fix_counts.get("quantity_negative", 0) + 1
                    row_changed = True
                if "quantity_zero" in anomaly_types and quantity_value == 0:
                    df.at[idx, "quantity"] = quantity_fallback
                    fix_counts["quantity_zero"] = fix_counts.get("quantity_zero", 0) + 1
                    row_changed = True
                if "quantity_extreme" in anomaly_types:
                    df.at[idx, "quantity"] = min(float(quantity_value), quantity_fallback * 10)
                    fix_counts["quantity_extreme"] = fix_counts.get("quantity_extreme", 0) + 1
                    row_changed = True

        if "discount" in df.columns and pd.notna(row.get("discount")):
            discount_value = pd.to_numeric(row["discount"], errors="coerce")
            if pd.notna(discount_value):
                if "discount_negative" in anomaly_types:
                    df.at[idx, "discount"] = max(float(discount_value), discount_fallback)
                    fix_counts["discount_negative"] = fix_counts.get("discount_negative", 0) + 1
                    row_changed = True
                if "discount_over_100" in anomaly_types:
                    df.at[idx, "discount"] = min(float(discount_value), 100.0)
                    fix_counts["discount_over_100"] = fix_counts.get("discount_over_100", 0) + 1
                    row_changed = True

        if "date" in df.columns and "date_invalid_format" in anomaly_types:
            df.at[idx, "date"] = pd.to_datetime(row.get("date"), errors="coerce", utc=False)
            fix_counts["date_invalid_format"] = fix_counts.get("date_invalid_format", 0) + 1
            row_changed = True

        if {"order_date", "shipping_date"}.issubset(df.columns) and "order_date_after_shipping" in anomaly_types:
            order_date = pd.to_datetime(row.get("order_date"), errors="coerce", utc=False)
            shipping_date = pd.to_datetime(row.get("shipping_date"), errors="coerce", utc=False)
            if pd.notna(order_date) and pd.notna(shipping_date):
                if order_date > shipping_date:
                    df.at[idx, "shipping_date"] = order_date
                    fix_counts["order_date_after_shipping"] = fix_counts.get("order_date_after_shipping", 0) + 1
                    row_changed = True

        if "total_price" in df.columns and {"price", "quantity"}.issubset(df.columns):
            if {"total_price_mismatch", "price_negative", "quantity_negative"}.intersection(anomaly_types):
                price_value = pd.to_numeric(df.at[idx, "price"], errors="coerce")
                quantity_value = pd.to_numeric(df.at[idx, "quantity"], errors="coerce")
                if pd.notna(price_value) and pd.notna(quantity_value):
                    df.at[idx, "total_price"] = float(price_value) * float(quantity_value)
                    fix_counts["total_price_recomputed"] = fix_counts.get("total_price_recomputed", 0) + 1
                    row_changed = True

        if "row_mostly_empty" in anomaly_types:
            df.at[idx, "clean_action"] = "review_or_drop"
            fix_counts["row_mostly_empty"] = fix_counts.get("row_mostly_empty", 0) + 1
            row_changed = True

        if row_changed:
            rows_updated += 1

    return df, {"rows_updated": rows_updated, "fixes": fix_counts}


def apply_llm_corrections(df, max_rows=2):
    """Use the local LLM to repair the hardest remaining anomalies.

    The function is intentionally defensive: if the model is unavailable or
    the response is not parseable JSON, the original row is left unchanged.
    """
    df = df.copy()

    if "anomaly" not in df.columns:
        return df, {"rows_sent": 0, "rows_updated": 0, "errors": []}

    try:
        from llm_fix import fix_row_with_llm
    except Exception as exc:
        return df, {"rows_sent": 0, "rows_updated": 0, "errors": [f"llm_import_failed: {exc}"]}

    errors = []
    rows_sent = 0
    rows_updated = 0

    for idx, row in df[df["anomaly"] == -1].head(max_rows).iterrows():
        rows_sent += 1
        try:
            response = fix_row_with_llm(row)
            parsed = None

            if isinstance(response, str):
                try:
                    parsed = json.loads(response)
                except json.JSONDecodeError:
                    parsed = None
            elif isinstance(response, dict):
                parsed = response

            if isinstance(parsed, dict):
                for key, value in parsed.items():
                    if key in df.columns:
                        df.at[idx, key] = value
                df.at[idx, "llm_correction_applied"] = True
                rows_updated += 1
            else:
                df.at[idx, "llm_raw_response"] = str(response)
        except Exception as exc:
            errors.append(f"row_{idx}: {exc}")

    return df, {"rows_sent": rows_sent, "rows_updated": rows_updated, "errors": errors}