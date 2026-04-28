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
    df = _normalize_columns(df)

    if "price" in df.columns:
        df["price"] = _to_numeric(df["price"])

    if "quantity" in df.columns:
        df["quantity"] = _to_numeric(df["quantity"])

    if "discount" in df.columns:
        df["discount"] = _to_numeric(df["discount"])

    if "total_price" in df.columns:
        df["total_price"] = _to_numeric(df["total_price"])

    if "total" in df.columns:
        df["total"] = _to_numeric(df["total"])

    for date_col in ["date", "order_date", "shipping_date"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    return df


def repair_detected_anomalies(df):
    df = df.copy()

    fix_counts = {}
    rows_updated = 0

    price_fallback = _safe_positive_median(df["price"], 1.0) if "price" in df.columns else 1.0
    quantity_fallback = _safe_positive_median(df["quantity"], 1.0) if "quantity" in df.columns else 1.0

    product_category_map = {
        "apple": "fruits",
        "banana": "fruits",
        "orange": "fruits",
        "grape": "fruits",
        "chicken": "meat",
        "beef": "meat",
        "fish": "meat",
        "pork": "meat",
        "milk": "dairy",
        "cheese": "dairy",
        "yogurt": "dairy",
        "butter": "dairy",
        "eggs": "dairy",
        "egg": "dairy",
        "bread": "bakery",
        "pasta": "dry",
        "rice": "dry",
        "juice": "beverages",
        "laptop": "electronics",
        "phone": "electronics",
        "shirt": "fashion",
        "shoes": "fashion",
        "pants": "fashion",
    }

    for idx, row in df.iterrows():
        row_changed = False

        if "sale_id" in df.columns:
            value = row.get("sale_id")
            if pd.isna(value) or str(value).strip().lower() in ["", "none", "nan"]:
                df.at[idx, "sale_id"] = idx + 1
                fix_counts["sale_id_generated"] = fix_counts.get("sale_id_generated", 0) + 1
                row_changed = True

        if "order_id" in df.columns:
            value = str(row.get("order_id")).strip()
            if value.lower() in ["", "none", "nan"]:
                df.at[idx, "order_id"] = f"ORD{idx + 1:04d}"
                fix_counts["order_id_generated"] = fix_counts.get("order_id_generated", 0) + 1
                row_changed = True

        if "product" in df.columns:
            value = str(row.get("product")).strip()
            if value.lower() in ["", "none", "nan"]:
                df.at[idx, "product"] = "Unknown"
                fix_counts["product_filled"] = fix_counts.get("product_filled", 0) + 1
                row_changed = True

        if "category" in df.columns and "product" in df.columns:
            product = str(df.at[idx, "product"]).strip().lower()
            category = str(row.get("category")).strip().lower()

            correct_category = product_category_map.get(product)

            if correct_category:
                if category != correct_category:
                    df.at[idx, "category"] = correct_category
                    fix_counts["category_corrected"] = fix_counts.get("category_corrected", 0) + 1
                    row_changed = True
            else:
                if category in ["", "none", "nan"]:
                    df.at[idx, "category"] = "unknown"
                    fix_counts["category_filled"] = fix_counts.get("category_filled", 0) + 1
                    row_changed = True

        if "price" in df.columns:
            price_value = pd.to_numeric(df.at[idx, "price"], errors="coerce")

            if pd.isna(price_value) or price_value <= 0:
                df.at[idx, "price"] = price_fallback
                fix_counts["price_fixed"] = fix_counts.get("price_fixed", 0) + 1
                row_changed = True

        if "quantity" in df.columns:
            quantity_value = pd.to_numeric(df.at[idx, "quantity"], errors="coerce")

            if pd.isna(quantity_value) or quantity_value <= 0:
                df.at[idx, "quantity"] = quantity_fallback
                fix_counts["quantity_fixed"] = fix_counts.get("quantity_fixed", 0) + 1
                row_changed = True

        total_col = None

        if "total" in df.columns:
            total_col = "total"
        elif "total_price" in df.columns:
            total_col = "total_price"

        if total_col and "price" in df.columns and "quantity" in df.columns:
            price_value = pd.to_numeric(df.at[idx, "price"], errors="coerce")
            quantity_value = pd.to_numeric(df.at[idx, "quantity"], errors="coerce")

            if pd.notna(price_value) and pd.notna(quantity_value):
                new_total = round(float(price_value) * float(quantity_value), 2)
                current_total = pd.to_numeric(df.at[idx, total_col], errors="coerce")

                if pd.isna(current_total) or current_total <= 0 or abs(current_total - new_total) > 0.01:
                    df.at[idx, total_col] = new_total
                    fix_counts["total_recalculated"] = fix_counts.get("total_recalculated", 0) + 1
                    row_changed = True

        if "store" in df.columns:
            value = str(row.get("store")).strip()
            if value.lower() in ["", "none", "nan"]:
                df.at[idx, "store"] = "Unknown Store"
                fix_counts["store_filled"] = fix_counts.get("store_filled", 0) + 1
                row_changed = True

        if "city" in df.columns:
            value = str(row.get("city")).strip()
            if value.lower() in ["", "none", "nan"]:
                df.at[idx, "city"] = "Unknown City"
                fix_counts["city_filled"] = fix_counts.get("city_filled", 0) + 1
                row_changed = True

        if "payment_method" in df.columns:
            value = str(row.get("payment_method")).strip().lower()

            if value in ["", "none", "nan", "unknown"]:
                df.at[idx, "payment_method"] = "cash"
                fix_counts["payment_method_filled"] = fix_counts.get("payment_method_filled", 0) + 1
                row_changed = True
            elif "cred" in value:
                df.at[idx, "payment_method"] = "credit_card"
                row_changed = True
            elif "pay" in value:
                df.at[idx, "payment_method"] = "paypal"
                row_changed = True

        if "email" in df.columns:
            email = str(row.get("email")).strip()

            invalid_email = (
                email.lower() in ["", "none", "nan"]
                or "@" not in email
                or email.count("@") != 1
                or "." not in email.split("@")[-1]
                or "invalidemail" in email.lower()
            )

            if invalid_email:
                df.at[idx, "email"] = f"user{idx + 1}@example.com"
                fix_counts["email_fixed"] = fix_counts.get("email_fixed", 0) + 1
                row_changed = True

        if "date" in df.columns:
            date_value = pd.to_datetime(df.at[idx, "date"], errors="coerce")

            if pd.isna(date_value):
                df.at[idx, "date"] = pd.NaT
                fix_counts["date_invalid_to_nat"] = fix_counts.get("date_invalid_to_nat", 0) + 1
                row_changed = True
            else:
                df.at[idx, "date"] = date_value.date()

        if row_changed:
            rows_updated += 1

    df = df.drop_duplicates()

    return df, {
        "rows_updated": rows_updated,
        "fixes": fix_counts
    }


def apply_fallback_aggressive_cleaning(df):
    df = df.copy()

    rows_updated = 0
    fix_counts = {}

    for idx, row in df.iterrows():
        row_changed = False

        if "order_id" in df.columns:
            order_id = str(df.at[idx, "order_id"]).strip()

            if order_id.lower() in ["", "none", "nan"]:
                df.at[idx, "order_id"] = f"ORD{idx + 1:04d}"
                fix_counts["order_id_generated"] = fix_counts.get("order_id_generated", 0) + 1
                row_changed = True

        if "sale_id" in df.columns:
            sale_id = str(df.at[idx, "sale_id"]).strip()

            if sale_id.lower() in ["", "none", "nan"]:
                df.at[idx, "sale_id"] = idx + 1
                fix_counts["sale_id_generated"] = fix_counts.get("sale_id_generated", 0) + 1
                row_changed = True

        if row_changed:
            rows_updated += 1

    return df, {
        "rows_updated": rows_updated,
        "fixes": fix_counts
    }


def apply_llm_corrections(df, max_rows=None):
    df = df.copy()

    if "anomaly" not in df.columns:
        return df, {
            "rows_sent": 0,
            "rows_updated": 0,
            "errors": [],
            "skipped": "no_anomaly_column"
        }

    try:
        import requests
        requests.get("http://localhost:11434/api/tags", timeout=2)
    except Exception:
        return df, {
            "rows_sent": 0,
            "rows_updated": 0,
            "errors": [],
            "skipped": "ollama_unavailable"
        }

    try:
        from llm_fix import fix_row_with_llm
    except Exception as exc:
        return df, {
            "rows_sent": 0,
            "rows_updated": 0,
            "errors": [f"llm_import_failed: {exc}"],
            "skipped": False
        }

    errors = []
    rows_sent = 0
    rows_updated = 0

    anomalous_rows = df[df["anomaly"] == -1]

    if max_rows is not None:
        anomalous_rows = anomalous_rows.head(max_rows)

    for idx, row in anomalous_rows.iterrows():
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

    return df, {
        "rows_sent": rows_sent,
        "rows_updated": rows_updated,
        "errors": errors
    }