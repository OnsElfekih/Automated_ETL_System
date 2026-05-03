import json
import pandas as pd
from datetime import datetime


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


def _get_hierarchical_median_price(df, product=None, category=None):
    """
    Get median price using hierarchical approach:
    1. Try median of same product
    2. Try median of same category
    3. Fall back to global median
    """
    if "price" not in df.columns:
        return 1.0

    prices = _to_numeric(df["price"])
    positive_prices = prices[prices > 0]

    # Try product-specific median
    if product is not None and "product" in df.columns:
        product_prices = _to_numeric(
            df[df["product"].astype(str).str.lower() == str(product).lower()]["price"]
        )
        product_positive = product_prices[product_prices > 0]
        if not product_positive.empty:
            return float(product_positive.median())

    # Try category-specific median
    if category is not None and "category" in df.columns:
        category_prices = _to_numeric(
            df[df["category"].astype(str).str.lower() == str(category).lower()]["price"]
        )
        category_positive = category_prices[category_positive > 0]
        if not category_positive.empty:
            return float(category_positive.median())

    # Fall back to global median
    if not positive_prices.empty:
        return float(positive_prices.median())

    return 1.0


def _standardize_category(category_str, product_str=None):
    """
    Standardize category names to ensure consistency.
    Uses product mapping if product is provided.
    """
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

    category_normalized = str(category_str).strip().lower()

    # If product is provided, try to infer category from product
    if product_str:
        product_normalized = str(product_str).strip().lower()
        if product_normalized in product_category_map:
            return product_category_map[product_normalized]

    # Standardize known categories
    category_map = {
        "fruit": "fruits",
        "fruits": "fruits",
        "vegetable": "vegetables",
        "vegetables": "vegetables",
        "meat": "meat",
        "fish": "meat",
        "poultry": "meat",
        "dairy": "dairy",
        "milk_products": "dairy",
        "bread": "bakery",
        "bakery": "bakery",
        "pasta": "dry",
        "rice": "dry",
        "dry_goods": "dry",
        "juice": "beverages",
        "beverage": "beverages",
        "beverages": "beverages",
        "electronics": "electronics",
        "electronic": "electronics",
        "fashion": "fashion",
        "clothing": "fashion",
        "clothes": "fashion",
        "shoes": "fashion",
    }

    if category_normalized in category_map:
        return category_map[category_normalized]

    if product_str:
        return product_category_map.get(str(product_str).lower(), "unknown")

    return "unknown"


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
    
    # Track existing IDs separately to ensure uniqueness
    existing_sale_ids = set()
    existing_order_ids = set()
    if "sale_id" in df.columns:
        existing_sale_ids.update(df["sale_id"].dropna().unique())
    if "order_id" in df.columns:
        existing_order_ids.update(df["order_id"].dropna().astype(str).unique())

    # Pre-calculate global fallback values
    price_fallback = _get_hierarchical_median_price(df)
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

        # Fix missing sale_id - generate unique ID not used before
        if "sale_id" in df.columns:
            value = row.get("sale_id")
            if pd.isna(value) or str(value).strip().lower() in ["", "none", "nan"]:
                new_id = idx + 1
                while new_id in existing_sale_ids:
                    new_id += 1
                df.at[idx, "sale_id"] = new_id
                existing_sale_ids.add(new_id)
                fix_counts["sale_id_generated"] = fix_counts.get("sale_id_generated", 0) + 1
                row_changed = True

        # Fix missing order_id - generate unique ID not used before
        if "order_id" in df.columns:
            value = str(row.get("order_id")).strip()
            if value.lower() in ["", "none", "nan"]:
                new_id = f"ORD{idx + 1:04d}"
                counter = idx + 1
                while new_id in existing_order_ids:
                    counter += 1
                    new_id = f"ORD{counter:04d}"
                df.at[idx, "order_id"] = new_id
                existing_order_ids.add(new_id)
                fix_counts["order_id_generated"] = fix_counts.get("order_id_generated", 0) + 1
                row_changed = True

        if "product" in df.columns:
            value = str(row.get("product")).strip()
            if value.lower() in ["", "none", "nan"]:
                df.at[idx, "product"] = "Unknown"
                fix_counts["product_filled"] = fix_counts.get("product_filled", 0) + 1
                row_changed = True

        # Enhanced category handling - infer from product and standardize
        if "category" in df.columns:
            product_val = df.at[idx, "product"] if "product" in df.columns else None
            category_val = row.get("category")

            original_category = str(category_val).strip()
            category_lower = original_category.lower()

            standardized = _standardize_category(category_val, product_val)

            if category_lower in ["", "none", "nan"]:
                df.at[idx, "category"] = standardized
                fix_counts["category_filled"] = fix_counts.get("category_filled", 0) + 1
                row_changed = True

            elif original_category != standardized:
                df.at[idx, "category"] = standardized
                fix_counts["category_standardized"] = fix_counts.get("category_standardized", 0) + 1
                row_changed = True

        # Enhanced price handling - hierarchical median (product -> category -> global)
        if "price" in df.columns:
            price_value = pd.to_numeric(df.at[idx, "price"], errors="coerce")
            product_val = df.at[idx, "product"] if "product" in df.columns else None
            category_val = df.at[idx, "category"] if "category" in df.columns else None

            if pd.isna(price_value):
                # Get hierarchical median: product-specific, then category, then global
                hierarchical_price = _get_hierarchical_median_price(df, product_val, category_val)
                df.at[idx, "price"] = hierarchical_price
                fix_counts["price_filled_hierarchical"] = fix_counts.get("price_filled_hierarchical", 0) + 1
                row_changed = True
            elif price_value < 0:
                # Handle negative prices by making them positive
                df.at[idx, "price"] = abs(price_value)
                fix_counts["price_made_positive"] = fix_counts.get("price_made_positive", 0) + 1
                row_changed = True
            elif price_value == 0:
                # Handle zero prices
                hierarchical_price = _get_hierarchical_median_price(df, product_val, category_val)
                df.at[idx, "price"] = hierarchical_price
                fix_counts["price_zero_replaced"] = fix_counts.get("price_zero_replaced", 0) + 1
                row_changed = True

        if "quantity" in df.columns:
            quantity_value = pd.to_numeric(df.at[idx, "quantity"], errors="coerce")

            if pd.isna(quantity_value) or quantity_value <= 0:
                df.at[idx, "quantity"] = quantity_fallback
                fix_counts["quantity_fixed"] = fix_counts.get("quantity_fixed", 0) + 1
                row_changed = True

        # Recalculate totals based on corrected price and quantity
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

        # Enhanced date handling - invalid dates become NaT, future dates become NaT
    if "date" in df.columns:
        today = pd.Timestamp(datetime.now().date())

    for idx in df.index:
        raw_value = df.at[idx, "date"]
        date_value = pd.to_datetime(raw_value, errors="coerce")

        if pd.isna(date_value):
            df.at[idx, "date"] = "unknown_date"
            fix_counts["date_missing_replaced"] = fix_counts.get("date_missing_replaced", 0) + 1

        elif date_value > today:
            df.at[idx, "date"] = "unknown_date"
            fix_counts["date_future_replaced"] = fix_counts.get("date_future_replaced", 0) + 1

        else:
            df.at[idx, "date"] = date_value.date()

        if row_changed:
            rows_updated += 1

    # Remove exact duplicates
    original_rows = len(df)
    df = df.drop_duplicates()
    exact_duplicates_removed = original_rows - len(df)
    if exact_duplicates_removed > 0:
        fix_counts["exact_duplicates_removed"] = exact_duplicates_removed

    # Correct duplicate sale_ids
    if "sale_id" in df.columns:
        sale_id_counts = df["sale_id"].value_counts()
        duplicate_sale_ids = sale_id_counts[sale_id_counts > 1]
        
        duplicates_corrected = 0
        for dup_id in duplicate_sale_ids.index:
            # Get all rows with this duplicate ID
            dup_indices = df[df["sale_id"] == dup_id].index.tolist()
            
            # Keep the first occurrence, fix the rest
            for i, idx in enumerate(dup_indices[1:], 1):
                new_id = dup_id
                if isinstance(dup_id, (int, float)):
                    new_id = int(dup_id)
                    while new_id in df["sale_id"].values:
                        new_id += 1
                else:
                    # If string-based, convert to int and increment
                    new_id = int(float(dup_id)) + i
                    while new_id in df["sale_id"].values:
                        new_id += 1
                
                df.at[idx, "sale_id"] = new_id
                duplicates_corrected += 1
        
        if duplicates_corrected > 0:
            fix_counts["duplicate_sale_ids_corrected"] = duplicates_corrected

    # Correct duplicate order_ids
    if "order_id" in df.columns:
        order_id_counts = df["order_id"].value_counts()
        duplicate_order_ids = order_id_counts[order_id_counts > 1]
        
        duplicates_corrected = 0
        for dup_id in duplicate_order_ids.index:
            # Get all rows with this duplicate ID
            dup_indices = df[df["order_id"] == dup_id].index.tolist()
            
            # Keep the first occurrence, fix the rest
            for i, idx in enumerate(dup_indices[1:], 1):
                # Extract number from order_id format (e.g., "ORD0001" -> 1)
                if isinstance(dup_id, str) and dup_id.upper().startswith("ORD"):
                    try:
                        base_num = int(dup_id[3:])
                    except ValueError:
                        base_num = i
                else:
                    try:
                        base_num = int(dup_id)
                    except ValueError:
                        base_num = i
                
                new_num = base_num + i
                while f"ORD{new_num:04d}" in df["order_id"].values:
                    new_num += 1
                
                new_id = f"ORD{new_num:04d}"
                df.at[idx, "order_id"] = new_id
                duplicates_corrected += 1
        
        if duplicates_corrected > 0:
            fix_counts["duplicate_order_ids_corrected"] = duplicates_corrected

    return df, {
        "rows_updated": rows_updated,
        "fixes": fix_counts
    }



def apply_fallback_aggressive_cleaning(df):
    df = df.copy()

    rows_updated = 0
    fix_counts = {}

    # Track existing IDs separately to ensure uniqueness
    existing_sale_ids = set()
    existing_order_ids = set()
    if "sale_id" in df.columns:
        existing_sale_ids.update(df["sale_id"].dropna().unique())
    if "order_id" in df.columns:
        existing_order_ids.update(df["order_id"].dropna().astype(str).unique())

    for idx, row in df.iterrows():
        row_changed = False

        if "order_id" in df.columns:
            order_id = str(df.at[idx, "order_id"]).strip()

            if order_id.lower() in ["", "none", "nan"]:
                new_id = f"ORD{idx + 1:04d}"
                counter = idx + 1
                while new_id in existing_order_ids:
                    counter += 1
                    new_id = f"ORD{counter:04d}"
                df.at[idx, "order_id"] = new_id
                existing_order_ids.add(new_id)
                fix_counts["order_id_generated"] = fix_counts.get("order_id_generated", 0) + 1
                row_changed = True

        if "sale_id" in df.columns:
            sale_id = str(df.at[idx, "sale_id"]).strip()

            if sale_id.lower() in ["", "none", "nan"]:
                new_id = idx + 1
                while new_id in existing_sale_ids:
                    new_id += 1
                df.at[idx, "sale_id"] = new_id
                existing_sale_ids.add(new_id)
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