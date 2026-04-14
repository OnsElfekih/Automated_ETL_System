import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta

class AdvancedAnomalyClassifier:
    """
    Comprehensive rule-based anomaly classifier implementing 12-category data quality checklist.
    
    Categories:
    1. Missing / Incomplete Data
    2. Numeric Constraints
    3. Date & Time Issues
    4. String / Format Validation
    5. Categorical Data Issues
    6. Business Logic Constraints ⭐ CRITICAL
    7. Duplicate Data
    8. Outliers (ML + Rules)
    9. Cross-Column Anomalies
    10. Data Type Issues
    11. Consistency & Standardization
    12. Advanced (Seasonal, Fraud patterns)
    """
    
    def __init__(self):
        self.anomaly_types = set()
        self.severity_levels = {}  # Track severity of each anomaly type
        
        # Configurable thresholds
        self.thresholds = {
            'price_min': 0.01,
            'price_max': 100000,
            'quantity_max': 1000000,
            'price_percentile_high': 95,
            'price_percentile_low': 5,
            'max_date_gap_days': 3650,  # 10 years
            'discount_max': 100,
            'discount_min': 0,
        }
        
        # Allowed categories (customize per domain)
        self.allowed_categories = {
            'payment': ['cash', 'credit_card', 'debit_card', 'check', 'other'],
            'status': ['pending', 'completed', 'cancelled', 'returned'],
            'country': ['US', 'CA', 'UK', 'FR', 'DE', 'Other'],
        }
        
        # Product-to-Category Mapping (comprehensive supermarket products)
        self.product_category_map = {
            # ===== PRODUCE / FRUITS & VEGETABLES =====
            'apple': 'produce',
            'banana': 'produce',
            'orange': 'produce',
            'grape': 'produce',
            'strawberry': 'produce',
            'blueberry': 'produce',
            'mango': 'produce',
            'pineapple': 'produce',
            'watermelon': 'produce',
            'lemon': 'produce',
            'lime': 'produce',
            'kiwi': 'produce',
            'peach': 'produce',
            'pear': 'produce',
            'plum': 'produce',
            'avocado': 'produce',
            'coconut': 'produce',
            'papaya': 'produce',
            'guava': 'produce',
            'lettuce': 'produce',
            'spinach': 'produce',
            'broccoli': 'produce',
            'carrot': 'produce',
            'tomato': 'produce',
            'cucumber': 'produce',
            'bell pepper': 'produce',
            'onion': 'produce',
            'garlic': 'produce',
            'potato': 'produce',
            'sweet potato': 'produce',
            'celery': 'produce',
            'cauliflower': 'produce',
            'cabbage': 'produce',
            'zucchini': 'produce',
            'eggplant': 'produce',
            'asparagus': 'produce',
            'green beans': 'produce',
            'corn': 'produce',
            'peas': 'produce',
            'radish': 'produce',
            'beet': 'produce',
            'squash': 'produce',
            'pumpkin': 'produce',
            
            # ===== MEAT & SEAFOOD =====
            'chicken': 'meat',
            'beef': 'meat',
            'pork': 'meat',
            'lamb': 'meat',
            'turkey': 'meat',
            'duck': 'meat',
            'ground beef': 'meat',
            'ground chicken': 'meat',
            'ground turkey': 'meat',
            'bacon': 'meat',
            'sausage': 'meat',
            'ham': 'meat',
            'salmon': 'seafood',
            'tuna': 'seafood',
            'cod': 'seafood',
            'shrimp': 'seafood',
            'crab': 'seafood',
            'lobster': 'seafood',
            'mussels': 'seafood',
            'oysters': 'seafood',
            'trout': 'seafood',
            'mackerel': 'seafood',
            'sardines': 'seafood',
            'herring': 'seafood',
            
            # ===== DAIRY & EGGS =====
            'milk': 'dairy',
            'yogurt': 'dairy',
            'cheese': 'dairy',
            'butter': 'dairy',
            'cream': 'dairy',
            'sour cream': 'dairy',
            'ice cream': 'dairy',
            'mozzarella': 'dairy',
            'cheddar': 'dairy',
            'feta': 'dairy',
            'parmesan': 'dairy',
            'ricotta': 'dairy',
            'cottage cheese': 'dairy',
            'eggs': 'dairy',
            'egg': 'dairy',
            
            # ===== GRAINS & BREAD =====
            'bread': 'grains',
            'white bread': 'grains',
            'wheat bread': 'grains',
            'sourdough': 'grains',
            'bagel': 'grains',
            'english muffin': 'grains',
            'tortilla': 'grains',
            'rice': 'grains',
            'white rice': 'grains',
            'brown rice': 'grains',
            'pasta': 'grains',
            'spaghetti': 'grains',
            'penne': 'grains',
            'cereal': 'grains',
            'oats': 'grains',
            'oatmeal': 'grains',
            'flour': 'grains',
            'cornmeal': 'grains',
            'crackers': 'grains',
            'granola': 'grains',
            'croissant': 'grains',
            'donut': 'grains',
            'muffin': 'grains',
            'pancake': 'grains',
            
            # ===== BEVERAGES =====
            'coffee': 'beverages',
            'tea': 'beverages',
            'milk': 'beverages',
            'juice': 'beverages',
            'apple juice': 'beverages',
            'orange juice': 'beverages',
            'cranberry juice': 'beverages',
            'grape juice': 'beverages',
            'water': 'beverages',
            'sparkling water': 'beverages',
            'soda': 'beverages',
            'cola': 'beverages',
            'sprite': 'beverages',
            'fanta': 'beverages',
            'beer': 'beverages',
            'wine': 'beverages',
            'vodka': 'beverages',
            'whiskey': 'beverages',
            'rum': 'beverages',
            'gin': 'beverages',
            'champagne': 'beverages',
            'kombucha': 'beverages',
            'smoothie': 'beverages',
            'coconut water': 'beverages',
            'almond milk': 'beverages',
            'soy milk': 'beverages',
            'oat milk': 'beverages',
            'energy drink': 'beverages',
            'sports drink': 'beverages',
            
            # ===== CONDIMENTS & SAUCES =====
            'ketchup': 'condiments',
            'mustard': 'condiments',
            'mayo': 'condiments',
            'mayonnaise': 'condiments',
            'oil': 'condiments',
            'olive oil': 'condiments',
            'vegetable oil': 'condiments',
            'vinegar': 'condiments',
            'soy sauce': 'condiments',
            'hot sauce': 'condiments',
            'sriracha': 'condiments',
            'salsa': 'condiments',
            'peanut butter': 'condiments',
            'almond butter': 'condiments',
            'jam': 'condiments',
            'jelly': 'condiments',
            'honey': 'condiments',
            'maple syrup': 'condiments',
            'caramel sauce': 'condiments',
            'chocolate sauce': 'condiments',
            'bbq sauce': 'condiments',
            'teriyaki sauce': 'condiments',
            'pasta sauce': 'condiments',
            'marinara sauce': 'condiments',
            'tomato sauce': 'condiments',
            'worcestershire sauce': 'condiments',
            'fish sauce': 'condiments',
            'oyster sauce': 'condiments',
            'salt': 'condiments',
            'pepper': 'condiments',
            'spice': 'condiments',
            'cumin': 'condiments',
            'paprika': 'condiments',
            'cinnamon': 'condiments',
            'vanilla extract': 'condiments',
            
            # ===== SNACKS & SWEETS =====
            'chips': 'snacks',
            'popcorn': 'snacks',
            'pretzels': 'snacks',
            'nuts': 'snacks',
            'almonds': 'snacks',
            'walnuts': 'snacks',
            'cashews': 'snacks',
            'peanuts': 'snacks',
            'trail mix': 'snacks',
            'granola bar': 'snacks',
            'energy bar': 'snacks',
            'protein bar': 'snacks',
            'candy': 'snacks',
            'chocolate': 'snacks',
            'dark chocolate': 'snacks',
            'milk chocolate': 'snacks',
            'white chocolate': 'snacks',
            'cookie': 'snacks',
            'brownies': 'snacks',
            'cake': 'snacks',
            'pie': 'snacks',
            'ice cream': 'snacks',
            'popsicle': 'snacks',
            'licorice': 'snacks',
            'gummy bears': 'snacks',
            'gummi': 'snacks',
            'fruit snacks': 'snacks',
            
            # ===== FROZEN FOODS =====
            'frozen pizza': 'frozen',
            'frozen vegetables': 'frozen',
            'frozen berries': 'frozen',
            'frozen chicken': 'frozen',
            'frozen fish': 'frozen',
            'frozen dinner': 'frozen',
            'tv dinner': 'frozen',
            'ice cream': 'frozen',
            'frozen french fries': 'frozen',
            'french fries': 'frozen',
            'frozen nuggets': 'frozen',
            'frozen shrimp': 'frozen',
            'frozen peas': 'frozen',
            'frozen corn': 'frozen',
            'frozen enchiladas': 'frozen',
            'frozen burritos': 'frozen',
            'frozen waffles': 'frozen',
            'frozen pancakes': 'frozen',
            
            # ===== CANNED & BOXED GOODS =====
            'canned beans': 'canned',
            'canned tomato': 'canned',
            'canned fruit': 'canned',
            'canned vegetables': 'canned',
            'canned soup': 'canned',
            'canned tuna': 'canned',
            'canned chicken': 'canned',
            'canned corn': 'canned',
            'canned peas': 'canned',
            'canned pineapple': 'canned',
            'canned peaches': 'canned',
            'canned mushroom': 'canned',
            'canned olives': 'canned',
            'boxed broth': 'canned',
            'boxed stock': 'canned',
            'boxed mac and cheese': 'canned',
            
            # ===== HEALTH & BEAUTY =====
            'shampoo': 'health_beauty',
            'conditioner': 'health_beauty',
            'body wash': 'health_beauty',
            'soap': 'health_beauty',
            'lotion': 'health_beauty',
            'deodorant': 'health_beauty',
            'toothpaste': 'health_beauty',
            'toothbrush': 'health_beauty',
            'floss': 'health_beauty',
            'mouthwash': 'health_beauty',
            'face cream': 'health_beauty',
            'sunscreen': 'health_beauty',
            'moisturizer': 'health_beauty',
            'lip balm': 'health_beauty',
            'makeup': 'health_beauty',
            'foundation': 'health_beauty',
            'mascara': 'health_beauty',
            'lipstick': 'health_beauty',
            'eyeshadow': 'health_beauty',
            'shaving cream': 'health_beauty',
            'razors': 'health_beauty',
            'hair dryer': 'health_beauty',
            'curling iron': 'health_beauty',
            'nail polish': 'health_beauty',
            'vitamins': 'health_beauty',
            'supplements': 'health_beauty',
            'vitamins and minerals': 'health_beauty',
            'pain reliever': 'health_beauty',
            'antacid': 'health_beauty',
            'cough medicine': 'health_beauty',
            'cold medicine': 'health_beauty',
            'band aids': 'health_beauty',
            'bandages': 'health_beauty',
            'first aid kit': 'health_beauty',
            'thermometer': 'health_beauty',
            
            # ===== HOUSEHOLD & CLEANING =====
            'dish soap': 'household',
            'laundry detergent': 'household',
            'fabric softener': 'household',
            'bleach': 'household',
            'surface cleaner': 'household',
            'window cleaner': 'household',
            'bathroom cleaner': 'household',
            'toilet cleaner': 'household',
            'floor cleaner': 'household',
            'furniture polish': 'household',
            'glass cleaner': 'household',
            'all purpose cleaner': 'household',
            'sponge': 'household',
            'scrub brush': 'household',
            'mop': 'household',
            'broom': 'household',
            'dustpan': 'household',
            'paper towels': 'household',
            'toilet paper': 'household',
            'tissue': 'household',
            'napkins': 'household',
            'trash bags': 'household',
            'garbage bags': 'household',
            'plastic wrap': 'household',
            'aluminum foil': 'household',
            'parchment paper': 'household',
            'freezer bags': 'household',
            'ziplock bags': 'household',
            'rubber gloves': 'household',
            'oven mitts': 'household',
            'dish brush': 'household',
            'dishwasher tablets': 'household',
            'dishwasher detergent': 'household',
            'laundry pods': 'household',
            'stain remover': 'household',
            'fabric refresher': 'household',
            
            # ===== PETS =====
            'dog food': 'pets',
            'cat food': 'pets',
            'dog treats': 'pets',
            'cat treats': 'pets',
            'dog toys': 'pets',
            'cat toys': 'pets',
            'cat litter': 'pets',
            'dog shampoo': 'pets',
            'cat shampoo': 'pets',
            'pet food': 'pets',
            'pet treats': 'pets',
            'pet toys': 'pets',
            'fish food': 'pets',
            'bird food': 'pets',
            'hamster food': 'pets',
            'rabbit food': 'pets',
            
            # ===== ELECTRONICS (if stocked) =====
            'laptop': 'electronics',
            'phone': 'electronics',
            'tablet': 'electronics',
            'headphones': 'electronics',
            'speaker': 'electronics',
            'monitor': 'electronics',
            'keyboard': 'electronics',
            'mouse': 'electronics',
            'usb cable': 'electronics',
            'charger': 'electronics',
            'power bank': 'electronics',
            'flash drive': 'electronics',
            'sd card': 'electronics',
            
            # ===== FASHION (if stocked) =====
            'shirt': 'fashion',
            'pants': 'fashion',
            'dress': 'fashion',
            'shoes': 'fashion',
            'jacket': 'fashion',
            'hat': 'fashion',
            'socks': 'fashion',
            'underwear': 'fashion',
            'bra': 'fashion',
            'coat': 'fashion',
            'sweater': 'fashion',
            'jeans': 'fashion',
            'shorts': 'fashion',
            'skirt': 'fashion',
            't-shirt': 'fashion',
            'hoodie': 'fashion',
            'gloves': 'fashion',
            'scarf': 'fashion',
            'belt': 'fashion',
            'tie': 'fashion',
            
            # ===== HOME & GARDEN =====
            'pillow': 'home',
            'blanket': 'home',
            'bed sheet': 'home',
            'comforter': 'home',
            'towel': 'home',
            'bath towel': 'home',
            'hand towel': 'home',
            'shower curtain': 'home',
            'curtains': 'home',
            'rug': 'home',
            'lamp': 'home',
            'light bulb': 'home',
            'candle': 'home',
            'chair': 'home',
            'table': 'home',
            'desk': 'home',
            'couch': 'home',
            'sofa': 'home',
            'dresser': 'home',
            'nightstand': 'home',
            'bookshelf': 'home',
            'plant': 'home',
            'flower': 'home',
            'gardening soil': 'home',
            'fertilizer': 'home',
            'potting soil': 'home',
            'Plant pot': 'home',
            'shovel': 'home',
            'rake': 'home',
            'hoe': 'home',
            'garden hose': 'home',
            'pruner': 'home',
            'hedge trimmer': 'home',
            'lawn mower': 'home',
            'grill': 'home',
            'bbq': 'home',
        }
        
        # Email & phone validation patterns
        self.email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        self.phone_pattern = r'^\+?1?\d{9,15}$'  # Basic international format
        
        # Column mappings for different dataset types
        self.column_mappings = {}
        self.dataset_type = None  # 'sales' or 'retail'
    
    def _detect_dataset_type(self, df):
        """Detect dataset type based on available columns"""
        cols = set(df.columns)
        
        # Check for retail dataset (OnlineRetail pattern)
        retail_indicators = {'InvoiceNo', 'StockCode', 'UnitPrice', 'Quantity'}
        if retail_indicators.issubset(cols):
            self.dataset_type = 'retail'
            self._map_retail_columns(df)
            return 'retail'
        
        # Check for sales dataset (default)
        sales_indicators = {'price', 'quantity'}
        if any(indicator in cols for indicator in sales_indicators):
            self.dataset_type = 'sales'
            return 'sales'
        
        # Unknown type, try to adapt
        self.dataset_type = 'generic'
        return 'generic'
    
    def _map_retail_columns(self, df):
        """Map retail dataset columns to standard names"""
        # Map OnlineRetail columns to standard names for processing
        self.column_mappings = {
            'UnitPrice': 'price',
            'Quantity': 'quantity',
            'InvoiceDate': 'date',
            'InvoiceNo': 'order_id',
            'StockCode': 'product_id',
            'CustomerID': 'customer_id',
            'Country': 'country'
        }
    
    def _get_critical_fields(self, df):
        """Get critical fields based on dataset type"""
        cols = df.columns
        critical = []
        
        # Check for price-like columns
        for price_col in ['price', 'UnitPrice']:
            if price_col in cols:
                critical.append(price_col)
                break
        
        # Check for quantity-like columns
        for qty_col in ['quantity', 'Quantity']:
            if qty_col in cols:
                critical.append(qty_col)
                break
        
        # Check for ID-like columns
        for id_col in ['order_id', 'InvoiceNo']:
            if id_col in cols:
                critical.append(id_col)
                break
        
        return critical if critical else ['price', 'quantity']
    
    def classify_anomalies(self, df):
        """
        Detect and classify all anomalies across 12 categories.
        Supports multiple dataset types: sales, retail, generic
        """
        import time
        import json
        import os
        from pathlib import Path
        
        start_time = time.time()
        df = df.copy()
        total_rows = len(df)
        
        # Status file handling
        Path("data/status").mkdir(parents=True, exist_ok=True)
        status_file = "data/status/processing_status.json"
        
        def update_tier_status(tier_num, tier_name):
            """Update status file with current tier progress"""
            try:
                elapsed = time.time() - start_time
                if os.path.exists(status_file):
                    with open(status_file, 'r') as f:
                        status = json.load(f)
                else:
                    status = {}
                
                status['current_tier'] = f'TIER {tier_num}/6 - {tier_name}'
                status['elapsed_seconds'] = elapsed
                status['last_update'] = datetime.now().isoformat()
                
                with open(status_file, 'w') as f:
                    json.dump(status, f, indent=2)
            except:
                pass  # Silently fail if status file not writable
        
        print(f"\n📊 Starting anomaly detection on {total_rows:,} rows...")
        print(f"   Dataset type: {self._detect_dataset_type(df).upper()}")
        
        # Initialize anomaly columns
        df['has_anomaly'] = False
        df['anomaly_types'] = ''
        df['anomaly_flags'] = 0.0  # Use float to allow partial flags (e.g., 0.5)
        df['anomaly_severity'] = 'low'  # low, medium, high, critical
        df['anomaly_score'] = 0  # 0-100 score
        
        # ========== TIER 1: CRITICAL CHECKS (do first) ==========
        
        print(f"\n   [TIER 1/6] Running critical checks...")
        update_tier_status(1, "Critical Checks")
        # 1. Missing / Incomplete Data
        df = self._check_missing_data(df)
        print(f"      ✓ Missing data check")
        
        # 10. Data Type Issues
        df = self._check_data_type_issues(df)
        print(f"      ✓ Data type check")
        
        # ========== TIER 2: DATA QUALITY CHECKS ==========
        
        print(f"\n   [TIER 2/6] Running data quality checks...")
        update_tier_status(2, "Data Quality Checks")
        # 2. Numeric Constraints
        df = self._check_numeric_constraints(df)
        print(f"      ✓ Numeric constraints check")
        
        # 4. String / Format Validation
        df = self._check_string_format(df)
        print(f"      ✓ String format check")
        
        # 3. Date & Time Issues
        df = self._check_date_issues(df)
        print(f"      ✓ Date/time check")
        
        # 5. Categorical Data Issues
        df = self._check_categorical_issues(df)
        print(f"      ✓ Categorical issues check")
        
        # 11. Consistency & Standardization
        df = self._check_consistency(df)
        print(f"      ✓ Consistency check")
        
        # ========== TIER 3: LOGICAL & CROSS-COLUMN CHECKS ==========
        
        print(f"\n   [TIER 3/6] Running logical & cross-column checks...")
        update_tier_status(3, "Logical Checks")
        # 6. Business Logic Constraints ⭐ MOST IMPORTANT
        df = self._check_business_logic(df)
        print(f"      ✓ Business logic check")
        
        # Product-Category Mapping Check
        df = self._check_product_category_match(df)
        print(f"      ✓ Product-category mapping check")
        
        # 9. Cross-Column Anomalies
        df = self._check_cross_column_anomalies(df)
        print(f"      ✓ Cross-column checks")
        
        # ========== TIER 4: DUPLICATE & OUTLIER CHECKS ==========
        
        print(f"\n   [TIER 4/6] Running duplicate & outlier checks...")
        update_tier_status(4, "Duplicate & Outlier Checks")
        # 7. Duplicate Data
        df = self._check_duplicates(df)
        print(f"      ✓ Duplicate check")
        
        # 8. Outliers (ML + Rules)
        df = self._check_outliers(df)
        print(f"      ✓ Outlier detection")
        
        # ========== TIER 5: ADVANCED CHECKS ==========
        
        print(f"\n   [TIER 5/6] Running advanced pattern checks...")
        update_tier_status(5, "Advanced Pattern Checks")
        # 12. Advanced (Seasonal, Fraud patterns)
        df = self._check_advanced_patterns(df)
        print(f"      ✓ Advanced patterns check")
        
        # ========== TIER 6: DATASET-SPECIFIC CHECKS ==========
        
        print(f"\n   [TIER 6/6] Running dataset-specific checks...")
        update_tier_status(6, "Dataset-Specific Checks")
        # Retail-specific anomalies (for OnlineRetail-like datasets)
        if self.dataset_type == 'retail':
            df = self._check_retail_anomalies(df)
            print(f"      ✓ Retail-specific checks")
        else:
            print(f"      ✓ No dataset-specific rules needed")
        
        # ========== FINAL SCORING & SUMMARY ==========
        
        print(f"\n   [FINAL] Calculating anomaly scores...")
        # Calculate anomaly score
        df = self._calculate_anomaly_score(df)
        
        # Display summary
        anomalies_found = df['has_anomaly'].sum()
        anomaly_pct = (anomalies_found / len(df) * 100) if len(df) > 0 else 0
        elapsed = time.time() - start_time
        
        print(f"\n✅ Anomaly detection complete in {elapsed:.2f}s")
        print(f"   📊 Results: {anomalies_found:,}/{total_rows:,} rows flagged ({anomaly_pct:.1f}%)")
        print(f"   ⏱️  Speed: {total_rows/elapsed:,.0f} rows/sec")
        
        return df
    
    # ============ 1. MISSING / INCOMPLETE DATA ============
    def _check_missing_data(self, df):
        """Check for null, empty, placeholder values"""
        
        # Define critical fields dynamically based on dataset type
        critical_fields = self._get_critical_fields(df)
        
        # Check for NULL values
        for col in df.columns:
            null_mask = df[col].isnull()
            if null_mask.any():
                df.loc[null_mask, 'anomaly_flags'] += 1
                df.loc[null_mask, 'anomaly_types'] += f'{col}_null|'
                df.loc[null_mask, 'has_anomaly'] = True  # ALL NULL values are anomalies
                if col in critical_fields:
                    df.loc[null_mask, 'anomaly_severity'] = 'critical'
        
        # Check for empty strings (exclude anomaly_types and other metadata columns)
        exclude_cols = ['anomaly_types', 'anomaly_flags', 'anomaly_severity', 'anomaly_score', 'has_anomaly', 'empty_count']
        for col in df.select_dtypes(include=['object']).columns:
            if col in exclude_cols:
                continue
            empty_mask = (df[col] == '') | (df[col] == 'nan')
            if empty_mask.any():
                df.loc[empty_mask, 'anomaly_flags'] += 1
                df.loc[empty_mask, 'anomaly_types'] += f'{col}_empty|'
                df.loc[empty_mask, 'has_anomaly'] = True
        
        # Check for placeholder values (exclude metadata columns)
        placeholder_values = ['N/A', 'N/a', 'na', 'unknown', 'Unknown', '-', '--', 'null', 'NULL']
        exclude_cols = ['anomaly_types', 'anomaly_flags', 'anomaly_severity', 'anomaly_score', 'has_anomaly', 'empty_count']
        for col in df.select_dtypes(include=['object']).columns:
            if col in exclude_cols:
                continue
            placeholder_mask = df[col].astype(str).str.strip().isin(placeholder_values)
            if placeholder_mask.any():
                df.loc[placeholder_mask, 'anomaly_flags'] += 1
                df.loc[placeholder_mask, 'anomaly_types'] += f'{col}_placeholder|'
                df.loc[placeholder_mask, 'has_anomaly'] = True
        
        # Check if row is mostly empty
        df['empty_count'] = df.isnull().sum(axis=1) + df.select_dtypes(include=['object']).isin(['', 'N/A']).sum(axis=1)
        mostly_empty = df['empty_count'] > (len(df.columns) * 0.5)
        df.loc[mostly_empty, 'anomaly_flags'] += 3  # Higher penalty
        df.loc[mostly_empty, 'anomaly_types'] += 'row_mostly_empty|'
        df.loc[mostly_empty, 'has_anomaly'] = True
        
        return df
    
    # ============ 2. NUMERIC CONSTRAINTS ============
    def _check_numeric_constraints(self, df):
        """Check price, quantity, totals, discounts - handles multiple column naming patterns"""
        
        # ===== PRICE (handles 'price' or 'UnitPrice') =====
        price_col = 'price' if 'price' in df.columns else ('UnitPrice' if 'UnitPrice' in df.columns else None)
        if price_col:
            price = pd.to_numeric(df[price_col], errors='coerce')
            
            # Negative price
            neg_price = (price < 0) & (price.notna())
            df.loc[neg_price, 'anomaly_flags'] += 2
            df.loc[neg_price, 'anomaly_types'] += 'price_negative|'
            df.loc[neg_price, 'has_anomaly'] = True
            df.loc[neg_price, 'anomaly_severity'] = 'high'
            
            # Zero price (if not allowed)
            zero_price = (price == 0) & (price.notna())
            df.loc[zero_price, 'anomaly_flags'] += 1
            df.loc[zero_price, 'anomaly_types'] += 'price_zero|'
            df.loc[zero_price, 'has_anomaly'] = True
            
            # Extreme high price
            high_price = (price > self.thresholds['price_max']) & (price.notna())
            df.loc[high_price, 'anomaly_flags'] += 2
            df.loc[high_price, 'anomaly_types'] += 'price_extreme_high|'
            df.loc[high_price, 'has_anomaly'] = True
            df.loc[high_price, 'anomaly_severity'] = 'medium'
            
            # Too many decimal places
            too_decimals = price.astype(str).str.contains(r'\.\d{3,}', regex=True, na=False)
            df.loc[too_decimals, 'anomaly_flags'] += 1
            df.loc[too_decimals, 'anomaly_types'] += 'price_too_many_decimals|'
            df.loc[too_decimals, 'has_anomaly'] = True
        
        # ===== QUANTITY (handles 'quantity' or 'Quantity') =====
        qty_col = 'quantity' if 'quantity' in df.columns else ('Quantity' if 'Quantity' in df.columns else None)
        if qty_col:
            qty = pd.to_numeric(df[qty_col], errors='coerce')
            
            # Negative quantity (for sales, but allowed for retail returns)
            neg_qty = (qty < 0) & (qty.notna())
            if self.dataset_type != 'retail':  # Only flag as critical for non-retail
                df.loc[neg_qty, 'anomaly_flags'] += 2
                df.loc[neg_qty, 'anomaly_types'] += 'quantity_negative|'
                df.loc[neg_qty, 'has_anomaly'] = True
                df.loc[neg_qty, 'anomaly_severity'] = 'high'
            
            # Zero quantity (depending on context)
            zero_qty = (qty == 0) & (qty.notna())
            df.loc[zero_qty, 'anomaly_flags'] += 1
            df.loc[zero_qty, 'anomaly_types'] += 'quantity_zero|'
            df.loc[zero_qty, 'has_anomaly'] = True
            
            # Extremely large quantity
            high_qty = (qty > self.thresholds['quantity_max']) & (qty.notna())
            df.loc[high_qty, 'anomaly_flags'] += 2
            df.loc[high_qty, 'anomaly_types'] += 'quantity_extreme|'
            df.loc[high_qty, 'has_anomaly'] = True
        
        # ===== DISCOUNTS =====
        if 'discount' in df.columns:
            discount = pd.to_numeric(df['discount'], errors='coerce')
            
            # Discount > 100%
            over_discount = (discount > self.thresholds['discount_max']) & (discount.notna())
            df.loc[over_discount, 'anomaly_flags'] += 2
            df.loc[over_discount, 'anomaly_types'] += 'discount_over_100|'
            df.loc[over_discount, 'has_anomaly'] = True
            df.loc[over_discount, 'anomaly_severity'] = 'high'
            
            # Negative discount
            neg_discount = (discount < self.thresholds['discount_min']) & (discount.notna())
            df.loc[neg_discount, 'anomaly_flags'] += 2
            df.loc[neg_discount, 'anomaly_types'] += 'discount_negative|'
            df.loc[neg_discount, 'has_anomaly'] = True
        
        return df
    
    # ============ 3. DATE & TIME ISSUES ============
    def _check_date_issues(self, df):
        """Check date formats, ranges, relationships - handles 'date' or 'InvoiceDate'"""
        
        # Check for date columns (sales uses 'date', retail uses 'InvoiceDate')
        date_col = 'date' if 'date' in df.columns else ('InvoiceDate' if 'InvoiceDate' in df.columns else None)
        
        if date_col:
            # Try to parse
            parsed_dates = pd.to_datetime(df[date_col], errors='coerce')
            invalid_dates = parsed_dates.isnull() & (df[date_col].notna())
            
            # Invalid date format
            df.loc[invalid_dates, 'anomaly_flags'] += 2
            df.loc[invalid_dates, 'anomaly_types'] += 'date_invalid_format|'
            df.loc[invalid_dates, 'has_anomaly'] = True
            df.loc[invalid_dates, 'anomaly_severity'] = 'high'
            
            # Future dates (unexpected)
            future_dates = parsed_dates > datetime.now()
            df.loc[future_dates, 'anomaly_flags'] += 1
            df.loc[future_dates, 'anomaly_types'] += 'date_future|'
            df.loc[future_dates, 'has_anomaly'] = True
            
            # Very old dates (> 10 years gap)
            very_old = (datetime.now() - parsed_dates).dt.days > self.thresholds['max_date_gap_days']
            df.loc[very_old, 'anomaly_flags'] += 1
            df.loc[very_old, 'anomaly_types'] += 'date_very_old|'
            df.loc[very_old, 'has_anomaly'] = True
            df.loc[very_old, 'has_anomaly'] = True
        
        # Check for date > shipping_date relationship
        if 'order_date' in df.columns and 'shipping_date' in df.columns:
            order_date = pd.to_datetime(df['order_date'], errors='coerce')
            ship_date = pd.to_datetime(df['shipping_date'], errors='coerce')
            
            invalid_order = order_date > ship_date
            df.loc[invalid_order, 'anomaly_flags'] += 2
            df.loc[invalid_order, 'anomaly_types'] += 'order_date_after_shipping|'
            df.loc[invalid_order, 'has_anomaly'] = True
            df.loc[invalid_order, 'anomaly_severity'] = 'high'
        
        return df
    
    # ============ 4. STRING / FORMAT VALIDATION ============
    def _check_string_format(self, df):
        """Check email, phone, ID formats"""
        
        # Email validation
        if 'email' in df.columns:
            invalid_email = ~df['email'].astype(str).str.match(self.email_pattern) & (df['email'].notna())
            df.loc[invalid_email, 'anomaly_flags'] += 1
            df.loc[invalid_email, 'anomaly_types'] += 'email_invalid_format|'
            df.loc[invalid_email, 'has_anomaly'] = True
        
        # Phone validation
        if 'phone' in df.columns:
            invalid_phone = ~df['phone'].astype(str).str.match(self.phone_pattern) & (df['phone'].notna())
            df.loc[invalid_phone, 'anomaly_flags'] += 1
            df.loc[invalid_phone, 'anomaly_types'] += 'phone_invalid_format|'
            df.loc[invalid_phone, 'has_anomaly'] = True
        
        # ID format validation (customize as needed)
        for id_col in ['order_id', 'product_id', 'customer_id']:
            if id_col in df.columns:
                # Check if ID is missing or empty
                missing_id = (df[id_col].isnull()) | (df[id_col].astype(str).str.strip() == '')
                df.loc[missing_id, 'anomaly_flags'] += 2
                df.loc[missing_id, 'anomaly_types'] += f'{id_col}_missing|'
                df.loc[missing_id, 'has_anomaly'] = True
                df.loc[missing_id, 'anomaly_severity'] = 'high'
        
        return df
    
    # ============ 5. CATEGORICAL DATA ISSUES ============
    def _check_categorical_issues(self, df):
        """Check for unexpected categories, typos, case inconsistency"""
        
        for col, allowed_values in self.allowed_categories.items():
            if col in df.columns:
                # Normalize for comparison
                col_values = df[col].astype(str).str.lower().str.strip()
                allowed_lower = [v.lower() for v in allowed_values]
                
                # Check if value is not in allowed list
                not_allowed = (~col_values.isin(allowed_lower)) & (df[col].notna())
                df.loc[not_allowed, 'anomaly_flags'] += 1
                df.loc[not_allowed, 'anomaly_types'] += f'{col}_not_in_allowed_list|'
                df.loc[not_allowed, 'has_anomaly'] = True
                
                # Check for case inconsistency
                has_mixed_case = df[col].astype(str).str.contains(r'[A-Z][a-z]|[a-z][A-Z]')
                mask = has_mixed_case & (df[col].notna())
                df.loc[mask, 'anomaly_flags'] = df.loc[mask, 'anomaly_flags'].astype(float) + 0.5  # Low penalty
                df.loc[mask, 'anomaly_types'] += f'{col}_case_inconsistent|'
        
        return df
    
    # ============ 6. BUSINESS LOGIC CONSTRAINTS ⭐ CRITICAL ============
    def _check_business_logic(self, df):
        """Check critical business rules - MOST IMPORTANT"""
        
        # Rule: total_price = price × quantity
        if all(col in df.columns for col in ['price', 'quantity', 'total']):
            price = pd.to_numeric(df['price'], errors='coerce')
            qty = pd.to_numeric(df['quantity'], errors='coerce')
            total = pd.to_numeric(df['total'], errors='coerce')
            
            # Allow small rounding errors (0.01)
            calculated_total = price * qty
            mismatch = (abs(total - calculated_total) > 0.01) & (price.notna()) & (qty.notna())
            
            df.loc[mismatch, 'anomaly_flags'] += 3  # High penalty
            df.loc[mismatch, 'anomaly_types'] += 'total_price_mismatch|'
            df.loc[mismatch, 'has_anomaly'] = True
            df.loc[mismatch, 'anomaly_severity'] = 'critical'
        
        # Rule: Shipping date >= order date
        if all(col in df.columns for col in ['order_date', 'shipping_date']):
            order = pd.to_datetime(df['order_date'], errors='coerce')
            shipping = pd.to_datetime(df['shipping_date'], errors='coerce')
            invalid = (shipping < order) & (order.notna()) & (shipping.notna())
            
            df.loc[invalid, 'anomaly_flags'] += 3
            df.loc[invalid, 'anomaly_types'] += 'shipping_date_before_order|'
            df.loc[invalid, 'has_anomaly'] = True
            df.loc[invalid, 'anomaly_severity'] = 'critical'
        
        # Rule: Order must have at least 1 item
        if 'quantity' in df.columns:
            no_items = (pd.to_numeric(df['quantity'], errors='coerce') < 1) & (df['quantity'].notna())
            df.loc[no_items, 'anomaly_flags'] += 2
            df.loc[no_items, 'anomaly_types'] += 'order_no_items|'
            df.loc[no_items, 'has_anomaly'] = True
            df.loc[no_items, 'anomaly_severity'] = 'high'
        
        # Rule: Stock cannot be negative
        if 'stock' in df.columns:
            neg_stock = (pd.to_numeric(df['stock'], errors='coerce') < 0) & (df['stock'].notna())
            df.loc[neg_stock, 'anomaly_flags'] += 3
            df.loc[neg_stock, 'anomaly_types'] += 'stock_negative|'
            df.loc[neg_stock, 'has_anomaly'] = True
            df.loc[neg_stock, 'anomaly_severity'] = 'critical'
        
        # Rule: Returned item must have return date
        if 'status' in df.columns and 'return_date' in df.columns:
            returned = df['status'].astype(str).str.lower() == 'returned'
            no_return_date = (df['return_date'].isnull()) | (df['return_date'].astype(str).str.strip() == '')
            missing_return = returned & no_return_date
            
            df.loc[missing_return, 'anomaly_flags'] += 2
            df.loc[missing_return, 'anomaly_types'] += 'returned_no_return_date|'
            df.loc[missing_return, 'has_anomaly'] = True
            df.loc[missing_return, 'anomaly_severity'] = 'high'
        
        return df
    
    # ============ PRODUCT-CATEGORY MATCHING ============
    def _check_product_category_match(self, df):
        """Check if products are categorized correctly based on mapping"""
        
        if 'product' not in df.columns or 'category' not in df.columns:
            return df
        
        # Normalize for comparison (lowercase)
        product_lower = df['product'].astype(str).str.lower().str.strip()
        category_lower = df['category'].astype(str).str.lower().str.strip()
        
        # Check each row against the product-category map
        for idx, (product, category) in enumerate(zip(product_lower, category_lower)):
            if pd.isna(product) or pd.isna(category) or product == '' or category == '':
                continue  # Skip if missing data
            
            # If product is in the map, check if category matches
            if product in self.product_category_map:
                expected_category = self.product_category_map[product]
                if category != expected_category:
                    df.loc[idx, 'anomaly_flags'] += 2
                    df.loc[idx, 'anomaly_types'] += f'product_category_mismatch|'
                    df.loc[idx, 'has_anomaly'] = True
                    df.loc[idx, 'anomaly_severity'] = 'high'
        
        return df
    
    # ============ 7. DUPLICATE DATA ============
    def _check_duplicates(self, df):
        """Check for exact duplicates and duplicate IDs"""
        
        # Exact duplicate rows
        duplicates = df.duplicated(keep=False)
        df.loc[duplicates, 'anomaly_flags'] += 2
        df.loc[duplicates, 'anomaly_types'] += 'exact_duplicate_row|'
        df.loc[duplicates, 'has_anomaly'] = True
        df.loc[duplicates, 'anomaly_severity'] = 'medium'
        
        # Duplicate IDs
        for id_col in ['order_id', 'product_id', 'customer_id']:
            if id_col in df.columns:
                dup_ids = df[id_col].duplicated(keep=False) & (df[id_col].notna())
                df.loc[dup_ids, 'anomaly_flags'] += 2
                df.loc[dup_ids, 'anomaly_types'] += f'duplicate_{id_col}|'
                df.loc[dup_ids, 'has_anomaly'] = True
                df.loc[dup_ids, 'anomaly_severity'] = 'high'
        
        return df
    
    # ============ 8. OUTLIERS (ML + Rules) ============
    def _check_outliers(self, df):
        """Detect statistical outliers using IQR method"""
        
        for col in ['price', 'quantity', 'total']:
            if col not in df.columns:
                continue
            
            numeric_data = pd.to_numeric(df[col], errors='coerce')
            
            # IQR method
            q1 = numeric_data.quantile(0.25)
            q3 = numeric_data.quantile(0.75)
            iqr = q3 - q1
            
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            outliers = ((numeric_data < lower_bound) | (numeric_data > upper_bound)) & (numeric_data.notna())
            
            df.loc[outliers, 'anomaly_flags'] += 1
            df.loc[outliers, 'anomaly_types'] += f'{col}_statistical_outlier|'
            df.loc[outliers, 'has_anomaly'] = True
        
        return df
    
    # ============ 9. CROSS-COLUMN ANOMALIES ============
    def _check_cross_column_anomalies(self, df):
        """Check relationships between columns"""
        
        # Same order_id should have same total (if multiple rows)
        if 'order_id' in df.columns and 'total' in df.columns:
            for order_id in df['order_id'].unique():
                if pd.isna(order_id):
                    continue
                subset = df[df['order_id'] == order_id]
                if len(subset) > 1:
                    totals = pd.to_numeric(subset['total'], errors='coerce').unique()
                    if len(totals) > 1:
                        mask = df['order_id'] == order_id
                        df.loc[mask, 'anomaly_flags'] += 1
                        df.loc[mask, 'anomaly_types'] += 'same_order_different_totals|'
                        df.loc[mask, 'has_anomaly'] = True
        
        # Same customer should have same email (if multiple rows)
        if 'customer_id' in df.columns and 'email' in df.columns:
            for cust_id in df['customer_id'].unique():
                if pd.isna(cust_id):
                    continue
                subset = df[df['customer_id'] == cust_id]
                if len(subset) > 1:
                    emails = subset['email'].unique()
                    if len(emails) > 1 and not all(pd.isna(emails)):
                        mask = df['customer_id'] == cust_id
                        df.loc[mask, 'anomaly_flags'] += 1
                        df.loc[mask, 'anomaly_types'] += 'same_customer_different_emails|'
                        df.loc[mask, 'has_anomaly'] = True
        
        return df
    
    # ============ 10. DATA TYPE ISSUES ============
    def _check_data_type_issues(self, df):
        """Check for type mismatches"""
        
        # Check if numeric columns contain non-numeric values
        numeric_cols = ['price', 'quantity', 'total', 'discount', 'stock']
        for col in numeric_cols:
            if col in df.columns:
                # Try to convert
                converted = pd.to_numeric(df[col], errors='coerce')
                non_numeric = converted.isnull() & (df[col].notna())
                
                if non_numeric.any():
                    df.loc[non_numeric, 'anomaly_flags'] += 2
                    df.loc[non_numeric, 'anomaly_types'] += f'{col}_not_numeric|'
                    df.loc[non_numeric, 'has_anomaly'] = True
                    df.loc[non_numeric, 'anomaly_severity'] = 'high'
        
        return df
    
    # ============ 11. CONSISTENCY & STANDARDIZATION ============
    def _check_consistency(self, df):
        """Check for standardization issues"""
        
        # Leading/trailing spaces in text columns (exclude metadata columns)
        exclude_cols = ['anomaly_types', 'anomaly_flags', 'anomaly_severity', 'anomaly_score', 'has_anomaly', 'empty_count']
        for col in df.select_dtypes(include=['object']).columns:
            if col in exclude_cols:
                continue
            has_spaces = df[col].astype(str).str.len() != df[col].astype(str).str.strip().str.len()
            mask = has_spaces & (df[col].notna())
            df.loc[mask, 'anomaly_flags'] = df.loc[mask, 'anomaly_flags'].astype(float) + 0.5
            df.loc[mask, 'anomaly_types'] += f'{col}_leading_trailing_spaces|'
        
        return df
    
    # ============ 12. ADVANCED PATTERNS ============
    def _check_advanced_patterns(self, df):
        """Check for fraud-like, seasonal, or unusual patterns"""
        
        # Sudden spike in quantity (potential fraud indicator)
        if 'quantity' in df.columns:
            qty = pd.to_numeric(df['quantity'], errors='coerce')
            extreme_qty = qty > (qty.mean() + 3 * qty.std())  # 3-sigma rule
            
            df.loc[extreme_qty, 'anomaly_flags'] += 1
            df.loc[extreme_qty, 'anomaly_types'] += 'unusual_quantity_spike|'
            df.loc[extreme_qty, 'has_anomaly'] = True
        
        # Price too good to be true
        if 'price' in df.columns:
            price = pd.to_numeric(df['price'], errors='coerce')
            suspiciously_low = price < (price.mean() - 3 * price.std())
            
            df.loc[suspiciously_low, 'anomaly_flags'] += 1
            df.loc[suspiciously_low, 'anomaly_types'] += 'suspiciously_low_price|'
            df.loc[suspiciously_low, 'has_anomaly'] = True
        
        return df
    
    # ============ UTILITY METHODS ============
    def _calculate_anomaly_score(self, df):
        """Calculate overall anomaly severity score (0-100)"""
        
        severity_map = {'low': 10, 'medium': 25, 'high': 50, 'critical': 100}
        
        # Base score from anomaly_flags
        df['anomaly_score'] = (df['anomaly_flags'] / df['anomaly_flags'].max()) * 50 if df['anomaly_flags'].max() > 0 else 0
        
        # Add severity weight
        df['severity_weight'] = df['anomaly_severity'].map(severity_map).fillna(0)
        df['anomaly_score'] = (df['anomaly_score'] + df['severity_weight']) / 2
        
        return df
    
    # ============ RETAIL-SPECIFIC ANOMALIES ============
    def _check_retail_anomalies(self, df):
        """Check for retail/e-commerce specific anomalies (OnlineRetail pattern)"""
        
        # ===== InvoiceNo validation =====
        if 'InvoiceNo' in df.columns:
            # Missing invoice number
            missing_invoice = df['InvoiceNo'].isnull() | (df['InvoiceNo'].astype(str).str.strip() == '')
            df.loc[missing_invoice, 'anomaly_flags'] += 2
            df.loc[missing_invoice, 'anomaly_types'] += 'invoice_no_missing|'
            df.loc[missing_invoice, 'has_anomaly'] = True
            df.loc[missing_invoice, 'anomaly_severity'] = 'high'
            
            # Cancelled invoice (marked with 'C')
            cancelled = df['InvoiceNo'].astype(str).str.startswith('C')
            df.loc[cancelled, 'anomaly_flags'] += 1
            df.loc[cancelled, 'anomaly_types'] += 'invoice_cancelled|'
            df.loc[cancelled, 'has_anomaly'] = True
            df.loc[cancelled, 'anomaly_severity'] = 'medium'
        
        # ===== Quantity validation for retail =====
        if 'Quantity' in df.columns:
            qty = pd.to_numeric(df['Quantity'], errors='coerce')
            
            # Negative quantity (returns)
            neg_qty = (qty < 0) & (qty.notna())
            df.loc[neg_qty, 'anomaly_flags'] += 1
            df.loc[neg_qty, 'anomaly_types'] += 'quantity_negative_return|'
            df.loc[neg_qty, 'has_anomaly'] = True
            df.loc[neg_qty, 'anomaly_severity'] = 'medium'
            
            # Zero quantity
            zero_qty = (qty == 0) & (qty.notna())
            df.loc[zero_qty, 'anomaly_flags'] += 1
            df.loc[zero_qty, 'anomaly_types'] += 'quantity_zero|'
            df.loc[zero_qty, 'has_anomaly'] = True
            
            # Extremely large orders (potential bulk/wholesale)
            high_qty = qty > (qty.quantile(0.95))
            mask = high_qty & (qty.notna())
            df.loc[mask, 'anomaly_flags'] = df.loc[mask, 'anomaly_flags'].astype(float) + 0.5
            df.loc[mask, 'anomaly_types'] += 'quantity_unusually_high|'
        
        # ===== UnitPrice validation =====
        if 'UnitPrice' in df.columns:
            price = pd.to_numeric(df['UnitPrice'], errors='coerce')
            
            # Negative price
            neg_price = (price < 0) & (price.notna())
            df.loc[neg_price, 'anomaly_flags'] += 2
            df.loc[neg_price, 'anomaly_types'] += 'unit_price_negative|'
            df.loc[neg_price, 'has_anomaly'] = True
            df.loc[neg_price, 'anomaly_severity'] = 'high'
            
            # Zero price
            zero_price = (price == 0) & (price.notna())
            df.loc[zero_price, 'anomaly_flags'] += 1
            df.loc[zero_price, 'anomaly_types'] += 'unit_price_zero|'
            df.loc[zero_price, 'has_anomaly'] = True
            
            # Suspiciously low price (potential discounting)
            low_price = (price < (price.quantile(0.05))) & (price > 0) & (price.notna())
            df.loc[low_price, 'anomaly_flags'] = df.loc[low_price, 'anomaly_flags'].astype(float) + 0.5
            df.loc[low_price, 'anomaly_types'] += 'unit_price_unusually_low|'
        
        # ===== InvoiceDate validation =====
        if 'InvoiceDate' in df.columns:
            try:
                parsed_dates = pd.to_datetime(df['InvoiceDate'], errors='coerce', format='%m/%d/%Y %H:%M')
                invalid_dates = parsed_dates.isnull() & (df['InvoiceDate'].notna())
                
                if invalid_dates.any():
                    df.loc[invalid_dates, 'anomaly_flags'] += 1
                    df.loc[invalid_dates, 'anomaly_types'] += 'invoice_date_invalid|'
                    df.loc[invalid_dates, 'has_anomaly'] = True
                
                # Future dates
                future_dates = parsed_dates > pd.Timestamp.now()
                df.loc[future_dates, 'anomaly_flags'] += 1
                df.loc[future_dates, 'anomaly_types'] += 'invoice_date_future|'
                df.loc[future_dates, 'has_anomaly'] = True
            except:
                pass
        
        # ===== Description validation =====
        if 'Description' in df.columns:
            # Missing description
            missing_desc = df['Description'].isnull() | (df['Description'].astype(str).str.strip() == '')
            df.loc[missing_desc, 'anomaly_flags'] += 1
            df.loc[missing_desc, 'anomaly_types'] += 'description_missing|'
            df.loc[missing_desc, 'has_anomaly'] = True
        
        # ===== Country validation =====
        if 'Country' in df.columns:
            # Missing country
            missing_country = df['Country'].isnull() | (df['Country'].astype(str).str.strip() == '')
            df.loc[missing_country, 'anomaly_flags'] += 1
            df.loc[missing_country, 'anomaly_types'] += 'country_missing|'
            df.loc[missing_country, 'has_anomaly'] = True
        
        # ===== StockCode validation =====
        if 'StockCode' in df.columns:
            # Missing stock code
            missing_stock = df['StockCode'].isnull() | (df['StockCode'].astype(str).str.strip() == '')
            df.loc[missing_stock, 'anomaly_flags'] += 1
            df.loc[missing_stock, 'anomaly_types'] += 'stock_code_missing|'
            df.loc[missing_stock, 'has_anomaly'] = True
        
        # ===== Price × Quantity calculation (for retail) =====
        if all(col in df.columns for col in ['Quantity', 'UnitPrice']):
            qty = pd.to_numeric(df['Quantity'], errors='coerce')
            price = pd.to_numeric(df['UnitPrice'], errors='coerce')
            
            # Calculate expected total (handle negative quantities for returns)
            expected_total = abs(qty) * price
            
            # Note: OnlineRetail dataset doesn't have a 'total' column by default
            # This check is for completeness if a total column is added
            if 'Total' in df.columns or 'total' in df.columns:
                total_col = 'Total' if 'Total' in df.columns else 'total'
                total = pd.to_numeric(df[total_col], errors='coerce')
                
                mismatch = (abs(total - expected_total) > 0.01) & (qty.notna()) & (price.notna())
                df.loc[mismatch, 'anomaly_flags'] += 2
                df.loc[mismatch, 'anomaly_types'] += 'total_calculation_mismatch|'
                df.loc[mismatch, 'has_anomaly'] = True
        
        return df
    
    def get_anomaly_summary(self, df):
        """Return comprehensive anomaly summary"""
        
        summary = {
            'total_rows': len(df),
            'anomalous_rows': df['has_anomaly'].sum(),
            'anomaly_percentage': (df['has_anomaly'].sum() / len(df) * 100) if len(df) > 0 else 0,
            'by_severity': {
                'critical': (df['anomaly_severity'] == 'critical').sum(),
                'high': (df['anomaly_severity'] == 'high').sum(),
                'medium': (df['anomaly_severity'] == 'medium').sum(),
                'low': (df['anomaly_severity'] == 'low').sum(),
            },
            'avg_flags_per_anomaly': df[df['has_anomaly']]['anomaly_flags'].mean() if df['has_anomaly'].sum() > 0 else 0,
            'avg_anomaly_score': df[df['has_anomaly']]['anomaly_score'].mean() if df['has_anomaly'].sum() > 0 else 0,
            'top_anomaly_types': df[df['has_anomaly']]['anomaly_types'].str.split('|').explode().value_counts().head(10).to_dict(),
        }
        
        return summary


# Alias for backward compatibility
AnomalyClassifier = AdvancedAnomalyClassifier
