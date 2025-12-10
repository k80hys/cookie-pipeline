# checktables.py
import pandas as pd

# ----------------------------
# Load Warehouse CSVs
# ----------------------------
def load_warehouse_csv(name):
    return pd.read_csv(f"warehouse/{name}.csv")

tables = {
    "dim_customer": load_warehouse_csv("dim_customer"),
    "dim_date": load_warehouse_csv("dim_date"),
    "dim_product": load_warehouse_csv("dim_product"),
    "dim_ingredient": load_warehouse_csv("dim_ingredient"),
    "fact_sales": load_warehouse_csv("fact_sales"),
    "fact_ingredient_usage": load_warehouse_csv("fact_ingredient_usage")
}

# ----------------------------
# Table Summary
# ----------------------------
def summarize_table(name, df):
    print(f"\n===== {name.upper()} =====")
    print(f"Rows: {len(df)}")
    print("Columns:")
    for col, dtype in df.dtypes.items():
        print(f"  - {col}: {dtype}")
    print("-" * 40)

# ----------------------------
# Helper Functions
# ----------------------------
def check_duplicates(df, key):
    duplicates = df[df.duplicated(subset=[key])]
    print(f"{key}: {len(duplicates)} duplicate(s)")
    return duplicates

def check_nulls(df, cols):
    nulls = df[cols].isnull().sum()
    print("Null values per column:\n", nulls)
    return nulls

def check_foreign_keys(fact_df, fact_col, dim_df, dim_col):
    invalid = fact_df[~fact_df[fact_col].isin(dim_df[dim_col])]
    print(f"Foreign key check {fact_col} → {dim_col}: {len(invalid)} invalid rows")
    return invalid

def check_positive(df, cols):
    for col in cols:
        negatives = df[df[col] < 0]
        print(f"{col}: {len(negatives)} negative values")
        if len(negatives) > 0:
            print(negatives.head())

# ----------------------------
# Run Quality Checks
# ----------------------------

print("\n==============================")
print("WAREHOUSE TABLE SUMMARIES")
print("==============================")
for name, df in tables.items():
    summarize_table(name, df)

print("\n==============================")
print("DUPLICATE CHECKS")
print("==============================")
for name, df in tables.items():
    key = f"{name.split('_')[0]}_id"  # assumes *_id naming convention
    if key in df.columns:
        check_duplicates(df, key)

print("\n==============================")
print("NULL VALUE CHECKS")
print("==============================")
check_nulls(tables["dim_customer"], ['customer_id', 'customer_email'])
check_nulls(tables["dim_date"], ['date_id', 'date'])
check_nulls(tables["dim_product"], ['product_id', 'cookie_sku'])
check_nulls(tables["dim_ingredient"], ['ingredient_id', 'ingredient', 'cost_per_gram'])
check_nulls(tables["fact_sales"], ['sales_id', 'order_id', 'date_id', 'customer_id', 'product_id', 'cookies_sold', 'revenue'])
check_nulls(tables["fact_ingredient_usage"], ['usage_id', 'order_id', 'product_id', 'ingredient_id', 'grams_used', 'ingredient_cost'])

print("\n==============================")
print("FOREIGN KEY CHECKS")
print("==============================")
check_foreign_keys(tables["fact_sales"], 'customer_id', tables["dim_customer"], 'customer_id')
check_foreign_keys(tables["fact_sales"], 'product_id', tables["dim_product"], 'product_id')
check_foreign_keys(tables["fact_sales"], 'date_id', tables["dim_date"], 'date_id')
check_foreign_keys(tables["fact_ingredient_usage"], 'product_id', tables["dim_product"], 'product_id')
check_foreign_keys(tables["fact_ingredient_usage"], 'ingredient_id', tables["dim_ingredient"], 'ingredient_id')
check_foreign_keys(tables["fact_ingredient_usage"], 'order_id', tables["fact_sales"], 'order_id')

print("\n==============================")
print("POSITIVE VALUE CHECKS")
print("==============================")
check_positive(tables["fact_sales"], ['quantity', 'pack_size', 'unit_price', 'cookies_sold', 'revenue'])
check_positive(tables["fact_ingredient_usage"], ['grams_used', 'ingredient_cost'])
check_positive(tables["dim_ingredient"], ['cost_per_gram'])
