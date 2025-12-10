# etl_pipeline.py
import pandas as pd
import os

# ----------------------------
# Extract Functions
# ----------------------------
def extract_ingredients(path="staging/ingredients.csv"):
    return pd.read_csv(path)

def extract_products(path="staging/shopify_products.csv"):
    return pd.read_csv(path)

def extract_orders(path="staging/shopify_orders.csv"):
    return pd.read_csv(path)

def extract_recipes(path="staging/recipes.csv"):
    return pd.read_csv(path)

# ----------------------------
# Transform Functions
# ----------------------------
def transform_ingredients(df):
    df = df.copy()
    df['cost_per_gram'] = df['unit_cost'] / df['unit_size']
    df['ingredient_id'] = range(1, len(df) + 1)
    return df[['ingredient_id', 'ingredient', 'unit', 'cost_per_gram', 'vendor']]

def transform_products(df):
    df = df.copy()
    df['product_id'] = range(1, len(df) + 1)
    return df[['product_id', 'cookie_sku', 'title', 'cost', 'variants']]

def transform_customers(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    customer_df = df.groupby('customer_email').agg(
        first_order_date=('date', 'min'),
        last_order_date=('date', 'max')
    ).reset_index()
    customer_df['customer_id'] = range(1, len(customer_df) + 1)
    return customer_df[['customer_id', 'customer_email', 'first_order_date', 'last_order_date']]

def transform_dates(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    date_df = df[['date']].drop_duplicates().sort_values('date')
    date_df['date_id'] = range(1, len(date_df) + 1)
    date_df['day'] = date_df['date'].dt.day
    date_df['week'] = date_df['date'].dt.isocalendar().week
    date_df['month'] = date_df['date'].dt.month
    date_df['year'] = date_df['date'].dt.year
    date_df['season'] = date_df['month'].apply(lambda m: 'Winter' if m in [12,1,2]
                                               else 'Spring' if m in [3,4,5]
                                               else 'Summer' if m in [6,7,8]
                                               else 'Fall')
    return date_df[['date_id', 'date', 'day', 'week', 'month', 'year', 'season']]

def transform_fact_sales(orders, dim_customer, dim_product, dim_date):
    df = orders.copy()
    df['date'] = pd.to_datetime(df['date'])
    
    # Map surrogate keys
    df = df.merge(dim_customer[['customer_id', 'customer_email']], on='customer_email', how='left')
    df = df.merge(dim_product[['product_id', 'cookie_sku']], on='cookie_sku', how='left')
    df = df.merge(dim_date[['date_id', 'date']], on='date', how='left')
    
    # Metrics
    df['cookies_sold'] = df['quantity'] * df['pack_size']
    df['revenue'] = df['quantity'] * df['unit_price']
    
    # Surrogate key for fact table
    df['sales_id'] = range(1, len(df) + 1)
    
    return df[['sales_id', 'order_id', 'date_id', 'customer_id', 'product_id',
               'quantity', 'pack_size', 'cookies_sold', 'unit_price', 'revenue']]

def transform_fact_ingredient_usage(orders, recipes, dim_ingredient, dim_product):
    df_orders = orders.copy()
    df_orders['date'] = pd.to_datetime(df_orders['date'])
    df_orders['cookies_sold'] = df_orders['quantity'] * df_orders['pack_size']
    
    # Merge orders with recipes to get ingredient per cookie
    df = df_orders.merge(recipes, on='cookie_sku', how='left')
    df = df.merge(dim_ingredient[['ingredient_id', 'ingredient', 'cost_per_gram']], on='ingredient', how='left')
    df = df.merge(dim_product[['product_id', 'cookie_sku']], on='cookie_sku', how='left')
    
    # Compute metrics
    df['grams_used'] = df['grams_per_cookie'] * df['cookies_sold']
    df['ingredient_cost'] = df['grams_used'] * df['cost_per_gram']
    
    # Surrogate key
    df['usage_id'] = range(1, len(df) + 1)
    
    return df[['usage_id', 'order_id', 'product_id', 'ingredient_id',
               'grams_used', 'ingredient_cost']]

# ----------------------------
# Load Function
# ----------------------------
def load_csv(df, filename):
    output_folder = "warehouse"
    os.makedirs(output_folder, exist_ok=True)
    path = os.path.join(output_folder, filename)
    df.to_csv(path, index=False)
    print(f"Saved: {path}")

# ----------------------------
# Main ETL Pipeline
# ----------------------------
def main():
    # Extract
    ingredients_raw = extract_ingredients()
    products_raw = extract_products()
    orders_raw = extract_orders()
    recipes_raw = extract_recipes()
    
    # Transform dimensions
    dim_ingredient = transform_ingredients(ingredients_raw)
    dim_product = transform_products(products_raw)
    dim_customer = transform_customers(orders_raw)
    dim_date = transform_dates(orders_raw)
    
    # Load dimensions
    load_csv(dim_ingredient, "dim_ingredient.csv")
    load_csv(dim_product, "dim_product.csv")
    load_csv(dim_customer, "dim_customer.csv")
    load_csv(dim_date, "dim_date.csv")
    
    # Transform fact tables
    fact_sales = transform_fact_sales(orders_raw, dim_customer, dim_product, dim_date)
    fact_ingredient_usage = transform_fact_ingredient_usage(orders_raw, recipes_raw, dim_ingredient, dim_product)
    
    # Load fact tables
    load_csv(fact_sales, "fact_sales.csv")
    load_csv(fact_ingredient_usage, "fact_ingredient_usage.csv")

if __name__ == "__main__":
    main()