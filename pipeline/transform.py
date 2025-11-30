import pandas as pd

# -------------------------
# Step 1: Load data from repo
# -------------------------
orders = pd.read_csv('../data/raw/shopify_orders.csv')
products = pd.read_csv('../data/raw/shopify_products.csv')
recipes = pd.read_csv('../data/raw/recipes.csv')
ingredients = pd.read_csv('../data/raw/ingredients.csv')

# -------------------------
# Step 2: Standardize ingredient units
# -------------------------
unit_to_grams = {
    "kg": 1000,
    "g": 1,
    "mg": 0.001,
    "oz": 28.3495,
    "lb": 453.592,
    "cup": 128  # adjust per ingredient if needed
}

# Use 'unit_size_unit' column for conversion
ingredients['unit_size_unit'] = ingredients['unit_size_unit'].str.lower()
ingredients['unit_size_grams'] = ingredients['unit_size'] * ingredients['unit_size_unit'].map(unit_to_grams)
ingredients['cost_per_gram'] = ingredients['unit_cost'] / ingredients['unit_size_grams']

# -------------------------
# Step 3: Calculate per-cookie cost from recipes
# -------------------------
recipes_costed = recipes.merge(
    ingredients[['ingredient', 'cost_per_gram']],
    on='ingredient',
    how='left'
)

recipes_costed['cost_per_cookie'] = recipes_costed['grams_per_cookie'] * recipes_costed['cost_per_gram']

cookie_costs = recipes_costed.groupby('cookie_sku')['cost_per_cookie'].sum().reset_index()

# -------------------------
# Step 4: Merge with products and orders
# -------------------------
# Add product info to orders
orders = orders.merge(products, on='cookie_sku', how='left')

# Add per-cookie costs
orders = orders.merge(cookie_costs, on='cookie_sku', how='left')

# -------------------------
# Step 5: Calculate totals
# -------------------------
orders['total_cost'] = orders['quantity'] * orders['cost_per_cookie']
orders['revenue'] = orders['quantity'] * orders['unit_price']
orders['profit'] = orders['revenue'] - orders['total_cost']

# -------------------------
# Step 6: Optional checks
# -------------------------
print("Orders after merging with products and costs:")
print(orders.head())

# -------------------------
# Step 7: Save processed data
# -------------------------
orders.to_csv('../data/processed_orders.csv', index=False)
print("\nSaved processed_orders.csv in /data/")
