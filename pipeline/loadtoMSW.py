import pandas as pd
from sqlalchemy import create_engine, text

# ----------------------------
# MySQL connection
# ----------------------------
engine = create_engine("mysql+mysqlconnector://root:Truehomie12@localhost:3306/cookie_warehouse")

# ----------------------------
# Disable foreign key checks for safe loading
# ----------------------------
with engine.begin() as conn:
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

# ----------------------------
# Load DIM_PRODUCT
# ----------------------------
'''
df = pd.read_csv("warehouse/dim_product.csv")
df = df.rename(columns={
    "cost": "base_cost",
    "variants": "pack_size"
})
df.to_sql("DIM_PRODUCT", engine, if_exists="append", index=False)
'''

# ----------------------------
# Load DIM_INGREDIENT
# ----------------------------
df = pd.read_csv("warehouse/dim_ingredient.csv")
df = df.rename(columns={
    "cost_per_gram": "cost_per_gram"  # matches table, included for clarity
})
df.to_sql("DIM_INGREDIENT", engine, if_exists="append", index=False)

# ----------------------------
# Load DIM_CUSTOMER
# ----------------------------
df = pd.read_csv("warehouse/dim_customer.csv")
df.to_sql("DIM_CUSTOMER", engine, if_exists="append", index=False)

# ----------------------------
# Load DIM_DATE
# ----------------------------
df = pd.read_csv("warehouse/dim_date.csv")
df.to_sql("DIM_DATE", engine, if_exists="append", index=False)

# ----------------------------
# Load FACT_SALES
# ----------------------------
df = pd.read_csv("warehouse/fact_sales.csv")
df.to_sql("FACT_SALES", engine, if_exists="append", index=False)

# ----------------------------
# Load FACT_INGREDIENT_USAGE
# ----------------------------
df = pd.read_csv("warehouse/fact_ingredient_usage.csv")
df = df.rename(columns={
    "ingredient_cost": "cost"
})
df.to_sql("FACT_INGREDIENT_USAGE", engine, if_exists="append", index=False)

# ----------------------------
# Re-enable foreign key checks
# ----------------------------
with engine.begin() as conn:
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

print("All tables loaded successfully!")
