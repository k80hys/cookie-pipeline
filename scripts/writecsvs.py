import pandas as pd
import random
import json
from datetime import datetime, timedelta

output_path = "/Users/katiewojciechowski/Projects/cookie-pipeline/staging/"

# ----------------------
# Shopify Products
# ----------------------
products_data = [
    {"cookie_sku":"BAS-CHIP","title":"Chocolate Chip","cost":0.75,"variants":[4,6,12]},
    {"cookie_sku":"BAS-SUG","title":"Sugar","cost":0.65,"variants":[4,6,12]},
    {"cookie_sku":"DLX-NUTSTUF","title":"Nutella Stuffed","cost":0.90,"variants":[4,6,12]},
    {"cookie_sku":"DLX-PBSTUF","title":"Peanut Butter Stuffed","cost":0.85,"variants":[4,6,12]},
    {"cookie_sku":"DLX-PECAN","title":"Pecan Chocolate Chip","cost":0.95,"variants":[4,6,12]},
    {"cookie_sku":"DLX-SNICK","title":"Snickerdoodle","cost":0.70,"variants":[4,6,12]},
]

products_df = pd.DataFrame(products_data)
products_df['variants'] = products_df['variants'].apply(json.dumps)
products_df.to_csv(output_path + "shopify_products.csv", index=False)

# ----------------------
# Ingredients
# ----------------------
ingredients_data = [
    {"ingredient":"Flour","unit":"grams","unit_cost":0.002,"unit_size":1000,"unit_size_unit":"grams","vendor":"King Arthur"},
    {"ingredient":"Sugar","unit":"grams","unit_cost":0.0015,"unit_size":1000,"unit_size_unit":"grams","vendor":"C&H"},
    {"ingredient":"Butter","unit":"grams","unit_cost":0.01,"unit_size":454,"unit_size_unit":"grams","vendor":"Kerrygold"},
    {"ingredient":"Chocolate Chips","unit":"grams","unit_cost":0.02,"unit_size":500,"unit_size_unit":"grams","vendor":"Ghirardelli"},
    {"ingredient":"Nutella","unit":"grams","unit_cost":0.03,"unit_size":350,"unit_size_unit":"grams","vendor":"Nutella Inc."},
    {"ingredient":"Peanut Butter","unit":"grams","unit_cost":0.015,"unit_size":500,"unit_size_unit":"grams","vendor":"Jif"},
    {"ingredient":"Pecans","unit":"grams","unit_cost":0.025,"unit_size":200,"unit_size_unit":"grams","vendor":"Diamond"},
    {"ingredient":"Cinnamon","unit":"grams","unit_cost":0.03,"unit_size":100,"unit_size_unit":"grams","vendor":"McCormick"},
]

ingredients_df = pd.DataFrame(ingredients_data)
ingredients_df.to_csv(output_path + "ingredients.csv", index=False)

# ----------------------
# Recipes
# ----------------------
recipes_data = [
    # BAS-CHIP (Chocolate Chip)
    {"cookie_sku":"BAS-CHIP","ingredient":"Flour","grams_per_cookie":15},
    {"cookie_sku":"BAS-CHIP","ingredient":"Sugar","grams_per_cookie":8},
    {"cookie_sku":"BAS-CHIP","ingredient":"Butter","grams_per_cookie":10},
    {"cookie_sku":"BAS-CHIP","ingredient":"Chocolate Chips","grams_per_cookie":12},

    # BAS-SUG (Sugar)
    {"cookie_sku":"BAS-SUG","ingredient":"Flour","grams_per_cookie":14},
    {"cookie_sku":"BAS-SUG","ingredient":"Sugar","grams_per_cookie":10},
    {"cookie_sku":"BAS-SUG","ingredient":"Butter","grams_per_cookie":9},

    # DLX-NUTSTUF (Nutella Stuffed)
    {"cookie_sku":"DLX-NUTSTUF","ingredient":"Flour","grams_per_cookie":16},
    {"cookie_sku":"DLX-NUTSTUF","ingredient":"Nutella","grams_per_cookie":15},
    {"cookie_sku":"DLX-NUTSTUF","ingredient":"Butter","grams_per_cookie":8},

    # DLX-PBSTUF (Peanut Butter Stuffed)
    {"cookie_sku":"DLX-PBSTUF","ingredient":"Flour","grams_per_cookie":15},
    {"cookie_sku":"DLX-PBSTUF","ingredient":"Peanut Butter","grams_per_cookie":14},
    {"cookie_sku":"DLX-PBSTUF","ingredient":"Butter","grams_per_cookie":7},

    # DLX-PECAN (Pecan Chocolate Chip)
    {"cookie_sku":"DLX-PECAN","ingredient":"Flour","grams_per_cookie":14},
    {"cookie_sku":"DLX-PECAN","ingredient":"Pecans","grams_per_cookie":10},
    {"cookie_sku":"DLX-PECAN","ingredient":"Chocolate Chips","grams_per_cookie":12},
    {"cookie_sku":"DLX-PECAN","ingredient":"Butter","grams_per_cookie":8},

    # DLX-SNICK (Snickerdoodle)
    {"cookie_sku":"DLX-SNICK","ingredient":"Flour","grams_per_cookie":14},
    {"cookie_sku":"DLX-SNICK","ingredient":"Sugar","grams_per_cookie":9},
    {"cookie_sku":"DLX-SNICK","ingredient":"Butter","grams_per_cookie":8},
    {"cookie_sku":"DLX-SNICK","ingredient":"Cinnamon","grams_per_cookie":2},
]

recipes_df = pd.DataFrame(recipes_data)
recipes_df.to_csv(output_path + "recipes.csv", index=False)

# ----------------------
# Shopify Orders (100 random orders)
# ----------------------
cookie_skus = ["BAS-CHIP","BAS-SUG","DLX-NUTSTUF","DLX-PBSTUF","DLX-PECAN","DLX-SNICK"]
pack_sizes = [4, 6, 12]
emails = [f"user{i}@example.com" for i in range(1, 21)]  # 20 sample customers

orders_data = []
start_date = datetime(2025, 10, 1, 8, 0, 0)

for i in range(100):
    sku = random.choice(cookie_skus)
    pack = random.choice(pack_sizes)
    quantity = random.randint(1, 3)
    unit_price = next(p['cost'] for p in products_data if p['cookie_sku'] == sku) * pack
    order_date = start_date + timedelta(minutes=random.randint(0, 60*24*30))
    customer_email = random.choice(emails)
    
    orders_data.append({
        "order_id": 2001 + i,
        "date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
        "customer_email": customer_email,
        "cookie_sku": sku,
        "pack_size": pack,
        "quantity": quantity,
        "unit_price": round(unit_price, 2)
    })

orders_df = pd.DataFrame(orders_data)
orders_df.to_csv(output_path + "shopify_orders.csv", index=False)

print("CSV files generated: shopify_products.csv, ingredients.csv, recipes.csv, shopify_orders.csv")