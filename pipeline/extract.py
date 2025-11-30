import pandas as pd

# Load Shopify orders
orders = pd.read_csv('~/Desktop/cookie_datasources/Shopify/shopify_orders.csv')
products = pd.read_csv('~/Desktop/cookie_datasources/Shopify/shopify_products.csv')

# Load recipes and ingredients
recipes = pd.read_csv('~/Desktop/cookie_datasources/Manual/recipes.csv')
ingredients = pd.read_csv('~/Desktop/cookie_datasources/Manual/ingredients.csv')


