# ==========================================================
# File: database.py
# This file creates a local SQLite database and
# loads the cleaned datasets into database tables.
# ==========================================================

# Importing required libraries.

# SQLite3 is used to create and manage
# the local database.
import sqlite3

# Pandas is used to read CSV files
# and insert the data into database tables.
import pandas as pd

# OS helps check file paths if needed.
import os


# Creating database folder if it does not exist.
# Reason: This keeps the database file
# organized inside the project.
os.makedirs("database", exist_ok=True)


# Creating SQLite database connection.
# Reason: If the database does not exist,
# SQLite will automatically create it.
connection = sqlite3.connect("database/ecommerce.db")

# Creating cursor object.
# Reason: Cursor executes SQL queries.
cursor = connection.cursor()

print("=" * 50)
print("SQLite Database Connected Successfully")
print("=" * 50)


# Creating Customers table.
# Reason: This table stores customer information
# which will be used while placing orders.
cursor.execute("""
CREATE TABLE IF NOT EXISTS customers (

    customer_id TEXT PRIMARY KEY,
    customer_name TEXT,
    email TEXT,
    registration_date TEXT,
    customer_type TEXT

)
""")


# Creating Products table.
# Reason: This table stores product details
# required for sales analysis.
cursor.execute("""
CREATE TABLE IF NOT EXISTS products (

    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    subcategory TEXT,
    cost_price REAL

)
""")


# Creating Orders table.
# Reason: This table stores customer orders.
cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (

    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_date TEXT,
    order_status TEXT,
    payment_mode TEXT

)
""")


# Creating Order Items table.
# Reason: This table stores products
# included in every order.
cursor.execute("""
CREATE TABLE IF NOT EXISTS order_items (

    item_id TEXT PRIMARY KEY,
    order_id TEXT,
    product_id TEXT,
    quantity INTEGER,
    unit_price REAL,
    discount_percent REAL

)
""")


# Saving all table creation changes.
# Reason: Changes are stored permanently
# inside the SQLite database.
connection.commit()

print("All database tables created successfully.")


# Reading cleaned CSV files.
# Reason: Only cleaned data should be stored
# in the database for accurate SQL analysis.
customers_df = pd.read_csv("data/cleaned/cleaned_customers.csv")
products_df = pd.read_csv("data/cleaned/cleaned_products.csv")
orders_df = pd.read_csv("data/cleaned/cleaned_orders.csv")
order_items_df = pd.read_csv("data/cleaned/cleaned_order_items.csv")


# Loading customer data into SQLite.
# Reason: This stores customer records
# inside the customers table.
customers_df.to_sql(
    "customers",
    connection,
    if_exists="replace",
    index=False
)

# Loading product data into SQLite.
# Reason: Product information will be used
# for sales and category analysis.
products_df.to_sql(
    "products",
    connection,
    if_exists="replace",
    index=False
)

# Loading order data into SQLite.
# Reason: Order records are required
# for transaction analysis.
orders_df.to_sql(
    "orders",
    connection,
    if_exists="replace",
    index=False
)

# Loading order items into SQLite.
# Reason: This table connects orders
# with individual products.
order_items_df.to_sql(
    "order_items",
    connection,
    if_exists="replace",
    index=False
)

print("Cleaned datasets loaded successfully.")


# Verifying the total number of records.
# Reason: This confirms that all datasets
# have been loaded successfully.

print("\nDatabase Verification")

tables = [
    "customers",
    "products",
    "orders",
    "order_items"
]

for table in tables:

    count = pd.read_sql(
        f"SELECT COUNT(*) AS total FROM {table}",
        connection
    )

    print(f"{table} :", count.iloc[0]["total"])



    # Closing database connection.
# Reason: Closing the connection
# releases database resources.
connection.close()

print("\nDatabase connection closed.")