# ==========================================================
# Week 8 - Mini Project
# File: generate_data.py
#
# Purpose:
# This file is responsible for generating realistic sample
# datasets required for the assignment.
#
# The generated datasets will be used later for:
# 1. Data Cleaning
# 2. SQL Analysis
# 3. SQLite Database
# 4. Python + SQL Integration
# ==========================================================

# Importing required libraries.

# Faker is used to generate realistic fake information
# like names, emails, addresses and dates.
from faker import Faker

# Pandas is used to create DataFrames
# and save them as CSV files.
import pandas as pd

# Random is used for generating random
# values and introducing required errors.
import random

# Datetime helps generate realistic dates.
from datetime import datetime, timedelta


# ----------------------------------------------------------
# Creating the Faker object.
#
# This object will generate realistic customer data.
# ----------------------------------------------------------
fake = Faker()


# ----------------------------------------------------------
# Printing a confirmation message.
#
# This helps verify that the environment has been
# configured correctly before generating data.
# ----------------------------------------------------------
print("=" * 50)
print("Environment setup completed successfully.")
print("All required libraries are imported.")
print("=" * 50)

# ----------------------------------------------------------
# Creating the folder where all raw CSV files
# generated during this project will be stored.
#
# If the folder already exists, Python will not
# create it again and no error will occur.
# ----------------------------------------------------------

import os

os.makedirs("data/raw", exist_ok=True)


# ----------------------------------------------------------
# The assignment requires at least 500 customer records.
# This variable stores the total number of records that
# will be generated.
# ----------------------------------------------------------

TOTAL_CUSTOMERS = 500


# ----------------------------------------------------------
# Creating an empty list.
#
# Every generated customer record will be stored
# inside this list first.
# Later it will be converted into a Pandas DataFrame.
# ----------------------------------------------------------

customers = []


# ----------------------------------------------------------
# Generating customer records one by one.
#
# Each loop creates one customer with a unique ID
# and realistic information.
# ----------------------------------------------------------

for i in range(1, TOTAL_CUSTOMERS + 1):

    # Creating a unique customer ID.
    # Example:
    # CUST0001
    # CUST0002
    customer_id = f"CUST{i:04d}"

    # Generating a realistic customer name.
    customer_name = fake.name()

    # Generating a realistic email address.
    # Invalid emails will be introduced later
    # because the assignment requires data cleaning.
    email = fake.email()

    # Generating a registration date
    # within the last three years.
    registration_date = fake.date_between(
        start_date="-3y",
        end_date="today"
    )

    # Selecting a customer type randomly.
    # The assignment specifies three customer types.
    customer_type = random.choice([
        "REGULAR",
        "PREMIUM",
        "VIP"
    ])

    # Storing all generated values
    # into the customer list.
    customers.append({
        "customer_id": customer_id,
        "customer_name": customer_name,
        "email": email,
        "registration_date": registration_date,
        "customer_type": customer_type
    })


# ----------------------------------------------------------
# Converting the customer list into
# a Pandas DataFrame.
#
# DataFrame makes it easier to save
# the data as a CSV file.
# ----------------------------------------------------------

customers_df = pd.DataFrame(customers)


# ----------------------------------------------------------
# Saving the generated customer dataset.
#
# This file will be used in the next steps
# for creating orders and performing data cleaning.
# ----------------------------------------------------------

customers_df.to_csv(
    "data/raw/customers.csv",
    index=False
)


# ----------------------------------------------------------
# Displaying a success message so that we know
# the file has been created successfully.
# ----------------------------------------------------------

print("\nCustomer dataset generated successfully.")
print("Total Customer Records:", len(customers_df))


# Displaying the first five records
# to verify the generated data.

print("\nFirst Five Records:\n")
print(customers_df.head())


# Creating a list of product categories.
# These categories are mentioned in the assignment.
categories = {
    "Electronics": [
        "Mobile Phone",
        "Laptop",
        "Smart Watch",
        "Bluetooth Speaker",
        "Headphones"
    ],
    "Clothing": [
        "T-Shirt",
        "Jeans",
        "Jacket",
        "Shirt",
        "Hoodie"
    ],
    "Home": [
        "Chair",
        "Dining Table",
        "Sofa",
        "Bed",
        "Lamp"
    ],
    "Books": [
        "Python Basics",
        "SQL Guide",
        "Machine Learning",
        "Data Science",
        "Web Development"
    ]
}

# Creating an empty list to store product records.
products = []

# Generating 500 product records.
for i in range(1, 501):

    # Selecting a random category.
    category = random.choice(list(categories.keys()))

    # Selecting a product name from the selected category.
    product_name = random.choice(categories[category])

    # Creating a simple subcategory.
    subcategory = category + " Items"

    # Generating a random cost price.
    cost_price = round(random.uniform(100, 5000), 2)

    # Storing product details.
    products.append({
        "product_id": f"PROD{i:04d}",
        "product_name": product_name,
        "category": category,
        "subcategory": subcategory,
        "cost_price": cost_price
    })

# Converting product list into DataFrame.
products_df = pd.DataFrame(products)

# Saving the generated products dataset.
products_df.to_csv(
    "data/raw/products.csv",
    index=False
)

# Displaying confirmation message.
print("\nProducts dataset generated successfully.")
print("Total Product Records:", len(products_df))

# Displaying first five records.
print("\nFirst Five Products:\n")
print(products_df.head())


# Creating the list of possible order status.
# These values are mentioned in the assignment.
order_status = [
    "PLACED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED"
]

# Creating the list of available regions.
region_codes = [
    "NORTH",
    "SOUTH",
    "EAST",
    "WEST"
]

# Creating an empty list to store order records.
orders = []

# Generating 500 order records.
for i in range(1, 501):

    # Creating a unique order ID.
    order_id = f"ORD{i:04d}"

    # Selecting a random customer ID.
    # Every order must belong to a customer.
    customer_id = f"CUST{random.randint(1, TOTAL_CUSTOMERS):04d}"

    # Generating a random order date
    # within the last two years.
    order_date = fake.date_time_between(
        start_date="-2y",
        end_date="now"
    )

    # Converting the date into the required format.
    # Format: YYYY-MM-DD HH:MM:SS
    order_date = order_date.strftime("%Y-%m-%d %H:%M:%S")

    # Selecting a random order status.
    status = random.choice(order_status)

    # Selecting a random region.
    region_code = random.choice(region_codes)

    # Adding the generated order
    # into the order list.
    orders.append({
        "order_id": order_id,
        "customer_id": customer_id,
        "order_date": order_date,
        "status": status,
        "region_code": region_code
    })

# Converting the order list
# into a Pandas DataFrame.
orders_df = pd.DataFrame(orders)

# Saving the generated orders dataset.
orders_df.to_csv(
    "data/raw/orders.csv",
    index=False
)

# Displaying a success message.
print("\nOrders dataset generated successfully.")
print("Total Order Records:", len(orders_df))

# Displaying first five records
# to verify the generated data.
print("\nFirst Five Orders:\n")
print(orders_df.head())


# Creating an empty list to store
# all order item records.
order_items = []

# Generating 500 order item records.
for i in range(1, 501):

    # Creating a unique item ID.
    item_id = f"ITEM{i:04d}"

    # Selecting an existing order ID.
    # This keeps referential integrity between
    # orders and order_items.
    order_id = f"ORD{random.randint(1,500):04d}"

    # Selecting an existing product ID.
    product_id = f"PROD{random.randint(1,500):04d}"

    # Generating random quantity.
    quantity = random.randint(1,5)

    # Generating unit price.
    unit_price = round(random.uniform(100,5000),2)

    # Generating discount percentage.
    discount_percent = random.randint(0,50)

    # Adding generated data into the list.
    order_items.append({
        "item_id": item_id,
        "order_id": order_id,
        "product_id": product_id,
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_percent": discount_percent
    })

# Converting list into DataFrame.
order_items_df = pd.DataFrame(order_items)

# Saving the generated dataset.
order_items_df.to_csv(
    "data/raw/order_items.csv",
    index=False
)

# Displaying success message.
print("\nOrder Items dataset generated successfully.")
print("Total Order Item Records:", len(order_items_df))

# Displaying first five records.
print("\nFirst Five Order Items:\n")
print(order_items_df.head())



# ----------------------------------------------------------
# Introducing intentional errors required by the assignment.
# These errors will later be cleaned using Python functions.
# ----------------------------------------------------------

# ------------------------------
# 1. Making 5% customer IDs NULL
# ------------------------------

null_customer_rows = random.sample(
    range(len(orders_df)),
    int(len(orders_df) * 0.05)
)

for row in null_customer_rows:
    orders_df.loc[row, "customer_id"] = None


# ---------------------------------------
# 2. Making 3% quantities negative
# ---------------------------------------

negative_quantity_rows = random.sample(
    range(len(order_items_df)),
    int(len(order_items_df) * 0.03)
)

for row in negative_quantity_rows:
    order_items_df.loc[row, "quantity"] *= -1


# ---------------------------------------
# 3. Changing some dates into DD-MM-YYYY
# ---------------------------------------

wrong_date_rows = random.sample(
    range(len(orders_df)),
    10
)

for row in wrong_date_rows:

    original_date = datetime.strptime(
        orders_df.loc[row, "order_date"],
        "%Y-%m-%d %H:%M:%S"
    )

    orders_df.loc[row, "order_date"] = original_date.strftime("%d-%m-%Y")


# ---------------------------------------
# 4. Adding extra spaces and mixed case
# ---------------------------------------

product_name_rows = random.sample(
    range(len(products_df)),
    10
)

for row in product_name_rows:

    product_name = products_df.loc[row, "product_name"]

    products_df.loc[row, "product_name"] = (
        "   " +
        product_name.upper() +
        "   "
    )


# ---------------------------------------
# 5. Creating invalid email addresses
# ---------------------------------------

invalid_email_rows = random.sample(
    range(len(customers_df)),
    int(len(customers_df) * 0.02)
)

for row in invalid_email_rows:

    customers_df.loc[row, "email"] = (
        customers_df.loc[row, "email"]
        .replace("@", "")
    )


# ----------------------------------------------------------
# Saving updated datasets with intentional errors.
# ----------------------------------------------------------

customers_df.to_csv(
    "data/raw/customers.csv",
    index=False
)

products_df.to_csv(
    "data/raw/products.csv",
    index=False
)

orders_df.to_csv(
    "data/raw/orders.csv",
    index=False
)

order_items_df.to_csv(
    "data/raw/order_items.csv",
    index=False
)

print("\nIntentional errors introduced successfully.")
print("Updated CSV files have been saved.")