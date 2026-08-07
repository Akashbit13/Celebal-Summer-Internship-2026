# Importing required libraries.

# Pandas is used for reading,
# cleaning and saving CSV files.
import pandas as pd

# Datetime is used to
# correct incorrect date formats.
from datetime import datetime

# Regular expressions will help
# while validating email addresses.
import re

# OS is used to create folders
# if they do not already exist.
import os


# Creating the cleaned data folder.
# All cleaned CSV files will be stored here.
os.makedirs("data/cleaned", exist_ok=True)
# Reading the order items dataset.
# Reason: This dataset is required to count
# negative quantities while generating the issues report.
order_items_df = pd.read_csv("data/raw/order_items.csv")
print("=" * 50)
print("Data Cleaning Module Started")
print("=" * 50)


# Reading the generated orders dataset.
# This dataset contains some intentional errors
# which will be fixed in this function.
orders_df = pd.read_csv("data/raw/orders.csv")


# Creating a function to clean the orders dataset.
def clean_orders():
    global corrected_dates
    print("\nCleaning orders dataset...")

    # -------------------------------
    # Fixing incorrect date formats.
    # -------------------------------

    corrected_dates = 0

    for index, row in orders_df.iterrows():

        order_date = str(row["order_date"])

        try:
            # Checking if the date is already
            # in the correct format.
            datetime.strptime(
                order_date,
                "%Y-%m-%d %H:%M:%S"
            )

        except ValueError:

            try:
                # Converting DD-MM-YYYY format
                # into YYYY-MM-DD HH:MM:SS format.
                fixed_date = datetime.strptime(
                    order_date,
                    "%d-%m-%Y"
                )

                orders_df.loc[index, "order_date"] = (
                    fixed_date.strftime("%Y-%m-%d 00:00:00")
                )

                corrected_dates += 1

            except ValueError:
                pass


    # ------------------------------------
    # Handling missing customer IDs.
    # ------------------------------------

    missing_customer_ids = orders_df["customer_id"].isnull().sum()

    # Replacing missing customer IDs
    # with UNKNOWN.
    orders_df["customer_id"] = orders_df["customer_id"].fillna("UNKNOWN")


    # Saving cleaned orders dataset.
    orders_df.to_csv(
        "data/cleaned/cleaned_orders.csv",
        index=False
    )


    print("Order dataset cleaned successfully.")
    print("Incorrect dates corrected :", corrected_dates)
    print("Missing customer IDs handled :", missing_customer_ids)

clean_orders()


# Reading the generated products dataset.
# This dataset contains product names with
# extra spaces and mixed case.
products_df = pd.read_csv("data/raw/products.csv")


# Creating a function to clean product names.
def clean_products():
    global cleaned_products
    print("\nCleaning products dataset...")

    cleaned_products = 0

    # Checking every product name.
    for index, row in products_df.iterrows():

        original_name = row["product_name"]

        # Removing extra spaces.
        cleaned_name = original_name.strip()

        # Converting into title case.
        cleaned_name = cleaned_name.title()

        # Updating only if any change is required.
        if original_name != cleaned_name:

            products_df.loc[index, "product_name"] = cleaned_name
            cleaned_products += 1

    # Saving cleaned products dataset.
    products_df.to_csv(
        "data/cleaned/cleaned_products.csv",
        index=False
    )

    print("Products dataset cleaned successfully.")
    print("Product names corrected :", cleaned_products)

clean_products()


# Reading the generated customers dataset.
# This dataset contains some invalid email addresses.
customers_df = pd.read_csv("data/raw/customers.csv")


# Creating a function to validate email addresses.
def validate_emails():
    global corrected_emails
    print("\nValidating customer email addresses...")

    corrected_emails = 0

    # Email validation pattern.
    email_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'

    # Checking every email address.
    for index, row in customers_df.iterrows():

        email = str(row["email"])

        # If email is invalid,
        # replace it with a placeholder email.
        if not re.match(email_pattern, email):

            customers_df.loc[index, "email"] = (
                f"unknown{index}@example.com"
            )

            corrected_emails += 1

    # Saving cleaned customer dataset.
    customers_df.to_csv(
        "data/cleaned/cleaned_customers.csv",
        index=False
    )

    print("Customer emails validated successfully.")
    print("Invalid emails corrected :", corrected_emails)


validate_emails()


# Creating a function to check referential integrity.
# This function verifies whether all related records
# are connected correctly across different datasets.
def check_referential_integrity():

    

    print("\nChecking referential integrity...")

    # Reading the cleaned datasets.
    # Cleaned files are used because the previous
    # cleaning functions have already corrected the data.
    customers = pd.read_csv("data/cleaned/cleaned_customers.csv")
    orders = pd.read_csv("data/cleaned/cleaned_orders.csv")
    products = pd.read_csv("data/cleaned/cleaned_products.csv")

    # Reading the order items dataset.
    # This file will be checked against
    # orders and products.
    order_items = pd.read_csv("data/raw/order_items.csv")

    # Creating sets for faster searching.
    # Sets make the lookup process faster than lists,
    # especially when checking many records.
    customer_ids = set(customers["customer_id"])
    order_ids = set(orders["order_id"])
    product_ids = set(products["product_id"])

    # Creating counters to store
    # the number of invalid records found.
    invalid_customers = 0
    invalid_orders = 0
    invalid_products = 0

    # Checking whether every customer ID
    # in the orders dataset exists in customers.csv.
    # UNKNOWN values are ignored because they were
    # intentionally created while handling missing IDs.
    for customer_id in orders["customer_id"]:

        if customer_id != "UNKNOWN" and customer_id not in customer_ids:
            invalid_customers += 1

    # Checking whether every order ID
    # exists in the orders dataset.
    # This helps verify that every order item
    # belongs to a valid order.
    for _, row in order_items.iterrows():

        if row["order_id"] not in order_ids:
            invalid_orders += 1

        # Checking whether every product ID
        # exists in the products dataset.
        # This ensures that every ordered product
        # is available in the product master data.
        if row["product_id"] not in product_ids:
            invalid_products += 1

    # Displaying the final validation results.
    # If all values are zero, it means
    # the datasets are linked correctly.
    print("Referential integrity check completed.")
    print("Invalid Customer IDs :", invalid_customers)
    print("Invalid Order IDs    :", invalid_orders)
    print("Invalid Product IDs  :", invalid_products)


check_referential_integrity()


# Creating a function to generate the issues report.
# Reason: This report summarizes all issues found
# and corrected during the data cleaning process.
def generate_issues_report():

    print("\nGenerating issues report...")

    # Creating a list to store issue details.
    # Reason: It is easier to convert this list
    # into a DataFrame later.
    issues = [
        {
            "Issue": "Missing Customer IDs",
            "Count": orders_df["customer_id"].eq("UNKNOWN").sum()
        },
        {
            "Issue": "Incorrect Date Formats Corrected",
            "Count": corrected_dates
        },
        {
            "Issue": "Product Names Corrected",
            "Count": cleaned_products
        },
        {
            "Issue": "Invalid Emails Corrected",
            "Count": corrected_emails
        },
        {
            "Issue": "Negative Quantities",
            "Count": (order_items_df["quantity"] < 0).sum()
        }
    ]

    # Converting the issue list into a DataFrame.
    # Reason: DataFrame can be easily saved as a CSV file.
    issues_df = pd.DataFrame(issues)

    # Saving the issues report.
    # Reason: This report can be included in the project
    # as evidence of the data cleaning process.
    issues_df.to_csv(
        "data/cleaned/issues_report.csv",
        index=False
    )

    print("Issues report generated successfully.")
    print("\nIssues Summary:\n")
    print(issues_df)

generate_issues_report()