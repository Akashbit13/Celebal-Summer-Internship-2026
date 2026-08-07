# ==========================================================
# Week 8 - Mini Project
# File: run_queries.py
#
# Purpose:
# This file connects Python with the SQLite database
# and executes SQL queries.
# ==========================================================

# Importing required libraries.

# SQLite3 is used to connect
# with the SQLite database.
import sqlite3

# Pandas is used to display
# query results in table format.
import pandas as pd

# Creating database connection.
# Reason: Connect Python with SQLite database.
connection = sqlite3.connect("database/ecommerce.db")

print("=" * 50)
print("Python + SQLite Integration Started")
print("=" * 50)

# Writing SQL query.
# Reason: This query calculates
# total revenue for each category.

query = """
SELECT

    p.category,

    ROUND(

        SUM(

            oi.quantity *
            oi.unit_price *
            (1 - oi.discount_percent / 100.0)

        ),

        2

    ) AS total_revenue

FROM order_items oi

INNER JOIN products p
ON oi.product_id = p.product_id

GROUP BY

    p.category

ORDER BY

    total_revenue DESC;
"""

# Executing SQL query.
# Reason: Read SQL result
# into a Pandas DataFrame.

result = pd.read_sql(query, connection)

print("\nRevenue Per Category\n")

print(result)



# Closing database connection.
# Reason: Release database resources.

connection.close()

print("\nDatabase connection closed.")