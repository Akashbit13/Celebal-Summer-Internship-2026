# Databricks notebook source
# MAGIC %md
# MAGIC # Apex Retail Intelligence
# MAGIC
# MAGIC ## Gold Layer
# MAGIC
# MAGIC ### Objective
# MAGIC Create Dimension and Fact tables for business reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project Pipeline
# MAGIC
# MAGIC ```text
# MAGIC                 Apex Retail Intelligence Pipeline
# MAGIC
# MAGIC  Historical CSV          Incremental CSV
# MAGIC         │                      │
# MAGIC         └──────────┬───────────┘
# MAGIC                    ▼
# MAGIC             Raw Landing Layer
# MAGIC                    │
# MAGIC                    ▼
# MAGIC              Bronze Layer
# MAGIC             (Delta Storage)
# MAGIC                    │
# MAGIC                    ▼
# MAGIC               Silver Layer
# MAGIC       (Cleaning + MERGE + SCD)
# MAGIC                    │
# MAGIC                    ▼
# MAGIC                Gold Layer
# MAGIC       (Dimensions & Fact Tables)
# MAGIC                    │
# MAGIC                    ▼
# MAGIC               KPI Dashboard
# MAGIC ```

# COMMAND ----------

# ============================================================
# Step 1 : Create Gold Layer Paths
# ============================================================

GOLD_PATH = "/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/04_Gold"

DIM_CUSTOMER_PATH = f"{GOLD_PATH}/dim_customer"
DIM_PRODUCT_PATH = f"{GOLD_PATH}/dim_product"
DIM_DATE_PATH = f"{GOLD_PATH}/dim_date"
FACT_SALES_PATH = f"{GOLD_PATH}/fact_sales"

print("Gold paths created successfully.")

# COMMAND ----------

# ============================================================
# Step 1.1 : Define Silver Path
# ============================================================

SILVER_PATH = "/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/03_Silver"

print(SILVER_PATH)

# COMMAND ----------

# ============================================================
# Step 2 : Load Silver Delta Tables
# ============================================================

customer_gold_df = spark.read.format("delta").load(
    "dbfs:/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/03_Silver/customer"
)

product_gold_df = spark.read.format("delta").load(
    "dbfs:/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/03_Silver/product"
)

sales_gold_df = spark.read.format("delta").load(
    "dbfs:/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/03_Silver/sales"
)

print("Silver tables loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 : Preview Silver Tables
# MAGIC
# MAGIC In this step, I am displaying the Silver Layer datasets.
# MAGIC
# MAGIC This helps verify that the cleaned data has been loaded successfully before creating the Gold Layer dimensions and fact table.

# COMMAND ----------

# ============================================================
# Step 3 : Preview Silver Tables
# ============================================================

display(customer_gold_df.limit(5))
display(product_gold_df.limit(5))
display(sales_gold_df.limit(5))

# COMMAND ----------

# ============================================================
# Step 4 : Check Dataset Size
# ============================================================

print("Customer :", customer_gold_df.count())
print("Product  :", product_gold_df.count())
print("Sales    :", sales_gold_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 : Create Customer Dimension
# MAGIC
# MAGIC In this step, I am creating the Customer Dimension table from the Silver Layer.
# MAGIC
# MAGIC Only the required customer attributes are selected for analytical reporting.

# COMMAND ----------

# ============================================================
# Step 5 : Create Customer Dimension
# ============================================================

dim_customer_df = customer_gold_df.select(
    "customer_sk",
    "customer_id",
    "age",
    "gender",
    "income_bracket",
    "customer_city",
    "customer_state"
)

display(dim_customer_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 : Create Product Dimension
# MAGIC
# MAGIC In this step, I am creating the Product Dimension table from the Silver Layer.
# MAGIC
# MAGIC Only the required product attributes are selected for reporting and analysis.

# COMMAND ----------

# ============================================================
# Step 6 : Create Product Dimension
# ============================================================

dim_product_df = product_gold_df.select(
    "product_sk",
    "product_id",
    "product_name",
    "product_brand",
    "product_category",
    "unit_price"
)

display(dim_product_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 : Create Date Dimension
# MAGIC
# MAGIC In this step, I am creating the Date Dimension from the Sales dataset.
# MAGIC
# MAGIC This dimension stores useful calendar attributes for reporting.

# COMMAND ----------

# ============================================================
# Step 7 : Create Date Dimension
# ============================================================

from pyspark.sql.functions import (
    to_date,
    year,
    month,
    weekofyear,
    dayofmonth,
    dayofweek
)

dim_date_df = (
    sales_gold_df
    .select("transaction_date")
    .distinct()
    .withColumn("date", to_date("transaction_date"))
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
    .withColumn("week", weekofyear("date"))
    .withColumn("day", dayofmonth("date"))
    .withColumn("day_of_week", dayofweek("date"))
)

display(dim_date_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 : Create Fact Sales Table
# MAGIC
# MAGIC In this step, I am creating the Fact Sales table.
# MAGIC
# MAGIC The fact table combines sales transactions with customer and product surrogate keys for analytical reporting.

# COMMAND ----------

# ============================================================
# Step 8 : Create Fact Sales Table
# ============================================================

fact_sales_df = sales_gold_df.select(
    "sales_sk",
    "transaction_id",
    "customer_id",
    "product_id",
    "transaction_date",
    "quantity",
    "unit_price",
    "discount_applied",
    "total_sales"
)

display(fact_sales_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 : Save Gold Tables
# MAGIC
# MAGIC In this step, I am storing all Gold Layer tables in Delta format.

# COMMAND ----------

# ============================================================
# Step 9 : Save Gold Tables
# ============================================================

dim_customer_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/04_Gold/dim_customer")

dim_product_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/04_Gold/dim_product")

dim_date_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/04_Gold/dim_date")

fact_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/04_Gold/fact_sales")

print("Gold tables saved successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 : Validate Gold Layer
# MAGIC
# MAGIC In this step, I am validating the Gold Layer by checking the record count of each Gold table.

# COMMAND ----------

# ============================================================
# Step 10 : Gold Validation
# ============================================================

validation = [

    ("Customer Dimension", dim_customer_df.count()),

    ("Product Dimension", dim_product_df.count()),

    ("Date Dimension", dim_date_df.count()),

    ("Fact Sales", fact_sales_df.count())

]

validation_df = spark.createDataFrame(
    validation,
    ["Table", "Records"]
)

display(validation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 : Gold Layer Summary
# MAGIC
# MAGIC The Gold Layer has been completed successfully.
# MAGIC
# MAGIC The following tables were created:
# MAGIC
# MAGIC - Customer Dimension
# MAGIC - Product Dimension
# MAGIC - Date Dimension
# MAGIC - Fact Sales
# MAGIC
# MAGIC The Gold Layer is now ready for KPI analysis.

# COMMAND ----------

# ============================================================
# Step 11 : Gold Layer Summary
# ============================================================

print("Gold Layer completed successfully.")

print("Customer Dimension :", dim_customer_df.count())
print("Product Dimension  :", dim_product_df.count())
print("Date Dimension     :", dim_date_df.count())
print("Fact Sales         :", fact_sales_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Summary
# MAGIC
# MAGIC ✔ Customer Dimension Created
# MAGIC
# MAGIC ✔ Product Dimension Created
# MAGIC
# MAGIC ✔ Date Dimension Created
# MAGIC
# MAGIC ✔ Fact Sales Created
# MAGIC
# MAGIC ✔ Ready for KPI Analysis

# COMMAND ----------

