# Databricks notebook source
# MAGIC %md
# MAGIC # Apex Retail Intelligence
# MAGIC
# MAGIC ## KPI Dashboard
# MAGIC
# MAGIC ### Objective
# MAGIC Generate business insights from Gold Layer.

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

# MAGIC %md
# MAGIC ## Step 1 : Load Gold Tables
# MAGIC
# MAGIC In this step, I am loading the Gold Layer tables into PySpark DataFrames.
# MAGIC
# MAGIC These tables will be used to calculate business KPIs.

# COMMAND ----------

# ============================================================
# Step 1 : Load Gold Tables
# ============================================================

dim_customer_df = spark.read.format("delta").load(
    "dbfs:/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/04_Gold/dim_customer"
)

dim_product_df = spark.read.format("delta").load(
    "dbfs:/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/04_Gold/dim_product"
)

dim_date_df = spark.read.format("delta").load(
    "dbfs:/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/04_Gold/dim_date"
)

fact_sales_df = spark.read.format("delta").load(
    "dbfs:/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/04_Gold/fact_sales"
)

print("Gold tables loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 : Preview Gold Tables
# MAGIC
# MAGIC In this step, I am displaying the Gold Layer tables to verify that they have been loaded successfully.

# COMMAND ----------

# ============================================================
# Step 2 : Preview Gold Tables
# ============================================================

display(dim_customer_df.limit(5))

display(dim_product_df.limit(5))

display(dim_date_df.limit(5))

display(fact_sales_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 : Calculate Total Sales
# MAGIC
# MAGIC In this step, I am calculating the total sales amount.

# COMMAND ----------

# ============================================================
# Step 3 : Total Sales
# ============================================================

from pyspark.sql.functions import sum

total_sales = fact_sales_df.select(
    sum("total_sales")
).collect()[0][0]

print("Total Sales :", total_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 : Total Customers
# MAGIC
# MAGIC In this step, I am calculating the total number of customers.

# COMMAND ----------

# ============================================================
# Step 4 : Total Customers
# ============================================================

print("Total Customers :", dim_customer_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 : Total Products
# MAGIC
# MAGIC In this step, I am calculating the total number of products.

# COMMAND ----------

# ============================================================
# Step 5 : Total Products
# ============================================================

print("Total Products :", dim_product_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 : Calculate Average Sales
# MAGIC
# MAGIC In this step, I am calculating the average sales amount per transaction.

# COMMAND ----------

# ============================================================
# Step 6 : Average Sales
# ============================================================

from pyspark.sql.functions import avg

average_sales = fact_sales_df.select(
    avg("total_sales")
).collect()[0][0]

print("Average Sales :", average_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 : Top 10 Products
# MAGIC
# MAGIC In this step, I am identifying the top 10 products based on total sales.

# COMMAND ----------

# ============================================================
# Step 7 : Top 10 Products
# ============================================================

from pyspark.sql.functions import sum

top_products = (

    fact_sales_df

    .groupBy("product_id")

    .agg(
        sum("total_sales").alias("Total Sales")
    )

    .orderBy(
        "Total Sales",
        ascending=False
    )

    .limit(10)

)

display(top_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 : Top 10 Customers
# MAGIC
# MAGIC In this step, I am identifying the top 10 customers based on total sales.

# COMMAND ----------

# ============================================================
# Step 8 : Top 10 Customers
# ============================================================

top_customers = (

    fact_sales_df

    .groupBy("customer_id")

    .agg(
        sum("total_sales").alias("Total Sales")
    )

    .orderBy(
        "Total Sales",
        ascending=False
    )

    .limit(10)

)

display(top_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 : Monthly Sales
# MAGIC
# MAGIC In this step, I am calculating monthly sales for business reporting.

# COMMAND ----------

# ============================================================
# Step 9 : Monthly Sales
# ============================================================

from pyspark.sql.functions import month

monthly_sales = (

    fact_sales_df

    .withColumn(
        "Month",
        month("transaction_date")
    )

    .groupBy("Month")

    .agg(
        sum("total_sales").alias("Total Sales")
    )

    .orderBy("Month")

)

display(monthly_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 : KPI Summary
# MAGIC
# MAGIC The KPI analysis has been completed successfully.
# MAGIC
# MAGIC The following business metrics were generated:
# MAGIC
# MAGIC - Total Sales
# MAGIC - Average Sales
# MAGIC - Total Customers
# MAGIC - Total Products
# MAGIC - Top 10 Products
# MAGIC - Top 10 Customers
# MAGIC - Monthly Sales
# MAGIC
# MAGIC The Apex Retail Intelligence project has been completed successfully.

# COMMAND ----------

# ============================================================
# Step 10 : KPI Summary
# ============================================================

print("KPI Notebook completed successfully.")

print("Total Sales      :", total_sales)
print("Average Sales    :", average_sales)
print("Total Customers  :", dim_customer_df.count())
print("Total Products   :", dim_product_df.count())

print("\nBusiness KPI generation completed successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project Completed
# MAGIC
# MAGIC ✔ Total Sales Calculated
# MAGIC
# MAGIC ✔ Average Sales Calculated
# MAGIC
# MAGIC ✔ Top Products Generated
# MAGIC
# MAGIC ✔ Monthly Sales Generated
# MAGIC
# MAGIC ✔ Apex Retail Intelligence Pipeline Completed Successfully

# COMMAND ----------

