# Databricks notebook source
# MAGIC %md
# MAGIC # Apex Retail Intelligence
# MAGIC
# MAGIC ## Notebook 2 : Bronze Layer
# MAGIC
# MAGIC ### Objective
# MAGIC
# MAGIC The objective of this notebook is to create the Bronze Layer using the Parquet files generated in the Landing Layer.
# MAGIC
# MAGIC In this notebook, I will:
# MAGIC
# MAGIC - Read the historical and incremental Parquet files from the Landing Layer.
# MAGIC - Store the data in Delta format.
# MAGIC - Add an ingestion timestamp to every record.
# MAGIC - Store historical and incremental data separately.
# MAGIC - Preserve all columns without any transformation.
# MAGIC
# MAGIC The Bronze Layer stores raw business data in Delta format and serves as the source for the Silver Layer.

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
# Apex Retail Intelligence
# Notebook 2 : Bronze Layer
# ============================================================

# Import required Spark SQL functions

from pyspark.sql.functions import *

# Import Spark SQL data types

from pyspark.sql.types import *

print("Libraries imported successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 : Define Bronze Layer Paths
# MAGIC
# MAGIC In this step, I am defining the Landing and Bronze Layer paths.
# MAGIC
# MAGIC The Landing Layer contains the Parquet files created in the previous notebook.
# MAGIC
# MAGIC The Bronze Layer will store the datasets in Delta format.

# COMMAND ----------

# ============================================================
# Step 1 : Define Project Paths
# ============================================================

LANDING_PATH = "/Volumes/workspace/default/apex_retail_volume/landing"

BRONZE_PATH = "/Volumes/workspace/default/apex_retail_volume/bronze"

print("Project paths defined successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 : Read Historical Parquet Files
# MAGIC
# MAGIC In this step, I am reading the historical Parquet files from the Landing Layer.
# MAGIC
# MAGIC These datasets will be used to create the Bronze Layer.

# COMMAND ----------

# ============================================================
# Step 2 : Read Historical Parquet Files
# ============================================================

# Read Customer Historical Dataset

customer_bronze_df = spark.read.parquet(
    f"{LANDING_PATH}/historical/customer"
)

# Read Product Historical Dataset

product_bronze_df = spark.read.parquet(
    f"{LANDING_PATH}/historical/product"
)

# Read Sales Historical Dataset

sales_bronze_df = spark.read.parquet(
    f"{LANDING_PATH}/historical/sales"
)

print("Historical Parquet files loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 : Preview Historical Datasets
# MAGIC
# MAGIC In this step, I am displaying the historical datasets to verify that the Parquet files have been loaded successfully.

# COMMAND ----------

# ============================================================
# Step 3 : Preview Historical Datasets
# ============================================================

display(customer_bronze_df)

display(product_bronze_df)

display(sales_bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 : Add Ingestion Timestamp
# MAGIC
# MAGIC In this step, I am adding an ingestion timestamp to each historical dataset.
# MAGIC
# MAGIC The `ingested_at` column stores the date and time when the records entered the Bronze Layer.
# MAGIC
# MAGIC This metadata helps track data ingestion and supports auditing.

# COMMAND ----------

# ============================================================
# Step 4 : Add Ingestion Timestamp
# ============================================================

from pyspark.sql.functions import current_timestamp

# Add ingestion timestamp to Customer Dataset

customer_bronze_df = customer_bronze_df.withColumn(
    "ingested_at",
    current_timestamp()
)

# Add ingestion timestamp to Product Dataset

product_bronze_df = product_bronze_df.withColumn(
    "ingested_at",
    current_timestamp()
)

# Add ingestion timestamp to Sales Dataset

sales_bronze_df = sales_bronze_df.withColumn(
    "ingested_at",
    current_timestamp()
)

print("Ingestion timestamp added successfully.")

# COMMAND ----------

# ============================================================
# Create Project Folder Structure
# ============================================================

dbutils.fs.mkdirs("/Volumes/workspace/default/apex_retail_volume/raw/historical")
dbutils.fs.mkdirs("/Volumes/workspace/default/apex_retail_volume/raw/incremental")
dbutils.fs.mkdirs("/Volumes/workspace/default/apex_retail_volume/raw/audit")

dbutils.fs.mkdirs("/Volumes/workspace/default/apex_retail_volume/landing/historical")
dbutils.fs.mkdirs("/Volumes/workspace/default/apex_retail_volume/landing/incremental")

dbutils.fs.mkdirs("/Volumes/workspace/default/apex_retail_volume/bronze/historical")
dbutils.fs.mkdirs("/Volumes/workspace/default/apex_retail_volume/bronze/incremental")

dbutils.fs.mkdirs("/Volumes/workspace/default/apex_retail_volume/silver")

dbutils.fs.mkdirs("/Volumes/workspace/default/apex_retail_volume/gold")

dbutils.fs.mkdirs("/Volumes/workspace/default/apex_retail_volume/kpi")

print("Project folders created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 : Verify Bronze Schema
# MAGIC
# MAGIC In this step, I am checking the schema after adding the ingestion timestamp.
# MAGIC
# MAGIC The new `ingested_at` column should be available in all Bronze datasets.

# COMMAND ----------

# ============================================================
# Step 5 : Verify Bronze Schema
# ============================================================

print("Customer Dataset Schema")
customer_bronze_df.printSchema()

print("\nProduct Dataset Schema")
product_bronze_df.printSchema()

print("\nSales Dataset Schema")
sales_bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 : Convert Historical Data into Delta Format
# MAGIC
# MAGIC In this step, I am storing the historical datasets in Delta format.
# MAGIC
# MAGIC The Bronze Layer stores raw business data without applying any transformations.
# MAGIC
# MAGIC The historical datasets will be written in overwrite mode because this is the initial load.

# COMMAND ----------

# ============================================================
# Step 6 : Write Historical Delta Tables
# ============================================================

# Write Customer Historical Dataset

customer_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{BRONZE_PATH}/historical/customer")

# Write Product Historical Dataset

product_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{BRONZE_PATH}/historical/product")

# Write Sales Historical Dataset

sales_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{BRONZE_PATH}/historical/sales")

print("Historical Delta tables created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 : Read Incremental Parquet Files
# MAGIC
# MAGIC In this step, I am reading the incremental Parquet files from the Landing Layer.
# MAGIC
# MAGIC These datasets contain the latest records that will be appended to the Bronze Layer.

# COMMAND ----------

# ============================================================
# Step 7 : Read Incremental Parquet Files
# ============================================================

# Read Customer Incremental Dataset

customer_incremental_bronze_df = spark.read.parquet(
    f"{LANDING_PATH}/incremental/customer"
)

# Read Product Incremental Dataset

product_incremental_bronze_df = spark.read.parquet(
    f"{LANDING_PATH}/incremental/product"
)

# Read Sales Incremental Dataset

sales_incremental_bronze_df = spark.read.parquet(
    f"{LANDING_PATH}/incremental/sales"
)

print("Incremental Parquet files loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 : Add Ingestion Timestamp to Incremental Data
# MAGIC
# MAGIC In this step, I am adding the ingestion timestamp to all incremental datasets.
# MAGIC
# MAGIC The timestamp records when the incremental data entered the Bronze Layer.
# MAGIC

# COMMAND ----------

# ============================================================
# Step 8 : Add Ingestion Timestamp
# ============================================================

from pyspark.sql.functions import current_timestamp

customer_incremental_bronze_df = customer_incremental_bronze_df.withColumn(
    "ingested_at",
    current_timestamp()
)

product_incremental_bronze_df = product_incremental_bronze_df.withColumn(
    "ingested_at",
    current_timestamp()
)

sales_incremental_bronze_df = sales_incremental_bronze_df.withColumn(
    "ingested_at",
    current_timestamp()
)

print("Incremental ingestion timestamp added successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 : Append Incremental Data to Bronze Layer
# MAGIC
# MAGIC In this step, I am storing the incremental datasets in Delta format.
# MAGIC
# MAGIC The data is written in append mode because the Bronze Layer stores every incoming record without removing duplicates.

# COMMAND ----------

# ============================================================
# Step 9 : Append Incremental Data to Bronze Layer
# ============================================================

# Append Customer Incremental Dataset

customer_incremental_bronze_df.write \
    .format("delta") \
    .mode("append") \
    .save(f"{BRONZE_PATH}/incremental/customer")

# Append Product Incremental Dataset

product_incremental_bronze_df.write \
    .format("delta") \
    .mode("append") \
    .save(f"{BRONZE_PATH}/incremental/product")

# Append Sales Incremental Dataset

sales_incremental_bronze_df.write \
    .format("delta") \
    .mode("append") \
    .save(f"{BRONZE_PATH}/incremental/sales")

print("Incremental Delta tables created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 : Verify Bronze Layer
# MAGIC
# MAGIC In this step, I am verifying that the historical and incremental Delta tables have been created successfully.
# MAGIC
# MAGIC This confirms that the Bronze Layer is ready for the Silver Layer.

# COMMAND ----------

# ============================================================
# Step 10 : Verify Bronze Layer
# ============================================================

print("Historical Bronze Files")

display(dbutils.fs.ls(f"{BRONZE_PATH}/historical"))

print("Incremental Bronze Files")

display(dbutils.fs.ls(f"{BRONZE_PATH}/incremental"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 : Display Bronze Layer Summary
# MAGIC
# MAGIC In this step, I am displaying a summary of the Bronze Layer.
# MAGIC
# MAGIC The summary shows the number of records available in the historical and incremental Bronze datasets.
# MAGIC
# MAGIC This helps verify that the data has been stored successfully in the Bronze Layer.

# COMMAND ----------

# ============================================================
# Step 11 : Display Bronze Layer Summary
# ============================================================

bronze_summary = [

    (
        "Customer Historical",
        customer_bronze_df.count()
    ),

    (
        "Product Historical",
        product_bronze_df.count()
    ),

    (
        "Sales Historical",
        sales_bronze_df.count()
    ),

    (
        "Customer Incremental",
        customer_incremental_bronze_df.count()
    ),

    (
        "Product Incremental",
        product_incremental_bronze_df.count()
    ),

    (
        "Sales Incremental",
        sales_incremental_bronze_df.count()
    )

]

bronze_summary_df = spark.createDataFrame(
    bronze_summary,
    ["Dataset", "Records"]
)

display(bronze_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 : Bronze Layer Summary
# MAGIC
# MAGIC The Bronze Layer has been created successfully.
# MAGIC
# MAGIC The following tasks were completed:
# MAGIC
# MAGIC - Read historical Parquet files from the Landing Layer.
# MAGIC - Read incremental Parquet files from the Landing Layer.
# MAGIC - Added the `ingested_at` timestamp column.
# MAGIC - Stored historical datasets in Delta format.
# MAGIC - Stored incremental datasets in Delta format using append mode.
# MAGIC - Verified the Bronze Layer.
# MAGIC - Generated the Bronze Layer summary.
# MAGIC
# MAGIC The Bronze Layer is now ready for the Silver Layer.

# COMMAND ----------

# ============================================================
# Step 12 : Bronze Layer Summary
# ============================================================

print("Bronze Layer completed successfully.")
print("Bronze Layer is ready for the Silver Layer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Summary
# MAGIC
# MAGIC ✔ Delta Tables Created
# MAGIC
# MAGIC ✔ Bronze Validation Completed
# MAGIC
# MAGIC ✔ Ready for Silver Layer

# COMMAND ----------

