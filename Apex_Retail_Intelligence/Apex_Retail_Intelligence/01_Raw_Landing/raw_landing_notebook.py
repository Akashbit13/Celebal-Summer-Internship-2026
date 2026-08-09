# Databricks notebook source
# MAGIC %md
# MAGIC # Apex Retail Intelligence
# MAGIC
# MAGIC ## Notebook 1 : Raw & Landing Layer
# MAGIC
# MAGIC ### Objective
# MAGIC
# MAGIC The main objective of this notebook is to perform the first two phases of the Medallion Architecture.
# MAGIC
# MAGIC In this notebook, I will:
# MAGIC
# MAGIC - Read all historical datasets.
# MAGIC - Read all incremental datasets.
# MAGIC - Store the raw data.
# MAGIC - Convert the raw CSV files into Parquet format.
# MAGIC - Validate the data using audit files.
# MAGIC - Generate a PASS / FAIL report before moving to the Bronze layer.
# MAGIC
# MAGIC ### Technologies Used
# MAGIC - PySpark
# MAGIC - Databricks
# MAGIC - CSV
# MAGIC - Parquet
# MAGIC
# MAGIC ### Expected Outcome
# MAGIC
# MAGIC After completing this notebook, all datasets will be available in the Landing Layer and will be ready for further processing in the Bronze Layer.

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

# ================================================================
# Apex Retail Intelligence
# Notebook 1 : Raw & Landing Layer
# ================================================================

# Import commonly used Spark SQL functions.
# These functions will be used throughout the project.

from pyspark.sql.functions import *

# Import Spark SQL data types.
# These data types will be useful while transforming columns
# in the Silver layer.

from pyspark.sql.types import *

# Import datetime module.
# It will help us record timestamps whenever required.

from datetime import datetime

print("Libraries imported successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Spark Environment
# MAGIC
# MAGIC Before reading any dataset, it is a good practice to verify that the Spark session is working properly.
# MAGIC
# MAGIC In this step, I will check:
# MAGIC
# MAGIC - Spark Version
# MAGIC - Current User
# MAGIC - Current Working Directory
# MAGIC
# MAGIC This helps in confirming that the Databricks environment is ready.

# COMMAND ----------

# Display the current Spark version.

print("Spark Version :", spark.version)

# COMMAND ----------

# Display the username of the current Databricks workspace.

print("Current User :", spark.sql("SELECT current_user()").first()[0])

# COMMAND ----------

# Import os module.
# This module helps us interact with the operating system.

import os

# Display the current working directory.

print("Current Working Directory :")
print(os.getcwd())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Available File System
# MAGIC
# MAGIC Before loading the datasets, I will check the available file system.
# MAGIC
# MAGIC This step helps me understand where the datasets are stored inside Databricks.

# COMMAND ----------

# Display the root directories available in Databricks.

display(dbutils.fs.ls("/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 : Define Dataset Locations
# MAGIC
# MAGIC Before reading the datasets, I will define the location of all CSV files.
# MAGIC
# MAGIC Keeping all file paths in one place makes the notebook easier to manage.
# MAGIC
# MAGIC If the dataset location changes in future, I only need to update this section.

# COMMAND ----------

# List the Workspace folder

display(dbutils.fs.ls("dbfs:/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/datasets"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why am I doing this?
# MAGIC
# MAGIC Before loading any dataset, it is a good practice to keep all file paths in one place.
# MAGIC
# MAGIC This makes the notebook easy to maintain. If the dataset location changes in the future, I only need to update the path here instead of changing it throughout the notebook.

# COMMAND ----------

# ============================================================
# Step 1 : Define Dataset Paths
# ============================================================

# Store the common project paths.
# All datasets will be read from the Unity Catalog Volume.

RAW_PATH = "/Volumes/workspace/default/apex_retail_volume/raw"

LANDING_PATH = "/Volumes/workspace/default/apex_retail_volume/landing"

BRONZE_PATH = "/Volumes/workspace/default/apex_retail_volume/bronze"

SILVER_PATH = "/Volumes/workspace/default/apex_retail_volume/silver"

GOLD_PATH = "/Volumes/workspace/default/apex_retail_volume/gold"


# ---------------- Historical Datasets ----------------

customer_historical_path = f"{RAW_PATH}/historical/customer_historical.csv"

product_historical_path = f"{RAW_PATH}/historical/product_historical.csv"

sales_historical_path = f"{RAW_PATH}/historical/sales_historical.csv"


# ---------------- Incremental Datasets ----------------

customer_incremental_path = f"{RAW_PATH}/incremental/customer_incremental.csv"

product_incremental_path = f"{RAW_PATH}/incremental/product_incremental.csv"

sales_incremental_path = f"{RAW_PATH}/incremental/sales_incremental.csv"


# ---------------- Audit Datasets ----------------

customer_historical_audit_path = (
    f"{RAW_PATH}/audit/customer_historical_audit.csv"
)

customer_incremental_audit_path = (
    f"{RAW_PATH}/audit/customer_incrementalaudit.csv"
)

product_historical_audit_path = (
    f"{RAW_PATH}/audit/product_historical_audit.csv"
)

product_incremental_audit_path = (
    f"{RAW_PATH}/audit/product_incrementalaudit.csv"
)

sales_historical_audit_path = (
    f"{RAW_PATH}/audit/sales_historical_audit.csv"
)

sales_incremental_audit_path = (
    f"{RAW_PATH}/audit/sales_incrementalaudit.csv"
)

print("All dataset paths have been defined successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 : Check Dataset Folders
# MAGIC
# MAGIC In this step, I am checking whether all the required dataset folders are available.
# MAGIC
# MAGIC The project contains three types of datasets:
# MAGIC
# MAGIC - Historical Data
# MAGIC - Incremental Data
# MAGIC - Audit Data
# MAGIC
# MAGIC If all folders are available, I can proceed to read the CSV files in the next step.

# COMMAND ----------

# ============================================================
# Step 2 : Check Available Files
# ============================================================

# Display all files available in the Raw Layer.

print("Historical Folder")

display(dbutils.fs.ls(f"{RAW_PATH}/historical"))

print("Incremental Folder")

display(dbutils.fs.ls(f"{RAW_PATH}/incremental"))

print("Audit Folder")

display(dbutils.fs.ls(f"{RAW_PATH}/audit"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 : Read Historical Datasets
# MAGIC
# MAGIC In this step, I am loading the historical customer, product and sales datasets into PySpark DataFrames.
# MAGIC
# MAGIC These datasets contain the initial data that will be used to build the Bronze, Silver and Gold layers.

# COMMAND ----------

# ============================================================
# Step 3 : Read Historical CSV Files
# ============================================================

# Read the Customer Historical Dataset.
# Header is enabled because the first row contains column names.
# All columns are loaded as String type.

customer_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(customer_historical_path)
)

# Read the Product Historical Dataset.

product_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(product_historical_path)
)

# Read the Sales Historical Dataset.

sales_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(sales_historical_path)
)

print("Historical datasets loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 : Verify Dataset Files
# MAGIC
# MAGIC In this step, I am checking whether all the required CSV files are available in their respective folders.
# MAGIC
# MAGIC This helps avoid file path errors before reading the datasets.

# COMMAND ----------

# ============================================================
# Step 3 : Verify Dataset Files
# ============================================================

# Display all files available in the Raw folders.

print("Historical Files")
historical_files = [file.name for file in dbutils.fs.ls(f"{RAW_PATH}/historical")]
print(historical_files)

print("\nIncremental Files")
incremental_files = [file.name for file in dbutils.fs.ls(f"{RAW_PATH}/incremental")]
print(incremental_files)

print("\nAudit Files")
audit_files = [file.name for file in dbutils.fs.ls(f"{RAW_PATH}/audit")]
print(audit_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 : Read Historical Datasets
# MAGIC
# MAGIC Now I am loading the historical datasets into PySpark DataFrames.
# MAGIC
# MAGIC At this stage, I am only reading the data.
# MAGIC
# MAGIC No cleaning or transformation is performed in this step.

# COMMAND ----------

# ============================================================
# Step 4 : Read Historical Datasets
# ============================================================

# Read Customer Historical Dataset

customer_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(customer_historical_path)
)

# Read Product Historical Dataset

product_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(product_historical_path)
)

# Read Sales Historical Dataset

sales_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(sales_historical_path)
)

print("Historical datasets loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 : Preview Historical Datasets
# MAGIC
# MAGIC After loading the datasets, I am displaying a few records from each dataset.
# MAGIC
# MAGIC This helps me verify that the files have been loaded successfully.

# COMMAND ----------

# ============================================================
# Step 5 : Preview Historical Datasets
# ============================================================

# Display Customer Historical Dataset

display(customer_df)

# Display Product Historical Dataset

display(product_df)

# Display Sales Historical Dataset

display(sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 : Check Dataset Schema
# MAGIC
# MAGIC In this step, I am checking the schema of each historical dataset.
# MAGIC
# MAGIC All columns should be loaded as StringType because no transformation has been performed yet.

# COMMAND ----------

# ============================================================
# Step 6 : Check Dataset Schema
# ============================================================

# Display the schema of all historical datasets.

print("Customer Dataset Schema")
customer_df.printSchema()

print("\nProduct Dataset Schema")
product_df.printSchema()

print("\nSales Dataset Schema")
sales_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 : Check Dataset Size
# MAGIC
# MAGIC In this step, I am checking the number of records available in each historical dataset.
# MAGIC
# MAGIC This helps verify that all datasets have been loaded completely before moving to the Landing layer.

# COMMAND ----------

# ============================================================
# Step 7 : Check Dataset Size
# ============================================================

# Count the number of records in each historical dataset.

print("Customer Records :", customer_df.count())
print("Product Records  :", product_df.count())
print("Sales Records    :", sales_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 : Check Missing Values
# MAGIC
# MAGIC In this step, I am checking whether any columns contain missing (NULL) values.
# MAGIC
# MAGIC This helps identify incomplete records before moving to the Landing Layer.

# COMMAND ----------

# ============================================================
# Step 8 : Check Missing Values
# ============================================================

from pyspark.sql.functions import col, when, count

# Check missing values in Customer Dataset

print("Customer Dataset")

display(
    customer_df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in customer_df.columns
    ])
)

# Check missing values in Product Dataset

print("Product Dataset")

display(
    product_df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in product_df.columns
    ])
)

# Check missing values in Sales Dataset

print("Sales Dataset")

display(
    sales_df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in sales_df.columns
    ])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 : Check Duplicate Records
# MAGIC
# MAGIC In this step, I am checking whether duplicate records are present in the historical datasets.
# MAGIC
# MAGIC This helps me understand the quality of the raw data before moving to the Landing Layer.
# MAGIC
# MAGIC No duplicate records will be removed in this notebook. Data cleaning and deduplication will be performed in the Silver Layer.

# COMMAND ----------

# ============================================================
# Step 9 : Check Duplicate Records
# ============================================================

# Calculate total records and unique records for each dataset.

customer_total = customer_df.count()
customer_unique = customer_df.dropDuplicates().count()

product_total = product_df.count()
product_unique = product_df.dropDuplicates().count()

sales_total = sales_df.count()
sales_unique = sales_df.dropDuplicates().count()


print("Customer Dataset")
print(f"Total Records     : {customer_total}")
print(f"Unique Records    : {customer_unique}")
print(f"Duplicate Records : {customer_total - customer_unique}")


print("\nProduct Dataset")
print(f"Total Records     : {product_total}")
print(f"Unique Records    : {product_unique}")
print(f"Duplicate Records : {product_total - product_unique}")


print("\nSales Dataset")
print(f"Total Records     : {sales_total}")
print(f"Unique Records    : {sales_unique}")
print(f"Duplicate Records : {sales_total - sales_unique}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 : Display Dataset Summary
# MAGIC
# MAGIC In this step, I am creating a summary of all historical datasets.
# MAGIC
# MAGIC The summary includes:
# MAGIC
# MAGIC - Number of rows
# MAGIC - Number of columns
# MAGIC
# MAGIC This provides a quick overview of the datasets before moving to the next step.

# COMMAND ----------

# ============================================================
# Step 10 : Display Dataset Summary
# ============================================================

summary = [
    ("Customer", customer_df.count(), len(customer_df.columns)),
    ("Product", product_df.count(), len(product_df.columns)),
    ("Sales", sales_df.count(), len(sales_df.columns))
]

summary_df = spark.createDataFrame(
    summary,
    ["Dataset", "Rows", "Columns"]
)

display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 : Read Incremental Datasets
# MAGIC
# MAGIC In this step, I am loading the incremental datasets into PySpark DataFrames.
# MAGIC
# MAGIC At this stage, I am only reading the data.
# MAGIC
# MAGIC No cleaning or transformation is performed in this step.

# COMMAND ----------

# ============================================================
# Step 11 : Read Incremental Datasets
# ============================================================

# Read Customer Incremental Dataset

customer_incremental_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(customer_incremental_path)
)

# Read Product Incremental Dataset

product_incremental_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(product_incremental_path)
)

# Read Sales Incremental Dataset

sales_incremental_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(sales_incremental_path)
)

print("Incremental datasets loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 : Preview Incremental Datasets
# MAGIC
# MAGIC In this step, I am displaying the incremental datasets to verify that they have been loaded successfully.

# COMMAND ----------

# ============================================================
# Step 12 : Preview Incremental Datasets
# ============================================================

# Display Customer Incremental Dataset

display(customer_incremental_df)

# Display Product Incremental Dataset

display(product_incremental_df)

# Display Sales Incremental Dataset

display(sales_incremental_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 : Check Incremental Dataset Size
# MAGIC
# MAGIC In this step, I am checking the number of records available in each incremental dataset.
# MAGIC
# MAGIC This confirms that all incremental datasets have been loaded successfully before moving to the audit validation.

# COMMAND ----------

# ============================================================
# Step 13 : Check Incremental Dataset Size
# ============================================================

# Count the number of records in each incremental dataset.

print("Customer Incremental Records :", customer_incremental_df.count())
print("Product Incremental Records  :", product_incremental_df.count())
print("Sales Incremental Records    :", sales_incremental_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 : Read Audit Datasets
# MAGIC
# MAGIC In this step, I am loading the audit datasets into PySpark DataFrames.
# MAGIC
# MAGIC These audit files contain the expected record count for each dataset. The audit information will be used later to compare the expected and actual record counts and validate the data before moving to the next layer.

# COMMAND ----------

# ============================================================
# Step 14 : Read Audit Datasets
# ============================================================

# Read Customer Audit Datasets

customer_historical_audit_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(customer_historical_audit_path)
)

customer_incremental_audit_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(customer_incremental_audit_path)
)

# Read Product Audit Datasets

product_historical_audit_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(product_historical_audit_path)
)

product_incremental_audit_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(product_incremental_audit_path)
)

# Read Sales Audit Datasets

sales_historical_audit_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(sales_historical_audit_path)
)

sales_incremental_audit_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(sales_incremental_audit_path)
)

print("Audit datasets loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15 : Check Audit Dataset Size
# MAGIC
# MAGIC In this step, I am checking the number of records available in each audit dataset.
# MAGIC
# MAGIC This helps verify that all audit files have been loaded successfully before performing the audit validation.

# COMMAND ----------

# ============================================================
# Step 15 : Check Audit Dataset Size
# ============================================================

# Count the number of records in each audit dataset.

print("Customer Historical Audit   :", customer_historical_audit_df.count())
print("Customer Incremental Audit  :", customer_incremental_audit_df.count())

print("Product Historical Audit    :", product_historical_audit_df.count())
print("Product Incremental Audit   :", product_incremental_audit_df.count())

print("Sales Historical Audit      :", sales_historical_audit_df.count())
print("Sales Incremental Audit     :", sales_incremental_audit_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 16 : Compare Historical and Incremental Datasets
# MAGIC
# MAGIC In this step, I am comparing the number of records in the historical and incremental datasets.
# MAGIC
# MAGIC This comparison helps verify that both datasets have been loaded successfully and provides a quick overview before moving to the Landing Layer.

# COMMAND ----------

# ============================================================
# Step 16 : Compare Historical and Incremental Datasets
# ============================================================

comparison = [
    ("Customer", customer_df.count(), customer_incremental_df.count()),
    ("Product", product_df.count(), product_incremental_df.count()),
    ("Sales", sales_df.count(), sales_incremental_df.count())
]

comparison_df = spark.createDataFrame(
    comparison,
    ["Dataset", "Historical Records", "Incremental Records"]
)

display(comparison_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 17 : Create Landing Layer
# MAGIC
# MAGIC In this step, I am creating the Landing Layer for this project.
# MAGIC
# MAGIC The historical and incremental CSV datasets will be converted into Parquet format and stored in the Landing Layer.
# MAGIC
# MAGIC Parquet is a columnar storage format that improves storage efficiency and provides faster read performance for downstream processing.
# MAGIC
# MAGIC The Landing Layer will be used as the source for the Bronze Layer.

# COMMAND ----------

# ============================================================
# Step 17 : Convert Historical CSV Files to Parquet
# ============================================================

# Convert Customer Historical Dataset

customer_df.write.mode("overwrite").parquet(
    f"{LANDING_PATH}/historical/customer"
)

# Convert Product Historical Dataset

product_df.write.mode("overwrite").parquet(
    f"{LANDING_PATH}/historical/product"
)

# Convert Sales Historical Dataset

sales_df.write.mode("overwrite").parquet(
    f"{LANDING_PATH}/historical/sales"
)

print("Historical datasets converted to Parquet successfully.")

# COMMAND ----------

# ============================================================
# Step 18 : Convert Incremental CSV Files to Parquet
# ============================================================

# Convert Customer Incremental Dataset

customer_incremental_df.write.mode("overwrite").parquet(
    f"{LANDING_PATH}/incremental/customer"
)

# Convert Product Incremental Dataset

product_incremental_df.write.mode("overwrite").parquet(
    f"{LANDING_PATH}/incremental/product"
)

# Convert Sales Incremental Dataset

sales_incremental_df.write.mode("overwrite").parquet(
    f"{LANDING_PATH}/incremental/sales"
)

print("Incremental datasets converted to Parquet successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 19 : Verify Landing Layer
# MAGIC
# MAGIC In this step, I am verifying that all Parquet files have been created successfully in the Landing Layer.
# MAGIC
# MAGIC This ensures that the Landing Layer is ready for the Bronze Layer.

# COMMAND ----------

# ============================================================
# Step 19 : Verify Landing Layer
# ============================================================

print("Historical Landing Files")

display(dbutils.fs.ls(f"{LANDING_PATH}/historical"))

print("Incremental Landing Files")

display(dbutils.fs.ls(f"{LANDING_PATH}/incremental"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 20 : Landing Layer Summary
# MAGIC
# MAGIC The Raw and Landing Layer has been completed successfully.
# MAGIC
# MAGIC The following tasks were completed:
# MAGIC
# MAGIC - Loaded historical datasets
# MAGIC - Loaded incremental datasets
# MAGIC - Loaded audit datasets
# MAGIC - Validated dataset information
# MAGIC - Converted CSV files into Parquet format
# MAGIC - Verified Landing Layer files
# MAGIC
# MAGIC The Landing Layer is now ready for the Bronze Layer.

# COMMAND ----------

# ============================================================
# Step 20 : Convert Incremental CSV Files to Parquet
# ============================================================

# Save Customer Incremental Dataset

customer_incremental_df.write.mode("overwrite").parquet(
    f"{LANDING_PATH}/incremental/customer"
)

# Save Product Incremental Dataset

product_incremental_df.write.mode("overwrite").parquet(
    f"{LANDING_PATH}/incremental/product"
)

# Save Sales Incremental Dataset

sales_incremental_df.write.mode("overwrite").parquet(
    f"{LANDING_PATH}/incremental/sales"
)

print("Incremental datasets converted to Parquet successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 21 : Verify Parquet Files
# MAGIC
# MAGIC In this step, I am checking whether all historical and incremental datasets have been successfully converted into Parquet format.
# MAGIC
# MAGIC This helps verify that the Landing Layer has been created successfully and is ready for the Bronze Layer.

# COMMAND ----------

# ============================================================
# Step 21 : Verify Parquet Files
# ============================================================

print("Historical Parquet Files")

display(dbutils.fs.ls(f"{LANDING_PATH}/historical"))

print("Incremental Parquet Files")

display(dbutils.fs.ls(f"{LANDING_PATH}/incremental"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 22 : Validate Landing Data Using Audit Files
# MAGIC
# MAGIC In this step, I am validating the Landing Layer using the audit files.
# MAGIC
# MAGIC Each audit file contains the expected number of records for a dataset. I will compare the expected row count with the actual row count from the Landing Layer.
# MAGIC
# MAGIC If the expected and actual row counts match, the validation status will be marked as **PASS**. Otherwise, it will be marked as **FAIL**.
# MAGIC
# MAGIC This validation confirms that all datasets have been loaded correctly before proceeding to the Bronze Layer.

# COMMAND ----------

# ============================================================
# Step 22 : Read Expected Row Counts from Audit Files
# ============================================================

customer_hist_expected = int(customer_historical_audit_df.first()["row_count"])
customer_inc_expected = int(customer_incremental_audit_df.first()["row_count"])

product_hist_expected = int(product_historical_audit_df.first()["row_count"])
product_inc_expected = int(product_incremental_audit_df.first()["row_count"])

sales_hist_expected = int(sales_historical_audit_df.first()["row_count"])
sales_inc_expected = int(sales_incremental_audit_df.first()["row_count"])

print("Expected row counts loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 23 : Compare Expected and Actual Row Counts
# MAGIC
# MAGIC In this step, I am comparing the expected row counts from the audit files with the actual row counts from the historical and incremental datasets.
# MAGIC
# MAGIC If the expected and actual row counts are the same, the validation status will be marked as **PASS**. Otherwise, it will be marked as **FAIL**.
# MAGIC
# MAGIC This validation ensures that all datasets have been loaded correctly into the Landing Layer before moving to the Bronze Layer.

# COMMAND ----------

# ============================================================
# Step 23 : Compare Expected and Actual Row Counts
# ============================================================

audit_report = [

    (
        "Customer Historical",
        customer_hist_expected,
        customer_df.count(),
        "PASS" if customer_hist_expected == customer_df.count() else "FAIL"
    ),

    (
        "Customer Incremental",
        customer_inc_expected,
        customer_incremental_df.count(),
        "PASS" if customer_inc_expected == customer_incremental_df.count() else "FAIL"
    ),

    (
        "Product Historical",
        product_hist_expected,
        product_df.count(),
        "PASS" if product_hist_expected == product_df.count() else "FAIL"
    ),

    (
        "Product Incremental",
        product_inc_expected,
        product_incremental_df.count(),
        "PASS" if product_inc_expected == product_incremental_df.count() else "FAIL"
    ),

    (
        "Sales Historical",
        sales_hist_expected,
        sales_df.count(),
        "PASS" if sales_hist_expected == sales_df.count() else "FAIL"
    ),

    (
        "Sales Incremental",
        sales_inc_expected,
        sales_incremental_df.count(),
        "PASS" if sales_inc_expected == sales_incremental_df.count() else "FAIL"
    )

]

audit_report_df = spark.createDataFrame(
    audit_report,
    ["Dataset", "Expected Rows", "Actual Rows", "Validation Status"]
)

display(audit_report_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 24 : Landing Layer Summary
# MAGIC
# MAGIC The Raw and Landing Layer has been completed successfully.
# MAGIC
# MAGIC The following tasks were completed in this notebook:
# MAGIC
# MAGIC - Verified the available dataset files.
# MAGIC - Loaded historical datasets.
# MAGIC - Loaded incremental datasets.
# MAGIC - Loaded audit datasets.
# MAGIC - Checked the dataset schema.
# MAGIC - Verified record counts.
# MAGIC - Checked missing values.
# MAGIC - Checked duplicate records.
# MAGIC - Compared historical and incremental datasets.
# MAGIC - Converted CSV files into Parquet format.
# MAGIC - Validated the Landing Layer using audit files.
# MAGIC - Generated a PASS / FAIL validation report.
# MAGIC
# MAGIC The Landing Layer is now ready for the Bronze Layer, where the Parquet files will be converted into Delta Lake tables with metadata for further processing.

# COMMAND ----------

# ============================================================
# Step 24 : Landing Layer Summary
# ============================================================

print("Raw and Landing Layer completed successfully.")
print("Landing Layer is ready for the Bronze Layer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Summary
# MAGIC
# MAGIC ✔ Historical Data Loaded
# MAGIC
# MAGIC ✔ Incremental Data Loaded
# MAGIC
# MAGIC ✔ Audit Validation Completed
# MAGIC
# MAGIC ✔ Parquet Files Created
# MAGIC
# MAGIC ✔ Ready for Bronze Layer

# COMMAND ----------

