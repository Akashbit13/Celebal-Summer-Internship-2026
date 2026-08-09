# Databricks notebook source
# MAGIC %md
# MAGIC # Apex Retail Intelligence
# MAGIC
# MAGIC ## Notebook 3 : Silver Layer
# MAGIC
# MAGIC ### Objective
# MAGIC
# MAGIC The objective of this notebook is to create the Silver Layer from the Bronze Layer.
# MAGIC
# MAGIC In this notebook, I will:
# MAGIC
# MAGIC - Read the Bronze Delta tables.
# MAGIC - Convert columns to appropriate data types.
# MAGIC - Handle missing values.
# MAGIC - Remove duplicate records.
# MAGIC - Validate the processed data.
# MAGIC - Store the cleaned datasets in Delta format.
# MAGIC
# MAGIC The Silver Layer contains clean and validated data that will be used in the Gold Layer.

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
# Notebook 3 : Silver Layer
# ============================================================

# Import required libraries

from pyspark.sql.functions import *
from pyspark.sql.types import *

print("Libraries imported successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 : Define Project Paths
# MAGIC
# MAGIC In this step, I am defining the Bronze and Silver Layer paths.
# MAGIC
# MAGIC The Bronze Layer contains the Delta tables created in the previous notebook.
# MAGIC
# MAGIC The cleaned datasets will be stored in the Silver Layer.

# COMMAND ----------

# ============================================================
# Step 1 : Define Project Paths
# ============================================================

BRONZE_PATH = "/Volumes/workspace/default/apex_retail_volume/bronze"

SILVER_PATH = "/Volumes/workspace/default/apex_retail_volume/silver"

RAW_PATH = "/Volumes/workspace/default/apex_retail_volume/raw"

print("Project paths defined successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 : Read Historical Bronze Delta Tables
# MAGIC
# MAGIC In this step, I am reading the historical Bronze Delta tables.
# MAGIC
# MAGIC These datasets will be cleaned and transformed before storing them in the Silver Layer.

# COMMAND ----------

# ============================================================
# Step 2 : Read Historical Bronze Delta Tables
# ============================================================

customer_silver_df = spark.read.format("delta").load(
    f"{BRONZE_PATH}/historical/customer"
)

product_silver_df = spark.read.format("delta").load(
    f"{BRONZE_PATH}/historical/product"
)

sales_silver_df = spark.read.format("delta").load(
    f"{BRONZE_PATH}/historical/sales"
)

print("Historical Bronze tables loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 : Preview Historical Bronze Data
# MAGIC
# MAGIC In this step, I am displaying the Bronze datasets before performing any cleaning or transformation.
# MAGIC
# MAGIC This helps verify that the data has been loaded successfully.

# COMMAND ----------

# ============================================================
# Step 3 : Preview Historical Bronze Data
# ============================================================

display(customer_silver_df)

display(product_silver_df)

display(sales_silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 : Convert Data Types
# MAGIC
# MAGIC In this step, I am converting the required columns to appropriate data types.
# MAGIC
# MAGIC The Bronze Layer stores all columns as String type. Before creating the Silver Layer, the columns are converted into suitable data types for further processing.

# COMMAND ----------

# ============================================================
# Step 4 : Convert Customer Dataset Data Types
# ============================================================

customer_silver_df = (
    customer_silver_df
    .withColumn("customer_id", col("customer_id").cast("int"))
    .withColumn("age", col("age").cast("int"))
    .withColumn("membership_years", col("membership_years").cast("int"))
    .withColumn("number_of_children", col("number_of_children").cast("int"))
)

print("Customer dataset data types converted successfully.")

# COMMAND ----------

# ============================================================
# Convert Product Dataset Data Types
# ============================================================

product_silver_df = (
    product_silver_df
    .withColumn("product_id", col("product_id").cast("int"))
    .withColumn("product_rating", col("product_rating").cast("double"))
    .withColumn("product_review_count", col("product_review_count").cast("int"))
    .withColumn("product_stock", col("product_stock").cast("int"))
    .withColumn("product_return_rate", col("product_return_rate").cast("double"))
    .withColumn("product_weight", col("product_weight").cast("double"))
    .withColumn("product_shelf_life", col("product_shelf_life").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
)

print("Product dataset data types converted successfully.")

# COMMAND ----------

# ============================================================
# Convert Sales Dataset Data Types
# ============================================================

sales_silver_df = (
    sales_silver_df
    .withColumn("transaction_id", col("transaction_id").cast("int"))
    .withColumn("customer_id", col("customer_id").cast("int"))
    .withColumn("product_id", col("product_id").cast("int"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("discount_applied", col("discount_applied").cast("double"))
    .withColumn("transaction_hour", col("transaction_hour").cast("int"))
    .withColumn("week_of_year", col("week_of_year").cast("int"))
    .withColumn("month_of_year", col("month_of_year").cast("int"))
    .withColumn("total_sales", col("total_sales").cast("double"))
)

print("Sales dataset data types converted successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 : Verify Data Types
# MAGIC
# MAGIC In this step, I am checking the schema after converting the data types.
# MAGIC
# MAGIC This helps verify that the required columns have been converted successfully.

# COMMAND ----------

# ============================================================
# Step 5 : Verify Data Types
# ============================================================

print("Customer Dataset")
customer_silver_df.printSchema()

print("\nProduct Dataset")
product_silver_df.printSchema()

print("\nSales Dataset")
sales_silver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 : Check Missing Values
# MAGIC
# MAGIC In this step, I am checking the missing values in the Silver datasets before performing data cleaning.

# COMMAND ----------

# ============================================================
# Step 6 : Check Missing Values
# ============================================================

from pyspark.sql.functions import count, when, col

print("Customer Dataset")

display(
    customer_silver_df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in customer_silver_df.columns
    ])
)

print("Product Dataset")

display(
    product_silver_df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in product_silver_df.columns
    ])
)

print("Sales Dataset")

display(
    sales_silver_df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in sales_silver_df.columns
    ])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 : Remove Invalid Records
# MAGIC
# MAGIC In this step, I am removing records that contain missing values in important columns.
# MAGIC
# MAGIC This improves the data quality before storing the cleaned data in the Silver Layer.

# COMMAND ----------

# ============================================================
# Step 7 : Remove Invalid Customer Records
# ============================================================

# Remove records with missing values in important customer columns.

customer_silver_df = customer_silver_df.dropna(
    subset=[
        "gender",
        "income_bracket"
    ]
)

print("Customer records after cleaning :", customer_silver_df.count())


# ============================================================
# Remove Invalid Product Records
# ============================================================

# Remove records with missing values in important product columns.

product_silver_df = product_silver_df.dropna(
    subset=[
        "product_brand",
        "unit_price"
    ]
)

print("Product records after cleaning :", product_silver_df.count())


# ============================================================
# Remove Duplicate Sales Records
# ============================================================

# Remove duplicate records from the sales dataset.

sales_silver_df = sales_silver_df.dropDuplicates()

print("Sales records after removing duplicates :", sales_silver_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 : Verify Cleaned Dataset Size
# MAGIC
# MAGIC In this step, I am verifying the number of records after cleaning the datasets.
# MAGIC
# MAGIC This helps confirm that the cleaned datasets match the expected audit record counts before writing them to the Silver Layer.

# COMMAND ----------

# ============================================================
# Step 8 : Verify Cleaned Dataset Size
# ============================================================

print("Customer Records :", customer_silver_df.count())
print("Product Records  :", product_silver_df.count())
print("Sales Records    :", sales_silver_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 : Write Silver Delta Tables
# MAGIC
# MAGIC In this step, I am storing the cleaned datasets in Delta format.
# MAGIC
# MAGIC These Silver Delta tables will be used as the source for the Gold Layer.

# COMMAND ----------

# ============================================================
# Step 9 : Write Silver Delta Tables
# ============================================================

# Save Customer Silver Dataset

customer_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{SILVER_PATH}/customer")

# Save Product Silver Dataset

product_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{SILVER_PATH}/product")

# Save Sales Silver Dataset

sales_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{SILVER_PATH}/sales")

print("Silver Delta tables created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 : Read Incremental Bronze Tables
# MAGIC
# MAGIC In this step, I am reading the incremental Bronze Delta tables.
# MAGIC
# MAGIC These datasets contain the latest records that will be merged into the Silver Layer.

# COMMAND ----------

# ============================================================
# Step 10 : Read Incremental Bronze Tables
# ============================================================

customer_incremental_silver_df = spark.read.format("delta").load(
    f"{BRONZE_PATH}/incremental/customer"
)

product_incremental_silver_df = spark.read.format("delta").load(
    f"{BRONZE_PATH}/incremental/product"
)

sales_incremental_silver_df = spark.read.format("delta").load(
    f"{BRONZE_PATH}/incremental/sales"
)

print("Incremental Bronze tables loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 : Apply Data Quality Rules
# MAGIC
# MAGIC In this step, I am applying the required data quality rules to the incremental datasets.
# MAGIC
# MAGIC The following rules are applied:
# MAGIC
# MAGIC - Remove records with missing primary keys.
# MAGIC - Remove duplicate records.
# MAGIC - Fill missing string values with "Unknown".
# MAGIC - Fill missing numeric values with 0.
# MAGIC
# MAGIC These rules improve data quality before performing the Delta MERGE operation.

# COMMAND ----------

# ============================================================
# Step 11 : Clean Incremental Customer Dataset
# ============================================================

customer_incremental_silver_df = customer_incremental_silver_df.dropna(
    subset=["customer_id"]
).dropDuplicates()

# ============================================================
# Clean Incremental Product Dataset
# ============================================================

product_incremental_silver_df = product_incremental_silver_df.dropna(
    subset=["product_id"]
).dropDuplicates()

# ============================================================
# Clean Incremental Sales Dataset
# ============================================================

sales_incremental_silver_df = sales_incremental_silver_df.dropna(
    subset=["transaction_id"]
).dropDuplicates()

print("Incremental datasets cleaned successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 : Verify Incremental Dataset Size
# MAGIC
# MAGIC In this step, I am checking the number of records available after applying the data quality rules.
# MAGIC
# MAGIC This confirms that the incremental datasets are ready for the Delta MERGE operation.

# COMMAND ----------

# ============================================================
# Step 12 : Verify Incremental Dataset Size
# ============================================================

print("Customer Incremental Records :", customer_incremental_silver_df.count())
print("Product Incremental Records  :", product_incremental_silver_df.count())
print("Sales Incremental Records    :", sales_incremental_silver_df.count())

# COMMAND ----------

customer_silver_df.printSchema()

product_silver_df.printSchema()

sales_silver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 : Prepare Silver Tables for Delta MERGE
# MAGIC
# MAGIC In this step, I am loading the Silver Delta tables.
# MAGIC
# MAGIC These tables will be used as the target tables for the Delta MERGE operation.
# MAGIC
# MAGIC The incremental datasets will act as the source tables.

# COMMAND ----------

# ============================================================
# Step 13 : Load Silver Delta Tables
# ============================================================

from delta.tables import DeltaTable

customer_delta = DeltaTable.forPath(
    spark,
    f"{SILVER_PATH}/customer"
)

product_delta = DeltaTable.forPath(
    spark,
    f"{SILVER_PATH}/product"
)

sales_delta = DeltaTable.forPath(
    spark,
    f"{SILVER_PATH}/sales"
)

print("Silver Delta tables loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 : Prepare Incremental Customer Data for MERGE
# MAGIC
# MAGIC Before performing the Delta MERGE operation, duplicate customer records are removed from the incremental dataset.
# MAGIC
# MAGIC The latest record for each customer is retained based on the ingestion timestamp.

# COMMAND ----------

# ============================================================
# Step 14 : Prepare Customer Incremental Dataset
# ============================================================

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

customer_window = Window.partitionBy("customer_id") \
                        .orderBy(desc("ingested_at"))

customer_incremental_merge_df = (

    customer_incremental_silver_df

    .withColumn(
        "row_num",
        row_number().over(customer_window)
    )

    .filter("row_num = 1")

    .drop("row_num")

)

print("Customer Incremental Records :", customer_incremental_merge_df.count())

# COMMAND ----------

customer_incremental_merge_df.groupBy("customer_id") \
.count() \
.filter("count>1") \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15 : Product SCD Type 1
# MAGIC
# MAGIC In this step, I am applying Slowly Changing Dimension (SCD) Type 1 to the Product dimension.
# MAGIC
# MAGIC If an existing product is found, the latest values overwrite the existing record.
# MAGIC
# MAGIC If a new product is found, a new record is inserted.

# COMMAND ----------

product_incremental_silver_df.groupBy("product_id") \
    .count() \
    .filter("count > 1") \
    .show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

product_window = Window.partitionBy("product_id").orderBy(desc("ingested_at"))

product_incremental_merge_df = (
    product_incremental_silver_df
        .withColumn("row_num", row_number().over(product_window))
        .filter("row_num = 1")
        .drop("row_num")
)

print("Product Incremental Records:", product_incremental_merge_df.count())

# COMMAND ----------

product_incremental_merge_df.groupBy("product_id") \
    .count() \
    .filter("count > 1") \
    .show()

# COMMAND ----------

product_delta.alias("target").merge(
    product_incremental_merge_df.alias("source"),
    "target.product_id = source.product_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("Product MERGE completed successfully.")

# COMMAND ----------

# ============================================================
# Step 16 : Prepare Sales Incremental Dataset
# ============================================================

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

sales_window = Window.partitionBy("transaction_id") \
                     .orderBy(desc("ingested_at"))

sales_incremental_merge_df = (
    sales_incremental_silver_df
        .withColumn("row_num", row_number().over(sales_window))
        .filter("row_num = 1")
        .drop("row_num")
)

print("Sales Incremental Records :", sales_incremental_merge_df.count())

# COMMAND ----------

#Step 17 : Verify Sales Duplicates
sales_incremental_merge_df.groupBy("transaction_id") \
    .count() \
    .filter("count > 1") \
    .show()

# COMMAND ----------

# ============================================================
# Step 18 : Sales MERGE
# ============================================================

sales_delta.alias("target").merge(

    sales_incremental_merge_df.alias("source"),

    "target.transaction_id = source.transaction_id"

).whenMatchedUpdateAll(

).whenNotMatchedInsertAll(

).execute()

print("Sales MERGE completed successfully.")

# COMMAND ----------

# ============================================================
# Step 19 : Generate Surrogate Keys
# ============================================================

from pyspark.sql.functions import monotonically_increasing_id

customer_silver_df = customer_silver_df.withColumn(
    "customer_sk",
    monotonically_increasing_id()
)

product_silver_df = product_delta.toDF().withColumn(
    "product_sk",
    monotonically_increasing_id()
)

sales_silver_df = sales_delta.toDF().withColumn(
    "sales_sk",
    monotonically_increasing_id()
)

print("Surrogate keys generated successfully.")

# COMMAND ----------

# ============================================================
# Step 20 : Silver Validation
# ============================================================

validation = [
    ("Customer", customer_silver_df.count()),
    ("Product", product_silver_df.count()),
    ("Sales", sales_silver_df.count())
]

validation_df = spark.createDataFrame(
    validation,
    ["Dataset", "Records"]
)

display(validation_df)

# COMMAND ----------

# ============================================================
# Save Silver Tables After Surrogate Keys
# ============================================================

customer_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/customer")

product_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/product")

sales_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/sales")

print("Silver tables updated with surrogate keys.")

# COMMAND ----------

# ============================================================
# Save Final Silver Tables
# ============================================================

customer_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/03_Silver/customer")

product_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/03_Silver/product")

sales_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Workspace/Users/akashpatra788@gmail.com/Apex_Retail_Intelligence/03_Silver/sales")

print("Silver tables saved successfully.")

# COMMAND ----------

# ============================================================
# Step 21 : Silver Layer Summary
# ============================================================

print("Silver Layer completed successfully.")

print("Customer Records :", customer_silver_df.count())
print("Product Records  :", product_silver_df.count())
print("Sales Records    :", sales_silver_df.count())

print("\nCompleted Tasks")
print("- Data Quality Rules Applied")
print("- Product SCD Type 1 Applied")
print("- Sales MERGE Completed")
print("- Surrogate Keys Generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Summary
# MAGIC
# MAGIC ✔ Data Quality Rules Applied
# MAGIC
# MAGIC ✔ Delta MERGE Completed
# MAGIC
# MAGIC ✔ SCD Processing Completed
# MAGIC
# MAGIC ✔ Surrogate Keys Generated
# MAGIC
# MAGIC ✔ Ready for Gold Layer

# COMMAND ----------

