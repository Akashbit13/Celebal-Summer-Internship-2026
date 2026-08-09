# Apex Retail Intelligence

## CEI'26 Major Project

An end-to-end retail data engineering pipeline built using **Databricks, PySpark, and Delta Lake**. The project follows a layered architecture to ingest, clean, transform, store, and analyze retail customer, product, and sales data.

---

## Project Overview

**Pipeline:** Raw Landing → Bronze → Silver → Gold → KPI

The Apex Retail Intelligence project processes historical and incremental retail datasets through multiple data engineering layers.

### Objectives

- Ingest historical and incremental datasets.
- Store and validate raw data.
- Convert raw CSV data into Parquet format.
- Store processed data using Delta Lake.
- Apply data quality and cleansing rules.
- Remove invalid and duplicate records.
- Perform incremental processing using Delta MERGE.
- Apply Product SCD Type 1 handling.
- Process sales transactions with deduplication.
- Generate surrogate keys for analytical joins.
- Build Gold dimension and fact tables.
- Generate business KPIs and visual insights.

---

# Architecture

```text
                    Apex Retail Intelligence

        Historical CSV              Incremental CSV
               │                         │
               └────────────┬────────────┘
                            ▼
                    Raw Landing Layer
                            │
                            ▼
                      Bronze Layer
                    (Delta Storage)
                            │
                            ▼
                       Silver Layer
              (Cleaning + DQ + MERGE)
                            │
                            ▼
                        Gold Layer
              (Dimensions + Fact Tables)
                            │
                            ▼
                        KPI Layer
                  (Business Insights)
```

---

# Technology Stack

| Technology | Purpose |
|---|---|
| Databricks | Data engineering and notebook execution |
| PySpark | Data processing and transformation |
| Delta Lake | Reliable table storage and MERGE operations |
| Python | Pipeline implementation |
| CSV | Source data format |
| Parquet | Landing data storage |
| Delta | Bronze, Silver and Gold storage |

---

# Project Structure

```text
Apex_Retail_Intelligence/
│
├── 01_Raw_Landing/
│   └── raw_landing_notebook.ipynb
│
├── 02_Bronze/
│   └── bronze_notebook.ipynb
│
├── 03_Silver/
│   └── silver_notebook.ipynb
│
├── 04_Gold/
│   └── gold_notebook.ipynb
│
├── 05_KPI/
│   └── kpi_notebook.ipynb
│
└── datasets/
```

---

# 1. Raw Landing Layer

The Raw Landing Layer handles the initial ingestion of retail datasets.

## Activities

- Read historical datasets.
- Read incremental datasets.
- Store raw data.
- Convert CSV files into Parquet format.
- Validate data using audit information.
- Generate PASS/FAIL validation results.

## Output

The raw datasets are prepared and made available for further processing in the Bronze Layer.

---

# 2. Bronze Layer

The Bronze Layer stores the ingested datasets in **Delta format**.

## Activities

- Read Landing Layer data.
- Create Bronze Delta tables.
- Preserve the source data structure.
- Validate Bronze record counts.

## Bronze Tables

- Customer
- Product
- Sales

The Bronze Layer provides reliable Delta-based storage for downstream processing.

---

# 3. Silver Layer

The Silver Layer is responsible for data cleansing, data quality, incremental processing, MERGE operations, and analytical preparation.

## Data Quality Rules

The following rules are applied:

- Remove records with missing primary keys.
- Remove duplicate records.
- Convert prices and quantities to numeric data types.
- Handle missing values.
- Prepare historical and incremental datasets.

## Delta MERGE

Incremental processing uses Delta Lake MERGE semantics.

The MERGE process:

- Updates existing records when keys match.
- Inserts new records when keys do not match.
- Deduplicates source records before MERGE to prevent multiple-source-row matching errors.

## Product SCD Type 1

Product updates are handled using Type 1 logic.

- Existing product records are updated in place.
- New products are inserted.
- Historical versions are not retained.

## Sales Processing

Sales transactions are processed using transaction-level keys and deduplication so that duplicate transaction instances are removed before processing.

## Surrogate Keys

Synthetic identifiers are generated for analytical joins:

- `customer_sk`
- `product_sk`
- `sales_sk`

## Silver Outputs

```text
03_Silver/
│
├── customer/
├── product/
└── sales/
```

---

# 4. Gold Layer

The Gold Layer converts the prepared Silver data into business-friendly analytical tables.

## Dimension Tables

### Customer Dimension

**Table:** `dim_customer`

Contains customer attributes required for reporting and analysis.

### Product Dimension

**Table:** `dim_product`

Contains product attributes such as:

- Product name
- Product brand
- Product category
- Unit price

### Date Dimension

**Table:** `dim_date`

Contains calendar attributes derived from transaction dates.

## Fact Table

### Sales Fact

**Table:** `fact_sales`

Contains sales transaction information including:

- Transaction ID
- Customer ID
- Product ID
- Transaction Date
- Quantity
- Unit Price
- Discount
- Total Sales
- Sales Surrogate Key

## Gold Outputs

```text
04_Gold/
│
├── dim_customer/
├── dim_product/
├── dim_date/
└── fact_sales/
```

---

# 5. KPI Layer

The KPI notebook uses Gold Layer data to generate business insights.

## KPIs

The following business metrics are generated:

- Total Sales
- Average Sales
- Total Customers
- Total Products
- Top 10 Products
- Top 10 Customers
- Monthly Sales

## Visualizations

The KPI analysis includes visual representations such as:

- Monthly Sales Trend
- Top Products by Sales
- Top Customers by Sales

These visualizations help understand sales performance and customer/product contribution.

---

# Data Quality and Validation

Validation is performed at different stages of the pipeline.

## Validation Checks

- Record counts
- Duplicate detection
- Missing value checks
- Schema verification
- Delta table availability
- MERGE validation

The pipeline validates datasets at each stage before moving them to the next layer.

---

# Key Learning Outcomes

This project demonstrates practical understanding of:

- Databricks notebooks
- PySpark DataFrames
- Data ingestion
- CSV processing
- Parquet format
- Delta Lake
- Delta MERGE
- Data cleansing
- Data quality validation
- Deduplication
- Incremental processing
- Dimensional modelling
- Fact and dimension tables
- Surrogate keys
- SCD Type 1
- Business KPI generation
- Data visualization

---

# How to Run

Run the notebooks in the following order:

```text
01_Raw_Landing
       ↓
02_Bronze
       ↓
03_Silver
       ↓
04_Gold
       ↓
05_KPI
```

## Execution Steps

1. Open the project in Databricks.
2. Run the Raw Landing notebook.
3. Run the Bronze notebook.
4. Run the Silver notebook.
5. Run the Gold notebook.
6. Run the KPI notebook.
7. Verify the generated Delta tables.
8. Verify the KPI outputs and visualizations.

---

# Pipeline Flow

```text
CSV Files
    │
    ▼
Raw Landing
    │
    │ CSV → Parquet
    ▼
Bronze
    │
    │ Delta Storage
    ▼
Silver
    │
    ├── Data Quality
    ├── Cleansing
    ├── Deduplication
    ├── Delta MERGE
    ├── SCD Processing
    └── Surrogate Keys
    │
    ▼
Gold
    │
    ├── Customer Dimension
    ├── Product Dimension
    ├── Date Dimension
    └── Sales Fact
    │
    ▼
KPI
    │
    ├── Total Sales
    ├── Average Sales
    ├── Top Products
    ├── Top Customers
    └── Monthly Sales
```

---

# Project Outcome

The Apex Retail Intelligence project creates an end-to-end retail data pipeline that transforms raw historical and incremental data into clean, structured, and analytics-ready datasets.

The layered architecture provides a clear separation between raw data ingestion, data cleansing, transformation, dimensional modelling, and business analysis.

The final Gold and KPI layers provide a foundation for retail reporting and business intelligence.

---

# Author

**Akash Patra**

**CEI'26 Internship Programme**

**Major Project: Apex Retail Intelligence**