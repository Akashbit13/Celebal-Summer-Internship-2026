# Week 5 - PySpark Data Processing

## Overview

This project is part of the **Celebal Summer Internship 2026 - Week 5** assignment. The objective of this assignment is to learn the basics of **PySpark** by performing data cleaning, filtering, transformation, aggregation, grouping, and building a simple data processing pipeline.

---

## Project Structure

```
Week-5/
│
├── dataset/
│   └── sales_data.csv
│
├── notebook/
│   ├── pyspark_data_processing.ipynb
│   └── pyspark_data_processing.html
│
├── output/
│   ├── final_pipeline_output/
│   └── final_pipeline_output.csv
│
├── screenshots/
│
└── README.md
```

---

## Dataset

**File:** `sales_data.csv`

The dataset contains sales transaction information with columns such as:

- user_id
- transaction_date
- region
- product_category
- sale_amount
- city
- age
- subscription
- status
- price
- store_id
- email
- username
- raw_timestamp

---

## Tasks Performed

The following tasks were completed using PySpark:

- Compared Spark with traditional MapReduce.
- Learned about In-Memory Computing.
- Removed duplicate records.
- Filtered data based on different conditions.
- Handled missing (null) values.
- Performed grouping and aggregation.
- Renamed and transformed columns.
- Understood Spark Shuffle and DataFrame immutability.
- Built a final data processing pipeline.

---

## Final Processing Pipeline

The final pipeline performs the following operations:

1. Removes duplicate records based on **user_id** and **transaction_date**.
2. Replaces null values in the **price** column with **0**.
3. Groups the data by **store_id**.
4. Calculates the **total revenue** for each store.

---

## Output

The processed data is available in the **output** folder.

```
output/
├── final_pipeline_output.csv
└── final_pipeline_output/
```

---

## Why Pandas Was Used for Saving the Output

PySpark provides the `write.csv()` method to save DataFrames, and it is the standard approach for large distributed datasets.

While working on this assignment in a Windows environment, the Spark write operation required additional Hadoop configuration (`HADOOP_HOME` and `winutils.exe`). To avoid this environment-specific issue, the final processed DataFrame was converted into a Pandas DataFrame using `toPandas()` and then saved using `to_csv()`.

All data processing tasks, including duplicate removal, null value handling, grouping, and aggregation, were performed using **PySpark**. Pandas was used only to export the final processed data as a CSV file because the final aggregated dataset is small.

---

## Technologies Used

- Python
- PySpark
- Pandas
- Jupyter Notebook
- VS Code

---

## Learning Outcomes

Through this assignment, I learned how to:

- Work with Spark DataFrames
- Clean and transform datasets
- Handle null values
- Perform filtering and aggregation
- Group records using PySpark
- Build a simple data processing pipeline
- Export processed data into a CSV file

---

## Author

**Akash Patra**

Celebal Summer Internship 2026