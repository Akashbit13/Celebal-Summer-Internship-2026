# Week 6 - Spark Architecture and Efficient Data Processing

## Overview

This week, I learned the basic architecture of Apache Spark and performed different DataFrame operations using PySpark. I also learned how Spark processes data efficiently using lazy evaluation and how different transformations and actions work.

---

## Objectives

- Understand Apache Spark architecture.
- Learn Lazy Evaluation.
- Read CSV files using Spark.
- Perform filtering and selection.
- Apply data transformations.
- Understand transformations and actions.
- Learn basic Spark performance concepts.
- Work with Parquet files.

---

## Dataset

**Dataset Used:**

`Grocery_Inventory_and_Sales_Dataset.csv`

The dataset contains information about grocery products, suppliers, stock quantity, warehouse location, sales volume, and product status.

---

## Tools Used

- Python
- PySpark
- Apache Spark
- Jupyter Notebook
- Visual Studio Code

---

## Folder Structure

```
Week-6
│
├── dataset
│   └── Grocery_Inventory_and_Sales_Dataset.csv
│
├── notebook
│   └── spark_architecture.ipynb
│
├── screenshots
│   ├── 01-pyspark-version.png
│   ├── 02-spark-session.png
│   ├── 03-lazy-evaluation.png
│   ├── 04-read-data.png
│   ├── 05-filtering-selection.png
│   ├── 06-data-transformation.png
│   ├── 07-transformations-actions.png
│   ├── 08-aggregation-results.png
│   └── 09-final-pipeline.png
│  
│
├── output
|   └──inventory_parquet
│
└── README.md
```

---

## Tasks Completed

- Created a Spark Session.
- Loaded the dataset into a Spark DataFrame.
- Learned Spark Lazy Evaluation.
- Selected and filtered data.
- Performed DataFrame transformations.
- Converted data types.
- Used transformations and actions.
- Calculated aggregate values.
- Learned basic Spark performance concepts.
- Implemented Parquet processing.

---

## Learning Outcomes

After completing this assignment, I learned how to:

- Create and use a Spark Session.
- Read CSV files using PySpark.
- Filter and transform data.
- Perform aggregation operations.
- Understand transformations and actions.
- Understand the importance of Lazy Evaluation.
- Work with Parquet files in Spark.

---

## Note

Some assignment questions used column names that were not available in the selected dataset. In those cases, similar columns from the dataset were used to perform the required Spark operations.

While performing Parquet processing, the write operation could not be completed because the local Spark environment on Windows required Hadoop configuration (`HADOOP_HOME` / `hadoop.home.dir`).

---

## Conclusion

This assignment helped me understand the basic working of Apache Spark and different DataFrame operations. I also learned how Spark processes data efficiently and how different transformations are used while working with large datasets.

