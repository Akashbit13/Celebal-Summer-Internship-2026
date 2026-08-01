## Delta Lake MERGE Implementation

## Overview

This assignment demonstrates how Delta Lake performs incremental data processing using the MERGE operation in Databricks. The project includes loading data into a Delta table, performing basic data cleaning, creating incremental data, applying the MERGE operation, validating the results, and displaying the final dataset.

---

## Objective

The objective of this assignment is to understand incremental data processing using Delta Lake. During this assignment, I learned how to:

- Load data into a Delta table
- Perform basic data cleaning
- Create an incremental dataset
- Apply the Delta Lake MERGE operation
- Validate the processed data
- Display the final dataset

---

## Dataset

**Dataset Used:** Sample Superstore Dataset

The dataset was uploaded into Databricks and stored as a Delta table for performing all the required operations.

---

## Technologies Used

- Python
- Apache Spark (PySpark)
- Delta Lake
- Databricks Free Edition

---

## Project Structure

```text
delta-lake-assignment/
│
├── data/
│   ├── customer_master.csv
│   └── customer_incremental.csv
│
├── notebooks/
│   ├── delta_scd_assignment.html
│   ├── delta_scd_assignment.ipynb
│   └── delta_scd_assignment.pdf
│
├── screenshots/
│   ├── data_loading/
│   ├── data_cleaning/
│   ├── scd1/
│   ├── scd2/
│   ├── validation/
│   └── final_output/
│
├── report/
│   └── assignment_summary.pdf
│
└── README.md
```

---

## Steps Performed

### Step 1 – Load Dataset into a Delta Table

- Uploaded the Sample Superstore dataset into Databricks.
- Created a Delta table.
- Loaded the Delta table into a Spark DataFrame.
- Displayed the dataset and schema.

---

### Step 2 – Perform Basic Data Cleaning

- Checked the dataset for null values.
- Removed duplicate records.
- Verified the cleaned dataset before moving to the next step.

---

### Step 3 – Create an Incremental Dataset

- Created a second dataset from the cleaned data.
- Updated the **Sales** value for a few existing records.
- Added one new record to simulate incremental data.

---

### Step 4 – Apply Delta Lake MERGE

- Renamed columns to remove spaces before saving the Delta table.
- Created the Delta table.
- Applied the MERGE operation using **Order_ID** as the matching column.
- Updated existing records and inserted the new record successfully.

---

### Step 5 – Validate the Results

- Verified the total number of rows.
- Checked duplicate records after the MERGE operation.
- Confirmed that the final dataset was processed successfully.

---

### Step 6 – Display Final Dataset

- Displayed the final Delta table.
- Verified that updated records and the newly inserted record were present.
- Confirmed the final row count.

---

## Output

The assignment was completed successfully. The Delta Lake MERGE operation updated the existing records and inserted the new record into the Delta table. The final dataset was validated and displayed successfully.

---

## Learning Outcome

Through this assignment, I learned how to:

- Work with Delta Lake tables
- Perform incremental data processing
- Use the MERGE operation
- Clean and validate data
- Work with PySpark in Databricks

This assignment helped me understand how Delta Lake efficiently handles updates and inserts in real-world data engineering workflows.

---

## Author

**Akash Patra**