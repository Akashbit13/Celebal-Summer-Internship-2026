# Azure Cloud Fundamentals and Data Pipeline Implementation using ADF

## Overview

This Week 4 assignment focuses on understanding basic Azure cloud services and building an end-to-end data pipeline using Azure Blob Storage and Azure Data Factory.

For this project, I used the Superstore CSV dataset as the source file. The pipeline checks the source file metadata and then copies the data from the source location to a new destination location in Azure Blob Storage.

## Objective

The main objective of this assignment was to understand how Azure Storage and Azure Data Factory can be used together to build a simple data pipeline.

## Architecture

The data flow used in this project is:

`Sample-Superstore.csv → Azure Blob Storage → Get Metadata → Copy Data → Destination Blob Storage`

## Azure Services Used

- Azure Resource Group
- Azure Storage Account
- Azure Blob Storage
- Azure Data Factory
- Azure Role-Based Access Control (RBAC)

## ADF Components Used

- Linked Service
- Source Dataset
- Destination Dataset
- Get Metadata Activity
- Copy Data Activity
- Pipeline

## Tasks Completed

### Task 1: Resource Group
Created a Resource Group to keep the Azure resources used in this project organized in one place.

### Task 2: Storage Setup
Created a Storage Account and a Blob Container, then uploaded the Superstore CSV file to the source location.

### Task 3: ADF Basics
Created an Azure Data Factory instance and configured:

- Azure Blob Storage Linked Service
- Source Dataset
- Destination Dataset
- Get Metadata Activity

### Task 4: Pipeline Development
Created a pipeline using Get Metadata and Copy Data activities.

The pipeline flow is:

`Get_Source_File_Metadata → Copy_Superstore_Data`

The Copy Data activity was configured to read the CSV file from the source directory and write the copied file to the destination directory.

### Task 5: Pipeline Execution
Validated and executed the pipeline using Debug. Both activities completed successfully, and the final pipeline status was `Succeeded`.

### Task 6: IAM and Access Control
Configured Azure RBAC permissions and provided the Azure Data Factory managed identity with `Storage Blob Data Contributor` access to Blob Storage.

## Source and Destination

### Source

`superstore-data/source/Sample-Superstore.csv`

### Destination

`superstore-data/destination/Sample-Superstore-Copy.csv`

## Final Result

The pipeline executed successfully and completed the following steps:

1. Retrieved metadata from the source CSV file.
2. Executed the Copy Data activity after the metadata activity succeeded.
3. Copied the Superstore data to the destination location.
4. Verified the output file in Azure Blob Storage.

## Key Learnings

Through this assignment, I learned how to:

- Create and organize Azure resources.
- Store CSV files in Azure Blob Storage.
- Connect Azure Data Factory with Blob Storage.
- Create and configure Linked Services and Datasets.
- Use Get Metadata to retrieve file information.
- Use Copy Data to move data between locations.
- Connect pipeline activities using a success dependency.
- Execute and monitor an ADF pipeline.
- Configure Azure RBAC access for ADF and Storage.

## Possible Improvements

The current pipeline processes a single CSV file. In the future, it can be improved by:

- Using a `ForEach` activity to process multiple files.
- Adding an `If Condition` to check whether the source file exists.
- Using pipeline parameters for dynamic source and destination paths.
- Adding more error-handling and monitoring steps.

## Conclusion

This project helped me understand the basic workflow of an Azure data pipeline. I successfully connected Azure Blob Storage with Azure Data Factory, checked the source file metadata, copied the CSV data to a new destination location, and verified the successful pipeline execution.