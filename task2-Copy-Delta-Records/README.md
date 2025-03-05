# Copy Delta Records

This directory contains the batch script to copy delta records from source storage to destination storage.

## Table of Contents

1. [Assumptions](#assumptions)
2. [Files](#files)
   - [copy_delta_records_batch_script.py](#copy_delta_records_batch_scriptpy)
3. [Script Logic](#script-logic)
4. [Functions Descriptions](#functions-descriptions)
5. [Usage](#usage)
6. [Requirements](#requirements)
7. [Emulating Delta Table Logic](#emulating-delta-table-logic)
8. [Additional Information](#additional-information)
   - [Parquet Files](#parquet-files)
   - [Spark Session](#spark-session)
   - [Ordinary Tables vs. Delta Tables](#ordinary-tables-vs-delta-tables)

## Assumptions

1. **Authentication**: It is assumed that the code will be run within a workspace like [Databricks](https://databricks.com/) or [Azure Synapse](https://azure.microsoft.com/en-us/services/synapse-analytics/), so the authentication is set up on a workspace level, i.e., through mounting or IAM.
2. **Existing Spark Session**: A [Spark session](#spark-session) object exists in the environment. If not, its creation should be added to the logic.
3. **Azure Storage**: It is assumed that we are working with Azure Storage specically with ADLS, so the [valid path structure](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri) should look like:<br>
    `abfss://<container>@<storage_account>.dfs.core.windows.net/<path>`.
4. **Manual or Scheduled Execution**: The script can be run manually or as a scheduled task (e.g., [Databricks job](https://docs.databricks.com/jobs.html)).
5. **Variable Configuration**: If running the code manually, variable definitions should be checked and updated to align with your storage account, container, and destination details.
6. **Parameterization for Scheduled Tasks**: When using the script for scheduled tasks, ensure the logic is adapted to accept parameters via arguments (e.g., as [Databricks job parameters](https://docs.databricks.com/aws/en/jobs/parameters)). This would allow the script to dynamically receive values at runtime, making it more flexible and reusable.
7. **Schema Stability**: The schema of the Parquet files is assumed to be stable and consistent over time, meaning that the columns and their data types do not change.

## Files

### [copy_delta_records_batch_script.py](./copy_delta_records_batch_script.py)

This script processes delta records in batch mode. It loads the current and previous versions of the data, computes delta records, and updates the previous version.

#### Script Logic:

1. **Get Today's Date**: Retrieve the current date to use in destination file paths.
2. **Define Storage Accounts and Paths**: Set up the source and destination storage accounts and paths. 
   - **Note**: This step needs to be adjusted if the script is to be used within a job.
3. [**Define Functions**](#functions-descriptions):
   - `load_parquet`: Load parquet file from storage and create DataFrame.
   - `get_subfolders`: Retrieve all subfolders dynamically from the given base path.
   - `get_delta_records`: Identify new and modified records based on hashed column comparison.
   - `get_next_file_name`: Get the next sequential file name in the destination path.
   - `file_exists`: Check if a file exists in Azure storage.
4. **Main script logic**:
- Retrieve the list of subfolders using `get_subfolders()`.
   - For each subfolder:
     - Construct the source, destination, and previous paths.
   - Load the current version of the data using `load_parquet()`.
   - Load the previous version using `load_parquet()` if it exists and matches the date, otherwise assign `None`.
   - Compute the delta records using `get_delta_records()`.
   - If there are delta records:
       - Add a `load_time` column.
       - Write the delta records to the destination path with a sequential file name using `get_next_file_name()`.
   - Update the previous version with only the hash column.

#### Functions Descriptions:

- `load_parquet(file_path)`: Load parquet file from storage and create DataFrame.
- `get_subfolders(base_path)`: Retrieve all subfolders dynamically from the given base path using `dbutils.fs.ls()`.
- `get_delta_records(new_df, prev_df)`: Identify new and modified records based on hashed column comparison.
  - This function creates a hash column for the new DataFrame by concatenating all columns and applying a SHA-256 hash function. If the previous DataFrame is `None`, it returns the new DataFrame without the hash column. Otherwise, it performs a left anti join on the hash column to identify records that are present in the new DataFrame but not in the previous DataFrame.
- `get_next_file_name(destination_path)`: Get the next sequential file name in the destination path using `dbutils.fs.ls`.
- `file_exists(path)`: Check if a file exists in Azure storage using `dbutils.fs.ls()`.

## Usage

1. Ensure you have the necessary credentials and configurations for [Azure Data Lake Storage](https://docs.microsoft.com/en-us/azure/storage/data-lake-storage/).
2. Ensure a [Spark session](#spark-session) object exists in your environment. Otherwise, add its creation to the logic.
3. If running the code manually, check and update the variable definitions to align with your storage account, container, and destination details.
4. If using the script for a scheduled task, such as a [Databricks job](https://docs.databricks.com/jobs.html), adjust the logic to take these parameters from arguments. Key-value pairs could be passed as [job parameters](https://docs.databricks.com/aws/en/jobs/parameters) and access them in the code through [widgets](https://docs.databricks.com/aws/en/dev-tools/databricks-utils#dbutils-widgets-get).
5. Execute the script in an environment where the [Spark](https://spark.apache.org/docs/latest/) engine is available, such as a Spark cluster, or platforms like [Databricks](https://databricks.com/) or [Azure Synapse](https://azure.microsoft.com/en-us/services/synapse-analytics/).

## Requirements

- `PySpark` - pre-installed on [Databricks clusters](https://docs.databricks.com/clusters/index.html) or on [Apache Spark pools in Azure Synapse Analytics](https://docs.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-overview).

## Emulating Delta Table Logic

The current script emulates key aspects of the Delta table logic impleme nted in Delta Lake. [While it do]esn't provide all the advanced features of Delta tables, it replicates core functionalities such as handling incremental updates and managing file output in an organized manner.

### Emulated Features:

- **Incremental Data Processing**: The script identifies and processes new and modified records by comparing the current dataset with the previous one, ensuring that only updated data is written to storage. This mirrors Delta's ability to process data incrementally and avoid unnecessary reprocessing of unchanged records.
- **Schema Evolution**: The script is designed to detect and process new records, but full schema evolution (such as the automatic handling of new or modified columns) is not fully implemented. Delta Lake automatically adapts to schema changes, such as adding new columns or changing data types, ensuring backward compatibility without manual intervention.
- **File Management**: The script generates new Parquet files for the updated data and retains a reference to the previous dataset. This approach mimics Delta's versioning mechanism to track changes over time, though it lacks the full versioning and time-travel features found in Delta Lake.

### Missing Features:

- **ACID Transactions**: The script does not implement ACID transactions, which are a core feature of Delta tables. This means that the script doesn't provide transaction guarantees, and there's no automatic rollback or consistency control during failures.
- **Advanced Schema Evolution**: While the script processes new and modified records, it doesn't fully support schema evolution as Delta Lake does. Changes like adding new columns or modifying existing column types are not automatically handled, and the schema must be manually adjusted.
- **Upsert (MERGE) Logic**: The script currently only identifies new records and does not support efficient upsert operations (i.e., updating existing records or deleting outdated records). Delta's MERGE operation allows you to insert, update, or delete records in a single operation, making it more efficient for handling complex data changes.
- **Data Compaction and Optimizations**: The script does not automatically compact small files or perform data optimizations like Delta tables do. Delta optimizes data storage by merging small files into larger ones and can apply indexing and Z-Ordering for faster query performance.
- **Time Travel and Versioning**: The script does not include full support for Delta's time travel or versionihttps://docs.databricks.com/aws/en/delta/historyng functionality. Delta Lake allows querying historical versions of the data and offers automatic versioning through a transaction log, which is not yet implemented in this script.

## Additional Information

### Parquet Files

[Parquet](https://parquet.apache.org/) is a columnar storage file format optimized for use with big data processing frameworks like Apache Spark. It provides efficient data compression and encoding schemes, resulting in reduced storage costs and improved query performance.

### Spark Session

A [Spark session](https://spark.apache.org/docs/latest/sql-getting-started.html) is the entry point to programming Spark with the Dataset and DataFrame API. It allows you to create DataFrames, execute SQL queries, and manage Spark configurations.

### Ordinary Tables vs. Delta Tables

#### Ordinary Tables (Parquet, CSV, JSON, etc.)

Ordinary tables are stored as raw files (e.g., Parquet, CSV) in a directory. They do not support ACID transactions, versioning, or built-in data change tracking. Any updates or deletes require manually rewriting the entire dataset.

**directory structure**:
```
dsprdsawf1/oem1/folder1/WT01
├── 20220926.parquet
├── 20220927.parquet
└── ...
```

#### Delta Tables

Delta Tables extend the Parquet format by adding transaction logs that maintain version history, enabling ACID transactions, schema enforcement, and time travel.

**example directory structure**:
```
dsprdsawf1/oem1/folder1/WT01
    ├── _delta_log/
    │   ├── 00000000000000000001.json
    │   ├── 00000000000000000002.json
    │   ├── 00000000000000000003.json
    │   └── ...
    ├── part-00000-abc123.snappy.parquet
    ├── part-00001-def456.snappy.parquet
    └── ...
```

The Delta table will manage the structure of the folder automatically in the background. However, there may be occasions when the directory requires manual maintenance, such as running optimization or vacuuming to ensure optimal performance and data consistency.