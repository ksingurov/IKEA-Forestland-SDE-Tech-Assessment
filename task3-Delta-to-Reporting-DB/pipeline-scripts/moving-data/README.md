# Scripts to Move Data

This directory contains scripts to create the necessary tables (Bronze, Silver, Gold) with the required schema.

## Summary of Pipeline Files

| File Name              | Source        | Destination   | Key Operations                                    |
|------------------------|---------------|---------------|---------------------------------------------------|
| [landing_to_bronze.py](#landing_to_bronze.py)   | Landing Zone  | Bronze        | Read raw files, deduplicate, merge new/updated records |
| [bronze_to_silver.py](#bronze_to_silver.py)    | Bronze        | Silver        | Validate data, mark invalid records               |
| [silver_to_gold.py](#silver_to_gold.py)      | Silver        | Gold          | Filter valid records, apply transformations       |
| [gold_to_reporting_db.py](#gold_to_reporting_db.py)| Gold          | Reporting DB  | Incremental updates to Reporting Database         |

Each script is designed to be modular, reusable, and parameterized for easy integration into Azure Data Factory (ADF) or Databricks Jobs.

## landing_to_bronze.py

### Purpose
Reads new/updated files from the Landing Zone and loads them into the Bronze Layer while performing incremental updates using Delta Lake.

### Key Functionalities
- Reads data from the Landing Zone (CSV, Parquet, or other formats).
- Deduplicates records before writing to Bronze.
- Uses MERGE to insert/update only new or changed records.

### Parameters
- `file_format`: Format of incoming files (csv, parquet, etc.).
- `landing_zone_path`: Location of raw data files.
- `bronze_path`: Delta table path for Bronze Layer.
- `match_column`: Column used to match existing records (e.g., DateTimeStamp).
- `update_columns`: Columns to update when a match is found (e.g., Tag1, Tag2, Tag3).

## bronze_to_silver.py

### Purpose
Moves validated data from Bronze to Silver, performing data validation and marking records as valid/invalid.

### Key Functionalities
- Reads from Bronze Table.
- Applies data validation rules (e.g., check if Tag1, Tag2, Tag3 follow expected patterns or ranges).
- Creates columns:
  - `valid_record`: True if the record passes validation, False otherwise.
  - `invalid_reason`: A list of validation failures (null if valid).
- Uses MERGE to update records in Silver Layer, ensuring incremental updates.

### Parameters
- `bronze_path`: Source path for Bronze Layer data.
- `silver_path`: Destination path for Silver Layer data.

## silver_to_gold.py

### Purpose
Moves only valid records from Silver to Gold and applies business transformations if needed.

### Key Functionalities
- Filters out records where `valid_record == False`.
- Applies business transformations (e.g., aggregations, renaming fields).
- Uses MERGE to insert/update new records into the Gold Layer.

### Parameters
- `silver_path`: Source path for Silver Layer data.
- `gold_path`: Destination path for Gold Layer data.

## gold_to_reporting_db.py

### Purpose
Moves final Gold data to the Reporting Database, ensuring only new records are inserted.

### Key Functionalities
- Reads from Gold Layer.
- Compares with existing Reporting Database entries to only insert new data.
- Uses incremental logic to avoid duplicates in the Reporting Database.

### Parameters
- `gold_table_path`: Source path for Gold Layer data.
- `reporting_db_connection_string`: Connection string or credentials for the reporting database.
- `reporting_table_name`: Target Reporting Database table name.
- `last_processed_timestamp`: The last processed DateTimeStamp.

