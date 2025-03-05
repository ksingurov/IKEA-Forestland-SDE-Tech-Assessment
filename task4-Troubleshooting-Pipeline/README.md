# Pipeline Troubleshooting Guide

## Scenario
We have a data pipeline which is running fine, all of a sudden the pipeline started failing. What is your approach in troubleshooting this issue?

## Setting Up Logging in the Pipeline
To effectively troubleshoot failures in our pipeline, we need comprehensive logging at different levels:

1. **Databricks Logs (Execution & Cluster Logs)**
    * Enabled via Cluster Logging in Databricks settings.
    * Logs include notebook execution logs, Spark logs, driver/executor logs.
    * Store logs in Azure Blob Storage or ADLS for long-term retention.

2. **Application-Level Logging (Python/Scala Logs in Scripts)**
    * Use Python’s logging module to log key steps in each script.
    * Store logs in Databricks File System (DBFS) or push them to a centralized logging service.

3. **ADF Pipeline Logs**
    * Enabled in Monitoring → Activity Runs in ADF.
    * Capture execution time, input/output, error messages.
    * Store logs in Azure Monitor or Log Analytics.

4. **Storage Access Logs (Blob Storage / ADLS)**
    * Enable Storage Diagnostics to log read/write failures.
    * Useful for debugging file arrival issues or permission errors.

5. **Delta Lake Transaction Logs**
    * Delta Table history tracks all changes, including MERGE, INSERT, UPDATE failures.
    * Use DESCRIBE HISTORY to check recent operations.

## Waterfall Troubleshooting Approach
If the pipeline fails, follow a structured waterfall approach to systematically narrow down the cause:

### Step 1: Did the pipeline trigger correctly?
Possible Issues:
* ADF Trigger Failed → Check ADF pipeline logs for trigger failures.
* Event-Based Trigger Failed → If using event-based triggers, check Storage Event Grid logs.
* Manual Job Trigger in Databricks Failed → Check Databricks Job Run history.

What to Check:
✅ ADF Pipeline Run Logs (Did it start?)  
✅ Databricks Job Logs (Did it trigger?)  
✅ Storage Event Logs (Was a new file detected?)

### Step 2: Did the data arrive in the Landing Zone?
If Step 1 Passed, but Pipeline Failed Here, Possible Issues:
* File Missing → Check Landing Zone storage (Azure Storage Explorer, ADLS).
* File Corrupted → Open file manually or validate format.
* Incorrect File Format → Expected CSV, got Parquet?

What to Check:
✅ Storage Account Logs (Was a file written?)  
✅ File Schema (Does it match expected format?)

### Step 3: Did the Landing to Bronze Process Work?
If Step 2 Passed, but Failure Occurs in Bronze Load:
* File Read Error → Wrong delimiter, header mismatch.
* Schema Mismatch → New columns in file causing read failure.
* Merge Condition Failure → Duplicate/missing keys preventing updates.
* Storage Access Issue → Bronze layer storage permissions changed.

What to Check:
✅ Databricks Job Logs (landing_to_bronze.py execution)  
✅ Spark Exception Logs (Did .read() fail?)  
✅ Delta Table History (DESCRIBE HISTORY bronze_table)

### Step 4: Did the Bronze to Silver Process Work?
If Step 3 Passed, but Silver Processing Failed:
* Validation Rule Failures → Too many records failing checks.
* Incorrect Merge Logic → Tags not updating properly.
* Schema Changes → Bronze changed, but Silver schema didn’t update.

What to Check:
✅ Delta Table Logs (DESCRIBE HISTORY silver_table)  
✅ Validation Failure Counts (SELECT COUNT(*) WHERE valid_record = false)  
✅ Data Sample (SELECT * FROM silver_table LIMIT 10)

### Step 5: Did the Silver to Gold Process Work?
If Step 4 Passed, but Gold Processing Failed:
* Invalid Records Included → valid_record = false incorrectly moving to Gold.
* Transformation Errors → Null values, unexpected calculations.
* Duplicate Data Issues → Same records appearing multiple times.

What to Check:
✅ Data Filters (SELECT * FROM silver_table WHERE valid_record = false)  
✅ Gold Table History (DESCRIBE HISTORY gold_table)

### Step 6: Did the Data Reach the Reporting DB?
If Step 5 Passed, but Data Not in Reporting DB:
* Database Connection Issue → Credentials expired, networking blocked.
* Incremental Logic Failure → Gold filtering incorrect, missing new records.
* Primary Key Conflict → Duplicate records causing DB constraint failures.

What to Check:
✅ Database Connection (SELECT 1 to check connectivity)  
✅ Merge Query Logs (Did INSERT execute correctly?)  
✅ Duplicate Check (SELECT COUNT(*) FROM reporting_db WHERE key_column = ?)

## Final Thoughts
* If failure is at an earlier step, stop deeper checks (e.g., no need to check DB if Bronze load failed).
* By following this waterfall approach, we systematically eliminate issues, reducing debugging time.