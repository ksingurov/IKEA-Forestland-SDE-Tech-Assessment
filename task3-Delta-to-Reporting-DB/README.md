# Pipeline to Read and Send Deltas to Reporting DB

This directory contains scripts to be used within a pipeline which will read the landing, identify deltas, and send it to reporting database.

## Table of Contents
1. [Scenario Overview](#scenario-overview)
   - [Requirements to the Pipeline](#requirements-to-the-pipeline)
   - [Scenario Conditions & Their Implications for the Pipeline](#scenario-conditions--their-implications-for-the-pipeline)
2. [Assumptions and Limitations](#assumptions-and-limitations)
   - [Data Format](#data-format)
   - [Attributes](#attributes)
3. [Pipeline Design](#pipeline-design)
   - [Data Pipeline Layers](#data-pipeline-layers)
   - [Tools and Technologies](#tools-and-technologies)
   - [Assumptions Regarding Data Handling](#assumptions-regarding-data-handling)
   - [Event-Driven Architecture](#event-driven-architecture)
   - [Summary of Architecture Assumptions](#summary-of-architecture-assumptions)
4. [Cost Effectiveness of the Pipeline Architecture](#cost-effectiveness-of-the-pipeline-architecture)
   - [Delta Lake for Incremental Processing](#1-delta-lake-for-incremental-processing)
   - [Only Process Valid Records](#2-only-process-valid-records)
   - [Avoidance of Full Data Refreshes](#3-avoidance-of-full-data-refreshes)
   - [Scalability and On-Demand Resources](#4-scalability-and-on-demand-resources)
   - [Azure Data Factory Orchestration](#5-azure-data-factory-orchestration)
   - [Use of Parquet Format in Silver and Gold Layers](#6-use-of-parquet-format-in-silver-and-gold-layers)
   - [Cost-Effective Reporting Database Integration](#7-cost-effective-reporting-database-integration)
   - [Summary of Cost-Effectiveness Factors](#summary-of-cost-effectiveness-factors)
5. [Uncertainty Factors in the Choice of Architecture and Pipeline Structure](#uncertainty-factors-in-the-choice-of-architecture-and-pipeline-structure)
   - [Data Volume and Velocity](#1-data-volume-and-velocity)
   - [Data Quality and Consistency](#2-data-quality-and-consistency)
   - [Schema Evolution](#3-schema-evolution)
   - [Data Processing Requirements](#4-data-processing-requirements)
   - [Integration with External Systems](#5-integration-with-external-systems)
   - [Cost Constraints](#6-cost-constraints)
   - [Security and Compliance](#7-security-and-compliance)
   - [Team Expertise](#8-team-expertise)
6. [Pipeline Overview & Files](#pipeline-overview--files)

## Scenario Overview

### Requirements to the Pipeline

There are two main requirements for the pipeline:
- **Req1:** The pipeline/notebook must detect changes (deltas) and update the reporting database.
- **Req2:** The solution should be cost-effective and reusable.

### Scenario Conditions & Their Implications for the Pipeline

- **Cond1:** Data is pushed by an external company to the **landing zone**. The external company may update previously sent data.
   <ul style="list-style-type: none;">
   <li>⤷ <b>Implication:</b> The algorithm will need to perform delta detection to identify changes.</li>
   <li>⤷ <b>Implication:</b> Since the data may be updated, previously processed records will also need to be revised accordingly.</li>
   </ul>
- **Cond2:** No "last updated" timestamp or flag is available to identify modified records.
   <ul style="list-style-type: none;">
   <li>⤷ <b>Implication:</b> We cannot use any columns to filter new or updated records.</li>
   </ul>
- **Cond3:** The file structure remains consistent, containing the following columns: `DateTimeStamp`, `Tag1`, `Tag2`, `Tag3`.
   <ul style="list-style-type: none;">
   <li>⤷ <b>Implication:</b> These column names can be used within the logic.</li>
   <li>⤷ <b>Assm1:</b> This consistency is one of the assumptions the logic will be built on, and it could break if the structure changes. A custom error message can be created to handle this scenario.</li>
   </ul>

## Assumptions and Limitations

To proceed with the design and development of the pipeline, several assumptions must be made due to uncertainties arising from the lack of detailed information about the data structure and attributes. Specifically, the absence of clarity regarding the data format (e.g., `Parquet`, `CSV`, or Blob storage) and the nature of attributes (such as primary keys or composite keys) creates ambiguity in how the data should be processed and integrated into the pipeline.

### Data Format
   - **Assm2:** We will assume that the format in which the data is provided is known and will remain consistent. This consistency is crucial for the proper functioning of the pipeline.

### Attributes

#### Possible Attribute Types

- **`DateTimeStamp`**: Based on the attribute name, this could potentially serve as a Primary Key (PK).
- **`Tag1`, `Tag2`, `Tag3`**: These are likely categorical attributes. Given their names, it's unlikely that they represent amounts.

#### Possible Combinations (Waterfall)

- **Assm3:** `DateTimeStamp` is PK.
   <ul style="list-style-type: none;">
   <li>⤷ <b>Implication:</b> In this case, it could be used for upserts via the `MERGE` operation (which will be explained further).</li>
   </ul>

- **Assm4:** `DateTimeStamp` is PK, but duplicates are possible.
   <ul style="list-style-type: none;">
   <li>⤷ <b>Implication:</b> If this is a data quality issue, pre-processing is needed to retain only one record per `DateTimeStamp`, and then we can use `MERGE`.</li>
   </ul>

- **Assm5:** Combination of `DateTimeStamp`, `Tag1`, `Tag2`, `Tag3` is unique (Composite Key).  
   <ul style="list-style-type: none;">
   <li>⤷ <b>Implication:</b> In this case, use `MERGE` with a Composite Key.</li>
   </ul>

- **Assm6:** Combination of `DateTimeStamp`, `Tag1`, `Tag2`, `Tag3` is Composite Key, but duplicates are possible.
   <ul style="list-style-type: none;">
   <li>⤷ <b>Implication:</b> If this is a data quality issue, pre-processing is needed to retain only one unique record, then use `MERGE`.</li>
   </ul>

- **Assm7:** Neither of the above
   <ul style="list-style-type: none;">
   <li>⤷ <b>Implication:</b> In the worst-case scenario, process the data first, then either create a sequence ID or deduplicate it based on some business logic.</li>
   </ul>

## Pipeline Design

### Data Pipeline Layers

#### **Landing Zone (Raw Data)**

- The **Landing Zone** is where external sources send raw data files.
- These data files could be in different formats (e.g., `CSV`, `Parquet`, `JSON`), and they might be either new or updated records.
- No structured rules in the raw data, hence, this layer holds unrefined, possibly incomplete, or messy data.
- **Assumption:** Data in the **Landing Zone** might contain duplicates or inconsistencies, and it may need validation or cleaning before moving to the next layers.

#### **Bronze Layer (Staging)**

- The **Bronze Layer** is the first structured layer where data from the **Landing Zone** is staged.
- In this layer, we do not perform much transformation; we simply store the raw data after a basic validation (e.g., deduplication).
- **Assumption:** The data is append-only from the **Landing Zone**, meaning we add new or updated records, but we don’t overwrite any existing data in the **Bronze** layer.
- **Data format:** **Delta Lake** tables, providing transactional support and enabling updates.
- **`MERGE` Logic:** When pushing data from **Landing Zone** to **Bronze**, the `DateTimeStamp` (or another unique identifier) is used for matching the new records with existing ones, ensuring that only new or updated records are written.

#### **Silver Layer (Validated Data)**

- The **Silver Layer** is the next layer where further transformation happens, such as validation (e.g., tag checks, data type corrections, range checks).
- Records that fail validation will be flagged with an `invalid_reason` column, while valid records will be marked as `valid_record = True`.
- **Assumption:** In this layer, data is validated, and only valid records are kept.
- **`MERGE` Logic:** The **Silver** layer will be updated with valid records from the **Bronze** layer using a `MERGE` operation, which ensures that only new or modified records are updated in the **Silver** table.

#### **Gold Layer (Business-Ready Data)**

- The **Gold Layer** contains the final, most business-ready version of the data, often transformed for reporting, analytics, or ML.
- The records in this layer are enriched, aggregated, and ready for final reporting.
- **Assumption:** We can perform transformations like aggregations or joins on the **Silver** Layer to get data suitable for reporting.
- **`MERGE` Logic:** Similar to **Silver**, only new or updated records from **Silver** will be pushed into **Gold**, ensuring incremental updates.

#### **Reporting Database (Final Destination)**

- After data reaches the **Gold Layer**, it is pushed to the **Reporting Database**.
- **Assumption:** This could be a SQL database (e.g., **Azure SQL Database**, **SQL Server**, or a similar relational database) or a Data Warehouse (e.g., **Azure Synapse** or **Redshift**), which serves as the source for BI or reporting tools.
- **Data Transfer:** Data will be transferred to the **reporting database** in a way that reflects the most recent and accurate information.

### Tools and Technologies

#### **Azure Data Factory (ADF)**

- **ADF** is used to orchestrate the pipeline, trigger **Databricks** notebooks, and handle data movement between different layers.
- **Parameterization:** **ADF** will pass parameters such as file paths, data formats, and columns to **Databricks jobs** for dynamic execution.
- **Triggers:** **ADF** can be set to trigger the pipeline based on events (e.g., file arrival or modification in the **landing zone**).

#### **Databricks**

- **Databricks** is the environment used for data processing, particularly for handling **Delta Lake** tables and performing transformations like `MERGE` and validation.
- **Job Orchestration:** Jobs in **Databricks** will handle the main processing (e.g., reading data, validation, writing to **Delta tables**).
- **Delta Lake:** **Delta Lake** will provide the transactional storage layer, allowing ACID transactions, version control, and efficient incremental updates.

#### **Azure Storage**

- **Blob Storage** (e.g., **Azure Data Lake Storage Gen2**) is used to store raw data in the **Landing Zone** and process it through **Delta Lake**.
- The **Bronze**, **Silver**, and **Gold** layers are stored as **Delta tables** in **Azure Storage** (e.g., using **Databricks File System (DBFS)** or **ADLS Gen2**).

#### **Databases for Reporting**

- **Reporting Database** could be **Azure SQL Database**, **SQL Server**, or another reporting database where processed data from the **Gold** layer is pushed for consumption by BI tools (e.g., **Power BI**, **Tableau**).
- **Data Transfer to Reporting DB** could be done using **ADF** or via **Databricks** directly, depending on the integration setup.

### Assumptions Regarding Data Handling

#### Data Duplication and Incremental Updates

- **`MERGE` operations** are essential to ensure that only new or updated records are moved between layers.
- Tag columns (e.g., `Tag1`, `Tag2`, `Tag3`) are treated as attributes, while `DateTimeStamp` is assumed to be the unique key.
- If `DateTimeStamp` isn’t unique, we might need additional columns or logic to handle duplicates.

#### Validation before Silver Layer

- Validation checks are applied to ensure only clean and correct records reach the **Silver** layer.
- Records failing validation (e.g., invalid tags) will be flagged with an `invalid_reason`.

#### `MERGE` for Incremental Processing

- **`MERGE` operations** ensure that the pipeline can process incremental changes efficiently, making updates and insertions based on the comparison of the `DateTimeStamp` and other relevant fields.

#### Reporting Database Push

- After data is processed and refined through the **Bronze**, **Silver**, and **Gold** layers, it is moved to the **Reporting Database** for final use.

#### Cost Considerations

- Since we are using **Delta Lake**, incremental data processing (via `MERGE`) ensures we minimize costs by only processing new or updated records.
- Validation checks may add some computational overhead, but this is necessary to ensure the data quality before it reaches the **Silver** and **Gold** layers.

### Event-Driven Architecture

#### Event Trigger

- The pipeline can be event-driven in **Azure** by triggering jobs in **Databricks** or **ADF** based on **Blob Storage** events (e.g., new or modified files in the **Landing Zone**).
- **ADF** can use event triggers or time-based triggers to monitor the **Landing Zone** for new files.
- **Databricks jobs** can also be scheduled or event-triggered, where a job is run every time a file is added or modified in the **Landing Zone**.

### Summary of Architecture Assumptions

- **Landing Zone** stores raw, unstructured data, where data quality issues may exist.
- **Bronze Layer** is an append-only layer that stores raw data for staging and basic transformations.
- **Silver Layer** applies validation and stores clean data, ready for more sophisticated analytics or reporting.
- **Gold Layer** provides business-ready data for final reporting or consumption.
- **Delta Lake** provides an ACID-compliant, transactional storage layer for the pipeline.
- **Databricks** is used for data processing and transformation, including validation, `MERGE`, and writing to **Delta tables**.
- **Azure Data Factory (ADF)** orchestrates the data pipeline and triggers **Databricks jobs**.
- **Reporting Database** holds the final, cleansed data used by reporting or analytics tools.

This architecture ensures the efficient, reliable, and cost-effective movement and processing of data through the stages of the pipeline, with incremental updates and validation at every step.

## Cost Effectiveness of the Pipeline Architecture

The proposed data pipeline architecture from **Landing Zone** to **Reporting Database** is designed with several optimizations that ensure it is cost-effective. These optimizations primarily focus on minimizing unnecessary computations, efficient data processing, and leveraging the scalability and features of **Azure** services like **Delta Lake** and **Databricks**. Below are the key factors contributing to the cost-effectiveness of the pipeline:

### 1. **Delta Lake** for Incremental Processing

#### Efficient Incremental Updates:
- By utilizing **Delta Lake's** `MERGE` functionality, we only process new or updated records, not the entire dataset. This incremental processing approach ensures that we are not re-processing all the data every time an update occurs, which would otherwise result in unnecessary computational costs.
- **Delta Lake** stores data as versions, which means that only changes (deltas) need to be written to the tables, reducing the volume of data processed at each step.

#### Efficient Storage Management:
- **Delta Lake** tables support partitioning, which further reduces the amount of data that needs to be read, processed, and written. For example, partitioning by `DateTimeStamp` (or other relevant fields) ensures that only data relevant to the current date or the changes is processed, rather than the entire dataset.
- **Delta Lake's** time travel and versioning features help reduce overhead by making it easy to track changes and rollback if needed, reducing the need for multiple data backups or full data refreshes.

### 2. Only Process Valid Records

#### Data Validation Before Processing:
- Validation checks are applied before records are pushed into the **Silver** layer, ensuring that only valid records are processed further. Invalid records are flagged with an `invalid_reason` column and excluded from further processing.
- This reduces downstream processing costs by ensuring that only high-quality data (which meets predefined criteria) is propagated to the **Silver** and **Gold** layers. It also prevents wasting compute resources on handling invalid or erroneous data.

#### Prevention of Data Overload:
- By validating and discarding invalid records early (in the **Silver** layer), we prevent the **Gold** layer and **Reporting Database** from holding unnecessary data that could impact reporting performance and lead to higher storage costs.

### 3. Avoidance of Full Data Refreshes

#### Append-Only Strategy:
- Both the **Bronze** and **Silver** layers use an append-only strategy for data processing. This means that only new or updated records are added to these layers. Rather than refreshing the entire dataset, the pipeline only performs updates when necessary, which significantly reduces the volume of data written and processed.

#### Efficient Delta `MERGE` Logic:
- The `MERGE` operation in **Delta Lake** ensures that we are not overwriting existing data but rather updating only the necessary rows based on unique keys (e.g., `DateTimeStamp`). This reduces the time spent on rewriting entire tables and minimizes computational overhead.

### 4. Scalability and On-Demand Resources

#### Dynamic Scaling of Compute Resources:
- In **Databricks**, compute resources can be dynamically scaled based on the actual workload. For instance, when data processing requirements increase due to a high volume of records in the **Landing Zone**, the pipeline can scale up the compute resources to handle the load efficiently and scale back down when the workload decreases, ensuring that resources are used only when necessary.
- **Databricks jobs** can be triggered only when required, so we don't have idle resources running unnecessarily. The cost of running these jobs is tied to the actual usage and job execution time.

### 5. **Azure Data Factory** Orchestration

#### Event-Driven Orchestration:
- By using **Azure Data Factory (ADF)**, we can trigger the pipeline on file arrival events in the **Landing Zone** (via **Azure Blob Storage** event triggers). This ensures that jobs run only when new or updated data is available, preventing unnecessary execution of the pipeline when there is no new data to process.
- This event-driven execution ensures that **ADF** triggers are highly responsive, minimizing the time resources are allocated for processing and reducing costs associated with idle compute resources.

#### Cost-Effective Orchestration:
- **ADF** provides the flexibility to orchestrate jobs across different services, and it integrates seamlessly with **Databricks**. **ADF’s** pricing model is based on pipeline activities (such as running a job) and data movement, so we can optimize the orchestration by triggering **Databricks jobs** based on relevant events and only processing the data when required.

### 6. Use of Parquet Format in Silver and Gold Layers

#### Columnar Storage Format:
- Storing data in **Parquet** format in the **Silver** and **Gold** layers is highly efficient for both storage and read performance due to **Parquet's** columnar nature. **Parquet** files are compressed, which reduces storage costs, and their efficient column-based access makes it faster to query only the required columns, thus reducing the I/O costs associated with querying large datasets.

#### Performance Optimizations:
- **Parquet’s** compression helps lower the storage footprint, and the split/partition capabilities allow for more efficient querying, especially when coupled with partitioned **Delta tables**. This leads to faster processing and reduces the overall time taken for both batch and real-time data processing tasks.

### 7. Cost-Effective Reporting Database Integration

#### Incremental Updates to Reporting DB:
- The pipeline ensures that the **Gold** layer only pushes new or updated records to the **Reporting Database**. This avoids unnecessary data duplication and reduces the storage and query costs in the reporting database.
- The `MERGE` logic in the pipeline ensures that only incremental changes are pushed to the reporting database, which keeps the database smaller, improving query performance and reducing storage costs.

### Summary of Cost-Effectiveness Factors:
1. **Delta Lake** and `MERGE` operations ensure that only new or updated records are processed, saving on computation and storage.
2. Data validation early in the pipeline prevents invalid data from progressing, saving processing time and storage resources.
3. Append-only and incremental updates prevent the need to reprocess entire datasets, reducing resource usage.
4. Dynamic scaling of compute resources in **Databricks** ensures that we only pay for compute time when it’s necessary.
5. Event-driven orchestration with **ADF** ensures that jobs are triggered only when new data arrives, avoiding idle time.
6. Efficient data formats like **Parquet** and columnar storage optimize both storage and query costs.
7. Efficient transfer to reporting database ensures only necessary data is moved to the final destination, reducing costs associated with large database storage.

By leveraging **Delta Lake**, event-driven orchestration, and incremental processing, this architecture is designed to maximize resource utilization, minimize unnecessary processing, and keep overall pipeline costs low.

## Uncertainty Factors in the Choice of Architecture and Pipeline Structure

When designing a data pipeline architecture, several uncertainty factors can influence the choice of architecture and pipeline structure. These factors can impact the overall performance, scalability, and cost-effectiveness of the pipeline. Below are some key uncertainty factors to consider:

### 1. Data Volume and Velocity

- The volume and velocity of incoming data can vary significantly, affecting the choice of storage and processing technologies. High data volume and velocity may require more scalable and performant solutions, such as **Delta Lake** and **Databricks**, to handle the load efficiently.

### 2. Data Quality and Consistency

- The quality and consistency of incoming data can impact the complexity of data validation and transformation processes. Poor data quality may require additional preprocessing steps, such as deduplication and validation, to ensure that only clean and accurate data is processed further.

### 3. Schema Evolution

- Changes in the data schema, such as the addition or removal of columns, can affect the pipeline's ability to process data correctly. The pipeline should be designed to handle schema evolution gracefully, using techniques like schema inference and versioning to accommodate changes.

### 4. Data Processing Requirements

- The specific data processing requirements, such as the need for real-time processing or complex transformations, can influence the choice of processing technologies and pipeline structure. Real-time processing may require event-driven architectures and streaming technologies, while complex transformations may benefit from the use of distributed processing frameworks like **Apache Spark**.

### 5. Integration with External Systems

- The need to integrate with external systems, such as third-party data sources or downstream reporting tools, can impact the choice of data formats and transfer mechanisms. The pipeline should be designed to support seamless integration with these systems, using standardized data formats and APIs.

### 6. Cost Constraints

- Budget constraints can influence the choice of technologies and the overall design of the pipeline. Cost-effective solutions, such as using cloud-based services with pay-as-you-go pricing models, can help manage costs while still meeting performance and scalability requirements.

### 7. Security and Compliance

- Security and compliance requirements, such as data encryption and access controls, can impact the choice of storage and processing technologies. The pipeline should be designed to meet these requirements, using secure storage solutions and implementing appropriate access controls.

### 8. Team Expertise

- The expertise and experience of the development team can influence the choice of technologies and the overall design of the pipeline. Leveraging technologies that the team is familiar with can help ensure successful implementation and maintenance of the pipeline.

By considering these uncertainty factors, the pipeline architecture can be designed to be more resilient, scalable, and cost-effective, ensuring that it meets the needs of the organization and can adapt to changing requirements.

## Pipeline Overview & Files

This pipeline is designed to process incoming data updates from an external company in the landing zone and ensure that any changes (deltas) are accurately reflected in the reporting database. The pipeline consists of multiple steps, each performed by a separate script:

1. **Data Ingestion ([landing_to_bronze.py](./pipeline-scripts/moving-data/landing_to_bronze.py))**
   - Monitors the landing zone for new or updated data.
   - Loads raw data into a staging area (Bronze layer) for further processing.

2. **Data Cleaning and Transformation ([bronze_to_silver.py](./pipeline-scripts/moving-data/bronze_to_silver.py))**
   - Compares incoming data with existing records in the Bronze layer.
   - Identifies changes, including new records, updates, and deletions.

3. **Further Data Transformation ([silver_to_gold.py](./pipeline-scripts/moving-data/silver_to_gold.py))**
   - Applies additional transformations and validations to the data in the Silver layer.
   - Prepares the data for the Gold layer.

4. **Database Update ([gold_to_reporting_db.py](./pipeline-scripts/moving-data/gold_to_reporting_db.py))**
   - Applies the detected changes to the reporting database, ensuring consistency.
   - Logs the updates for audit and monitoring purposes.

This modular approach ensures flexibility, scalability, and easier maintenance of the data pipeline.
