# Azure Event Hub: Handling Batch and Streaming Data

This document provides an overview of how to handle batch and streaming data using Azure Event Hub. It covers the purpose, setup, and configuration of Event Hub, as well as strategies for ingesting both streaming and batch data.

## Table of Contents
- [Scenario](#scenario)
- [Understanding the Scenario](#understanding-the-scenario)
- [Azure Event Hub: Purpose, Setup, and Configuration](#azure-event-hub-purpose-setup-and-configuration)
  - [Purpose of Event Hub](#purpose-of-event-hub)
  - [Setting Up Event Hub](#setting-up-event-hub)
- [Setting Up Event Hub for Streaming and Batch Data](#setting-up-event-hub-for-streaming-and-batch-data)
  - [Streaming Data Ingestion (Real-Time)](#streaming-data-ingestion-real-time)
  - [Batch Data Ingestion (Challenges & Limitations)](#batch-data-ingestion-challenges--limitations)
- [How to Ingest Batch Data into Azure (Preferred Approach)](#how-to-ingest-batch-data-into-azure-preferred-approach)
  - [Recommended Flow for Batch Data](#-recommended-flow-for-batch-data)
- [How to Send Batch Data to Event Hub (If Absolutely Required)](#how-to-send-batch-data-to-event-hub-if-absolutely-required)
- [Summary](#summary)

## Scenario
We have data coming from different sources (including streaming, batch). The processed data from this source should be sent to the Event Hub, what is your approach to solve this?

## Understanding the Scenario
As I understand it, our Azure account is a data platform mainly built for Business Intelligence (BI). Since BI focuses on storage, analytics, and insights, I don’t see why we would send processed data back to Event Hub, which is not meant for that purpose. Instead, Event Hub seems to be used as a message broker to handle real-time event ingestion and distribution.

With this in mind, the likely intent behind the question is:

> 💡 **"How do we ingest both streaming and batch data into our Azure data platform, with Event Hub handling the streaming data?"**

To break it down further:

- Event Hub acts as the entry point for streaming data, receiving real-time events from external sources.
- Batch data ingestion also needs to be handled, coming from sources like databases, files, or APIs.
- The core challenge is understanding how both streaming and batch data should be efficiently ingested into our BI-focused Azure platform.

This scenario sets the stage for designing an appropriate data ingestion and processing strategy within our platform.

## Azure Event Hub: Purpose, Setup, and Configuration

### 🎯 Purpose of Event Hub
Azure Event Hub is a real-time data ingestion service optimized for high-throughput streaming data. It acts as a central hub to receive, store, and distribute event data to various consumers, enabling real-time analytics and processing.

### ⚙️ Setting Up Event Hub
Event Hub can be set up using the Azure Portal, ARM Templates, Terraform, CLI, or SDKs. The following are key steps:
1. Create an Event Hubs Namespace
    * Go to Azure Portal → Search for Event Hubs → Click Create.
    * Choose a Resource Group and provide a Namespace Name.
    * Select the Pricing Tier (Basic, Standard, or Premium).
2. Create an Event Hub
    * Inside the namespace, create an Event Hub instance.
    * Configure partitions (for parallel processing).
    * Set message retention (default 1 day, max 7 days in Standard).
3. Configure Authentication & Networking
    * Use Shared Access Policies (SAS) or Managed Identity for security.
    * Optionally enable Virtual Network (VNet) integration for security.

## Setting Up Event Hub for Streaming and Batch Data

### Streaming Data Ingestion (Real-Time)
💾  **Common Sources:** IoT devices, logs, sensors, application telemetry, Kafka producers.

⚙️ **How to Set Up:**
* Use Azure SDK to send events.
* Direct integration with Azure Stream Analytics, Azure Functions, or Databricks.
* Kafka-compatible API for Kafka producers.

🧑‍💻 **Example (Python SDK - Streaming Producer):**
```python
from azure.eventhub import EventHubProducerClient, EventData

producer = EventHubProducerClient.from_connection_string("your_connection_string", eventhub_name="your_eventhub")
event_data_batch = producer.create_batch()
event_data_batch.add(EventData("streaming event"))
producer.send_batch(event_data_batch)
producer.close()
```

### Batch Data Ingestion (Challenges & Limitations)
💾  **Common Sources:** `CSV`, `JSON`, databases, files, bulk transactions.

⚠️ **Limitations of Sending Batch Data Directly to Event Hub:**
* Message Size Limit: Standard tier supports 256 KB, Premium supports 1 MB.
* No Native File Support: Event Hub handles event messages, not bulk files.
* Throughput Considerations: Sending large datasets requires splitting into smaller messages, which increases API calls and latency.

🚧 **How to Set Up (If Absolutely Needed)**
* Convert batch data into small messages (split large files into rows/records).
* Use Azure SDKs or Event Hub REST API to send messages.

🧑‍💻 **Example (Python - Batch Data as Messages):**
```python
import json
from azure.eventhub import EventHubProducerClient, EventData

data = [{"id": 1, "value": "row1"}, {"id": 2, "value": "row2"}]  # Simulated batch
producer = EventHubProducerClient.from_connection_string("your_connection_string", eventhub_name="your_eventhub")

event_data_batch = producer.create_batch()
for record in data:
    event_data_batch.add(EventData(json.dumps(record)))  # Convert to JSON string

producer.send_batch(event_data_batch)
producer.close()
```

## How to Ingest Batch Data into Azure (Preferred Approach)
Instead of sending batch data directly to Event Hub, it’s better to use Azure services designed for batch processing and then forward the data if needed.

### ✅ Recommended Flow for Batch Data
1. Ingest Data into Azure Data Lake Storage (ADLS) or Azure Blob Storage.
2. Process Data using Azure Databricks, Synapse, or Data Factory.
3. Send Processed Data to Event Hub (only if required).

### 🛠️ Tools for Batch Ingestion:
* Azure Data Factory (ADF) → ETL from databases, APIs, or files.
* Azure Databricks → Large-scale transformations before sending to Event Hub.
* Azure Synapse Analytics → Handling structured batch data before streaming.

### 🔹 Example: Azure Data Factory Pipeline to Event Hub
1. Create an ADF pipeline with a Copy Activity.
2. Set Blob Storage/ADLS as the source.
3. Set Event Hub as the sink/destination.
4. Configure a mapping to ensure proper format.

## How to Send Batch Data to Event Hub (If Absolutely Required)
📝 **If the requirement insists on sending batch data to Event Hub, follow these steps:**
1. Break the batch into individual messages (rows/JSON records).
2. Ensure each message is within the 256 KB/1 MB limit.
3. Use an Event Hub Producer (SDK, REST API, or Logic Apps).
4. Use Parallel Processing if needed to handle high-volume batch data.

💡 **Example Approaches:**
<ul style="list-style-type: none;">
<li>✅ Azure Function: Reads from ADLS and pushes row-by-row to Event Hub.</li>
<li>✅ Databricks Structured Streaming: Reads batch data, processes it, and sends</li>
<li>✅ Logic Apps: Automates transformation and event forwarding.</li>
</ul>

🧑‍💻 **Example: Databricks Structured Streaming to Event Hub**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

spark = SparkSession.builder.appName("EventHubBatch").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("abfss://your_adls_path")
df = df.select(to_json(struct("*")).alias("body"))  # Convert to JSON

# Write to Event Hub
df.writeStream \
  .format("eventhubs") \
  .option("checkpointLocation", "/your_checkpoint_dir") \
  .start()
```

## Summary
### ✅ Best Approach (Recommended)
* Batch Data → ADLS/Blob → Processing (Databricks/ADF) → Event Hub (if required).
* Event Hub is not optimized for bulk data, so structured ingestion is better.

### ❌ When to Avoid Sending Batch Data to Event Hub Directly
* Large files, database dumps, or bulk inserts (use ADLS instead).
* Data that doesn't require real-time processing.

### 🚧 If Batch Data Must Go to Event Hub
* Convert to small messages within size limits.
* Use Databricks Structured Streaming, Azure Functions, or SDKs.