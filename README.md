# InvestCloud: Scalable Streaming ETL Pipeline

This repository contains a solution for the InvestCloud Senior Data Engineer technical assignment, focusing on designing and implementing a scalable ETL pipeline for large-scale user activity log data. The solution leverages modern data engineering practices and cloud-native Azure services.

## Project Overview

The objective is to process large volumes of user activity log data (simulated as 1-10 GB CSV files daily), perform transformations, and store the results efficiently. The pipeline is designed for high throughput, reliability, and maintainability in an Azure cloud environment.

**Key Requirements Addressed:**

* **Parallel Processing:** Achieved using Azure Databricks (Apache Spark) for distributed computation.
* **Data Ingestion:** Utilizing Azure Event Hubs for real-time ingestion and Event Hubs Capture for efficient landing in ADLS Gen2.
* **Data Transformations:**
    * Deduplication by `log_id` (both within files and globally).
    * Geo-enrichment (simulated IP-to-region mapping).
    * Aggregation of total watch time per user.
* **Data Quality Checks:** Implemented at the Bronze layer to quarantine invalid records.
* **Output Format:** Results written to compressed Parquet files (via Delta Lake).
* **Orchestration:** Designed for orchestration using Azure Data Factory.
* **Scalability & Reliability:** Leverages managed Azure services and Delta Lake for ACID properties.

## Architecture Design

The pipeline follows a multi-layered (Bronze, Silver, Gold) Data Lakehouse architecture on Azure.

**Services Used:**

* **Azure Event Hubs:** High-throughput event ingestion.
* **Azure Data Lake Storage Gen2 (ADLS Gen2):** Scalable storage for all data layers.
* **Azure Databricks:** Managed Apache Spark for distributed ETL processing.
* **Azure Data Factory (ADF):** (Recommended) For pipeline orchestration, scheduling, and monitoring.

A detailed explanation of the architecture, service roles, and data flow can be found in `docs/architecture_design.md`.

## Repository Structure
