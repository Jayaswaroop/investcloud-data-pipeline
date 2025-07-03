## Setup Guide

Follow these steps to set up and run the pipeline in your Azure environment.

### 1. Azure Resource Provisioning

Provision the following Azure resources:

* **Azure Event Hubs:**
    * Create an Event Hubs Namespace (e.g., `investcloud-log-events-ns`).
    * Create an Event Hub (e.g., `user-activity-logs`).
    * **Enable Event Hubs Capture:** Configure it to write to your ADLS Gen2 account (see below) in Parquet format, with a suitable capture window (e.g., 5 minutes).
* **Azure Data Lake Storage Gen2 (ADLS Gen2):**
    * Create a Storage Account with Hierarchical Namespace enabled.
    * Create the following containers:
        * `raw-data`
        * `curated-data`
        * `aggregated-data`
        * `checkpoints` (for Databricks streaming checkpoints)
* **Azure Databricks Workspace:**
    * Create an Azure Databricks workspace.
    * Configure a cluster with a suitable Databricks Runtime (e.g., 11.3 LTS or higher with Spark 3.x), auto-scaling enabled, and appropriate worker VM sizes.
* **Azure Data Factory (ADF):** (Optional, but recommended for orchestration)
    * Create an Azure Data Factory instance.

### 2. Service Principal for ADLS Gen2 Access

Databricks needs permissions to read/write to ADLS Gen2. Use an Azure Service Principal:

1.  Create an Azure AD Application (Service Principal).
2.  Grant the Service Principal **"Storage Blob Data Contributor"** role on your ADLS Gen2 Storage Account (or specific containers).
3.  Record the **Application (client) ID**, **Directory (tenant) ID**, and generate a **Client Secret** (value). **Store the Client Secret securely in Azure Key Vault or Databricks Secrets.**

### 3. Databricks Secrets (Highly Recommended)

Store your Service Principal's client secret in Databricks Secrets:

1.  Create a secret scope (e.g., `investcloud-scope`).
    ```bash
    # In Databricks CLI or Databricks Notebook
    databricks secrets create-scope --scope investcloud-scope
    ```
2.  Add your client secret to the scope.
    ```bash
    # In Databricks CLI
    databricks secrets put --scope investcloud-scope --key adls-client-secret
    # You will be prompted to enter the secret value
    ```

### 4. Mount ADLS Gen2 in Databricks

Execute the mounting script (`databricks_notebooks/00_utils.py` - see below for content) in a Databricks notebook **once**. Replace placeholders with your actual values and use the secret scope for the client secret.

### 5. Import Databricks Notebooks

Import the Python files from the `databricks_notebooks/` directory into your Databricks workspace. You can do this manually via the Databricks UI (Workspace -> Import) or programmatically using the Databricks CLI/API.

### 6. Local Data Generation (for testing)

If you want to test the pipeline locally or generate mock data for your ADLS Gen2 `raw-data` container (bypassing Event Hubs for initial testing), use the `data_generator.py` script:

1.  Ensure you have Python and `pandas`, `pyarrow` installed (`pip install -r requirements.txt`).
2.  Run the script:
    ```bash
    python data_generator/data_generator.py
    ```
    This will create an `input_data/` directory with mock CSV files. You can then manually upload these to your `raw-data/user-activity-logs/` container in ADLS Gen2 for testing the Databricks processing.

## How to Run the Pipeline

Once all setup steps are complete:

### Option A: Running as Databricks Jobs (Manual Trigger)

1.  In your Databricks workspace, navigate to "Workflows" -> "Jobs".
2.  Create a new Job.
3.  Add tasks for each notebook:
    * **Task 1 (Bronze):** Select `01_bronze_layer_ingestion.py`.
    * **Task 2 (Silver):** Select `02_silver_layer_processing.py`. Set its dependency to "Run if successful" on Task 1.
    * **Task 3 (Gold):** Select `03_gold_layer_aggregation.py`. Set its dependency to "Run if successful" on Task 2.
4.  Configure the cluster for each task (you can use the same cluster).
5.  Run the job manually or schedule it within Databricks.

### Option B: Orchestrating with Azure Data Factory (Recommended for Production)

1.  In your Azure Data Factory instance, create a new pipeline.
2.  Add three "Databricks Notebook" activities:
    * Drag and drop three "Databricks Notebook" activities onto the canvas.
    * **Linked Service:** Create a linked service to connect ADF to your Azure Databricks workspace.
    * **Notebook Path:** For each activity, specify the path to the corresponding notebook (`01_bronze_layer_ingestion.py`, `02_silver_layer_processing.py`, `03_gold_layer_aggregation.py`).
    * **Dependencies:** Connect the activities with "On Success" arrows: Bronze -> Silver -> Gold.
3.  **Trigger:** Add a schedule trigger to your ADF pipeline (e.g., daily, hourly).
4.  Publish and monitor your ADF pipeline.

## Key Features and Design Decisions

* **Layered Data Lakehouse:** Bronze (raw), Silver (curated), Gold (aggregated) layers in ADLS Gen2 using Delta Lake for ACID properties.
* **Streaming Ingestion:** Event Hubs Capture automatically lands data to ADLS Gen2, simplifying streaming data ingestion.
* **Auto Loader:** Efficiently processes new files incrementally from ADLS Gen2, handling schema evolution and bad records.
* **Data Quality Gates:** Explicit semantic validation in the Bronze layer, quarantining invalid records for review.
* **Global Deduplication:** Using Spark Structured Streaming's `withWatermark` and `dropDuplicates` for robust deduplication across micro-batches.
* **Scalable Transformations:** Leveraging Spark's distributed processing capabilities for all ETL steps.
* **Modular Code:** Separating concerns into distinct Databricks notebooks for better organization and job management.

## Future Enhancements

* **Alerting:** Integrate Azure Monitor alerts for pipeline failures or data quality issues (e.g., high volume in quarantine zone).
* **Data Lineage & Governance:** Implement tools like Azure Purview for end-to-end data lineage.
* **Performance Optimization:** Fine-tune Spark configurations (e.g., shuffle partitions, memory settings) based on actual data volume and cluster performance.
* **Cost Management:** Implement stricter cluster policies in Databricks, explore Reserved Instances for long-running clusters.
* **CI/CD:** Automate deployment of Databricks notebooks and ADF pipelines using Azure DevOps or GitHub Actions.
* **More Robust Geo-IP Mapping:** Integrate with a dedicated Geo-IP database (e.g., MaxMind GeoLite2) or a managed Azure service.
