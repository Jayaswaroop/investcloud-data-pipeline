import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.streaming import StreamingQueryException
from databricks_notebooks.00_utils import (
    RAW_DATA_PATH, BRONZE_LAYER_PATH, BRONZE_BAD_RECORDS_PATH,
    BRONZE_QUARANTINE_PATH, CHECKPOINT_LOCATION_BRONZE_VALID,
    CHECKPOINT_LOCATION_BRONZE_QUARANTINE
)

print("Starting Bronze Layer ingestion notebook...")

# Auto Loader options
auto_loader_options = {
    "cloudFiles.format": "parquet", # Assuming Event Hubs Capture writes Parquet
    "cloudFiles.schemaLocation": CHECKPOINT_LOCATION_BRONZE_VALID + "schema_inference/",
    "cloudFiles.inferColumnTypes": "true", # Let Auto Loader infer types for raw data
    "cloudFiles.maxFilesPerTrigger": 10, # Process up to 10 files per micro-batch
    "cloudFiles.maxBytesPerTrigger": "10g", # Process up to 10 GB per micro-batch
    "cloudFiles.badRecordsPath": BRONZE_BAD_RECORDS_PATH, # For unparseable files/records
    "cloudFiles.validateEncoding": "false"
}

# Read stream from the raw data path using Auto Loader
raw_df_stream = (
    spark.readStream
    .format("cloudFiles")
    .options(**auto_loader_options)
    .load(RAW_DATA_PATH)
)

# Apply transformations and quality checks
bronze_df = raw_df_stream.withColumnRenamed("watch_time(min)", "watch_time")

# Convert timestamp string to actual timestamp type and watch_time to double
bronze_df = bronze_df.withColumn("timestamp_parsed", F.to_timestamp(F.col("timestamp"))) \
                     .withColumn("watch_time_parsed", F.col("watch_time").cast(DoubleType()))

# Define conditions for valid records (semantic checks)
valid_conditions = (
    F.col("log_id").isNotNull() &
    F.col("user_id").isNotNull() &
    F.col("timestamp_parsed").isNotNull() &
    F.col("watch_time_parsed").isNotNull() &
    (F.col("watch_time_parsed") >= 0)
)

# Separate valid and invalid records
valid_records_df = bronze_df.filter(valid_conditions)
invalid_records_df = bronze_df.filter(~valid_conditions) \
                              .withColumn("dq_reason", F.lit("Failed semantic validation: missing required fields or invalid format")) \
                              .withColumn("processing_time", F.current_timestamp())

# Select and cast to the final parsed schema for valid records
valid_records_df = valid_records_df.select(
    F.col("log_id"),
    F.col("user_id"),
    F.col("timestamp_parsed").alias("timestamp"),
    F.col("ip_address"),
    F.col("watch_time_parsed").alias("watch_time")
)

# --- Write Streams for Bronze Layer ---

# Write valid records to the main Bronze Delta table
print(f"Writing valid records to Bronze Layer: {BRONZE_LAYER_PATH}")
bronze_valid_query = (
    valid_records_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION_BRONZE_VALID)
    .trigger(processingTime="1 minute") # Process every minute
    .start(BRONZE_LAYER_PATH)
)

# Write invalid records to the Quarantine Delta table
print(f"Writing invalid records to Quarantine Layer: {BRONZE_QUARANTINE_PATH}")
bronze_quarantine_query = (
    invalid_records_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION_BRONZE_QUARANTINE)
    .trigger(processingTime="1 minute") # Process every minute
    .start(BRONZE_QUARANTINE_PATH)
)

# For Databricks Jobs orchestrated by ADF, you typically don't await termination here.
# ADF will manage the job's lifecycle. However, for interactive testing, you might use:
# try:
#     bronze_valid_query.awaitTermination(timeout=3600) # Wait for 1 hour for testing
#     bronze_quarantine_query.awaitTermination(timeout=3600)
# except StreamingQueryException as e:
#     print(f"Streaming query terminated with error: {e}")
# print("Bronze Layer streams started and potentially awaited for testing.")
