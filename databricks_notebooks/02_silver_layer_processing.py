import pyspark.sql.functions as F
from pyspark.sql.streaming import StreamingQueryException
from databricks_notebooks.00_utils import (
    BRONZE_LAYER_PATH, SILVER_LAYER_PATH, CHECKPOINT_LOCATION_SILVER,
    map_ip_to_region_udf
)

print("Starting Silver Layer processing notebook...")

# Read from the Bronze Delta table as a stream
silver_input_df_stream = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true") # Only process new appends, ignore updates/deletes in Bronze
    .load(BRONZE_LAYER_PATH)
)

# 1. Global Deduplication by log_id
silver_deduplicated_df = (
    silver_input_df_stream
    .withWatermark("timestamp", "2 hours") # Adjust watermark duration based on expected late arrivals
    .dropDuplicates(["log_id"])
)

# 2. Enrich data by adding geo_region
silver_enriched_df = silver_deduplicated_df.withColumn("geo_region", map_ip_to_region_udf(F.col("ip_address")))

# --- Write Stream for Silver Layer ---
print(f"Writing processed data to Silver Layer: {SILVER_LAYER_PATH}")
silver_query = (
    silver_enriched_df.writeStream
    .format("delta")
    .outputMode("append") # Use append mode for new unique records
    .option("checkpointLocation", CHECKPOINT_LOCATION_SILVER)
    .trigger(processingTime="1 minute") # Process every minute
    .start(SILVER_LAYER_PATH)
)

# For Databricks Jobs orchestrated by ADF, you typically don't await termination here.
# try:
#     silver_query.awaitTermination(timeout=3600) # Wait for 1 hour for testing
# except StreamingQueryException as e:
#     print(f"Streaming query terminated with error: {e}")
# print("Silver Layer stream started and potentially awaited for testing.")
