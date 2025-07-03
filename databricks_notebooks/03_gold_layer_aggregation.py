import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.streaming import StreamingQueryException
from databricks_notebooks.00_utils import (
    SILVER_LAYER_PATH, GOLD_LAYER_PATH, CHECKPOINT_LOCATION_GOLD
)

print("Starting Gold Layer aggregation notebook...")

# Read from the Silver Delta table as a stream
gold_input_df_stream = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true") # Only process new appends from Silver
    .load(SILVER_LAYER_PATH)
)

# Calculate total watch time per user and determine representative geo_region
# For geo_region, we'll take the region associated with the highest watch time for that user.
# First, aggregate watch time by user and region
user_activity_agg = gold_input_df_stream.groupBy("user_id", "geo_region") \
                                        .agg(F.sum("watch_time").alias("watch_time_by_region"))

# Now, find the most frequent geo_region for each user based on watch_time
window_spec = Window.partitionBy("user_id").orderBy(F.col("watch_time_by_region").desc())

gold_aggregated_df = user_activity_agg.withColumn("rank", F.rank().over(window_spec)) \
                                      .filter(F.col("rank") == 1) \
                                      .groupBy("user_id") \
                                      .agg(F.sum("watch_time_by_region").alias("total_watch_time"),
                                           F.first("geo_region").alias("geo_region")) # Take the region with highest watch time

# --- Write Stream for Gold Layer ---
print(f"Writing aggregated data to Gold Layer: {GOLD_LAYER_PATH}")
gold_query = (
    gold_aggregated_df.writeStream
    .format("delta")
    .outputMode("complete") # Use complete mode for aggregations over entire dataset
    .option("checkpointLocation", CHECKPOINT_LOCATION_GOLD)
    .trigger(processingTime="1 minute") # Process every minute
    .start(GOLD_LAYER_PATH)
)

# For Databricks Jobs orchestrated by ADF, you typically don't await termination here.
# try:
#     gold_query.awaitTermination(timeout=3600) # Wait for 1 hour for testing
# except StreamingQueryException as e:
#     print(f"Streaming query terminated with error: {e}")
# print("Gold Layer stream started and potentially awaited for testing.")
