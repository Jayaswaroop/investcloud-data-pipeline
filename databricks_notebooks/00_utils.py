import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType
import random

# --- Configuration ---
# Replace with your actual mounted paths and secret scope details
# These paths assume ADLS Gen2 containers are mounted under /mnt/
STORAGE_ACCOUNT_NAME = "<your_storage_account_name>"
ADLS_CLIENT_ID = "<your_service_principal_client_id>"
ADLS_TENANT_ID = "<your_tenant_id>"
DATABRICKS_SECRET_SCOPE = "investcloud-scope" # Name of your Databricks secret scope
ADLS_CLIENT_SECRET_KEY = "adls-client-secret" # Key for your client secret in the scope

RAW_DATA_PATH = "/mnt/raw-data/user-activity-logs/"
BRONZE_LAYER_PATH = "/mnt/raw-data/user-activity-parsed/" # Auto Loader output path
BRONZE_BAD_RECORDS_PATH = "/mnt/raw-data/user-activity-logs/_bad_records/" # For parsing errors
BRONZE_QUARANTINE_PATH = "/mnt/raw-data/user-activity-parsed/_quarantine/" # For semantic errors

SILVER_LAYER_PATH = "/mnt/curated-data/user-activity-deduplicated/"
GOLD_LAYER_PATH = "/mnt/aggregated-data/user-activity-summary/"

CHECKPOINT_LOCATION_BASE = "/mnt/checkpoints/"
CHECKPOINT_LOCATION_BRONZE_VALID = CHECKPOINT_LOCATION_BASE + "bronze_user_activity_valid/"
CHECKPOINT_LOCATION_BRONZE_QUARANTINE = CHECKPOINT_LOCATION_BASE + "bronze_user_activity_quarantine/"
CHECKPOINT_LOCATION_SILVER = CHECKPOINT_LOCATION_BASE + "silver_user_activity/"
CHECKPOINT_LOCATION_GOLD = CHECKPOINT_LOCATION_BASE + "gold_user_activity/"

# Define the schema for the incoming raw Parquet files from Event Hubs Capture
# Assuming Event Hubs Capture writes the original CSV content as Parquet files with this schema.
RAW_SCHEMA = StructType([
    StructField("log_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True), # Raw timestamp string
    StructField("ip_address", StringType(), True),
    StructField("watch_time(min)", LongType(), True)
])

# Mock IP to Geo-Region Mapping
# In a production environment, this would be a lookup against a dedicated Geo-IP database (e.g., MaxMind GeoLite2)
# or a small Delta table loaded into memory and broadcasted.
IP_TO_REGION_MAPPING = {
    '192.168.1.1': 'North America',
    '192.168.1.2': 'North America',
    '192.168.1.3': 'Europe',
    '192.168.1.4': 'Asia',
    '192.168.1.5': 'South America',
    '192.168.1.6': 'Africa',
    '192.168.1.7': 'Oceania',
    **{f"192.168.1.{i}": random.choice(['North America', 'Europe', 'Asia']) for i in range(8, 255)},
    **{f"10.0.0.{i}": random.choice(['Europe', 'Asia', 'North America']) for i in range(255)},
    **{f"172.16.0.{i}": random.choice(['South America', 'Africa', 'Oceania']) for i in range(255)},
}

# Register UDF for IP to Region mapping
@F.udf(StringType())
def map_ip_to_region_udf(ip_address):
    return IP_TO_REGION_MAPPING.get(ip_address, 'Unknown')

# Function to mount ADLS Gen2 (to be run once in a separate notebook/cell)
def mount_adls_gen2(storage_account, client_id, tenant_id, secret_scope, secret_key):
    try:
        client_secret = dbutils.secrets.get(scope=secret_scope, key=secret_key)
        configs = {"fs.azure.account.auth.type": "OAuth",
                   "fs.azure.account.oauth2.client.id": client_id,
                   "fs.azure.account.oauth2.client.secret": client_secret,
                   "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

        mount_points = {
            "raw-data": f"abfss://raw-data@{storage_account}.dfs.core.windows.net/",
            "curated-data": f"abfss://curated-data@{storage_account}.dfs.core.windows.net/",
            "aggregated-data": f"abfss://aggregated-data@{storage_account}.dfs.core.windows.net/",
            "checkpoints": f"abfss://checkpoints@{storage_account}.dfs.core.windows.net/"
        }

        for name, source_path in mount_points.items():
            mount_point = f"/mnt/{name}"
            if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
                dbutils.fs.mount(
                  source = source_path,
                  mount_point = mount_point,
                  extra_configs = configs)
                print(f"Mounted {source_path} to {mount_point}")
            else:
                print(f"{mount_point} already mounted.")
        print("ADLS Gen2 containers mounted successfully!")
    except Exception as e:
        print(f"Error mounting ADLS Gen2: {e}")
        print("Please ensure your Service Principal details are correct and secrets are configured.")

# Example of how to call mount_adls_gen2 in a separate notebook cell:
# from databricks_notebooks.00_utils import mount_adls_gen2, STORAGE_ACCOUNT_NAME, ADLS_CLIENT_ID, ADLS_TENANT_ID, DATABRICKS_SECRET_SCOPE, ADLS_CLIENT_SECRET_KEY
# mount_adls_gen2(STORAGE_ACCOUNT_NAME, ADLS_CLIENT_ID, ADLS_TENANT_ID, DATABRICKS_SECRET_SCOPE, ADLS_CLIENT_SECRET_KEY)
