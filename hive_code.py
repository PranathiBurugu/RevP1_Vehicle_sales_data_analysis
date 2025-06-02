from pyspark.sql import SparkSession

# Step 1: Start Spark session with Hive support
spark = SparkSession.builder \
    .appName("ExportHiveTablesIndividuallyToBigQuery") \
    .enableHiveSupport() \
    .getOrCreate()

# Temporary GCS bucket used by BigQuery connector
gcs_bucket = "vehicle-project1-bucket"  

# === Export dim_vehicles ===
fact_vehicles_df = spark.sql("SELECT * FROM default.fact_vehicle_sales_datas")
fact_vehicles_df.write \
    .format("bigquery") \
    .option("table", "compact-codex-461603-j5.normal_tbl.fact_vehicle_sales_datas") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

# === Export dim_sellers ===
dim_sellers_df = spark.sql("SELECT * FROM default.dim_sellers")
dim_sellers_df.write \
    .format("bigquery") \
    .option("table", "compact-codex-461603-j5.normal_tbl.dim_sellers") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

# === Export dim_dates ===
dim_dates_df = spark.sql("SELECT * FROM default.dim_dates")
dim_dates_df.write \
    .format("bigquery") \
    .option("table", "compact-codex-461603-j5.normal_tbl.dim_dates") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

# === Export dim_vehicle_details ===
dim_vehicle_details_df = spark.sql("SELECT * FROM default.dim_vehicle_details")
dim_vehicle_details_df.write \
    .format("bigquery") \
    .option("table", "compact-codex-461603-j5.normal_tbl.dim_vehicles") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()
