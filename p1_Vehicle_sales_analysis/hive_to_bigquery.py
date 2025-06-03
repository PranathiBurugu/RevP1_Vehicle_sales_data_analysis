from pyspark.sql import SparkSession

# Step 1: Start Spark session with Hive support
spark = SparkSession.builder \
    .appName("ExportHiveTablesIndividuallyToBigQuery") \
    .enableHiveSupport() \
    .getOrCreate()

# Temporary GCS bucket used by BigQuery connector
gcs_bucket = "p1-vehicles-sales-bucket"  

# === Export dim_vehicles ===
fact_vehicles_df = spark.sql("SELECT * FROM default.fact_vehicles")
fact_vehicles_df.write \
    .format("bigquery") \
    .option("table", "zinc-wares-460713-v0.p1_vehicle_sales_dataset.dim_vehicles_table") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

# === Export dim_sellers ===
dim_sellers_df = spark.sql("SELECT * FROM default.dim_sellers")
dim_sellers_df.write \
    .format("bigquery") \
    .option("table", "zinc-wares-460713-v0.p1_vehicle_sales_dataset.dim_vehicles_table") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

# === Export dim_dates ===
dim_dates_df = spark.sql("SELECT * FROM default.dim_dates")
dim_dates_df.write \
    .format("bigquery") \
    .option("table", "zinc-wares-460713-v0.p1_vehicle_sales_dataset.dim_vehicles_table") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

# === Export dim_vehicle_details ===
dim_vehicle_details_df = spark.sql("SELECT * FROM default.dim_vehicle_details")
dim_vehicle_details_df.write \
    .format("bigquery") \
    .option("table", "zinc-wares-460713-v0.p1_vehicle_sales_dataset.dim_vehicles_table") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()