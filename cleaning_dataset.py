from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, desc, lit, when, isnan
from pyspark.sql.types import StringType, NumericType

# Initialize SparkSession with Hive support
spark = SparkSession.builder \
    .appName("HDFS to Hive Example") \
    .enableHiveSupport() \
    .getOrCreate()

# Input HDFS file path
hdfs_path = "hdfs:///user/data/data_23_raw.csv"

# Read CSV from HDFS
df = spark.read.option("header", "true").csv(hdfs_path)

# Drop duplicate rows by VIN
df = df.dropDuplicates(['vin'])

# Handle nulls per column
for field in df.schema.fields:
    col_name = field.name
    dtype = field.dataType

    null_count = df.filter(col(col_name).isNull() | isnan(col(col_name))).count()

    if null_count > 0:
        print(f"Handling NULLs in column: {col_name} (type: {dtype}) with {null_count} missing values.")

        if isinstance(dtype, NumericType):
            # Fill numeric nulls with mean
            mean_val = df.select(mean(col_name)).first()[0]
            if mean_val is not None:
                df = df.withColumn(col_name,
                    when(col(col_name).isNull() | isnan(col(col_name)), lit(mean_val)).otherwise(col(col_name))
                )
            else:
                df = df.fillna(0, subset=[col_name])

        elif isinstance(dtype, StringType):
            # Fill string nulls with mode
            mode_val = df.groupBy(col_name).count().orderBy(desc("count")).first()
            if mode_val is not None:
                df = df.withColumn(col_name,
                    when(col(col_name).isNull(), lit(mode_val[0])).otherwise(col(col_name))
                )
            else:
                df = df.fillna("Unknown", subset=[col_name])

        else:
            # Drop rows for unsupported types
            df = df.filter(col(col_name).isNotNull())

# Output path on local filesystem or cloud
output_path = "gs://vechiles-11/vehicle-output"
df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path)
print(f"Data cleaning complete. Output written to: {output_path}")
