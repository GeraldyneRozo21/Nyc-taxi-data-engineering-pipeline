# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — Raw ingestion from the public TLC source
# MAGIC Downloads 3 months of Yellow Taxi data (January-March 2024, ~9M rows)
# MAGIC directly from `https://d37ci6vzurychx.cloudfront.net/trip-data/`, lands
# MAGIC them in a Unity Catalog Volume, and converts them into a Delta table
# MAGIC with traceability metadata. No business logic here — that lives in
# MAGIC Silver.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## 1. Create catalog, schemas, and volume

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS nyc_taxi")
spark.sql("CREATE SCHEMA IF NOT EXISTS nyc_taxi.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS nyc_taxi.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS nyc_taxi.gold")

# The Volume is the "landing zone": managed storage accessible by every
# node in the cluster, unlike /tmp (only local to the driver).
spark.sql("CREATE VOLUME IF NOT EXISTS nyc_taxi.bronze.landing_zone")

# COMMAND ----------

# MAGIC %md ## 2. Download the source files
# MAGIC Each URL corresponds to one month of Yellow Taxi trips, published
# MAGIC directly by the NYC TLC. `-q` silences wget's output and `-O` fixes
# MAGIC the output filename, making the download idempotent.

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /Volumes/nyc_taxi/bronze/landing_zone/yellow_2024
# MAGIC cd /Volumes/nyc_taxi/bronze/landing_zone/yellow_2024
# MAGIC wget -q -O yellow_tripdata_2024-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
# MAGIC wget -q -O yellow_tripdata_2024-02.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
# MAGIC wget -q -O yellow_tripdata_2024-03.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet
# MAGIC ls -lh /Volumes/nyc_taxi/bronze/landing_zone/yellow_2024

# COMMAND ----------

# MAGIC %md
# MAGIC If this cell fails with something like "Could not resolve host" or a
# MAGIC timeout, it means the cluster has no outbound internet access — some
# MAGIC workspaces block it by policy. If that happens, upload the files
# MAGIC manually to the Volume instead.

# COMMAND ----------

# MAGIC %md ## 3. Read the 3 files as a single DataFrame
# MAGIC The `*.parquet` wildcard tells Spark to read every file matching the
# MAGIC pattern in a single distributed operation.

# COMMAND ----------

raw_path = "/Volumes/nyc_taxi/bronze/landing_zone/yellow_2024/*.parquet"
raw_df = spark.read.parquet(raw_path)

print(f"Rows read: {raw_df.count():,}")
raw_df.printSchema()

# COMMAND ----------

# MAGIC %md ## 4. Add traceability metadata
# MAGIC `F.col("_metadata.file_path")` captures, per row, which physical file
# MAGIC it came from — useful for auditing which month each record belongs
# MAGIC to without relying solely on the date column. Note: Unity Catalog
# MAGIC blocks the older `input_file_name()` function for governance reasons
# MAGIC (it can expose storage paths outside of UC's permission model) — the
# MAGIC `_metadata` column is the governed equivalent.

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit("NYC TLC - Yellow Taxi 2024-01 to 2024-03"))
)

# COMMAND ----------

# MAGIC %md ## 5. Write the Bronze table

# COMMAND ----------

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("nyc_taxi.bronze.trips_raw")
)

print(f"Bronze written: {spark.table('nyc_taxi.bronze.trips_raw').count():,} rows")

# COMMAND ----------

display(spark.table("nyc_taxi.bronze.trips_raw").limit(10))
