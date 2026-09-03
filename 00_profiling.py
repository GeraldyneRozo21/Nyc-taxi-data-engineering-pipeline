# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Profiling — NYC Taxi Dataset
# MAGIC **Note:** this notebook was originally run against `samples.nyctaxi.trips`
# MAGIC (sample dataset, 21,932 rows) to validate the source. After that
# MAGIC profiling, the decision was made to switch to the full public TLC
# MAGIC source (Yellow Taxi 2024-01 to 2024-03, ~9M rows) for real volume. This
# MAGIC notebook can be re-run against `nyc_taxi.bronze.trips_raw` (see notebook
# MAGIC 01) to profile the new source before building Silver.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## 1. Locate the dataset

# COMMAND ----------

display(spark.sql("SHOW SCHEMAS IN samples"))

# COMMAND ----------

display(spark.sql("SHOW TABLES IN samples.nyctaxi"))

# COMMAND ----------

# Original source (sample dataset, already validated):
# df = spark.table("samples.nyctaxi.trips")

# New source (public TLC, run after notebook 01_bronze.py):
df = spark.table("nyc_taxi.bronze.trips_raw")
display(df.limit(20))

# COMMAND ----------

# MAGIC %md ## 2. Volume and schema

# COMMAND ----------

row_count = df.count()
col_count = len(df.columns)
print(f"Total rows: {row_count:,}")
print(f"Total columns: {col_count}")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md ## 3. Nulls per column

# COMMAND ----------

null_counts = df.select(
    [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
)
display(null_counts)

# COMMAND ----------

# MAGIC %md ## 4. Descriptive statistics of numeric columns

# COMMAND ----------

display(df.describe())

# COMMAND ----------

# MAGIC %md ## 5. Date range covered

# COMMAND ----------

display(
    df.select(
        F.min("tpep_pickup_datetime").alias("min_pickup"),
        F.max("tpep_pickup_datetime").alias("max_pickup"),
    )
)

# COMMAND ----------

# MAGIC %md ## 6. Preliminary anomaly detection
# MAGIC This does NOT clean anything yet, it just quantifies how many records
# MAGIC are suspicious, to decide the cleaning rules for the Silver layer.

# COMMAND ----------

anomaly_summary = df.select(
    F.sum(F.when(F.col("fare_amount") <= 0, 1).otherwise(0)).alias("fare_zero_or_negative"),
    F.sum(F.when(F.col("trip_distance") <= 0, 1).otherwise(0)).alias("distance_zero_or_negative"),
    F.sum(
        F.when(
            (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) <= 0,
            1,
        ).otherwise(0)
    ).alias("duration_zero_or_negative"),
    F.sum(
        F.when(
            (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) > 3 * 3600,
            1,
        ).otherwise(0)
    ).alias("duration_over_3h"),
)
display(anomaly_summary)

# COMMAND ----------

# MAGIC %md ## 7. Conclusion
# MAGIC
# MAGIC Fill in manually after running this notebook:
# MAGIC
# MAGIC - **Total rows:** ___
# MAGIC - **Is the volume enough to justify partitioning?** Yes / No
# MAGIC - **% of relevant nulls:** ___
# MAGIC - **Date range covered:** ___
# MAGIC - **Cleaning rules to apply in Silver:** (based on section 6)
# MAGIC
# MAGIC These findings are documented in `docs/data_quality_report.md`.
