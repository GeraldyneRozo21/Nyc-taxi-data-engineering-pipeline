# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — Cleaning and validation
# MAGIC Applies quality rules defined during profiling (notebook 00), refined
# MAGIC iteratively based on findings made while building Gold and the
# MAGIC dashboard (see `docs/data_quality_report.md`). Each rule is documented
# MAGIC here with its justification.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze = spark.table("nyc_taxi.bronze.trips_raw")
bronze_count = bronze.count()

# COMMAND ----------

# MAGIC %md ## Typing and derived columns

# COMMAND ----------

typed_df = (
    bronze
    # tpep_*_datetime arrives as timestamp_ntz (TLC source) — cast to a
    # regular timestamp before it can be subtracted as epoch seconds
    # (see ARCHITECTURE.md)
    .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
    .withColumn("pickup_date", F.to_date("pickup_ts"))
    .withColumn(
        "trip_duration_min",
        (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60,
    )
)

# COMMAND ----------

# MAGIC %md ## Quality rules (documented)
# MAGIC
# MAGIC | Rule | Justification |
# MAGIC |---|---|
# MAGIC | `fare_amount > 0` | Negative or zero fares are meter recording errors |
# MAGIC | `fare_amount < 500` | Max observed was $999 in Bronze — capture error, not a real fare |
# MAGIC | `trip_distance > 0` | Zero distance indicates an invalid or cancelled trip |
# MAGIC | `trip_distance < 100` | Buckets up to ~290 miles found in Bronze — impossible for an urban trip |
# MAGIC | `0 < trip_duration_min < 180` | Negative durations are timestamp errors; >3h is unusual for urban taxi |
# MAGIC | no exact duplicates | Avoids double-counting in downstream aggregations |

# COMMAND ----------

silver_df = (
    typed_df
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("fare_amount") < 500)   # see docs/data_quality_report.md #6: detected max_fare = 999
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("trip_distance") < 100)  # see docs/data_quality_report.md #5: distances up to ~290 miles
    .filter((F.col("trip_duration_min") > 0) & (F.col("trip_duration_min") < 180))
    .dropDuplicates()
)

silver_count = silver_df.count()

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("pickup_date")
    .saveAsTable("nyc_taxi.silver.trips_clean")
)

# COMMAND ----------

# MAGIC %md ## Rejected records table (traceability)
# MAGIC Records what was discarded and why, instead of simply dropping it.

# COMMAND ----------

rejected_df = (
    typed_df
    .withColumn(
        "rejection_reason",
        F.when(F.col("fare_amount") <= 0, "fare_amount <= 0")
        .when(F.col("fare_amount") >= 500, "fare_amount >= 500")
        .when(F.col("trip_distance") <= 0, "trip_distance <= 0")
        .when(F.col("trip_distance") >= 100, "trip_distance >= 100")
        .when(F.col("trip_duration_min") <= 0, "trip_duration_min <= 0")
        .when(F.col("trip_duration_min") >= 180, "trip_duration_min >= 180")
        .otherwise(None),
    )
    .filter(F.col("rejection_reason").isNotNull())
)

(
    rejected_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("nyc_taxi.silver.trips_rejected")
)

# COMMAND ----------

# MAGIC %md ## Quality summary

# COMMAND ----------

print(f"Bronze:   {bronze_count:,} rows")
print(f"Silver:   {silver_count:,} rows")
print(f"Rejected: {rejected_df.count():,} rows")
print(f"% retained: {silver_count / bronze_count * 100:.2f}%")

# COMMAND ----------

display(
    spark.table("nyc_taxi.silver.trips_rejected")
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)
