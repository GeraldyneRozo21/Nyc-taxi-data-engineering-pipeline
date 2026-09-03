# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — Business aggregations
# MAGIC Tables ready for BI / dashboard consumption.

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("nyc_taxi.silver.trips_clean")

# COMMAND ----------

# MAGIC %md ## 1. Trips by hour of day

# COMMAND ----------

trips_by_hour = (
    silver
    .withColumn("pickup_hour", F.hour("pickup_ts"))
    .groupBy("pickup_hour")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    )
    .orderBy("pickup_hour")
)

trips_by_hour.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("nyc_taxi.gold.trips_by_hour")

display(trips_by_hour)

# COMMAND ----------

# MAGIC %md ## 2. Zones with the most demand (enriched with zone name)
# MAGIC The TLC source uses `PULocationID`/`DOLocationID` (taxi zone IDs), not
# MAGIC zip codes. Enriched via a LEFT JOIN against the TLC's official
# MAGIC reference table to show zone name and borough.

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /Volumes/nyc_taxi/bronze/landing_zone/reference
# MAGIC cd /Volumes/nyc_taxi/bronze/landing_zone/reference
# MAGIC wget -q -O taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
# MAGIC ls -lh /Volumes/nyc_taxi/bronze/landing_zone/reference

# COMMAND ----------

zone_lookup = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/nyc_taxi/bronze/landing_zone/reference/taxi_zone_lookup.csv")
)

# COMMAND ----------

demand_by_zone = (
    silver
    .groupBy("PULocationID")
    .agg(F.count("*").alias("total_trips"))
    .join(zone_lookup, silver["PULocationID"] == zone_lookup["LocationID"], "left")
    .select("PULocationID", "Zone", "Borough", "total_trips")
    .orderBy(F.desc("total_trips"))
)

demand_by_zone.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("nyc_taxi.gold.demand_by_zone")

display(demand_by_zone.limit(20))

# COMMAND ----------

# MAGIC %md ## 3. Average fare by distance (bucketed)

# COMMAND ----------

fare_by_distance = (
    silver
    .withColumn("distance_bucket", F.floor(F.col("trip_distance")))
    .groupBy("distance_bucket")
    .agg(
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.count("*").alias("total_trips"),
    )
    .orderBy("distance_bucket")
)

fare_by_distance.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("nyc_taxi.gold.fare_by_distance")

display(fare_by_distance)

# COMMAND ----------

# MAGIC %md ## 4. Outlier detection (IQR method)

# COMMAND ----------

q1, q3 = silver.approxQuantile("fare_amount", [0.25, 0.75], 0.01)
iqr = q3 - q1
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr

print(f"Q1={q1:.2f}  Q3={q3:.2f}  IQR={iqr:.2f}")
print(f"Bounds: [{lower_bound:.2f}, {upper_bound:.2f}]")

# COMMAND ----------

outliers = silver.filter(
    (F.col("fare_amount") < lower_bound) | (F.col("fare_amount") > upper_bound)
)

outliers.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("nyc_taxi.gold.outliers")

print(f"Outliers detected: {outliers.count():,} of {silver.count():,} trips")
display(outliers.select("pickup_ts", "trip_distance", "fare_amount").limit(20))
