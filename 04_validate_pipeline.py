# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Pipeline — Post-run row count verification
# MAGIC Compares the actual row count of each table against the expected
# MAGIC values, documented after manual cell-by-cell validation during
# MAGIC development. Designed to run after `gold_aggregate` (can be added as
# MAGIC a 4th task in the Job, dependent on `gold_aggregate`).

# COMMAND ----------

tables_to_check = {
    "nyc_taxi.bronze.trips_raw": 9_554_778,
    "nyc_taxi.silver.trips_clean": 9_204_707,
    "nyc_taxi.silver.trips_rejected": 350_070,
    "nyc_taxi.gold.trips_by_hour": 24,
    "nyc_taxi.gold.demand_by_zone": 261,
    "nyc_taxi.gold.fare_by_distance": 98,
    "nyc_taxi.gold.outliers": 889_006,
}

all_ok = True
for table, expected in tables_to_check.items():
    actual = spark.table(table).count()
    ok = actual == expected
    all_ok = all_ok and ok
    status = "✅" if ok else "❌"
    print(f"{status} {table}: expected={expected:,}  actual={actual:,}")

if not all_ok:
    raise ValueError("Validation failed: at least one table doesn't match the expected value.")

print("\nValidation complete: 7/7 tables verified correctly.")
