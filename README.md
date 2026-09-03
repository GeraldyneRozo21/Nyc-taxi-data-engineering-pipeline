# NYC Taxi Data Engineering Pipeline (Databricks + PySpark + Delta Lake)

End-to-end data pipeline over **9.5 million real trips** from NYC Yellow
Taxi (January-March 2024), built with a **medallion architecture
(bronze → silver → gold)**, orchestrated as a Databricks Workflows Job,
and visualized in a published SQL Dashboard.

## Project goal

Demonstrate, with a real case and production-scale data, the day-to-day
work of a data engineer on Databricks/Spark: ingestion from a public
source, data validation and cleaning backed by quantitative evidence,
business aggregations, production-grade orchestration, and visualization —
not an isolated exploratory notebook.

## Final result

| Layer | Table | Rows |
|---|---|---|
| Bronze | `nyc_taxi.bronze.trips_raw` | 9,554,778 |
| Silver | `nyc_taxi.silver.trips_clean` | 9,204,707 |
| Silver | `nyc_taxi.silver.trips_rejected` | 350,070 |
| Gold | `nyc_taxi.gold.trips_by_hour` | 24 |
| Gold | `nyc_taxi.gold.demand_by_zone` | 261 |
| Gold | `nyc_taxi.gold.fare_by_distance` | 98 |
| Gold | `nyc_taxi.gold.outliers` | 889,006 |

**Orchestrated pipeline:** `bronze_ingest → silver_clean → gold_aggregate`,
runs end-to-end in **~3 minutes** (2m53s measured), on Serverless compute,
with email notification on failure.

**Published dashboard:** 6 visualizations (3 KPIs + 3 charts) built on top
of the Gold tables — see the Dashboard section below.

## Data source

**NYC TLC (Taxi and Limousine Commission)** — official public data,
downloaded directly in Parquet format from
`https://d37ci6vzurychx.cloudfront.net/trip-data/`:

- `yellow_tripdata_2024-01.parquet`
- `yellow_tripdata_2024-02.parquet`
- `yellow_tripdata_2024-03.parquet`

Plus a reference table (`taxi_zone_lookup.csv`) used to translate
`PULocationID`/`DOLocationID` into human-readable zone names (e.g. "JFK
Airport", "Times Square").

This source was chosen over the sample dataset `samples.nyctaxi.trips`
(21,932 rows) because it didn't represent enough volume to justify
partitioning and performance decisions — see `ARCHITECTURE.md` for the
full detail behind that decision.

## Architecture

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full diagram and
technical decisions (partitioning, table format, Volumes, orchestration).

```
TLC (public parquet)
        │  wget → Volume (landing zone)
        ▼
  nyc_taxi.bronze.trips_raw        (9,554,778 rows)
        │  cleaning + validation
        ▼
  nyc_taxi.silver.trips_clean      (9,204,707 rows, partitioned by pickup_date)
  nyc_taxi.silver.trips_rejected   (350,070 rows, with documented reason)
        │  business aggregations
        ▼
  nyc_taxi.gold.trips_by_hour      (24 rows)
  nyc_taxi.gold.demand_by_zone     (261 rows, enriched with zone name)
  nyc_taxi.gold.fare_by_distance   (98 rows)
  nyc_taxi.gold.outliers           (889,006 rows)
```

## How to run it

1. Import the `notebooks/` folder into your Databricks workspace.
2. Run `01_bronze.py` — downloads the TLC parquet files into a Volume and
   writes the Bronze table (~1m14s).
3. Run `02_silver.py` — cleans and validates, writes `trips_clean` and
   `trips_rejected` (~49s).
4. Run `03_gold.py` — builds the 4 business tables, including the join
   with `taxi_zone_lookup.csv` (~48s).
5. (Recommended) Create a Workflows Job chaining the 3 tasks by
   dependency — see the Orchestration section below — to run everything
   with one click.
6. On top of the `gold` tables, build the dashboard following the
   Dashboard section.

## Cleaning rules applied (Silver)

Each rule was refined iteratively, backed by quantitative evidence on the
real 9.5M records — not defined arbitrarily up front. See the full detail
in [`docs/data_quality_report.md`](./docs/data_quality_report.md).

| Rule | Rows affected | Justification |
|---|---|---|
| `fare_amount <= 0` | 139,596 | Invalid fare |
| `fare_amount >= 500` | small (post-adjustment) | Detected a max of **$999** in Bronze — capture error, not a real fare |
| `trip_distance <= 0` | 215,764 | Invalid distance |
| `trip_distance >= 100` | 219 | Found while reviewing `fare_by_distance`: buckets up to ~290 miles, impossible for an urban taxi trip |
| `trip_duration_min <= 0` | 2,801 | Timestamp error |
| `trip_duration_min >= 180` | 6,103 | Unusual for an urban taxi ride (>3h) |
| `passenger_count` null/0 | — (kept) | Secondary field, doesn't invalidate the trip |
| `RatecodeID = 99` | — (kept) | "Unknown" per the TLC data dictionary, doesn't invalidate the trip |

**Final retention: 96.34%** (9,204,707 of 9,554,778 rows).

## Orchestration (Databricks Workflows)

Job `nyc_taxi_pipeline`, 3 tasks chained by dependency (`Depends on`,
`Run if dependencies: All succeeded`), Serverless compute:

```
bronze_ingest → silver_clean → gold_aggregate
   1m14s          49s              48s        = 2m53s total
```

Validated with a verification notebook (`04_validate_pipeline.py`, see
`notebooks/`) that compares the actual row count of each table against
the expected values after each run — 7/7 tables verified with no
discrepancy.

Email notification configured on failure of any task.

## Dashboard

Published with Databricks SQL Dashboards:
`NYC Taxi - Trip Analysis 2024 Q1`

**6 visualizations:**
1. KPI — Total Outliers Detected (889,006)
2. KPI — Average Fare, outliers ($61.09)
3. KPI — Total Revenue, outliers ($54.31M)
4. Bar chart — Trips by hour of day
5. Bar chart — Top 10 zones by volume (colored by Borough)
6. Line chart — Average fare by distance, with a data quality note

## Business findings

- **Peak hour:** 6:00 PM, with 666,563 trips — aligns with typical
  end-of-workday commute.
- **High fares in the early morning:** at 4-5 AM, volume is the lowest of
  the day, but average fare is the highest (~$23-27) — suggesting long
  airport trips during early flight hours.
- **Flat rate to JFK:** the fare-distance relationship "flattens" around
  $70 between 16 and 21 miles (the Manhattan-JFK distance range), with a
  sharp volume spike right in that stretch — evidence of NYC's official
  flat rate to JFK Airport, detected through data analysis without
  consulting the fare policy beforehand.
- **JFK Airport** is the 3rd zone by pickup demand (408,109 trips) and one
  of only two zones outside Manhattan in the top 20 (along with
  LaGuardia).
- **Outliers aren't necessarily errors:** 9.66% of trips fall outside the
  IQR fare range, but a large share corresponds to legitimate long trips
  (airports), not data errors.

## Next steps

See [`next_steps.md`](./next_steps.md) — extension to streaming with
GTFS-RT from the TTC (Toronto) as a real-time analogous source.
