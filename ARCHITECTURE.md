# Architecture

## Medallion diagram

```
┌─────────────────────┐      ┌──────────────────────┐      ┌───────────────────────┐
│       BRONZE          │      │        SILVER          │      │         GOLD           │
│                      │      │                        │      │                        │
│ TLC public parquet   │ ───► │ trips_clean            │ ───► │ trips_by_hour          │
│  → trips_raw          │      │ trips_rejected         │      │ demand_by_zone         │
│                      │      │                        │      │ fare_by_distance       │
│ Faithful copy +       │      │ Typed, validated,      │      │ outliers               │
│ ingestion metadata     │      │ partitioned by date     │      │ Business metrics,      │
│ No business            │      │ Documented quality      │      │ ready for BI           │
│ transformations         │      │ rules                   │      │                        │
└─────────────────────┘      └──────────────────────┘      └───────────────────────┘
```

## Catalog and schemas

A dedicated catalog (`nyc_taxi`) is used instead of writing on top of
`samples` (which is read-only and shared across the whole workspace):

```sql
CREATE CATALOG IF NOT EXISTS nyc_taxi;
CREATE SCHEMA IF NOT EXISTS nyc_taxi.bronze;
CREATE SCHEMA IF NOT EXISTS nyc_taxi.silver;
CREATE SCHEMA IF NOT EXISTS nyc_taxi.gold;
```

## Data source: why we moved from `samples.nyctaxi.trips` to the public TLC data

The initial plan used the sample dataset preloaded in Databricks
(`samples.nyctaxi.trips`), for zero-friction setup. Initial profiling
(notebook `00_profiling.py`) revealed that dataset only had **21,932
rows** — not enough to justify partitioning and performance decisions in
a technical interview.

**Decision:** move to the NYC TLC's public Parquet files
(`https://d37ci6vzurychx.cloudfront.net/trip-data/`), downloading 3
months of Yellow Taxi data (January-March 2024) directly with `wget` into
a **Unity Catalog Volume**, resulting in **9,554,778 rows** — a volume
that does require thinking about partitioning, write times, and
distributed read strategies.

This migration is itself part of the project's narrative: demonstrating
the ability to revisit an architecture decision based on evidence (the
actual profiling count) instead of assuming the sample dataset "would be
enough."

## Ingesting from an external public source (Volume + wget pattern)

Unlike reading an existing catalog table, ingesting a public file requires
a "landing zone" pattern:

```
Public URL (TLC)
   │  wget -O fixed_name.parquet  (idempotent: overwrites, never duplicates)
   ▼
/Volumes/nyc_taxi/bronze/landing_zone/   (Unity Catalog Volume)
   │  spark.read.parquet("*.parquet")    (wildcard, reads all 3 files as one)
   ▼
nyc_taxi.bronze.trips_raw                (governed Delta table)
```

**Why a Volume and not `/tmp`:** a Unity Catalog Volume is managed storage,
accessible by every node in the cluster — `/tmp` is only visible to the
driver node, and Spark wouldn't be able to distribute the read across
workers.

**Why `wget -O fixed_name` instead of plain `wget`:** it makes the
download idempotent. Without the `-O` flag, running the cell twice
generates duplicate files with a suffix (`file.parquet.1`), breaking the
guarantee that the pipeline produces the same result on every run.

**Source traceability:** every Bronze row includes `_metadata.file_path`
(via `F.col("_metadata.file_path")` — not `input_file_name()`, which
Unity Catalog blocks for governance reasons), allowing any record to be
traced back to the file/month it came from.

## Enrichment with a reference (dimension) table

`demand_by_zone` is enriched with a `LEFT JOIN` against
`taxi_zone_lookup.csv` (also public, from the TLC), translating
`PULocationID` (a numeric key) into `Zone`/`Borough` (human-readable
name). A `LEFT JOIN` is used deliberately — not `INNER` — so trips whose
zone ID has no match in the reference table are not silently dropped.

## Decisions and justification

### Format: Delta Lake across all layers
- ACID transactions during concurrent writes.
- Time travel to audit or roll back changes.
- Controlled schema enforcement / evolution.

### Partitioning: `pickup_date` in Silver
- Business aggregations (trips by hour, demand by zone) are almost always
  computed over time windows.
- Partitioning by date enables partition pruning: Spark can skip
  partitions outside the queried range.
- High-cardinality columns (e.g. `pickup_zip`) are deliberately avoided
  as partition keys, to prevent the small-file problem.

### Write mode
- **Bronze / Silver / Gold**: `overwrite` in this project, since we're
  working with a single batch load of the sample dataset.
- In a production scenario with incremental data, Bronze would switch to
  `append` with Auto Loader, and Silver/Gold would use `MERGE INTO`
  (upsert) to process only new data.

### Data quality traceability
- Every row dropped in Silver is preserved in `trips_rejected` with its
  `rejection_reason`, instead of being silently discarded.
- This allows auditing the pipeline and adjusting business rules without
  losing visibility into what's being discarded and why.

### Outlier detection
- IQR (interquartile range) method on `fare_amount`, computed with
  `approxQuantile` (approximate, suitable for large datasets without the
  cost of an exact percentile calculation).

## Orchestration

The pipeline runs as a **Databricks Workflows Job** with 3 tasks chained
by explicit dependency:

```
bronze_ingest ──► silver_clean ──► gold_aggregate
   1m14s              49s              48s        = 2m53s total (measured)
```

- **Compute: Serverless** — Databricks manages compute automatically
  without defining machine size or type; conceptually equivalent to a
  dedicated job cluster (spins up and down with the job), but without
  manual configuration.
- **`Depends on` + `Run if dependencies: All succeeded`** on each task,
  guaranteeing sequential execution — each task shows as "Blocked" until
  the previous one succeeds.
- Email notification configured on failure of any task.
- **Post-run validation**: an additional notebook
  (`04_validate_pipeline.py`) compares the actual row count of all 7
  tables against the documented expected values, confirming that the
  orchestrated run produces exactly the same results as the manual,
  cell-by-cell run validated during development.
