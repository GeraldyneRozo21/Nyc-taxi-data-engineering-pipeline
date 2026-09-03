# Data Quality Report — NYC Taxi Trips

Source: NYC TLC, Yellow Taxi, January-March 2024 (3 public parquet files,
~9.5M trips). This report documents the actual profiling process,
iterative cleaning, and the decisions made along with their
justification.

## 1. Source dataset profile (Bronze)

| Metric | Value |
|---|---|
| Source files | 3x `yellow_tripdata_2024-0{1,2,3}.parquet` (TLC) |
| Total rows | 9,554,778 |
| Total columns | 20 (+ 3 traceability metadata columns) |
| Date range | January - March 2024 |
| Size on disk | 153 MB (compressed parquet) |

## 2. Nulls per column

No nulls in business-critical columns (`fare_amount`, `trip_distance`,
`tpep_pickup_datetime`, `tpep_dropoff_datetime`). Nulls were found in
`passenger_count` (~7.87% of rows) — see decision in section 7.

## 3. Anomalies detected (before cleaning, on Bronze)

| Anomaly type | Count | % of total |
|---|---|---|
| `fare_amount <= 0` | 139,596 | 1.46% |
| `trip_distance <= 0` | 215,764 | 2.26% |
| `passenger_count = 0` | 105,931 | 1.11% |
| `passenger_count` null | 751,962 | 7.87% |
| `RatecodeID = 99` (unknown) | 96,699 | 1.01% |
| duration ≤ 0 | 2,801 | 0.03% |
| duration > 3h | 6,103 | 0.06% |

## 4. Cleaning iteration 1 — initial rules

First rule set applied in Silver:
- `fare_amount > 0`
- `trip_distance > 0`
- `0 < trip_duration_min < 180`

Result: 9,204,926 rows retained (96.34%).

## 5. Finding while building Gold: unrealistic distances

While building `gold.fare_by_distance`, the resulting table had **291
buckets** for distance — far more than expected for an urban taxi
dataset. Investigation:

```sql
SELECT COUNT(*) FROM silver.trips_clean WHERE trip_distance > 50
```

Confirmed the existence of trips with distances up to ~290 miles,
technically impossible for an urban taxi trip in NYC (Manhattan is only
~13 miles long). Likely cause: GPS or data entry errors, not caught by
the original rule (which only excluded `<= 0`, with no upper bound).

**Correction applied:** `trip_distance < 100` (a generous threshold,
covering any real trip within and around NYC, including farther
airports).

**Impact:** 219 additional rows discarded. `fare_by_distance` went from
291 to 98 buckets after the adjustment.

## 6. Finding while building the Dashboard: unrealistic fares

While charting `avg_fare` vs. `distance_bucket` in the SQL Dashboard, the
line showed erratic spikes and a scale up to 600, inconsistent with what
had previously been observed in PySpark (max ~$98). Investigation:

```sql
SELECT MAX(fare_amount), MIN(fare_amount) FROM silver.trips_clean
```

Result: `max_fare = 999`, `min_fare = 0.01` — both values suspicious (999
is a typical round number for a capture error or an artificial system
cap; 0.01 doesn't correspond to a real billed trip).

**Correction applied:** `fare_amount < 500` (a generous threshold
compared to real high fares in NYC, ~$300-400 for long trips in heavy
traffic).

**Impact:** after the adjustment, the real `max_fare` settled at $498.60.
The visual noise in distance buckets >60 miles decreased, but didn't
disappear completely — see section 8 for why that remaining noise is
**not** a data quality problem.

**Note:** `min_fare = 0.01` was not corrected in the final iteration of
the project — it doesn't visibly affect any of the 4 current Gold tables
(it's an extreme, not a value with meaningful weight in the averages). It
is documented here as a cleanup candidate for a v2, applying
`fare_amount > 1` instead of `> 0`.

## 7. Decisions on secondary columns (not filtered)

| Column | Situation | Decision | Justification |
|---|---|---|---|
| `passenger_count` | 7.87% null, 1.11% zero | **Keep**, don't use as an exclusion filter | Secondary field; discarding 8% of the dataset over a non-critical field would be over-cleaning |
| `RatecodeID` | 1.01% = 99 (unknown) | **Keep**, flag as incomplete data | Doesn't invalidate the rest of the record (fare, distance, duration remain valid) |

## 8. Final cleaning rules (final version)

| Rule | Action |
|---|---|
| `fare_amount > 0` | Discard if not met |
| `fare_amount < 500` | Discard if not met |
| `trip_distance > 0` | Discard if not met |
| `trip_distance < 100` | Discard if not met |
| `0 < trip_duration_min < 180` | Discard if not met |
| exact duplicates | Discard |
| `passenger_count` null/0 | Keep |
| `RatecodeID = 99` | Keep |

## 9. Final cleaning result (Silver)

| Metric | Value |
|---|---|
| Rows in Bronze | 9,554,778 |
| Rows in Silver (retained) | 9,204,707 |
| Rows rejected | 350,070 |
| % retained | 96.34% |

## 10. Note on variability in high-distance buckets (>60 miles)

After both corrections (distance and fare), some visual variability
remains in `avg_fare` for buckets above ~60 miles. Verified that this is
**not** a data quality issue: these buckets have very small sample sizes
(1 to 32 trips), so one or two high (but valid, e.g. $350-490 for a real
85+ mile trip) values naturally move the average. 99%+ of trips are
concentrated between 0 and 50 miles, a range where the fare-distance
pattern is stable and reliable — including evidence of a flat rate to
JFK Airport (~$70, buckets 16-21 miles).

Decision: the full chart is kept in the dashboard, with an explanatory
note visible next to the visualization, instead of truncating the
range — for the sake of data transparency.

## 11. Outliers in Gold (IQR method on `fare_amount`, post-cleaning)

| Metric | Value |
|---|---|
| Q1 | $9.30 |
| Q3 | $21.20 |
| IQR | $11.90 |
| Lower bound | -$8.55 (not applicable in practice, `fare_amount > 0` already guaranteed) |
| Upper bound | $39.05 |
| Outliers detected | 889,006 of 9,204,707 (9.66%) |
| Average fare of outliers | $61.09 |
| Total revenue from outliers | ~$54,310,000 |

**Interpretation:** a high percentage of "outliers" (9.66%) doesn't imply
an equally high proportion of data errors. Most correspond to a
legitimate business segment — long trips, particularly to airports with
a flat fare — not properly captured by a single statistical threshold
applied over the entire dataset without segmenting by trip type.
