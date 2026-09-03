# Next steps: from batch to streaming

This project processes a static dataset (public TLC parquet files) in
batch mode. The natural extension to demonstrate further maturity as a
data engineer is to bring it into **streaming**.

## How this pipeline would scale to real-time data

1. **Ingestion**: replace the batch read into Bronze with
   [Auto Loader](https://docs.databricks.com/ingestion/auto-loader)
   (`cloudFiles`), which incrementally detects new files without
   reprocessing the full history.
2. **Processing**: convert the Silver/Gold notebooks to
   **Structured Streaming**, using `readStream` / `writeStream` with
   checkpointing to guarantee exactly-once processing.
3. **Aggregations**: the Gold metrics (trips by hour, demand by zone)
   would move to **time-window** calculations (`window()`), using
   watermarking to handle late-arriving data.
4. **Orchestration**: instead of a scheduled batch Job, the pipeline
   would run as a continuous streaming Job (`Trigger.AvailableNow` or
   `Trigger.Continuous`, depending on the use case).

## Proposed real-time source: TTC (Toronto) GTFS-RT

As a concrete next step, this pipeline would be extended using real-time
transit data from the **TTC (Toronto Transit Commission)**, which
publishes **GTFS-RT** feeds (vehicle positions, arrival updates, service
alerts):

- Structurally analogous to the taxi use case: timestamped events with
  geolocation, requiring time-window aggregation.
- Enables applying the exact same bronze/silver/gold pattern, but over a
  real stream instead of a static batch.
- A free, public data source — ideal for a portfolio project with no
  data-access friction.

## What this would add to the interview narrative

- Demonstrates handling of **incremental data**, not just full loads.
- Demonstrates knowledge of **watermarking and late data handling**, a
  topic that distinguishes a junior data engineer from one with real
  streaming experience.
- Connects the project to a geographically relevant use case (Toronto)
  if the portfolio is presented in the local job market.
