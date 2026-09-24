# Runbook

This is the exact sequence a reviewer (or you, at the defense) runs to see
the whole pipeline come alive, one stage at a time. Every step maps to
either an Airflow DAG or a `make` command - both do the same thing.

## Step 0 - Start the infrastructure (Stage 2)

| | |
|---|---|
| Command | `make env` then `make up` then `make health` |
| UI | none yet |
| Before | Nothing running |
| After | `make health` reports every service healthy; Grafana is reachable but empty; Kafka UI shows no topics |

## Step 1 - Start ingestion (Stage 3)

| | |
|---|---|
| Command | `make start-ingestion` (DAG `01_start_ingestion`) |
| UI | `localhost:8000/docs` (ingestor status) |
| Before | Ingestor status shows `connected: false`, idle |
| After | Status shows a live Jetstream connection, current host, a growing cursor and events/sec > 0; `source-db` row counts increase every 5 seconds |

## Step 2 - Start CDC into Kafka (Stage 4)

| | |
|---|---|
| Command | `make start-cdc` (DAG `02_start_cdc_kafka`) |
| UI | `localhost:8085` (Kafka UI), `localhost:8083/connectors` |
| Before | No topics in Kafka UI |
| After | One topic per source-db table appears; messages stream in live; opening a message shows `before` / `after` / `op` |

## Step 3 - Start Spark streaming (Stage 5)

| | |
|---|---|
| Command | `make start-spark` (DAG `03_start_spark_streaming`) |
| UI | `localhost:8080` (Spark Master), `localhost:4040` (Spark Streaming) |
| Before | No running job on Spark Master |
| After | Job listed as running; a new micro-batch appears every 5 seconds; warehouse `raw` and `realtime` schemas grow |

## Step 4 - Open Grafana real-time panels (Stage 7)

| | |
|---|---|
| Command | open `localhost:3000` (or `make status` for the same row counts in the terminal) |
| UI | Grafana, "Bluesky Real-Time Activity" dashboard, Row 1 |
| Before | Real-time panels are empty (no data yet if Steps 1-3 were skipped) |
| After | Stat panels, time series and tables refresh every 5 seconds; end-to-end latency stays under 10 seconds |

## Step 5 - Run dbt (Stage 6)

| | |
|---|---|
| Command | `make run-dbt` (DAG `04_dbt_transform`) |
| UI | `localhost:8088` (dbt docs), Grafana Row 2 |
| Before | Analytics panels (segments, trends, anomalous accounts) are empty |
| After | dbt snapshot/run/test complete successfully; Grafana Row 2 fills in with user segments, trending hashtags and anomalous accounts |

## Step 6 - Stop ingestion (Stage 3)

| | |
|---|---|
| Command | `make stop-ingestion` (DAG `05_stop_ingestion`) |
| UI | Grafana Row 1 (events per second graph) |
| Before | Graph is moving |
| After | Graph flattens to zero; an Airflow DAG-run annotation marks the stop time on the graph |

## Step 7 - Resume ingestion (Stage 3/8)

| | |
|---|---|
| Command | `make start-ingestion` again |
| UI | Same graph as Step 6 |
| Before | Graph is flat |
| After | Flow resumes; `source-db`, Kafka, `raw` and marts all keep growing with no data loss and no duplicate rows (verified via the cursor and unique keys - see Stage 8) |

## Resetting for a fresh demo

`make reset-demo` (DAG `99_reset_demo`) turns off every switch, removes the
Debezium connector and Kafka topics, and truncates all tables, so you can
run Steps 0-7 again from a clean state. Full behaviour lands in Stage 8.
