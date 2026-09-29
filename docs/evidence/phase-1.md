# Phase 1 evidence

This document collects the proof Phase 1's Definition of Done asks for:
a successful orchestrator run, a deliberately-failed run, and row counts
per layer from the smoke test.

> **Status: partially complete.** The row counts below are real, captured
> from the live server during verification on 2026-09-26/27. The two
> screenshots still need to be taken by hand (I - the assistant - have no
> way to capture a browser screenshot) and pasted into the two marked
> sections below before this is submission-ready.

## 1. Successful orchestrator run

**TODO (needs a screenshot):** Open `localhost:8081`, click into a
successful run of `01_start_ingestion` (or `07_daily_batch_report`),
screenshot the green/success task graph, and paste it here.

Textual proof in the meantime (from `airflow dags list-runs`, captured
live on the server):

```
dag_id             | run_id                         | state   | execution_date            | start_date                      | end_date
02_start_cdc_kafka | manual__2026-09-26T13:43:39+00 | success | 2026-09-26T13:43:39+00:00 | 2026-09-26T13:43:40.679410+00: | 2026-09-26T13:43:42.724676+00:0
```

## 2. Deliberately-failed run

**TODO (needs a screenshot):** Trigger the failure path, then screenshot
the red/failed task graph in the Airflow UI:

```bash
docker compose exec -e FORCE_INGESTION_FAILURE=true airflow-scheduler \
    airflow dags trigger 01_start_ingestion
```

Expected result: task `enable_ingestion` shows `failed` (raises
`RuntimeError: Deliberate failure: FORCE_INGESTION_FAILURE=true is set...`),
task `check_source_db_growth` shows `upstream_failed`, and the DAG run
itself is marked `failed`. Confirm from the CLI first if you like:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run 01_start_ingestion <run_id>
```

## 3. Row counts per layer (from `make smoke`)

Captured live on the real server (`204.168.173.51`) on 2026-09-27, after
DAGs 01-04 had been running against real Bluesky Jetstream data:

| Layer | Table | Row count |
|---|---|---|
| raw | `raw.posts` | 4,474 |
| curated (marts) | `marts.dim_user` (current rows) | 59,589 (36,663 lurker + 19,099 engager + 3,827 creator) |
| curated (marts) | `marts.mart_trending_hashtags` (top rows) | "news" x340, "ai" x281, "bluesky" x239, "africa" x227 |
| pipeline health | `dq.stream_batches` | batches every ~5-6s, `latency_ms` ~4,400-4,950 |

These numbers predate DAG `07_daily_batch_report` and the smoke test
itself (both added after this verification session). **Next step:** run

```bash
make start-ingestion   # if not already running
make start-cdc
make start-spark
make run-dbt
docker compose exec airflow-scheduler airflow dags trigger 07_daily_batch_report
make smoke
```

and paste the real `make smoke` output here, replacing this paragraph -
it will print `raw.posts`, `staging.stg_posts`, `marts.fct_posts` and the
`marts.mart_daily_summary` row count for today's logical date directly.

## 4. Kafka connectivity test

```bash
make kafka-test
```

Expected output ends with:
```
[kafka_connectivity_test] PASS - message round-tripped through Kafka successfully
```

**TODO:** paste the real output here once run.
