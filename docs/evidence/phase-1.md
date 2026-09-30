# Phase 1 evidence

This document collects the proof Phase 1's Definition of Done asks for:
a successful orchestrator run, a deliberately-failed run, the batch slice
run for several logical dates, row counts per layer from the smoke test,
and a Kafka connectivity test.

All output below was captured live on the real server (`204.168.173.51`)
on 2026-09-30, against real Bluesky Jetstream data. The orchestrator
evidence is Airflow's own CLI output, read from its metadata database -
the same records the Airflow UI (`localhost:8081`) renders as the
green/red task grid.

## 1. Successful orchestrator runs, several logical dates

`07_daily_batch_report` is parameterized by logical date and writes its
partition with delete-then-insert (see
`docs/decisions/0001-streaming-vs-batch-architecture.md`). It has
succeeded for four different logical dates, both triggered manually
(`airflow dags trigger 07_daily_batch_report -e <date>`) and by its own
daily schedule:

```
$ docker compose exec airflow-scheduler airflow dags list-runs -d 07_daily_batch_report
dag_id               | run_id              | state   | execution_date      | start_date           | end_date
=====================+=====================+=========+=====================+======================+=====================
07_daily_batch_repor | manual__2026-09-30T | success | 2026-09-30T00:00:00 | 2026-09-30T18:03:24. | 2026-09-30T18:03:26.
t                    | 00:00:00+00:00      |         | +00:00              | 400989+00:00         | 636200+00:00
07_daily_batch_repor | manual__2026-09-29T | success | 2026-09-29T18:03:41 | 2026-09-29T18:03:41. | 2026-09-29T18:03:44.
t                    | 18:03:41+00:00      |         | +00:00              | 884015+00:00         | 686310+00:00
07_daily_batch_repor | scheduled__2026-09- | success | 2026-09-29T00:00:00 | 2026-09-30T00:00:00. | 2026-09-30T00:00:03.
t                    | 29T00:00:00+00:00   |         | +00:00              | 634799+00:00         | 060690+00:00
07_daily_batch_repor | scheduled__2026-09- | success | 2026-09-28T00:00:00 | 2026-09-29T18:09:02. | 2026-09-29T18:09:05.
t                    | 28T00:00:00+00:00   |         | +00:00              | 562876+00:00         | 628200+00:00
07_daily_batch_repor | manual__2026-09-20T | success | 2026-09-20T00:00:00 | 2026-09-29T18:03:45. | 2026-09-29T18:03:47.
t                    | 00:00:00+00:00      |         | +00:00              | 734103+00:00         | 968966+00:00
```

Logical dates covered: 2026-09-20, 2026-09-28, 2026-09-29, 2026-09-30.
Triggering an already-scheduled date again is rejected by Airflow with
`DagRunAlreadyExists` - one run per logical date, never a silent
duplicate.

## 2. Deliberately-failed run

The failure path is switched on per run with an environment variable:

```bash
docker compose exec -e FORCE_INGESTION_FAILURE=true airflow-scheduler \
    airflow dags test 01_start_ingestion
```

`enable_ingestion` raises `AirflowFailException`, which fails the task
immediately (no retries), and Airflow propagates the failure downstream:

```
... {taskinstance.py:1206} INFO - Immediate failure requested. Marking task as FAILED. dag_id=01_start_ingestion, task_id=enable_ingestion, ...
... {dagrun.py:819} ERROR - Marking run <DagRun 01_start_ingestion @ 2026-09-30 18:46:16.110365+00:00: ...> failed
... {dagrun.py:901} INFO - DagRun Finished: dag_id=01_start_ingestion, ..., run_duration=0.462657, state=failed, ...
DagRun failed
```

Per-task states of that run:

```
$ docker compose exec airflow-scheduler airflow tasks states-for-dag-run 01_start_ingestion "manual__2026-09-30T18:46:16.110365+00:00"
dag_id             | execution_date     | task_id            | state           | start_date         | end_date
===================+====================+====================+=================+====================+===================
01_start_ingestion | 2026-09-30T18:46:1 | enable_ingestion   | failed          |                    | 2026-09-30T18:46:1
                   | 6.110365+00:00     |                    |                 |                    | 6.553682+00:00
01_start_ingestion | 2026-09-30T18:46:1 | check_source_db_gr | upstream_failed | 2026-09-30T18:46:1 | 2026-09-30T18:46:1
                   | 6.110365+00:00     | owth               |                 | 6.564869+00:00     | 6.564869+00:00
```

Two lessons from getting this demo right, both fixed in the code:

- `airflow dags trigger` with `exec -e ...` does **not** work: trigger
  only queues the run, and the scheduler then executes the task in its
  own process, which never sees the variable. `dags test` runs the tasks
  in the CLI's own process.
- With the DAG's normal `retries: 2`, the first attempt failed, but the
  retry 10 minutes later was picked up by the real scheduler (without
  the variable), succeeded, and the run ended `success` - retries masked
  the failure. The forced failure now uses `AirflowFailException`, which
  skips retries.

## 3. Row counts per layer (`make smoke`)

```
$ make smoke
tests/smoke/test_pipeline_smoke.py::test_raw_layer_has_data PASSED                                               [ 20%]
tests/smoke/test_pipeline_smoke.py::test_staging_layer_has_data PASSED                                           [ 40%]
tests/smoke/test_pipeline_smoke.py::test_curated_layer_has_data_for_logical_date PASSED                          [ 60%]
tests/smoke/test_pipeline_smoke.py::test_serving_layer_is_queryable PASSED                                       [ 80%]
tests/smoke/test_pipeline_smoke.py::test_row_counts_reconcile_and_orchestrator_run_succeeded PASSED              [100%]

=================================================== 5 passed in 0.44s ===================================================
```

Row counts from a run a few minutes earlier the same day:

| Layer | Table | Row count |
|---|---|---|
| raw | `raw.posts` | 1,044,954 |
| staging | `staging.stg_posts` (view over raw) | 1,044,954 |
| curated | `marts.fct_posts` | 1,042,384 |
| curated | `marts.mart_daily_summary` for today's logical date | 1 row |

`staging` equals `raw` exactly because it is an unfiltered view.
`marts.fct_posts` is a table refreshed only when dbt runs (DAG 04), while
ingestion keeps writing to `raw` in between, so it lags `raw` by the rows
that arrived since the last dbt run (2,570 here). The smoke test asserts
`marts <= raw`, not equality. (That earlier run was the one that exposed
the old equality assertion as wrong on a live stream.) `make smoke` now
runs pytest with `-s`, so these counts print on every run, including
passing ones.

## 4. Kafka connectivity test (`make kafka-test`)

```
$ make kafka-test
[kafka_connectivity_test] creating throwaway topic 'connectivity-test-1790792993'...
Created topic connectivity-test-1790792993.
[kafka_connectivity_test] producing message: ping-1790792993
[kafka_connectivity_test] consuming it back...
[kafka_connectivity_test] received: ping-1790792993
[kafka_connectivity_test] PASS - message round-tripped through Kafka successfully
```

Each run uses a fresh topic and deletes it on exit. An earlier version
reused one fixed topic, and `--from-beginning` then read a stale ping
left over from a previous run.
