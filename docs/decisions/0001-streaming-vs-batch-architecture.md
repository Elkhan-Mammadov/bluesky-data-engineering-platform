# ADR 0001: Core pipeline is continuous streaming, not logical-date-partitioned batch

## Status

Accepted (2026-09-29)

## Context

The platform's original design brief (`CLAUDE.md`, written before this ADR) specifies a
**continuous, real-time pipeline**: Bluesky Jetstream -> ingestor -> source-db -> Debezium ->
Kafka -> Spark (5-second micro-batches) -> warehouse-db -> dbt -> Grafana, with an explicit
end-to-end latency target of 5-10 seconds. Airflow deliberately does not sit on this data
path - it only flips control switches (`control.pipeline_switches`) and verifies results
(DAGs `01`-`06`, `99`). None of these DAGs are parameterized by a "logical date": there is no
notion of "run the pipeline for 2026-09-25 vs 2026-09-26" in the streaming path, because data
for every day flows through the exact same tables continuously.

A separate, later requirement ("Phase 1 - Walking Skeleton") asks for a pipeline that is:
- parameterized by a logical date or partition, re-runnable for a specific day,
- idempotent per partition (delete-then-insert / overwrite / merge on a key),
- runnable for at least two different logical dates without one overwriting the other.

These two requirements are in genuine tension: retrofitting logical-date partitioning onto the
streaming path would mean either (a) reprocessing all of `raw` on every "day" boundary, which
contradicts the 5-10 second latency goal and the already-idempotent upsert/cursor design, or
(b) faking a date parameter that the streaming tables don't actually use, which would be
dishonest about what the system does.

## Decision

Keep the core streaming pipeline (DAGs `01`-`06`, `99`) exactly as designed - continuous,
switch-controlled, not date-partitioned. Its own idempotency guarantee is different from (but
equally real as) partition-overwrite: every write is a upsert or delete keyed by a natural key
(`user_id_hash` + `record_key`, etc.), and Spark's checkpoint (`spark_checkpoints` volume)
guarantees Kafka offsets are never reprocessed from scratch after a restart. This was
demonstrated live: `docker compose down` + `up` preserves all data and the pipeline resumes
without duplication (see `docs/RUNBOOK.md`).

Add one new, genuinely batch DAG - `07_daily_batch_report` - specifically to satisfy the
logical-date requirement, kept deliberately separate from the streaming DAGs:

- Runs daily, parameterized by Airflow's `{{ ds }}` (logical date).
- Reads already-aggregated data from `marts.fct_user_daily_activity` (which IS naturally
  day-grained) for that one date.
- Writes one row to `marts.mart_daily_summary`, keyed by `report_date`, using
  delete-then-insert: re-running the same date never duplicates; running two different dates
  never overwrites each other.
- Can be triggered for any historical date via
  `airflow dags trigger 07_daily_batch_report --exec-date <date>`.

## Consequences

- The platform now has two, clearly distinct orchestration patterns living side by side:
  switch-based streaming control (DAGs `01`-`06`, `99`) and logical-date batch aggregation
  (DAG `07`). This is intentional and documented here, not an inconsistency.
- `docs/evidence/phase-1.md` demonstrates DAG `07` run twice for the same date (no duplicate
  row) and once for a second date (both rows present).
- If a future requirement needs the *entire* warehouse to be date-partitioned, that would be a
  larger architectural change (effectively moving away from continuous streaming) and would
  warrant its own ADR - it is out of scope here.

## Update (2026-09-30): DAG 07 became the full end-to-end pipeline

A check of the Phase 1 rubric showed that the version of DAG `07` above was
too thin. It read `marts` and wrote `marts`, so it touched only one hop.
The streaming path also needed DAGs `01`-`04` triggered by hand in
sequence, which a reviewer could fairly read as manual steps between
stages. The rubric requires one pipeline, parameterized by logical date,
with a separate task for each stage.

DAG `07_daily_batch_report` now carries one logical date through every
component, with explicit dependencies:
`start_ingestion -> start_cdc -> start_spark -> check_source_partition ->
load_raw_partition -> dbt_run_before_snapshot -> dbt_snapshot ->
dbt_run_dim_user -> dbt_test -> check_partition_quality -> publish_daily_summary`.

The core decision above still stands. The data path stays continuous and
is not reprocessed per date. The component-starting tasks are idempotent,
and on a stack that is already running they are no-ops. The per-date
tasks prove that the date's partition reached source-db and then `raw.*`,
with counts reconciled against a source snapshot. They then rebuild
staging and marts, check the partition, and publish it. The rest is
unchanged: `raw.*` is upserted on natural keys, dbt rebuilds its models,
and the summary uses delete-then-insert. A re-run therefore never
duplicates, and two dates never overwrite each other.

Also changed alongside:
- The deliberate-failure switch is read from the run's conf. An
  environment variable passed to `airflow dags trigger` never reaches the
  scheduler process that runs the task. The forced failure raises
  `AirflowFailException`, so it does not retry.
- The dbt tasks of DAGs `04` and `07` share a one-slot `dbt` pool. Two
  concurrent `dbt run`s swapping the same tables fail.

