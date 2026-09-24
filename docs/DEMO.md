# Demo script

> Status: skeleton written in Stage 1, following the scenario in
> `docs/RUNBOOK.md`. The talking points and screenshots are finalized in
> Stage 7 (Grafana), once the dashboard exists and has real data to show.

## Before the demo: tabs to open

1. Terminal, in the project root, with `.env` already created.
2. `localhost:8000/docs` - Ingestor status
3. `localhost:8085` - Kafka UI
4. `localhost:8080` and `localhost:4040` - Spark Master / Streaming UI
5. `localhost:8081` - Airflow
6. `localhost:8088` - dbt docs
7. `localhost:3000` - Grafana, "Bluesky Real-Time Activity" dashboard

## Script

1. **Introduce the problem** (30s): "We want to understand how activity on
   Bluesky changes in real time - what's trending, and which accounts look
   like bots. Everything you'll see is built from the public, real Bluesky
   firehose."
2. **Show empty state** (Step 0 of the runbook): `make up`, `make health`.
   Point out Grafana is empty and Kafka has no topics - "nothing is
   flowing yet."
3. **Turn on ingestion** (Step 1): trigger DAG `01_start_ingestion` from
   Airflow. Switch to the ingestor status page and show real events
   arriving, cursor advancing.
4. **Turn on CDC** (Step 2): trigger DAG `02_start_cdc_kafka`. Switch to
   Kafka UI, show a topic filling up, open one message and point out
   `before` / `after` / `op`.
5. **Turn on Spark** (Step 3): trigger DAG `03_start_spark_streaming`.
   Switch to `localhost:4040`, show a batch completing every 5 seconds.
6. **Open Grafana** (Step 4): show Row 1 filling in live - events/sec,
   active users, top hashtags - refreshing every 5 seconds.
7. **Run dbt** (Step 5): trigger DAG `04_dbt_transform`. Switch to Row 2 of
   Grafana - user segments, trending hashtags by hour, anomalous accounts.
8. **Show resilience** (Steps 6-7): stop ingestion, show the graph flatten
   and the Airflow annotation appear; start it again and show it resume
   with no gap in the data and no duplicates.
9. **Wrap up** (30s): point at Row 3 (pipeline health) and mention the data
   quality checks that ran silently underneath the whole demo (Spark
   quarantine, dbt tests).

## Fallback if Jetstream is down during the demo

Switch `SOURCE_MODE=simulator` in `.env` and restart the ingestor - the
rest of the pipeline behaves identically, using synthetic events (see
`docs/PROJECT_PLAN.md` section 9, Risks & assumptions).
