.PHONY: help env venv up up-infra up-stream up-orchestration up-serving down restart clean         ps health status logs urls tunnel start-ingestion start-cdc start-spark run-dbt         stop-ingestion reset-demo test lint

.DEFAULT_GOAL := help

help: ## Show this list of commands
	@grep -E '^[a-zA-Z0-9_-]+:.*##' Makefile | sort | sed -E 's/:[^#]*## / - /'

# ---- Stage 1 (Skeleton & docs): usable today, no infra required ----

env: ## Create .env from .env.example (never overwrites an existing .env)
	@if [ -f .env ]; then 		echo ".env already exists - not overwriting"; 	else 		cp .env.example .env; 		echo ".env created from .env.example - edit it before running 'make up'"; 	fi

venv: ## Create a local Python virtual environment with all dependencies
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements/dev.txt
	@echo "Run 'source .venv/bin/activate' to use it."

test: ## Run the unit test suite
	python3 -m pytest tests/

lint: ## Run the linter
	python3 -m ruff check .

urls: ## Print all UI links
	@echo "Ingestor status  : http://localhost:8000/docs"
	@echo "Simulator        : http://localhost:8001/docs"
	@echo "Kafka UI         : http://localhost:8085"
	@echo "Kafka Connect    : http://localhost:8083/connectors"
	@echo "Spark Master     : http://localhost:8080"
	@echo "Spark Streaming  : http://localhost:4040"
	@echo "Airflow          : http://localhost:8081"
	@echo "dbt docs         : http://localhost:8088"
	@echo "Grafana          : http://localhost:3000"

tunnel: ## Print the SSH tunnel command with all ports
	@echo 'ssh -N -L 8000:localhost:8000 -L 8001:localhost:8001 -L 8085:localhost:8085 \'
	@echo '    -L 8083:localhost:8083 -L 8080:localhost:8080 -L 4040:localhost:4040 \'
	@echo '    -L 8081:localhost:8081 -L 8088:localhost:8088 -L 3000:localhost:3000 \'
	@echo '    -L 5432:localhost:5432 -L 5433:localhost:5433 <user>@<remote-host>'

# ---- Stage 2 (Infrastructure) ----

up: ## Build and start every service
	docker compose up -d --build

up-infra: ## Databases and Kafka only
	docker compose up -d --build source-db warehouse-db airflow-metadata-db kafka kafka-init kafka-ui

up-stream: ## Kafka Connect, ingestor, simulator, Spark cluster
	docker compose up -d --build kafka-connect ingestor simulator spark-master spark-worker spark-streaming

up-orchestration: ## Airflow
	docker compose up -d --build airflow-init airflow-webserver airflow-scheduler

up-serving: ## Grafana and dbt-docs
	docker compose up -d --build grafana dbt-docs

down: ## Stop everything, keep data (named volumes are untouched)
	docker compose down

restart: ## Stop and start again, keep data
	docker compose restart

clean: ## Stop everything AND delete all volumes (data loss)
	docker compose down -v

ps: ## Service status
	docker compose ps

health: ## Run check_health.sh
	@bash scripts/check_health.sh

status: ## Run pipeline_status.sh (row counts per layer every 5s)
	@bash scripts/pipeline_status.sh

logs: ## Show logs for one service, e.g. make logs service=ingestor
	docker compose logs -f $(service)

# ---- Stage 3-6: Airflow DAG equivalents ----

start-ingestion: ## Equivalent of DAG 01_start_ingestion
	docker compose exec airflow-scheduler airflow dags trigger 01_start_ingestion

start-cdc: ## Equivalent of DAG 02_start_cdc_kafka
	docker compose exec airflow-scheduler airflow dags trigger 02_start_cdc_kafka

start-spark: ## Equivalent of DAG 03_start_spark_streaming
	docker compose exec airflow-scheduler airflow dags trigger 03_start_spark_streaming

run-dbt: ## Equivalent of DAG 04_dbt_transform
	docker compose exec airflow-scheduler airflow dags trigger 04_dbt_transform

stop-ingestion: ## Equivalent of DAG 05_stop_ingestion
	docker compose exec airflow-scheduler airflow dags trigger 05_stop_ingestion

reset-demo: ## Run reset_demo.sh
	@bash scripts/reset_demo.sh
