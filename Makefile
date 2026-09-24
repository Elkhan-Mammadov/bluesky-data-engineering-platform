.PHONY: env venv up up-infra up-stream up-orchestration up-serving down restart clean         ps health status logs urls tunnel start-ingestion start-cdc start-spark run-dbt         stop-ingestion reset-demo test lint

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

# ---- Stage 2 (Infrastructure): real bodies added when docker-compose.yml exists ----

up: ## Build and start everything
	@echo "make up: implemented in Stage 2 (Infrastructure)"

up-infra: ## Databases and Kafka
	@echo "make up-infra: implemented in Stage 2 (Infrastructure)"

up-stream: ## Kafka Connect, ingestor, simulator, Spark
	@echo "make up-stream: implemented across Stage 2, Stage 4 and Stage 5"

up-orchestration: ## Airflow
	@echo "make up-orchestration: implemented in Stage 2 (Infrastructure)"

up-serving: ## Grafana and dbt-docs
	@echo "make up-serving: implemented in Stage 2 (Infrastructure) and Stage 7 (Grafana)"

down: ## Stop everything, keep data
	@echo "make down: implemented in Stage 2 (Infrastructure)"

restart: ## Stop and start again, keep data
	@echo "make restart: implemented in Stage 2 (Infrastructure)"

clean: ## Remove everything including volumes
	@echo "make clean: implemented in Stage 2 (Infrastructure)"

ps: ## Service status
	@echo "make ps: implemented in Stage 2 (Infrastructure)"

health: ## Run check_health.sh
	@bash scripts/check_health.sh

status: ## Run pipeline_status.sh (row counts per layer every 5s)
	@bash scripts/pipeline_status.sh

logs: ## Show logs for one service, e.g. make logs service=ingestor
	@echo "make logs: implemented in Stage 2 (Infrastructure). Usage: make logs service=<name>"

# ---- Stage 3-6: Airflow DAG equivalents ----

start-ingestion: ## Equivalent of DAG 01_start_ingestion
	@echo "make start-ingestion: implemented in Stage 3 (Ingestion)"

start-cdc: ## Equivalent of DAG 02_start_cdc_kafka
	@echo "make start-cdc: implemented in Stage 4 (CDC & Kafka)"

start-spark: ## Equivalent of DAG 03_start_spark_streaming
	@echo "make start-spark: implemented in Stage 5 (Spark streaming)"

run-dbt: ## Equivalent of DAG 04_dbt_transform
	@echo "make run-dbt: implemented in Stage 6 (dbt)"

stop-ingestion: ## Equivalent of DAG 05_stop_ingestion
	@echo "make stop-ingestion: implemented in Stage 3 (Ingestion)"

reset-demo: ## Run reset_demo.sh
	@bash scripts/reset_demo.sh
