.PHONY: help up down restart logs ps init clean \
        producer-start producer-stop producer-logs \
        spark-streaming spark-batch \
        dbt-run dbt-test dbt-docs \
        trino-cli \
        test lint format

# Default target
help:
	@echo "Lakehouse Crypto - Available Commands"
	@echo "======================================"
	@echo ""
	@echo "Docker Compose:"
	@echo "  make up              - Start all services"
	@echo "  make down            - Stop all services"
	@echo "  make restart         - Restart all services"
	@echo "  make logs            - Tail logs from all services"
	@echo "  make logs s=<svc>    - Tail logs from specific service"
	@echo "  make ps              - Show running services"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make init            - Initialize buckets, topics, catalog"
	@echo "  make clean           - Remove all data and volumes"
	@echo ""
	@echo "Ingestion:"
	@echo "  make producer-start  - Start Coinbase producer"
	@echo "  make producer-stop   - Stop Coinbase producer"
	@echo "  make producer-logs   - View producer logs"
	@echo ""
	@echo "Processing:"
	@echo "  make spark-streaming - Start Spark streaming job (Kafka -> Bronze)"
	@echo "  make spark-batch     - Run Bronze to Silver batch job"
	@echo ""
	@echo "dbt:"
	@echo "  make dbt-run         - Run dbt models"
	@echo "  make dbt-test        - Run dbt tests"
	@echo "  make dbt-docs        - Generate and serve dbt docs"
	@echo ""
	@echo "Query:"
	@echo "  make trino-cli       - Open Trino CLI"
	@echo ""
	@echo "Development:"
	@echo "  make test            - Run all tests"
	@echo "  make lint            - Run linters"
	@echo "  make format          - Format code"
	@echo ""
	@echo "URLs:"
	@echo "  Grafana:     http://localhost:3000  (admin/admin)"
	@echo "  Airflow:     http://localhost:8081  (admin/admin)"
	@echo "  Spark UI:    http://localhost:8090"
	@echo "  Trino:       http://localhost:8082"
	@echo "  Prometheus:  http://localhost:9090"
	@echo "  Nessie:      http://localhost:19120"

# =============================================================================
# Docker Compose
# =============================================================================

up:
	docker compose up -d

up-core:
	docker compose up -d seaweedfs-master seaweedfs-volume seaweedfs-filer seaweedfs-s3 \
		zookeeper kafka nessie postgres

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

logs:
ifdef s
	docker compose logs -f $(s)
else
	docker compose logs -f
endif

ps:
	docker compose ps

# =============================================================================
# Infrastructure
# =============================================================================

init:
	docker compose run --rm init python /init/init-all.py

clean:
	docker compose down -v
	@echo "All volumes removed"

# =============================================================================
# Ingestion
# =============================================================================

producer-start:
	docker compose up -d coinbase-producer

producer-stop:
	docker compose stop coinbase-producer

producer-logs:
	docker compose logs -f coinbase-producer

# =============================================================================
# Processing
# =============================================================================

spark-streaming:
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.0,org.apache.iceberg:iceberg-aws-bundle:1.4.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		/opt/spark-jobs/streaming_bronze_writer.py

spark-batch:
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.0,org.apache.iceberg:iceberg-aws-bundle:1.4.3 \
		/opt/spark-jobs/bronze_to_silver.py

# =============================================================================
# dbt
# =============================================================================

dbt-run:
	cd processing/dbt && dbt run --profiles-dir .

dbt-test:
	cd processing/dbt && dbt test --profiles-dir .

dbt-docs:
	cd processing/dbt && dbt docs generate --profiles-dir . && dbt docs serve --port 8084

# =============================================================================
# Query
# =============================================================================

trino-cli:
	docker compose exec trino trino --catalog iceberg --schema silver

# =============================================================================
# Development
# =============================================================================

test:
	pytest tests/ -v

lint:
	ruff check .

format:
	ruff format .
	ruff check --fix .

# =============================================================================
# Iceberg Maintenance
# =============================================================================

iceberg-compact:
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.0,org.apache.iceberg:iceberg-aws-bundle:1.4.3 \
		/opt/spark-jobs/compaction.py
