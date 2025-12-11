# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Lakehouse Crypto is a local data lakehouse that ingests real-time cryptocurrency data from Coinbase, processes it through a medallion architecture (Bronze → Silver → Gold), and serves it for analytics via Grafana dashboards.

## Architecture

**Data Flow:** Coinbase Websocket → Kafka → Spark Streaming → Bronze (Iceberg) → Spark Batch → Silver → dbt → Gold → Trino → Grafana

**Medallion Layers:**
- **Bronze**: Raw append-only events, partitioned by `day(_ingested_at)`
- **Silver**: Deduplicated, typed trades, partitioned by `day(trade_time), product_id`
- **Gold**: OHLCV candles, daily metrics, latest prices

**Key Technologies:**
- Storage: SeaweedFS (S3-compatible)
- Table Format: Apache Iceberg with Nessie catalog
- Processing: PySpark (streaming + batch)
- Orchestration: Airflow
- Transformation: dbt-spark
- Query: Trino
- Monitoring: Prometheus + Grafana

## Commands

```bash
# Start all services
docker compose up -d

# Initialize infrastructure (buckets, topics, catalog)
docker compose exec init python /init/init-all.py

# View logs
docker compose logs -f [service]

# Run Spark streaming (Kafka → Bronze)
docker compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark-jobs/streaming_bronze_writer.py

# Run batch job (Bronze → Silver)
docker compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark-jobs/bronze_to_silver.py

# Run dbt models
docker compose exec dbt dbt run

# Open Trino CLI
docker compose exec trino trino --catalog iceberg --schema silver

# Run tests
pytest tests/ -v

# Lint
ruff check .
```

## Service Ports

| Service | Port |
|---------|------|
| Grafana | 3000 |
| Spark UI | 8090 |
| Airflow | 8081 |
| Trino | 8082 |
| SeaweedFS S3 | 8333 |
| Kafka | 9092 |
| Prometheus | 9090 |
| Nessie | 19120 |

## Schema Reference

**Bronze trades:** `trade_id`, `product_id`, `price` (string), `size` (string), `side`, `time`, `_ingested_at`, `_raw_payload`, `_kafka_offset`, `_kafka_partition`

**Silver trades:** `trade_id` (bigint), `product_id`, `price` (decimal), `size` (decimal), `side`, `trade_time`, `ingested_at`, `_is_late_arrival`, `_source_latency_ms`

**Gold OHLCV:** `product_id`, `window_start`, `window_end`, `open`, `high`, `low`, `close`, `volume`, `trade_count`, `vwap`

## Trading Pairs

BTC-USD, ETH-USD, SOL-USD, DOGE-USD

## Kafka Topics

- `coinbase.raw.trades` (4 partitions, keyed by product_id)
- `coinbase.dlq` (dead letter queue)
