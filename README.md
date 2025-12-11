# Lakehouse Crypto

A production-grade local data lakehouse that ingests real-time cryptocurrency market data, processes it through a medallion architecture, and serves it for analytics via Grafana dashboards.

## Architecture

```
Coinbase WebSocket → Kafka → Spark Streaming → Bronze (Iceberg)
                                                    ↓
                              Spark Batch → Silver (Iceberg)
                                                    ↓
                                    dbt → Gold (OHLCV, Metrics)
                                                    ↓
                                         Trino → Grafana
```

**Medallion Layers:**
- **Bronze**: Raw append-only events from Coinbase
- **Silver**: Deduplicated, typed, validated trades
- **Gold**: OHLCV candles (1m, 1h), daily metrics, latest prices

## Tech Stack

| Component | Technology |
|-----------|------------|
| Object Storage | SeaweedFS (S3-compatible) |
| Table Format | Apache Iceberg |
| Catalog | Nessie |
| Streaming | Apache Kafka |
| Processing | Apache Spark (PySpark) |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| Query Engine | Trino |
| Visualization | Grafana |
| Monitoring | Prometheus |
| Data Quality | Great Expectations |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- 24-32GB RAM recommended (full stack)
- Make (optional, for convenience commands)

### Start Services

```bash
# Start all services
docker compose up -d

# Initialize infrastructure (buckets, topics, catalog)
make init

# Start the Coinbase producer
make producer-start

# Start Spark streaming (Kafka → Bronze)
make spark-streaming

# View logs
make logs s=coinbase-producer
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Airflow | http://localhost:8081 | admin / admin |
| Spark UI | http://localhost:8090 | - |
| Trino | http://localhost:8082 | - |
| Prometheus | http://localhost:9090 | - |
| Nessie | http://localhost:19120 | - |

## Project Structure

```
lakehouse-crypto/
├── docker-compose.yml          # All services
├── Makefile                    # Common commands
│
├── infrastructure/
│   ├── init/                   # Bucket, topic, catalog setup
│   ├── seaweedfs/              # S3 credentials
│   └── spark/                  # Spark config
│
├── ingestion/
│   └── producers/              # Coinbase WebSocket → Kafka
│
├── processing/
│   ├── spark_jobs/             # Streaming & batch jobs
│   └── dbt/                    # Gold layer models
│
├── orchestration/
│   └── dags/                   # Airflow DAGs
│
├── quality/
│   └── great_expectations/     # Data quality checks
│
├── monitoring/
│   ├── prometheus/             # Scrape config
│   └── grafana/                # Dashboards
│
└── trino/
    └── catalog/                # Iceberg connector config
```

## Data Pipeline

### Streaming Path (Real-time)

1. **Coinbase Producer** connects to WebSocket, publishes to Kafka
2. **Spark Streaming** reads from Kafka, writes to Bronze (5s micro-batches)

### Batch Path (Hourly)

1. **Bronze → Silver**: Dedupe, type cast, compute latency metrics
2. **dbt Gold**: Build OHLCV candles and aggregations
3. **Great Expectations**: Validate data quality

### Trading Pairs

- BTC-USD
- ETH-USD
- SOL-USD
- DOGE-USD

## Commands

```bash
# Docker
make up                  # Start all services
make down                # Stop all services
make ps                  # Show running services
make logs s=<service>    # View service logs

# Infrastructure
make init                # Initialize buckets, topics, catalog
make clean               # Remove all data and volumes

# Processing
make spark-streaming     # Start streaming job
make spark-batch         # Run Bronze → Silver
make dbt-run             # Run dbt models

# Query
make trino-cli           # Open Trino CLI

# Development
make test                # Run tests
make lint                # Run linters
```

## Iceberg Features Demonstrated

- **Time Travel**: Query historical data via `FOR TIMESTAMP AS OF`
- **Schema Evolution**: Add fields without breaking existing data
- **Partition Evolution**: Change partitioning without rewriting
- **Branching**: Nessie git-like branches for testing
- **Compaction**: Scheduled small file consolidation
- **Snapshot Expiry**: Automatic old snapshot cleanup

## Monitoring

### Grafana Dashboards

- **Market Overview**: Live prices, 24h charts, volume
- **Pipeline Health**: WebSocket status, message rates, latency

### Prometheus Metrics

- `coinbase_messages_received_total`
- `coinbase_messages_produced_total`
- `coinbase_websocket_connected`
- `coinbase_message_latency_seconds`

## Development

```bash
# Run tests
pytest tests/ -v

# Run linter
ruff check .

# Format code
ruff format .
```

## License

MIT
