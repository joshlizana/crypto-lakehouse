# Lakehouse Crypto — Project Specification

A production-grade local lakehouse portfolio project showcasing modern data engineering tools and practices.

---

## Project Overview

### Objective

Build a fully local, end-to-end data lakehouse that ingests real-time cryptocurrency market data, processes it through a medallion architecture, and serves it for analytics and monitoring. The stack prioritizes tools with high demand in job postings while demonstrating production-grade patterns.

### Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Architecture | Lakehouse (Medallion) | More sophisticated than warehouse-only; demonstrates understanding of industry convergence trend |
| Deployment | Fully local (Docker Compose) | Reproducible, no cloud costs, anyone can clone and run |
| Table Format | Apache Iceberg | Vendor-neutral, multi-engine support, strongest industry momentum |
| Data Source | Coinbase Websocket | True real-time stream, no API key required, stable and well-documented |

---

## Tool Stack

### Complete Technology Choices

| Layer | Tool | Alternatives Considered | Selection Rationale |
|-------|------|-------------------------|---------------------|
| Languages | Python, SQL | — | Present in 70-80% of job postings |
| Object Storage | SeaweedFS | MinIO (maintenance mode), Garage, Ceph | S3-compatible, active development, appropriate complexity |
| Table Format | Apache Iceberg | Delta Lake, Apache Hudi | Open standard, multi-engine support, best momentum |
| Catalog | Nessie | AWS Glue, Hive Metastore | Git-like branching/tagging — strong differentiator |
| Streaming | Apache Kafka | Redpanda | "Kafka" is the resume keyword; industry standard |
| Processing | Apache Spark (PySpark) | Flink, Polars | 38% of job postings; distributed processing standard |
| Orchestration | Apache Airflow | Prefect, Dagster | Dominates job postings; production credibility |
| Transformation | dbt (dbt-spark) | Plain SQL, custom Python | Software engineering practices for analytics |
| Query Engine | Trino | DuckDB, Presto | Fast interactive queries on Iceberg; production-grade |
| Visualization | Grafana | Superset, Metabase | Dual-purpose: business dashboards + pipeline observability |
| Monitoring | Prometheus | — | Pairs with Grafana; industry standard for metrics |
| Data Quality | Great Expectations | Soda, dbt tests | Most recognized; integrates with Airflow |
| Containerization | Docker + Docker Compose | Kubernetes | Right scope for portfolio project |
| CI/CD | GitHub Actions | GitLab CI, Jenkins | Most accessible; free for public repos |

---

## Architecture

### Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                 │
│                            STREAMING PATH                                       │
│                                                                                 │
│  ┌──────────────┐      ┌─────────────┐      ┌────────────────────────────────┐  │
│  │   Coinbase   │      │    Kafka    │      │         Spark Streaming        │  │
│  │  Websocket   │─────▶│   (Topic:   │─────▶│                                │  │
│  │              │      │ raw.trades) │      │  • Parse JSON                  │  │
│  └──────────────┘      └─────────────┘      │  • Add ingestion metadata      │  │
│                              │              │  • Write to Bronze (append)    │  │
│                              │              └────────────────┬───────────────┘  │
│                              ▼                               │                  │
│                        ┌──────────┐                          ▼                  │
│                        │   DLQ    │                 ┌─────────────────┐         │
│                        │ (Failed) │                 │  BRONZE.trades  │         │
│                        └──────────┘                 │    (Iceberg)    │         │
│                                                     └────────┬────────┘         │
│─────────────────────────────────────────────────────────────────────────────────│
│                                                              │                  │
│                            BATCH PATH                        │                  │
│                                                              ▼                  │
│  ┌─────────────┐      ┌─────────────────────────────────────────────────────┐   │
│  │   Airflow   │      │                    Spark Batch                      │   │
│  │  (Hourly)   │─────▶│                                                     │   │
│  └─────────────┘      │  • Deduplicate by trade_id                          │   │
│                       │  • Cast types, validate                             │   │
│                       │  • Compute latency metrics                          │   │
│                       │  • MERGE into Silver                                │   │
│                       └──────────────────────────┬──────────────────────────┘   │
│                                                  │                              │
│                                                  ▼                              │
│  ┌─────────────┐                        ┌─────────────────┐                     │
│  │    Great    │                        │  SILVER.trades  │                     │
│  │ Expectations│───── validates ───────▶│    (Iceberg)    │                     │
│  └─────────────┘                        └────────┬────────┘                     │
│                                                  │                              │
│                                                  ▼                              │
│  ┌─────────────┐      ┌─────────────────────────────────────────────────────┐   │
│  │   Airflow   │      │                      dbt                            │   │
│  │  (Hourly)   │─────▶│                                                     │   │
│  └─────────────┘      │  • stg_trades (staging)                             │   │
│                       │  • ohlcv_1m, ohlcv_1h (incremental)                 │   │
│                       │  • daily_metrics                                    │   │
│                       │  • price_latest                                     │   │
│                       └──────────────────────────┬──────────────────────────┘   │
│                                                  │                              │
│                                                  ▼                              │
│                                         ┌─────────────────┐                     │
│                                         │   GOLD tables   │                     │
│                                         │    (Iceberg)    │                     │
│                                         └────────┬────────┘                     │
│                                                  │                              │
└──────────────────────────────────────────────────┼──────────────────────────────┘
                                                   │
                    ┌──────────────────────────────┼──────────────────────────────┐
                    │                              ▼                              │
                    │  ┌──────────┐        ┌─────────────┐        ┌──────────┐    │
                    │  │  Trino   │───────▶│   Grafana   │◀───────│Prometheus│    │
                    │  │ (Query)  │        │ (Dashboard) │        │(Metrics) │    │
                    │  └──────────┘        └─────────────┘        └──────────┘    │
                    │                                                             │
                    │                      SERVING LAYER                          │
                    └─────────────────────────────────────────────────────────────┘
```

### Medallion Architecture Layers

| Layer | Purpose | Update Pattern | Partitioning |
|-------|---------|----------------|--------------|
| Bronze | Raw event capture | Append-only | day(ingested_at) |
| Silver | Cleaned, typed, deduplicated | MERGE/upsert | day(trade_time), product_id |
| Gold | Business aggregations | Incremental rebuild | Varies by table |

---

## Data Source

### Coinbase Websocket

**Endpoint:** `wss://ws-feed.exchange.coinbase.com`

**Channels:**

| Channel | Data | Purpose |
|---------|------|---------|
| `matches` | Executed trades | Core price/volume data |
| `ticker` | Price updates with 24h stats | Enrichment, real-time dashboard |
| `status` | System status | Pipeline health monitoring |

**Trading Pairs (Initial):**
- BTC-USD
- ETH-USD
- SOL-USD
- DOGE-USD

---

## Schema Design

### Bronze Layer — Raw Events

Append-only, exactly as received from source.

```
bronze.trades
├── _ingested_at: timestamp      -- when we received it
├── _raw_payload: string         -- full JSON (schema-on-read safety)
├── _kafka_offset: bigint        -- for replay/debugging
├── _kafka_partition: int
│
├── trade_id: string
├── product_id: string           -- BTC-USD, ETH-USD, etc.
├── price: string                -- keep as string, convert in Silver
├── size: string
├── side: string                 -- buy/sell
├── time: string                 -- ISO timestamp from Coinbase
│
└── Partitioned by: day(_ingested_at)
```

### Silver Layer — Cleaned Trades

Deduplicated, typed, validated.

```
silver.trades
├── trade_id: bigint
├── product_id: string
├── price: decimal(18,8)
├── size: decimal(18,8)
├── side: string
├── trade_time: timestamp
├── ingested_at: timestamp
│
├── _is_late_arrival: boolean    -- arrived after expected window
├── _source_latency_ms: bigint   -- trade_time vs ingested_at delta
│
└── Partitioned by: day(trade_time), product_id
```

### Gold Layer — Aggregated Metrics

#### OHLCV Candles (1-minute and 1-hour)

```
gold.ohlcv_1m / gold.ohlcv_1h
├── product_id: string
├── window_start: timestamp
├── window_end: timestamp
├── open: decimal(18,8)
├── high: decimal(18,8)
├── low: decimal(18,8)
├── close: decimal(18,8)
├── volume: decimal(18,8)
├── trade_count: bigint
├── vwap: decimal(18,8)          -- volume-weighted average price
│
└── Partitioned by: day(window_start), product_id
```

#### Daily Metrics

```
gold.daily_metrics
├── product_id: string
├── date: date
├── open: decimal(18,8)
├── high: decimal(18,8)
├── low: decimal(18,8)
├── close: decimal(18,8)
├── total_volume: decimal(18,8)
├── total_trades: bigint
├── volatility: decimal(18,8)    -- standard deviation of returns
├── max_drawdown: decimal(18,8)
│
└── Partitioned by: month(date)
```

#### Latest Prices (for Dashboard)

```
gold.price_latest
├── product_id: string
├── price: decimal(18,8)
├── updated_at: timestamp
```

---

## Kafka Configuration

### Topics

| Topic | Partitions | Key | Retention |
|-------|------------|-----|-----------|
| `coinbase.raw.trades` | 4 | `product_id` | 7 days |
| `coinbase.dlq` | 1 | — | 30 days |

Keying by `product_id` ensures ordering per trading pair.

---

## Docker Services

### Port Assignments

| Service | Port(s) | Purpose |
|---------|---------|---------|
| seaweedfs-master | 9333 | Cluster coordination |
| seaweedfs-volume | 8080 | Data storage |
| seaweedfs-filer | 8888 | File system abstraction |
| seaweedfs-s3 | 8333 | S3 API endpoint |
| kafka | 9092 | Message streaming |
| spark-master | 8090, 7077 | Spark UI, master protocol |
| spark-worker | — | Spark execution |
| airflow-webserver | 8081 | Airflow UI |
| airflow-scheduler | — | DAG scheduling |
| nessie | 19120 | Iceberg catalog |
| trino | 8082 | SQL query engine |
| prometheus | 9090 | Metrics collection |
| grafana | 3000 | Dashboards & monitoring |

### Resource Requirements

| Profile | RAM | Description |
|---------|-----|-------------|
| Minimal | ~12-16GB | Core pipeline without Spark streaming |
| Standard | ~24-32GB | Full stack, single Spark worker |
| Comfortable | 48GB+ | Full stack with headroom |

**Target environment:** 128GB RAM (no constraints)

---

## Component Details

### 1. Coinbase Producer (Python)

**Location:** `ingestion/producers/coinbase_producer.py`

**Responsibilities:**
- Connect to Coinbase websocket
- Subscribe to configured channels and trading pairs
- Publish to `coinbase.raw.trades` Kafka topic
- Handle reconnection with exponential backoff
- Emit metrics (messages/sec) to Prometheus

### 2. Spark Streaming Job

**Location:** `processing/spark_jobs/streaming_bronze_writer.py`

**Responsibilities:**
- Consume from `coinbase.raw.trades`
- Write to Bronze Iceberg table (append-only)
- Checkpoint to SeaweedFS for exactly-once semantics
- Micro-batch interval: 10 seconds

### 3. Spark Batch Job (Bronze → Silver)

**Location:** `processing/spark_jobs/bronze_to_silver.py`

**Responsibilities:**
- Deduplicate by trade_id
- Cast string fields to proper types
- Validate data (price > 0, valid timestamps)
- Compute latency metrics
- MERGE into Silver table

### 4. dbt Models

**Location:** `processing/dbt/models/`

**Structure:**
```
models/
├── staging/
│   └── stg_trades.sql           -- light transform from Silver
├── intermediate/
│   └── int_trades_enriched.sql  -- computed fields
└── marts/
    ├── ohlcv_1m.sql             -- 1-minute candles
    ├── ohlcv_1h.sql             -- 1-hour candles
    ├── daily_metrics.sql        -- daily aggregations
    └── price_latest.sql         -- current prices
```

### 5. Airflow DAGs

**Location:** `orchestration/dags/`

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `bronze_to_silver.py` | Hourly | Run Spark batch job |
| `dbt_gold.py` | Hourly (after Silver) | Run dbt models |
| `iceberg_maintenance.py` | Daily | Compaction, snapshot expiry |
| `data_quality.py` | After each layer | Great Expectations checkpoints |

### 6. Grafana Dashboards

**Real-Time Market Dashboard** (Trino → Gold)
- Current price per pair (big number panels)
- 24h price chart (time series)
- OHLCV candlestick visualization
- Volume by pair (bar chart)
- Trade count heatmap by hour

**Pipeline Health Dashboard** (Prometheus)
- Kafka consumer lag
- Messages/sec throughput
- Spark job durations
- Airflow task success/failure
- SeaweedFS storage utilization
- End-to-end latency (ingestion to Gold)

---

## Iceberg Features to Demonstrate

| Feature | Location | Demonstration |
|---------|----------|---------------|
| Time travel | Trino query | `SELECT * FROM silver.trades FOR TIMESTAMP AS OF '2025-01-01 12:00:00'` |
| Schema evolution | Bronze table | Add new field, show old data still readable |
| Partition evolution | Silver table | Evolve from day partition to day+product_id |
| Branching (Nessie) | Catalog | Create `dev` branch for testing, merge to main |
| Compaction | Maintenance DAG | Scheduled `rewrite_data_files` procedure |
| Snapshot expiry | Maintenance DAG | Expire snapshots older than 7 days |

---

## Production-Grade Elements

| Element | Implementation |
|---------|----------------|
| CI/CD | GitHub Actions for linting, tests, Docker builds |
| Data quality gates | Great Expectations checkpoints between layers |
| Idempotent pipelines | Handle reruns gracefully via MERGE and deduplication |
| Dead letter queue | Capture failed Kafka messages to `coinbase.dlq` |
| Compaction jobs | Iceberg small file management via maintenance DAG |
| Monitoring & alerting | Grafana dashboards + alert rules |
| Documentation | README, architecture diagrams, dbt docs |

---

## Project Structure

```
lakehouse-crypto/
├── docker-compose.yml
├── .env
├── Makefile
├── README.md
│
├── infrastructure/
│   └── init/
│       ├── init-all.py              # Single orchestrator script
│       ├── wait-for-services.sh     # Health checks before init
│       ├── seaweedfs-buckets.py     # Create S3 buckets
│       ├── kafka-topics.py          # Create topics
│       └── nessie-setup.py          # Create namespaces, branches
│
├── ingestion/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producers/
│       ├── coinbase_producer.py
│       └── config.py
│
├── processing/
│   ├── spark_jobs/
│   │   ├── streaming_bronze_writer.py
│   │   ├── bronze_to_silver.py
│   │   └── compaction.py
│   └── dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── models/
│       │   ├── staging/
│       │   ├── intermediate/
│       │   └── marts/
│       └── tests/
│
├── orchestration/
│   └── dags/
│       ├── bronze_to_silver.py
│       ├── dbt_gold.py
│       ├── iceberg_maintenance.py
│       └── data_quality.py
│
├── quality/
│   └── great_expectations/
│       ├── great_expectations.yml
│       ├── expectations/
│       │   ├── bronze_trades.json
│       │   └── silver_trades.json
│       └── checkpoints/
│
├── monitoring/
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       └── provisioning/
│           ├── datasources/
│           ├── dashboards/
│           └── alerting/
│
├── trino/
│   └── catalog/
│       └── iceberg.properties
│
├── tests/
│   ├── unit/
│   └── integration/
│
├── docs/
│   ├── architecture.md
│   └── cloud-deployment.md
│
└── .github/
    └── workflows/
        └── ci.yml
```

---

## Cloud Migration Path

For production deployment, the following substitutions would apply:

| Local Component | AWS Equivalent | Notes |
|-----------------|----------------|-------|
| SeaweedFS | S3 | Direct replacement |
| Kafka | MSK | Managed Kafka |
| Spark | EMR Serverless | Pay-per-job |
| Nessie | AWS Glue Catalog | Or keep Nessie on ECS |
| Airflow | MWAA | Managed Airflow |
| Trino | Athena | Serverless queries |
| Grafana | Amazon Managed Grafana | Or self-hosted on ECS |
| Prometheus | Amazon Managed Prometheus | Or CloudWatch |

See `docs/cloud-deployment.md` for detailed migration guide.

---

## Next Steps

1. **Docker Compose** — Full service definitions with all configs
2. **Init scripts** — SeaweedFS buckets, Kafka topics, Nessie setup
3. **Coinbase Producer** — Python websocket client → Kafka
4. **Spark Streaming Job** — Kafka → Bronze Iceberg
5. **Spark Batch Job** — Bronze → Silver
6. **dbt Models** — Gold layer transformations
7. **Airflow DAGs** — Orchestration setup
8. **Grafana Dashboards** — Market data + pipeline health
9. **Great Expectations** — Data quality checkpoints
10. **CI/CD** — GitHub Actions workflows
