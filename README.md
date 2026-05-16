# Event-Driven E-Commerce Analytics Platform

An end-to-end streaming data platform built on a medallion lakehouse architecture, processing retail transaction events through Kafka, PySpark, and Airflow to deliver analytics-ready datasets and business dashboards.

---

## Problem Statement

A UK-based online retail company generates transactional events across orders, products, and customers. Business teams need visibility into sales performance, customer behaviour, and product trends — but raw transactional data arrives with quality issues: duplicate records, malformed JSON, guest orders with no customer identity, and mixed cancellation and purchase events.

This platform ingests retail events in near real-time, processes them through a structured medallion architecture, enforces data quality at every layer, and serves clean analytics to a BI dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│              Online Retail II Dataset (525,461 records)             │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                              │
│                                                                     │
│   Kafka Producer  ──►  retail_order_events    (Kafka Topic)         │
│                   ──►  retail_customer_events (Kafka Topic)         │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     MEDALLION LAKEHOUSE (MinIO / S3)                │
│                                                                     │
│  BRONZE  ──►  Raw JSON events, schema-on-read, append-only          │
│     │                                                               │
│     ▼                                                               │
│  SILVER  ──►  Parsed, cleaned, deduplicated, typed, partitioned     │
│     │                                                               │
│     ▼                                                               │
│  GOLD    ──►  Star schema: fact + dims + reporting tables           │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       SERVING LAYER                                 │
│                                                                     │
│   PostgreSQL (analytics db)  ──►  Superset Dashboards               │
└─────────────────────────────────────────────────────────────────────┘
                             │
                    Orchestrated by
                             │
                       Apache Airflow
                    (DAG: retail_pipeline)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka (KRaft mode) |
| Stream Processing | Apache Spark Structured Streaming |
| Batch Processing | PySpark |
| Storage | MinIO (S3-compatible object store) |
| Orchestration | Apache Airflow 2.9.1 |
| Serving DB | PostgreSQL 15 |
| Dashboarding | Apache Superset |
| Containerisation | Docker + Docker Compose |
| Language | Python 3.11 |

---

## Data Flow

### Ingestion
The Kafka producer replays the Online Retail II dataset row by row with controlled time delays, emitting two event types to two Kafka topics:
- `order_item_created` — one event per order line item (Quantity > 0)
- `order_item_cancelled` — one event per cancellation (Quantity < 0)
- `customer_activity` — emitted once per new customer (first appearance)

### Bronze Layer
Spark Structured Streaming consumes both Kafka topics and writes raw JSON envelopes to MinIO as partitioned Parquet files. No transformation — bronze is the source of truth.

### Silver Layer
PySpark reads bronze, parses JSON with schema enforcement, applies a NaN fix for malformed records, deduplicates on `(invoice_id, stock_code, event_time, quantity)`, casts types, and writes partitioned silver tables.

### Gold Layer
Star schema built on silver:
- **Dimensions:** `dim_date`, `dim_product`, `dim_customer`, `dim_geography`
- **Fact:** `fact_order_items`
- **Reporting:** revenue trends, customer CLV, retention rate, basket size, product demand

### Serving
Gold reporting tables are written to PostgreSQL and visualised in Apache Superset across three dashboards: Revenue & Sales, Customer Analytics, Product Insights.

---

## Key Engineering Decisions & Problems Solved

### Small Files Problem (Bronze Layer)
Without `maxOffsetsPerTrigger`, Spark Structured Streaming created one Parquet file per micro-batch, resulting in 30,000+ files and 940 partitions in bronze. Fixed in silver by repartitioning to 10 partitions and partitioning by `year/month`.

### NaN Serialisation Bug (Producer)
Pandas `NaN` values in the `description` column were serialised as literal `NaN` (not valid JSON) by `json.dumps`. This caused `get_json_object` to return NULL for all fields in those records — not just description. Fixed in silver using `regexp_replace` to substitute `NaN` with `null` before JSON parsing, recovering all 2,928 affected records.

### Incorrect Deduplication Key
Initial dedup used `(invoice_id, stock_code, event_time)` — this incorrectly dropped 11,908 valid records where the same product appeared on the same invoice with different quantities. Corrected to `(invoice_id, stock_code, event_time, quantity)`, where only truly identical rows are dropped.

### Layer-by-Layer Reconciliation
Every row drop between layers is traced to a concrete cause:

| Layer | Count | Drop Reason |
|---|---|---|
| Raw | 525,461 | — |
| Bronze | 525,425 | -36 producer checkpoint skip |
| Silver | 518,446 | -6,979 true duplicates removed |

---

## Project Structure

```
.
├── infra/                          # Docker infrastructure
│   ├── docker-compose.yml
│   ├── Dockerfile_spark
│   ├── Dockerfile_airflow
│   ├── Dockerfile_jupyter
│   └── Dockerfile_superset
│
├── ingestion/                      # Kafka producers
│   └── producer.py
│
├── spark/                          # PySpark jobs
│   ├── bronze_layer.py             # Spark Structured Streaming consumer
│   ├── silver_layer.py             # Cleaning, dedup, typing
│   ├── dimensions.py               # Gold dimension tables
│   ├── facts.py                    # Gold fact table
│   ├── reporting.py                # Gold reporting tables
│   ├── data_quality_checks.py      # Bronze + silver DQ checks
│   ├── gold_quality_checks.py      # Gold DQ checks
│   ├── gold_to_postgres.py         # Serving layer writer
│   └── utils/
│       └── logger.py               # PipelineLogger utility
│
├── dags/                           # Airflow DAGs
│   └── retail_pipeline_dag.py
│
├── docs/                           # Documentation
│   └── event_schema.md
│
└── notebooks/                      # Exploratory analysis
    └── data_validation.ipynb
```

---

## Airflow Pipeline

```
start
  └── silver_layer
        └── silver_quality_checks
              └── gold_dimensions
                    └── gold_facts
                          └── gold_reporting
                                └── gold_quality_checks
                                      └── gold_to_postgres
                                            └── end
```

Schedule: daily at 06:00 UTC. Each task uses `SparkSubmitOperator` to submit jobs to the standalone Spark cluster.

---

## Data Quality Framework

A unified `data_quality_metrics` table in MinIO stores check results from every layer on every run:

| Check Type | Examples |
|---|---|
| Row count | Bronze vs raw, silver vs bronze |
| Null checks | Critical columns with configurable thresholds |
| Duplicate checks | Post-dedup sanity on all key columns |
| Business rules | Zero quantity, negative prices, invalid event types |
| Referential integrity | Orders customer_id must exist in customers silver |
| Aggregate anomaly | Zero revenue months, MoM deviation > 30% |
| Gold reconciliation | Revenue in reporting tables matches fact table sum |

---

## Dashboards (Apache Superset)

**Revenue & Sales**
- Monthly revenue trend with MoM change
- Average order value over time
- Top 10 products by revenue
- Revenue by country

**Customer Analytics**
- New vs returning customers per month
- Monthly retention rate
- Customers by country
- Top customers by lifetime value

**Product Insights**
- Average basket size trend
- Top 5 product demand trends

---

## Setup & Running

### Prerequisites
- Docker Desktop
- 8GB+ RAM allocated to Docker

### Start the platform
```bash
cd infra

# First time only — initialise Airflow DB
docker compose run --rm airflow-init

# Start all services
docker compose up -d
```

### Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8082 | admin / admin |
| Spark Master UI | http://localhost:8081 | — |
| MinIO Console | http://localhost:9001 | admin / admin123 |
| Kafka UI | http://localhost:8080 | — |
| Superset | http://localhost:8088 | admin / admin |
| Jupyter | http://localhost:8888 | — |

### Run the pipeline
1. Start the Kafka producer: `python ingestion/producer.py`
2. Start the bronze consumer: `spark-submit spark/bronze_layer.py`
3. Trigger the Airflow DAG: http://localhost:8082 → retail_pipeline → Trigger

---

## Environment Variables

Sensitive configuration is managed via a `.env` file. Copy `.env.example` and fill in your values:

```bash
cp .env.example .env
```

Key variables:
```
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=
POSTGRES_USER=airflow
POSTGRES_PASSWORD=
KAFKA_BOOTSTRAP_SERVER=kafka:29092
```

---

## Known Limitations & Future Improvements

- **Static dataset replayed as streaming** — in production, the Kafka producer would connect to a live transactional database CDC feed or clickstream
- **Full rewrite on each run** — silver and gold layers overwrite completely. Delta Lake with merge/upsert would enable true incremental processing
- **No watermarking** — late-arriving events are not handled. Spark Structured Streaming watermarks would be added in production
- **Local deployment** — MinIO mimics S3. Production deployment would use AWS S3 + EMR or Databricks
- **No schema registry** — Confluent Schema Registry would enforce schema contracts at the Kafka level in production

---

## Dataset

[Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii) — UCI Machine Learning Repository  
UK-based online retail transactions, Dec 2009 – Dec 2010.