# Real-Time Fraud Detection Pipeline

Built this to understand how streaming fraud detection actually works at the infrastructure level — not just the ML model, but the full pipeline: ingestion, anomaly scoring, data quality enforcement, and serving enriched results to compliance dashboards.

Uses synthetic transaction data (10K+ events/sec) with realistic distributions across merchant categories, geographies, and customer behavior patterns.

[![CI](https://github.com/Poojareddy0/fraud-detection-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Poojareddy0/fraud-detection-pipeline/actions)

---

## What it does

Simulates a high-volume payment transaction stream → detects suspicious activity in real time using 4 statistical rules → lands enriched events to a Medallion lakehouse → serves a live compliance dashboard via Power BI.

End-to-end latency target: under 800ms from event ingestion to Bronze write.

---

## Architecture

```
Python Producer (async, 10K+ events/sec)
  · Pydantic schema validation before send
  · Realistic distributions: 85% US, 2% anomaly rate
  · Batch every 100ms for Event Hubs efficiency
        │
        ▼
Azure Event Hubs — 8 partitions
  · Partitioned by customer_id
  · Same partition = correct rolling window stats per customer
        │
        ▼
PySpark Structured Streaming
  · Watermark: 10 minutes (handles late-arriving events)
  · Rule 1: Z-score on 5-min rolling amount per customer (weight: 40)
  · Rule 2: High-risk country + card-not-present (weight: 30)
  · Rule 3: >5 transactions in 2-min window (velocity, weight: 20)
  · Rule 4: Amount > $2,000 + card-not-present (weight: 10)
  · Score ≥ 40 = suspicious
  · Malformed records → dead-letter zone (never silently dropped)
        │
        ▼
ADLS Gen2 — Bronze (Delta Lake)
  · ACID writes — crash-safe, no partial files
  · Partitioned by merchant_category
  · Checkpointed — restarts resume from last committed offset
        │
        ▼ (Airflow — every 30 min)
Great Expectations — 8 checks
  · Blocks Silver load if any check fails
  · Failed batch stays in Bronze for investigation
        │
        ▼
dbt — Bronze → Silver → Gold
  · stg_transactions: type casting, null filtering
  · dim_customers: SCD Type 2 (risk tier history)
  · fct_fraud_events: incremental merge on transaction_id
        │
        ▼
Azure Synapse Analytics → Power BI
  · Compliance dashboard: flagged counts, rules breakdown, audit trail
```

---

## Why these choices

**Partitioning Event Hubs by customer_id** — the z-score rule needs all events for the same customer to land on the same Spark partition. Random partitioning would spread a customer's events across tasks, making per-customer rolling stats impossible without expensive shuffles.

**Delta Lake over Parquet for Bronze** — if the Spark job crashes mid-write, Parquet leaves partial files that corrupt downstream reads. Delta rolls back incomplete writes atomically. Also gives time travel — useful for debugging why a batch was flagged.

**Dead-letter zone instead of dropping bad records** — silently dropping malformed events is the worst thing you can do in a fraud pipeline. Every failed record lands in the dead-letter zone with the reason attached, so compliance teams can audit the full event history.

**SCD Type 2 on dim_customers** — compliance requirement: "what was this customer's risk tier at the time of the transaction?" A simple overwrite would destroy that history. SCD2 keeps every change with valid_from / valid_to timestamps.

**Great Expectations before Silver load** — dbt tests catch issues after transformation. GE catches them before. Discovered in testing that ~0.5% of events had country codes that were 3 chars instead of 2 — a dbt test would have loaded them and failed silently on downstream aggregations.

---

## Fraud scoring

| Rule | Condition | Weight |
|---|---|---|
| Z-score outlier | Amount z-score > 3.0 in 5-min customer window | 40 |
| High-risk country | Country in {NG, RU, VN, UA, KP, IR} + card not present | 30 |
| Velocity | > 5 transactions in 2 min for same customer | 20 |
| Amount threshold | > $2,000 + card not present | 10 |

Score ≥ 40 = suspicious. Multiple rules can fire — all stored in `fraud_rules_fired`.

---

## Performance

| Metric | Result |
|---|---|
| Throughput | 10,000+ events/sec (8-partition, 2 CU namespace) |
| End-to-end latency | < 800ms to Bronze Delta write |
| Dead-letter rate | ~0.1% under normal load |
| dbt Gold freshness | < 35 min (30-min Airflow schedule + dbt runtime) |

---

## Project structure

```
fraud-detection-pipeline/
├── infra/          # Terraform — Event Hubs, Synapse, ADLS, all 4 containers
├── producer/       # transaction_producer.py — async, Pydantic schema
├── streaming/      # fraud_detector.py — PySpark scoring + dead-letter
├── dbt_project/    # staging + dim_customers (SCD2) + fct_fraud_events
├── quality/        # Great Expectations suite — 8 inline checks
├── airflow/        # fraud_pipeline_dag.py — GE → dbt → freshness → Slack
└── .github/        # CI: lint + dbt test + GE on every PR
```

---

## Running locally

```bash
git clone https://github.com/Poojareddy0/fraud-detection-pipeline
cd fraud-detection-pipeline
make setup

# Local stack (Kafka + Airflow + Postgres)
docker compose up -d

# Run producer locally
make produce

# dbt
make dbt-run && make dbt-test
```

---

## Stack

Python 3.11 · Azure Event Hubs · PySpark · Delta Lake · ADLS Gen2 · Azure Synapse · dbt · Great Expectations · Apache Airflow · Power BI · Terraform · GitHub Actions
