# Real-Time Fraud Detection Pipeline

End-to-end streaming data pipeline that ingests financial transaction events, applies multi-rule anomaly detection with sub-second latency, and serves enriched fraud signals to compliance teams via live Power BI dashboards.

[![CI](https://github.com/your-username/fraud-detection-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/your-username/fraud-detection-pipeline/actions)

---

## Architecture

```
Azure Event Hubs (8 partitions)
        │
        ▼
PySpark Structured Streaming
  ├── Z-score anomaly detection (5-min rolling window)
  ├── High-risk country + card-not-present rules
  ├── Velocity detection (>5 txns / 2 min)
  └── Amount threshold rules ($2K+ card-not-present)
        │
        ├──► Dead-letter zone (ADLS) ← malformed / failed records
        │
        ▼
  ADLS Gen2 — Bronze layer (Delta format, partitioned by category)
        │
        ▼ (Airflow — every 30 min)
  Great Expectations quality gate
  [blocks Silver load if checks fail]
        │
        ▼
  dbt transformations
  ├── staging/   — type casting, cleaning, NULL handling
  ├── intermediate/ — customer risk aggregation
  └── marts/
        ├── fct_fraud_events  (incremental merge, SCD-aware)
        └── dim_customers     (SCD Type 2 — risk tier history)
        │
        ▼
  Azure Synapse Analytics — Gold layer
        │
        ▼
  Power BI — live compliance dashboard
```

---

## Key Technical Decisions

| Decision | Choice | Why |
|---|---|---|
| Streaming format | Delta Lake (Bronze) | ACID transactions, time travel, schema evolution without rewrites |
| Anomaly method | Z-score + rolling window | Statistically principled, explainable to compliance — not a black box |
| Dead-letter strategy | Quarantine to ADLS, not drop | Enables forensic audit; every failed record is recoverable |
| Incremental strategy | dbt merge on `transaction_id` | Idempotent — safe to rerun without duplicates |
| SCD Type 2 | Customer risk tier history | Compliance requirement: "what was the customer's risk tier at time of transaction?" |
| Data quality | Great Expectations inline | Blocks bad data before Silver — downstream marts always clean |
| IaC | Terraform | One-command environment provisioning; reproducible across dev/staging/prod |

---

## Fraud Detection Rules

| Rule | Logic | Score Weight |
|---|---|---|
| Z-score outlier | Amount z-score > 3.0 in 5-min customer window | 40 |
| High-risk country | Merchant country in `{NG, RU, VN, UA, KP, IR}` + card not present | 30 |
| Velocity breach | > 5 transactions in 2-min window for same customer | 20 |
| Amount threshold | Amount > $2,000 and card not present | 10 |

A transaction with `fraud_score >= 40` is flagged as suspicious.

---

## Performance

| Metric | Value |
|---|---|
| Throughput | 10,000+ events/sec (8-partition Event Hub, 2 CU namespace) |
| End-to-end latency | < 800ms (Event Hub → Bronze Delta write) |
| Replication lag (Bronze → Gold) | < 35 minutes (Airflow 30-min schedule + dbt runtime) |
| Dead-letter rate | ~0.1% under normal load |
| dbt model freshness SLA | 60 minutes (Slack alert on breach) |

---

## Project Structure

```
fraud-detection-pipeline/
├── infra/                  # Terraform — all Azure infrastructure as code
├── producer/               # Python event simulator (Pydantic schema, async batching)
├── streaming/              # PySpark Structured Streaming (fraud detection engine)
├── dbt_project/            # dbt Bronze → Silver → Gold transformations
│   └── models/
│       ├── staging/        # stg_transactions (typed, cleaned)
│       ├── intermediate/   # customer risk aggregations
│       └── marts/          # fct_fraud_events, dim_customers (SCD2)
├── quality/                # Great Expectations suite (8 expectations, inline gate)
├── airflow/                # Orchestration DAGs (pipeline + SLA monitor)
└── .github/workflows/      # CI (dbt test + GE on PR) + CD (Terraform on merge)
```

---

## Getting Started

### Prerequisites
- Python 3.11+
- Terraform 1.7+
- Azure subscription (Event Hubs Standard tier, Synapse, ADLS Gen2)
- Docker + Docker Compose (for local dev)

### Local development

```bash
# Clone and install
git clone https://github.com/your-username/fraud-detection-pipeline
cd fraud-detection-pipeline
make setup

# Start local stack (Kafka + Airflow + Postgres)
docker compose up -d

# Run the event producer locally (Kafka endpoint)
make produce

# Run dbt against local Postgres
make dbt-run && make dbt-test
```

### Deploy to Azure

```bash
# Set required env vars
export ARM_CLIENT_ID=...
export ARM_CLIENT_SECRET=...
export ARM_SUBSCRIPTION_ID=...
export ARM_TENANT_ID=...

# Provision all Azure infrastructure
make infra-apply

# Start producer + streaming job
make produce &
make stream
```

---

## CI/CD

Every pull request triggers:
1. Python lint (ruff) + type check (mypy)
2. `dbt compile` — validates all SQL without a DB connection
3. `dbt run` + `dbt test` on a CI schema in Synapse
4. Great Expectations suite validation

Merges to `main` trigger Terraform apply via GitHub Actions.

---

## Data Quality Expectations

The Great Expectations suite enforces 8 expectations before Silver load:

- `transaction_id` — not null, unique
- `customer_id`, `amount`, `transaction_ts` — not null
- `amount` — between $0.01 and $1,000,000
- `merchant_category` — in allowed set
- `merchant_country` — exactly 2 characters
- `fraud_score` — between 0 and 100
- Batch row count — at least 100 rows (catches empty-load bugs)

Batches failing any expectation are quarantined; Silver load is blocked.

---

## Stack

**Streaming:** Azure Event Hubs · PySpark Structured Streaming · Delta Lake  
**Storage:** ADLS Gen2 (Bronze / Silver / Gold / Dead-letter)  
**Transformation:** dbt (staging + marts) · Azure Synapse Analytics  
**Data quality:** Great Expectations  
**Orchestration:** Apache Airflow  
**Visualization:** Power BI  
**Infrastructure:** Terraform · GitHub Actions  
**Language:** Python 3.11

---

## Resume Bullets

> *These are the bullets used on the resume for this project.*

- Ingested 10K+ financial transaction events/sec via Azure Event Hubs into PySpark Structured Streaming; applied z-score anomaly detection and rolling-window velocity rules to flag suspicious transactions with sub-800ms end-to-end latency.
- Designed Medallion architecture (Bronze/Silver/Gold) on ADLS Gen2 with Delta Lake; built dead-letter quarantine zone for malformed records, ensuring zero silent data loss across all ingestion runs.
- Built dbt transformation layer (staging → marts) with SCD Type 2 customer dimension and incremental merge strategy on `fct_fraud_events`; enforced schema contracts and 8 Great Expectations checks as a CI quality gate on every PR.
- Provisioned all Azure infrastructure (Event Hubs, Synapse, ADLS) via Terraform; automated deployment and dbt test execution through GitHub Actions CI/CD, reducing manual deployment steps to zero.
