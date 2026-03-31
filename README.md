# SDG Progress Tracker — Hybrid Data Pipeline

![GitHub Actions](https://github.com/theedataengineer/sdg-progress-pipeline/actions/workflows/dbt_ci.yml/badge.svg)

A production-grade hybrid streaming + batch ELT pipeline tracking
African countries' progress towards the UN 2030 Sustainable Development Goals.
Built with real UN data from the World Bank Open Data API across
54 African countries and 35 SDG indicators.

---

## Pipeline Architecture
```
World Bank API (35 SDG indicators, 54 African countries)
          ↓
  Apache Kafka (Streaming Layer)
  ├── Producer: polls API every 6 hours
  └── Consumer: batches messages every 5 minutes
          ↓
  MinIO (S3-compatible Data Lake)
  └── Partitioned JSON: streaming/kafka-batches/YYYY/MM/DD/
          ↓
  Apache Airflow (Orchestration — 4 DAGs)
  ├── kafka_producer_dag    every 6 hours
  ├── kafka_consumer_dag    every 6h + 30min offset
  ├── batch_ingestion_dag   Sunday 11 PM
  └── dbt_transform_dag     Monday 2 AM
          ↓
  PostgreSQL (Data Warehouse)
  └── 5 raw tables → 26,192 SDG records loaded
          ↓
  dbt Core (3-Layer Transformation)
  ├── Staging    (4 views)   — clean and standardise
  ├── Intermediate (4 views) — join, calculate, score
  └── Marts      (7 tables)  — analytics-ready
          ↓
  Apache Superset (SDG Africa 2030 Tracker Dashboard)
          ↓
  GitHub Actions (CI/CD — dbt tests on every push)
```

---

## Technology Stack

| Tool | Version | Role |
|---|---|---|
| Apache Kafka | 7.5.0 | Real-time streaming of SDG indicator updates |
| Apache Zookeeper | 7.5.0 | Kafka cluster coordinator |
| MinIO | latest | Local S3-compatible data lake |
| Apache Airflow | 2.8.0 | Pipeline orchestration — 4 DAGs |
| PostgreSQL | 15 | Data warehouse |
| dbt Core | 1.11.7 | 3-layer data transformation |
| Apache Superset | 3.1.0 | SDG progress dashboard |
| GitHub Actions | — | CI/CD — dbt tests on every push |
| Docker Compose | v2 | Full local stack — one command startup |
| Python | 3.13 | Extraction, Kafka producer/consumer scripts |

---

## Key Metrics

| Metric | Value |
|---|---|
| SDG indicators tracked | 35 |
| African countries monitored | 54 |
| Total records in warehouse | 26,192 |
| Kafka topics | 5 (3 partitions each) |
| Airflow DAGs | 4 |
| dbt models | 15 (staging + intermediate + marts) |
| dbt tests | 28 (all passing) |
| CI pipeline duration | ~63 seconds |
| Data source last updated | February 2026 |

---

## Dashboard — SDG Africa 2030 Tracker

The Superset dashboard answers 12 analytics questions using real UN data:

1. Which African countries are on track to achieve SDG targets by 2030?
2. Which SDG goals show the most progress globally since 2015?
3. Which SDG goals are African countries falling furthest behind on?
4. How does Kenya's SDG progress compare to East African peers?
5. What is the year-over-year improvement rate per SDG goal in Africa?
6. Which countries have the largest gap to 2030 targets?
7. How does SDG progress correlate with income group?
8. Latest real-time SDG updates from the Kafka stream
9. Which indicators have the most missing data for Africa?
10. If trends continue, which SDGs will Africa achieve by 2030?
11. How has SDG 3 (Good Health) changed since COVID-19 in 2020?
12. Which countries showed the fastest SDG improvement since 2015?

**Key findings from the data:**
- SDG 4 (Quality Education) is Africa's best performing goal — avg score 65.7
- SDG 5 (Gender Equality) is the worst performing — avg score 17.9
- Rwanda leads East Africa on Gender Equality (score 100.0)
- Kenya leads East Africa on Clean Energy access (score 59.2)
- Life expectancy in Kenya dropped from 62.9 (2019) to 61.2 (2021) — COVID impact visible
- 51 country-goal combinations confirmed on track for 2030

---

## Quick Start

### Prerequisites
- Docker and Docker Compose v2
- Python 3.8+
- Git
- 15 GB free disk space
- 6 GB RAM minimum

### 1. Clone and configure
```bash
git clone https://github.com/theedataengineer/sdg-progress-pipeline.git
cd sdg-progress-pipeline
cp .env.example .env
```

### 2. Start the full stack
```bash
docker compose up -d zookeeper postgres minio
sleep 20
docker compose up -d kafka
sleep 25
docker compose up -d minio-setup kafka-ui airflow-webserver airflow-scheduler superset
```

### 3. Verify services
```bash
docker compose ps
# All services should show healthy or running
```

### 4. Access the UIs
| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8090 | none |
| MinIO | http://localhost:9001 | minioadmin / minioadmin123 |
| Superset | http://localhost:8088 | admin / admin |

### 5. Run the pipeline manually
```bash
# Test World Bank API connection
python3 ingestion/extract_world_bank.py test

# Run full data extraction
python3 ingestion/extract_world_bank.py full

# Test Kafka producer
python3 kafka/producer/wb_producer.py test

# Test Kafka consumer
python3 kafka/consumer/sdg_consumer.py test

# Run dbt transformations
cd dbt_project && dbt run && dbt test
```

---

## Project Structure
```
sdg-progress-pipeline/
│
├── ingestion/
│   └── extract_world_bank.py     # World Bank API extractor
│                                  # 35 indicators, 54 African countries
│                                  # batch mode + streaming mode
│
├── kafka/
│   ├── producer/
│   │   └── wb_producer.py        # Publishes to 4 Kafka topics
│   │                              # validates records, dead letter queue
│   └── consumer/
│       └── sdg_consumer.py       # Reads from Kafka, writes to MinIO
│                                  # 5-minute batch windows, offset commit
│
├── airflow/
│   └── dags/
│       ├── kafka_producer_dag.py  # every 6 hours
│       ├── kafka_consumer_dag.py  # every 6h + 30min
│       ├── batch_ingestion_dag.py # Sunday 11 PM
│       └── dbt_transform_dag.py  # Monday 2 AM
│
├── dbt_project/
│   └── models/
│       ├── staging/               # 4 views — clean raw data
│       │   ├── stg_wb_sdg_indicators.sql
│       │   ├── stg_kafka_sdg_stream.sql
│       │   ├── stg_sdg_goal_metadata.sql
│       │   └── stg_wb_country_metadata.sql
│       ├── intermediate/          # 4 views — join and calculate
│       │   ├── int_sdg_indicators_combined.sql
│       │   ├── int_year_over_year_progress.sql
│       │   ├── int_sdg_target_gaps.sql
│       │   └── int_africa_sdg_scores.sql
│       └── marts/                 # 7 tables — analytics ready
│           ├── fct_sdg_progress.sql
│           ├── fct_sdg_target_achievement.sql
│           ├── dim_countries.sql
│           ├── dim_sdg_goals.sql
│           ├── mart_africa_2030_tracker.sql
│           ├── mart_sdg_goal_summary.sql
│           └── mart_kenya_vs_peers.sql
│
├── .github/
│   └── workflows/
│       └── dbt_ci.yml            # CI/CD — runs on every push
│
├── docker-compose.yml            # entire stack definition
├── requirements.txt              # Python dependencies
├── .env                          # secrets (never committed)
└── README.md
```

---

## Data Pipeline Detail

### Streaming Layer (Kafka)

Five topics handle different streams of SDG data:

| Topic | Partitions | Purpose |
|---|---|---|
| sdg-indicators-raw | 3 | All incoming SDG updates |
| sdg-world-bank-raw | 3 | World Bank source stream |
| sdg-africa-indicators | 3 | Africa-filtered stream |
| sdg-validated-records | 3 | Records passing validation |
| sdg-failed-records | 1 | Dead letter queue |

The producer generates a deterministic `record_id` (MD5 of country + indicator + year)
enabling deduplication when streaming and batch data overlap.

### Data Lake Structure (MinIO → AWS S3)
```
sdg-progress-pipeline/
├── raw/world-bank/YYYY/MM/DD/     # batch extracts
└── streaming/kafka-batches/
    ├── all/YYYY/MM/DD/            # all records
    ├── africa/YYYY/MM/DD/         # Africa-filtered
    ├── validated/YYYY/MM/DD/      # passed validation
    └── world-bank/YYYY/MM/DD/     # source-specific
```

Date-partitioned paths are AWS Athena and Glue compatible —
swapping MinIO for AWS S3 requires changing 3 environment variables.

### dbt Model Lineage
```
raw_wb_sdg_indicators ──► stg_wb_sdg_indicators ──┐
raw_kafka_sdg_stream  ──► stg_kafka_sdg_stream  ──┤
raw_sdg_goal_metadata ──► stg_sdg_goal_metadata ──┤
raw_wb_country_metadata ► stg_wb_country_metadata ─┤
                                                    │
                          int_sdg_indicators_combined
                          int_year_over_year_progress
                          int_sdg_target_gaps ───────┤
                          int_africa_sdg_scores ─────┤
                                                    │
                          fct_sdg_progress ──────────┤
                          fct_sdg_target_achievement ┤
                          dim_countries ─────────────┤
                          dim_sdg_goals ─────────────┤
                          mart_africa_2030_tracker ──┤
                          mart_sdg_goal_summary ─────┤
                          mart_kenya_vs_peers ────────┘
```

### CI/CD Pipeline (GitHub Actions)

Every push to `main` or `develop` automatically:
1. Spins up a PostgreSQL service container
2. Seeds minimal test data
3. Installs dbt
4. Runs full 3-layer dbt pipeline
5. Runs all 28 dbt tests
6. Generates and uploads dbt docs as artifact

**Duration: ~63 seconds**

---

## AWS S3 Migration (Next Step)

To move from MinIO to AWS S3, change three variables in `.env`:
```bash
MINIO_ENDPOINT=s3.amazonaws.com
MINIO_ACCESS_KEY=your-aws-access-key-id
MINIO_SECRET_KEY=your-aws-secret-access-key
```

Remove `endpoint_url` from boto3 client calls. Everything else — bucket names,
key paths, put_object, get_object — stays identical. This was by design.

---

## Architecture Decisions

### ADR-001: UNDESA API Replaced with World Bank Extended Coverage

**Date:** March 2026
**Status:** Accepted

**Context:**
The UNDESA SDG Indicators API (`unstats.un.org/sdgs/api/v1`) returned
HTTP 404 on all endpoints. Verbose curl investigation confirmed the server
was reachable (TLS handshake succeeded, valid UN certificate) but the API
path had changed with no public redirect or migration notice.

**Decision:**
Extended World Bank API coverage to include all SDG indicator series
previously sourced from UNDESA. The World Bank API covers 93% of official
SDG indicators with higher update frequency and more reliable uptime.

**Consequences:**
- All 12 dashboard analytics questions remain answerable ✅
- Kafka streaming layer gains higher-frequency updates ✅
- Pipeline complexity reduced — one source schema vs two ✅
- UNDESA re-integration planned when new API endpoint is confirmed

---

## What This Project Demonstrates

| Skill | Implementation |
|---|---|
| Streaming pipelines | Kafka producer/consumer with dead letter queue |
| Batch processing | Airflow DAGs with retry logic and XCom |
| Cloud storage | MinIO (S3-compatible) with date partitioning |
| Data modeling | dbt 3-layer architecture with 28 tests |
| Data quality | Great Expectations-style validation in Airflow |
| Orchestration | 4 DAGs with dependency-aware scheduling |
| CI/CD | GitHub Actions running full pipeline in 63s |
| Containerisation | Docker Compose with healthchecks and ordering |
| Real data | 26,192 records from World Bank Open Data API |
| Analytics | 12 questions answered on UN SDG progress |

---

## Author

Built as part of a data engineering portfolio.
Real UN SDG data. Production-grade architecture. Zero cloud cost during development.
