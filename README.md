# SDG Progress Tracker — Hybrid Data Pipeline

A production-grade hybrid streaming + batch ELT pipeline tracking
African countries' progress towards the UN 2030 Sustainable Development Goals.

## Architecture
```
World Bank API → Kafka → MinIO → Airflow → PostgreSQL → dbt → Superset
```

## Stack

| Tool | Role |
|---|---|
| Apache Kafka | Real-time streaming of SDG indicator updates |
| MinIO | Local S3-compatible data lake (swap to AWS S3 later) |
| Apache Airflow | Pipeline orchestration — 4 DAGs |
| PostgreSQL | Data warehouse |
| dbt Core | 3-layer data transformations |
| Great Expectations | Data quality validation |
| Apache Superset | SDG progress dashboard |
| GitHub Actions | CI/CD — dbt tests on every push |
| Docker Compose | Full local stack |

## Quick Start
```bash
cp .env.example .env          # configure environment
docker compose up -d          # start all services
python ingestion/extract_world_bank.py   # test API connection
```

## Project Status

- [ ] Step 1: Prerequisites & project scaffold
- [ ] Step 2: Docker Compose — full stack
- [ ] Step 3: Kafka topics setup
- [ ] Step 4: Ingestion scripts
- [ ] Step 5: Kafka producer & consumer
- [ ] Step 6: Airflow DAGs
- [ ] Step 7: PostgreSQL raw tables
- [ ] Step 8: Great Expectations suites
- [ ] Step 9: dbt models (staging → intermediate → marts)
- [ ] Step 10: Superset dashboard
- [ ] Step 11: GitHub Actions CI/CD

## Data Sources

- **World Bank Open Data**: `https://api.worldbank.org/v2/`

## Focus Region

Primary: African countries (54 countries) — benchmarked against global averages.




## Architecture Decisions

### ADR-001: UNDESA API Replaced with World Bank Extended Coverage

**Date:** March 2026
**Status:** Accepted

**Context:**
The UNDESA SDG Indicators API (unstats.un.org/sdgs/api/v1) 
returned HTTP 404 on all endpoints. Investigation confirmed 
the API migrated to a new path with no public redirect.

**Decision:**
Extended World Bank API coverage to include all SDG indicator
series previously sourced from UNDESA. The World Bank API 
covers 93% of official SDG indicators with higher update 
frequency and more reliable uptime.

**Consequences:**
- All 12 dashboard analytics questions remain answerable
- Kafka streaming layer gains higher-frequency updates
- Pipeline complexity reduced (one source schema vs two)
- UNDESA re-integration planned when new API endpoint confirmed
