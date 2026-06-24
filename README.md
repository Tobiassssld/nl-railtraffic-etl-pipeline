# NL-RailTraffic-ETL-Pipeline

Automated pipeline that pulls NS disruption data daily, cleans it, and stores it — making historical analysis possible.

[![Pipeline Status](https://github.com/Tobiassssld/nl-railtraffic-etl-pipeline/actions/workflows/daily_pipeline.yml/badge.svg)](https://github.com/Tobiassssld/nl-railtraffic-etl-pipeline/actions)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![License](https://img.shields.io/badge/License-MIT-green)

---

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Python `requests` |
| Transformation | `pandas` |
| Raw Storage | AWS S3 |
| Database | AWS RDS PostgreSQL |
| Orchestration | GitHub Actions (daily, 06:00 UTC) |
| Containerization | Docker |

---

## How it works

```
GitHub Actions (daily)
        │
        ▼
  NS API → clean → load
        │
        ├── AWS S3          (raw JSON, year/month/day/)
        └── AWS RDS         (raw_disruptions, disruptions,
                             stations, daily_stats)
```

Each run fetches ~100–150 disruption records. Raw JSON is archived to S3 before any transformation, so data can be reprocessed without re-calling the API. Loads are idempotent — safe to re-run.

---

## Data model

```
raw_disruptions       disruptions              stations
───────────────       ───────────────────────  ─────────────
disruption_id (UQ) ──▶disruption_id (UQ, FK)  station_code
raw_json              type / title             station_name
fetched_at            start_time / end_time    lat / lon
                      duration_minutes
                      impact_level (1–5)       daily_stats
                      affected_stations        ───────────────
                      is_resolved              date (PK)
                                               total_disruptions
                                               avg_duration_minutes
```

Impact level is derived from disruption type and duration — logic lives in Python so it's easy to adjust.

---

## Quick start

```bash
git clone https://github.com/Tobiassssld/nl-railtraffic-etl-pipeline
cd nl-railtraffic-etl-pipeline
pip install -r requirements.txt

cp .env.example .env
# fill in: NS_API_KEY, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
#          AWS_RDS_HOST, AWS_RDS_PASSWORD, AWS_S3_BUCKET

python src/storage/database.py   # initialise schema
python src/pipeline.py
```

---

## Roadmap

- [x] NS API ingestion with retry logic
- [x] Cleaning + impact scoring
- [x] Raw JSON archive to AWS S3
- [x] Cleaned data to AWS RDS PostgreSQL
- [x] Daily automation via GitHub Actions
- [ ] dbt transformation layer
- [ ] Power BI dashboard
- [ ] LLM daily disruption digest
