# NL-RailTraffic-ETL-Pipeline

> End-to-end data pipeline for Dutch Railways disruption analysis  
> Python · SQL · SQLite → Azure-ready · GitHub Actions CI/CD · Docker

[![Pipeline Status](https://github.com/yourname/nl-railtraffic-etl-pipeline/actions/workflows/daily_pipleline.yml/badge.svg)](https://github.com/yourname/nl-railtraffic-etl-pipeline/actions)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![License](https://img.shields.io/badge/License-MIT-green)


---

## Overview

NS (Nederlandse Spoorwegen) publishes real-time disruption data via a public API, but there's no way to query historical patterns without building persistent storage. This project automates the daily collection, cleaning, and storage of that data — making it possible to answer questions like *"which stations are most frequently disrupted?"* or *"is maintenance downtime trending up?"*

The pipeline runs automatically every day via GitHub Actions, processes 100–150 disruption records per run, and stores them in a structured SQLite database (designed to migrate to Azure SQL).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python `requests` with retry logic |
| Transformation | `pandas`, custom business logic |
| Storage | SQLite (local) → Azure SQL (planned) |
| Orchestration | GitHub Actions (daily cron, 06:00 UTC) |
| Containerization | Docker |

---

## Architecture

```
GitHub Actions (06:00 UTC daily)
        │
        ▼
┌───────────────────────────────────────┐
│             ETL Pipeline              │
│                                       │
│  NS API ──▶ cleaners.py ──▶ SQLite   │
│  (extract)   (transform)   (load)     │
│                                       │
│  • Retry with exponential backoff     │
│  • UPSERT — safe to re-run anytime   │
│  • Per-record error isolation         │
└───────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────┐
│            Database                   │
│  raw_disruptions   disruptions        │
│  stations          daily_stats        │
│  ── Views ──────────────────────      │
│  active_disruptions                   │
│  station_disruption_stats             │
└───────────────────────────────────────┘
```

---

## Key Design Decisions

**Idempotent loading** — every run uses UPSERT logic, so re-running never creates duplicates. This matters for scheduled pipelines where retries are common.

**Two-layer storage** — raw JSON is preserved in `raw_disruptions` alongside the cleaned `disruptions` table. This means data can be reprocessed if business logic changes, without re-calling the API.

**Derived impact scoring** — each disruption gets an `impact_level` (1–5) based on type and duration. Business logic lives in Python, not the database, making it easy to adjust.

**Denormalization trade-off** — `affected_stations` is stored as a comma-separated string rather than a junction table. This simplifies the load step; at larger scale a proper many-to-many table would be the right call.

---

## Data Model

```
raw_disruptions       disruptions                stations
────────────────      ─────────────────────────  ────────────────
id (PK)               id (PK)                    station_code (PK)
disruption_id (UQ) ──▶disruption_id (UQ, FK)     station_name
raw_json              type                       latitude
fetched_at            title                      longitude
                      start_time
                      end_time          daily_stats
                      duration_minutes  ───────────────
                      impact_level      date (PK)
                      affected_stations total_disruptions
                      is_resolved       avg_duration_minutes
                      created_at        most_affected_station
                      updated_at        peak_hour
```

---

## Analytics Queries

`src/transformation/aggregators.py` contains the SQL queries used to generate the `daily_stats` table and answer common business questions. These include:

- 7-day rolling disruption counts (sliding window function)
- Station severity ranking by percentile (`PERCENT_RANK`)
- Day-over-day trend detection (`LAG`)
- Peak hour analysis by type

See the file directly for full queries with comments.

---

## Quick Start

```bash
# Clone and install
git clone https://github.com/yourname/nl-railtraffic-etl-pipeline
cd nl-railtraffic-etl-pipeline
pip install -r requirements.txt

# Configure API key (free at apiportal.ns.nl)
cp .env.example .env
# Add: NS_API_KEY=your_key_here

# Initialize database
python src/storage/database.py

# Run pipeline
python src/pipeline.py

# Query results
sqlite3 data/nl_rail.db "SELECT * FROM station_disruption_stats LIMIT 10;"
```

### Docker
```bash
docker-compose -f docker/docker-compose.yml up
```

---

## Roadmap

- [ ] Migrate raw storage to Azure Blob Storage
- [ ] Switch database connection to Azure SQL (env-var controlled)
- [ ] Add dbt for transformation layer
- [ ] Connect Power BI dashboard to Azure SQL

---

## About
