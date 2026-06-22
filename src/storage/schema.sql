-- ============================================
-- NS Rail Disruptions Database Schema
-- PostgreSQL (AWS RDS) Version
-- ============================================

-- Table 1: Raw data archive
CREATE TABLE IF NOT EXISTS raw_disruptions (
    id               SERIAL PRIMARY KEY,
    disruption_id    VARCHAR(100) NOT NULL UNIQUE,
    raw_json         TEXT NOT NULL,
    fetched_at       TIMESTAMP DEFAULT NOW()
);

-- Table 2: Cleaned disruptions
CREATE TABLE IF NOT EXISTS disruptions (
    id                 SERIAL PRIMARY KEY,
    disruption_id      VARCHAR(100) NOT NULL UNIQUE,

    type               VARCHAR(50) NOT NULL,
    title              VARCHAR(500),
    description        TEXT,

    start_time         TIMESTAMP,
    end_time           TIMESTAMP,
    duration_minutes   FLOAT,

    impact_level       INTEGER CHECK (impact_level BETWEEN 1 AND 5),
    affected_stations  VARCHAR(500),

    is_resolved        BOOLEAN DEFAULT FALSE,
    created_at         TIMESTAMP DEFAULT NOW(),
    updated_at         TIMESTAMP DEFAULT NOW(),

    FOREIGN KEY (disruption_id) REFERENCES raw_disruptions(disruption_id)
);

-- Table 3: Station master data
CREATE TABLE IF NOT EXISTS stations (
    station_code   VARCHAR(10) PRIMARY KEY,
    station_name   VARCHAR(200) NOT NULL,
    country        VARCHAR(10) DEFAULT 'NL',
    latitude       FLOAT,
    longitude      FLOAT,
    last_updated   TIMESTAMP DEFAULT NOW()
);

-- Table 4: Daily stats
CREATE TABLE IF NOT EXISTS daily_stats (
    date                    DATE PRIMARY KEY,
    total_disruptions       INTEGER DEFAULT 0,
    total_cancellations     INTEGER DEFAULT 0,
    avg_duration_minutes    FLOAT,
    max_duration_minutes    INTEGER,
    most_affected_station   VARCHAR(10),
    peak_hour               INTEGER,
    calculated_at           TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- Indexes
-- ============================================
CREATE INDEX IF NOT EXISTS idx_raw_fetched_at
    ON raw_disruptions(fetched_at);

CREATE INDEX IF NOT EXISTS idx_disruptions_type_resolved
    ON disruptions(type, is_resolved);

CREATE INDEX IF NOT EXISTS idx_disruptions_start_time
    ON disruptions(start_time);

CREATE INDEX IF NOT EXISTS idx_disruptions_impact
    ON disruptions(impact_level);

-- ============================================
-- Seed station data
-- INSERT ... ON CONFLICT DO NOTHING is PostgreSQL's
-- equivalent of SQLite's INSERT OR IGNORE
-- ============================================
INSERT INTO stations (station_code, station_name, latitude, longitude) VALUES
    ('ASD',  'Amsterdam Centraal', 52.3791, 4.9003),
    ('UTR',  'Utrecht Centraal',   52.0894, 5.1101),
    ('RTD',  'Rotterdam Centraal', 51.9249, 4.4690),
    ('EHV',  'Eindhoven Centraal', 51.4433, 5.4814),
    ('GVC',  'Den Haag Centraal',  52.0808, 4.3247),
    ('LEDN', 'Leiden Centraal',    52.1664, 4.4817)
ON CONFLICT (station_code) DO NOTHING;