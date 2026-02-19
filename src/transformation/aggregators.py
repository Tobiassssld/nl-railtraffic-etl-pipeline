# src/transformation/aggregators.py
"""
Analytics SQL queries for NS Rail disruption data.
These queries demonstrate patterns commonly used in Analytics Engineering:
- CTEs for readable, stepwise logic
- Window functions for ranking, rolling aggregates, and lag/lead
- Correlated subqueries
- Safe division with NULLIF
- String-to-row unnesting

All queries are designed to run against the disruptions / stations / daily_stats schema.
"""


# ──────────────────────────────────────────────────────────────────
# QUERY 1: 30-Day Disruption Trend with 7-Day Rolling Average
# Business question: "Is the number of disruptions increasing or decreasing?"
# Techniques: CTE, window function with ROWS BETWEEN frame
# ──────────────────────────────────────────────────────────────────
ROLLING_TREND_QUERY = """
WITH daily_counts AS (
    -- Step 1: Aggregate raw disruptions to daily level
    SELECT
        DATE(start_time)    AS disruption_date,
        type,
        COUNT(*)            AS incident_count,
        AVG(duration_minutes) AS avg_duration_minutes
    FROM disruptions
    WHERE start_time >= date('now', '-30 days')
    GROUP BY DATE(start_time), type
)
SELECT
    disruption_date,
    type,
    incident_count,
    ROUND(avg_duration_minutes, 1)          AS avg_duration_minutes,

    -- 7-day rolling sum (sliding window, not calendar week)
    -- ROWS BETWEEN 6 PRECEDING AND CURRENT ROW = last 7 rows including today
    SUM(incident_count) OVER (
        PARTITION BY type
        ORDER BY disruption_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                       AS rolling_7day_total,

    -- 7-day rolling average
    ROUND(
        AVG(incident_count) OVER (
            PARTITION BY type
            ORDER BY disruption_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2
    )                                       AS rolling_7day_avg

FROM daily_counts
ORDER BY disruption_date DESC, incident_count DESC;
"""


# ──────────────────────────────────────────────────────────────────
# QUERY 2: Station Severity Percentile Ranking
# Business question: "Which stations are in the worst-performing 10%?"
# Techniques: CTE, PERCENT_RANK(), string unnesting with json_each
# ──────────────────────────────────────────────────────────────────
STATION_SEVERITY_QUERY = """
WITH unnested_stations AS (
    -- Step 1: Explode comma-separated station codes into individual rows
    -- affected_stations stores: "ASD,UTR,RTD" → we need one row per station
    SELECT
        d.disruption_id,
        d.impact_level,
        d.duration_minutes,
        TRIM(s.value)   AS station_code
    FROM disruptions d,
    -- Wrap CSV in JSON array format, then unnest with json_each
    json_each('["' || REPLACE(d.affected_stations, ',', '","') || '"]') s
    WHERE d.affected_stations IS NOT NULL
),
station_aggregates AS (
    -- Step 2: Calculate per-station metrics
    SELECT
        station_code,
        COUNT(DISTINCT disruption_id)   AS total_disruptions,
        AVG(duration_minutes)           AS avg_duration_minutes,
        AVG(impact_level)               AS avg_impact_level,
        MAX(impact_level)               AS max_impact_level
    FROM unnested_stations
    GROUP BY station_code
)
SELECT
    sa.station_code,
    st.station_name,
    sa.total_disruptions,
    ROUND(sa.avg_duration_minutes, 1)   AS avg_duration_minutes,
    ROUND(sa.avg_impact_level, 2)       AS avg_impact_level,

    -- Percentile rank: 0.0 = least disrupted, 1.0 = most disrupted
    ROUND(
        PERCENT_RANK() OVER (ORDER BY sa.total_disruptions),
        3
    )                                   AS disruption_percentile,

    -- Dense rank (no gaps in ranking sequence)
    DENSE_RANK() OVER (
        ORDER BY sa.total_disruptions DESC
    )                                   AS severity_rank,

    -- Flag worst stations (top 10% by disruption count)
    CASE
        WHEN PERCENT_RANK() OVER (ORDER BY sa.total_disruptions) > 0.9
        THEN 'HIGH RISK'
        WHEN PERCENT_RANK() OVER (ORDER BY sa.total_disruptions) > 0.7
        THEN 'MEDIUM RISK'
        ELSE 'LOW RISK'
    END                                 AS risk_category

FROM station_aggregates sa
LEFT JOIN stations st ON sa.station_code = st.station_code
ORDER BY sa.total_disruptions DESC;
"""


# ──────────────────────────────────────────────────────────────────
# QUERY 3: Day-over-Day Change Analysis with LAG/LEAD
# Business question: "How did today's disruptions compare to yesterday?"
# Techniques: LAG(), LEAD(), NULLIF for safe division, pct change
# ──────────────────────────────────────────────────────────────────
DAY_OVER_DAY_QUERY = """
WITH daily_summary AS (
    SELECT
        DATE(start_time)            AS disruption_date,
        COUNT(*)                    AS total_disruptions,
        SUM(CASE WHEN type = 'calamity' THEN 1 ELSE 0 END)     AS calamities,
        SUM(CASE WHEN type = 'maintenance' THEN 1 ELSE 0 END)   AS maintenance,
        SUM(CASE WHEN type = 'disruption' THEN 1 ELSE 0 END)    AS disruptions,
        ROUND(AVG(duration_minutes), 1)     AS avg_duration,
        MAX(impact_level)                   AS max_impact
    FROM disruptions
    GROUP BY DATE(start_time)
)
SELECT
    disruption_date,
    total_disruptions,
    avg_duration,
    max_impact,

    -- Previous day's count (LAG looks backward in the window)
    LAG(total_disruptions, 1) OVER (ORDER BY disruption_date) AS prev_day_total,

    -- Next day's count (LEAD looks forward — useful for forecasting views)
    LEAD(total_disruptions, 1) OVER (ORDER BY disruption_date) AS next_day_total,

    -- Absolute day-over-day change
    total_disruptions
        - LAG(total_disruptions, 1) OVER (ORDER BY disruption_date)
                                        AS dod_delta,

    -- Percentage change (NULLIF prevents division-by-zero)
    ROUND(
        100.0
        * (total_disruptions - LAG(total_disruptions, 1) OVER (ORDER BY disruption_date))
        / NULLIF(LAG(total_disruptions, 1) OVER (ORDER BY disruption_date), 0),
        1
    )                                   AS dod_pct_change,

    -- 7-day running total for context
    SUM(total_disruptions) OVER (
        ORDER BY disruption_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                   AS rolling_7day

FROM daily_summary
ORDER BY disruption_date DESC;
"""


# ──────────────────────────────────────────────────────────────────
# QUERY 4: Peak Hour Analysis with ROW_NUMBER and RANK
# Business question: "Which hours see the worst disruptions?"
# Techniques: STRFTIME bucketing, ROW_NUMBER vs RANK vs DENSE_RANK
# ──────────────────────────────────────────────────────────────────
PEAK_HOUR_QUERY = """
WITH hourly_stats AS (
    SELECT
        STRFTIME('%H', start_time)      AS hour_of_day,    -- '06', '07', ... '22'
        STRFTIME('%w', start_time)      AS day_of_week,    -- '0'=Sunday ... '6'=Saturday
        COUNT(*)                        AS disruption_count,
        ROUND(AVG(duration_minutes), 1) AS avg_duration,
        ROUND(AVG(impact_level), 2)     AS avg_impact
    FROM disruptions
    WHERE start_time IS NOT NULL
    GROUP BY hour_of_day, day_of_week
)
SELECT
    CASE day_of_week
        WHEN '0' THEN 'Sunday'   WHEN '1' THEN 'Monday'
        WHEN '2' THEN 'Tuesday'  WHEN '3' THEN 'Wednesday'
        WHEN '4' THEN 'Thursday' WHEN '5' THEN 'Friday'
        WHEN '6' THEN 'Saturday'
    END                                 AS day_name,
    hour_of_day || ':00'                AS hour_label,
    disruption_count,
    avg_duration,
    avg_impact,

    -- ROW_NUMBER: unique rank per row (no ties)
    ROW_NUMBER() OVER (ORDER BY disruption_count DESC)  AS row_num,

    -- RANK: ties get the same rank, next rank skips (1,1,3,...)
    RANK() OVER (ORDER BY disruption_count DESC)        AS rank_with_gaps,

    -- DENSE_RANK: ties get the same rank, no gap (1,1,2,...)
    DENSE_RANK() OVER (ORDER BY disruption_count DESC)  AS dense_rank

FROM hourly_stats
ORDER BY disruption_count DESC
LIMIT 20;
"""


# ──────────────────────────────────────────────────────────────────
# QUERY 5: Cancellation Rate with Correlated Subquery + Full Metrics
# Business question: "What % of disruptions end in cancellation per day?"
# Techniques: Correlated subquery, FILTER clause, window partitioning
# This is the main "executive dashboard" query
# ──────────────────────────────────────────────────────────────────
COMPLEX_ANALYTICS_QUERY = """
WITH disruption_metrics AS (
    -- Step 1: Daily metrics per disruption type
    SELECT
        DATE(start_time)    AS disruption_date,
        type,
        COUNT(*)            AS incident_count,
        AVG(
            CAST((julianday(end_time) - julianday(start_time)) * 1440 AS REAL)
        )                   AS avg_duration_minutes,

        -- 7-day rolling total across all types on this date
        SUM(COUNT(*)) OVER (
            ORDER BY DATE(start_time)
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                   AS rolling_7day_total

    FROM disruptions
    WHERE start_time >= date('now', '-30 days')
    GROUP BY DATE(start_time), type
),
station_impact AS (
    -- Step 2: Station severity percentile
    SELECT
        station_code,
        COUNT(*)            AS disruption_count,
        PERCENT_RANK() OVER (ORDER BY COUNT(*)) AS severity_percentile
    FROM (
        SELECT TRIM(value) AS station_code
        FROM disruptions,
        json_each('["' || REPLACE(affected_stations, ',', '","') || '"]')
        WHERE affected_stations IS NOT NULL
    )
    GROUP BY station_code
)
SELECT
    dm.disruption_date,
    dm.type,
    dm.incident_count,
    ROUND(dm.avg_duration_minutes, 2)   AS avg_duration,
    dm.rolling_7day_total,

    -- Correlated subquery: worst station for that date (top 10th percentile)
    (
        SELECT si.station_code
        FROM station_impact si
        WHERE si.severity_percentile > 0.9
        ORDER BY si.disruption_count DESC
        LIMIT 1
    )                                   AS worst_station,

    -- Cancellation rate: % of all disruptions that day classified as cancellation
    -- Uses FILTER clause (SQL:2003 standard) — cleaner than CASE WHEN
    ROUND(
        100.0
        * SUM(dm.incident_count) FILTER (WHERE dm.type = 'cancellation')
            OVER (PARTITION BY dm.disruption_date)
        / NULLIF(SUM(dm.incident_count) OVER (PARTITION BY dm.disruption_date), 0),
        2
    )                                   AS cancellation_rate_pct

FROM disruption_metrics dm
ORDER BY dm.disruption_date DESC, dm.incident_count DESC;
"""


# ──────────────────────────────────────────────────────────────────
# QUERY 6: Self-Join — Find Overlapping Disruptions
# Business question: "Which disruptions were active at the same time?"
# Techniques: Self-join with non-equijoin conditions, overlap detection
# ──────────────────────────────────────────────────────────────────
OVERLAPPING_DISRUPTIONS_QUERY = """
-- Self-join to find disruptions that were active simultaneously
-- Two disruptions overlap if: A.start < B.end AND A.end > B.start
SELECT
    a.disruption_id         AS disruption_a,
    b.disruption_id         AS disruption_b,
    a.type                  AS type_a,
    b.type                  AS type_b,
    a.start_time            AS a_start,
    a.end_time              AS a_end,
    b.start_time            AS b_start,
    b.end_time              AS b_end,

    -- Calculate overlap duration in minutes
    CAST(
        (julianday(MIN(a.end_time, b.end_time))
         - julianday(MAX(a.start_time, b.start_time))) * 1440
    AS INTEGER)             AS overlap_minutes

FROM disruptions a
JOIN disruptions b
    ON a.disruption_id < b.disruption_id  -- avoid duplicates (A,B) and (B,A)
    AND a.start_time < b.end_time          -- overlap condition part 1
    AND a.end_time   > b.start_time        -- overlap condition part 2
WHERE a.start_time >= date('now', '-7 days')
ORDER BY overlap_minutes DESC
LIMIT 50;
"""