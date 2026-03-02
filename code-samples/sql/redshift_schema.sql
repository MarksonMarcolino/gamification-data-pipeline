-- ============================================================================
-- Redshift Analytical Schema: Streaks & Bestie Points
-- ============================================================================
-- This schema supports the analytical layer for the gamification system.
-- Data flows from DynamoDB → S3 (via Glue) → Redshift (via COPY).
-- ============================================================================

-- Schema for gamification analytics
CREATE SCHEMA IF NOT EXISTS gamification;


-- ────────────────────────────────────────────────────────────────────────────
-- Fact: BP Events (point-earning interactions)
-- ────────────────────────────────────────────────────────────────────────────
-- Partitioned by event_date using DISTKEY on user_id for join performance.
-- SORTKEY on event_date enables efficient range scans for time-based queries.

CREATE TABLE IF NOT EXISTS gamification.fact_bp_events (
    event_id        VARCHAR(64)     NOT NULL,
    user_id         VARCHAR(64)     NOT NULL    DISTKEY,
    event_type      VARCHAR(32)     NOT NULL,
    points_awarded  INTEGER         NOT NULL,
    source          VARCHAR(32),
    idempotency_key VARCHAR(128)    NOT NULL,
    client_timestamp TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    event_date      DATE            NOT NULL,
    country         VARCHAR(8),
    user_segment    VARCHAR(32),
    loaded_at       TIMESTAMP       DEFAULT GETDATE()
)
SORTKEY (event_date, user_id);


-- ────────────────────────────────────────────────────────────────────────────
-- Fact: Daily User Summaries (one row per user per day)
-- ────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gamification.fact_daily_summaries (
    user_id             VARCHAR(64)     NOT NULL    DISTKEY,
    event_date          DATE            NOT NULL,
    total_events        INTEGER         NOT NULL,
    total_points        INTEGER         NOT NULL,
    distinct_event_types INTEGER,
    event_types         VARCHAR(256),
    first_event_at      TIMESTAMP WITH TIME ZONE,
    last_event_at       TIMESTAMP WITH TIME ZONE,
    country             VARCHAR(8),
    user_segment        VARCHAR(32),
    loaded_at           TIMESTAMP       DEFAULT GETDATE(),
    PRIMARY KEY (user_id, event_date)
)
SORTKEY (event_date, user_id);


-- ────────────────────────────────────────────────────────────────────────────
-- Dimension: User Streaks Snapshot (daily snapshot from DynamoDB)
-- ────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gamification.dim_user_streaks (
    user_id             VARCHAR(64)     NOT NULL    DISTKEY,
    snapshot_date       DATE            NOT NULL,
    current_streak      INTEGER,
    longest_streak      INTEGER,
    total_bestie_points INTEGER,
    last_activity_date  DATE,
    timezone            VARCHAR(64),
    loaded_at           TIMESTAMP       DEFAULT GETDATE(),
    PRIMARY KEY (user_id, snapshot_date)
)
SORTKEY (snapshot_date, user_id);


-- ────────────────────────────────────────────────────────────────────────────
-- Staging: temp table for incremental COPY loads
-- ────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gamification.stg_bp_events (LIKE gamification.fact_bp_events);


-- ============================================================================
-- Analytical Views
-- ============================================================================

-- Weekly active users with streaks
CREATE OR REPLACE VIEW gamification.v_weekly_streak_stats AS
SELECT
    DATE_TRUNC('week', event_date)  AS week_start,
    country,
    COUNT(DISTINCT user_id)         AS active_users,
    AVG(total_points)               AS avg_daily_points,
    AVG(total_events)               AS avg_daily_events
FROM gamification.fact_daily_summaries
GROUP BY 1, 2;


-- Streak distribution (how many users at each streak length)
CREATE OR REPLACE VIEW gamification.v_streak_distribution AS
SELECT
    snapshot_date,
    CASE
        WHEN current_streak = 0       THEN '0 (inactive)'
        WHEN current_streak BETWEEN 1 AND 3   THEN '1-3 days'
        WHEN current_streak BETWEEN 4 AND 7   THEN '4-7 days'
        WHEN current_streak BETWEEN 8 AND 14  THEN '8-14 days'
        WHEN current_streak BETWEEN 15 AND 30 THEN '15-30 days'
        ELSE '30+ days'
    END AS streak_bucket,
    COUNT(*)                AS user_count,
    AVG(total_bestie_points) AS avg_bp
FROM gamification.dim_user_streaks
GROUP BY 1, 2;


-- Top event types by points generated
CREATE OR REPLACE VIEW gamification.v_event_type_performance AS
SELECT
    event_date,
    event_type,
    COUNT(*)            AS event_count,
    SUM(points_awarded) AS total_points,
    COUNT(DISTINCT user_id) AS unique_users
FROM gamification.fact_bp_events
GROUP BY 1, 2;


-- ============================================================================
-- COPY command template (run after each Glue job)
-- ============================================================================
/*
COPY gamification.stg_bp_events
FROM 's3://luzia-data-lake/curated/bp_events/event_date=2025-03-15/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftCopyRole'
FORMAT AS PARQUET;

-- Merge from staging to fact (upsert on idempotency_key)
BEGIN TRANSACTION;

DELETE FROM gamification.fact_bp_events
USING gamification.stg_bp_events
WHERE fact_bp_events.idempotency_key = stg_bp_events.idempotency_key;

INSERT INTO gamification.fact_bp_events
SELECT * FROM gamification.stg_bp_events;

TRUNCATE gamification.stg_bp_events;

END TRANSACTION;
*/
