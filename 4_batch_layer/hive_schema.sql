-- hive_schema.sql — DDL completo para el Batch Layer
-- Ejecutar: hive -f hive_schema.sql

CREATE DATABASE IF NOT EXISTS gaming_kdd
COMMENT 'KDD Gaming Analytics — Batch Layer'
LOCATION '/user/hive/warehouse/gaming_kdd';

USE gaming_kdd;

-- ── Tabla principal de snapshots (particionada por fecha) ─────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS player_snapshots (
    event_id           STRING,
    ts                 BIGINT,
    hour               INT,
    appid              STRING,
    game               STRING,
    genre              STRING,
    price_usd          DOUBLE,
    current_players    INT,
    player_tier        STRING,
    owners_range       STRING,
    average_playtime   INT,
    median_playtime    INT,
    positive_reviews   INT,
    negative_reviews   INT,
    review_score       DOUBLE,
    health_score       DOUBLE,
    total_achievements INT,
    source             STRING,
    pipeline_version   STRING
)
PARTITIONED BY (dt STRING COMMENT 'Fecha YYYY-MM-DD')
STORED AS PARQUET
LOCATION '/user/hive/warehouse/gaming_kdd/player_snapshots'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ── Tabla de agregados diarios (resultado KDD Batch) ─────────────────────────
CREATE TABLE IF NOT EXISTS kdd_daily_summary (
    appid            STRING,
    game             STRING,
    genre            STRING,
    avg_players      DOUBLE,
    max_players      INT,
    min_players      INT,
    stddev_players   DOUBLE,
    avg_review_score DOUBLE,
    avg_health_score DOUBLE,
    total_snapshots  INT,
    player_tier_mode STRING,
    growth_rate      DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ── Vista KDD para análisis exploratorio ─────────────────────────────────────
CREATE OR REPLACE VIEW kdd_analysis AS
SELECT
    dt,
    game,
    genre,
    avg_players,
    max_players,
    avg_review_score,
    avg_health_score,
    growth_rate,
    RANK() OVER (PARTITION BY dt ORDER BY avg_players DESC)      AS player_rank,
    RANK() OVER (PARTITION BY dt ORDER BY avg_review_score DESC) AS review_rank
FROM kdd_daily_summary;

-- ── Query de validación ───────────────────────────────────────────────────────
-- SELECT * FROM kdd_analysis WHERE dt = CURRENT_DATE ORDER BY player_rank LIMIT 10;
