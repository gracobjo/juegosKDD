-- cassandra_schema.cql — DDL completo para el Serving Layer
-- Ejecutar: cqlsh -f cassandra_schema.cql

CREATE KEYSPACE IF NOT EXISTS gaming_kdd
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;

USE gaming_kdd;

-- ── Speed Layer: ventanas de 5 minutos (escritura frecuente) ─────────────────
CREATE TABLE IF NOT EXISTS player_windows (
    game             TEXT,
    window_start     TIMESTAMP,
    window_end       TIMESTAMP,
    appid            TEXT,
    avg_players      DOUBLE,
    max_players      DOUBLE,
    min_players      DOUBLE,
    avg_review_score DOUBLE,
    avg_health_score DOUBLE,
    snapshots        BIGINT,
    player_tier      TEXT,
    PRIMARY KEY (game, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 604800;  -- TTL 7 días

-- ── Insights KDD (resultado de la fase Evaluation) ──────────────────────────
CREATE TABLE IF NOT EXISTS kdd_insights (
    insight_id    UUID,
    created_at    TIMESTAMP,
    game          TEXT,
    appid         TEXT,
    insight_type  TEXT,   -- 'player_surge','review_drop','peak','health_alert'
    message       TEXT,
    severity      TEXT,   -- 'info','warning','alert','critical'
    metric_value  DOUBLE,
    metric_name   TEXT,
    layer         TEXT,   -- 'speed','batch'
    PRIMARY KEY (insight_id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC)
  AND default_time_to_live = 2592000;  -- TTL 30 días

-- ── Stats diarios por juego (Batch Layer → Serving) ──────────────────────────
CREATE TABLE IF NOT EXISTS game_stats_daily (
    appid            TEXT,
    dt               TEXT,
    game             TEXT,
    genre            TEXT,
    avg_players      DOUBLE,
    max_players      DOUBLE,
    avg_review_score DOUBLE,
    avg_health_score DOUBLE,
    growth_rate      DOUBLE,
    player_rank      INT,
    total_snapshots  INT,
    PRIMARY KEY (appid, dt)
) WITH CLUSTERING ORDER BY (dt DESC);

-- ── Índices secundarios para queries del dashboard ───────────────────────────
CREATE INDEX IF NOT EXISTS ON kdd_insights   (game);
CREATE INDEX IF NOT EXISTS ON kdd_insights   (severity);
CREATE INDEX IF NOT EXISTS ON player_windows (appid);

-- ── Verificación ─────────────────────────────────────────────────────────────
-- DESCRIBE TABLES;
