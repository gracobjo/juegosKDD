-- cassandra_schema.cql — DDL completo para el Serving Layer
-- Aplicar (recomendado si cqlsh del tarball falla en Python 3.12):
--   source venv/bin/activate && python3 0_infra/apply_cassandra_schema.py
-- Alternativa: cqlsh -f cassandra_schema.cql

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

-- ═══════════════════════════════════════════════════════════════════════════
-- Keyspace gaming_recommender — Recomendador híbrido (ALS + TF-IDF + Kafka)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE KEYSPACE IF NOT EXISTS gaming_recommender
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE gaming_recommender;

CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id             TEXT,
    recommended_game    TEXT,
    hybrid_score        DOUBLE,
    cf_component        DOUBLE,
    cb_component        DOUBLE,
    recommendation_rank INT,
    alpha_used          DOUBLE,
    source              TEXT,
    generated_at        TIMESTAMP,
    PRIMARY KEY (user_id, hybrid_score, recommended_game)
) WITH CLUSTERING ORDER BY (hybrid_score DESC)
  AND default_time_to_live = 86400;

CREATE TABLE IF NOT EXISTS game_similarity (
    game_a      TEXT,
    game_b      TEXT,
    similarity  DOUBLE,
    PRIMARY KEY (game_a, similarity, game_b)
) WITH CLUSTERING ORDER BY (similarity DESC);

CREATE TABLE IF NOT EXISTS model_metrics (
    run_id           UUID,
    evaluated_at     TIMESTAMP,
    dt               TEXT,
    rmse             DOUBLE,
    precision_at_k   DOUBLE,
    recall_at_k      DOUBLE,
    k                INT,
    alpha            DOUBLE,
    users_with_recs  BIGINT,
    model_path       TEXT,
    PRIMARY KEY (run_id, evaluated_at)
) WITH CLUSTERING ORDER BY (evaluated_at DESC);

CREATE TABLE IF NOT EXISTS user_history_cache (
    user_id      TEXT,
    game_title   TEXT,
    hours_played DOUBLE,
    last_played  TIMESTAMP,
    PRIMARY KEY (user_id, hours_played, game_title)
) WITH CLUSTERING ORDER BY (hours_played DESC)
  AND default_time_to_live = 604800;

CREATE TABLE IF NOT EXISTS popular_games_fallback (
    recommended_game TEXT,
    hybrid_score     DOUBLE,
    PRIMARY KEY (recommended_game)
);

CREATE TABLE IF NOT EXISTS player_windows (
    user_id          TEXT,
    window_start     TIMESTAMP,
    events_in_batch  BIGINT,
    avg_engagement   DOUBLE,
    avg_hours        DOUBLE,
    PRIMARY KEY (user_id, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 604800;

CREATE TABLE IF NOT EXISTS kdd_insights (
    insight_id   UUID,
    created_at   TIMESTAMP,
    insight_type TEXT,
    severity     TEXT,
    message      TEXT,
    layer        TEXT,
    PRIMARY KEY (insight_id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC)
  AND default_time_to_live = 2592000;

CREATE INDEX IF NOT EXISTS ON user_recommendations (recommended_game);
CREATE INDEX IF NOT EXISTS ON kdd_insights (severity);
