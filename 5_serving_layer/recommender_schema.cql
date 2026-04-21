-- recommender_schema.cql - Serving Layer del sistema de recomendacion hibrido.
-- Ejecutar:  cqlsh -f 5_serving_layer/recommender_schema.cql
-- Keyspace separado para aislar del pipeline original gaming_kdd.

CREATE KEYSPACE IF NOT EXISTS gaming_recommender
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

USE gaming_recommender;

-- ============================================================================
-- user_recommendations: recomendaciones hibridas por usuario (top-K).
--   * clave primaria: (user_id, hybrid_score DESC, recommended_game)
--   * source: 'batch' | 'speed_layer'
--   * TTL 86400 para que solo vivan las ultimas ejecuciones
-- ============================================================================
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
) WITH CLUSTERING ORDER BY (hybrid_score DESC, recommended_game ASC)
  AND default_time_to_live = 86400;

-- ============================================================================
-- game_similarity: matriz TF-IDF content-based (pares game_a, game_b).
-- ============================================================================
CREATE TABLE IF NOT EXISTS game_similarity (
    game_a      TEXT,
    game_b      TEXT,
    similarity  DOUBLE,
    PRIMARY KEY (game_a, similarity, game_b)
) WITH CLUSTERING ORDER BY (similarity DESC, game_b ASC);

-- ============================================================================
-- model_metrics: RMSE / Precision@K por ejecucion del batch KDD.
-- Consulta tipica: SELECT * FROM model_metrics ORDER BY evaluated_at DESC;
-- ============================================================================
CREATE TABLE IF NOT EXISTS model_metrics (
    model_name      TEXT,
    evaluated_at    TIMESTAMP,
    run_id          UUID,
    dt              TEXT,
    rmse            DOUBLE,
    precision_at_k  DOUBLE,
    recall_at_k     DOUBLE,
    k               INT,
    alpha           DOUBLE,
    users_with_recs BIGINT,
    games_covered   BIGINT,
    model_path      TEXT,
    PRIMARY KEY (model_name, evaluated_at, run_id)
) WITH CLUSTERING ORDER BY (evaluated_at DESC, run_id ASC);

-- ============================================================================
-- user_history_cache: ultimos juegos jugados por usuario (alimenta a los agentes).
-- ============================================================================
CREATE TABLE IF NOT EXISTS user_history_cache (
    user_id      TEXT,
    game_title   TEXT,
    hours_played DOUBLE,
    last_played  TIMESTAMP,
    PRIMARY KEY (user_id, hours_played, game_title)
) WITH CLUSTERING ORDER BY (hours_played DESC, game_title ASC)
  AND default_time_to_live = 604800;

-- ============================================================================
-- popular_games_fallback: cold-start (top global por horas).
-- ============================================================================
CREATE TABLE IF NOT EXISTS popular_games_fallback (
    recommended_game TEXT PRIMARY KEY,
    hybrid_score     DOUBLE,
    total_hours      DOUBLE,
    n_players        BIGINT,
    generated_at     TIMESTAMP
);

-- ============================================================================
-- player_windows: ventanas agregadas del Speed Layer (recomendador).
-- No colisiona con gaming_kdd.player_windows porque esta en otro keyspace.
-- ============================================================================
CREATE TABLE IF NOT EXISTS player_windows (
    user_id         TEXT,
    window_start    TIMESTAMP,
    events_in_batch BIGINT,
    avg_engagement  DOUBLE,
    avg_hours       DOUBLE,
    PRIMARY KEY (user_id, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 604800;

-- ============================================================================
-- kdd_insights: alertas generadas por speed + batch en el recomendador.
-- ============================================================================
CREATE TABLE IF NOT EXISTS kdd_insights (
    insight_id   UUID,
    created_at   TIMESTAMP,
    insight_type TEXT,
    severity     TEXT,    -- info | warning | alert | critical
    message      TEXT,
    user_id      TEXT,
    game_title   TEXT,
    metric_name  TEXT,
    metric_value DOUBLE,
    layer        TEXT,    -- speed | batch
    PRIMARY KEY (insight_id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC)
  AND default_time_to_live = 2592000;

-- ============================================================================
-- agent_explanations: cache de respuestas generadas por los agentes Claude.
-- ============================================================================
CREATE TABLE IF NOT EXISTS agent_explanations (
    user_id       TEXT,
    game_title    TEXT,
    generated_at  TIMESTAMP,
    explanation   TEXT,
    agent_name    TEXT,
    PRIMARY KEY ((user_id, game_title), generated_at)
) WITH CLUSTERING ORDER BY (generated_at DESC)
  AND default_time_to_live = 86400;

-- Indices secundarios para dashboards
CREATE INDEX IF NOT EXISTS ON user_recommendations (recommended_game);
CREATE INDEX IF NOT EXISTS ON kdd_insights (severity);
CREATE INDEX IF NOT EXISTS ON kdd_insights (layer);
