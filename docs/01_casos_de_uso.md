# Manual de Casos de Uso — KDD λ Gaming

Este documento describe los casos de uso soportados por el sistema
`juegosKDD` (plataforma híbrida de analítica de videojuegos +
recomendador). Cada caso de uso se describe con: actor, disparador,
precondiciones, flujo principal, flujo alternativo, postcondiciones y
artefactos técnicos que lo materializan.

Los dos **sub-sistemas** que conviven en el proyecto son:

| Sub-sistema | Keyspace Cassandra | Datos fuente |
|---|---|---|
| Analítica Steam en vivo | `gaming_kdd` | Steam Web API + SteamSpy (producer Python) |
| Recomendador híbrido | `gaming_recommender` | Kaggle (behaviors + reviews) + FreeToGame/SteamSpy streaming |

---

## Mapa de casos de uso

```
┌──────────────────────────────────────────────────────────────────────┐
│                         ACTORES                                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐  ┌─────────────────┐  │
│  │ Jugador  │  │ Analista │  │ Data Scient. │  │ Operador/DevOps │  │
│  └────┬─────┘  └────┬─────┘  └───────┬──────┘  └────────┬────────┘  │
└───────┼─────────────┼────────────────┼──────────────────┼───────────┘
        │             │                │                  │
        ▼             ▼                ▼                  ▼
   ┌────────────────────────────────────────────────────────────────┐
   │ CU-01  Consultar recomendaciones para un usuario               │
   │ CU-02  Consultar juegos similares por contenido                │
   │ CU-03  Explorar recomendaciones populares (cold-start)         │
   │ CU-04  Ingesta de interacciones en tiempo real                 │
   │ CU-05  Ver analítica Steam en vivo (player counts)             │
   │ CU-06  Explorar tendencias históricas (7/30 días)              │
   │ CU-07  Revisar insights KDD (alertas)                          │
   │ CU-08  Consultar métricas del modelo (RMSE / P@K / R@K)        │
   │ CU-09  Consultar agentes IA (explorer/kdd/recommend/monitor)   │
   │ CU-10  Ejecutar pipeline batch diario (Airflow)                │
   │ CU-11  Re-entrenar el recomendador manualmente                 │
   │ CU-12  Descargar/validar datasets Kaggle                       │
   │ CU-13  Arrancar/parar el stack completo                        │
   │ CU-14  Monitor automático con trigger de reentrenamiento       │
   └────────────────────────────────────────────────────────────────┘
```

---

## CU-01 — Consultar recomendaciones para un usuario

| Campo | Valor |
|---|---|
| **Actor principal** | Jugador / Aplicación cliente |
| **Precondiciones** | Stack arrancado; pipeline KDD ejecutado al menos una vez; `user_id` existe en `user_recommendations` o no (cold-start). |
| **Disparador** | `GET /api/recommendations/{user_id}?limit=10` o pestaña "🎮 Recomendaciones" del dashboard. |

**Flujo principal**

1. El cliente llama al endpoint con el `user_id`.
2. FastAPI consulta `gaming_recommender.user_recommendations` ordenadas por `hybrid_score DESC`.
3. Si hay filas, las devuelve marcando `cold_start=false` y la `source` real de cada fila (`batch` o `speed_layer`).
4. El dashboard renderiza una lista con `rank`, `hybrid_score`, componentes CF / CB y origen.

**Flujo alternativo (cold-start)**

- Si el usuario no tiene filas → se llama a `_get_popular_fallback()` que
  consulta `popular_games_fallback` y devuelve el top global, marcando
  `cold_start=true`.

**Postcondiciones**

- El usuario ve entre 1 y 50 juegos recomendados con su puntuación y explicación de origen.

**Artefactos**

- API: `5_serving_layer/api/main.py::api_get_recommendations`
- Tabla: `gaming_recommender.user_recommendations`
- Modelo CF: `hdfs:///user/gaming_recommender/models/als_model`
- Dashboard: `7_dashboard/src/components/RecommendationsTab.jsx`

---

## CU-02 — Consultar juegos similares por contenido

| Campo | Valor |
|---|---|
| **Actor** | Jugador / Analista |
| **Precondiciones** | Pipeline KDD ejecutado → tabla `game_similarity` poblada. |
| **Disparador** | `GET /api/similar/{game_title}?limit=10` |

**Flujo principal**

1. El cliente envía el título del juego (normalizado a lowercase por el endpoint).
2. FastAPI lee `game_similarity WHERE game_a = ?` ordenado por `similarity DESC`.
3. Devuelve una lista `[ { game, similarity }, … ]`.

**Postcondiciones**

- El usuario dispone de una lista de juegos relacionados por texto de reviews (TF-IDF + coseno).

**Artefactos**

- Código: `4_batch_layer/recommender/content_based.py`
- Pipeline TF-IDF persistido: `hdfs:///user/gaming_recommender/models/tfidf_pipeline`

---

## CU-03 — Explorar recomendaciones populares (cold-start)

| Campo | Valor |
|---|---|
| **Actor** | Jugador nuevo / Web anónima |
| **Disparador** | `GET /api/recommendations/popular?limit=10` o fallback implícito. |

**Flujo**

1. FastAPI lee `popular_games_fallback` (top global por horas jugadas).
2. Se devuelve `recommended_game`, `total_hours`, `n_players`, `hybrid_score=0.5` y `source="popular_fallback"`.

**Artefactos**

- Cálculo: `4_batch_layer/recommender/hybrid_recommender.py` (sección de fallback).
- Tabla: `gaming_recommender.popular_games_fallback`.

---

## CU-04 — Ingesta de interacciones en tiempo real

| Campo | Valor |
|---|---|
| **Actor** | Cliente / servicio externo |
| **Precondiciones** | Kafka en `:9092`, topic `gaming.user.interactions` creado, speed layer corriendo. |
| **Disparador** | `POST /api/ingest/interaction` con `{user_id, game_title, event_type, hours, recommended}`. |

**Flujo principal**

1. FastAPI serializa y empuja el evento a Kafka (`gaming.user.interactions`).
2. El speed layer (`streaming_recommender.py`) consume en micro-batches de 30 s.
3. Actualiza `player_windows`, detecta anomalías (`hours>100` o `engagement<0.15`) en `kdd_insights` y, si el `user_id` existe en el modelo ALS, recalcula su top-10 en `user_recommendations` con `source='speed_layer'`.

**Postcondiciones**

- A los 30-60 s el dashboard puede mostrar recomendaciones actualizadas (las `speed_layer` tienen score mayor que las `batch` y el `CLUSTERING ORDER` las devuelve primero).

**Artefactos**

- API: `5_serving_layer/api/main.py::api_ingest_interaction`
- Stream: `3_speed_layer/streaming_recommender.py`
- Producers alternativos: `1_ingesta/api_producer/freetogame_producer.py`, `steamspy_producer.py`, `synthetic_interactions_producer.py`

---

## CU-05 — Analítica Steam en vivo

| Campo | Valor |
|---|---|
| **Actor** | Analista |
| **Disparador** | Pestaña "⚡ Tiempo Real" del dashboard / `GET /api/realtime`. |

**Flujo**

1. El producer de Steam (`1_ingesta/producer/steam_producer.py`) consulta la API oficial de Steam + SteamSpy y emite a `gaming.events.raw` (cada 60 s).
2. El Speed Layer original (`3_speed_layer/spark_streaming_kdd.py`) agrega ventanas de 5 min por `game` y escribe en `gaming_kdd.player_windows` (TTL 7 días).
3. El endpoint `/api/realtime` devuelve las últimas N ventanas.
4. El dashboard muestra sparklines por juego, tier (`massive / popular / active / niche`) y salud (`health_score`).

---

## CU-06 — Tendencias históricas

| Campo | Valor |
|---|---|
| **Actor** | Analista |
| **Disparador** | Pestaña "📊 Histórico" / `GET /api/historical?days=7`. |

**Flujo**

1. El DAG diario `gaming_kdd_batch_pipeline` genera `game_stats_daily` (avg/max/min players, growth_rate, rank) a partir de `player_snapshots`.
2. El endpoint devuelve las filas de los últimos N días ordenadas por rank.
3. El dashboard renderiza tablas y rankings.

---

## CU-07 — Revisar insights KDD

| Campo | Valor |
|---|---|
| **Actor** | Analista / DevOps |
| **Disparador** | Pestaña "🔬 KDD Insights" / `GET /api/insights?source=both&severity=critical`. |

**Flujo**

1. La fase **Evaluation** de cada Spark job (batch y speed) detecta anomalías y las graba en `kdd_insights` con `severity ∈ {info, warning, alert, critical}` y `layer ∈ {speed, batch}`.
2. El endpoint agrega los insights de ambos keyspaces (opcionalmente filtrados por severidad).
3. El dashboard los lista con color por severidad.

---

## CU-08 — Consultar métricas del modelo

| Campo | Valor |
|---|---|
| **Actor** | Data Scientist |
| **Disparador** | `GET /api/recommender/metrics` / panel de métricas del dashboard. |

**Flujo**

1. `model_evaluator.py` escribe una fila en `model_metrics` por cada run del KDD, con RMSE, Precision@K, Recall@K, α, users_with_recs, games_covered y ruta del modelo ALS.
2. El endpoint devuelve las últimas N filas ordenadas por `evaluated_at DESC`.

---

## CU-09 — Consultar agentes IA

| Campo | Valor |
|---|---|
| **Actor** | Analista / DevOps |
| **Disparador** | Pestaña "🤖 Agentes IA" o endpoints `/api/agent/*`. |

| Endpoint | Agente | Propósito |
|---|---|---|
| `GET /api/agent/explorer` | **Explorer** | Valida datasets locales (tamaño, fecha modificación, conteo de líneas) y si Hive/HDFS tienen las tablas cargadas. |
| `GET /api/agent/kdd` | **KDD** | Resumen en lenguaje natural del estado de las 5 fases (recuentos, últimas métricas). |
| `GET /api/agent/recommend/{user_id}` | **Recommender** | Top-K con explicación por ítem (CF vs CB, juegos semilla). |
| `GET /api/agent/monitor` | **Monitor** | Detecta drift (comparando últimas métricas vs baseline) y puede disparar Airflow (`auto_trigger=true`). |

Cada agente usa Claude si hay `ANTHROPIC_API_KEY` (`5_serving_layer/api/agents/_claude.py`); si no, degrada a una explicación heurística.

---

## CU-10 — Ejecutar pipeline batch diario

| Campo | Valor |
|---|---|
| **Actor** | Airflow Scheduler (automático) / DevOps (manual) |
| **Precondiciones** | Stack arrancado, `.env` con credenciales Kaggle. |
| **Disparador** | Cron: 02:00 UTC (`gaming_kdd_batch_pipeline`), 02:30 UTC (`gaming_recommender_batch_pipeline`). |

**Flujo — DAG `gaming_recommender_batch_pipeline`**

1. `download_datasets` → Kaggle (o sintético si falla).
2. `check_kafka_topics` + `check_cassandra_keyspace` en paralelo.
3. `load_kaggle_to_hive` → Parquet en HDFS + tablas EXTERNAL en Hive.
4. `run_kdd_hybrid_pipeline` → ALS + TF-IDF + Hybrid + métricas (`--alpha 0.7`).
5. `validate_cassandra_output` → comprueba que `user_recommendations` tiene ≥ 1 fila.
6. `run_monitor_agent` → llama al endpoint del monitor.

**Flujo — DAG `gaming_kdd_batch_pipeline`**

1. Verifica Kafka/Cassandra/Hive-Metastore.
2. Ingesta batch adicional de Steam → Kafka.
3. Kafka → Hive (`kafka_to_hive.py`).
4. `MSCK REPAIR TABLE` sobre `player_snapshots`.
5. Spark Batch KDD (`spark_batch_kdd.py --date={{ ds }}`).
6. Validación en Cassandra.
7. Genera reporte diario.

**Artefactos**

- `6_orchestration/dags/gaming_recommender_batch.py`
- `6_orchestration/dags/gaming_kdd_batch.py`
- `6_orchestration/dags/_daily_report.py`

---

## CU-11 — Re-entrenar el recomendador manualmente

| Campo | Valor |
|---|---|
| **Actor** | Data Scientist |
| **Disparador** | `bash 4_batch_layer/recommender/submit_kdd.sh` (opcional: `--alpha`, `--top-k`, `--date`). |

**Flujo**

1. `kdd_pipeline.py` ejecuta las 5 fases KDD.
2. Escribe Parquet en HDFS + tablas EXTERNAL Hive.
3. Publica recomendaciones en Cassandra.
4. Registra nueva fila en `model_metrics`.

---

## CU-12 — Descargar / validar datasets Kaggle

| Campo | Valor |
|---|---|
| **Actor** | DevOps / Data Engineer |
| **Disparador** | `python 0_data/kaggle/download_datasets.py` + `python 0_data/kaggle/validate_datasets.py`. |

**Flujo**

1. Carga credenciales desde `.env` (`KAGGLE_USERNAME`, `KAGGLE_KEY`) o `~/.kaggle/kaggle.json`.
2. Descarga `tamber/steam-video-games` (200K interacciones) y `luthfim/steam-reviews-dataset` (434K reviews).
3. Si no hay credenciales o fallo de red, genera CSVs sintéticos con la misma forma.
4. `validate_datasets.py` chequea columnas y rango de valores.

---

## CU-13 — Arrancar / parar el stack

| Campo | Valor |
|---|---|
| **Actor** | DevOps |
| **Disparador** | `bash 0_infra/start_stack.sh` / `bash 0_infra/stop_stack.sh`. |

**Flujo start**

1. Limpia PIDs huérfanos.
2. Arranca HDFS → Kafka → Cassandra → Hive (Metastore + HS2) → Airflow (scheduler + api-server + dag-processor, puerto **8090**).
3. Muestra estado final con `jps` y puertos abiertos.

El script documenta la ruta de cada servicio (`/opt/kafka`, `/opt/spark`,
`/opt/hadoop`, `~/apache-hive-4.2.0-bin`, `~/smart_energy/cassandra`,
`~/smart_energy/nifi-2.6.0`, venv en `~/smart_energy/venv`).

---

## CU-14 — Monitor automático con trigger de reentrenamiento

| Campo | Valor |
|---|---|
| **Actor** | Sistema (cron) |
| **Disparador** | Última tarea del DAG diario (`run_monitor_agent`) o `GET /api/agent/monitor?auto_trigger=true`. |

**Flujo**

1. El monitor compara `rmse` y `precision_at_k` actuales vs baseline (media de las últimas N ejecuciones).
2. Si el drift supera el umbral, marca `drift_detected=true`.
3. Si `auto_trigger=true`, invoca la API REST de Airflow (`/dags/{DAG_ID}/dagRuns`) para lanzar un run.

---

## Matriz casos de uso × tecnología

| CU | Kafka | Spark | Hive | Cassandra | FastAPI | Dashboard | Airflow | LLM |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| CU-01 | | | | ✓ | ✓ | ✓ | | |
| CU-02 | | | | ✓ | ✓ | ✓ | | |
| CU-03 | | | | ✓ | ✓ | ✓ | | |
| CU-04 | ✓ | ✓ | | ✓ | ✓ | | | |
| CU-05 | ✓ | ✓ | | ✓ | ✓ | ✓ | | |
| CU-06 | | | ✓ | ✓ | ✓ | ✓ | | |
| CU-07 | | ✓ | | ✓ | ✓ | ✓ | | |
| CU-08 | | | ✓ | ✓ | ✓ | ✓ | | |
| CU-09 | | | | ✓ | ✓ | ✓ | | (✓) |
| CU-10 | ✓ | ✓ | ✓ | ✓ | | | ✓ | |
| CU-11 | | ✓ | ✓ | ✓ | | | | |
| CU-12 | | | | | | | (✓) | |
| CU-13 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | |
| CU-14 | | | | ✓ | ✓ | | ✓ | (✓) |
