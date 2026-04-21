# Diseño del sistema — KDD λ Gaming

Este documento describe el diseño global del sistema, la materialización
de la **Arquitectura Lambda** y el mapeo a las **5 fases del proceso
KDD**, con diagramas de flujo, modelo de datos y decisiones de
arquitectura clave.

---

## 1. Vista de alto nivel

```
                        ┌──────────────────────────────────┐
                        │    FUENTES DE DATOS              │
                        │  ┌────────┐ ┌────────┐ ┌──────┐  │
                        │  │ Steam  │ │SteamSpy│ │Kaggle│  │
                        │  │Web API │ │  API   │ │ CSV  │  │
                        │  │Free2Gm │ │        │ │      │  │
                        │  └───┬────┘ └───┬────┘ └──┬───┘  │
                        └──────┼──────────┼─────────┼──────┘
                               │          │         │
                  ┌────────────▼──┐  ┌────▼─────────▼─────┐
                  │ INGESTION     │  │ INGESTION (BATCH)  │
                  │ Producers Py  │  │ kaggle_loader.py   │
                  │ (steam, ftg,  │  │ download + clean   │
                  │  steamspy,    │  └────────┬───────────┘
                  │  synthetic)   │           │
                  └────┬──────────┘           │
                       │                      │
                       ▼                      ▼
                ┌──────────────┐        ┌──────────────┐
                │  KAFKA TOPIC │        │ HDFS / HIVE  │
                │ gaming.events│        │ user_behaviors│
                │   .raw       │        │ user_reviews │
                │ gaming.user. │        │ game_catalog │
                │ interactions │        └──────┬───────┘
                └──────┬───────┘               │
                       │                       │
        ┌──────────────┴──────┐                │
        ▼                     ▼                ▼
┌───────────────────┐  ┌─────────────────┐  ┌──────────────────┐
│  SPEED LAYER      │  │  SPEED LAYER    │  │ BATCH LAYER      │
│  (Steam live)     │  │  (recommender)  │  │ Spark KDD        │
│  spark_streaming  │  │  streaming_     │  │ ALS+TF-IDF+      │
│  _kdd.py          │  │  recommender.py │  │ Hybrid+Eval      │
│  ventanas 5min    │  │  micro-batch 30s│  │ + spark_batch_   │
│                   │  │                 │  │   kdd.py daily   │
└──────┬────────────┘  └─────────┬───────┘  └────────┬─────────┘
       │                         │                   │
       ▼                         ▼                   ▼
┌────────────────────────────────────────────────────────────────┐
│                  SERVING LAYER (Cassandra)                      │
│  ┌─────────────────────┐   ┌──────────────────────────────────┐ │
│  │ Keyspace            │   │ Keyspace                         │ │
│  │ gaming_kdd          │   │ gaming_recommender               │ │
│  │ ─ player_windows    │   │ ─ user_recommendations           │ │
│  │ ─ game_stats_daily  │   │ ─ game_similarity                │ │
│  │ ─ kdd_insights      │   │ ─ popular_games_fallback         │ │
│  └─────────────────────┘   │ ─ player_windows                 │ │
│                            │ ─ user_history_cache             │ │
│                            │ ─ model_metrics                  │ │
│                            │ ─ kdd_insights                   │ │
│                            │ ─ agent_explanations             │ │
│                            └──────────────────────────────────┘ │
└────────────────────┬─────────────────────────────────────┬─────┘
                     │                                     │
                     ▼                                     ▼
              ┌──────────────┐                    ┌──────────────┐
              │ FastAPI      │   <── HTTP/REST ──>│  Dashboard   │
              │ (puerto 8000)│                    │  React + Vite│
              │ + Agentes IA │                    │  (puerto 5173)│
              └──────────────┘                    └──────────────┘
                     ▲
                     │ trigger DAG
                     │
              ┌──────┴───────┐
              │ Apache Airflow│
              │  (puerto 8090)│
              │ 2 DAGs diarios│
              └──────────────┘
```

---

## 2. Arquitectura Lambda

La arquitectura Lambda en este proyecto está implementada con **dos
ramas independientes** que confluyen en el mismo Serving Layer
(Cassandra), accedido por una única API.

### 2.1 Batch Layer

Responsabilidad: producir vistas **completas y precisas** a partir de
toda la historia de datos (Hive).

| Job | Frecuencia | Entrada | Salida | Script |
|---|---|---|---|---|
| `kafka_to_hive` | diaria | topic `gaming.events.raw` | `gaming_kdd.player_snapshots` (Parquet, particionado por `dt`) | `4_batch_layer/kafka_to_hive.py` |
| `spark_batch_kdd` | diaria | `player_snapshots` | `game_stats_daily`, `kdd_insights` (`layer='batch'`) | `4_batch_layer/spark_batch_kdd.py` |
| `kaggle_loader` | bajo demanda + diaria | CSVs Kaggle | `user_behaviors`, `user_reviews`, `game_catalog` (Hive) | `1_ingesta/kaggle_to_hive/loader.py` |
| `kdd_pipeline` | diaria | tablas Hive | `user_recommendations`, `game_similarity`, `popular_games_fallback`, `model_metrics` | `4_batch_layer/recommender/kdd_pipeline.py` |

### 2.2 Speed Layer

Responsabilidad: producir vistas **incrementales y aproximadas** en
tiempo real, con latencia de segundos.

| Job | Trigger | Entrada | Salida | Script |
|---|---|---|---|---|
| `spark_streaming_kdd` | 30 s | topic `gaming.events.raw` | `gaming_kdd.player_windows`, `gaming_kdd.kdd_insights` (`layer='speed'`) | `3_speed_layer/spark_streaming_kdd.py` |
| `streaming_recommender` | 30 s | topic `gaming.user.interactions` | `gaming_recommender.player_windows`, `user_recommendations` (`source='speed_layer'`), `kdd_insights` | `3_speed_layer/streaming_recommender.py` |

### 2.3 Serving Layer

Cassandra unifica ambas ramas. La estrategia clave es:

- **`user_recommendations` con `CLUSTERING ORDER BY hybrid_score DESC`** y TTL 24 h: las recomendaciones nuevas del speed layer (con score más alto por construcción) aparecen automáticamente por delante de las del batch sin necesidad de borrar nada.
- **TTLs por tabla** para que las vistas viejas se purguen solas:
  - `player_windows`: 7 días
  - `user_recommendations`, `user_history_cache`, `agent_explanations`: 24 h
  - `kdd_insights`: 30 días
  - `game_stats_daily`, `game_similarity`, `model_metrics`, `popular_games_fallback`: sin TTL (histórico).

### 2.4 Decisión: keyspaces separados

Se mantienen dos keyspaces distintos (`gaming_kdd` y `gaming_recommender`)
para **aislar fallos** entre el pipeline original (Steam live) y el
recomendador. La API los consulta por separado y los une donde tiene
sentido (`/api/insights?source=both`).

---

## 3. Mapeo Lambda ↔ KDD

```
┌────────────────────────────────────────────────────────────────────┐
│                    PROCESO KDD (5 FASES)                           │
└────────────────────────────────────────────────────────────────────┘

┌──────────────┐  ┌──────────────┐  ┌────────────────┐
│ 1. SELECTION │  │ 2. PREPROC.  │  │ 3. TRANSFORM.  │
│ ─ Lectura    │  │ ─ Filtrado   │  │ ─ Rating impl. │
│   Kafka /    │  │   nulos      │  │ ─ Perfiles TF  │
│   Hive       │  │ ─ Dedup      │  │ ─ Tiers,health │
│   Kaggle     │  │              │  │   _score       │
└──────┬───────┘  └──────┬───────┘  └────────┬───────┘
       │                 │                   │
       ▼                 ▼                   ▼
┌────────────────────────────────────────────────────────────────────┐
│                  4. MINING (modelos)                                │
│ ┌────────────────────┐  ┌──────────────────┐  ┌─────────────────┐  │
│ │ ALS (CF)           │  │ TF-IDF (CB)      │  │ Hybrid          │  │
│ │ Spark MLlib        │  │ Spark ML +       │  │ score = α·CF +  │  │
│ │ rating=log1p(h)    │  │ cosine_sim       │  │         (1-α)·CB│  │
│ │ rank=10, regParam  │  │                  │  │ α=0.7 default   │  │
│ │ implicitPrefs=True │  │                  │  │                 │  │
│ └────────────────────┘  └──────────────────┘  └─────────────────┘  │
│                                                                    │
│ ┌──────────────────────────────────────────────────────────────┐   │
│ │ Reglas de mineria sobre eventos (speed):                     │   │
│ │  ─ window(5min) por game            ─ avg/max/min players   │   │
│ │  ─ player_tier (massive/popular/active/niche)               │   │
│ │  ─ growth_rate vs dia anterior                              │   │
│ └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────┬─────────────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│ 5. EVALUATION                                                       │
│ ─ RMSE (regression evaluator sobre split train/test)                │
│ ─ Precision@K, Recall@K (top-K vs ground truth en test)             │
│ ─ Anomaly detection → kdd_insights con severity                     │
│ ─ Persistencia en model_metrics + Cassandra                         │
└────────────────────────────────────────────────────────────────────┘
```

| Fase KDD | Donde ocurre (batch) | Donde ocurre (speed) | Tecnología |
|---|---|---|---|
| **Selection** | `hio.read_table()` desde Hive | `spark.readStream(kafka)` | Hive, Kafka |
| **Preprocessing** | filtros nulos, dedup, lower-case | parsing JSON + filtros sanity | Spark SQL |
| **Transformation** | `log1p(hours)`, perfiles TF-IDF | `engagement_index` por evento | Spark ML, Spark SQL |
| **Mining** | ALS + TF-IDF + Hybrid | windowed agg + scoring incremental | Spark MLlib |
| **Evaluation** | RMSE, P@K, R@K → `model_metrics` | umbrales de anomalía → `kdd_insights` | Spark + Python |

---

## 4. Modelo de datos

### 4.1 Modelo Hive (Batch / Selection)

```
gaming_kdd                                  gaming_recommender (Hive db)
│                                           │
├─ player_snapshots (PARTITIONED BY dt)     ├─ user_behaviors
│   └─ raw events Steam                     │   ├─ user_id  (string)
│                                           │   ├─ game_title (string)
├─ kdd_daily_summary (PARTITIONED BY dt)    │   ├─ behavior  (purchase|play)
│   └─ aggregates por game/dia              │   ├─ value     (double)
│                                           │   └─ hours_played (double)
└─ VIEW kdd_analysis                        │
    └─ rank por dt sobre kdd_daily_summary  ├─ user_reviews
                                            │   ├─ app_id, app_name
                                            │   ├─ review_text, review_clean
                                            │   ├─ recommendation (bool)
                                            │   └─ helpful_votes
                                            │
                                            ├─ game_catalog
                                            ├─ user_index_map
                                            ├─ game_index_map
                                            ├─ user_behaviors_indexed
                                            ├─ game_similarity
                                            ├─ popular_games_fallback
                                            ├─ hybrid_recommendations
                                            └─ model_metrics
```

### 4.2 Modelo Cassandra (Serving Layer)

#### Keyspace `gaming_kdd`

| Tabla | PK | Cluster | TTL | Uso |
|---|---|---|---|---|
| `player_windows` | `(game)` | `(window_start DESC)` | 7d | Speed layer Steam |
| `kdd_insights` | `(insight_id)` | `(created_at DESC)` | 30d | Alertas |
| `game_stats_daily` | `(appid)` | `(dt DESC)` | — | Histórico daily |

#### Keyspace `gaming_recommender`

| Tabla | PK | Cluster | TTL | Uso |
|---|---|---|---|---|
| `user_recommendations` | `(user_id)` | `(hybrid_score DESC, recommended_game)` | 24h | Top-K híbrido |
| `game_similarity` | `(game_a)` | `(similarity DESC, game_b)` | — | Vecinos CB |
| `model_metrics` | `(model_name)` | `(evaluated_at DESC, run_id)` | — | Histórico métricas |
| `user_history_cache` | `(user_id)` | `(hours_played DESC, game_title)` | 7d | Cache para agentes |
| `popular_games_fallback` | `(recommended_game)` | — | — | Cold-start |
| `player_windows` | `(user_id)` | `(window_start DESC)` | 7d | Speed layer recomm. |
| `kdd_insights` | `(insight_id)` | `(created_at DESC)` | 30d | Alertas recomm. |
| `agent_explanations` | `((user_id,game_title))` | `(generated_at DESC)` | 24h | Cache LLM |

> **Nota**: las dos `player_windows` y dos `kdd_insights` no colisionan porque están en keyspaces diferentes.

### 4.3 Topics Kafka

| Topic | Particiones | Producer | Consumer |
|---|---|---|---|
| `gaming.events.raw` | 3 | `steam_producer.py`, `freetogame_producer.py`, `steamspy_producer.py` | `spark_streaming_kdd.py`, `kafka_to_hive.py` |
| `gaming.events.processed` | 3 | (futuro: speed layer) | analytics agnósticos |
| `gaming.kdd.insights` | 3 | jobs Spark | feed de alertas externas |
| `gaming.batch.trigger` | 1 | DAG / monitor agent | future-proof |
| `gaming.user.interactions` | 3 | `synthetic_interactions_producer.py`, `freetogame_producer.py`, FastAPI `/api/ingest/interaction` | `streaming_recommender.py` |
| `gaming.recommendations` | 3 | (futuro speed layer publish) | clientes externos |
| `gaming.model.updates` | 1 | DAG batch (señal de modelo nuevo) | streaming_recommender (broadcast en futuro) |

---

## 5. Flujo end-to-end

### 5.1 Camino batch (recomendador, una vez al día)

```
02:30 UTC Airflow
  │
  ├─► download_datasets   ─► /home/hadoop/juegosKDD/0_data/raw/*.csv
  ├─► check_kafka         ─► kafka-topics --list
  ├─► check_cassandra     ─► cqlsh DESCRIBE KEYSPACES
  │
  ├─► load_kaggle_to_hive  spark-submit loader.py
  │     ├─ leer CSV con Spark
  │     ├─ Selection / Preproc / Transform
  │     ├─ escribir Parquet → hdfs:///user/hive/warehouse/gaming_recommender/...
  │     └─ beeline CREATE EXTERNAL TABLE ...
  │
  ├─► run_kdd_hybrid_pipeline  spark-submit kdd_pipeline.py --alpha 0.7
  │     ├─ KDD 1 SELECTION   hio.read_table("user_behaviors","user_reviews")
  │     ├─ KDD 2 PREPROC     count nulos / sanity
  │     ├─ KDD 3 TRANSFORM   build_game_profiles (concat reviews por juego)
  │     ├─ KDD 4 MINING      train_collaborative_filter (ALS)
  │     │                    compute_tfidf_similarity
  │     │                    generate_hybrid_recommendations(α=0.7, k=10)
  │     └─ KDD 5 EVAL        evaluate_and_store_metrics → model_metrics
  │
  ├─► validate_cassandra_output  cqlsh COUNT(*) > 0
  └─► run_monitor_agent  curl /api/agent/monitor
```

### 5.2 Camino speed (recomendador, continuo)

```
[productor (FastAPI POST | freetogame | synthetic | externo)]
       │
       ▼  JSON {user_id, game_title, event_type, hours, recommended, ts}
┌───────────────────┐
│ Kafka topic       │  gaming.user.interactions
│ partitions = 3    │
└────────┬──────────┘
         │
         ▼  trigger 30s, watermark 5 min
┌────────────────────────────────────────────────┐
│ Spark Structured Streaming                      │
│  streaming_recommender.py                       │
│                                                 │
│ foreachBatch:                                   │
│  1. agregar player_windows (avg_engagement)     │
│  2. detectar anomalias → kdd_insights           │
│  3. JOIN con user_index_map (cached desde HDFS) │
│  4. si user existe en ALS:                      │
│       cargar als_model.recommendForUserSubset() │
│       → user_recommendations(source=speed_layer)│
└────────────────────┬───────────────────────────┘
                     │
                     ▼
              Cassandra (gaming_recommender)
                     │
                     ▼
              FastAPI /api/recommendations/{user_id}
                     │
                     ▼
              React Dashboard
```

### 5.3 Camino Steam live (analítica)

```
steam_producer.py ──60s──► kafka:gaming.events.raw
                                  │
                                  ├──► spark_streaming_kdd.py ──► gaming_kdd.player_windows (5min)
                                  │                              + kdd_insights (speed)
                                  │
                                  └──► kafka_to_hive.py (daily) ──► hive:player_snapshots
                                                                          │
                                                                          ▼
                                                                  spark_batch_kdd.py
                                                                          │
                                                                          ▼
                                                            gaming_kdd.game_stats_daily
                                                                  + kdd_insights (batch)
```

---

## 6. Algoritmo del recomendador híbrido

### 6.1 Filtrado colaborativo (CF) — ALS implícito

Spark MLlib `ALS` con:

- `userCol="user_idx"`, `itemCol="game_idx"`, `ratingCol="rating"`
- `rating = log1p(hours_played)` (rating implícito normalizado)
- `implicitPrefs=True`, `rank=10`, `regParam=0.1`, `alpha=1.0`
- `maxIter=10`, `coldStartStrategy="drop"`
- Split train/test 80/20.
- Evaluación con `RegressionEvaluator(metricName="rmse")`.
- Modelo persistido en `hdfs:///user/gaming_recommender/models/als_model`.
- Mapas `user_index_map`/`game_index_map` (StringIndexer) persistidos también para ser reutilizados por el speed layer.

### 6.2 Content-based (CB) — TF-IDF

Pipeline Spark ML:

```
Tokenizer → StopWordsRemover → HashingTF(numFeatures=2^14) → IDF → Normalizer
```

- Entrada: concatenación de hasta **200 reviews limpias** por juego.
- Similaridad coseno entre vectores normalizados (driver-side numpy/sklearn por simplicidad).
- Salida: tabla `game_similarity (game_a, game_b, similarity)`.

### 6.3 Hybrid

Para cada par `(user, candidate_game)`:

```
cf_score    = ALS.predict(user, game)        # normalizado por usuario
cb_score    = mean(sim(seed_i, candidate))   # seeds = top juegos del usuario
hybrid      = α · cf_score + (1 - α) · cb_score
```

- `α=0.7` por defecto (favorece CF cuando hay datos suficientes).
- `top_k=10` por defecto.
- Las componentes individuales se persisten (`cf_component`, `cb_component`) para hacer **explicabilidad** posterior por el agente recomendador.

### 6.4 Métricas

- **RMSE**: heredado del CF (regresión sobre el split de test).
- **Precision@K** = `|relevantes ∩ top_k| / k`.
- **Recall@K** = `|relevantes ∩ top_k| / |relevantes|`.

> Donde `relevantes` = juegos en el test set con `hours_played > umbral` para ese usuario.

---

## 7. Decisiones arquitectónicas (ADRs en compacto)

| ADR | Decisión | Motivo |
|---|---|---|
| **ADR-01** | Mantener carpetas existentes (`3_speed_layer`, `4_batch_layer`) y añadir el recomendador como subcarpeta `recommender/` dentro del batch. | No tocar el pipeline original que ya funcionaba. Pedido explícito del usuario. |
| **ADR-02** | Usar dos **keyspaces** Cassandra (`gaming_kdd` + `gaming_recommender`). | Aislar fallos entre el pipeline original y el recomendador; permitir borrar uno sin tocar el otro. |
| **ADR-03** | Evitar `enableHiveSupport()` y escribir Parquet directo en HDFS + `beeline CREATE EXTERNAL TABLE`. | Incompatibilidad entre el cliente Hive 2.3.9 embebido en Spark 3.5.1 y el Hive Metastore 4.2.0 (`Invalid method name: get_table`). Materializado en `_hive_io.py`. |
| **ADR-04** | Speed Layer **no** entrena un nuevo modelo: solo reusa `als_model` cargado desde HDFS. | Estabilidad y latencia. El reentrenamiento es responsabilidad del DAG batch nocturno. |
| **ADR-05** | Cassandra `user_recommendations` con `CLUSTERING ORDER BY hybrid_score DESC` y TTL 24h. | Las recomendaciones nuevas del speed layer (score normalmente más alto) aparecen primero sin borrar las del batch. |
| **ADR-06** | Mantener un fallback **sintético** cuando Kaggle no es accesible. | El pipeline siempre debe terminar con un modelo válido (laboratorio educativo). |
| **ADR-07** | Agentes IA con **degradación heurística** si no hay `ANTHROPIC_API_KEY`. | El proyecto debe poder demostrarse sin dependencias externas. |
| **ADR-08** | Airflow corre en **puerto 8090** (no 8080) y `AIRFLOW_HOME=0_infra/airflow_home`. | Aislar de NiFi (puerto 8080) y de cualquier otro proyecto Airflow del equipo. |
| **ADR-09** | Producer dedicado (`synthetic_interactions_producer.py`) para inyectar eventos con `user_id` reales del modelo. | El speed layer no podía mostrar updates con los `ftg_user_XXXX` de FreeToGame (cold-start permanente). |
| **ADR-10** | FastAPI sirve `gaming_kdd` y `gaming_recommender` en una sola app. | Un único endpoint para el dashboard, una sola política CORS, un solo Swagger. |

---

## 8. Vista de despliegue

```
┌─────────────────────────────────────────────────────────────────┐
│ Host único (Linux Mint, nodo1, 16 GB RAM)                        │
│                                                                  │
│ ┌──────────────────────┐   ┌──────────────────────┐             │
│ │ /opt/hadoop          │   │ /opt/kafka           │             │
│ │   NameNode  :9000    │   │   KRaft :9092        │             │
│ │   DataNode  :9866    │   │                      │             │
│ └──────────────────────┘   └──────────────────────┘             │
│                                                                  │
│ ┌──────────────────────┐   ┌──────────────────────┐             │
│ │ /opt/spark           │   │ ~/apache-hive-4.2.0  │             │
│ │   spark-submit       │   │   metastore  :9083   │             │
│ │   local[*]           │   │   hiveserver2 :10000 │             │
│ └──────────────────────┘   └──────────────────────┘             │
│                                                                  │
│ ┌──────────────────────┐   ┌──────────────────────┐             │
│ │ ~/smart_energy/      │   │ Airflow              │             │
│ │   cassandra :9042    │   │   webserver  :8090   │             │
│ │   (compartido)       │   │   AIRFLOW_HOME=      │             │
│ └──────────────────────┘   │   0_infra/airflow_home              │
│                            └──────────────────────┘             │
│                                                                  │
│ ┌──────────────────────┐   ┌──────────────────────┐             │
│ │ FastAPI :8000        │   │ Vite dashboard :5173 │             │
│ │ uvicorn main:app     │   │ npm run dev (proxy)  │             │
│ └──────────────────────┘   └──────────────────────┘             │
└─────────────────────────────────────────────────────────────────┘
```

> El despliegue en producción multi-host se conseguiría externalizando
> Cassandra y Spark a clúster dedicado, conservando el resto.

---

## 9. Diagrama de secuencia — recomendación en tiempo real

```
Usuario      Producer / API     Kafka         Spark Speed     Cassandra      FastAPI       Dashboard
   │              │                │                │              │            │              │
   │  abre app    │                │                │              │            │              │
   ├─────────────────────────────────────────────────────────────────────────────────────────► │
   │              │                │                │              │            │   GET /api/  │
   │              │                │                │              │   ◄────────┤   recommen…  │
   │              │                │                │              │   user_rec │              │
   │              │                │                │              ├───────────►│  JSON list   │
   │              │                │                │              │            ├─────────────►│
   │              │                │                │              │            │              │
   │ click "play" │                │                │              │            │              │
   ├─────────────►│ POST /ingest   │                │              │            │              │
   │              ├───────────────►│ produce        │              │            │              │
   │              │                │ topic=gaming.user.interactions│            │              │
   │              │                ├───────────────►│ micro-batch  │            │              │
   │              │                │                │ 30s          │            │              │
   │              │                │                ├─ update player_windows ──►│              │
   │              │                │                ├─ detect anomaly → insight│              │
   │              │                │                ├─ ALS.predict for user ───►│              │
   │              │                │                │              │ user_recommendations      │
   │              │                │                │              │            │              │
   │  refresh     │                │                │              │            │   GET /api/  │
   ├─────────────────────────────────────────────────────────────────────────────────────────► │
   │              │                │                │              │            │   recommen…  │
   │              │                │                │              ◄────────────┤              │
   │              │                │                │              │  source=   │              │
   │              │                │                │              │  speed_layer              │
   │              │                │                │              ├───────────►│ con score    │
   │              │                │                │              │            │ MAYOR que    │
   │              │                │                │              │            │ batch        │
```
