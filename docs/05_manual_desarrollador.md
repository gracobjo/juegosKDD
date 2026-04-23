# Manual del Desarrollador — KDD λ Gaming

Este manual está dirigido a **desarrolladores** que quieran instalar,
modificar o extender el sistema. Cubre: estructura del repo, setup,
convenciones de código, cómo arrancar/parar cada componente, cómo
añadir un nuevo dataset, una nueva tabla, un nuevo agente o una nueva
pestaña, y cómo desplegar.

---

## 1. Estructura del repositorio

```
juegosKDD/
├── 0_data/
│   └── kaggle/
│       ├── download_datasets.py     # descarga real o sintético
│       └── validate_datasets.py     # smoke test de los CSVs
├── 0_infra/
│   ├── start_stack.sh               # arranque ordenado (HDFS→Kafka→Cass→Hive→Airflow)
│   ├── stop_stack.sh
│   ├── start_pipeline.sh            # arranca FastAPI + dashboard + producers
│   ├── init_schemas.sh              # cqlsh + beeline create tables
│   ├── setup_venv.sh                # crea venv con numpy/pyarrow/setuptools
│   ├── airflow_home/                # AIRFLOW_HOME aislado
│   ├── logs/
│   └── pids/
├── 1_ingesta/
│   ├── producer/                    # Steam Web API → Kafka (pipeline original)
│   │   ├── steam_producer.py
│   │   ├── config.py
│   │   └── requirements.txt
│   ├── api_producer/                # FreeToGame + SteamSpy + Synthetic
│   │   ├── freetogame_producer.py
│   │   ├── steamspy_producer.py
│   │   ├── synthetic_interactions_producer.py
│   │   ├── config.py
│   │   └── requirements.txt
│   └── kaggle_to_hive/              # CSV → HDFS Parquet → Hive EXTERNAL
│       ├── loader.py
│       ├── hive_schema.sql
│       └── run_loader.sh
├── 2_kafka/
│   └── setup_topics.sh              # crea 7 topics
├── 3_speed_layer/
│   ├── spark_streaming_kdd.py       # Steam live → ventanas 5 min
│   ├── streaming_recommender.py     # interactions → recs en tiempo real
│   ├── submit_streaming.sh
│   └── submit_streaming_recommender.sh
├── 4_batch_layer/
│   ├── kafka_to_hive.py             # Kafka → HDFS Parquet (player_snapshots)
│   ├── spark_batch_kdd.py           # KDD Steam diario
│   ├── hive_schema.sql
│   ├── submit_batch.sh
│   ├── submit_kafka_to_hive.sh
│   └── recommender/
│       ├── kdd_pipeline.py          # orquesta 5 fases
│       ├── _hive_io.py              # helper Parquet+beeline
│       ├── collaborative_filtering.py
│       ├── content_based.py
│       ├── hybrid_recommender.py
│       ├── model_evaluator.py
│       ├── submit_kdd.sh
│       └── __init__.py
├── 5_serving_layer/
│   ├── cassandra_schema.cql         # gaming_kdd
│   ├── recommender_schema.cql       # gaming_recommender
│   └── api/
│       ├── main.py                  # FastAPI (puerto 8000)
│       ├── requirements.txt
│       └── agents/                  # 4 agentes IA
│           ├── _claude.py
│           ├── explorer_agent.py
│           ├── kdd_agent.py
│           ├── recommender_agent.py
│           └── monitor_agent.py
├── 6_orchestration/
│   └── dags/
│       ├── gaming_kdd_batch.py             # 02:00 UTC
│       ├── gaming_recommender_batch.py     # 02:30 UTC
│       └── _daily_report.py
├── 7_dashboard/
│   ├── src/
│   │   ├── App.jsx                   # 6 pestañas
│   │   ├── api.js                    # cliente REST
│   │   ├── main.jsx
│   │   └── components/
│   │       ├── RecommendationsTab.jsx
│   │       └── AgentsTab.jsx
│   └── vite.config.js                # proxy /api → :8000
├── docs/                             # ← este manual
├── .env / .env.example
├── requirements.txt                  # consolidado raíz
└── README.md
```

---

## 2. Setup desde cero

### 2.1 Prerrequisitos

| Componente | Versión | Cómo verificar |
|---|---|---|
| Java | 21 | `java -version` |
| Python | 3.12 | `python3 --version` |
| Node | 18+ | `node --version` |
| Spark | 3.5.1 | `$SPARK_HOME/bin/spark-submit --version` |
| Kafka | 3.x KRaft | `$KAFKA_HOME/bin/kafka-topics.sh --version` |
| Hive | 4.2.0 | `beeline --version` |
| Cassandra | 4.x | `cqlsh -e "SHOW VERSION"` |
| Airflow | 3.x | `airflow version` |

### 2.2 Clonar y configurar entorno

```bash
git clone https://github.com/gracobjo/juegosKDD.git
cd juegosKDD
cp .env.example .env
# Editar .env (KAGGLE_USERNAME/KEY, ANTHROPIC_API_KEY opcional, STEAM_API_KEY opcional)

bash 0_infra/setup_venv.sh
source venv/bin/activate
pip install -r requirements.txt
pip install -r 5_serving_layer/api/requirements.txt
```

### 2.3 Arrancar la infraestructura

```bash
# Arranca HDFS, Kafka, Cassandra, Hive (Metastore + HS2) y Airflow
bash 0_infra/start_stack.sh

# Verificar
bash 0_infra/start_stack.sh --status
```

### 2.4 Inicializar esquemas

```bash
# Topics Kafka
bash 2_kafka/setup_topics.sh

# Cassandra
cqlsh -f 5_serving_layer/cassandra_schema.cql
cqlsh -f 5_serving_layer/recommender_schema.cql

# Hive (databases)
beeline -u jdbc:hive2://localhost:10000 -n hadoop -f 4_batch_layer/hive_schema.sql
```

### 2.5 Cargar datos

```bash
# Descarga Kaggle (o sintético si fallan credenciales)
python 0_data/kaggle/download_datasets.py
python 0_data/kaggle/validate_datasets.py

# CSV → HDFS Parquet → Hive EXTERNAL
bash 1_ingesta/kaggle_to_hive/run_loader.sh
```

### 2.6 Ejecutar el KDD una vez

```bash
bash 4_batch_layer/recommender/submit_kdd.sh
# Esto crea: user_recommendations, game_similarity, popular_games_fallback,
# user_history_cache, model_metrics
```

### 2.7 Levantar API + dashboard

```bash
# Terminal 1
cd 5_serving_layer/api
python main.py

# Terminal 2
cd 7_dashboard
npm install
npm run dev
```

### 2.8 (Opcional) Arrancar Speed Layer y producers

```bash
# Speed Layer del recomendador
bash 3_speed_layer/submit_streaming_recommender.sh   # background

# Producer sintético con user_id reales (mejor para demos)
python 1_ingesta/api_producer/synthetic_interactions_producer.py --rate 5

# Speed Layer Steam (analítica)
bash 3_speed_layer/submit_streaming.sh

# Producer Steam (necesita STEAM_API_KEY)
python 1_ingesta/producer/steam_producer.py
```

---

## 3. Convenciones de código

### 3.1 Python

- **Logging** estándar: `logging.basicConfig(level=INFO, format="%(asctime)s [%(name)s] %(message)s")`.
- **Argparse** en todos los scripts batch (`--date`, `--alpha`, `--top-k`, `--no-cassandra`).
- Spark: nunca `enableHiveSupport()` — usar `_hive_io.read_table()` / `write_table()`.
- Cassandra: usar `_safe_execute()` en FastAPI (no abortar el endpoint si una query falla).
- Type hints obligatorios en módulos del recomendador (`from __future__ import annotations`).
- Nombres de tablas y topics en `snake.dot.style`.

### 3.2 Spark submit

- Siempre pasar `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1` cuando aplique.
- Configurar `PYSPARK_PYTHON` y `PYSPARK_DRIVER_PYTHON` apuntando al venv para que numpy/sklearn estén disponibles.
- Pasar `_hive_io.py` con `--py-files` para todos los jobs Spark del recomendador.

### 3.3 React

- Componentes funcionales con hooks.
- Estado local por pestaña (no Redux ni Zustand).
- Llamadas REST centralizadas en `src/api.js` — no hacer `fetch` directo en los componentes.
- CSS embebido en `App.jsx` (estilo cyberpunk/terminal); para nuevas pestañas, reutilizar las clases existentes (`.section`, `.feed-item`, `.bar-chart`, `.ai-panel`).

### 3.6 Dashboard (pestañas) y contrato de datos

Las pestañas están definidas en `7_dashboard/src/App.jsx` y consumen FastAPI vía `7_dashboard/src/api.js`.
Este es el “contrato” recomendado (útil para debugging):

| Pestaña | Front | Endpoint(s) | Keyspace / tabla |
|---|---|---|---|
| Tiempo Real | `App.jsx` | `GET /api/realtime` | `gaming_kdd.player_windows` |
| Histórico | `App.jsx` | `GET /api/historical` | `gaming_kdd.game_stats_daily` |
| KDD Insights | `App.jsx` | `GET /api/insights?source=both` | `gaming_kdd.kdd_insights` + `gaming_recommender.kdd_insights` |
| Recomendaciones | `RecommendationsTab.jsx` | `GET /api/recommendations/{user_id}` | `gaming_recommender.user_recommendations` |
|  |  | `GET /api/recommendations/popular` | `gaming_recommender.popular_games_fallback` |
|  |  | `GET /api/similar/{game_title}` | `gaming_recommender.game_similarity` |
|  |  | `GET /api/recommender/metrics` | `gaming_recommender.model_metrics` |
|  |  | `GET /api/recommender/users` | `gaming_recommender.player_windows` (IDs observados) |
| Agentes IA | `AgentsTab.jsx` | `GET /api/agent/*` | mixto (Cassandra + checks) |
| Sistema | `App.jsx` | `GET /api/agent/monitor` | `gaming_recommender.*` |
| Arquitectura | `App.jsx` | `GET /api/kdd/summary` (y estático) | `gaming_kdd.*` |

Notas:
- El dashboard **no** lee Kaggle directamente. Kaggle se usa para poblar Hive/Cassandra (batch) y luego la UI consume Cassandra vía FastAPI.
- El selector de usuarios en Recomendaciones usa `GET /api/recommender/users`, que lista `user_id` vistos en el Speed Layer del recomendador.

### 3.4 Cassandra schema

- Toda tabla nueva debe declarar:
  - `PRIMARY KEY` natural (no UUID si hay un campo de negocio claro).
  - `CLUSTERING ORDER BY` apropiado para la query principal.
  - `default_time_to_live` si la tabla es de alta rotación.
- Crear índices secundarios solo cuando el endpoint lo necesite explícitamente.

### 3.5 Git

- Sin commits ni push **automáticos**: solo cuando el usuario lo pide.
- Mensajes en español, primera línea ≤ 72 chars, modo imperativo.

---

## 4. Cómo extender el sistema

### 4.1 Añadir una nueva fuente de datos

1. Añadir un producer en `1_ingesta/api_producer/` o `1_ingesta/producer/`.
2. Si va a Kafka, usar uno de los topics existentes (`gaming.events.raw` para eventos de juego, `gaming.user.interactions` para interacciones user-game) o añadir uno nuevo a `2_kafka/setup_topics.sh`.
3. Si va a Hive (CSV grande), añadir un loader en `1_ingesta/kaggle_to_hive/` siguiendo el patrón de `loader.py` (Spark → Parquet HDFS → beeline `CREATE EXTERNAL TABLE`).

### 4.2 Añadir una nueva tabla Cassandra

1. Editarla en `5_serving_layer/cassandra_schema.cql` o `recommender_schema.cql`.
2. Aplicar con `cqlsh -f`.
3. Si la tabla la pueblan jobs Spark, añadir el `df.write.format("org.apache.spark.sql.cassandra")` correspondiente.
4. Exponer un endpoint en `main.py` siguiendo el patrón `_safe_execute` + `Pydantic` opcional.
5. Si hay frontend, añadir el método en `7_dashboard/src/api.js` y consumirlo desde el componente correspondiente.

### 4.3 Añadir un nuevo agente IA

1. Crear `5_serving_layer/api/agents/mi_agente.py` con una función `run_mi_agente(session) -> dict`.
2. Importar `from ._claude import call_claude, llm_available` para el LLM (con fallback heurístico).
3. Registrar el endpoint en `main.py`:
   ```python
   from agents.mi_agente import run_mi_agente

   @app.get("/api/agent/mi_agente")
   def api_agent_mi_agente():
       session = get_session_rec()
       return run_mi_agente(session)
   ```
4. Añadir el método en `api.js` y, si procede, una tarjeta en `AgentsTab.jsx`.

### 4.4 Añadir una nueva pestaña al dashboard

1. Crear `7_dashboard/src/components/MiTab.jsx`.
2. Importarlo en `App.jsx`:
   ```jsx
   import MiTab from "./components/MiTab.jsx";
   ```
3. Añadir el id en el array de pestañas y el bloque condicional `{tab === "mi_tab" && <MiTab />}`.

### 4.5 Añadir una fase KDD adicional al pipeline

1. Crear el módulo dentro de `4_batch_layer/recommender/` (p. ej. `clustering.py`).
2. Llamarlo desde `kdd_pipeline.py::run_pipeline()` en el bloque correspondiente.
3. Persistir resultados con `_hive_io.write_table()` y/o el conector Cassandra.
4. Añadir el `.py` al `--py-files` de `submit_kdd.sh` y del DAG `gaming_recommender_batch.py`.

### 4.6 Cambiar hiperparámetros del recomendador

| Parámetro | Dónde se cambia | Default |
|---|---|---|
| `α` (peso CF vs CB) | `--alpha` en `submit_kdd.sh` o DAG; argparse en `kdd_pipeline.py`. | 0.7 |
| `top-K` | `--top-k`. | 10 |
| `rank` ALS | `collaborative_filtering.py::train_collaborative_filter`. | 10 |
| `regParam` ALS | idem. | 0.1 |
| `numFeatures` HashingTF | `content_based.py`. | 2^14 |
| `max_reviews_per_game` | `content_based.py::build_game_profiles`. | 200 |

---

## 5. Operación

### 5.1 Verificar el estado

```bash
# Servicios
bash 0_infra/start_stack.sh --status

# Topics Kafka
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Cassandra
cqlsh -e "SELECT keyspace_name FROM system_schema.keyspaces;"
cqlsh -e "SELECT COUNT(*) FROM gaming_recommender.user_recommendations;"

# HDFS
hdfs dfs -ls /user/hive/warehouse/gaming_recommender/

# API
curl -s http://localhost:8000/health | jq .
curl -s http://localhost:8000/api/keyspaces | jq .

# Airflow DAGs
curl -s http://localhost:8090/api/v1/dags | jq '.dags[].dag_id'
```

### 5.2 Logs

| Componente | Log |
|---|---|
| start_stack | `0_infra/logs/*.log` |
| Speed Layer Steam | `/tmp/kdd_streaming.log` |
| Speed Layer recomm. | `/tmp/kdd_recommender_streaming.log` |
| FastAPI | stdout (uvicorn) |
| Producers | stdout |
| Airflow | `0_infra/airflow_home/logs/dag_id=.../task_id=.../...log` |

### 5.3 Arranque completo (orden recomendado)

El sistema se arranca **de abajo a arriba**: primero la infraestructura
(HDFS, Kafka, Cassandra, Hive, Airflow), luego los esquemas, luego las
aplicaciones (FastAPI, dashboard) y, opcionalmente, el speed layer y
los producers de demo.

```bash
cd /home/hadoop/juegosKDD

# ── 1. Infraestructura (HDFS → Kafka → Cassandra → Hive → Airflow) ──
bash 0_infra/start_stack.sh
bash 0_infra/start_stack.sh --status         # verificar puertos abiertos

# ── 2. Esquemas (idempotente, solo la primera vez o tras un reset) ──
bash 2_kafka/setup_topics.sh                 # 7 topics Kafka
cqlsh -f 5_serving_layer/cassandra_schema.cql       # keyspace gaming_kdd
cqlsh -f 5_serving_layer/recommender_schema.cql     # keyspace gaming_recommender
beeline -u jdbc:hive2://localhost:10000 -n hadoop \
        -f 4_batch_layer/hive_schema.sql            # databases Hive

# ── 3. Datos (Kaggle → HDFS/Hive) — solo la primera vez ─────────────
python 0_data/kaggle/download_datasets.py
python 0_data/kaggle/validate_datasets.py
bash   1_ingesta/kaggle_to_hive/run_loader.sh

# ── 4. Pipeline KDD batch (genera modelo + recomendaciones) ─────────
bash 4_batch_layer/recommender/submit_kdd.sh        # ~5–10 min

# ── 5. Aplicaciones (API + dashboard) ───────────────────────────────
nohup /home/hadoop/juegosKDD/venv/bin/python \
      5_serving_layer/api/main.py \
      > 0_infra/logs/api.log 2>&1 &
echo $! > 0_infra/pids/api.pid

nohup bash -c 'cd 7_dashboard && npm run dev -- --host 0.0.0.0' \
      > 0_infra/logs/dashboard.log 2>&1 &
echo $! > 0_infra/pids/dashboard.pid

# ── 6. Speed Layer del recomendador (opcional, demo en tiempo real) ─
nohup bash 3_speed_layer/submit_streaming_recommender.sh \
      > 0_infra/logs/streaming_recommender.log 2>&1 &
echo $! > 0_infra/pids/streaming_recommender.pid

# ── 7. Producer sintético (opcional, alimenta el speed con user_id reales) ─
nohup /home/hadoop/juegosKDD/venv/bin/python \
      1_ingesta/api_producer/synthetic_interactions_producer.py \
      --rate 5 --users 200 --games 300 \
      > 0_infra/logs/synthetic_producer.log 2>&1 &
echo $! > 0_infra/pids/synthetic_producer.pid

# ── 8. Verificación ─────────────────────────────────────────────────
curl -s http://localhost:8000/health | jq .
curl -s http://localhost:8000/api/keyspaces | jq .
xdg-open http://localhost:5173      # dashboard
xdg-open http://localhost:8090      # Airflow UI
```

**Atajo rápido** (después de cargar datos al menos una vez):

```bash
# Infra + esquemas + apps usando los scripts del proyecto
bash 0_infra/start_stack.sh
bash 0_infra/start_pipeline.sh all     # producer + streaming + api + dashboard
```

> El script `0_infra/start_pipeline.sh all` lanza el **pipeline Steam
> live** (producer + spark_streaming_kdd + api + dashboard). El speed
> layer del **recomendador** y su producer sintético hay que lanzarlos
> aparte (pasos 6 y 7), porque son una rama distinta del proyecto.

### 5.4 Parada completa (orden inverso)

Se para **de arriba a abajo**: primero las aplicaciones (para evitar
errores de conexión cuando la infra cae), luego la infraestructura.

```bash
cd /home/hadoop/juegosKDD

# ── 1. Producers Python (sintético, freetogame, steamspy, steam) ────
pkill -TERM -f "synthetic_interactions_producer.py" 2>/dev/null
pkill -TERM -f "freetogame_producer.py"             2>/dev/null
pkill -TERM -f "steamspy_producer.py"               2>/dev/null
pkill -TERM -f "steam_producer.py"                  2>/dev/null

# ── 2. Speed Layer (driver Python + spark-submit Java en cascada) ───
pkill -TERM -f "streaming_recommender.py" 2>/dev/null
pkill -TERM -f "spark_streaming_kdd.py"   2>/dev/null
sleep 4
pkill -TERM -f "org.apache.spark.deploy.SparkSubmit" 2>/dev/null

# ── 3. Dashboard Vite ───────────────────────────────────────────────
pkill -TERM -f "node.*juegosKDD/7_dashboard.*vite" 2>/dev/null
pkill -TERM -f "sh -c vite"                         2>/dev/null

# ── 4. FastAPI (uvicorn :8000) ──────────────────────────────────────
pkill -TERM -f "uvicorn.*main:app"                              2>/dev/null
pkill -TERM -f "python.*5_serving_layer/api/main.py"            2>/dev/null

# Forzar (KILL) lo que siga vivo tras 5 s
sleep 5
pkill -KILL -f "synthetic_interactions_producer.py|freetogame_producer.py|steamspy_producer.py|steam_producer.py" 2>/dev/null
pkill -KILL -f "streaming_recommender.py|spark_streaming_kdd.py" 2>/dev/null
pkill -KILL -f "uvicorn.*main:app|5_serving_layer/api/main.py"   2>/dev/null
pkill -KILL -f "node.*juegosKDD/7_dashboard.*vite"               2>/dev/null

# ── 5. Infraestructura (Airflow → Hive → Cassandra → Kafka → HDFS) ──
bash 0_infra/stop_stack.sh

# ── 6. Verificación ─────────────────────────────────────────────────
jps                                              # no debe haber nada del proyecto
for p in 5173 8000 8090 9000 9042 9083 9092 10000; do
    ss -ltn | grep -q ":$p " && echo "  :$p ⚠ OCUPADO" || echo "  :$p libre"
done
```

**Atajo rápido**:

```bash
bash 0_infra/start_pipeline.sh stop   # producer + streaming + api + dashboard
bash 0_infra/stop_stack.sh            # infra
```

**Equivalencias por componente**:

| Componente | Cómo arrancar | Cómo parar |
|---|---|---|
| HDFS / Kafka / Cassandra / Hive / Airflow | `bash 0_infra/start_stack.sh` | `bash 0_infra/stop_stack.sh` |
| Solo Airflow del proyecto | (parte de `start_stack.sh`) | `bash 0_infra/stop_stack.sh airflow` |
| FastAPI | `bash 0_infra/start_pipeline.sh api` | `pkill -f "uvicorn.*main:app"` |
| Dashboard | `bash 0_infra/start_pipeline.sh dashboard` | `pkill -f "node.*juegosKDD/7_dashboard.*vite"` |
| Speed Layer Steam (analítica) | `bash 0_infra/start_pipeline.sh streaming` | `pkill -f spark_streaming_kdd.py` |
| Speed Layer recomendador | `bash 3_speed_layer/submit_streaming_recommender.sh &` | `pkill -f streaming_recommender.py` |
| Producer Steam | `bash 0_infra/start_pipeline.sh producer` | `pkill -f steam_producer.py` |
| Producer sintético (demo) | `python 1_ingesta/api_producer/synthetic_interactions_producer.py --rate 5 &` | `pkill -f synthetic_interactions_producer.py` |
| KDD batch (one-shot) | `bash 4_batch_layer/recommender/submit_kdd.sh` | (termina solo) |
| DAG Airflow | (Airflow Scheduler dispara cron) | (parar Airflow) |

### 5.5 Procedimientos comunes

```bash
# Limpiar tablas Cassandra (cuidado!)
cqlsh -e "TRUNCATE gaming_recommender.user_recommendations;"

# Re-entrenar el modelo ya
bash 4_batch_layer/recommender/submit_kdd.sh

# Forzar el DAG batch
curl -X POST -u airflow:airflow \
     http://localhost:8090/api/v1/dags/gaming_recommender_batch_pipeline/dagRuns \
     -H 'Content-Type: application/json' \
     -d '{"conf":{}}'

# Parar el speed layer
ps -ef | grep streaming_recommender | grep -v grep | awk '{print $2}' | xargs -r kill

# Producer sintético en background
nohup python 1_ingesta/api_producer/synthetic_interactions_producer.py --rate 10 \
     > /tmp/synth.log 2>&1 &
```

---

## 6. Despliegue

### 6.1 Producción local (single host)

Es el escenario actual: un host, Spark `local[*]`, Cassandra
`replication_factor=1`. **No** está pensado para alta disponibilidad,
pero sí para uso continuo en laboratorio.

Pasos:

1. `start_stack.sh` con `systemd` o `cron @reboot`.
2. `start_pipeline.sh` (FastAPI + producers + speed layer) con
   `nohup`/`systemd`.
3. Dashboard buildado: `npm run build` y servir `dist/` con nginx.
4. Reverse proxy (nginx/traefik) con TLS si se expone fuera del LAN.

### 6.2 Producción multi-host (referencia)

| Capa | Despliegue recomendado |
|---|---|
| Cassandra | Clúster 3 nodos, RF=3, consistency QUORUM. |
| Kafka | Clúster 3 nodos, particiones = 3-6 por topic. |
| Spark | YARN o Spark Standalone con 1 master + N workers. |
| Hive Metastore | Único, MySQL/Postgres como backend. |
| Airflow | Celery o Kubernetes executor; Postgres como metadata DB. |
| FastAPI | Múltiples réplicas detrás de un load balancer. |
| Dashboard | Build estático servido por CDN/nginx. |

Variables que pasarían por entorno: `CASSANDRA_HOST`, `KAFKA_BOOTSTRAP`,
`HIVE_METASTORE_URI`, `ANTHROPIC_API_KEY`, `AIRFLOW_URL`.

### 6.3 CI/CD (sugerido)

- Lint: `ruff check .`, `eslint src/`.
- Tests unitarios: `pytest 4_batch_layer/recommender/tests/`.
- Smoke test: arrancar el stack en docker-compose y `curl /health`.
- Build dashboard: `npm run build` con artefacto subido al CDN.

---

## 7. Troubleshooting

| Síntoma | Causa probable | Acción |
|---|---|---|
| `ModuleNotFoundError: No module named 'distutils'` en Spark | Python 3.12 sin `setuptools`. | `pip install setuptools` en el venv que usa `PYSPARK_PYTHON`. |
| `Invalid method name: get_table` al leer Hive desde Spark | Spark Hive client (2.3) ↔ Hive Metastore (4.2) incompatibles. | Usar `_hive_io.read_table()` (ya implementado). |
| FastAPI devuelve 503 en endpoints del recomendador | Cassandra abajo o keyspace `gaming_recommender` no existe. | `nodetool status`, `cqlsh -f 5_serving_layer/recommender_schema.cql`. |
| Speed layer no genera recs nuevas | El `user_id` no existe en `user_index_map` (cold-start permanente). | Usar `synthetic_interactions_producer.py` que samplea IDs reales. |
| Airflow `Connection error: OperationTimedOut` con `cqlsh` | Mezcla con el venv de Python del proyecto. | Invocar `cqlsh` con `env -i PATH=/usr/bin:$PATH cqlsh ...` o desactivar `VIRTUAL_ENV`. |
| Dashboard "API error 404" en `/api/recommendations/popular` | URL antigua con `/api/recommender/`. | Usar la actual: `/api/recommendations/popular`. |
| Kafka producer cuelga al cerrar | `producer.flush()` antes de `producer.close()`. | Ya implementado. |
| `kafka-python` rompe en Python 3.12 | `six.moves` removido. | Usar `kafka-python-ng` (ya en `requirements.txt`). |

---

## 8. Tests rápidos

```bash
# Sanity de los datasets
python 0_data/kaggle/validate_datasets.py

# Smoke test API
curl -s http://localhost:8000/health | jq .
curl -s http://localhost:8000/api/keyspaces | jq .
curl -s http://localhost:8000/api/recommendations/popular?limit=3 | jq .

# Smoke test pipeline (sin Cassandra)
bash 4_batch_layer/recommender/submit_kdd.sh --no-cassandra

# Smoke test inyección + speed layer
curl -X POST http://localhost:8000/api/ingest/interaction \
  -H "Content-Type: application/json" \
  -d '{"user_id":"151603712","game_title":"dota 2","event_type":"play","hours":3.0,"recommended":true}'
sleep 60
cqlsh -e "SELECT recommended_game, hybrid_score, source FROM gaming_recommender.user_recommendations WHERE user_id='151603712' LIMIT 5;"
```

---

## 9. Mapa de variables de entorno

| Variable | Default | Usada por |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | producers, Spark streaming, FastAPI |
| `CASSANDRA_HOST` | `localhost` | FastAPI, Spark batch, Spark streaming |
| `CASSANDRA_PORT` | `9042` | igual |
| `CASSANDRA_KEYSPACE` | `gaming_kdd` | FastAPI |
| `STEAM_API_KEY` | — | `steam_producer.py` |
| `KAGGLE_USERNAME` / `KAGGLE_KEY` | — | `download_datasets.py` |
| `ANTHROPIC_API_KEY` | — | agentes IA |
| `RAWG_API_KEY` | — | (opcional, futuro) |
| `API_HOST` | `0.0.0.0` | FastAPI |
| `API_PORT` | `8000` | FastAPI |
| `AIRFLOW_URL` | `http://localhost:8090` | monitor agent |
| `AIRFLOW_USER` / `AIRFLOW_PASS` | `airflow / airflow` | monitor agent |
| `RECOMMENDER_DAG_ID` | `gaming_recommender_batch_pipeline` | monitor agent |
| `PRODUCER_POLL_SEC` | `30` | api_producer |
| `JAVA_HOME` | `/usr/lib/jvm/java-21-openjdk-amd64` | start_stack.sh, submits |
| `SPARK_HOME` | `/opt/spark` | submits |
| `KAFKA_HOME` | `/opt/kafka` | start_stack.sh |
| `HIVE_HOME` | `~/apache-hive-4.2.0-bin` | start_stack.sh |
| `HADOOP_HOME` | `/opt/hadoop` | start_stack.sh |
| `AIRFLOW_HOME` | `0_infra/airflow_home` | aislamiento de DAGs |
| `AIRFLOW_API_PORT` | `8090` | aislamiento de puertos |
