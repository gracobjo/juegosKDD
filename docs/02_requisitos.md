# Requisitos — KDD λ Gaming

Documento de requisitos del sistema `juegosKDD`. Se clasifican en
**funcionales (RF)**, **no funcionales (RNF)**, **de datos (RD)** y **de
entorno / infraestructura (RE)**. Todos los requisitos se derivan de
casos de uso en `01_casos_de_uso.md`.

---

## 1. Requisitos funcionales

### 1.1 Recomendador híbrido

| ID | Requisito | Prioridad |
|---|---|---|
| RF-01 | El sistema generará, para cada `user_id` con historial, un **top-K hybrid** (CF + CB) combinando ALS y TF-IDF con peso α configurable (default **α=0.7**). | ALTA |
| RF-02 | El sistema proporcionará un **fallback cold-start** (top global por horas jugadas) cuando el usuario no tenga historial. | ALTA |
| RF-03 | El sistema expondrá un endpoint **REST** para consultar las recomendaciones por usuario con `limit` configurable (1-50). | ALTA |
| RF-04 | El sistema proporcionará búsqueda de **juegos similares** por título usando similitud coseno TF-IDF. | MEDIA |
| RF-05 | El sistema almacenará la **procedencia** de cada recomendación (`batch` vs `speed_layer`) y su componente CF/CB. | ALTA |
| RF-06 | El sistema **recalculará** en tiempo real (30 s) las recomendaciones de los usuarios cuyos eventos lleguen por Kafka. | ALTA |

### 1.2 Analítica Steam en vivo

| ID | Requisito | Prioridad |
|---|---|---|
| RF-07 | El sistema ingestará periódicamente (60 s) snapshots de Steam Web API + SteamSpy. | ALTA |
| RF-08 | El sistema generará ventanas de **5 minutos** con `avg/max/min players`, review score y health score. | ALTA |
| RF-09 | El sistema clasificará juegos en tiers **massive / popular / active / niche** a partir de `current_players`. | MEDIA |
| RF-10 | El sistema producirá un resumen diario (`game_stats_daily`) con ranking y `growth_rate`. | ALTA |

### 1.3 KDD / Evaluación

| ID | Requisito | Prioridad |
|---|---|---|
| RF-11 | El sistema implementará las **5 fases KDD** (Selection, Preprocessing, Transformation, Mining, Evaluation) y las reflejará como pasos discretos del código. | ALTA |
| RF-12 | El sistema calculará y persistirá **RMSE**, **Precision@K** y **Recall@K** (K=10 por defecto) por cada run. | ALTA |
| RF-13 | El sistema detectará **anomalías** (horas excesivas, engagement bajo, drops de jugadores) y las grabará en `kdd_insights` con severidad. | MEDIA |

### 1.4 Orquestación

| ID | Requisito | Prioridad |
|---|---|---|
| RF-14 | El sistema orquestará dos DAGs diarios en Airflow: `gaming_kdd_batch_pipeline` (02:00 UTC) y `gaming_recommender_batch_pipeline` (02:30 UTC). | ALTA |
| RF-15 | Los DAGs validarán la disponibilidad de Kafka, Cassandra y Hive antes de lanzar el pipeline. | ALTA |
| RF-16 | Los DAGs escribirán logs con el **ID de ejecución** y el nº de filas generadas en cada tabla. | MEDIA |
| RF-17 | El sistema permitirá **re-ejecutar manualmente** cualquier fase o el pipeline completo vía scripts shell. | ALTA |

### 1.5 Agentes IA

| ID | Requisito | Prioridad |
|---|---|---|
| RF-18 | El sistema incluirá 4 agentes REST: **Explorer**, **KDD**, **Recommender**, **Monitor**. | MEDIA |
| RF-19 | Si hay `ANTHROPIC_API_KEY`, los agentes usarán Claude; si no, degradarán a una **explicación heurística**. | ALTA |
| RF-20 | El agente Monitor podrá **disparar** un `dagRun` vía REST de Airflow si detecta drift y `auto_trigger=true`. | MEDIA |

### 1.6 Dashboard

| ID | Requisito | Prioridad |
|---|---|---|
| RF-21 | El dashboard React ofrecerá 6 pestañas: **Tiempo Real**, **Histórico**, **Recomendaciones**, **Agentes IA**, **KDD Insights**, **λ Arquitectura**. | ALTA |
| RF-22 | El dashboard hará **polling** cada 15 s a los endpoints activos de la pestaña visible. | MEDIA |
| RF-23 | El dashboard mostrará el **estado de conexión** con la API (indicador verde/rojo). | MEDIA |
| RF-24 | El dashboard permitirá consultar recomendaciones por `user_id` y juegos similares por título. | ALTA |

---

## 2. Requisitos no funcionales

### 2.1 Rendimiento

| ID | Requisito | Medida |
|---|---|---|
| RNF-01 | El endpoint `/api/recommendations/{user_id}` responderá en **< 300 ms** para el p95 en volúmenes de 200K interacciones. | latencia |
| RNF-02 | El **Speed Layer** procesará al menos **1.000 eventos / minuto** con trigger de 30 s. | throughput |
| RNF-03 | El **Batch KDD** completo (200K × 5K juegos × 400K reviews) terminará en **< 30 min** sobre hardware local con 16 GB RAM y 8 cores. | tiempo total |
| RNF-04 | Las queries Cassandra por `user_id` serán **partition-key** y no usarán `ALLOW FILTERING` salvo en endpoints de métricas. | tipo de query |

### 2.2 Disponibilidad y tolerancia a fallos

| ID | Requisito |
|---|---|
| RNF-05 | El pipeline tolerará la **ausencia de Claude API**: los agentes IA deben degradar a explicaciones heurísticas. |
| RNF-06 | El pipeline tolerará la **ausencia de credenciales Kaggle**: generará datasets sintéticos con la misma forma y marcará el archivo `_SYNTHETIC`. |
| RNF-07 | El speed layer usará **checkpointing** (`/tmp/kdd_recommender_checkpoint/interactions`) para retomar offsets tras caídas. |
| RNF-08 | Las tablas Cassandra con alto churn (`player_windows`, `kdd_insights`, `user_recommendations`, `user_history_cache`) aplicarán **TTL** (24 h - 30 días). |

### 2.3 Mantenibilidad y trazabilidad

| ID | Requisito |
|---|---|
| RNF-09 | Todos los scripts Python usarán **logging** con timestamp, nivel y módulo. |
| RNF-10 | Los jobs Spark escribirán su nombre (`appName`) con sufijo de fecha para facilitar el seguimiento en la UI de Spark. |
| RNF-11 | Todas las configuraciones (hosts, puertos, claves API) se leerán de `.env` — **nunca** hard-coded. |
| RNF-12 | Los esquemas de tablas (`*.cql`, `*.sql`) serán **versionables** y reproducibles con `cqlsh -f` / `hive -f`. |

### 2.4 Seguridad

| ID | Requisito |
|---|---|
| RNF-13 | Las claves API (`ANTHROPIC_API_KEY`, `KAGGLE_KEY`, `STEAM_API_KEY`) permanecerán en `.env` excluido de Git (`.gitignore`). |
| RNF-14 | El dashboard **no** incrustará la clave Anthropic por defecto en builds de producción; debe recibirla por un backend. |
| RNF-15 | FastAPI restringirá **CORS** a `localhost:5173` y `localhost:3000` por defecto. |

### 2.5 Escalabilidad

| ID | Requisito |
|---|---|
| RNF-16 | Kafka usará al menos **3 particiones** para los topics de alto volumen (`gaming.events.raw`, `gaming.events.processed`, `gaming.user.interactions`). |
| RNF-17 | Spark Batch estará diseñado para **escalar horizontalmente** con `--master yarn` o `spark://master:7077` sin cambios de código (solo configuración). |
| RNF-18 | Las tablas Cassandra usarán **partition keys naturales** (`user_id`, `game`, `appid`) para permitir sharding lineal. |

---

## 3. Requisitos de datos

| ID | Requisito |
|---|---|
| RD-01 | El dataset `steam_games_behaviors.csv` tendrá como mínimo las columnas: `user_id, game_title, behavior, value`. |
| RD-02 | El dataset `steam_reviews.csv` tendrá como mínimo las columnas: `app_id, app_name, review_text, recommendation, …`. |
| RD-03 | El campo `hours_played` se derivará filtrando `behavior='play'` y castando `value` a `double`. |
| RD-04 | El `rating implícito` para ALS será `log1p(hours_played)` (normaliza la cola larga). |
| RD-05 | Los perfiles textuales por juego concatenarán hasta **200 reviews** por juego (configurable). |
| RD-06 | Los nulos se filtrarán en preprocessing; el pipeline no tolerará `user_id IS NULL` ni `game_title IS NULL`. |
| RD-07 | Se validará que hay al menos **1 fila** en cada tabla clave (`user_behaviors`, `user_reviews`, `user_recommendations`). |

---

## 4. Requisitos de entorno

### 4.1 Software base (ya instalado en el equipo)

| Componente | Versión | Ruta |
|---|---|---|
| Apache Kafka | 3.x (KRaft) | `/opt/kafka` |
| Apache Spark | 3.5.1 | `/opt/spark` |
| Apache Hadoop / HDFS | 3.x | `/opt/hadoop` |
| Apache Hive | 4.2.0 | `~/apache-hive-4.2.0-bin` |
| Apache Cassandra | 4.x | `~/smart_energy/cassandra` |
| Apache Airflow | 3.x | venv `~/smart_energy/venv`; `AIRFLOW_HOME=0_infra/airflow_home` |
| Apache NiFi (opcional) | 2.6.0 | `~/smart_energy/nifi-2.6.0` |
| Python | 3.12 | venv ver arriba |
| Node.js | 18+ | dashboard Vite |
| Java | 21 (OpenJDK) | `/usr/lib/jvm/java-21-openjdk-amd64` |

### 4.2 Servicios activos y puertos

| Servicio | Puerto | Bind |
|---|---|---|
| HDFS NameNode | 9000 | `nodo1` / `127.0.1.1` |
| Kafka broker | 9092 | `localhost` / `nodo1` |
| Cassandra CQL | 9042 | `127.0.0.1` |
| Hive Metastore | 9083 | `localhost` |
| HiveServer2 | 10000 | `localhost` |
| Airflow API | **8090** | `0.0.0.0` (aislado de otros proyectos) |
| FastAPI | 8000 | `0.0.0.0` |
| Dashboard Vite | 5173 | `0.0.0.0` |

### 4.3 Dependencias Python (ver `requirements.txt`)

- `kafka-python-ng==2.2.3` (fork mantenido; `kafka-python` rompe en Python 3.12)
- `fastapi==0.115.4`, `uvicorn[standard]==0.32.0`, `pydantic==2.9.2`
- `cassandra-driver==3.29.2`
- `python-dotenv==1.0.1`, `requests==2.32.3`
- `pyspark` NO se instala en el venv (lo provee `/opt/spark`).
- `numpy`, `setuptools`, `pyarrow` (para el venv del pipeline, instalados por `0_infra/setup_venv.sh`).

### 4.4 Claves API

| Variable | Obligatoria | Uso |
|---|---|---|
| `KAGGLE_USERNAME` / `KAGGLE_KEY` | No (fallback sintético) | Descargar datasets reales de Steam. |
| `STEAM_API_KEY` | Sí para pipeline Steam live | Steam Web API (AppList/PlayerCount). |
| `ANTHROPIC_API_KEY` | No (fallback heurístico) | Respuestas de los agentes IA. |
| `RAWG_API_KEY` | Opcional | Enriquecido adicional del catálogo. |

### 4.5 Packages Spark (runtime)

- `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` → ingesta desde Kafka.
- `com.datastax.spark:spark-cassandra-connector_2.12:3.5.1` → escritura a Cassandra.

Se pasan en `--packages` desde los scripts `submit_*.sh` y DAGs.

---

## 5. Suposiciones y limitaciones

- La instalación ejecuta Spark en modo **`local[*]`** y Cassandra con `replication_factor=1`: válido para desarrollo / laboratorio KDD, **no** es HA.
- El cliente Hive embebido de Spark 3.5.1 es 2.3.9 y es **incompatible** con el Metastore 4.2.0; por eso el proyecto evita `enableHiveSupport()` y usa un helper (`_hive_io.py`) que escribe Parquet directo en HDFS y registra tablas externas vía `beeline`.
- El sistema comparte **la instalación física** de Cassandra, venv y NiFi con el proyecto `smart_energy` (documentado en `start_stack.sh`), pero **no comparte keyspaces, DAGs, AIRFLOW_HOME ni puertos**.
- Los resultados del modelo con datos reales esperados (runs observadas):

  | Métrica | Sintético | Kaggle real |
  |---|---|---|
  | RMSE | ~2.58 | **2.146** |
  | Precision@10 | ~0.014 | **0.0107** |
  | Recall@10 | ~0.040 | **0.0915** |
  | Users cubiertos | ~1.000 | **9.971** |
  | Games cubiertos | ~60 | **667** |
