# Glosario y diccionario de acrónimos — KDD λ Gaming

Diccionario de los términos, siglas y acrónimos que aparecen en el
proyecto, ordenados alfabéticamente. Se incluye **expansión**,
**descripción** y, cuando aplica, **dónde se materializa** en el
código.

---

## A

**ADR** — *Architecture Decision Record*. Registro corto de una
decisión arquitectónica con su motivo. En este proyecto los ADRs van
inline en `docs/03_diseno.md` § 7.

**Airflow** — Orquestador de workflows (Apache). En este proyecto
ejecuta los DAGs `gaming_kdd_batch_pipeline` y
`gaming_recommender_batch_pipeline`. Puerto **8090**.

**ALS** — *Alternating Least Squares*. Algoritmo de factorización
matricial usado para Filtrado Colaborativo. Implementación:
`pyspark.ml.recommendation.ALS` en
`4_batch_layer/recommender/collaborative_filtering.py`.

**API** — *Application Programming Interface*. En este proyecto se
refiere principalmente a la **REST API** servida por FastAPI en el
puerto 8000.

**appid** — Identificador numérico de un juego en Steam (p. ej.
`570 = Dota 2`). Se usa como clave en `gaming_kdd.player_windows` y
`game_stats_daily`.

---

## B

**Batch Layer** — Capa de la Arquitectura Lambda que procesa **toda
la historia** de datos en jobs periódicos (lentos pero exactos). Aquí
se materializa con Spark Batch + Hive (`4_batch_layer/`).

**Beeline** — Cliente CLI JDBC de Hive. Se usa en `_hive_io.py` para
crear tablas EXTERNAL sin pasar por el cliente embebido de Spark.

---

## C

**CB** — *Content-Based*. Filtrado basado en contenido. En este
proyecto se calcula con TF-IDF + similitud coseno
(`content_based.py`).

**Cassandra** — Base de datos NoSQL distribuida (Apache). Hace de
**Serving Layer** en este proyecto. Puerto **9042**. Dos keyspaces:
`gaming_kdd` y `gaming_recommender`.

**CF** — *Collaborative Filtering*. Filtrado Colaborativo. Aquí se
implementa con **ALS implícito** sobre `log1p(hours_played)`.

**Checkpointing** — Mecanismo de Spark Structured Streaming para
guardar el offset de Kafka procesado y poder retomar tras un fallo.
Path en este proyecto: `/tmp/kdd_recommender_checkpoint/interactions`.

**cold-start** — Problema de los recomendadores cuando un usuario o
un ítem **no tiene historia**. El sistema lo resuelve con
`popular_games_fallback` (top global).

**CORS** — *Cross-Origin Resource Sharing*. Política HTTP que controla
qué orígenes pueden llamar a la API. Configurada en
`5_serving_layer/api/main.py` con `allow_origins`.

**cqlsh** — Cliente CLI de Cassandra (`cqlsh -e "SELECT ..."` o
`cqlsh -f schema.cql`).

**CRUD** — *Create / Read / Update / Delete*. La API expone solo R
(GET) y un C (POST `/api/ingest/interaction`).

---

## D

**DAG** — *Directed Acyclic Graph*. Definición de workflow en Airflow.
Aquí: `gaming_kdd_batch_pipeline` y `gaming_recommender_batch_pipeline`.

**Drift** — Degradación del modelo a lo largo del tiempo (los datos
nuevos siguen una distribución distinta a los del entrenamiento). El
agente Monitor lo detecta comparando RMSE/Precision actuales contra
una baseline.

---

## E

**ETL** — *Extract / Transform / Load*. Patrón clásico de movimiento
de datos. Aquí se materializa en `1_ingesta/kaggle_to_hive/loader.py`
y `4_batch_layer/kafka_to_hive.py`.

**EXTERNAL TABLE** (Hive) — Tabla cuyo dato físico vive fuera del
warehouse de Hive (en HDFS). Se usa para que Spark escriba Parquet
directo y Hive solo guarde el esquema.

---

## F

**FastAPI** — Framework Python para APIs REST. Sirve el Serving Layer
en el puerto 8000.

**FreeToGame** — API pública de catálogos de juegos free-to-play
(`https://www.freetogame.com/api`). Producer:
`1_ingesta/api_producer/freetogame_producer.py`.

---

## H

**HashingTF** — Transformer de Spark ML que mapea n-gramas a vectores
sparse usando hashing trick. Se usa en el pipeline TF-IDF (`numFeatures=2^14`).

**HDFS** — *Hadoop Distributed File System*. Sistema de ficheros
distribuido. Aquí guarda los Parquet de Hive y los modelos serializados
(`als_model`, `tfidf_pipeline`).

**Hive** — Almacén de datos estructurado sobre HDFS (Apache). Se usa
para persistir los datos del batch layer. Metastore en `:9083`.

**HS2** — *HiveServer2*. Servidor JDBC de Hive en `:10000`.

**Hybrid recommender** — Recomendador que combina **CF + CB** con un
peso α. Implementación: `4_batch_layer/recommender/hybrid_recommender.py`.

---

## I

**IDF** — *Inverse Document Frequency*. Componente del TF-IDF que
penaliza palabras frecuentes en el corpus. Implementación en
`pyspark.ml.feature.IDF`.

**Implicit feedback** — Feedback no explícito (vistas, horas jugadas,
clicks) usado por ALS implícito. En este proyecto:
`rating = log1p(hours_played)`.

**Insight** — Alerta o conclusión generada por el pipeline (anomalía,
pico, drift). Tabla `kdd_insights` con severity ∈
{`info, warning, alert, critical`}.

---

## J

**JDBC** — *Java Database Connectivity*. Lo usa Beeline para
conectarse a HiveServer2 (`jdbc:hive2://localhost:10000`).

**JPS** — *Java Process Status*. Comando que lista procesos Java
(útil para ver si HDFS, Kafka, Hive, Spark están vivos).

---

## K

**Kafka** — Plataforma de mensajería distribuida (Apache). Capa de
ingesta en tiempo real. Puerto **9092** en modo KRaft.

**Kaggle** — Plataforma de datasets de ML. Aquí se descargan
`tamber/steam-video-games` y `luthfim/steam-reviews-dataset`.

**KDD** — *Knowledge Discovery in Databases*. Proceso de 5 fases
(Selection → Preprocessing → Transformation → Mining → Evaluation).
Es el **proceso teórico** que estructura todo el pipeline.

**Keyspace** — Equivalente a "base de datos" en Cassandra. Aquí:
`gaming_kdd` y `gaming_recommender`.

**KRaft** — *Kafka Raft Metadata mode*. Modo de Kafka **sin
Zookeeper** (Kafka maneja su propio quórum con Raft).

---

## L

**Lambda Architecture** — Arquitectura de procesamiento de datos con
**dos ramas**: Batch Layer (precisa pero lenta) + Speed Layer (rápida
pero aproximada), unificadas en un Serving Layer común.

**LLM** — *Large Language Model*. Modelo de lenguaje grande. Aquí:
**Claude** (Anthropic), opcional, vía
`5_serving_layer/api/agents/_claude.py`.

**log1p** — Función `log(1+x)`. Usada para normalizar la cola larga
de `hours_played` antes de pasarlo a ALS como rating implícito.

---

## M

**Metastore (Hive)** — Servicio que guarda los metadatos (esquemas,
particiones, propiedades) de las tablas Hive. Puerto **9083**.

**Micro-batch** — Modelo de procesamiento de Spark Structured
Streaming: en lugar de procesar evento a evento, procesa lotes
pequeños cada T segundos. Aquí: **30 s**.

**Mining** (KDD fase 4) — Aplicación del algoritmo de aprendizaje. En
este proyecto: **ALS + TF-IDF + Hybrid**.

**MLlib** — Librería de Machine Learning de Spark.
`pyspark.ml.recommendation.ALS`, `pyspark.ml.feature.IDF`, etc.

**MSCK REPAIR TABLE** — Comando Hive para que el Metastore recargue
las particiones detectadas en HDFS. Lo usa
`gaming_kdd_batch_pipeline.hive_repair_partitions`.

---

## N

**NameNode** — Master de HDFS (puerto **9000**). Guarda el árbol de
ficheros y la ubicación de los bloques.

**NiFi** — Herramienta visual de integración de datos (Apache).
Opcional en este proyecto; se usa solo si se quiere reemplazar al
producer Python por un flujo gráfico.

**nodo1** — Hostname del único nodo del clúster local
(equivalente a `127.0.1.1`).

---

## P

**P@K** / **Precision@K** — Métrica de calidad del recomendador:
fracción de los top-K recomendados que el usuario realmente "consume"
en el test set. Default `K=10`.

**Parquet** — Formato columnar comprimido (Snappy). Es el formato
físico de todas las tablas Hive del proyecto.

**Polling** — Estrategia del dashboard: cada **15 segundos** llama a
los endpoints visibles para refrescar.

**Producer (Kafka)** — Cliente que publica en un topic. En este
proyecto: `steam_producer.py`, `freetogame_producer.py`,
`steamspy_producer.py`, `synthetic_interactions_producer.py`.

**PySpark** — Binding Python de Spark.

---

## R

**R@K** / **Recall@K** — Métrica de calidad del recomendador:
fracción de los ítems "relevantes" que aparecen en el top-K.

**RDD** — *Resilient Distributed Dataset*. Estructura de datos baja
de Spark. Aquí casi no se usa: el código usa DataFrame API.

**REST** — *Representational State Transfer*. Estilo arquitectónico
de la API (FastAPI).

**RMSE** — *Root Mean Squared Error*. Métrica de regresión usada para
evaluar la predicción del rating implícito de ALS.

---

## S

**Serving Layer** — Capa de servicio de la Arquitectura Lambda. En
este proyecto: **Cassandra + FastAPI + dashboard React**.

**Snappy** — Algoritmo de compresión usado en los Parquet
(`parquet.compression=SNAPPY`).

**Spark** — Motor de procesamiento distribuido (Apache). Versión
**3.5.1** en `/opt/spark`.

**Spark Structured Streaming** — API de streaming de Spark basada en
DataFrames y micro-batches. Soporta `outputMode("update" | "append" |
"complete")` con watermark.

**Speed Layer** — Capa de la Arquitectura Lambda que produce vistas
rápidas (incrementales) en tiempo real con Spark Streaming.

**Steam Web API** — API oficial de Valve para datos de jugadores
y juegos. Necesita `STEAM_API_KEY`.

**SteamSpy** — API gratuita con métricas agregadas de Steam (sin
key, rate limit 1 req/s para `all`, 4 req/s para los demás endpoints).

**StringIndexer** — Transformer de Spark ML que mapea cadenas a
índices numéricos (necesario para ALS, que solo acepta enteros). Aquí
crea `user_index_map` y `game_index_map`.

---

## T

**TF-IDF** — *Term Frequency × Inverse Document Frequency*.
Representación vectorial de texto que pondera cada término por su
relevancia. Es el corazón del filtrado **content-based**.

**Tier (player tier)** — Categoría asignada a un juego según su
número de jugadores actuales:

| Tier | Umbral |
|---|---|
| `massive` | ≥ 100.000 |
| `popular` | ≥ 10.000 |
| `active`  | ≥ 1.000 |
| `niche`   | < 1.000 |

**Topic (Kafka)** — Cola de mensajes nominada. Aquí: 7 topics, ver
`docs/03_diseno.md` § 4.3.

**TTL** — *Time To Live*. Tiempo en segundos tras el cual Cassandra
borra automáticamente la fila. Aquí: 7d para `player_windows`, 24h
para `user_recommendations`, 30d para `kdd_insights`.

---

## U

**UDF** — *User Defined Function*. Función Python registrada en Spark
SQL. (En este proyecto se evitan por rendimiento; casi todo se hace
con funciones nativas).

**Uvicorn** — Servidor ASGI que ejecuta FastAPI (`uvicorn main:app
--host 0.0.0.0 --port 8000`).

---

## V

**Venv** — *Virtual environment* de Python. Aquí en
`/home/hadoop/juegosKDD/venv`. `PYSPARK_PYTHON` debe apuntar a este
venv para que numpy/sklearn estén disponibles en los executors.

**Vite** — Bundler/dev server de React usado por el dashboard. Puerto
**5173**. Tiene un proxy `/api → :8000` configurado en
`vite.config.js`.

---

## W

**Watermark** — Concepto de Spark Structured Streaming: tiempo
máximo que se espera por un evento "tardío" antes de descartarlo.
Aquí: 5-10 minutos según el job.

---

## Y

**YARN** — *Yet Another Resource Negotiator*. Resource manager de
Hadoop. **No** se usa en este proyecto (Spark corre en `local[*]`),
pero está pensado el `--master yarn` para producción.

---

## Z

**Zookeeper** — Coordinador distribuido. **No** se usa en este
proyecto: Kafka corre en modo **KRaft**.

---

## Símbolos y nombres especiales

| Símbolo | Significado |
|---|---|
| **λ** (Lambda) | Arquitectura Lambda. Aparece en el logo del dashboard ("KDD λ GAMING"). |
| **α** (alpha) | Peso del componente CF en el score híbrido. Default 0.7. |
| `gaming.events.raw` | Topic Kafka principal del pipeline original (Steam). |
| `gaming.user.interactions` | Topic Kafka del recomendador (eventos user-game). |
| `_hive_io.py` | Helper crítico que evita la incompatibilidad Spark↔Hive Metastore. |
| `gaming_kdd` | Keyspace Cassandra del pipeline original. |
| `gaming_recommender` | Keyspace Cassandra del sistema híbrido de recomendación. |
