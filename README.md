# KDD λ GAMING — Pipeline real Steam → Kafka → Spark → Cassandra → React

Implementación completa de la metodología **KDD** (Knowledge Discovery in
Databases) sobre una **Arquitectura Lambda** aplicada a analítica de videojuegos,
con datos reales provenientes de la Steam Web API + SteamSpy.

## Prerequisitos (ya instalados en Linux Mint)
- Apache Kafka     → `localhost:9092`
- Apache Spark     → `$SPARK_HOME`
- Apache Hive      → `localhost:10000`
- Apache Cassandra → `localhost:9042`
- Apache NiFi      → `http://localhost:8080/nifi`
- Apache Airflow   → `$AIRFLOW_HOME`
- Python 3.8+ y Node.js 18+

## Estructura del proyecto

```
kdd-lambda-gaming/
├── 0_data/kaggle/                      # Descarga/validación datasets Kaggle
├── 1_ingesta/
│   ├── nifi_flow/ + nifi_templates/    # NiFi (Steam + plantilla recomendador)
│   ├── producer/                       # Steam → Kafka
│   ├── kaggle_to_hive/                 # CSV Kaggle → Hive gaming_recommender
│   └── api_producer/                   # FreeToGame / SteamSpy → Kafka
├── 2_kafka/setup_topics.sh             # Topics gaming.* (incl. user.interactions)
├── 3_speed_layer/                      # Spark Streaming Steam + recomendador
├── 4_batch_layer/                      # Batch Steam + pipeline ALS/TF-IDF híbrido
├── 5_serving_layer/
│   ├── cassandra_schema.cql            # gaming_kdd + gaming_recommender
│   └── api/                            # FastAPI :8000 (ambos keyspaces)
├── 6_orchestration/dags/               # DAGs Airflow (Steam + recomendador opcional)
└── 7_dashboard/                        # React + Vite :5173
```

## Paso 1 — Configurar credenciales

```bash
cp .env.example .env
# Editar .env con tu STEAM_API_KEY (https://steamcommunity.com/dev/apikey)
```

El dashboard usa el **agente monitor heurístico** vía FastAPI (`/api/agent/monitor`)
y explicaciones locales (`/api/agent/recommend/{user_id}`), sin LLM externo.

## Paso 2 — Inicializar Kafka

```bash
bash 2_kafka/setup_topics.sh
# Verificar:
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Paso 3 — Inicializar Cassandra

Si el `cqlsh` de tu instalación falla en Python 3.12 (p. ej. `asyncore` / libev), usa el **venv** y el aplicador con `cassandra-driver`:

```bash
source venv/bin/activate
python3 0_infra/apply_cassandra_schema.py
python3 0_infra/apply_cassandra_schema.py --exists gaming_kdd && echo OK gaming_kdd
```

Equivalente clásico (si tu `cqlsh` funciona):

```bash
cqlsh -f 5_serving_layer/cassandra_schema.cql
```

## Paso 4 — Inicializar Hive

```bash
hive -f 4_batch_layer/hive_schema.sql
hive -e "SHOW TABLES IN gaming_kdd;"
```

## Paso 5 — Arrancar el Producer (ingesta Steam → Kafka)

```bash
cd 1_ingesta/producer
pip install -r requirements.txt
python steam_producer.py
# Logs esperados:
# ✓ Counter-Strike 2        | players=1,234,567 | tier=massive  | health=87.3
```

Alternativamente puedes usar NiFi: arranca NiFi, importa el template de
`1_ingesta/nifi_flow/gaming_pipeline.xml` (o reproduce el flow siguiendo los
comentarios del archivo) y activa el Process Group.

## Paso 6 — Arrancar el Speed Layer (Spark Streaming)

```bash
bash 3_speed_layer/submit_streaming.sh
# O manualmente:
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 \
  3_speed_layer/spark_streaming_kdd.py
```

## Paso 7 — Registrar el DAG en Airflow

```bash
cp 6_orchestration/dags/gaming_kdd_batch.py $AIRFLOW_HOME/dags/
cp 6_orchestration/dags/_daily_report.py    $AIRFLOW_HOME/dags/
# Si lo prefieres, enlaza la carpeta completa:
ln -sfn $(pwd)/6_orchestration/dags/* $AIRFLOW_HOME/dags/

# Primera ejecución manual:
airflow dags trigger gaming_kdd_batch_pipeline
```

Para ejecutar el batch sin Airflow (debugging):

```bash
bash 4_batch_layer/submit_batch.sh 2026-04-20
```

## Paso 8 — Arrancar la REST API (Serving Layer)

```bash
cd 5_serving_layer/api
pip install -r requirements.txt
python main.py
# Swagger UI: http://localhost:8000/docs
```

## Paso 9 — Arrancar el Dashboard React

```bash
cd 7_dashboard
npm install
npm run dev
# Dashboard: http://localhost:5173
```

## Verificación del flujo extremo a extremo

```bash
# 1) Ver mensajes llegando a Kafka
$KAFKA_HOME/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic gaming.events.raw \
  --from-beginning | head -5

# 2) Ver ventanas agregadas en Cassandra
cqlsh -e "SELECT game, avg_players, player_tier \
         FROM gaming_kdd.player_windows LIMIT 5;"

# 3) Healthcheck de la API
curl http://localhost:8000/health
curl http://localhost:8000/api/realtime | python3 -m json.tool | head -30
```

## Mapeo KDD ↔ Lambda

| Fase KDD        | Dónde ocurre                                    | Tecnología              |
| --------------- | ----------------------------------------------- | ----------------------- |
| Selection       | Lectura del topic Kafka / partición Hive        | Kafka, Hive             |
| Preprocessing   | `.filter(...)` en Spark Streaming / Batch       | Spark                   |
| Transformation  | `review_tier`, `engagement_index`, `market_seg` | Spark SQL               |
| Mining          | `.groupBy(...).agg(...)` ventanas y daily       | Spark                   |
| Evaluation      | Reglas en `foreachBatch` / post-aggregation     | Spark + Python          |
| **Serving**     | Persistencia para consumo                       | Cassandra + FastAPI     |

## Obtener API Keys gratuitas

- **Steam Web API** — https://steamcommunity.com/dev/apikey (requiere login Steam)
- **SteamSpy / FreeToGame** — sin key (respetar rate limits)
- **Kaggle** — `python3 0_data/kaggle/download_datasets.py` usa **kagglehub** por defecto; credenciales: `KAGGLE_API_TOKEN` en `.env`, o `~/.kaggle/kaggle.json`, o `KAGGLE_USERNAME` + `KAGGLE_KEY`
- **RAWG** (opcional) — https://rawg.io/apidocs

## Notas de producción

1. El producer respeta el rate limit de SteamSpy (`time.sleep(1.1)`).
2. El Spark Streaming usa `outputMode("update")` con watermark de 10 min.
3. FastAPI usa `ALLOW FILTERING` en Cassandra — en prod conviene añadir
   materialized views o índices secundarios adicionales.
4. El dashboard hace polling cada 15s (configurable en `useEffect` de `App.jsx`).
5. Las credenciales van en `.env` (cargadas vía `python-dotenv` / `import.meta.env`).
6. Las explicaciones del recomendador pasan por FastAPI (`/api/agent/recommend`) con
   reglas locales; no se expone ninguna clave de LLM en el frontend.
