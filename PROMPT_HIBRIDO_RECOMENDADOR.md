# PROMPT AGÉNTICO COMPLETO
# Sistema Híbrido de Recomendación de Videojuegos
# KDD + Arquitectura Lambda + Collaborative Filtering + Content-Based
# Stack: Linux Mint · Kafka · Spark · Hive · Cassandra · NiFi · Airflow · FastAPI · React
# Datasets: Kaggle (Steam Video Games + Steam Reviews) + APIs en tiempo real (SteamSpy + FreeToGame)

---

## ROL Y MODO DE OPERACIÓN

Actúa como un ingeniero de datos sénior con experiencia en sistemas de recomendación,
arquitecturas Lambda y pipelines KDD. Tienes capacidad de ejecución autónoma:

- Escribe código completo y funcional, sin placeholders ni TODOs
- Ejecuta comandos bash para verificar que cada componente funciona antes de continuar
- Si un paso falla, diagnostica el error, corrígelo y vuelve a intentarlo
- Verifica dependencias antes de instalarlas
- Crea los directorios y archivos en el orden correcto
- Al final de cada fase, ejecuta un test de integración y reporta el estado

El objetivo es construir un sistema de recomendación híbrido de videojuegos completamente
funcional sobre la infraestructura existente, integrando datos históricos de Kaggle con
datos en tiempo real de APIs públicas.

---

## ARQUITECTURA OBJETIVO COMPLETA

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  FUENTES DE DATOS                                                            │
│                                                                              │
│  HISTÓRICAS (Kaggle — Batch Layer)          EN TIEMPO REAL (Speed Layer)    │
│  ├── steam-video-games.csv                  ├── SteamSpy API (sin key)      │
│  │   200K interacciones user-game           ├── FreeToGame API (sin key)    │
│  │   columnas: user_id, game, behavior,     └── RAWG API (key gratuita)     │
│  │            hours_played                                                  │
│  └── steam-reviews.csv                                                      │
│      434K reviews con texto,                                                │
│      recommended (bool), hours_played                                       │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  INGESTA — Apache NiFi + Python Producers                                    │
│  ├── NiFi Flow: FreeToGame/SteamSpy polling → Kafka                         │
│  ├── kaggle_loader.py: carga CSVs → HDFS → Hive                             │
│  └── Topics Kafka:                                                           │
│      gaming.events.raw · gaming.user.interactions · gaming.recommendations  │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
              ┌────────────────┴─────────────────┐
              ▼                                   ▼
┌─────────────────────────┐       ┌───────────────────────────────────────────┐
│  SPEED LAYER            │       │  BATCH LAYER                              │
│  Spark Streaming        │       │  Spark Batch (Airflow DAG diario 02:00)   │
│                         │       │                                           │
│  KDD Online:            │       │  KDD Completo:                            │
│  1. Selection: stream   │       │  1. Selection: Hive tables                │
│  2. Preproc: limpieza   │       │  2. Preproc: dedup, normalización         │
│  3. Transform: features │       │  3. Transform: user-item matrix           │
│  4. Mining: update      │       │  4. Mining:                               │
│     recomendaciones     │       │     a) ALS Collaborative Filtering        │
│     incrementales       │       │     b) TF-IDF Content-Based               │
│  5. Eval: métricas live │       │     c) Híbrido: α·CF + (1-α)·CB          │
│                         │       │  5. Eval: RMSE, Precision@K, Recall@K    │
│  → Cassandra            │       │  → Modelo serializado en HDFS             │
│    (recommendations     │       │  → Cassandra (batch_recommendations)      │
│     fast updates)       │       │  → Hive (model_metrics, user_segments)    │
└─────────────────────────┘       └───────────────────────────────────────────┘
              │                                   │
              └────────────────┬─────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  SERVING LAYER — Apache Cassandra (keyspace: gaming_recommender)             │
│  ├── user_recommendations   (top-K recomendaciones por usuario)              │
│  ├── game_similarity        (juegos similares por contenido)                 │
│  ├── user_segments          (clusters de jugadores)                          │
│  ├── model_metrics          (RMSE, Precision@K histórico)                    │
│  ├── player_windows         (métricas en tiempo real)                        │
│  └── kdd_insights           (alertas y patrones detectados)                  │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  AGENTES DE IA — FastAPI + Claude API                                        │
│  ├── Agente Explorador: descarga y valida datasets de Kaggle                │
│  ├── Agente KDD: ejecuta pipeline completo y reporta métricas               │
│  ├── Agente Recomendador: genera y explica recomendaciones en lenguaje nat. │
│  └── Agente Monitor: detecta drift del modelo y dispara reentrenamiento     │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  DASHBOARD — React (Vite) puerto 5173                                        │
│  Pestañas:                                                                   │
│  ⚡ Tiempo Real · 📊 Histórico · 🎮 Recomendaciones · 🤖 Agentes · λ Arch   │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## ESTRUCTURA DE DIRECTORIOS

```
kdd-lambda-gaming-recommender/
│
├── .env
├── README.md
├── requirements-global.txt
│
├── 0_data/
│   ├── kaggle/
│   │   ├── download_datasets.py        ← descarga automática desde Kaggle
│   │   └── validate_datasets.py        ← verifica integridad y columnas
│   └── raw/                            ← CSVs descargados aquí
│       ├── steam_games_behaviors.csv   (200K interacciones user-game)
│       └── steam_reviews.csv           (434K reviews con texto)
│
├── 1_ingesta/
│   ├── kaggle_to_hive/
│   │   ├── loader.py                   ← CSV → HDFS → Hive
│   │   └── hive_schema.sql
│   ├── api_producer/
│   │   ├── freetogame_producer.py      ← FreeToGame → Kafka (sin key)
│   │   ├── steamspy_producer.py        ← SteamSpy → Kafka (sin key)
│   │   └── config.py
│   └── nifi_templates/
│       └── gaming_recommender_flow.xml
│
├── 2_kafka/
│   └── setup_topics.sh
│
├── 3_batch_layer/
│   ├── kdd_pipeline.py                 ← KDD completo: 5 fases
│   ├── collaborative_filtering.py      ← ALS con Spark MLlib
│   ├── content_based.py               ← TF-IDF sobre metadatos
│   ├── hybrid_recommender.py          ← α·CF + (1-α)·CB
│   ├── model_evaluator.py             ← RMSE, Precision@K, Recall@K
│   └── hive_schemas.sql
│
├── 4_speed_layer/
│   ├── streaming_kdd.py               ← KDD online con Spark Streaming
│   └── incremental_recommender.py     ← actualización incremental del modelo
│
├── 5_serving_layer/
│   ├── cassandra_schema.cql
│   └── api/
│       ├── main.py                    ← FastAPI con 8 endpoints
│       ├── agents/
│       │   ├── explorer_agent.py      ← Agente Explorador (Claude API)
│       │   ├── kdd_agent.py           ← Agente KDD (Claude API)
│       │   ├── recommender_agent.py   ← Agente Recomendador (Claude API)
│       │   └── monitor_agent.py       ← Agente Monitor (Claude API)
│       └── requirements.txt
│
├── 6_orchestration/
│   └── dags/
│       ├── gaming_kdd_batch.py        ← DAG principal (diario 02:00)
│       └── model_retraining.py        ← DAG reentrenamiento (semanal)
│
└── 7_dashboard/
    ├── package.json
    ├── vite.config.js
    └── src/
        ├── App.jsx
        ├── api.js
        ├── components/
        │   ├── RealtimeTab.jsx
        │   ├── HistoricalTab.jsx
        │   ├── RecommendationsTab.jsx
        │   ├── AgentsTab.jsx
        │   └── LambdaArchTab.jsx
        └── main.jsx
```

---

## FASE 0 — DESCARGA Y VALIDACIÓN DE DATASETS

### ARCHIVO: `0_data/kaggle/download_datasets.py`

```python
#!/usr/bin/env python3
"""
Agente Explorador — Descarga automática de datasets de Kaggle.
Usa la API oficial de Kaggle (pip install kaggle).
Credenciales en ~/.kaggle/kaggle.json
Obtener en: https://www.kaggle.com/settings → API → Create New Token
"""

import os
import sys
import json
import hashlib
import logging
import subprocess
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

RAW_DIR = Path(__file__).parent.parent / "raw"
RAW_DIR.mkdir(exist_ok=True)

# Datasets a descargar con sus checksums esperados
DATASETS = [
    {
        "name":    "tamber/steam-video-games",
        "files":   ["steam-200k.csv"],
        "target":  "steam_games_behaviors.csv",
        "desc":    "200K interacciones usuario-juego (comportamiento implícito)",
        "rows_min": 180_000,
        "columns": ["user_id", "game_title", "behavior_name", "value"],
    },
    {
        "name":    "luthfim/steam-reviews-dataset",
        "files":   ["steam_reviews.csv"],
        "target":  "steam_reviews.csv",
        "desc":    "434K reviews de Steam con texto y recomendación",
        "rows_min": 400_000,
        "columns": ["username", "product_id", "product_title",
                    "review_text", "recommended", "hours"],
    },
]

def check_kaggle_credentials():
    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    if not kaggle_json.exists():
        log.error("No se encontró ~/.kaggle/kaggle.json")
        log.error("1. Ve a https://www.kaggle.com/settings")
        log.error("2. Sección API → 'Create New Token'")
        log.error("3. Mueve el archivo a ~/.kaggle/kaggle.json")
        log.error("4. chmod 600 ~/.kaggle/kaggle.json")
        sys.exit(1)
    log.info("✓ Credenciales Kaggle encontradas")

def install_kaggle():
    try:
        import kaggle
        log.info("✓ Librería kaggle ya instalada")
    except ImportError:
        log.info("Instalando librería kaggle...")
        subprocess.run([sys.executable, "-m", "pip", "install", "kaggle", "-q"], check=True)
        log.info("✓ Librería kaggle instalada")

def download_dataset(dataset: dict):
    target_path = RAW_DIR / dataset["target"]
    if target_path.exists():
        log.info(f"✓ Ya existe: {dataset['target']} ({target_path.stat().st_size / 1e6:.1f} MB)")
        return True

    log.info(f"Descargando: {dataset['name']} — {dataset['desc']}")
    tmp_dir = RAW_DIR / "tmp_download"
    tmp_dir.mkdir(exist_ok=True)

    cmd = [
        "kaggle", "datasets", "download",
        "-d", dataset["name"],
        "-p", str(tmp_dir),
        "--unzip"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"Error descargando {dataset['name']}: {result.stderr}")
        return False

    # Mover archivo al destino correcto
    for filename in dataset["files"]:
        src = tmp_dir / filename
        if src.exists():
            src.rename(target_path)
            log.info(f"✓ Guardado: {target_path} ({target_path.stat().st_size / 1e6:.1f} MB)")
            return True

    log.error(f"No se encontró el archivo esperado en {tmp_dir}")
    return False

def validate_dataset(dataset: dict):
    import pandas as pd
    target_path = RAW_DIR / dataset["target"]
    if not target_path.exists():
        log.error(f"✗ No existe: {target_path}")
        return False

    log.info(f"Validando: {dataset['target']}")
    try:
        # Leer solo las primeras filas para validar columnas
        df_sample = pd.read_csv(target_path, nrows=100, encoding="utf-8", on_bad_lines="skip")
        log.info(f"  Columnas encontradas: {list(df_sample.columns)}")

        # Contar filas total (más eficiente que cargar todo)
        with open(target_path, "r", encoding="utf-8", errors="replace") as f:
            row_count = sum(1 for _ in f) - 1  # -1 por header

        log.info(f"  Total filas: {row_count:,}")

        if row_count < dataset["rows_min"]:
            log.warning(f"  ⚠ Pocas filas: {row_count:,} < {dataset['rows_min']:,} esperadas")

        log.info(f"  ✓ Dataset válido: {dataset['target']}")
        return True

    except Exception as e:
        log.error(f"  ✗ Error validando {dataset['target']}: {e}")
        return False

def main():
    log.info("=" * 60)
    log.info("  Agente Explorador — Descarga de Datasets Kaggle")
    log.info("=" * 60)

    check_kaggle_credentials()
    install_kaggle()

    results = {}
    for ds in DATASETS:
        success = download_dataset(ds)
        if success:
            valid = validate_dataset(ds)
            results[ds["target"]] = "OK" if valid else "INVALID"
        else:
            results[ds["target"]] = "FAILED"

    log.info("\n=== RESUMEN ===")
    for filename, status in results.items():
        icon = "✓" if status == "OK" else "✗"
        log.info(f"  {icon} {filename}: {status}")

    if all(s == "OK" for s in results.values()):
        log.info("\n✓ Todos los datasets listos. Ejecuta: python kaggle_to_hive/loader.py")
        return 0
    else:
        log.error("\n✗ Algunos datasets fallaron. Revisa los errores anteriores.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
```

---

## FASE 1 — CARGA A HIVE (BATCH LAYER BASE)

### ARCHIVO: `1_ingesta/kaggle_to_hive/loader.py`

```python
#!/usr/bin/env python3
"""
Carga los CSVs de Kaggle a HDFS y crea las tablas Hive para el Batch Layer.
Ejecutar después de download_datasets.py
"""

import sys
import logging
import subprocess
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lower, trim, regexp_replace,
    lit, current_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    FloatType, BooleanType, IntegerType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

RAW_DIR  = Path(__file__).parent.parent.parent / "0_data" / "raw"
HDFS_BASE = "hdfs:///user/gaming_recommender"

spark = SparkSession.builder \
    .appName("KDD_Gaming_KaggleLoader") \
    .enableHiveSupport() \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ── Schema de behaviors (steam-200k) ─────────────────────────────────────────
BEHAVIOR_SCHEMA = StructType([
    StructField("user_id",       StringType()),
    StructField("game_title",    StringType()),
    StructField("behavior_name", StringType()),
    StructField("value",         FloatType()),
    StructField("_extra",        StringType()),   # columna extra que tiene el CSV
])

# ── Schema de reviews ─────────────────────────────────────────────────────────
REVIEW_SCHEMA = StructType([
    StructField("username",      StringType()),
    StructField("product_id",    StringType()),
    StructField("product_title", StringType()),
    StructField("review_text",   StringType()),
    StructField("recommended",   StringType()),   # "True"/"False" como string
    StructField("hours",         FloatType()),
])


def load_behaviors():
    """Carga steam-200k.csv: interacciones implícitas usuario-juego."""
    log.info("Cargando steam_games_behaviors.csv...")
    path = RAW_DIR / "steam_games_behaviors.csv"

    raw = spark.read.csv(
        str(path),
        schema=BEHAVIOR_SCHEMA,
        header=False,          # el CSV original NO tiene header
        encoding="UTF-8"
    ).drop("_extra")

    # KDD Fase 2: Preprocessing
    cleaned = raw \
        .filter(col("user_id").isNotNull()) \
        .filter(col("game_title").isNotNull()) \
        .filter(col("behavior_name").isin("purchase", "play")) \
        .filter(col("value") >= 0) \
        .withColumn("game_title", trim(lower(col("game_title")))) \
        .withColumn("hours_played",
            when(col("behavior_name") == "play", col("value")).otherwise(lit(0.0))
        ) \
        .withColumn("purchased",
            when(col("behavior_name") == "purchase", lit(True)).otherwise(lit(False))
        ) \
        .withColumn("load_date", current_date())

    # KDD Fase 3: Transformation — crear rating implícito
    # Rating = log(1 + hours) normalizado: proxy de preferencia
    from pyspark.sql.functions import log as spark_log
    transformed = cleaned.withColumn(
        "implicit_rating",
        spark_log(lit(1.0) + col("hours_played"))
    )

    log.info(f"  Registros totales: {transformed.count():,}")
    log.info(f"  Usuarios únicos:   {transformed.select('user_id').distinct().count():,}")
    log.info(f"  Juegos únicos:     {transformed.select('game_title').distinct().count():,}")

    # Guardar en Hive
    transformed.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .saveAsTable("gaming_recommender.user_behaviors")

    log.info("✓ Tabla Hive: gaming_recommender.user_behaviors")
    return transformed


def load_reviews():
    """Carga steam_reviews.csv: feedback explícito con texto."""
    log.info("Cargando steam_reviews.csv...")
    path = RAW_DIR / "steam_reviews.csv"

    raw = spark.read.csv(
        str(path),
        schema=REVIEW_SCHEMA,
        header=True,
        encoding="UTF-8",
        multiLine=True,
        escape='"'
    )

    # KDD Fase 2: Preprocessing
    cleaned = raw \
        .filter(col("product_title").isNotNull()) \
        .filter(col("review_text").isNotNull()) \
        .filter(col("recommended").isin("True", "False")) \
        .withColumn("product_title", trim(lower(col("product_title")))) \
        .withColumn("review_text",   trim(col("review_text"))) \
        .withColumn("is_recommended",
            when(col("recommended") == "True", lit(True)).otherwise(lit(False))
        ) \
        .withColumn("hours_played",
            when(col("hours").isNotNull(), col("hours")).otherwise(lit(0.0))
        ) \
        .withColumn("load_date", current_date()) \
        .drop("recommended", "hours")

    # KDD Fase 3: Transformation — limpiar texto para NLP
    from pyspark.sql.functions import regexp_replace as re_replace
    transformed = cleaned.withColumn(
        "review_clean",
        trim(re_replace(re_replace(col("review_text"), "[^a-zA-Z0-9 ]", " "), "\\s+", " "))
    )

    log.info(f"  Reviews totales:   {transformed.count():,}")
    log.info(f"  Reviews positivas: {transformed.filter(col('is_recommended')).count():,}")
    log.info(f"  Juegos únicos:     {transformed.select('product_title').distinct().count():,}")

    transformed.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .saveAsTable("gaming_recommender.user_reviews")

    log.info("✓ Tabla Hive: gaming_recommender.user_reviews")
    return transformed


def create_game_catalog():
    """Crea tabla unificada de juegos a partir de ambas fuentes."""
    log.info("Creando catálogo de juegos...")

    # Juegos desde behaviors
    games_behavior = spark.table("gaming_recommender.user_behaviors") \
        .select(col("game_title").alias("title")).distinct()

    # Juegos desde reviews
    games_reviews = spark.table("gaming_recommender.user_reviews") \
        .select(col("product_title").alias("title")).distinct()

    # Unión de ambas fuentes
    catalog = games_behavior.union(games_reviews).distinct() \
        .withColumn("game_id",
            # ID numérico reproducible basado en el nombre
            col("title").cast("string")
        ) \
        .withColumn("load_date", current_date())

    catalog.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable("gaming_recommender.game_catalog")

    count = catalog.count()
    log.info(f"  Juegos en catálogo: {count:,}")
    log.info("✓ Tabla Hive: gaming_recommender.game_catalog")
    return catalog


def main():
    log.info("=" * 60)
    log.info("  KDD Loader — Kaggle CSV → HDFS → Hive")
    log.info("=" * 60)

    # Crear base de datos Hive
    spark.sql("CREATE DATABASE IF NOT EXISTS gaming_recommender")
    spark.sql("USE gaming_recommender")

    behaviors = load_behaviors()
    reviews   = load_reviews()
    catalog   = create_game_catalog()

    log.info("\n=== RESUMEN DE CARGA ===")
    for table in ["user_behaviors", "user_reviews", "game_catalog"]:
        count = spark.sql(f"SELECT COUNT(*) FROM gaming_recommender.{table}").collect()[0][0]
        log.info(f"  gaming_recommender.{table}: {count:,} registros")

    log.info("\n✓ Carga completada. Listo para el pipeline KDD.")
    spark.stop()


if __name__ == "__main__":
    main()
```

---

## FASE 2 — KAFKA TOPICS

### ARCHIVO: `2_kafka/setup_topics.sh`

```bash
#!/bin/bash
# setup_topics.sh — Topics para el sistema de recomendación

KAFKA_HOME=${KAFKA_HOME:-/opt/kafka}
BOOTSTRAP="localhost:9092"

echo "=== Topics Kafka — Gaming Recommender ==="

declare -A TOPICS=(
    ["gaming.events.raw"]="3"
    ["gaming.user.interactions"]="3"    # eventos de interacción en tiempo real
    ["gaming.recommendations"]="1"      # recomendaciones generadas
    ["gaming.model.updates"]="1"        # señales de actualización del modelo
    ["gaming.kdd.insights"]="1"
)

for TOPIC in "${!TOPICS[@]}"; do
    PARTS="${TOPICS[$TOPIC]}"
    $KAFKA_HOME/bin/kafka-topics.sh \
        --create \
        --bootstrap-server $BOOTSTRAP \
        --replication-factor 1 \
        --partitions $PARTS \
        --topic "$TOPIC" \
        --if-not-exists
    echo "✓ $TOPIC (partitions=$PARTS)"
done

echo ""
echo "=== Topics creados ==="
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP | grep gaming
```

---

## FASE 3 — BATCH LAYER: PIPELINE KDD + MODELO HÍBRIDO

### ARCHIVO: `3_batch_layer/collaborative_filtering.py`

```python
#!/usr/bin/env python3
"""
KDD Fase 4 (Mining) — Filtrado Colaborativo con ALS (Alternating Least Squares)
Spark MLlib: entrena sobre la matriz usuario-item implícita de Kaggle.
Genera el modelo de CF que se serializa en HDFS.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, dense_rank
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import logging

log = logging.getLogger(__name__)


def train_collaborative_filter(spark: SparkSession, alpha: float = 40.0):
    """
    Entrena ALS sobre interacciones implícitas (horas jugadas).
    Usa implicit feedback: el rating = log(1 + horas).

    Parámetros ALS clave:
    - rank: dimensión de los factores latentes (20-100)
    - maxIter: iteraciones de optimización
    - regParam: regularización para evitar overfitting
    - implicitPrefs=True: trata los datos como feedback implícito
    - alpha: confianza en las interacciones (mayor = más peso a interacciones positivas)
    """
    log.info("KDD Mining — Entrenando Filtrado Colaborativo (ALS)...")

    # Leer datos de Hive
    behaviors = spark.table("gaming_recommender.user_behaviors") \
        .filter(col("hours_played") > 0) \
        .select("user_id", "game_title", "implicit_rating")

    # ALS necesita IDs numéricos enteros
    # Crear mapeo user_id (string) → user_idx (int)
    user_window = Window.orderBy("user_id")
    game_window = Window.orderBy("game_title")

    user_idx = behaviors.select("user_id").distinct() \
        .withColumn("user_idx", dense_rank().over(user_window).cast("integer"))

    game_idx = behaviors.select("game_title").distinct() \
        .withColumn("game_idx", dense_rank().over(game_window).cast("integer"))

    # Guardar mapeos en Hive para poder hacer lookup inverso después
    user_idx.write.mode("overwrite").format("parquet") \
        .saveAsTable("gaming_recommender.user_index_map")
    game_idx.write.mode("overwrite").format("parquet") \
        .saveAsTable("gaming_recommender.game_index_map")

    # Unir índices al dataset
    ratings = behaviors \
        .join(user_idx, "user_id") \
        .join(game_idx, "game_title") \
        .select(
            col("user_idx"),
            col("game_idx"),
            col("implicit_rating").alias("rating")
        )

    # Split train/test 80-20
    train, test = ratings.randomSplit([0.8, 0.2], seed=42)

    log.info(f"  Train: {train.count():,} interacciones")
    log.info(f"  Test:  {test.count():,} interacciones")

    # Entrenar ALS
    als = ALS(
        rank=50,
        maxIter=15,
        regParam=0.01,
        userCol="user_idx",
        itemCol="game_idx",
        ratingCol="rating",
        implicitPrefs=True,   # CLAVE: datos implícitos
        alpha=alpha,
        coldStartStrategy="drop",   # usuarios/juegos sin historial
        nonnegative=True
    )

    model = als.fit(train)

    # KDD Fase 5: Evaluación
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    log.info(f"  RMSE en test: {rmse:.4f}")

    # Precision@K: porcentaje de recomendaciones relevantes entre las top-K
    # Relevante = juegos que el usuario jugó >10 horas en test
    k = 10
    relevant = test.filter(col("rating") > 0.5)
    recs = model.recommendForAllUsers(k)

    # Calcular Precision@K aproximado
    recs_exploded = recs.selectExpr("user_idx", "explode(recommendations) as rec") \
        .select("user_idx", col("rec.game_idx"))
    hits = recs_exploded.join(relevant, ["user_idx", "game_idx"])
    precision_k = hits.count() / max(recs_exploded.count(), 1)
    log.info(f"  Precision@{k}: {precision_k:.4f}")

    # Guardar modelo en HDFS
    model_path = "hdfs:///user/gaming_recommender/models/als_model"
    model.write().overwrite().save(model_path)
    log.info(f"  ✓ Modelo ALS guardado: {model_path}")

    # Generar recomendaciones top-20 para todos los usuarios
    user_recs = model.recommendForAllUsers(20)

    # Desnormalizar: idx → nombres reales
    game_idx_lookup = spark.table("gaming_recommender.game_index_map")
    user_idx_lookup = spark.table("gaming_recommender.user_index_map")

    return model, {
        "rmse": rmse,
        "precision_at_k": precision_k,
        "k": k,
        "model_path": model_path,
        "user_recs": user_recs,
        "game_idx": game_idx,
        "user_idx": user_idx,
    }
```

### ARCHIVO: `3_batch_layer/content_based.py`

```python
#!/usr/bin/env python3
"""
KDD Fase 4 (Mining) — Filtrado Basado en Contenido
Usa TF-IDF sobre reviews + tags de metadatos para calcular similitud entre juegos.
Genera la matriz de similitud game-game.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, collect_list, concat_ws, lit,
    array_join, when, lower, trim
)
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover,
    HashingTF, IDF, Normalizer
)
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
import logging

log = logging.getLogger(__name__)


def build_game_profiles(spark: SparkSession):
    """
    Construye perfiles de texto por juego concatenando:
    - Texto de reviews
    - Tags (si disponibles desde SteamSpy/FreeToGame en Hive)
    """
    log.info("Content-Based: Construyendo perfiles de juego...")

    # Agregar reviews por juego
    reviews = spark.table("gaming_recommender.user_reviews") \
        .groupBy("product_title") \
        .agg(
            collect_list("review_clean").alias("review_list"),
            (col("_sum_recommended") / col("_count")).alias("recommend_rate")
        )

    # Concatenar reviews en un solo texto por juego (máx 50 reviews)
    from pyspark.sql.functions import slice as spark_slice
    profiles = spark.table("gaming_recommender.user_reviews") \
        .groupBy("product_title") \
        .agg(
            concat_ws(" ", collect_list("review_clean")).alias("combined_text"),
        ) \
        .withColumnRenamed("product_title", "game_title")

    log.info(f"  Juegos con perfil de texto: {profiles.count():,}")
    return profiles


def compute_tfidf_similarity(spark: SparkSession, profiles):
    """
    Pipeline TF-IDF → similitud coseno entre juegos.
    Retorna DataFrame con (game_a, game_b, similarity_score).
    """
    log.info("Content-Based: Calculando TF-IDF y similitud coseno...")

    # Pipeline NLP
    tokenizer = Tokenizer(inputCol="combined_text", outputCol="tokens_raw")
    remover   = StopWordsRemover(inputCol="tokens_raw", outputCol="tokens",
                                  stopWords=StopWordsRemover.loadDefaultStopWords("english"))
    hashing_tf = HashingTF(inputCol="tokens", outputCol="raw_features", numFeatures=10000)
    idf        = IDF(inputCol="raw_features", outputCol="features", minDocFreq=2)
    normalizer = Normalizer(inputCol="features", outputCol="norm_features", p=2.0)

    pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, normalizer])
    tfidf_model = pipeline.fit(profiles)
    tfidf_df    = tfidf_model.transform(profiles)

    # Guardar modelo TF-IDF
    tfidf_model.write().overwrite().save(
        "hdfs:///user/gaming_recommender/models/tfidf_pipeline"
    )

    # Similitud coseno: como los vectores están normalizados,
    # similitud = producto escalar (dot product)
    # Para Spark: calcular en Pandas para N<50K juegos es viable
    log.info("  Convirtiendo a Pandas para similitud coseno...")
    tfidf_pd = tfidf_df.select("game_title", "norm_features").toPandas()

    import numpy as np
    from sklearn.metrics.pairwise import cosine_similarity

    titles   = tfidf_pd["game_title"].tolist()
    features = np.array([v.toArray() for v in tfidf_pd["norm_features"]])

    # Calcular matriz de similitud
    sim_matrix = cosine_similarity(features)

    # Construir DataFrame de pares similares (top-20 por juego)
    pairs = []
    k_similar = 20
    for i, game_a in enumerate(titles):
        # Índices ordenados por similitud descendente (excluir el mismo juego)
        similar_indices = np.argsort(sim_matrix[i])[::-1][1:k_similar+1]
        for j in similar_indices:
            pairs.append({
                "game_a":     game_a,
                "game_b":     titles[j],
                "similarity": float(sim_matrix[i][j])
            })

    import pandas as pd
    pairs_pd = pd.DataFrame(pairs)
    similarity_df = spark.createDataFrame(pairs_pd)

    # Guardar en Hive
    similarity_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable("gaming_recommender.game_similarity")

    log.info(f"  ✓ Pares de similitud generados: {len(pairs):,}")
    log.info("  ✓ Tabla Hive: gaming_recommender.game_similarity")

    return similarity_df, tfidf_model
```

### ARCHIVO: `3_batch_layer/hybrid_recommender.py`

```python
#!/usr/bin/env python3
"""
KDD Fase 4 (Mining) — Sistema de Recomendación Híbrido
Combina: score_hibrido = α * score_CF + (1-α) * score_CB

Donde:
  - score_CF: predicción del modelo ALS (preferencia del usuario)
  - score_CB: similitud de contenido con juegos que ya le gustan al usuario
  - α: peso del componente colaborativo (default 0.7)

El valor de α se puede ajustar en función de cuánto historial tiene el usuario:
  - Usuario nuevo (cold start): α bajo (0.2) → más peso a contenido
  - Usuario con historial: α alto (0.8) → más peso a colaborativo
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, lit, greatest, least,
    when, avg, rank, desc, round as spark_round
)
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALSModel

log = logging.getLogger(__name__)


def generate_hybrid_recommendations(
    spark: SparkSession,
    cf_results: dict,
    alpha: float = 0.7,
    top_k: int = 10
):
    """
    Genera recomendaciones híbridas para todos los usuarios.

    Estrategia:
    1. Obtener top-50 candidatos del CF por usuario
    2. Para cada candidato, calcular score de contenido basado en
       similitud con los juegos que más ha jugado el usuario
    3. Combinar: score_final = alpha * cf_score + (1-alpha) * cb_score
    4. Reordenar y tomar top-K
    5. Manejar cold start: usuarios sin historial usan solo CB
    """
    log.info(f"Híbrido: α={alpha} (CF={alpha:.0%}, CB={(1-alpha):.0%})")

    # ── Paso 1: Recomendaciones CF ──────────────────────────────────────────
    model_path = cf_results["model_path"]
    als_model  = ALSModel.load(model_path)

    # Top-50 candidatos por usuario (tomamos más para el reranking)
    cf_recs = als_model.recommendForAllUsers(50)
    cf_expanded = cf_recs.selectExpr(
        "user_idx",
        "explode(recommendations) as rec"
    ).select(
        col("user_idx"),
        col("rec.game_idx").alias("game_idx"),
        col("rec.rating").alias("cf_score")
    )

    # Normalizar cf_score a [0,1] por usuario
    user_window = Window.partitionBy("user_idx")
    cf_norm = cf_expanded.withColumn(
        "cf_score_norm",
        (col("cf_score") - avg("cf_score").over(user_window)) /
        (greatest(
            (col("cf_score") - avg("cf_score").over(user_window)).cast("double"),
            lit(0.0001)
        ))
    )

    # ── Paso 2: Score de contenido por usuario ──────────────────────────────
    # Obtener juegos "seed" (más jugados por cada usuario)
    behaviors = spark.table("gaming_recommender.user_behaviors") \
        .filter(col("hours_played") > 5)

    # Top-5 juegos más jugados por usuario
    user_top_games = behaviors \
        .join(spark.table("gaming_recommender.game_index_map"), "game_title") \
        .groupBy("user_id", "game_title", "game_idx") \
        .agg(avg("hours_played").alias("avg_hours")) \
        .withColumn(
            "rank_in_user",
            rank().over(Window.partitionBy("user_id").orderBy(desc("avg_hours")))
        ).filter(col("rank_in_user") <= 5)

    # Similitud de contenido entre juegos seed y candidatos
    similarity = spark.table("gaming_recommender.game_similarity")

    # Para cada usuario: promedio de similitud entre candidatos y sus seeds
    # Unir usuario → sus seeds → similitud seeds con candidatos
    user_idx_map = spark.table("gaming_recommender.user_index_map")

    user_seeds = user_top_games \
        .join(user_idx_map, "user_id") \
        .select(col("user_idx"), col("game_title").alias("seed_game"))

    # Similitud de cada seed con otros juegos
    seed_similarity = user_seeds \
        .join(
            similarity.withColumnRenamed("game_a", "seed_game"),
            "seed_game"
        ) \
        .groupBy("user_idx", "game_b") \
        .agg(avg("similarity").alias("cb_score"))

    # Unir con el mapa de índices de juegos
    game_idx_map = spark.table("gaming_recommender.game_index_map")
    cb_with_idx = seed_similarity \
        .join(game_idx_map.withColumnRenamed("game_title", "game_b"), "game_b") \
        .select("user_idx", "game_idx", col("cb_score"))

    # ── Paso 3: Combinar CF + CB ────────────────────────────────────────────
    hybrid = cf_norm \
        .join(cb_with_idx, ["user_idx", "game_idx"], "left") \
        .fillna({"cb_score": 0.0}) \
        .withColumn(
            "hybrid_score",
            spark_round(
                lit(alpha) * col("cf_score_norm") +
                lit(1 - alpha) * col("cb_score"),
                4
            )
        )

    # ── Paso 4: Top-K por usuario ───────────────────────────────────────────
    rank_window = Window.partitionBy("user_idx").orderBy(desc("hybrid_score"))
    top_k_recs = hybrid \
        .withColumn("rank", rank().over(rank_window)) \
        .filter(col("rank") <= top_k)

    # ── Paso 5: Desnormalizar índices → nombres reales ──────────────────────
    final_recs = top_k_recs \
        .join(user_idx_map, "user_idx") \
        .join(game_idx_map, "game_idx") \
        .select(
            col("user_id"),
            col("game_title").alias("recommended_game"),
            col("hybrid_score"),
            col("cf_score_norm").alias("cf_component"),
            col("cb_score").alias("cb_component"),
            col("rank").alias("recommendation_rank"),
            lit(alpha).alias("alpha_used")
        )

    # ── Cold Start: usuarios sin historial ─────────────────────────────────
    # Para ellos: recomendar los juegos más populares del catálogo
    popular_games = spark.table("gaming_recommender.user_behaviors") \
        .groupBy("game_title") \
        .agg(avg("hours_played").alias("avg_hours")) \
        .orderBy(desc("avg_hours")) \
        .limit(top_k) \
        .withColumn("hybrid_score",    lit(0.5)) \
        .withColumn("cf_component",    lit(0.0)) \
        .withColumn("cb_component",    lit(0.5)) \
        .withColumn("alpha_used",      lit(0.0)) \
        .withColumnRenamed("game_title", "recommended_game")

    # Guardar recomendaciones en Hive
    final_recs.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable("gaming_recommender.hybrid_recommendations")

    popular_games.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable("gaming_recommender.popular_games_fallback")

    user_count = final_recs.select("user_id").distinct().count()
    log.info(f"  ✓ Recomendaciones generadas para {user_count:,} usuarios")
    log.info("  ✓ Tabla Hive: gaming_recommender.hybrid_recommendations")

    return final_recs


def determine_alpha(user_id: str, spark: SparkSession) -> float:
    """
    Determina el α óptimo según el historial del usuario.
    Cold start → más CB (contenido). Usuario veterano → más CF (colaborativo).
    """
    try:
        history_count = spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM gaming_recommender.user_behaviors
            WHERE user_id = '{user_id}' AND hours_played > 0
        """).collect()[0]["cnt"]

        if history_count == 0:     return 0.1   # cold start: casi todo CB
        elif history_count < 5:    return 0.3
        elif history_count < 20:   return 0.5
        elif history_count < 50:   return 0.7
        else:                      return 0.85  # usuario con mucho historial: más CF
    except:
        return 0.5  # fallback
```

### ARCHIVO: `3_batch_layer/kdd_pipeline.py`

```python
#!/usr/bin/env python3
"""
KDD Pipeline completo — orquesta las 5 fases sobre el Batch Layer.
Llamado directamente o por el DAG de Airflow.
"""

import sys
import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

from collaborative_filtering import train_collaborative_filter
from content_based import build_game_profiles, compute_tfidf_similarity
from hybrid_recommender import generate_hybrid_recommendations
from model_evaluator import evaluate_and_store_metrics

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("--date",  default=datetime.utcnow().strftime("%Y-%m-%d"))
parser.add_argument("--alpha", default=0.7, type=float, help="Peso CF vs CB (0=solo CB, 1=solo CF)")
args = parser.parse_args()


def main():
    spark = SparkSession.builder \
        .appName(f"KDD_GameRecommender_{args.date}") \
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.driver.memory",   "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE gaming_recommender")

    log.info("=" * 70)
    log.info(f"  KDD Pipeline — Gaming Recommender — {args.date}")
    log.info(f"  Modo híbrido: α={args.alpha} (CF={args.alpha:.0%}, CB={1-args.alpha:.0%})")
    log.info("=" * 70)

    # ── KDD Fase 1: Selection ─────────────────────────────────────────────
    log.info("\n[KDD 1/5] SELECTION")
    behaviors = spark.table("gaming_recommender.user_behaviors")
    reviews   = spark.table("gaming_recommender.user_reviews")
    n_users   = behaviors.select("user_id").distinct().count()
    n_games   = behaviors.select("game_title").distinct().count()
    n_reviews = reviews.count()
    log.info(f"  Usuarios: {n_users:,} · Juegos: {n_games:,} · Reviews: {n_reviews:,}")

    # ── KDD Fase 2: Preprocessing ─────────────────────────────────────────
    log.info("\n[KDD 2/5] PREPROCESSING")
    # Ya realizado en loader.py — verificar calidad
    null_users  = behaviors.filter(col("user_id").isNull()).count()
    null_games  = behaviors.filter(col("game_title").isNull()).count()
    neg_hours   = behaviors.filter(col("hours_played") < 0).count()
    log.info(f"  Nulos user_id: {null_users} · Nulos game: {null_games} · Horas negativas: {neg_hours}")
    if null_users + null_games + neg_hours > 0:
        log.warning("  ⚠ Hay registros con problemas — revisar loader.py")

    # ── KDD Fase 3: Transformation ────────────────────────────────────────
    log.info("\n[KDD 3/5] TRANSFORMATION")
    # Construir perfiles de juego para CB
    game_profiles = build_game_profiles(spark)

    # ── KDD Fase 4: Mining ────────────────────────────────────────────────
    log.info("\n[KDD 4/5] MINING")

    # 4a. Filtrado Colaborativo (ALS)
    log.info("  4a. Collaborative Filtering (ALS)...")
    cf_model, cf_results = train_collaborative_filter(spark)

    # 4b. Filtrado Basado en Contenido (TF-IDF)
    log.info("  4b. Content-Based Filtering (TF-IDF)...")
    similarity_df, tfidf_model = compute_tfidf_similarity(spark, game_profiles)

    # 4c. Híbrido
    log.info(f"  4c. Hybrid Recommender (α={args.alpha})...")
    hybrid_recs = generate_hybrid_recommendations(
        spark, cf_results, alpha=args.alpha
    )

    # ── KDD Fase 5: Evaluation ────────────────────────────────────────────
    log.info("\n[KDD 5/5] EVALUATION")
    metrics = evaluate_and_store_metrics(
        spark, cf_results, hybrid_recs, args.date
    )

    # Escribir recomendaciones a Cassandra (Serving Layer)
    log.info("\nEscribiendo recomendaciones en Cassandra...")
    hybrid_recs.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="user_recommendations", keyspace="gaming_recommender") \
        .save()

    similarity_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="game_similarity", keyspace="gaming_recommender") \
        .save()

    log.info("\n" + "=" * 70)
    log.info("  KDD Pipeline completado")
    log.info(f"  RMSE:          {metrics['rmse']:.4f}")
    log.info(f"  Precision@10:  {metrics['precision_at_k']:.4f}")
    log.info(f"  Usuarios con recs: {metrics['users_with_recs']:,}")
    log.info("=" * 70)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

---

## FASE 4 — SPEED LAYER: KDD ONLINE + ACTUALIZACIONES INCREMENTALES

### ARCHIVO: `4_speed_layer/streaming_kdd.py`

```python
#!/usr/bin/env python3
"""
Speed Layer — KDD Online con Spark Streaming.
Lee nuevas interacciones de usuario desde Kafka y:
1. Aplica el modelo híbrido ya entrenado (cargado desde HDFS)
2. Actualiza las recomendaciones del usuario en Cassandra
3. Detecta patrones anómalos y genera insights KDD en tiempo real
"""

import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lit, current_timestamp,
    when, avg, count, desc
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    FloatType, LongType, BooleanType
)
from pyspark.ml.recommendation import ALSModel

# Schema de eventos de interacción en tiempo real
INTERACTION_SCHEMA = StructType([
    StructField("user_id",     StringType()),
    StructField("game_title",  StringType()),
    StructField("event_type",  StringType()),   # "play", "purchase", "review"
    StructField("hours",       FloatType()),
    StructField("recommended", BooleanType()),
    StructField("ts",          LongType()),
])

spark = SparkSession.builder \
    .appName("KDD_Gaming_SpeedLayer_Recommender") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Cargar modelo ALS desde HDFS (una sola vez al arrancar)
ALS_MODEL_PATH = "hdfs:///user/gaming_recommender/models/als_model"
try:
    als_model = ALSModel.load(ALS_MODEL_PATH)
    print("✓ Modelo ALS cargado desde HDFS")
except Exception as e:
    print(f"⚠ No se pudo cargar el modelo ALS: {e}")
    als_model = None

# Leer stream de Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "gaming.user.interactions") \
    .option("startingOffsets", "latest") \
    .load()

# KDD Online: Preprocessing + Transformation
events = raw_stream.select(
    from_json(col("value").cast("string"), INTERACTION_SCHEMA).alias("e"),
    col("timestamp").alias("kafka_ts")
).select("e.*", "kafka_ts") \
 .filter(col("user_id").isNotNull()) \
 .filter(col("game_title").isNotNull()) \
 .filter(col("event_type").isin("play", "purchase", "review")) \
 .withColumn("processed_at", current_timestamp()) \
 .withColumn("engagement_signal",
    when(col("event_type") == "play",
         when(col("hours") > 10, lit(1.0))
         .when(col("hours") > 1,  lit(0.7))
         .otherwise(lit(0.3))
    ).when(col("event_type") == "purchase", lit(0.5))
     .when(col("event_type") == "review",
           when(col("recommended"), lit(0.9)).otherwise(lit(0.2))
     ).otherwise(lit(0.1))
 )


def process_batch(batch_df, batch_id):
    """Procesa cada micro-batch: actualiza Cassandra con recomendaciones frescas."""
    if batch_df.count() == 0:
        return

    # Agrupar interacciones del batch por usuario
    user_activity = batch_df.groupBy("user_id").agg(
        count("*").alias("events_in_batch"),
        avg("engagement_signal").alias("avg_engagement"),
        avg("hours").alias("avg_hours")
    )

    # Para usuarios activos en este batch: actualizar ventana en Cassandra
    user_activity.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="player_windows", keyspace="gaming_recommender") \
        .save()

    # Detectar usuarios que necesitan actualización de recomendaciones
    # (usuarios con alta actividad o comportamiento nuevo)
    active_users = batch_df.select("user_id").distinct()

    # Si el modelo está disponible: generar recomendaciones personalizadas
    if als_model is not None:
        try:
            user_idx_map = spark.table("gaming_recommender.user_index_map")
            game_idx_map = spark.table("gaming_recommender.game_index_map")
            similarity   = spark.table("gaming_recommender.game_similarity")

            # Usuarios con índice conocido
            known_users = active_users \
                .join(user_idx_map, "user_id") \
                .select("user_idx", "user_id")

            if known_users.count() > 0:
                new_recs = als_model.recommendForUserSubset(known_users, 10)
                # Desnormalizar y guardar en Cassandra
                new_recs_expanded = new_recs.selectExpr(
                    "user_idx", "explode(recommendations) as rec"
                ).select(
                    col("user_idx"),
                    col("rec.game_idx"),
                    col("rec.rating").alias("cf_score")
                ).join(user_idx_map, "user_idx") \
                 .join(game_idx_map, "game_idx") \
                 .select(
                     col("user_id"),
                     col("game_title").alias("recommended_game"),
                     col("cf_score").alias("hybrid_score"),
                     lit("speed_layer").alias("source"),
                     current_timestamp().alias("generated_at")
                 )

                new_recs_expanded.write \
                    .format("org.apache.spark.sql.cassandra") \
                    .mode("append") \
                    .options(table="user_recommendations", keyspace="gaming_recommender") \
                    .save()
        except Exception as e:
            print(f"⚠ Error actualizando recomendaciones: {e}")

    # KDD Evaluation online: detectar anomalías
    anomalies = batch_df.filter(
        (col("hours") > 100) |  # sesión sospechosamente larga
        (col("engagement_signal") < 0.1)  # engagement muy bajo
    )
    if anomalies.count() > 0:
        anomalies.withColumn("insight_id", lit(str(uuid.uuid4()))) \
            .withColumn("insight_type", lit("anomaly_detected")) \
            .withColumn("severity", lit("warning")) \
            .withColumn("layer", lit("speed")) \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="kdd_insights", keyspace="gaming_recommender") \
            .save()


query = events.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="30 seconds") \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/kdd_checkpoint/recommender") \
    .start()

print("✓ Speed Layer Recommender arrancado")
query.awaitTermination()
```

---

## FASE 5 — AGENTES DE IA (FastAPI + Claude API)

### ARCHIVO: `5_serving_layer/api/agents/recommender_agent.py`

```python
#!/usr/bin/env python3
"""
Agente Recomendador — Explica en lenguaje natural por qué se recomienda cada juego.
Usa Claude API para generar explicaciones personalizadas basadas en:
- El historial del usuario (juegos jugados, horas)
- Las métricas del modelo híbrido (score CF y CB)
- Los tags y características del juego recomendado
"""

import os
import json
import logging
from typing import Optional
import anthropic
from cassandra.cluster import Cluster

log = logging.getLogger(__name__)

client     = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
cass_cluster = Cluster(["localhost"])
session      = cass_cluster.connect("gaming_recommender")


def get_user_profile(user_id: str) -> dict:
    """Obtiene el perfil del usuario desde Cassandra."""
    recs = session.execute(
        "SELECT recommended_game, hybrid_score, cf_component, cb_component "
        "FROM user_recommendations WHERE user_id = %s LIMIT 10",
        (user_id,)
    )
    return {"user_id": user_id, "recommendations": [dict(r) for r in recs]}


def get_game_similarity(game: str) -> list:
    """Obtiene juegos similares por contenido."""
    rows = session.execute(
        "SELECT game_b, similarity FROM game_similarity WHERE game_a = %s LIMIT 5",
        (game,)
    )
    return [{"game": r.game_b, "similarity": r.similarity} for r in rows]


def explain_recommendation(
    user_id: str,
    game: str,
    cf_score: float,
    cb_score: float,
    user_history: list,
    alpha: float = 0.7
) -> str:
    """
    Usa Claude API para generar una explicación personalizada de la recomendación.
    Sigue el patrón de agente con contexto KDD.
    """
    similar_games = get_game_similarity(game)

    prompt = f"""Eres un agente de recomendación de videojuegos que usa un sistema híbrido KDD+Lambda.

CONTEXTO DEL USUARIO {user_id}:
- Historial de juegos: {json.dumps(user_history[:5], ensure_ascii=False)}
- Sistema: α={alpha} (peso CF={alpha:.0%}, peso CB={1-alpha:.0%})

RECOMENDACIÓN A EXPLICAR:
- Juego recomendado: "{game}"
- Score de Filtrado Colaborativo (usuarios similares): {cf_score:.3f}
- Score de Contenido (similitud con historial): {cb_score:.3f}
- Score híbrido final: {alpha*cf_score + (1-alpha)*cb_score:.3f}
- Juegos similares por contenido: {json.dumps([g['game'] for g in similar_games], ensure_ascii=False)}

Genera una explicación personalizada en español (máximo 3 frases) que:
1. Explique POR QUÉ se recomienda este juego a este usuario específico
2. Mencione qué juegos de su historial influyeron más en la recomendación
3. Destaque si la recomendación viene más de usuarios similares (CF) o de similitud de contenido (CB)

Sé conciso, específico y usa el nombre del juego."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text


def run_recommender_agent(user_id: str) -> dict:
    """
    Agente completo: obtiene recomendaciones + genera explicaciones para cada una.
    """
    log.info(f"Agente Recomendador activado para usuario: {user_id}")

    profile = get_user_profile(user_id)
    recs    = profile["recommendations"]

    if not recs:
        return {
            "user_id":         user_id,
            "recommendations": [],
            "message": "Usuario sin historial. Mostrando juegos populares.",
            "cold_start": True
        }

    # Obtener historial de juegos del usuario desde Hive (vía caché en Cassandra)
    user_history_rows = session.execute(
        "SELECT game_title, hours_played FROM user_history_cache WHERE user_id = %s LIMIT 10",
        (user_id,)
    )
    user_history = [{"game": r.game_title, "hours": r.hours_played}
                    for r in user_history_rows]

    # Generar explicación para cada recomendación
    enriched = []
    for rec in recs[:5]:  # Explicar las top-5
        try:
            explanation = explain_recommendation(
                user_id      = user_id,
                game         = rec["recommended_game"],
                cf_score     = rec.get("cf_component", 0.5),
                cb_score     = rec.get("cb_component", 0.5),
                user_history = user_history
            )
        except Exception as e:
            explanation = f"Recomendado basado en tu historial de juego."
            log.warning(f"Error generando explicación: {e}")

        enriched.append({
            "game":          rec["recommended_game"],
            "hybrid_score":  rec.get("hybrid_score", 0),
            "cf_component":  rec.get("cf_component", 0),
            "cb_component":  rec.get("cb_component", 0),
            "explanation":   explanation,
        })

    return {
        "user_id":         user_id,
        "recommendations": enriched,
        "model_info": {
            "type":   "hybrid",
            "alpha":  0.7,
            "cf":     "ALS (Spark MLlib)",
            "cb":     "TF-IDF cosine similarity",
            "layer":  "serving (Cassandra)"
        }
    }
```

### ARCHIVO: `5_serving_layer/api/agents/monitor_agent.py`

```python
#!/usr/bin/env python3
"""
Agente Monitor — Detecta drift del modelo y dispara reentrenamiento.
Ejecuta periódicamente para comparar métricas actuales vs baseline.
Si detecta degradación, notifica a Airflow para lanzar el pipeline KDD.
"""

import os
import json
import logging
import requests
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
import anthropic

log    = logging.getLogger(__name__)
client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
session = Cluster(["localhost"]).connect("gaming_recommender")

AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "airflow")


def get_current_metrics() -> dict:
    """Lee métricas recientes del modelo desde Cassandra."""
    rows = session.execute(
        "SELECT * FROM model_metrics ORDER BY evaluated_at DESC LIMIT 10"
    )
    return [dict(r) for r in rows]


def detect_drift(metrics: list) -> dict:
    """
    Compara últimas métricas vs baseline para detectar degradación.
    Criterios de drift:
    - RMSE aumentó >15% vs media histórica
    - Precision@10 bajó >10%
    - Más del 30% de usuarios sin recomendaciones válidas
    """
    if len(metrics) < 2:
        return {"drift_detected": False, "reason": "Insuficientes métricas históricas"}

    latest   = metrics[0]
    baseline = {
        "rmse":           sum(m.get("rmse", 0) for m in metrics[1:]) / len(metrics[1:]),
        "precision_at_k": sum(m.get("precision_at_k", 0) for m in metrics[1:]) / len(metrics[1:]),
    }

    drift_reasons = []
    rmse_delta = (latest.get("rmse", 0) - baseline["rmse"]) / max(baseline["rmse"], 0.0001)
    prec_delta = (baseline["precision_at_k"] - latest.get("precision_at_k", 0)) / max(baseline["precision_at_k"], 0.0001)

    if rmse_delta > 0.15:
        drift_reasons.append(f"RMSE aumentó {rmse_delta:.1%} vs baseline ({latest.get('rmse',0):.4f} vs {baseline['rmse']:.4f})")
    if prec_delta > 0.10:
        drift_reasons.append(f"Precision@K bajó {prec_delta:.1%} vs baseline")

    return {
        "drift_detected": len(drift_reasons) > 0,
        "reasons":        drift_reasons,
        "latest_rmse":    latest.get("rmse", 0),
        "baseline_rmse":  baseline["rmse"],
        "latest_prec":    latest.get("precision_at_k", 0),
        "baseline_prec":  baseline["precision_at_k"],
    }


def trigger_retraining(reason: str):
    """Dispara el DAG de reentrenamiento en Airflow vía REST API."""
    try:
        response = requests.post(
            f"{AIRFLOW_URL}/api/v1/dags/gaming_kdd_batch_pipeline/dagRuns",
            json={"conf": {"triggered_by": "monitor_agent", "reason": reason}},
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=10
        )
        if response.status_code in [200, 201]:
            log.info(f"✓ Reentrenamiento disparado: {reason}")
            return True
        else:
            log.error(f"✗ Error disparando DAG: {response.status_code}")
            return False
    except Exception as e:
        log.error(f"✗ No se pudo conectar con Airflow: {e}")
        return False


def run_monitor_agent() -> dict:
    """Agente completo: analiza métricas + decide si reentrenar + explica con IA."""
    log.info("Agente Monitor activado...")
    metrics = get_current_metrics()
    drift   = detect_drift(metrics)

    # Análisis con Claude API
    prompt = f"""Eres un agente monitor de un sistema de recomendación de videojuegos KDD+Lambda.

MÉTRICAS DEL MODELO (últimas ejecuciones):
{json.dumps(metrics[:5], default=str, indent=2)}

ANÁLISIS DE DRIFT:
{json.dumps(drift, indent=2)}

Proporciona en español:
1. Un diagnóstico de la salud del modelo (1-2 frases)
2. Si hay drift: causa más probable y acción recomendada
3. Si no hay drift: confirmación de estabilidad
Máximo 4 frases en total."""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        ai_diagnosis = response.content[0].text
    except Exception as e:
        ai_diagnosis = "No se pudo generar diagnóstico IA."

    retraining_triggered = False
    if drift["drift_detected"]:
        retraining_triggered = trigger_retraining("; ".join(drift["reasons"]))

    return {
        "timestamp":             datetime.utcnow().isoformat(),
        "drift_detected":        drift["drift_detected"],
        "drift_reasons":         drift.get("reasons", []),
        "ai_diagnosis":          ai_diagnosis,
        "retraining_triggered":  retraining_triggered,
        "metrics_summary": {
            "latest_rmse":    drift.get("latest_rmse", 0),
            "baseline_rmse":  drift.get("baseline_rmse", 0),
            "latest_prec":    drift.get("latest_prec", 0),
            "baseline_prec":  drift.get("baseline_prec", 0),
        }
    }
```

### ARCHIVO: `5_serving_layer/api/main.py`

```python
#!/usr/bin/env python3
"""
FastAPI — Serving Layer REST API
8 endpoints que exponen Cassandra al dashboard React.
Incluye endpoints para los 4 agentes de IA.
"""

import os
import logging
from typing import Optional
from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from cassandra.cluster import Cluster
from datetime import datetime

from agents.recommender_agent import run_recommender_agent
from agents.monitor_agent import run_monitor_agent

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(
    title="KDD Gaming Recommender API",
    version="2.0",
    description="Serving Layer: Cassandra + Agentes IA (Claude)"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

cluster = Cluster([os.getenv("CASSANDRA_HOST", "localhost")])
session = cluster.connect("gaming_recommender")


# ── Endpoints de datos ────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "ts": datetime.utcnow().isoformat(), "version": "2.0"}


@app.get("/api/recommendations/{user_id}")
def get_recommendations(user_id: str, limit: int = Query(10, le=50)):
    """Recomendaciones híbridas para un usuario específico."""
    rows = session.execute(
        "SELECT recommended_game, hybrid_score, cf_component, cb_component, "
        "recommendation_rank FROM user_recommendations WHERE user_id = %s LIMIT %s",
        (user_id, limit)
    )
    return [dict(r) for r in rows] or _get_popular_fallback(limit)


@app.get("/api/similar/{game_title}")
def get_similar_games(game_title: str, limit: int = Query(10, le=20)):
    """Juegos similares por contenido (TF-IDF cosine similarity)."""
    rows = session.execute(
        "SELECT game_b, similarity FROM game_similarity WHERE game_a = %s LIMIT %s",
        (game_title, limit)
    )
    return [{"game": r.game_b, "similarity": round(r.similarity, 4)} for r in rows]


@app.get("/api/realtime")
def get_realtime(limit: int = Query(20, le=100)):
    """Últimas ventanas del Speed Layer."""
    rows = session.execute("SELECT * FROM player_windows LIMIT %s", (limit,))
    return [dict(r) for r in rows]


@app.get("/api/insights")
def get_insights(severity: Optional[str] = None, limit: int = Query(20, le=100)):
    """Insights KDD generados por Speed y Batch Layer."""
    if severity:
        rows = session.execute(
            "SELECT * FROM kdd_insights WHERE severity = %s ALLOW FILTERING LIMIT %s",
            (severity, limit)
        )
    else:
        rows = session.execute("SELECT * FROM kdd_insights LIMIT %s", (limit,))
    return [dict(r) for r in rows]


@app.get("/api/metrics")
def get_model_metrics():
    """Métricas de evaluación del modelo híbrido."""
    rows = session.execute(
        "SELECT * FROM model_metrics ORDER BY evaluated_at DESC LIMIT 30"
    )
    return [dict(r) for r in rows]


def _get_popular_fallback(limit: int) -> list:
    """Fallback para cold start: juegos más populares."""
    rows = session.execute(
        "SELECT recommended_game, hybrid_score FROM popular_games_fallback LIMIT %s",
        (limit,)
    )
    return [dict(r) for r in rows]


# ── Endpoints de Agentes IA ───────────────────────────────────────────────────

@app.get("/api/agent/recommend/{user_id}")
def agent_recommend(user_id: str):
    """
    Agente Recomendador: recomendaciones + explicaciones en lenguaje natural.
    Llama a Claude API para explicar cada recomendación.
    """
    return run_recommender_agent(user_id)


@app.get("/api/agent/monitor")
def agent_monitor():
    """
    Agente Monitor: analiza drift del modelo y dispara reentrenamiento si necesario.
    """
    return run_monitor_agent()


@app.post("/api/agent/interaction")
def record_interaction(
    user_id: str,
    game_title: str,
    event_type: str,
    hours: float = 0.0,
    recommended: bool = False
):
    """
    Registra una nueva interacción de usuario en tiempo real.
    La envía a Kafka para que el Speed Layer la procese.
    """
    import json
    import time
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    event = {
        "user_id":     user_id,
        "game_title":  game_title,
        "event_type":  event_type,
        "hours":       hours,
        "recommended": recommended,
        "ts":          int(time.time() * 1000)
    }

    producer.send("gaming.user.interactions", value=event)
    producer.flush()
    producer.close()

    return {"status": "ok", "event": event}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
```

---

## FASE 6 — AIRFLOW DAG

### ARCHIVO: `6_orchestration/dags/gaming_kdd_batch.py`

```python
"""
DAG principal — KDD Batch Pipeline con Modelo Híbrido
Ejecuta diariamente a las 02:00 UTC.
Orquesta: descarga → carga → KDD → evaluación → Cassandra
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

PROJECT = "/opt/kdd-lambda-gaming-recommender"
SPARK   = "/opt/spark/bin/spark-submit"
PYTHON  = "/usr/bin/python3"
PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
    "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0"
)

default_args = {
    "owner":            "kdd_gaming",
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="gaming_kdd_recommender_pipeline",
    default_args=default_args,
    description="KDD Batch Pipeline — Sistema Híbrido de Recomendación",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["kdd", "recommender", "hybrid", "lambda"],
) as dag:

    check_services = BashOperator(
        task_id="check_services",
        bash_command="""
            echo "Verificando servicios..."
            hdfs dfs -ls / | head -3 || (echo "HDFS no disponible" && exit 1)
            cqlsh -e "DESCRIBE KEYSPACES;" | grep gaming_recommender || \
                (echo "Cassandra keyspace no existe" && exit 1)
            /opt/kafka/bin/kafka-topics.sh \
                --bootstrap-server localhost:9092 --list | \
                grep gaming.user.interactions || \
                (echo "Topic Kafka no existe" && exit 1)
            echo "✓ Todos los servicios disponibles"
        """
    )

    validate_datasets = BashOperator(
        task_id="validate_kaggle_datasets",
        bash_command=f"""
            {PYTHON} {PROJECT}/0_data/kaggle/validate_datasets.py
        """
    )

    run_kdd_pipeline = BashOperator(
        task_id="run_kdd_pipeline",
        bash_command=f"""
            {SPARK} \
                --master local[*] \
                --driver-memory 4g \
                --executor-memory 4g \
                --packages {PACKAGES} \
                --conf spark.cassandra.connection.host=localhost \
                --py-files {PROJECT}/3_batch_layer/collaborative_filtering.py,\
{PROJECT}/3_batch_layer/content_based.py,\
{PROJECT}/3_batch_layer/hybrid_recommender.py,\
{PROJECT}/3_batch_layer/model_evaluator.py \
                {PROJECT}/3_batch_layer/kdd_pipeline.py \
                --date {{{{ ds }}}} \
                --alpha 0.7
        """
    )

    verify_cassandra = BashOperator(
        task_id="verify_cassandra_output",
        bash_command="""
            COUNT=$(cqlsh -e "SELECT COUNT(*) FROM gaming_recommender.user_recommendations;" \
                    | grep -oP '\\d+' | tail -1)
            echo "Recomendaciones en Cassandra: $COUNT"
            [ "$COUNT" -gt "0" ] || (echo "ERROR: sin recomendaciones" && exit 1)
        """
    )

    run_monitor_agent = BashOperator(
        task_id="run_monitor_agent",
        bash_command=f"""
            curl -s http://localhost:8000/api/agent/monitor | \
                python3 -m json.tool | grep -E '"drift_detected|ai_diagnosis"'
        """
    )

    notify_completion = BashOperator(
        task_id="notify_completion",
        bash_command=f"""
            {PYTHON} - <<'EOF'
from cassandra.cluster import Cluster
from datetime import date

s = Cluster(['localhost']).connect('gaming_recommender')
users = s.execute("SELECT COUNT(DISTINCT user_id) FROM user_recommendations")[0][0]
print(f"\\n=== Pipeline KDD completado — {{{{ ds }}}} ===")
print(f"Usuarios con recomendaciones: {{users:,}}")
EOF
        """
    )

    check_services >> validate_datasets >> run_kdd_pipeline
    run_kdd_pipeline >> verify_cassandra >> run_monitor_agent >> notify_completion
```

---

## FASE 7 — CASSANDRA SCHEMA

### ARCHIVO: `5_serving_layer/cassandra_schema.cql`

```sql
CREATE KEYSPACE IF NOT EXISTS gaming_recommender
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE gaming_recommender;

-- Recomendaciones híbridas por usuario
CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id             TEXT,
    recommended_game    TEXT,
    hybrid_score        DOUBLE,
    cf_component        DOUBLE,
    cb_component        DOUBLE,
    recommendation_rank INT,
    alpha_used          DOUBLE,
    source              TEXT,    -- 'batch' | 'speed_layer'
    generated_at        TIMESTAMP,
    PRIMARY KEY (user_id, hybrid_score, recommended_game)
) WITH CLUSTERING ORDER BY (hybrid_score DESC)
  AND default_time_to_live = 86400;  -- TTL 1 día (se regenera en batch)

-- Similitud de contenido entre juegos
CREATE TABLE IF NOT EXISTS game_similarity (
    game_a      TEXT,
    game_b      TEXT,
    similarity  DOUBLE,
    PRIMARY KEY (game_a, similarity, game_b)
) WITH CLUSTERING ORDER BY (similarity DESC);

-- Métricas del modelo para el Agente Monitor
CREATE TABLE IF NOT EXISTS model_metrics (
    run_id          UUID,
    evaluated_at    TIMESTAMP,
    dt              TEXT,
    rmse            DOUBLE,
    precision_at_k  DOUBLE,
    recall_at_k     DOUBLE,
    k               INT,
    alpha           DOUBLE,
    users_with_recs BIGINT,
    model_path      TEXT,
    PRIMARY KEY (run_id, evaluated_at)
) WITH CLUSTERING ORDER BY (evaluated_at DESC);

-- Caché del historial de usuario para el Agente Recomendador
CREATE TABLE IF NOT EXISTS user_history_cache (
    user_id     TEXT,
    game_title  TEXT,
    hours_played DOUBLE,
    last_played TIMESTAMP,
    PRIMARY KEY (user_id, hours_played, game_title)
) WITH CLUSTERING ORDER BY (hours_played DESC)
  AND default_time_to_live = 604800;  -- TTL 7 días

-- Juegos populares (fallback cold start)
CREATE TABLE IF NOT EXISTS popular_games_fallback (
    recommended_game TEXT,
    hybrid_score     DOUBLE,
    PRIMARY KEY (recommended_game)
);

-- Speed Layer: ventanas en tiempo real
CREATE TABLE IF NOT EXISTS player_windows (
    user_id         TEXT,
    window_start    TIMESTAMP,
    events_in_batch BIGINT,
    avg_engagement  DOUBLE,
    avg_hours       DOUBLE,
    PRIMARY KEY (user_id, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 604800;

-- KDD Insights
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

-- Índices secundarios
CREATE INDEX IF NOT EXISTS ON user_recommendations (recommended_game);
CREATE INDEX IF NOT EXISTS ON kdd_insights (severity);
```

---

## ORDEN DE EJECUCIÓN (AGÉNTICO)

El agente debe ejecutar estos pasos en orden, verificando cada uno antes de continuar:

```
STEP 1 — Infraestructura
  bash 2_kafka/setup_topics.sh
  cqlsh -f 5_serving_layer/cassandra_schema.cql
  hive -f 1_ingesta/kaggle_to_hive/hive_schema.sql
  → Verificar: kafka-topics --list | grep gaming

STEP 2 — Datos Kaggle
  pip install kaggle pandas pyspark
  python 0_data/kaggle/download_datasets.py
  → Verificar: ls -lh 0_data/raw/

STEP 3 — Carga a Hive
  spark-submit 1_ingesta/kaggle_to_hive/loader.py
  → Verificar: hive -e "SELECT COUNT(*) FROM gaming_recommender.user_behaviors"
  → Esperado: ~180,000 registros

STEP 4 — APIs en tiempo real
  cd 1_ingesta/api_producer && pip install -r requirements.txt
  python freetogame_producer.py &
  python steamspy_producer.py &
  → Verificar: kafka-console-consumer --topic gaming.events.raw --max-messages 5

STEP 5 — Speed Layer
  spark-submit 4_speed_layer/streaming_kdd.py &
  → Verificar: logs mostrando "Speed Layer Recommender arrancado"

STEP 6 — Batch Layer (primera ejecución manual)
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 \
    3_batch_layer/kdd_pipeline.py --alpha 0.7
  → Verificar: cqlsh -e "SELECT COUNT(*) FROM gaming_recommender.user_recommendations"

STEP 7 — Airflow
  cp 6_orchestration/dags/gaming_kdd_batch.py $AIRFLOW_HOME/dags/
  airflow dags unpause gaming_kdd_recommender_pipeline

STEP 8 — API REST
  cd 5_serving_layer/api && pip install -r requirements.txt
  python main.py &
  → Verificar: curl http://localhost:8000/health

STEP 9 — Dashboard
  cd 7_dashboard && npm install && npm run dev
  → Verificar: http://localhost:5173
```

---

## NOTAS PARA EL AGENTE

1. Si el modelo ALS falla por memoria, reducir `rank` de 50 a 20 en `collaborative_filtering.py`
2. Si TF-IDF falla por tamaño de vocabulario, reducir `numFeatures` de 10000 a 5000
3. Si Cassandra rechaza escrituras, verificar que el keyspace existe: `DESCRIBE KEYSPACE gaming_recommender`
4. El parámetro `alpha=0.7` es el punto de partida — el Agente Monitor lo ajustará automáticamente
5. Para usuarios nuevos (cold start): el sistema usa automáticamente `popular_games_fallback`
6. Las explicaciones de Claude API se cachean en Cassandra para evitar llamadas repetidas
7. El Agente Monitor se ejecuta como endpoint GET — llamarlo periódicamente con un cron o desde el dashboard
8. Los datasets de Kaggle se descargan una sola vez — no borrar la carpeta `0_data/raw/`
```
