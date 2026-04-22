#!/usr/bin/env python3
"""
Spark Streaming — Speed Layer / KDD Online
-----------------------------------------------------------------------------
Lee del topic Kafka `gaming.events.raw`, aplica el pipeline KDD en micro-batches
de 30 segundos y escribe en Cassandra:
  - player_windows   (ventanas de 5 minutos por juego)
  - kdd_insights     (insights generados por la fase Evaluation)

Submit recomendado:
  spark-submit \\
    --master local[*] \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,\\
com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 \\
    spark_streaming_kdd.py
"""

import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    from_json,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    when,
    window,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Schema del evento raw (debe coincidir con steam_producer.py) ─────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",           StringType()),
    StructField("batch_id",           StringType()),
    StructField("ts",                 LongType()),
    StructField("dt",                 StringType()),
    StructField("hour",               IntegerType()),
    StructField("appid",              StringType()),
    StructField("game",               StringType()),
    StructField("genre",              StringType()),
    StructField("price_usd",          DoubleType()),
    StructField("current_players",    IntegerType()),
    StructField("player_tier",        StringType()),
    StructField("owners_range",       StringType()),
    StructField("average_playtime",   IntegerType()),
    StructField("median_playtime",    IntegerType()),
    StructField("positive_reviews",   IntegerType()),
    StructField("negative_reviews",   IntegerType()),
    StructField("review_score",       DoubleType()),
    StructField("health_score",       DoubleType()),
    StructField("total_achievements", IntegerType()),
    StructField("source",             StringType()),
    StructField("pipeline_version",   StringType()),
    StructField("steam_api_ok",       BooleanType()),
    StructField("steamspy_api_ok",    BooleanType()),
])

# ── SparkSession ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("KDD_Gaming_SpeedLayer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1",
    )
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", "9042")
    # Checkpoint en filesystem LOCAL (no HDFS): si la ruta no lleva esquema
    # explícito, Spark la resuelve contra fs.defaultFS=hdfs://nodo1:9000 y
    # acabamos escribiendo cientos de .delta diminutos en HDFS, que con
    # replication=1 se corrompen al primer reinicio sucio.
    .config("spark.sql.streaming.checkpointLocation", "file:///tmp/kdd_checkpoint")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── 1. SELECTION — Leer stream de Kafka ──────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "gaming.events.raw")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 1000)
    .load()
)

# ── 2. PREPROCESSING — Parsear JSON y filtrar inválidos ──────────────────────
parsed = (
    raw_stream
    .select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"),
        col("timestamp").alias("kafka_ts"),
    )
    .select("e.*", "kafka_ts")
    .filter(col("steam_api_ok") == True)   # noqa: E712 — Spark requiere == True
    .filter(col("current_players") >= 0)
    .filter(col("review_score").between(0, 100))
    .filter(col("game").isNotNull())
)

# ── 3. TRANSFORMATION — Derivar features KDD ─────────────────────────────────
transformed = (
    parsed
    .withColumn("processed_at", current_timestamp())
    .withColumn(
        "review_tier",
        when(col("review_score") >= 90, "overwhelmingly_positive")
        .when(col("review_score") >= 80, "very_positive")
        .when(col("review_score") >= 70, "mostly_positive")
        .when(col("review_score") >= 40, "mixed")
        .otherwise("negative"),
    )
    .withColumn(
        "engagement_index",
        spark_round(
            (col("average_playtime") / 60.0)
            * (col("review_score") / 100.0)
            * (col("health_score") / 100.0),
            2,
        ),
    )
    .withColumn(
        "market_segment",
        when(col("price_usd") == 0, "free_to_play")
        .when(col("price_usd") < 10, "budget")
        .when(col("price_usd") < 30, "mid_range")
        .otherwise("premium"),
    )
)

# ── 4. MINING — Agregaciones por ventana de 1 minuto (dev-friendly) ─────────
# En producción, subir a window=5min y watermark=10min. Reducido para que el
# dashboard muestre datos dentro del primer par de minutos tras arrancar.
windowed = (
    transformed
    .withWatermark("processed_at", "2 minutes")
    .groupBy(
        window("processed_at", "1 minute"),
        col("game"),
        col("appid"),
        col("genre"),
        col("market_segment"),
    )
    .agg(
        spark_round(avg("current_players"), 0).alias("avg_players"),
        spark_max("current_players").cast("double").alias("max_players"),
        spark_min("current_players").cast("double").alias("min_players"),
        spark_round(avg("review_score"), 1).alias("avg_review_score"),
        spark_round(avg("health_score"), 1).alias("avg_health_score"),
        spark_round(avg("engagement_index"), 3).alias("avg_engagement"),
        count("*").alias("snapshots"),
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end",   col("window.end"))
    .withColumn(
        "player_tier",
        when(col("avg_players") >= 100000, "massive")
        .when(col("avg_players") >= 10000, "popular")
        .when(col("avg_players") >= 1000, "active")
        .otherwise("niche"),
    )
    .drop("window")
)

# Columnas que coinciden con el schema de la tabla Cassandra player_windows
WINDOW_COLUMNS = [
    "game", "window_start", "window_end", "appid",
    "avg_players", "max_players", "min_players",
    "avg_review_score", "avg_health_score",
    "snapshots", "player_tier",
]


# ── 5. EVALUATION — Generar insights automáticos ─────────────────────────────
def write_batch(batch_df, batch_id):
    """foreachBatch: escribe ventanas en Cassandra y genera insights KDD."""
    n = batch_df.count()
    print(f"▸ [speed] batch {batch_id}: {n} ventanas agregadas", flush=True)
    if n == 0:
        return

    # → Serving Layer (Cassandra): player_windows
    batch_df.select(*WINDOW_COLUMNS).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="player_windows", keyspace="gaming_kdd") \
        .save()
    print(f"  ✓ {n} filas escritas en gaming_kdd.player_windows", flush=True)

    # → Fase Evaluation: reglas sobre el batch agregado
    insights = []
    for row in batch_df.collect():
        avg_p = row.avg_players or 0
        max_p = row.max_players or 0
        health = row.avg_health_score or 0

        if max_p > avg_p * 1.5 and avg_p > 1000:
            insights.append({
                "insight_id":   uuid.uuid4(),
                "created_at":   datetime.utcnow(),
                "game":         row.game,
                "appid":        row.appid,
                "insight_type": "player_surge",
                "message":      f"{row.game}: pico de {int(max_p):,} jugadores (+50% sobre media)",
                "severity":     "info",
                "metric_value": float(max_p),
                "metric_name":  "current_players",
                "layer":        "speed",
            })

        if health and health < 40:
            insights.append({
                "insight_id":   uuid.uuid4(),
                "created_at":   datetime.utcnow(),
                "game":         row.game,
                "appid":        row.appid,
                "insight_type": "health_alert",
                "message":      f"{row.game}: health score crítico ({health:.1f}/100)",
                "severity":     "warning",
                "metric_value": float(health),
                "metric_name":  "health_score",
                "layer":        "speed",
            })

    if insights:
        insights_df = batch_df.sparkSession.createDataFrame(insights)
        insights_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="kdd_insights", keyspace="gaming_kdd") \
            .save()
        print(f"  ✓ {len(insights)} insights escritos en gaming_kdd.kdd_insights", flush=True)


# ── Arrancar streaming ────────────────────────────────────────────────────────
query = (
    windowed.writeStream
    .foreachBatch(write_batch)
    .trigger(processingTime="30 seconds")
    .outputMode("update")
    .option("checkpointLocation", "file:///tmp/kdd_checkpoint/speed")
    .start()
)

print("✓ Speed Layer arrancado. Esperando eventos de Kafka gaming.events.raw...")
query.awaitTermination()
