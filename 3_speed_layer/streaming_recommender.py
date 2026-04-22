#!/usr/bin/env python3
"""
Speed Layer del recomendador - KDD online.

Consume gaming.user.interactions desde Kafka, aplica el modelo ALS ya entrenado
(cargado desde HDFS) sobre los usuarios activos del batch y:
  1) Actualiza player_windows (engagement en tiempo real).
  2) Recomputa top-10 recomendaciones y las upserts en user_recommendations.
  3) Detecta anomalias (sesiones desproporcionadas, engagement bajo) en
     gaming_recommender.kdd_insights.

Submit recomendado:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
    3_speed_layer/streaming_recommender.py
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Mismo helper Hive que el batch (Parquet directo en HDFS, sin hive-metastore client).
# Se incluye via --py-files al hacer spark-submit.
try:
    import _hive_io as hio  # type: ignore
except ImportError:  # pragma: no cover - fallback si se lanza fuera de spark-submit
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "4_batch_layer" / "recommender"))
    import _hive_io as hio  # type: ignore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [streaming-recommender] %(message)s")
log = logging.getLogger(__name__)

INTERACTION_SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("game_title", StringType()),
    StructField("event_type", StringType()),  # play | purchase | review
    StructField("hours", FloatType()),
    StructField("recommended", BooleanType()),
    StructField("ts", LongType()),
])

ALS_MODEL_PATH = os.getenv(
    "ALS_MODEL_PATH", "hdfs:///user/gaming_recommender/models/als_model"
)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
TOPIC = os.getenv("TOPIC_USER_INTERACTIONS", "gaming.user.interactions")
CHECKPOINT = os.getenv(
    "STREAMING_CHECKPOINT", "file:///tmp/kdd_recommender_checkpoint/interactions"
)


def build_spark() -> SparkSession:
    # OJO: no usamos enableHiveSupport() porque el Hive client embebido de
    # Spark (2.3.9) es incompatible con nuestro Metastore (Hive 4.2.0).
    # Leemos los mapas directamente desde Parquet en HDFS via _hive_io.
    return (
        SparkSession.builder
        .appName("KDD_Gaming_SpeedLayer_Recommender")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT)
        .getOrCreate()
    )


def load_als_model(spark: SparkSession):
    """Carga ALS si existe. Si no, devuelve None y el pipeline sigue sin CF."""
    from pyspark.ml.recommendation import ALSModel

    try:
        model = ALSModel.load(ALS_MODEL_PATH)
        log.info("ALS cargado desde %s", ALS_MODEL_PATH)
        return model
    except Exception as exc:
        log.warning("No se pudo cargar ALS (%s). Speed-Layer operara sin CF online.", exc)
        return None


def make_processor(spark: SparkSession, als_model):
    """Factory que encapsula el estado (modelo + mapas) en el closure."""
    # Cacheamos mapas (user_idx / game_idx) una sola vez, no por batch.
    try:
        user_idx_map = hio.read_table(spark, "user_index_map").cache()
        game_idx_map = hio.read_table(spark, "game_index_map").cache()
        log.info(
            "Mapas cargados: %d usuarios x %d juegos",
            user_idx_map.count(),
            game_idx_map.count(),
        )
    except Exception as exc:
        log.warning("No hay mapas user_index/game_index (%s)", exc)
        user_idx_map = None
        game_idx_map = None

    def process_batch(batch_df, batch_id):
        n = batch_df.count()
        if n == 0:
            return
        log.info("[batch %s] %d eventos", batch_id, n)

        # ── Aggregacion por usuario ─────────────────────────────────────────
        windows = (
            batch_df.groupBy("user_id")
            .agg(
                F.count("*").alias("events_in_batch"),
                F.avg("engagement_signal").alias("avg_engagement"),
                F.coalesce(F.avg("hours"), F.lit(0.0)).alias("avg_hours"),
            )
            .withColumn("window_start", F.current_timestamp())
        )
        try:
            (
                windows.select(
                    F.col("user_id").cast("string"),
                    F.col("window_start").cast("timestamp"),
                    F.col("events_in_batch").cast("bigint"),
                    F.col("avg_engagement").cast("double"),
                    F.col("avg_hours").cast("double"),
                )
                .write.format("org.apache.spark.sql.cassandra")
                .mode("append")
                .options(table="player_windows", keyspace="gaming_recommender")
                .save()
            )
        except Exception as exc:
            log.warning("Fallo escribiendo player_windows: %s", exc)

        # ── Recomendaciones incrementales ───────────────────────────────────
        if als_model is not None and user_idx_map is not None and game_idx_map is not None:
            try:
                active_users = batch_df.select("user_id").distinct()
                known = active_users.join(user_idx_map, "user_id").select("user_idx", "user_id")
                if known.count() > 0:
                    recs = als_model.recommendForUserSubset(known, 10)
                    recs_expanded = (
                        recs.selectExpr("user_idx", "explode(recommendations) as rec")
                        .select(
                            "user_idx",
                            F.col("rec.game_idx").alias("game_idx"),
                            F.col("rec.rating").alias("cf_score"),
                        )
                        .join(user_idx_map, "user_idx")
                        .join(game_idx_map, "game_idx")
                        .select(
                            F.col("user_id").cast("string"),
                            F.col("game_title").cast("string").alias("recommended_game"),
                            F.col("cf_score").cast("double").alias("hybrid_score"),
                            F.col("cf_score").cast("double").alias("cf_component"),
                            F.lit(0.0).cast("double").alias("cb_component"),
                            F.lit(0).cast("int").alias("recommendation_rank"),
                            F.lit(1.0).cast("double").alias("alpha_used"),
                            F.lit("speed_layer").alias("source"),
                            F.current_timestamp().alias("generated_at"),
                        )
                    )
                    (
                        recs_expanded.write.format("org.apache.spark.sql.cassandra")
                        .mode("append")
                        .options(table="user_recommendations", keyspace="gaming_recommender")
                        .save()
                    )
                    log.info("  + recomendaciones actualizadas para %d usuarios", known.count())
            except Exception as exc:
                log.warning("Fallo actualizando recomendaciones online: %s", exc)

        # ── Deteccion de anomalias ─────────────────────────────────────────
        try:
            anomalies = batch_df.filter(
                (F.col("hours") > 100) | (F.col("engagement_signal") < 0.15)
            )
            if anomalies.count() > 0:
                insights = (
                    anomalies
                    .withColumn("insight_id", F.expr("uuid()"))
                    .withColumn("created_at", F.current_timestamp())
                    .withColumn("insight_type", F.lit("anomaly_detected"))
                    .withColumn(
                        "severity",
                        F.when(F.col("hours") > 100, F.lit("warning")).otherwise(F.lit("info")),
                    )
                    .withColumn(
                        "message",
                        F.concat_ws(
                            " ",
                            F.lit("user"),
                            F.col("user_id"),
                            F.lit("- event"),
                            F.col("event_type"),
                            F.lit("- hours"),
                            F.col("hours").cast("string"),
                        ),
                    )
                    .withColumn("metric_name", F.lit("hours"))
                    .withColumn("metric_value", F.col("hours").cast("double"))
                    .withColumn("layer", F.lit("speed"))
                    .select(
                        F.col("insight_id").cast("string"),
                        "created_at",
                        "insight_type",
                        "severity",
                        "message",
                        F.col("user_id").cast("string"),
                        F.col("game_title").cast("string"),
                        "metric_name",
                        "metric_value",
                        "layer",
                    )
                )
                (
                    insights.write.format("org.apache.spark.sql.cassandra")
                    .mode("append")
                    .options(table="kdd_insights", keyspace="gaming_recommender")
                    .save()
                )
                log.info("  + %d anomalias enviadas a kdd_insights", insights.count())
        except Exception as exc:
            log.warning("Fallo detectando anomalias: %s", exc)

    return process_batch


def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    als_model = load_als_model(spark)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )

    events = (
        raw.select(
            F.from_json(F.col("value").cast("string"), INTERACTION_SCHEMA).alias("e"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("e.*", "kafka_ts")
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("game_title").isNotNull())
        .filter(F.col("event_type").isin("play", "purchase", "review"))
        .withColumn("game_title", F.trim(F.lower(F.col("game_title"))))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn(
            "engagement_signal",
            F.when(
                F.col("event_type") == "play",
                F.when(F.col("hours") > 10, F.lit(1.0))
                .when(F.col("hours") > 1, F.lit(0.7))
                .otherwise(F.lit(0.3)),
            )
            .when(F.col("event_type") == "purchase", F.lit(0.5))
            .when(
                F.col("event_type") == "review",
                F.when(F.col("recommended"), F.lit(0.9)).otherwise(F.lit(0.2)),
            )
            .otherwise(F.lit(0.1)),
        )
        .withColumn("hours", F.coalesce(F.col("hours"), F.lit(0.0)))
    )

    processor = make_processor(spark, als_model)

    query = (
        events.writeStream
        .foreachBatch(processor)
        .trigger(processingTime="30 seconds")
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )

    log.info("Speed Layer Recommender arrancado. Esperando eventos de %s...", TOPIC)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
