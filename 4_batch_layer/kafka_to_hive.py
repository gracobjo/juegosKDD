#!/usr/bin/env python3
"""
Kafka → HDFS/Hive bridge — rellena player_snapshots para el Batch Layer.
-----------------------------------------------------------------------------
Consume el topic `gaming.events.raw` en modo batch (startingOffsets=earliest,
endingOffsets=latest) y escribe Parquet particionado por `dt` en la ruta que
la tabla externa Hive `gaming_kdd.player_snapshots` espera.

Esto cierra el hueco que dejaba la Arquitectura Lambda sin NiFi/Kafka Connect:
el Speed Layer sigue alimentando Cassandra para la vista en tiempo real y,
paralelamente, este job drena el mismo topic a Hive para que el Spark Batch
pueda ejecutar el KDD diario sobre datos históricos.

Uso directo:
  spark-submit \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
    kafka_to_hive.py [--date YYYY-MM-DD] [--bootstrap localhost:9092]

Desde el DAG se llama con --date={{ ds }}. Si no se pasa --date se drena todo
lo que haya en el topic y se particiona por el campo `dt` que trae cada evento
(el producer ya lo emite en cada mensaje).
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
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

# Columnas que espera la tabla Hive gaming_kdd.player_snapshots (sin `dt`, que
# es la columna de partición y se maneja aparte con partitionBy).
HIVE_COLUMNS = [
    "event_id", "ts", "hour", "appid", "game", "genre", "price_usd",
    "current_players", "player_tier", "owners_range", "average_playtime",
    "median_playtime", "positive_reviews", "negative_reviews",
    "review_score", "health_score", "total_achievements", "source",
    "pipeline_version",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None,
                        help="YYYY-MM-DD. Si se pasa, filtra solo eventos de ese día.")
    parser.add_argument("--topic", default="gaming.events.raw")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--warehouse", default="/user/hive/warehouse/gaming_kdd")
    args = parser.parse_args()

    target_dir = f"{args.warehouse}/player_snapshots"
    print(f"=== Kafka → Hive bridge ===")
    print(f"  topic      : {args.topic}")
    print(f"  bootstrap  : {args.bootstrap}")
    print(f"  target dir : {target_dir}")
    print(f"  filter dt  : {args.date or '(all available)'}")

    spark = (
        SparkSession.builder
        .appName("KDD_Gaming_KafkaToHive")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Lectura batch del topic: earliest → latest (drena lo que haya).
    raw = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # Parseo del JSON según el schema del producer.
    parsed = (
        raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .filter(col("event_id").isNotNull())
    )

    if args.date:
        parsed = parsed.filter(col("dt") == args.date)

    # Normalizamos `dt` (por si algún mensaje viene con formato raro).
    normalized = parsed.withColumn("dt", to_date(col("dt")).cast("string"))

    total = normalized.count()
    print(f"  → {total} eventos a escribir")

    if total == 0:
        print("  ! No hay eventos que escribir. Nada que hacer.")
        spark.stop()
        return 0

    # Escritura Parquet particionada por dt. Usamos saveAsTable para que Hive
    # Metastore registre la partición automáticamente. append para no pisar
    # datos previos.
    (
        normalized.select(*HIVE_COLUMNS, "dt")
        .write
        .mode("append")
        .format("parquet")
        .partitionBy("dt")
        .option("path", target_dir)
        .saveAsTable("gaming_kdd.player_snapshots")
    )

    # Refrescamos la tabla para que el Metastore vea las nuevas particiones.
    spark.sql("MSCK REPAIR TABLE gaming_kdd.player_snapshots")

    print(f"=== OK: {total} eventos escritos en Hive gaming_kdd.player_snapshots ===")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
