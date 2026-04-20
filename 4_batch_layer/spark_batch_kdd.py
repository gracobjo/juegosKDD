#!/usr/bin/env python3
"""
Spark Batch — Batch Layer / KDD Completo
-----------------------------------------------------------------------------
Ejecutado diariamente por Airflow a las 02:00 UTC. Aplica el pipeline KDD
completo sobre la partición del día en Hive y escribe los resultados en
Cassandra (game_stats_daily + kdd_insights) y en Hive (kdd_daily_summary).

Uso:
  spark-submit spark_batch_kdd.py --date 2026-04-20
"""

import argparse
import uuid
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    desc,
    lit,
    max as spark_max,
    min as spark_min,
    rank,
    round as spark_round,
    stddev,
    when,
)
from pyspark.sql.window import Window

# ── Args ─────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--date", default=datetime.utcnow().strftime("%Y-%m-%d"))
args = parser.parse_args()

PROCESS_DATE = args.date
PREV_DATE = (
    datetime.strptime(PROCESS_DATE, "%Y-%m-%d") - timedelta(days=1)
).strftime("%Y-%m-%d")

print(f"=== KDD Batch Layer — procesando fecha: {PROCESS_DATE} ===")

# ── SparkSession con Hive + Cassandra ────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName(f"KDD_Gaming_BatchLayer_{PROCESS_DATE}")
    .config(
        "spark.jars.packages",
        "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0",
    )
    .config("spark.cassandra.connection.host", "localhost")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── 1. SELECTION — Leer partición de Hive ────────────────────────────────────
print("KDD fase 1: Selection")
raw = spark.sql(
    f"SELECT * FROM gaming_kdd.player_snapshots WHERE dt = '{PROCESS_DATE}'"
)
raw.cache()
print(f"  → {raw.count()} registros seleccionados")

# ── 2. PREPROCESSING ─────────────────────────────────────────────────────────
print("KDD fase 2: Preprocessing")
cleaned = (
    raw
    .filter(col("current_players") >= 0)
    .filter(col("review_score").between(0, 100))
    .filter(col("health_score").between(0, 100))
    .dropDuplicates(["event_id"])
    .fillna({
        "average_playtime": 0,
        "total_achievements": 0,
        "genre": "Unknown",
    })
)
print(f"  → {cleaned.count()} registros limpios")

# ── 3. TRANSFORMATION — Ingeniería de características ────────────────────────
print("KDD fase 3: Transformation")
transformed = (
    cleaned
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
            (col("average_playtime") / 60.0) * (col("review_score") / 100.0),
            3,
        ),
    )
)

# ── 4. MINING — Agregaciones diarias ─────────────────────────────────────────
print("KDD fase 4: Mining")
daily = (
    transformed.groupBy("appid", "game", "genre")
    .agg(
        spark_round(avg("current_players"), 0).alias("avg_players"),
        spark_max("current_players").alias("max_players"),
        spark_min("current_players").alias("min_players"),
        spark_round(stddev("current_players"), 1).alias("stddev_players"),
        spark_round(avg("review_score"), 1).alias("avg_review_score"),
        spark_round(avg("health_score"), 1).alias("avg_health_score"),
        spark_round(avg("engagement_index"), 3).alias("avg_engagement"),
        count("*").alias("total_snapshots"),
    )
)

# ── Calcular growth_rate respecto al día anterior ────────────────────────────
def _table_exists(spark_session, fqname: str) -> bool:
    try:
        return spark_session.catalog.tableExists(fqname)
    except Exception:
        db, tbl = fqname.split(".")
        return tbl in [t.name for t in spark_session.catalog.listTables(db)]


if _table_exists(spark, "gaming_kdd.kdd_daily_summary"):
    prev = spark.sql(
        f"""
        SELECT appid, avg_players AS prev_players
        FROM gaming_kdd.kdd_daily_summary
        WHERE dt = '{PREV_DATE}'
        """
    )
    daily = (
        daily.join(prev, "appid", "left")
        .withColumn(
            "growth_rate",
            when(
                col("prev_players") > 0,
                spark_round(
                    (col("avg_players") - col("prev_players"))
                    / col("prev_players")
                    * 100,
                    1,
                ),
            ).otherwise(lit(0.0)),
        )
        .drop("prev_players")
    )
else:
    daily = daily.withColumn("growth_rate", lit(0.0))

# ── Rankings ─────────────────────────────────────────────────────────────────
w = Window.orderBy(desc("avg_players"))
daily = (
    daily
    .withColumn("player_rank", rank().over(w))
    .withColumn("dt", lit(PROCESS_DATE))
)
daily.cache()
print(f"  → {daily.count()} juegos procesados")

# ── 5. EVALUATION — Insights batch ───────────────────────────────────────────
print("KDD fase 5: Evaluation")

insights = []
for row in daily.collect():
    avg_p  = row.avg_players or 0
    growth = row.growth_rate or 0
    std_p  = row.stddev_players or 0

    if growth < -20 and avg_p > 1000:
        insights.append({
            "insight_id":   uuid.uuid4(),
            "created_at":   datetime.utcnow(),
            "game":         row.game,
            "appid":        row.appid,
            "insight_type": "player_decline",
            "message":      f"{row.game}: caída del {abs(growth):.1f}% de jugadores vs ayer",
            "severity":     "alert",
            "metric_value": float(growth),
            "metric_name":  "growth_rate",
            "layer":        "batch",
        })

    if growth > 30 and avg_p > 500:
        insights.append({
            "insight_id":   uuid.uuid4(),
            "created_at":   datetime.utcnow(),
            "game":         row.game,
            "appid":        row.appid,
            "insight_type": "player_growth",
            "message":      f"{row.game}: crecimiento del {growth:.1f}% respecto a ayer",
            "severity":     "info",
            "metric_value": float(growth),
            "metric_name":  "growth_rate",
            "layer":        "batch",
        })

    if std_p and std_p > avg_p * 0.5:
        insights.append({
            "insight_id":   uuid.uuid4(),
            "created_at":   datetime.utcnow(),
            "game":         row.game,
            "appid":        row.appid,
            "insight_type": "irregular_activity",
            "message":      f"{row.game}: actividad irregular (σ={std_p:.0f} vs μ={avg_p:.0f})",
            "severity":     "warning",
            "metric_value": float(std_p),
            "metric_name":  "stddev_players",
            "layer":        "batch",
        })

# ── Escritura en Cassandra: game_stats_daily ─────────────────────────────────
print(f"  → Escribiendo {daily.count()} filas en Cassandra game_stats_daily")
cassandra_df = daily.select(
    col("appid"),
    col("dt"),
    col("game"),
    col("genre"),
    col("avg_players"),
    col("max_players").cast("double").alias("max_players"),
    col("avg_review_score"),
    col("avg_health_score"),
    col("growth_rate"),
    col("player_rank"),
    col("total_snapshots"),
)
cassandra_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="game_stats_daily", keyspace="gaming_kdd") \
    .save()

# ── Escritura en Cassandra: kdd_insights ─────────────────────────────────────
if insights:
    print(f"  → Escribiendo {len(insights)} insights KDD")
    insights_df = spark.createDataFrame(insights)
    insights_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="kdd_insights", keyspace="gaming_kdd") \
        .save()

# ── Guardar resumen en Hive ──────────────────────────────────────────────────
print("  → Guardando resumen diario en Hive kdd_daily_summary")
(
    daily.write
    .mode("overwrite")
    .partitionBy("dt")
    .format("parquet")
    .saveAsTable("gaming_kdd.kdd_daily_summary")
)

print(f"=== Batch KDD completado para {PROCESS_DATE} ===")
spark.stop()
