#!/usr/bin/env python3
"""
KDD Evaluation — métricas del modelo y persistencia en Hive + Cassandra.
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, desc, expr, row_number
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

log = logging.getLogger(__name__)


def evaluate_and_store_metrics(
    spark: SparkSession,
    cf_results: dict,
    hybrid_df,
    run_date: str,
    alpha: float,
):
    users_with_recs = hybrid_df.select("user_id").distinct().count()
    rmse = float(cf_results.get("rmse", 0.0))
    pk = float(cf_results.get("precision_at_k", 0.0))
    rk = 0.0
    k = int(cf_results.get("k", 10))
    model_path = str(cf_results.get("model_path", ""))

    log.info(
        "Métricas — RMSE=%.4f P@%s=%.4f usuarios_con_recs=%s",
        rmse,
        k,
        pk,
        f"{users_with_recs:,}",
    )

    base = spark.createDataFrame(
        [
            (
                run_date,
                rmse,
                pk,
                rk,
                k,
                float(alpha),
                int(users_with_recs),
                model_path,
            )
        ],
        schema=StructType(
            [
                StructField("dt", StringType(), False),
                StructField("rmse", DoubleType(), False),
                StructField("precision_at_k", DoubleType(), False),
                StructField("recall_at_k", DoubleType(), False),
                StructField("k", IntegerType(), False),
                StructField("alpha", DoubleType(), False),
                StructField("users_with_recs", LongType(), False),
                StructField("model_path", StringType(), False),
            ]
        ),
    )
    metrics_df = base.withColumn("run_id", expr("uuid()")).withColumn(
        "evaluated_at", current_timestamp()
    )

    (
        metrics_df.write.mode("append")
        .format("parquet")
        .saveAsTable("gaming_recommender.model_metrics_hive")
    )

    (
        metrics_df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="model_metrics", keyspace="gaming_recommender")
        .save()
    )

    return {
        "rmse": rmse,
        "precision_at_k": pk,
        "recall_at_k": rk,
        "users_with_recs": users_with_recs,
        "k": k,
    }


def write_user_history_cache(spark: SparkSession):
    """Top juegos por usuario → Cassandra (contexto para explicaciones)."""
    beh = spark.table("gaming_recommender.user_behaviors").filter(col("hours_played") > 0)
    w = Window.partitionBy("user_id").orderBy(desc("hours_played"))
    hist = (
        beh.withColumn("rn", row_number().over(w))
        .filter(col("rn") <= 15)
        .drop("rn")
        .select(
            col("user_id"),
            col("game_title").alias("game_title"),
            col("hours_played").cast("double").alias("hours_played"),
            current_timestamp().alias("last_played"),
        )
    )
    (
        hist.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="user_history_cache", keyspace="gaming_recommender")
        .save()
    )
    log.info("user_history_cache escrito (%s filas)", f"{hist.count():,}")
