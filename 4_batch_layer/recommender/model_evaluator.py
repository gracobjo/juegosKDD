#!/usr/bin/env python3
"""
KDD Fase 5 (Evaluation) - Persiste metricas del modelo hibrido en:
  * gaming_recommender.model_metrics  (Hive - historico completo)
  * Cassandra gaming_recommender.model_metrics (vista rapida para agente monitor)
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import _hive_io as hio  # type: ignore

log = logging.getLogger(__name__)


def evaluate_and_store_metrics(
    spark: SparkSession,
    cf_results: dict[str, Any],
    hybrid_recs,
    alpha: float,
    date_str: str,
    write_cassandra: bool = True,
) -> dict[str, Any]:
    """Calcula stats del pipeline y los escribe en Hive + Cassandra."""
    users_with_recs = hybrid_recs.select("user_id").distinct().count()
    games_covered = hybrid_recs.select("recommended_game").distinct().count()

    metrics_row = {
        "model_name": "hybrid_als_tfidf",
        "run_id": str(uuid.uuid4()),
        "evaluated_at": datetime.utcnow(),
        "dt": date_str,
        "rmse": float(cf_results.get("rmse", 0.0) or 0.0),
        "precision_at_k": float(cf_results.get("precision_at_k", 0.0) or 0.0),
        "recall_at_k": float(cf_results.get("recall_at_k", 0.0) or 0.0),
        "k": int(cf_results.get("k", 10)),
        "alpha": float(alpha),
        "users_with_recs": int(users_with_recs),
        "games_covered": int(games_covered),
        "model_path": cf_results.get("model_path", ""),
    }

    log.info("METRICS %s", metrics_row)

    metrics_df = spark.createDataFrame([metrics_row])

    # Hive (historico) - particionado por dt
    try:
        if hio.table_exists(spark, "model_metrics"):
            hio.write_table(spark, metrics_df, "model_metrics", mode="append",
                            partition_by=["dt"], register_external=False)
        else:
            hio.write_table(spark, metrics_df, "model_metrics", partition_by=["dt"])
    except Exception as exc:
        log.warning("No se pudo escribir metrics en Hive: %s", exc)

    # Cassandra (serving) - best effort
    if write_cassandra:
        try:
            cass = metrics_df.withColumn(
                "run_id", F.col("run_id").cast("string")
            )
            (
                cass.write.format("org.apache.spark.sql.cassandra")
                .mode("append")
                .options(table="model_metrics", keyspace="gaming_recommender")
                .save()
            )
            log.info("METRICS: escritas en Cassandra gaming_recommender.model_metrics")
        except Exception as exc:
            log.warning("No se pudo escribir metrics en Cassandra: %s", exc)

    return metrics_row
