#!/usr/bin/env python3
"""
KDD Fase 4 (Mining) - Filtrado Colaborativo con ALS (Spark MLlib).

Entrena sobre gaming_recommender.user_behaviors usando el rating implicito
(log1p(hours)) y serializa el modelo en HDFS. Devuelve un dict con RMSE,
Precision@K, rutas y dataframes de recomendaciones crudas.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import _hive_io as hio  # type: ignore

log = logging.getLogger(__name__)

MODEL_BASE = "hdfs:///user/gaming_recommender/models"


@dataclass
class CFResult:
    rmse: float
    precision_at_k: float
    recall_at_k: float
    k: int
    users_in_train: int
    users_in_test: int
    model_path: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "rmse": self.rmse,
            "precision_at_k": self.precision_at_k,
            "recall_at_k": self.recall_at_k,
            "k": self.k,
            "users_in_train": self.users_in_train,
            "users_in_test": self.users_in_test,
            "model_path": self.model_path,
        }


def _rank_by(col: str, spark: SparkSession, df, out_col: str):
    """Devuelve df con una columna out_col = dense_rank(col asc)."""
    w = Window.orderBy(col)
    return df.withColumn(out_col, F.dense_rank().over(w).cast("integer"))


def _save_index_maps(spark: SparkSession, behaviors):
    user_map = behaviors.select("user_id").distinct()
    user_map = user_map.withColumn(
        "user_idx",
        F.dense_rank().over(Window.orderBy("user_id")).cast("integer"),
    )
    game_map = behaviors.select("game_title").distinct()
    game_map = game_map.withColumn(
        "game_idx",
        F.dense_rank().over(Window.orderBy("game_title")).cast("integer"),
    )
    hio.write_table(spark, user_map, "user_index_map")
    hio.write_table(spark, game_map, "game_index_map")
    return user_map, game_map


def train_collaborative_filter(
    spark: SparkSession,
    rank: int = 32,
    max_iter: int = 12,
    reg_param: float = 0.08,
    alpha_conf: float = 30.0,
    k_top: int = 10,
) -> CFResult:
    """Entrena ALS implicito y deja el modelo en HDFS."""
    log.info("CF[ALS] rank=%s maxIter=%s regParam=%s alpha=%s", rank, max_iter, reg_param, alpha_conf)

    behaviors = (
        hio.read_table(spark, "user_behaviors")
        .filter(F.col("hours_played") > 0)
        .select("user_id", "game_title", "implicit_rating")
    )

    user_map, game_map = _save_index_maps(spark, behaviors)

    ratings = (
        behaviors
        .join(user_map, "user_id")
        .join(game_map, "game_title")
        .select(
            F.col("user_idx"),
            F.col("game_idx"),
            F.col("implicit_rating").cast("float").alias("rating"),
        )
    ).cache()

    train, test = ratings.randomSplit([0.8, 0.2], seed=42)
    n_train = train.count()
    n_test = test.count()
    log.info("CF[ALS] train=%s test=%s", f"{n_train:,}", f"{n_test:,}")

    als = ALS(
        rank=rank,
        maxIter=max_iter,
        regParam=reg_param,
        userCol="user_idx",
        itemCol="game_idx",
        ratingCol="rating",
        implicitPrefs=True,
        alpha=alpha_conf,
        coldStartStrategy="drop",
        nonnegative=True,
        seed=42,
    )
    model = als.fit(train)

    # RMSE sobre test (escala del rating implicito)
    preds = model.transform(test)
    evaluator = RegressionEvaluator(
        metricName="rmse", labelCol="rating", predictionCol="prediction"
    )
    try:
        rmse = float(evaluator.evaluate(preds))
    except Exception:
        rmse = float("nan")
    log.info("CF[ALS] RMSE=%.4f", rmse)

    # Precision@K / Recall@K aproximados
    relevant = test.filter(F.col("rating") > 0.5)
    recs = model.recommendForAllUsers(k_top).selectExpr(
        "user_idx", "explode(recommendations) as rec"
    ).select("user_idx", F.col("rec.game_idx").alias("game_idx"))

    hits = recs.join(relevant, ["user_idx", "game_idx"])
    denom_p = max(recs.count(), 1)
    denom_r = max(relevant.count(), 1)
    n_hits = hits.count()
    precision = n_hits / denom_p
    recall = n_hits / denom_r
    log.info("CF[ALS] Precision@%d=%.4f Recall@%d=%.4f", k_top, precision, k_top, recall)

    model_path = f"{MODEL_BASE}/als_model"
    try:
        model.write().overwrite().save(model_path)
        log.info("CF[ALS] modelo guardado en %s", model_path)
    except Exception as exc:
        log.warning("CF[ALS] no se pudo guardar en HDFS (%s). Intentando ruta local.", exc)
        model_path = "file:///tmp/gaming_recommender/models/als_model"
        model.write().overwrite().save(model_path)
        log.info("CF[ALS] modelo guardado en %s", model_path)

    # Dataset de recomendaciones top-50 (para el rerank hibrido)
    raw_recs = model.recommendForAllUsers(50)
    cf_cands = (
        raw_recs.selectExpr("user_idx", "explode(recommendations) as rec")
        .select(
            "user_idx",
            F.col("rec.game_idx").alias("game_idx"),
            F.col("rec.rating").alias("cf_score"),
        )
    )
    hio.write_table(spark, cf_cands, "cf_candidates")

    users_train = train.select("user_idx").distinct().count()
    users_test = test.select("user_idx").distinct().count()

    return CFResult(
        rmse=rmse,
        precision_at_k=precision,
        recall_at_k=recall,
        k=k_top,
        users_in_train=users_train,
        users_in_test=users_test,
        model_path=model_path,
    )
