#!/usr/bin/env python3
"""
KDD Mining — ALS (filtrado colaborativo) sobre ratings implícitos.
"""

import logging

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

log = logging.getLogger(__name__)


def train_collaborative_filter(spark: SparkSession, alpha: float = 40.0):
    log.info("KDD Mining — Entrenando ALS (implicitPrefs=True)...")

    behaviors = (
        spark.table("gaming_recommender.user_behaviors")
        .filter(col("hours_played") > 0)
        .select("user_id", "game_title", "implicit_rating")
    )

    user_window = Window.orderBy("user_id")
    game_window = Window.orderBy("game_title")

    user_idx = (
        behaviors.select("user_id")
        .distinct()
        .withColumn("user_idx", dense_rank().over(user_window).cast("int"))
    )
    game_idx = (
        behaviors.select("game_title")
        .distinct()
        .withColumn("game_idx", dense_rank().over(game_window).cast("int"))
    )

    user_idx.write.mode("overwrite").format("parquet").saveAsTable(
        "gaming_recommender.user_index_map"
    )
    game_idx.write.mode("overwrite").format("parquet").saveAsTable(
        "gaming_recommender.game_index_map"
    )

    ratings = (
        behaviors.join(user_idx, "user_id")
        .join(game_idx, "game_title")
        .select(col("user_idx"), col("game_idx"), col("implicit_rating").alias("rating"))
    )

    train, test = ratings.randomSplit([0.8, 0.2], seed=42)
    log.info("Train: %s | Test: %s", f"{train.count():,}", f"{test.count():,}")

    als = ALS(
        rank=40,
        maxIter=12,
        regParam=0.02,
        userCol="user_idx",
        itemCol="game_idx",
        ratingCol="rating",
        implicitPrefs=True,
        alpha=alpha,
        coldStartStrategy="drop",
        nonnegative=True,
    )
    model = als.fit(train)

    predictions = model.transform(test)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction",
    )
    rmse = float(evaluator.evaluate(predictions))

    k = 10
    relevant = test.filter(col("rating") > 0.5)
    recs_exploded = (
        model.recommendForAllUsers(k)
        .selectExpr("user_idx", "explode(recommendations) as rec")
        .select("user_idx", col("rec.game_idx"))
    )
    hits = recs_exploded.join(relevant, ["user_idx", "game_idx"])
    precision_k = hits.count() / max(recs_exploded.count(), 1)
    log.info("RMSE test: %.4f | Precision@%s aprox: %.4f", rmse, k, precision_k)

    model_path = "hdfs:///user/gaming_recommender/models/als_model"
    model.write().overwrite().save(model_path)
    log.info("Modelo ALS guardado en %s", model_path)

    user_recs = model.recommendForAllUsers(20)

    return model, {
        "rmse": rmse,
        "precision_at_k": float(precision_k),
        "k": k,
        "model_path": model_path,
        "user_recs": user_recs,
        "game_idx": game_idx,
        "user_idx": user_idx,
    }
