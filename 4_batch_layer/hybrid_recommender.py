#!/usr/bin/env python3
"""
Recomendador híbrido: α·CF_normalizado + (1-α)·CB (similitud con seeds).
"""

import logging

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    desc,
    lit,
    max as spark_max,
    min as spark_min,
    rank,
    round as spark_round,
    when,
)
from pyspark.sql.window import Window

log = logging.getLogger(__name__)


def generate_hybrid_recommendations(
    spark: SparkSession,
    cf_results: dict,
    alpha: float = 0.7,
    top_k: int = 10,
):
    log.info("Híbrido — alpha=%.2f, top_k=%s", alpha, top_k)

    als_model = ALSModel.load(cf_results["model_path"])

    cf_recs = als_model.recommendForAllUsers(50)
    cf_expanded = (
        cf_recs.selectExpr("user_idx", "explode(recommendations) as rec")
        .select(
            col("user_idx"),
            col("rec.game_idx").alias("game_idx"),
            col("rec.rating").alias("cf_score"),
        )
    )

    uw = Window.partitionBy("user_idx")
    cf_norm = (
        cf_expanded.withColumn("cf_min", spark_min("cf_score").over(uw))
        .withColumn("cf_max", spark_max("cf_score").over(uw))
        .withColumn(
            "cf_score_norm",
            when(
                col("cf_max") > col("cf_min"),
                (col("cf_score") - col("cf_min")) / (col("cf_max") - col("cf_min")),
            ).otherwise(lit(1.0)),
        )
    )

    behaviors = spark.table("gaming_recommender.user_behaviors").filter(col("hours_played") > 5)
    user_top_games = (
        behaviors.join(spark.table("gaming_recommender.game_index_map"), "game_title")
        .groupBy("user_id", "game_title", "game_idx")
        .agg(avg("hours_played").alias("avg_hours"))
        .withColumn(
            "rank_in_user",
            rank().over(Window.partitionBy("user_id").orderBy(desc("avg_hours"))),
        )
        .filter(col("rank_in_user") <= 5)
    )

    similarity = spark.table("gaming_recommender.game_similarity")
    user_idx_map = spark.table("gaming_recommender.user_index_map")
    game_idx_map = spark.table("gaming_recommender.game_index_map")

    user_seeds = user_top_games.join(user_idx_map, "user_id").select(
        col("user_idx"), col("game_title").alias("seed_game")
    )

    seed_similarity = (
        user_seeds.join(similarity.withColumnRenamed("game_a", "seed_game"), "seed_game")
        .groupBy("user_idx", "game_b")
        .agg(avg("similarity").alias("cb_score"))
    )

    cb_with_idx = seed_similarity.join(
        game_idx_map.withColumnRenamed("game_title", "game_b"), "game_b"
    ).select("user_idx", "game_idx", col("cb_score"))

    hybrid = (
        cf_norm.join(cb_with_idx, ["user_idx", "game_idx"], "left")
        .fillna({"cb_score": 0.0})
        .withColumn(
            "hybrid_score",
            spark_round(
                lit(alpha) * col("cf_score_norm") + lit(1.0 - alpha) * col("cb_score"),
                4,
            ),
        )
    )

    rank_window = Window.partitionBy("user_idx").orderBy(desc("hybrid_score"))
    top_k_recs = (
        hybrid.withColumn("rank", rank().over(rank_window))
        .filter(col("rank") <= top_k)
        .join(user_idx_map, "user_idx")
        .join(game_idx_map, "game_idx")
        .select(
            col("user_id"),
            col("game_title").alias("recommended_game"),
            col("hybrid_score"),
            col("cf_score_norm").alias("cf_component"),
            col("cb_score").alias("cb_component"),
            col("rank").alias("recommendation_rank"),
            lit(alpha).alias("alpha_used"),
            lit("batch").alias("source"),
            current_timestamp().alias("generated_at"),
        )
    )

    popular_games = (
        spark.table("gaming_recommender.user_behaviors")
        .groupBy("game_title")
        .agg(avg("hours_played").alias("avg_hours"))
        .orderBy(desc("avg_hours"))
        .limit(top_k)
        .withColumn("hybrid_score", lit(0.5))
        .withColumnRenamed("game_title", "recommended_game")
    )

    (
        top_k_recs.write.mode("overwrite")
        .format("parquet")
        .saveAsTable("gaming_recommender.hybrid_recommendations")
    )
    (
        popular_games.write.mode("overwrite")
        .format("parquet")
        .saveAsTable("gaming_recommender.popular_games_fallback")
    )

    user_count = top_k_recs.select("user_id").distinct().count()
    log.info("Recomendaciones híbridas para %s usuarios", f"{user_count:,}")
    return top_k_recs, popular_games


def determine_alpha(user_id: str, spark: SparkSession) -> float:
    try:
        cnt = spark.sql(
            f"""
            SELECT COUNT(*) FROM gaming_recommender.user_behaviors
            WHERE user_id = '{user_id.replace("'", "''")}' AND hours_played > 0
            """
        ).collect()[0][0]
        if cnt == 0:
            return 0.1
        if cnt < 5:
            return 0.3
        if cnt < 20:
            return 0.5
        if cnt < 50:
            return 0.7
        return 0.85
    except Exception:
        return 0.5
