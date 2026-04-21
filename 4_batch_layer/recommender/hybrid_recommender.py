#!/usr/bin/env python3
"""
KDD Fase 4 (Mining) - Recomendador hibrido.

score_final = alpha * norm(cf_score) + (1 - alpha) * cb_score

Donde:
  cf_score: prediccion ALS (implicit).
  cb_score: promedio de similitud TF-IDF entre los top-N juegos jugados por el
            usuario ("seeds") y cada juego candidato.

Persiste gaming_recommender.hybrid_recommendations + popular_games_fallback.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import _hive_io as hio  # type: ignore

log = logging.getLogger(__name__)


def generate_hybrid_recommendations(
    spark: SparkSession,
    cf_results: dict[str, Any],
    alpha: float = 0.7,
    top_k: int = 10,
    seed_games_per_user: int = 5,
):
    """Genera recomendaciones hibridas por usuario y las guarda en Hive."""
    log.info("HYBRID alpha=%.2f (CF=%.0f%% CB=%.0f%%)", alpha, alpha * 100, (1 - alpha) * 100)

    # ── Candidatos CF (producidos por collaborative_filtering) ───────────────
    cf_candidates = hio.read_table(spark, "cf_candidates")
    # Normalizacion min-max de cf_score por usuario para poder sumarlo al cb_score
    w_user = Window.partitionBy("user_idx")
    cf_norm = (
        cf_candidates
        .withColumn("_min", F.min("cf_score").over(w_user))
        .withColumn("_max", F.max("cf_score").over(w_user))
        .withColumn(
            "cf_score_norm",
            F.when(
                (F.col("_max") - F.col("_min")) > 0,
                (F.col("cf_score") - F.col("_min")) / (F.col("_max") - F.col("_min")),
            ).otherwise(F.lit(0.5)),
        )
        .drop("_min", "_max")
    )

    # ── Seeds (top juegos del usuario) ───────────────────────────────────────
    behaviors = hio.read_table(spark, "user_behaviors").filter(
        F.col("hours_played") > 1.0
    )
    game_map = hio.read_table(spark, "game_index_map")
    user_map = hio.read_table(spark, "user_index_map")

    user_games = (
        behaviors
        .groupBy("user_id", "game_title")
        .agg(F.avg("hours_played").alias("avg_hours"))
    )
    w_seed = Window.partitionBy("user_id").orderBy(F.desc("avg_hours"))
    seeds = (
        user_games.withColumn("rank_in_user", F.row_number().over(w_seed))
        .filter(F.col("rank_in_user") <= seed_games_per_user)
        .join(user_map, "user_id")
        .select("user_idx", F.col("game_title").alias("seed_game"))
    )

    # ── Similitud (seed, candidate) via game_similarity ──────────────────────
    similarity = hio.read_table(spark, "game_similarity")
    sim_by_user = (
        seeds
        .join(similarity.withColumnRenamed("game_a", "seed_game"), "seed_game")
        .groupBy("user_idx", "game_b")
        .agg(F.avg("similarity").alias("cb_score"))
    )

    cb_with_idx = (
        sim_by_user
        .join(game_map.withColumnRenamed("game_title", "game_b"), "game_b")
        .select("user_idx", "game_idx", "cb_score")
    )

    # ── Combinar CF + CB ─────────────────────────────────────────────────────
    hybrid = (
        cf_norm.join(cb_with_idx, ["user_idx", "game_idx"], "left")
        .fillna({"cb_score": 0.0})
        .withColumn(
            "hybrid_score",
            F.round(
                F.lit(alpha) * F.col("cf_score_norm")
                + F.lit(1 - alpha) * F.col("cb_score"),
                4,
            ),
        )
    )

    # Rank + top-K por usuario
    w_rank = Window.partitionBy("user_idx").orderBy(F.desc("hybrid_score"))
    top_k_recs = (
        hybrid.withColumn("rank", F.row_number().over(w_rank))
        .filter(F.col("rank") <= top_k)
    )

    final_recs = (
        top_k_recs
        .join(user_map, "user_idx")
        .join(game_map, "game_idx")
        .select(
            F.col("user_id"),
            F.col("game_title").alias("recommended_game"),
            F.col("hybrid_score").cast("double"),
            F.col("cf_score_norm").cast("double").alias("cf_component"),
            F.col("cb_score").cast("double").alias("cb_component"),
            F.col("rank").cast("int").alias("recommendation_rank"),
            F.lit(alpha).cast("double").alias("alpha_used"),
            F.lit("batch").alias("source"),
            F.current_timestamp().alias("generated_at"),
        )
    )

    hio.write_table(spark, final_recs, "hybrid_recommendations")
    log.info("HYBRID: persistido en Hive hybrid_recommendations")

    # ── Cold-start: popular games ────────────────────────────────────────────
    popular = (
        hio.read_table(spark, "user_behaviors")
        .filter(F.col("hours_played") > 0)
        .groupBy("game_title")
        .agg(
            F.sum("hours_played").alias("total_hours"),
            F.countDistinct("user_id").alias("n_players"),
        )
        .orderBy(F.desc("total_hours"))
        .limit(top_k * 5)
    )
    popular_out = popular.select(
        F.col("game_title").alias("recommended_game"),
        F.lit(0.5).cast("double").alias("hybrid_score"),
        F.col("total_hours").cast("double"),
        F.col("n_players").cast("bigint"),
        F.current_timestamp().alias("generated_at"),
    )
    hio.write_table(spark, popular_out, "popular_games_fallback")
    log.info("HYBRID: popular_games_fallback persistido (%d juegos)", popular_out.count())

    return final_recs


def determine_alpha_for_history(history_count: int) -> float:
    if history_count <= 0:
        return 0.1
    if history_count < 5:
        return 0.3
    if history_count < 20:
        return 0.5
    if history_count < 50:
        return 0.7
    return 0.85
