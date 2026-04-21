#!/usr/bin/env python3
"""
KDD Pipeline completo del recomendador hibrido.

Orquesta las 5 fases KDD sobre las tablas Hive cargadas por el loader:
  1) Selection      : leer Hive
  2) Preprocessing  : validar nulos / duplicados
  3) Transformation : perfiles textuales, rating implicito (ya hecho por el loader)
  4) Mining         : ALS (CF) + TF-IDF (CB) + Hybrid
  5) Evaluation     : RMSE + Precision@K + escritura a Cassandra

Uso directo:
  spark-submit --packages ... 4_batch_layer/recommender/kdd_pipeline.py --alpha 0.7
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Permitimos ejecutar "python kdd_pipeline.py" desde esta carpeta.
THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import _hive_io as hio  # type: ignore
from collaborative_filtering import train_collaborative_filter
from content_based import build_game_profiles, compute_tfidf_similarity
from hybrid_recommender import generate_hybrid_recommendations
from model_evaluator import evaluate_and_store_metrics

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("kdd_pipeline")


def build_spark(app_suffix: str) -> SparkSession:
    """SparkSession con conector de Cassandra configurado (best effort)."""
    builder = (
        SparkSession.builder
        .appName(f"KDD_GameRecommender_{app_suffix}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "32")
        .config(
            "spark.cassandra.connection.host",
            os.getenv("CASSANDRA_HOST", "localhost"),
        )
        .config(
            "spark.cassandra.connection.port",
            os.getenv("CASSANDRA_PORT", "9042"),
        )
    )
    return builder.getOrCreate()


def write_recs_to_cassandra(spark: SparkSession, hybrid_recs) -> None:
    """Escribe las recomendaciones hibridas + popular fallback en Cassandra."""
    try:
        cols = [
            F.col("user_id").cast("string"),
            F.col("recommended_game").cast("string"),
            F.col("hybrid_score").cast("double"),
            F.col("cf_component").cast("double"),
            F.col("cb_component").cast("double"),
            F.col("recommendation_rank").cast("int"),
            F.col("alpha_used").cast("double"),
            F.col("source").cast("string"),
            F.col("generated_at").cast("timestamp"),
        ]
        (
            hybrid_recs.select(*cols)
            .write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table="user_recommendations", keyspace="gaming_recommender")
            .save()
        )
        log.info("OK: user_recommendations escritas en Cassandra")
    except Exception as exc:
        log.warning("No se pudieron escribir recs en Cassandra: %s", exc)

    try:
        sim = hio.read_table(spark, "game_similarity")
        (
            sim.select(
                F.col("game_a").cast("string"),
                F.col("game_b").cast("string"),
                F.col("similarity").cast("double"),
            )
            .write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table="game_similarity", keyspace="gaming_recommender")
            .save()
        )
        log.info("OK: game_similarity escrita en Cassandra")
    except Exception as exc:
        log.warning("No se pudo escribir game_similarity en Cassandra: %s", exc)

    try:
        pop = hio.read_table(spark, "popular_games_fallback")
        (
            pop.select(
                F.col("recommended_game").cast("string"),
                F.col("hybrid_score").cast("double"),
                F.col("total_hours").cast("double"),
                F.col("n_players").cast("bigint"),
                F.col("generated_at").cast("timestamp"),
            )
            .write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table="popular_games_fallback", keyspace="gaming_recommender")
            .save()
        )
        log.info("OK: popular_games_fallback escrita en Cassandra")
    except Exception as exc:
        log.warning("No se pudo escribir popular_games_fallback en Cassandra: %s", exc)


def write_user_history_cache(spark: SparkSession) -> None:
    """Cachea en Cassandra el historial por usuario (top 20 juegos)."""
    try:
        behaviors = hio.read_table(spark, "user_behaviors")
        from pyspark.sql.window import Window

        w = Window.partitionBy("user_id").orderBy(F.desc("hours_played"))
        top_per_user = (
            behaviors.filter(F.col("hours_played") > 0)
            .withColumn("rnk", F.row_number().over(w))
            .filter(F.col("rnk") <= 20)
            .select(
                F.col("user_id").cast("string"),
                F.col("game_title").cast("string"),
                F.col("hours_played").cast("double"),
                F.current_timestamp().alias("last_played"),
            )
        )
        (
            top_per_user.write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table="user_history_cache", keyspace="gaming_recommender")
            .save()
        )
        log.info("OK: user_history_cache escrita en Cassandra")
    except Exception as exc:
        log.warning("No se pudo escribir user_history_cache: %s", exc)


def run_pipeline(date_str: str, alpha: float, top_k: int, do_cassandra: bool) -> int:
    spark = build_spark(date_str)
    spark.sparkContext.setLogLevel("WARN")

    log.info("=" * 70)
    log.info(" KDD Pipeline - Gaming Recommender - %s", date_str)
    log.info(" alpha=%.2f top_k=%d cassandra=%s", alpha, top_k, do_cassandra)
    log.info("=" * 70)

    # ── KDD 1 Selection ─────────────────────────────────────────────────────
    log.info("[KDD 1/5] SELECTION")
    behaviors = hio.read_table(spark, "user_behaviors")
    reviews = hio.read_table(spark, "user_reviews")
    n_users = behaviors.select("user_id").distinct().count()
    n_games = behaviors.select("game_title").distinct().count()
    n_reviews = reviews.count()
    log.info("  users=%s games=%s reviews=%s", f"{n_users:,}", f"{n_games:,}", f"{n_reviews:,}")

    # ── KDD 2 Preprocessing (chequeo sanity) ────────────────────────────────
    log.info("[KDD 2/5] PREPROCESSING - sanity")
    null_u = behaviors.filter(F.col("user_id").isNull()).count()
    null_g = behaviors.filter(F.col("game_title").isNull()).count()
    log.info("  nulos user=%d nulos game=%d", null_u, null_g)

    # ── KDD 3 Transformation - perfiles ─────────────────────────────────────
    log.info("[KDD 3/5] TRANSFORMATION")
    profiles = build_game_profiles(spark)

    # ── KDD 4 Mining ────────────────────────────────────────────────────────
    log.info("[KDD 4/5] MINING - Collaborative Filtering")
    cf_result = train_collaborative_filter(spark)

    log.info("[KDD 4/5] MINING - Content-Based")
    compute_tfidf_similarity(spark, profiles)

    log.info("[KDD 4/5] MINING - Hybrid")
    hybrid = generate_hybrid_recommendations(
        spark, cf_result.to_dict(), alpha=alpha, top_k=top_k
    )

    # ── KDD 5 Evaluation ────────────────────────────────────────────────────
    log.info("[KDD 5/5] EVALUATION")
    metrics = evaluate_and_store_metrics(
        spark, cf_result.to_dict(), hybrid, alpha=alpha, date_str=date_str,
        write_cassandra=do_cassandra,
    )

    # ── Serving Layer ───────────────────────────────────────────────────────
    if do_cassandra:
        write_recs_to_cassandra(spark, hybrid)
        write_user_history_cache(spark)

    log.info("\n" + "=" * 70)
    log.info(" KDD Pipeline terminado")
    log.info(" RMSE          : %.4f", metrics["rmse"])
    log.info(" Precision@%d  : %.4f", metrics["k"], metrics["precision_at_k"])
    log.info(" Recall@%d     : %.4f", metrics["k"], metrics["recall_at_k"])
    log.info(" Users with recs: %s", f"{metrics['users_with_recs']:,}")
    log.info(" Games covered  : %s", f"{metrics['games_covered']:,}")
    log.info("=" * 70)

    spark.stop()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=datetime.utcnow().strftime("%Y-%m-%d"))
    parser.add_argument("--alpha", type=float, default=0.7)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument(
        "--no-cassandra", action="store_true",
        help="No intentar escribir a Cassandra (modo solo Hive).",
    )
    args = parser.parse_args()
    return run_pipeline(args.date, args.alpha, args.top_k, not args.no_cassandra)


if __name__ == "__main__":
    sys.exit(main())
