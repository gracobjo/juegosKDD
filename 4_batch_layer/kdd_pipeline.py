#!/usr/bin/env python3
"""
Pipeline KDD completo — ALS + TF-IDF + híbrido → Hive + Cassandra (gaming_recommender).
"""

import argparse
import logging
import os
import sys

ROOT_BATCH = os.path.dirname(os.path.abspath(__file__))
if ROOT_BATCH not in sys.path:
    sys.path.insert(0, ROOT_BATCH)

from collaborative_filtering import train_collaborative_filter
from content_based import build_game_profiles, compute_tfidf_similarity
from hybrid_recommender import generate_hybrid_recommendations
from model_evaluator import evaluate_and_store_metrics, write_user_history_cache

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("--date", default=None)
parser.add_argument("--alpha", default=0.7, type=float)
args = parser.parse_args()


def _truncate_serving(host: str):
    try:
        from cassandra.cluster import Cluster

        sess = Cluster([host]).connect()
        for tbl in (
            "user_recommendations",
            "game_similarity",
            "popular_games_fallback",
            "user_history_cache",
        ):
            sess.execute(f"TRUNCATE gaming_recommender.{tbl}")
        log.info("Tablas Cassandra truncadas (recomendador)")
    except Exception as e:
        log.warning("No se pudo truncar Cassandra (¿cassandra-driver / servicio?): %s", e)


def main():
    from datetime import datetime

    run_date = args.date or datetime.utcnow().strftime("%Y-%m-%d")
    cass_host = os.getenv("SPARK_CASSANDRA_HOST", os.getenv("CASSANDRA_HOST", "localhost"))

    spark = (
        SparkSession.builder.appName(f"KDD_GameRecommender_{run_date}")
        .config(
            "spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1",
        )
        .config("spark.cassandra.connection.host", cass_host)
        .config("spark.cassandra.connection.port", os.getenv("CASSANDRA_PORT", "9042"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE gaming_recommender")

    log.info("KDD Pipeline — Gaming Recommender — %s | alpha=%s", run_date, args.alpha)

    log.info("[KDD 1/5] SELECTION")
    behaviors = spark.table("gaming_recommender.user_behaviors")
    reviews = spark.table("gaming_recommender.user_reviews")
    log.info(
        "Usuarios=%s juegos(beh)=%s reviews=%s",
        f"{behaviors.select('user_id').distinct().count():,}",
        f"{behaviors.select('game_title').distinct().count():,}",
        f"{reviews.count():,}",
    )

    log.info("[KDD 2/5] PREPROCESSING (validación)")
    from pyspark.sql.functions import col as c

    bad = (
        behaviors.filter(c("user_id").isNull() | c("game_title").isNull()).count()
        + behaviors.filter(c("hours_played") < 0).count()
    )
    if bad:
        log.warning("Registros sospechosos: %s", bad)

    log.info("[KDD 3/5] TRANSFORMATION (perfiles texto)")
    game_profiles = build_game_profiles(spark)

    log.info("[KDD 4/5] MINING")
    _, cf_results = train_collaborative_filter(spark)
    similarity_df, _tfidf = compute_tfidf_similarity(spark, game_profiles)
    hybrid_df, popular_df = generate_hybrid_recommendations(
        spark, cf_results, alpha=args.alpha
    )

    log.info("[KDD 5/5] EVALUATION + SERVING")
    metrics = evaluate_and_store_metrics(spark, cf_results, hybrid_df, run_date, args.alpha)

    _truncate_serving(cass_host)

    recs_out = hybrid_df.select(
        "user_id",
        "recommended_game",
        "hybrid_score",
        "cf_component",
        "cb_component",
        "recommendation_rank",
        "alpha_used",
        "source",
        "generated_at",
    )
    (
        recs_out.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="user_recommendations", keyspace="gaming_recommender")
        .save()
    )
    (
        similarity_df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="game_similarity", keyspace="gaming_recommender")
        .save()
    )
    pop_out = popular_df.select("recommended_game", "hybrid_score")
    (
        pop_out.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="popular_games_fallback", keyspace="gaming_recommender")
        .save()
    )

    write_user_history_cache(spark)

    log.info(
        "Completado — RMSE=%.4f P@%s=%.4f usuarios=%s",
        metrics["rmse"],
        metrics["k"],
        metrics["precision_at_k"],
        f"{metrics['users_with_recs']:,}",
    )
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
