#!/usr/bin/env python3
"""
KDD Mining — TF-IDF + similitud coseno entre juegos (reviews agregadas).
"""

import logging

import numpy as np
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Normalizer, StopWordsRemover, Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, concat_ws, desc, length, row_number
from pyspark.sql.window import Window

log = logging.getLogger(__name__)

MAX_GAMES_FOR_SIM = 6000
TOP_K_SIM = 20


def build_game_profiles(spark: SparkSession):
    log.info("Content-Based — perfiles de texto por juego")
    profiles = (
        spark.table("gaming_recommender.user_reviews")
        .groupBy("product_title")
        .agg(concat_ws(" ", collect_list("review_clean")).alias("combined_text"))
        .withColumnRenamed("product_title", "game_title")
        .filter(length(col("combined_text")) > 10)
    )
    w = Window.orderBy(desc(length(col("combined_text"))))
    profiles = (
        profiles.withColumn("rn", row_number().over(w))
        .filter(col("rn") <= MAX_GAMES_FOR_SIM)
        .drop("rn")
    )
    n = profiles.count()
    log.info("Juegos con perfil TF-IDF (cap=%s): %s", MAX_GAMES_FOR_SIM, f"{n:,}")
    return profiles


def compute_tfidf_similarity(spark: SparkSession, profiles):
    log.info("Content-Based — pipeline TF-IDF")

    tokenizer = Tokenizer(inputCol="combined_text", outputCol="tokens_raw")
    remover = StopWordsRemover(
        inputCol="tokens_raw",
        outputCol="tokens",
        stopWords=StopWordsRemover.loadDefaultStopWords("english"),
    )
    hashing_tf = HashingTF(inputCol="tokens", outputCol="raw_features", numFeatures=8192)
    idf = IDF(inputCol="raw_features", outputCol="features", minDocFreq=2)
    normalizer = Normalizer(inputCol="features", outputCol="norm_features", p=2.0)

    pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, normalizer])
    tfidf_model = pipeline.fit(profiles)
    tfidf_df = tfidf_model.transform(profiles)

    tfidf_model.write().overwrite().save("hdfs:///user/gaming_recommender/models/tfidf_pipeline")

    log.info("Calculando similitud coseno (pandas/sklearn)...")
    tfidf_pd = tfidf_df.select("game_title", "norm_features").toPandas()
    titles = tfidf_pd["game_title"].tolist()
    features = np.asarray([v.toArray() for v in tfidf_pd["norm_features"]], dtype=np.float32)

    from sklearn.metrics.pairwise import cosine_similarity

    sim_matrix = cosine_similarity(features)
    pairs = []
    for i, game_a in enumerate(titles):
        similar_indices = np.argsort(sim_matrix[i])[::-1][1 : TOP_K_SIM + 1]
        for j in similar_indices:
            pairs.append(
                {
                    "game_a": game_a,
                    "game_b": titles[int(j)],
                    "similarity": float(sim_matrix[i][int(j)]),
                }
            )

    pairs_pd = pd.DataFrame(pairs)
    similarity_df = spark.createDataFrame(pairs_pd)
    (
        similarity_df.write.mode("overwrite")
        .format("parquet")
        .saveAsTable("gaming_recommender.game_similarity")
    )
    log.info("Tabla Hive gaming_recommender.game_similarity (%s pares)", f"{len(pairs):,}")

    return similarity_df, tfidf_model
