#!/usr/bin/env python3
"""
KDD Fase 4 (Mining) - Filtrado basado en contenido con TF-IDF.

Construye perfiles textuales por juego (concatenando sus reviews limpias) y
calcula similitud coseno entre juegos con spark.ml + numpy/sklearn.
Persiste la tabla game_similarity en Hive y en /tmp un pickle con el pipeline
TF-IDF por si el speed-layer quiere consultarlo.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    HashingTF,
    IDF,
    Normalizer,
    StopWordsRemover,
    Tokenizer,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import _hive_io as hio  # type: ignore

log = logging.getLogger(__name__)

PIPELINE_PATH = "hdfs:///user/gaming_recommender/models/tfidf_pipeline"


def build_game_profiles(spark: SparkSession, max_reviews_per_game: int = 200) -> Any:
    """Concatena hasta N reviews limpias por juego."""
    log.info("CB: construyendo perfiles textuales (max %d reviews/juego)", max_reviews_per_game)
    # Ventana para quedarnos con las N reviews mas largas por juego.
    reviews = hio.read_table(spark, "user_reviews")
    reviews = reviews.withColumn("review_len", F.length(F.col("review_clean")))
    from pyspark.sql.window import Window

    w = Window.partitionBy("product_title").orderBy(F.desc("review_len"))
    top_reviews = reviews.withColumn("rnk", F.row_number().over(w)).filter(
        F.col("rnk") <= max_reviews_per_game
    )

    profiles = (
        top_reviews.groupBy("product_title")
        .agg(
            F.concat_ws(" ", F.collect_list("review_clean")).alias("combined_text"),
            F.count("*").alias("review_count"),
            F.avg(F.col("is_recommended").cast("int")).alias("recommend_rate"),
        )
        .withColumnRenamed("product_title", "game_title")
    )
    n = profiles.count()
    log.info("CB: %s juegos con perfil de texto", f"{n:,}")
    return profiles


def compute_tfidf_similarity(
    spark: SparkSession,
    profiles,
    num_features: int = 4096,
    k_similar: int = 20,
    max_games_for_dense: int = 6_000,
):
    """Entrena TF-IDF + normaliza vectores y genera top-K similares por juego."""
    log.info("CB: pipeline TF-IDF (num_features=%d)", num_features)
    tokenizer = Tokenizer(inputCol="combined_text", outputCol="tokens_raw")
    remover = StopWordsRemover(
        inputCol="tokens_raw", outputCol="tokens",
        stopWords=StopWordsRemover.loadDefaultStopWords("english"),
    )
    hashing_tf = HashingTF(
        inputCol="tokens", outputCol="raw_features", numFeatures=num_features
    )
    idf = IDF(inputCol="raw_features", outputCol="features", minDocFreq=2)
    normalizer = Normalizer(inputCol="features", outputCol="norm_features", p=2.0)

    pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, normalizer])
    model = pipeline.fit(profiles)
    vectors = model.transform(profiles).select("game_title", "norm_features")

    try:
        model.write().overwrite().save(PIPELINE_PATH)
        log.info("CB: pipeline TF-IDF guardado en %s", PIPELINE_PATH)
    except Exception as exc:
        log.warning("CB: no se pudo guardar pipeline en HDFS (%s).", exc)

    # Para N > max_games_for_dense la matriz densa seria enorme: tomamos
    # los juegos con mas reviews para limitar el coste.
    n_games = vectors.count()
    if n_games > max_games_for_dense:
        log.warning("CB: %d juegos detectados, limitando a los %d mas populares", n_games, max_games_for_dense)
        top = (
            profiles.orderBy(F.desc("review_count"))
            .select("game_title").limit(max_games_for_dense)
        )
        vectors = vectors.join(top, "game_title")

    rows = vectors.toPandas()
    titles = rows["game_title"].tolist()
    if not titles:
        log.warning("CB: sin perfiles textuales, skip similitud")
        return None

    feature_matrix = np.array([v.toArray() for v in rows["norm_features"]], dtype=np.float32)
    # Vectores ya normalizados L2 -> similitud = producto punto
    sim = feature_matrix @ feature_matrix.T
    np.fill_diagonal(sim, -1.0)  # excluir el propio juego

    k = min(k_similar, len(titles) - 1)
    # Para cada fila, indices de los top-k mayores (argpartition es O(n))
    top_idx = np.argpartition(-sim, kth=k - 1, axis=1)[:, :k]

    pairs = []
    for i, title_a in enumerate(titles):
        idxs = top_idx[i]
        # Orden final descendente
        order = np.argsort(-sim[i, idxs])
        for j in idxs[order]:
            pairs.append((title_a, titles[int(j)], float(sim[i, int(j)])))

    import pandas as pd
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    pairs_df = pd.DataFrame(pairs, columns=["game_a", "game_b", "similarity"])
    # Mantenemos similitudes >=0 para no perder todo con datos sinteticos donde
    # el TF-IDF es casi nulo. Si hay variedad preferimos >0 estricto.
    nonzero = pairs_df[pairs_df["similarity"] > 1e-6]
    if len(nonzero) > 0:
        pairs_df = nonzero
    else:
        log.warning("CB: similitudes TF-IDF despreciables (probablemente datos sinteticos); conservando todos los pares")
        pairs_df = pairs_df[pairs_df["similarity"] >= 0.0]

    schema = StructType([
        StructField("game_a", StringType(), nullable=False),
        StructField("game_b", StringType(), nullable=False),
        StructField("similarity", DoubleType(), nullable=False),
    ])
    if len(pairs_df) == 0:
        log.warning("CB: sin pares de similitud - escribiendo tabla vacia para no romper el pipeline")
        similarity_df = spark.createDataFrame([], schema=schema)
    else:
        similarity_df = spark.createDataFrame(pairs_df, schema=schema)
    hio.write_table(spark, similarity_df, "game_similarity")
    log.info("CB: %s pares de similitud persistidos", f"{len(pairs_df):,}")
    return similarity_df
