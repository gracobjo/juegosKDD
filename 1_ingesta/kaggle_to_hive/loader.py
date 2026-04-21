#!/usr/bin/env python3
"""
Carga CSVs de Kaggle (0_data/raw) a Hive bajo gaming_recommender.*
Ejecutar tras download_datasets.py / validate_datasets.py

Sin cluster YARN activo, usa modo local (evita conexión rehusada a :8032):

  spark-submit --master local[*] 1_ingesta/kaggle_to_hive/loader.py

O: bash 0_infra/run_recommender_pipeline.sh --hive-only
"""

import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit, lower, trim, when
from pyspark.sql.functions import log as spark_log
from pyspark.sql.functions import regexp_replace as re_replace
from pyspark.sql.types import FloatType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = ROOT / "0_data" / "raw"


def _local_file_uri(path: Path) -> str:
    """Si fs.defaultFS es HDFS, rutas absolutas locales se resuelven mal; forzar file://."""
    return path.resolve().as_uri()


# Metastore Derby embebido por defecto en ~/.hive_metastore — choca si Hive/Spark
# ya lo tienen abierto. Usamos rutas bajo el repo salvo HIVE_METASTORE_URIS.
_LOCAL_METASTORE_DB = ROOT / ".spark_loader_metastore" / "metastore_db"
_LOCAL_WAREHOUSE = ROOT / ".spark_loader_warehouse"


def _try_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(ROOT / ".env")
    except ImportError:
        pass


def _spark_builder_configs() -> list[tuple[str, str]]:
    _try_load_dotenv()
    uris = os.environ.get("HIVE_METASTORE_URIS", "").strip()
    if uris:
        wh = os.environ.get("SPARK_WAREHOUSE_DIR", "").strip() or str(
            (ROOT / "spark-warehouse").resolve()
        )
        return [
            ("spark.hadoop.hive.metastore.uris", uris),
            ("spark.sql.warehouse.dir", wh),
            ("spark.hadoop.hive.metastore.warehouse.dir", wh),
        ]
    _LOCAL_METASTORE_DB.parent.mkdir(parents=True, exist_ok=True)
    _LOCAL_WAREHOUSE.mkdir(parents=True, exist_ok=True)
    derby = _LOCAL_METASTORE_DB.resolve()
    wh = str(_LOCAL_WAREHOUSE.resolve())
    derby_url = f"jdbc:derby:;databaseName={derby};create=true"
    return [
        ("spark.hadoop.javax.jdo.option.ConnectionURL", derby_url),
        (
            "spark.hadoop.javax.jdo.option.ConnectionDriverName",
            "org.apache.derby.jdbc.EmbeddedDriver",
        ),
        ("spark.hadoop.datanucleus.autoCreateSchema", "true"),
        ("spark.hadoop.datanucleus.fixedDatastore", "false"),
        ("spark.sql.warehouse.dir", wh),
        ("spark.hadoop.hive.metastore.warehouse.dir", wh),
    ]

BEHAVIOR_SCHEMA = StructType(
    [
        StructField("user_id", StringType()),
        StructField("game_title", StringType()),
        StructField("behavior_name", StringType()),
        StructField("value", FloatType()),
    ]
)

REVIEW_SCHEMA = StructType(
    [
        StructField("username", StringType()),
        StructField("product_id", StringType()),
        StructField("product_title", StringType()),
        StructField("review_text", StringType()),
        StructField("recommended", StringType()),
        StructField("hours", FloatType()),
    ]
)


def main():
    log.info("KDD Loader — Kaggle CSV → Hive (gaming_recommender)")

    b = (
        SparkSession.builder.appName("KDD_Gaming_KaggleLoader")
        .enableHiveSupport()
        .config("spark.sql.adaptive.enabled", "true")
    )
    for k, v in _spark_builder_configs():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS gaming_recommender")
    spark.sql("USE gaming_recommender")

    # ── Behaviors ───────────────────────────────────────────────────────────
    path_b = RAW_DIR / "steam_games_behaviors.csv"
    if not path_b.exists():
        log.error("No existe %s — ejecuta 0_data/kaggle/download_datasets.py", path_b)
        sys.exit(1)

    raw_b = spark.read.csv(
        _local_file_uri(path_b),
        schema=BEHAVIOR_SCHEMA,
        header=False,
        encoding="UTF-8",
    )
    cleaned_b = (
        raw_b.filter(col("user_id").isNotNull())
        .filter(col("game_title").isNotNull())
        .filter(col("behavior_name").isin("purchase", "play"))
        .filter(col("value") >= 0)
        .withColumn("game_title", trim(lower(col("game_title"))))
        .withColumn(
            "hours_played",
            when(col("behavior_name") == "play", col("value")).otherwise(lit(0.0)),
        )
        .withColumn(
            "purchased",
            when(col("behavior_name") == "purchase", lit(True)).otherwise(lit(False)),
        )
        .withColumn("load_date", current_date())
    )
    transformed_b = cleaned_b.withColumn(
        "implicit_rating",
        spark_log(lit(1.0) + col("hours_played")),
    )
    log.info("Behaviors: %s filas", f"{transformed_b.count():,}")
    (
        transformed_b.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .saveAsTable("gaming_recommender.user_behaviors")
    )
    log.info("Tabla Hive: gaming_recommender.user_behaviors")

    # ── Reviews ─────────────────────────────────────────────────────────────
    path_r = RAW_DIR / "steam_reviews.csv"
    if not path_r.exists():
        log.error("No existe %s", path_r)
        sys.exit(1)

    raw_r = spark.read.csv(
        _local_file_uri(path_r),
        schema=REVIEW_SCHEMA,
        header=True,
        encoding="UTF-8",
        multiLine=True,
        escape='"',
    )
    cleaned_r = (
        raw_r.filter(col("product_title").isNotNull())
        .filter(col("review_text").isNotNull())
        .filter(col("recommended").isin("True", "False"))
        .withColumn("product_title", trim(lower(col("product_title"))))
        .withColumn("review_text", trim(col("review_text")))
        .withColumn(
            "is_recommended",
            when(col("recommended") == "True", lit(True)).otherwise(lit(False)),
        )
        .withColumn(
            "hours_played",
            when(col("hours").isNotNull(), col("hours")).otherwise(lit(0.0)),
        )
        .withColumn("load_date", current_date())
        .drop("recommended", "hours")
    )
    transformed_r = cleaned_r.withColumn(
        "review_clean",
        trim(
            re_replace(
                re_replace(col("review_text"), "[^a-zA-Z0-9 ]", " "),
                "\\s+",
                " ",
            )
        ),
    )
    log.info("Reviews: %s filas", f"{transformed_r.count():,}")
    (
        transformed_r.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .saveAsTable("gaming_recommender.user_reviews")
    )
    log.info("Tabla Hive: gaming_recommender.user_reviews")

    # ── Catálogo ────────────────────────────────────────────────────────────
    games_behavior = spark.table("gaming_recommender.user_behaviors").select(
        col("game_title").alias("title")
    ).distinct()
    games_reviews = spark.table("gaming_recommender.user_reviews").select(
        col("product_title").alias("title")
    ).distinct()
    catalog = (
        games_behavior.union(games_reviews)
        .distinct()
        .withColumn("load_date", current_date())
    )
    (
        catalog.write.mode("overwrite")
        .format("parquet")
        .saveAsTable("gaming_recommender.game_catalog")
    )
    log.info("Catálogo: %s juegos", f"{catalog.count():,}")

    for table in ["user_behaviors", "user_reviews", "game_catalog"]:
        cnt = spark.sql(f"SELECT COUNT(*) AS c FROM gaming_recommender.{table}").collect()[0][
            0
        ]
        log.info("gaming_recommender.%s: %s", table, f"{cnt:,}")

    spark.stop()


if __name__ == "__main__":
    main()
