#!/usr/bin/env python3
"""
Speed Layer — interacciones usuario-juego desde Kafka (gaming.user.interactions).
Actualiza Cassandra gaming_recommender: player_windows, user_recommendations (ALS),
kdd_insights (anomalías simples).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    current_timestamp,
    desc,
    expr,
    from_json,
    lit,
    rank,
    when,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
)

INTERACTION_SCHEMA = StructType(
    [
        StructField("user_id", StringType()),
        StructField("game_title", StringType()),
        StructField("event_type", StringType()),
        StructField("hours", FloatType()),
        StructField("recommended", BooleanType()),
        StructField("ts", LongType()),
    ]
)

CASS_HOST = os.getenv("SPARK_CASSANDRA_HOST", os.getenv("CASSANDRA_HOST", "localhost"))
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
ALS_MODEL_PATH = os.getenv("ALS_MODEL_PATH", "hdfs:///user/gaming_recommender/models/als_model")

spark = (
    SparkSession.builder.appName("KDD_Gaming_SpeedLayer_Recommender")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1",
    )
    .config("spark.cassandra.connection.host", CASS_HOST)
    .config("spark.cassandra.connection.port", os.getenv("CASSANDRA_PORT", "9042"))
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    from pyspark.ml.recommendation import ALSModel

    als_model = ALSModel.load(ALS_MODEL_PATH)
    print("Modelo ALS cargado:", ALS_MODEL_PATH)
except Exception as e:
    print("ALS no disponible (ejecuta antes el batch kdd_pipeline):", e)
    als_model = None

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", "gaming.user.interactions")
    .option("startingOffsets", "latest")
    .load()
)

events = (
    raw_stream.select(
        from_json(col("value").cast("string"), INTERACTION_SCHEMA).alias("e"),
    )
    .select("e.*")
    .filter(col("user_id").isNotNull())
    .filter(col("game_title").isNotNull())
    .filter(col("event_type").isin("play", "purchase", "review"))
    .withColumn(
        "engagement_signal",
        when(col("event_type") == "play", when(col("hours") > 10, lit(1.0)).when(col("hours") > 1, lit(0.7)).otherwise(lit(0.3)))
        .when(col("event_type") == "purchase", lit(0.5))
        .when(col("event_type") == "review", when(col("recommended"), lit(0.9)).otherwise(lit(0.2)))
        .otherwise(lit(0.1)),
    )
)


def process_batch(batch_df, _batch_id):
    if batch_df.count() == 0:
        return

    user_activity = batch_df.groupBy("user_id").agg(
        count("*").alias("events_in_batch"),
        avg("engagement_signal").alias("avg_engagement"),
        avg("hours").alias("avg_hours"),
    )
    win = user_activity.withColumn("window_start", current_timestamp())
    (
        win.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="player_windows", keyspace="gaming_recommender")
        .save()
    )

    if als_model is not None:
        try:
            user_idx_map = spark.table("gaming_recommender.user_index_map")
            game_idx_map = spark.table("gaming_recommender.game_index_map")
            active_users = batch_df.select("user_id").distinct()
            known = active_users.join(user_idx_map, "user_id").select("user_idx").distinct()
            if known.count() > 0:
                recs = als_model.recommendForUserSubset(known, 10)
                base_recs = (
                    recs.selectExpr("user_idx", "explode(recommendations) as rec")
                    .select(col("user_idx"), col("rec.game_idx"), col("rec.rating").alias("cf_score"))
                    .join(user_idx_map, "user_idx")
                    .join(game_idx_map, "game_idx")
                )
                rw = Window.partitionBy("user_id").orderBy(desc("cf_score"))
                expanded = (
                    base_recs.withColumn("recommendation_rank", rank().over(rw))
                    .select(
                        col("user_id"),
                        col("game_title").alias("recommended_game"),
                        col("cf_score").alias("hybrid_score"),
                        lit(1.0).alias("cf_component"),
                        lit(0.0).alias("cb_component"),
                        col("recommendation_rank"),
                        lit(1.0).alias("alpha_used"),
                        lit("speed_layer").alias("source"),
                        current_timestamp().alias("generated_at"),
                    )
                )
                (
                    expanded.write.format("org.apache.spark.sql.cassandra")
                    .mode("append")
                    .options(table="user_recommendations", keyspace="gaming_recommender")
                    .save()
                )
        except Exception as ex:
            print("Error actualizando recomendaciones speed:", ex)

    anomalies = batch_df.filter((col("hours") > 100) | (col("engagement_signal") < 0.1))
    if anomalies.count() > 0:
        ins = (
            anomalies.withColumn("insight_id", expr("uuid()"))
            .withColumn("created_at", current_timestamp())
            .withColumn("insight_type", lit("anomaly_detected"))
            .withColumn("severity", lit("warning"))
            .withColumn(
                "message",
                concat_ws(
                    " | ",
                    col("user_id"),
                    col("game_title"),
                    col("event_type").cast("string"),
                ),
            )
            .withColumn("layer", lit("speed"))
            .select("insight_id", "created_at", "insight_type", "severity", "message", "layer")
        )
        (
            ins.write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table="kdd_insights", keyspace="gaming_recommender")
            .save()
        )


query = (
    events.writeStream.foreachBatch(process_batch)
    .trigger(processingTime="30 seconds")
    .outputMode("update")
    .option("checkpointLocation", "/tmp/kdd_checkpoint/recommender_interactions")
    .start()
)

print("Speed Layer recomendador activo (gaming.user.interactions → Cassandra)")
query.awaitTermination()
