#!/bin/bash
# submit_kdd.sh - lanza el pipeline KDD del recomendador con spark-submit.
set -euo pipefail

cd "$(dirname "$0")/../.."

# Usa el Python del venv (tiene numpy, scipy, sklearn, pandas)
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-/home/hadoop/smart_energy/venv/bin/python3}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PYSPARK_PYTHON}"

SPARK_BIN="${SPARK_HOME:-/opt/spark}/bin/spark-submit"
PKG="com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"

ALPHA="${1:-0.7}"
DATE="${2:-$(date -u +%F)}"

exec "$SPARK_BIN" \
    --master "local[*]" \
    --driver-memory 4g \
    --executor-memory 4g \
    --packages "$PKG" \
    --conf spark.cassandra.connection.host="${CASSANDRA_HOST:-localhost}" \
    --py-files "4_batch_layer/recommender/_hive_io.py,4_batch_layer/recommender/collaborative_filtering.py,4_batch_layer/recommender/content_based.py,4_batch_layer/recommender/hybrid_recommender.py,4_batch_layer/recommender/model_evaluator.py" \
    4_batch_layer/recommender/kdd_pipeline.py \
    --alpha "$ALPHA" \
    --date "$DATE"
