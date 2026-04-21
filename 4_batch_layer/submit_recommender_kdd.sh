#!/usr/bin/env bash
# Ejecuta el pipeline KDD del recomendador híbrido (Spark + Hive + Cassandra).
set -euo pipefail
ROOT="/home/hadoop/juegosKDD"
SPARK="${SPARK_HOME:-/opt/spark}/bin/spark-submit"
PKG="com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
HOST="${CASSANDRA_HOST:-localhost}"

exec "$SPARK" \
  --master local[*] \
  --driver-memory "${SPARK_DRIVER_MEMORY:-4g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-4g}" \
  --packages "$PKG" \
  --conf spark.cassandra.connection.host="$HOST" \
  --py-files "$ROOT/4_batch_layer/collaborative_filtering.py,$ROOT/4_batch_layer/content_based.py,$ROOT/4_batch_layer/hybrid_recommender.py,$ROOT/4_batch_layer/model_evaluator.py" \
  "$ROOT/4_batch_layer/kdd_pipeline.py" \
  "$@"
