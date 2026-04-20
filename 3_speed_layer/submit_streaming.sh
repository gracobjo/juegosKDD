#!/bin/bash
# submit_streaming.sh — Lanza el Spark Streaming job del Speed Layer.

set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$SPARK_HOME/bin/spark-submit" \
    --master "local[*]" \
    --driver-memory 2g \
    --executor-memory 2g \
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0" \
    --conf "spark.cassandra.connection.host=localhost" \
    --conf "spark.cassandra.connection.port=9042" \
    "$SCRIPT_DIR/spark_streaming_kdd.py"
