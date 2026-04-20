#!/bin/bash
# submit_batch.sh — Lanza manualmente el Spark Batch del Batch Layer.
# Uso: bash submit_batch.sh [YYYY-MM-DD]

set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROCESS_DATE="${1:-$(date -u +%Y-%m-%d)}"

echo "=== Batch KDD para fecha: $PROCESS_DATE ==="

"$SPARK_HOME/bin/spark-submit" \
    --master "local[*]" \
    --driver-memory 2g \
    --executor-memory 2g \
    --packages "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0" \
    --conf "spark.cassandra.connection.host=localhost" \
    "$SCRIPT_DIR/spark_batch_kdd.py" \
    --date "$PROCESS_DATE"
