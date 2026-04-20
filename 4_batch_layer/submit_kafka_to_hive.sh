#!/usr/bin/env bash
# ============================================================================
# submit_kafka_to_hive.sh — Drena Kafka a Hive para el Batch Layer
# ============================================================================
# Uso:
#   bash submit_kafka_to_hive.sh              # drena todo lo disponible
#   bash submit_kafka_to_hive.sh 2026-04-20   # filtra por fecha
# ============================================================================
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$HOME/juegosKDD}"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
SCRIPT="$PROJECT_DIR/4_batch_layer/kafka_to_hive.py"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"

DATE_ARG=""
if [[ $# -ge 1 && -n "$1" ]]; then
  DATE_ARG="--date $1"
fi

echo "▸ Ejecutando Kafka → Hive bridge  (fecha: ${1:-all})"

"$SPARK_HOME/bin/spark-submit" \
  --master "local[2]" \
  --name "KDD_Kafka_To_Hive" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.sql.sources.partitionOverwriteMode=dynamic" \
  "$SCRIPT" \
  --bootstrap "$KAFKA_BOOTSTRAP" \
  $DATE_ARG
