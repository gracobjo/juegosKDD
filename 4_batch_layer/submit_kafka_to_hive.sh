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

# Caché Ivy local al repo: si `spark.sql.hive.metastore.jars=maven` usa
# ~/.ivy2 y el task corre como otro usuario (p. ej. root vía Airflow), los
# jars pueden quedar a medias o sin permisos → Spark cae al cliente 2.3 y
# reaparece "Invalid method name: 'get_table'" contra Metastore Hive 4.x.
IVY2_HOME="${IVY2_HOME:-$PROJECT_DIR/0_infra/.ivy2}"
export IVY2_HOME
mkdir -p "$IVY2_HOME"

DATE_ARG=""
if [[ $# -ge 1 && -n "$1" ]]; then
  DATE_ARG="--date $1"
fi

echo "▸ Ejecutando Kafka → Hive bridge  (fecha: ${1:-all})"

# Cliente Hive del driver: el Metastore que corre en esta máquina es Hive
# 4.2.0, pero Spark 3.5.1 viene con el cliente embebido 2.3. Cuando el driver
# llama al Thrift `get_table` del Metastore 4.x, este responde "Invalid method
# name: 'get_table'" (en Hive 4 se renombró a `get_table_req`). Spark 3.5.1
# solo admite valores en los rangos 0.12–2.3.9 y 3.0.0–3.1.3, así que forzamos
# 3.1.3 — el protocolo Thrift del cliente 3.x sigue siendo compatible con el
# Metastore 4.x en modo legado. Los jars se descargan desde Maven la primera
# vez (~100 MB) y quedan cacheados en ~/.ivy2.
HIVE_CLIENT_CONF=(
  --conf "spark.sql.hive.metastore.version=3.1.3"
  --conf "spark.sql.hive.metastore.jars=maven"
)

"$SPARK_HOME/bin/spark-submit" \
  --master "local[2]" \
  --name "KDD_Kafka_To_Hive" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.sql.sources.partitionOverwriteMode=dynamic" \
  "${HIVE_CLIENT_CONF[@]}" \
  "$SCRIPT" \
  --bootstrap "$KAFKA_BOOTSTRAP" \
  $DATE_ARG
