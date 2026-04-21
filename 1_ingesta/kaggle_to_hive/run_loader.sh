#!/bin/bash
# run_loader.sh - lanza el loader con spark-submit usando el entorno del repo.
set -euo pipefail

cd "$(dirname "$0")/../.."

export PYSPARK_PYTHON="${PYSPARK_PYTHON:-/home/hadoop/smart_energy/venv/bin/python3}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PYSPARK_PYTHON}"

SPARK_BIN="${SPARK_HOME:-/opt/spark}/bin/spark-submit"

exec "$SPARK_BIN" \
    --master "local[*]" \
    --driver-memory 4g \
    --executor-memory 4g \
    --conf spark.sql.adaptive.enabled=true \
    1_ingesta/kaggle_to_hive/loader.py "$@"
