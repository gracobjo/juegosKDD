#!/bin/bash
# submit_streaming_recommender.sh
# Lanza el Speed Layer del recomendador (Spark Structured Streaming).
# Consume gaming.user.interactions y escribe en Cassandra
# (gaming_recommender.player_windows | user_recommendations | kdd_insights).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}"

# Venv con numpy + setuptools (necesario para distutils en Python 3.12).
# Es el mismo venv que usa el resto de la infra KDD de este equipo.
VENV_PY="${VENV_PY:-/home/hadoop/smart_energy/venv/bin/python}"
if [ -x "$VENV_PY" ]; then
    export PYSPARK_PYTHON="$VENV_PY"
    export PYSPARK_DRIVER_PYTHON="$VENV_PY"
fi

# Carga .env si existe (KAFKA_BOOTSTRAP, CASSANDRA_HOST, ...).
[ -f "$PROJECT_DIR/.env" ] && set -a && . "$PROJECT_DIR/.env" && set +a

HIO="$PROJECT_DIR/4_batch_layer/recommender/_hive_io.py"
if [ ! -f "$HIO" ]; then
    echo "ERROR: no se encuentra $HIO" >&2
    exit 1
fi

echo "▸ Spark streaming recomendador"
echo "  KAFKA_BOOTSTRAP : ${KAFKA_BOOTSTRAP:-localhost:9092}"
echo "  CASSANDRA_HOST  : ${CASSANDRA_HOST:-localhost}"
echo "  TOPIC           : ${TOPIC_USER_INTERACTIONS:-gaming.user.interactions}"
echo "  PYSPARK_PYTHON  : ${PYSPARK_PYTHON:-(default)}"

exec "$SPARK_HOME/bin/spark-submit" \
    --master "local[*]" \
    --driver-memory 2g \
    --executor-memory 2g \
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1" \
    --conf "spark.cassandra.connection.host=${CASSANDRA_HOST:-localhost}" \
    --conf "spark.cassandra.connection.port=${CASSANDRA_PORT:-9042}" \
    --conf "spark.sql.shuffle.partitions=4" \
    --py-files "$HIO" \
    "$SCRIPT_DIR/streaming_recommender.py"
