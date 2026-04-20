#!/bin/bash
# stop_stack.sh — Apaga limpiamente todos los servicios lanzados por start_stack.sh

set -u

export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
CASSANDRA_HOME="${CASSANDRA_HOME:-/home/hadoop/smart_energy/cassandra}"
AIRFLOW_VENV="${AIRFLOW_VENV:-/home/hadoop/smart_energy/venv}"
NIFI_HOME="${NIFI_HOME:-/home/hadoop/smart_energy/nifi-2.6.0}"

PID_DIR="/home/hadoop/juegosKDD/0_infra/pids"

C_RED="\033[1;31m"; C_GREEN="\033[1;32m"; C_RESET="\033[0m"
log() { echo -e "${C_RED}▸${C_RESET} $*"; }
ok()  { echo -e "  ${C_GREEN}✓${C_RESET} $*"; }

kill_pidfile() {
    local label=$1 pidfile=$2
    if [ -f "$pidfile" ]; then
        local pid
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" && ok "$label detenido (PID $pid)"
        fi
        rm -f "$pidfile"
    fi
}

log "Parando nuestro Airflow (venv $AIRFLOW_VENV)"
# Solo matar los procesos Airflow que usen nuestro venv, no los de otros proyectos
for kw in "airflow scheduler" "airflow api-server" "airflow api_server"; do
    for pid in $(pgrep -af "$kw" | grep "$AIRFLOW_VENV" | awk '{print $1}'); do
        kill "$pid" 2>/dev/null && ok "$kw (PID $pid) detenido"
    done
done
rm -f "$PID_DIR"/airflow-*.pid

log "Parando HiveServer2 + Metastore"
kill_pidfile "HiveServer2" "$PID_DIR/hiveserver2.pid"
kill_pidfile "Metastore"   "$PID_DIR/hive-metastore.pid"
pkill -f "HiveServer2" 2>/dev/null
pkill -f "HiveMetaStore" 2>/dev/null

log "Parando Cassandra"
kill_pidfile "Cassandra" "$PID_DIR/cassandra.pid"
pkill -f "CassandraDaemon" 2>/dev/null

log "Parando Kafka"
"$KAFKA_HOME/bin/kafka-server-stop.sh" >/dev/null 2>&1
kill_pidfile "Kafka" "$PID_DIR/kafka.pid"

log "Parando HDFS"
"$HADOOP_HOME/sbin/stop-dfs.sh" >/dev/null 2>&1

log "Parando NiFi (si estaba arrancado)"
"$NIFI_HOME/bin/nifi.sh" stop >/dev/null 2>&1 || true

echo ""
ok "Stack detenido."
