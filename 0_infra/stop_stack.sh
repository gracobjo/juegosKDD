#!/bin/bash
# stop_stack.sh — Apaga limpiamente todos los servicios lanzados por start_stack.sh

set -u

export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
CASSANDRA_HOME="${CASSANDRA_HOME:-/home/hadoop/smart_energy/cassandra}"
# Venv principal de juegosKDD (el que usa start_stack.sh)
AIRFLOW_VENV="${AIRFLOW_VENV:-/home/hadoop/juegosKDD/venv}"
AIRFLOW_VENV_LEGACY="/home/hadoop/smart_energy/venv"
# AIRFLOW_HOME dedicado a juegosKDD — NUNCA tocamos smart_grid ni transporte
AIRFLOW_HOME_DIR="${AIRFLOW_HOME:-/home/hadoop/juegosKDD/0_infra/airflow_home}"
# Nuestro puerto de API — no tocar smart_grid (8080) ni transporte (8088)
OUR_AIRFLOW_PORT="${AIRFLOW_API_PORT:-8090}"
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
            kill "$pid" 2>/dev/null && ok "$label detenido (PID $pid)"
            # Esperamos y forzamos si no muere
            for i in 1 2 3 4 5; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
        fi
        rm -f "$pidfile"
    fi
}

# Mata procesos Airflow de este proyecto identificándolos por AIRFLOW_HOME o venv
kill_our_airflow() {
    local found=0
    # El api-server de Airflow 3.x puede aparecer como "airflow api_server", sin
    # rutas de venv, así que cruzamos por AIRFLOW_HOME en /proc/PID/environ.
    # NUESTRO puerto de api = 8090 (smart_grid:8080 · transporte:8088).
    for pid in $(pgrep -f 'airflow (scheduler|api.?server|dag.?processor)' 2>/dev/null); do
        [ -r "/proc/$pid/environ" ] || continue
        local env_home exe_path cmdline is_ours=0
        env_home=$(tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | \
                   awk -F= '$1=="AIRFLOW_HOME"{print $2; exit}')
        exe_path=$(readlink -f "/proc/$pid/exe" 2>/dev/null)
        cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)

        # Criterio 1: AIRFLOW_HOME del env coincide con el nuestro
        [ "$env_home" = "$AIRFLOW_HOME_DIR" ] && is_ours=1

        # Criterio 2: cmdline/env mencionan nuestro AIRFLOW_HOME
        [[ "$cmdline" == *"$AIRFLOW_HOME_DIR"* ]] && is_ours=1

        # Criterio 3: ejecutable está bajo el venv de juegosKDD o el legacy
        [[ "$exe_path" == "$AIRFLOW_VENV"* || "$exe_path" == "$AIRFLOW_VENV_LEGACY"* ]] && \
            [[ "$cmdline" == *"port:${OUR_AIRFLOW_PORT}"* || \
               "$cmdline" == *"--port ${OUR_AIRFLOW_PORT}"* || \
               "$cmdline" == *"port ${OUR_AIRFLOW_PORT}"* ]] && is_ours=1

        # Criterio 4 (red): si escucha exactamente en NUESTRO puerto, es nuestro
        if ss -ltnp 2>/dev/null | awk -v p=":${OUR_AIRFLOW_PORT}$" \
               '$4 ~ p {print}' | grep -q "pid=${pid},"; then
            is_ours=1
        fi

        if [ "$is_ours" = "1" ]; then
            kill "$pid" 2>/dev/null && \
                ok "Airflow PID $pid detenido (home=${env_home:-?} cmd=${cmdline:0:60})"
            found=$((found+1))
        fi
    done

    # Criterio extra: cualquier cosa que aún escuche en NUESTRO puerto 8090
    # tras el primer barrido es inequívocamente nuestro — lo matamos.
    for pid in $(ss -ltnp 2>/dev/null | \
                 awk -v p=":${OUR_AIRFLOW_PORT}$" '$4 ~ p {print}' | \
                 grep -oE 'pid=[0-9]+' | cut -d= -f2 | sort -u); do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null && \
                ok "Airflow api-server PID $pid detenido (port :${OUR_AIRFLOW_PORT})"
            found=$((found+1))
        fi
    done
    # Dar tiempo a que los workers uvicorn hijos también mueran
    sleep 3
    # Forzar los que sigan vivos (mismo conjunto de criterios)
    for pid in $(pgrep -f 'airflow (scheduler|api.?server|dag.?processor)' 2>/dev/null); do
        [ -r "/proc/$pid/environ" ] || continue
        local env_home cmdline exe_path is_ours=0
        env_home=$(tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | \
                   awk -F= '$1=="AIRFLOW_HOME"{print $2; exit}')
        cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)
        exe_path=$(readlink -f "/proc/$pid/exe" 2>/dev/null)
        [ "$env_home" = "$AIRFLOW_HOME_DIR" ] && is_ours=1
        [[ "$cmdline" == *"$AIRFLOW_HOME_DIR"* ]] && is_ours=1
        [[ "$exe_path" == "$AIRFLOW_VENV"* || "$exe_path" == "$AIRFLOW_VENV_LEGACY"* ]] && \
            [[ "$cmdline" == *":${OUR_AIRFLOW_PORT}"* ]] && is_ours=1
        if [ "$is_ours" = "1" ]; then
            kill -9 "$pid" 2>/dev/null && ok "Airflow PID $pid (forzado)"
        fi
    done
    # Fuerza residuales que aún ocupan nuestro puerto
    for pid in $(ss -ltnp 2>/dev/null | \
                 awk -v p=":${OUR_AIRFLOW_PORT}$" '$4 ~ p {print}' | \
                 grep -oE 'pid=[0-9]+' | cut -d= -f2 | sort -u); do
        kill -9 "$pid" 2>/dev/null && ok "PID $pid en :${OUR_AIRFLOW_PORT} forzado"
    done
    # También liberar log-server (8793) si quedó huérfano con nuestro AIRFLOW_HOME
    for pid in $(ss -ltnp 2>/dev/null | awk '/:8793/{for(i=1;i<=NF;i++) if($i~/pid=/){sub(/.*pid=/,"",$i); sub(/,.*/,"",$i); print $i}}'); do
        [ -r "/proc/$pid/environ" ] || continue
        local env_home
        env_home=$(tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | \
                   awk -F= '$1=="AIRFLOW_HOME"{print $2; exit}')
        if [ "$env_home" = "$AIRFLOW_HOME_DIR" ]; then
            kill -9 "$pid" 2>/dev/null && ok "log-server :8793 liberado (PID $pid)"
        fi
    done
    [ "$found" -eq 0 ] && ok "No había Airflow nuestro corriendo"
}

case "${1:-all}" in
    airflow)
        log "Parando SOLO Airflow de juegosKDD (AIRFLOW_HOME=$AIRFLOW_HOME_DIR)"
        kill_our_airflow
        rm -f "$PID_DIR"/airflow-*.pid
        exit 0
        ;;
esac

log "Parando Airflow de juegosKDD (AIRFLOW_HOME=$AIRFLOW_HOME_DIR)"
kill_our_airflow
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
