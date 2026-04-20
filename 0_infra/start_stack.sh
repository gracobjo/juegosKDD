#!/bin/bash
# ============================================================================
# start_stack.sh — Arranque ordenado de la infraestructura KDD λ Gaming
# ----------------------------------------------------------------------------
# Uso:
#   bash 0_infra/start_stack.sh            # arranca todo
#   bash 0_infra/start_stack.sh kafka      # arranca solo Kafka
#   bash 0_infra/start_stack.sh --status   # solo muestra estado
#
# Adaptado al entorno detectado en este equipo:
#   - Kafka 3.x en modo KRaft (sin Zookeeper) → /opt/kafka
#   - HDFS (Hadoop 3.x) → /opt/hadoop + /opt/hadoopdata
#   - Cassandra → ~/smart_energy/cassandra
#   - Hive 4.2.0 → ~/apache-hive-4.2.0-bin
#   - Airflow 3.x → venv en ~/smart_energy/venv
#   - NiFi 2.6.0 → ~/smart_energy/nifi-2.6.0
# ============================================================================

set -u

# ── Rutas ────────────────────────────────────────────────────────────────────
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}"
export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
export HIVE_HOME="${HIVE_HOME:-/home/hadoop/apache-hive-4.2.0-bin}"
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
CASSANDRA_HOME="${CASSANDRA_HOME:-/home/hadoop/smart_energy/cassandra}"
AIRFLOW_VENV="${AIRFLOW_VENV:-/home/hadoop/smart_energy/venv}"
AIRFLOW_HOME="${AIRFLOW_HOME:-/home/hadoop/airflow}"
NIFI_HOME="${NIFI_HOME:-/home/hadoop/smart_energy/nifi-2.6.0}"

LOG_DIR="/home/hadoop/juegosKDD/0_infra/logs"
PID_DIR="/home/hadoop/juegosKDD/0_infra/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

# ── Colores para logs ────────────────────────────────────────────────────────
C_BLUE="\033[1;34m"; C_GREEN="\033[1;32m"; C_YELLOW="\033[1;33m"
C_RED="\033[1;31m";  C_GREY="\033[0;90m"; C_RESET="\033[0m"

log()  { echo -e "${C_BLUE}▸${C_RESET} $*"; }
ok()   { echo -e "  ${C_GREEN}✓${C_RESET} $*"; }
warn() { echo -e "  ${C_YELLOW}⚠${C_RESET} $*"; }
err()  { echo -e "  ${C_RED}✗${C_RESET} $*"; }

# ── Utilidades ───────────────────────────────────────────────────────────────
# Prueba el puerto en varios hosts porque HDFS/Hive a veces bindean a "nodo1"
# (127.0.1.1) y Kafka/Cassandra a 127.0.0.1. Devuelve 0 si alguno responde.
port_open() {
    local port=$1 host
    for host in 127.0.0.1 localhost nodo1; do
        (echo > /dev/tcp/"$host"/"$port") >/dev/null 2>&1 && return 0
    done
    return 1
}

wait_for_port() {
    local name=$1 port=$2 timeout="${3:-60}"
    local i=0
    while [ $i -lt "$timeout" ]; do
        if port_open "$port"; then
            ok "$name escuchando en :$port"
            return 0
        fi
        sleep 1; i=$((i + 1))
        [ $((i % 10)) -eq 0 ] && echo -ne "  ${C_GREY}...esperando $name (${i}s/${timeout}s)${C_RESET}\r"
    done
    err "$name no arrancó en $timeout segundos (:$port cerrado)"
    return 1
}

# Informa (sin alarmar) si hay otros Airflow corriendo en el mismo equipo.
# Siempre que usen puertos y AIRFLOW_HOME distintos, pueden coexistir.
check_foreign_airflow() {
    local foreign
    foreign=$(pgrep -af "airflow (scheduler|api[-_]server)" 2>/dev/null \
              | grep -v "$AIRFLOW_VENV" | awk '{print $1}')
    if [ -n "$foreign" ]; then
        echo -e "  ${C_GREY}ℹ${C_RESET} Detectados otros Airflow en el equipo (coexisten si usan"
        echo -e "    distinto AIRFLOW_HOME y puerto; no se tocan):"
        ps -p $(echo "$foreign" | tr '\n' ',' | sed 's/,$//') -o pid,user=USUARIO,cmd 2>/dev/null \
            | sed 's/^/      /'
    fi
    return 0
}

# ── HDFS ─────────────────────────────────────────────────────────────────────
start_hdfs() {
    log "HDFS (namenode + datanode)"
    if port_open 9000; then ok "HDFS ya arrancado"; return 0; fi
    "$HADOOP_HOME/sbin/start-dfs.sh" >"$LOG_DIR/hdfs.log" 2>&1
    # Puede tardar bastante si HDFS entra en safe-mode extension por muchos blocks
    wait_for_port HDFS 9000 120
    # Crear directorios base que Hive necesitará
    "$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/hive/warehouse 2>/dev/null
    "$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /tmp 2>/dev/null
    "$HADOOP_HOME/bin/hdfs" dfs -chmod g+w /user/hive/warehouse /tmp 2>/dev/null
}

# ── Kafka (KRaft) ────────────────────────────────────────────────────────────
start_kafka() {
    log "Kafka (modo KRaft)"
    if port_open 9092; then ok "Kafka ya arrancado"; return 0; fi

    # Si el storage no está formateado, formatearlo
    if [ ! -f "$KAFKA_HOME/logs/meta.properties" ]; then
        warn "Storage Kafka sin formatear, creando cluster-id..."
        local CLUSTER_ID
        CLUSTER_ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
        "$KAFKA_HOME/bin/kafka-storage.sh" format \
            -t "$CLUSTER_ID" -c "$KAFKA_HOME/config/server.properties" \
            >>"$LOG_DIR/kafka.log" 2>&1 || true
    fi

    nohup "$KAFKA_HOME/bin/kafka-server-start.sh" \
        "$KAFKA_HOME/config/server.properties" \
        >"$LOG_DIR/kafka.log" 2>&1 &
    echo $! > "$PID_DIR/kafka.pid"
    wait_for_port Kafka 9092 45
}

# ── Cassandra ────────────────────────────────────────────────────────────────
start_cassandra() {
    log "Cassandra (puede tardar 60–90s con Java 21)"
    if port_open 9042; then ok "Cassandra ya arrancada"; return 0; fi

    # Cassandra 4.x requiere Java 11; con Java 21 añadimos la opción
    CASSANDRA_CONF="$CASSANDRA_HOME/conf" \
    JVM_EXTRA_OPTS="--add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
                    --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
                    --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED \
                    --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED \
                    --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED \
                    --add-exports java.sql/java.sql=ALL-UNNAMED \
                    --add-opens java.base/java.lang.module=ALL-UNNAMED \
                    --add-opens java.base/jdk.internal.loader=ALL-UNNAMED \
                    --add-opens java.base/jdk.internal.ref=ALL-UNNAMED \
                    --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED \
                    --add-opens java.base/jdk.internal.math=ALL-UNNAMED \
                    --add-opens java.base/jdk.internal.module=ALL-UNNAMED \
                    --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED \
                    --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED" \
    "$CASSANDRA_HOME/bin/cassandra" -R -p "$PID_DIR/cassandra.pid" \
        >"$LOG_DIR/cassandra.log" 2>&1

    wait_for_port Cassandra 9042 120
}

# ── Hive (Metastore + HiveServer2) ───────────────────────────────────────────
start_hive() {
    log "Hive Metastore + HiveServer2"

    # HDFS debe estar operativo: HS2 conecta a hdfs://nodo1:9000 al arrancar
    if ! port_open 9000; then
        err "HDFS no está disponible en :9000. Arranca HDFS primero."
        return 1
    fi

    # Inicialización del schema si es necesario (Derby local)
    # Hive 4.x por defecto crea metastore_db en $CWD o en /home/hadoop/.hive_metastore
    if [ ! -d "$HIVE_HOME/metastore_db" ] && [ ! -d "/home/hadoop/.hive_metastore/metastore_db" ]; then
        warn "Inicializando Hive Metastore (Derby)..."
        (cd "$HIVE_HOME" && "$HIVE_HOME/bin/schematool" -dbType derby -initSchema) \
            >>"$LOG_DIR/hive-init.log" 2>&1 || warn "schematool devolvió error (puede estar ya inicializado)"
    fi

    # Metastore (puerto 9083)
    if ! port_open 9083; then
        nohup "$HIVE_HOME/bin/hive" --service metastore \
            >"$LOG_DIR/hive-metastore.log" 2>&1 &
        echo $! > "$PID_DIR/hive-metastore.pid"
        wait_for_port "Hive Metastore" 9083 40
    else
        ok "Hive Metastore ya arrancado"
    fi

    # HiveServer2 (puerto 10000) — puede tardar 1–3 minutos
    if ! port_open 10000; then
        nohup "$HIVE_HOME/bin/hiveserver2" \
            >"$LOG_DIR/hiveserver2.log" 2>&1 &
        echo $! > "$PID_DIR/hiveserver2.pid"
        wait_for_port "HiveServer2" 10000 180
    else
        ok "HiveServer2 ya arrancado"
    fi
}

# ── Airflow (Scheduler + API server) ─────────────────────────────────────────
start_airflow() {
    log "Airflow (scheduler + api-server)"
    export AIRFLOW_HOME
    # Que nuestros DAGs se carguen desde el repo, no desde ~/airflow/dags
    export AIRFLOW__CORE__DAGS_FOLDER="/home/hadoop/juegosKDD/6_orchestration/dags"
    # Fuerza el puerto también vía env por si algún subproceso no recibe --port
    export AIRFLOW__API__PORT=8080
    export AIRFLOW__API__HOST=0.0.0.0

    # Informativo: si hay otro Airflow coexistiendo (lo respetamos)
    check_foreign_airflow

    # Limpiar PIDs viejos nuestros
    rm -f "$AIRFLOW_HOME"/*.pid 2>/dev/null

    # Scheduler — solo consideramos como "ya corriendo" el que use NUESTRO venv
    # (ignoramos los schedulers de otros proyectos como proyecto_transporte)
    if ! pgrep -af "airflow scheduler" | grep -q "$AIRFLOW_VENV"; then
        nohup "$AIRFLOW_VENV/bin/airflow" scheduler \
            >"$LOG_DIR/airflow-scheduler.log" 2>&1 &
        echo $! > "$PID_DIR/airflow-scheduler.pid"
        ok "Airflow scheduler lanzado (PID $!) — AIRFLOW_HOME=$AIRFLOW_HOME"
    else
        ok "Airflow scheduler ya corriendo (nuestro venv)"
    fi

    # API server forzando puerto 8080 (el de transporte usa 8088, no se pisan)
    if ! port_open 8080; then
        nohup "$AIRFLOW_VENV/bin/airflow" api-server --host 0.0.0.0 --port 8080 \
            >"$LOG_DIR/airflow-apiserver.log" 2>&1 &
        echo $! > "$PID_DIR/airflow-apiserver.pid"
        wait_for_port "Airflow API" 8080 60 || \
            warn "Revisa $LOG_DIR/airflow-apiserver.log"
    else
        ok "Airflow API server ya escuchando en :8080"
    fi
}

# ── NiFi (opcional) ──────────────────────────────────────────────────────────
start_nifi() {
    log "NiFi 2.x"
    if port_open 8443 || port_open 8081; then ok "NiFi ya arrancado"; return 0; fi
    "$NIFI_HOME/bin/nifi.sh" start >"$LOG_DIR/nifi.log" 2>&1
    warn "NiFi tarda ~60s en estar listo (HTTPS en :8443). Revisa con: nifi.sh status"
}

# ── Estado ───────────────────────────────────────────────────────────────────
show_status() {
    echo ""
    echo "═══ Estado del stack ═══════════════════════════════════════════════"
    for row in \
        "HDFS (namenode):9000" \
        "Kafka:9092" \
        "Cassandra:9042" \
        "Hive Metastore:9083" \
        "HiveServer2:10000" \
        "Airflow API:8080" \
        "FastAPI:8000" \
        "Dashboard Vite:5173" \
        ; do
        name="${row%:*}"; port="${row##*:}"
        if port_open "$port"; then
            printf "  ${C_GREEN}✓${C_RESET} %-22s escuchando en :%s\n" "$name" "$port"
        else
            printf "  ${C_RED}✗${C_RESET} %-22s %s cerrado\n" "$name" ":$port"
        fi
    done
    echo ""
    echo "Procesos Java activos:"
    jps 2>/dev/null | sed 's/^/  /'
    echo "═══════════════════════════════════════════════════════════════════"
}

# ── Clean: limpia solo PIDs huérfanos NUESTROS (no toca otros proyectos) ────
clean_stale() {
    log "Limpieza de PIDs huérfanos (solo de este proyecto)"
    for pf in "$PID_DIR"/*.pid; do
        [ -f "$pf" ] || continue
        local pid; pid=$(cat "$pf" 2>/dev/null)
        if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
            rm -f "$pf"
            ok "PID huérfano limpiado: $(basename "$pf")"
        fi
    done
    check_foreign_airflow
}

# ── Dispatcher ───────────────────────────────────────────────────────────────
case "${1:-all}" in
    --status|status) show_status ;;
    clean)     clean_stale ;;
    hdfs)      start_hdfs ;;
    kafka)     start_kafka ;;
    cassandra) start_cassandra ;;
    hive)      start_hive ;;
    airflow)   start_airflow ;;
    nifi)      start_nifi ;;
    all)
        clean_stale
        start_hdfs
        start_kafka
        start_cassandra
        start_hive
        start_airflow
        show_status
        ;;
    *)
        echo "Uso: $0 [all|hdfs|kafka|cassandra|hive|airflow|nifi|clean|--status]"
        exit 1
        ;;
esac
