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
# Airflow corre AISLADO en el venv propio del proyecto (~/juegosKDD/venv).
# Antes se reutilizaba ~/smart_energy/venv, pero eso acoplaba dependencias
# entre proyectos (un pip upgrade en smart_energy rompía este Airflow).
# Se instala con:  pip install -r 0_infra/requirements-airflow.txt --constraint …
AIRFLOW_VENV="${AIRFLOW_VENV:-/home/hadoop/juegosKDD/venv}"
# AIRFLOW_HOME DEDICADO a juegosKDD para NO pisar smart_grid (/home/hadoop/airflow)
# ni proyecto_transporte (~/proyecto_transporte_global/airflow_home).
AIRFLOW_HOME="${AIRFLOW_HOME:-/home/hadoop/juegosKDD/0_infra/airflow_home}"
AIRFLOW_API_PORT="${AIRFLOW_API_PORT:-8090}"   # smart_grid:8080 · transporte:8088
# NiFi propio de juegosKDD (copia limpia sin flow/repos/keystores del
# NiFi compartido de smart_energy). Así el canvas arranca vacío y no vemos
# procesadores de otros proyectos. El :8443 sigue siendo el mismo: no
# puede coexistir al mismo tiempo con el NiFi de smart_energy.
NIFI_HOME="${NIFI_HOME:-/home/hadoop/juegosKDD/0_infra/nifi-2.6.0}"

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
    # Evita cargar DAGs de ejemplo (desordenan la UI)
    export AIRFLOW__CORE__LOAD_EXAMPLES=False
    # Da margen a la importación del DAG (Python lento en el primer parseo)
    export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=60
    # DB SQLite dedicada a juegosKDD (aislada de smart_grid y transporte)
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///${AIRFLOW_HOME}/airflow.db"
    # Fuerza el puerto también vía env por si algún subproceso no recibe --port
    export AIRFLOW__API__PORT=$AIRFLOW_API_PORT
    export AIRFLOW__API__HOST=0.0.0.0
    # Airflow 3.x: el Task SDK del worker habla con la "Execution API" expuesta
    # por el api-server. Si no se indica URL, cae por defecto a
    # http://localhost:8080/ y, como nuestro api-server corre en 8090, el worker
    # revienta con `httpx.ConnectError: [Errno 111] Connection refused` ANTES
    # de imprimir nada en el log del task (logs vacíos de 0 bytes, task en
    # state=queued + executor_state=failed). Con estas variables alineamos
    # scheduler/workers/api-server al mismo puerto.
    export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="http://localhost:${AIRFLOW_API_PORT}/execution/"
    export AIRFLOW__API__BASE_URL="http://localhost:${AIRFLOW_API_PORT}"

    mkdir -p "$AIRFLOW_HOME"

    # ── Secretos persistentes (CRÍTICO en Airflow 3.x) ───────────────────────
    # El Task SDK del worker pasa un JWT al api-server. Si scheduler,
    # dag-processor y api-server no comparten la MISMA jwt_secret, los tokens
    # emitidos por uno son rechazados por los otros (Signature verification
    # failed → tasks en state mismatch/failed). Los generamos una vez y los
    # persistimos en el propio AIRFLOW_HOME.
    local secrets_file="$AIRFLOW_HOME/.secrets.env"
    if [ ! -f "$secrets_file" ]; then
        local jwt fernet
        jwt=$("$AIRFLOW_VENV/bin/python" -c \
              'import secrets; print(secrets.token_urlsafe(48))')
        fernet=$("$AIRFLOW_VENV/bin/python" -c \
                 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')
        cat >"$secrets_file" <<EOF
# Generado automáticamente por start_stack.sh — NO commit (contiene secretos)
AIRFLOW__API_AUTH__JWT_SECRET=$jwt
AIRFLOW__CORE__INTERNAL_API_SECRET_KEY=$jwt
AIRFLOW__CORE__FERNET_KEY=$fernet
EOF
        chmod 600 "$secrets_file"
        ok "Secretos Airflow generados en $secrets_file"
    fi
    # shellcheck disable=SC1090
    set -a; . "$secrets_file"; set +a

    # Inicialización automática de la DB la primera vez
    if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
        warn "AIRFLOW_HOME sin DB. Inicializando DB SQLite en $AIRFLOW_HOME..."
        "$AIRFLOW_VENV/bin/airflow" db migrate \
            >"$LOG_DIR/airflow-init.log" 2>&1 \
            && ok "DB Airflow inicializada" \
            || warn "db migrate devolvió errores. Revisa $LOG_DIR/airflow-init.log"
    fi

    # Informativo: si hay otro Airflow coexistiendo (lo respetamos)
    check_foreign_airflow

    # Limpiar PIDs viejos nuestros
    rm -f "$AIRFLOW_HOME"/*.pid 2>/dev/null

    # "Nuestro" = proceso con AIRFLOW_HOME igual + exe dentro de AIRFLOW_VENV.
    # Esto evita confundirnos con el Airflow de proyecto_transporte (otro venv).
    is_our_airflow_running() {
        local pattern=$1
        local pid env_home exe_path
        for pid in $(pgrep -f "$pattern" 2>/dev/null); do
            [ -r "/proc/$pid/environ" ] || continue
            env_home=$(tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | \
                       awk -F= '$1=="AIRFLOW_HOME"{print $2; exit}')
            exe_path=$(readlink -f "/proc/$pid/exe" 2>/dev/null)
            if [ "$env_home" = "$AIRFLOW_HOME" ] && \
               [[ "$exe_path" == "$AIRFLOW_VENV"* ]]; then
                return 0
            fi
        done
        return 1
    }

    # Scheduler
    if ! is_our_airflow_running 'airflow scheduler'; then
        nohup "$AIRFLOW_VENV/bin/airflow" scheduler \
            >"$LOG_DIR/airflow-scheduler.log" 2>&1 &
        echo $! > "$PID_DIR/airflow-scheduler.pid"
        ok "Airflow scheduler lanzado (PID $!) — AIRFLOW_HOME=$AIRFLOW_HOME"
    else
        ok "Airflow scheduler ya corriendo (nuestro)"
    fi

    # DAG-Processor (OBLIGATORIO en Airflow 3.x — el scheduler ya no parsea DAGs)
    if ! is_our_airflow_running 'airflow dag.?processor'; then
        nohup "$AIRFLOW_VENV/bin/airflow" dag-processor \
            >"$LOG_DIR/airflow-dagprocessor.log" 2>&1 &
        echo $! > "$PID_DIR/airflow-dagprocessor.pid"
        ok "Airflow dag-processor lanzado (PID $!)"
    else
        ok "Airflow dag-processor ya corriendo (nuestro)"
    fi

    # API server en puerto propio (por defecto :8090)
    if is_our_airflow_running 'airflow api.?server'; then
        ok "Airflow API server ya corriendo (nuestro) en :$AIRFLOW_API_PORT"
    elif port_open "$AIRFLOW_API_PORT"; then
        warn "El puerto :$AIRFLOW_API_PORT está ocupado pero no por Airflow nuestro."
        warn "Ejecuta: bash 0_infra/stop_stack.sh airflow  y reintenta."
    else
        nohup "$AIRFLOW_VENV/bin/airflow" api-server \
            --host 0.0.0.0 --port "$AIRFLOW_API_PORT" \
            >"$LOG_DIR/airflow-apiserver.log" 2>&1 &
        echo $! > "$PID_DIR/airflow-apiserver.pid"
        wait_for_port "Airflow API" "$AIRFLOW_API_PORT" 60 || \
            warn "Revisa $LOG_DIR/airflow-apiserver.log"
    fi
}

# ── NiFi ─────────────────────────────────────────────────────────────────────
# NOTA de aislamiento: a diferencia del venv de Airflow (que SÍ hemos
# independizado en ~/juegosKDD/venv), NiFi sigue ejecutándose desde
# $NIFI_HOME (por defecto ~/smart_energy/nifi-2.6.0) porque toda su
# configuración (flow.json.gz, keystore/truststore, usuarios) está allí.
# Mover esto implicaría una migración mayor. Si varios proyectos usan la
# misma instalación de NiFi, comparten el :8443.
start_nifi() {
    log "NiFi 2.x (HTTPS en :8443)"
    if port_open 8443; then ok "NiFi ya arrancado"; return 0; fi
    if [ ! -x "$NIFI_HOME/bin/nifi.sh" ]; then
        err "NiFi no encontrado en $NIFI_HOME — instala o ajusta NIFI_HOME"
        return 1
    fi
    "$NIFI_HOME/bin/nifi.sh" start >"$LOG_DIR/nifi.log" 2>&1
    # NiFi 2.x tarda 60–120s en levantar Jetty + cargar el flow
    wait_for_port NiFi 8443 150 || \
        warn "Si no respondió, revisa $NIFI_HOME/logs/nifi-app.log"
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
        "Airflow API (juegosKDD):$AIRFLOW_API_PORT" \
        "NiFi (HTTPS):8443" \
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
        start_nifi
        show_status
        ;;
    *)
        echo "Uso: $0 [all|hdfs|kafka|cassandra|hive|airflow|nifi|clean|--status]"
        exit 1
        ;;
esac
