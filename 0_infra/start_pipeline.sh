#!/bin/bash
# ============================================================================
# start_pipeline.sh — Arranca los componentes del pipeline KDD Gaming
# ----------------------------------------------------------------------------
# Cada componente corre en background; logs y PIDs quedan en 0_infra/.
#
# Uso:
#   bash 0_infra/start_pipeline.sh producer     # Steam → Kafka
#   bash 0_infra/start_pipeline.sh streaming    # Kafka → Spark → Cassandra
#   bash 0_infra/start_pipeline.sh api          # FastAPI (Cassandra → REST)
#   bash 0_infra/start_pipeline.sh dashboard    # React (Vite dev server)
#   bash 0_infra/start_pipeline.sh speed        # producer + streaming + api
#   bash 0_infra/start_pipeline.sh all          # todo lo anterior + dashboard
#   bash 0_infra/start_pipeline.sh --status     # ver estado
#   bash 0_infra/start_pipeline.sh stop         # parar todo el pipeline
# ============================================================================

set -u

ROOT="/home/hadoop/juegosKDD"
VENV="$ROOT/venv"
LOG_DIR="$ROOT/0_infra/logs"
PID_DIR="$ROOT/0_infra/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
export CASSANDRA_HOST="${CASSANDRA_HOST:-127.0.0.1}"
export CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
export CASSANDRA_KEYSPACE="${CASSANDRA_KEYSPACE:-gaming_kdd}"

C_BLUE="\033[1;34m"; C_GREEN="\033[1;32m"; C_YELLOW="\033[1;33m"
C_RED="\033[1;31m"; C_GREY="\033[0;90m"; C_RESET="\033[0m"
log()  { echo -e "${C_BLUE}▸${C_RESET} $*"; }
ok()   { echo -e "  ${C_GREEN}✓${C_RESET} $*"; }
warn() { echo -e "  ${C_YELLOW}⚠${C_RESET} $*"; }
err()  { echo -e "  ${C_RED}✗${C_RESET} $*"; }

port_open() {
    local port=$1
    for host in 127.0.0.1 localhost nodo1; do
        (echo > /dev/tcp/"$host"/"$port") >/dev/null 2>&1 && return 0
    done
    return 1
}

ensure_venv() {
    if [ ! -x "$VENV/bin/python" ]; then
        err "venv no existe. Ejecuta primero: bash 0_infra/setup_venv.sh"
        exit 1
    fi
}

pid_alive() {
    local pidfile=$1
    [ -f "$pidfile" ] || return 1
    local pid; pid=$(cat "$pidfile" 2>/dev/null)
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

# ── 1. Producer (Steam → Kafka) ─────────────────────────────────────────────
start_producer() {
    log "Producer Steam → Kafka (topic gaming.events.raw)"
    ensure_venv
    if ! port_open 9092; then err "Kafka no responde en :9092"; return 1; fi

    if pid_alive "$PID_DIR/producer.pid"; then
        ok "producer ya corriendo (PID $(cat $PID_DIR/producer.pid))"
        return 0
    fi

    cd "$ROOT/1_ingesta/producer" && \
    nohup "$VENV/bin/python" steam_producer.py --mode=stream \
        >"$LOG_DIR/producer.log" 2>&1 &
    echo $! > "$PID_DIR/producer.pid"
    ok "producer lanzado (PID $!) — logs: $LOG_DIR/producer.log"
}

# ── 2. Spark Streaming (Kafka → Cassandra) ──────────────────────────────────
start_streaming() {
    log "Spark Streaming (Kafka → Cassandra)"
    if ! port_open 9092; then err "Kafka no responde en :9092"; return 1; fi
    if ! port_open 9042; then err "Cassandra no responde en :9042"; return 1; fi

    if pid_alive "$PID_DIR/streaming.pid"; then
        ok "streaming ya corriendo (PID $(cat $PID_DIR/streaming.pid))"
        return 0
    fi

    cd "$ROOT/3_speed_layer" && \
    nohup bash submit_streaming.sh \
        >"$LOG_DIR/streaming.log" 2>&1 &
    echo $! > "$PID_DIR/streaming.pid"
    ok "streaming lanzado (PID $!) — logs: $LOG_DIR/streaming.log"
    warn "Spark tarda 30–60s en iniciar; vigila con: tail -f $LOG_DIR/streaming.log"
}

# ── 3. FastAPI (Cassandra → REST) ───────────────────────────────────────────
start_api() {
    log "FastAPI (Cassandra → REST en :8000)"
    ensure_venv
    if ! port_open 9042; then err "Cassandra no responde en :9042"; return 1; fi

    if port_open 8000; then
        ok "FastAPI ya escuchando en :8000"
        return 0
    fi

    cd "$ROOT/5_serving_layer/api" && \
    nohup "$VENV/bin/uvicorn" main:app --host 0.0.0.0 --port 8000 \
        >"$LOG_DIR/api.log" 2>&1 &
    echo $! > "$PID_DIR/api.pid"
    ok "API lanzada (PID $!) — logs: $LOG_DIR/api.log"
    ok "Prueba: curl http://localhost:8000/api/realtime"
}

# ── 4. Dashboard React (Vite dev server) ────────────────────────────────────
start_dashboard() {
    log "Dashboard React (Vite en :5173)"
    if [ ! -d "$ROOT/7_dashboard/node_modules" ]; then
        warn "node_modules no existe — instalando dependencias npm..."
        (cd "$ROOT/7_dashboard" && npm install) \
            || { err "npm install falló"; return 1; }
    fi

    if port_open 5173; then
        ok "dashboard ya escuchando en :5173"
        return 0
    fi

    cd "$ROOT/7_dashboard" && \
    nohup npm run dev -- --host 0.0.0.0 \
        >"$LOG_DIR/dashboard.log" 2>&1 &
    echo $! > "$PID_DIR/dashboard.pid"
    ok "dashboard lanzado (PID $!) — abre http://localhost:5173"
}

# ── Parar todo ──────────────────────────────────────────────────────────────
stop_all() {
    log "Deteniendo componentes del pipeline..."
    for comp in producer streaming api dashboard; do
        if pid_alive "$PID_DIR/$comp.pid"; then
            local pid; pid=$(cat "$PID_DIR/$comp.pid")
            kill "$pid" 2>/dev/null && ok "$comp detenido (PID $pid)"
            rm -f "$PID_DIR/$comp.pid"
        else
            ok "$comp no estaba corriendo"
        fi
    done
    # Por si quedan procesos de spark-submit huérfanos
    pgrep -f "spark_streaming_kdd" | xargs -r kill 2>/dev/null
}

# ── Estado ──────────────────────────────────────────────────────────────────
show_status() {
    echo ""
    echo "═══ Pipeline KDD Gaming ═══════════════════════════════════════════"
    for row in \
        "Kafka:9092" \
        "Cassandra:9042" \
        "HiveServer2:10000" \
        "Airflow API:8080" \
        "FastAPI:8000" \
        "Dashboard Vite:5173" \
        ; do
        name="${row%:*}"; port="${row##*:}"
        if port_open "$port"; then
            printf "  ${C_GREEN}✓${C_RESET} %-18s :%s\n" "$name" "$port"
        else
            printf "  ${C_RED}✗${C_RESET} %-18s :%s cerrado\n" "$name" "$port"
        fi
    done
    echo ""
    echo "Procesos del pipeline:"
    for comp in producer streaming api dashboard; do
        if pid_alive "$PID_DIR/$comp.pid"; then
            printf "  ${C_GREEN}✓${C_RESET} %-12s PID=%s\n" "$comp" "$(cat $PID_DIR/$comp.pid)"
        else
            printf "  ${C_GREY}·${C_RESET} %-12s parado\n" "$comp"
        fi
    done
    echo "═══════════════════════════════════════════════════════════════════"
}

# ── Dispatcher ──────────────────────────────────────────────────────────────
case "${1:-}" in
    producer)   start_producer ;;
    streaming)  start_streaming ;;
    api)        start_api ;;
    dashboard)  start_dashboard ;;
    speed)      start_producer && sleep 2 && start_streaming && sleep 5 && start_api ;;
    all)        start_producer && sleep 2 && start_streaming && sleep 5 && start_api && start_dashboard ;;
    stop)       stop_all ;;
    --status|status) show_status ;;
    *)
        cat <<EOF
Uso: $0 <comando>

Componentes:
  producer    Steam API → Kafka (topic gaming.events.raw)
  streaming   Kafka → Spark Streaming → Cassandra (Speed Layer)
  api         FastAPI (Cassandra → REST en :8000)
  dashboard   React + Vite (dashboard en :5173)

Atajos:
  speed       producer + streaming + api (Speed Layer completo)
  all         speed + dashboard (todo)
  stop        detiene todos los componentes
  --status    muestra estado de puertos y procesos

Prerrequisitos:
  · bash 0_infra/start_stack.sh     (infraestructura arrancada)
  · bash 0_infra/init_schemas.sh    (schemas creados)
  · bash 0_infra/setup_venv.sh      (venv listo)
EOF
        ;;
esac
