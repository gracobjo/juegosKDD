#!/bin/bash
# ============================================================================
# init_schemas.sh — Crea los esquemas (BD, keyspace, topics) del proyecto
# juegosKDD SIN interferir con otros proyectos (logistica_espana, etc.)
#
# Todos los recursos se crean bajo nombres exclusivos:
#   · Hive:       database  gaming_kdd
#   · Cassandra:  keyspace  gaming_kdd
#   · Kafka:      topics    gaming.*
#
# Idempotente: usa IF NOT EXISTS, se puede ejecutar varias veces.
# Uso:
#   bash 0_infra/init_schemas.sh              # crea todo (hive + cass + kafka)
#   bash 0_infra/init_schemas.sh --check      # solo muestra qué existe hoy
#   bash 0_infra/init_schemas.sh hive         # solo schema Hive
#   bash 0_infra/init_schemas.sh cassandra    # solo keyspace Cassandra
#   bash 0_infra/init_schemas.sh kafka        # solo topics Kafka
# ============================================================================

set -u

ROOT="/home/hadoop/juegosKDD"
export KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"

C_BLUE="\033[1;34m"; C_GREEN="\033[1;32m"; C_YELLOW="\033[1;33m"
C_RED="\033[1;31m";  C_GREY="\033[0;90m"; C_RESET="\033[0m"

log()  { echo -e "${C_BLUE}▸${C_RESET} $*"; }
ok()   { echo -e "  ${C_GREEN}✓${C_RESET} $*"; }
warn() { echo -e "  ${C_YELLOW}⚠${C_RESET} $*"; }
err()  { echo -e "  ${C_RED}✗${C_RESET} $*"; }

# ── CHECK: muestra qué ya existe en cada sistema ─────────────────────────────
check_all() {
    log "Hive databases existentes"
    beeline -u jdbc:hive2://localhost:10000 -n hadoop --showHeader=false --outputformat=tsv2 \
        -e "SHOW DATABASES;" 2>/dev/null | sed 's/^/    /'

    log "Hive tablas dentro de gaming_kdd (si existe)"
    beeline -u jdbc:hive2://localhost:10000 -n hadoop --showHeader=false --outputformat=tsv2 \
        -e "SHOW TABLES IN gaming_kdd;" 2>/dev/null | sed 's/^/    /' \
        || warn "gaming_kdd aún no existe"

    log "Cassandra keyspaces existentes"
    cqlsh -e "DESCRIBE KEYSPACES;" 2>/dev/null | sed 's/^/    /'

    log "Cassandra tablas dentro de gaming_kdd (si existe)"
    if cqlsh -e "DESCRIBE KEYSPACE gaming_kdd;" >/dev/null 2>&1; then
        cqlsh -k gaming_kdd -e "DESCRIBE TABLES;" 2>/dev/null | sed 's/^/    /'
    else
        warn "keyspace gaming_kdd aún no existe"
    fi

    log "Kafka topics existentes (filtrados por gaming.*)"
    "$KAFKA_HOME/bin/kafka-topics.sh" --list --bootstrap-server localhost:9092 2>/dev/null \
        | grep -E '^gaming\.' | sed 's/^/    /' \
        || warn "sin topics gaming.* creados"
}

# ── HIVE ─────────────────────────────────────────────────────────────────────
init_hive() {
    log "Hive → creando BD 'gaming_kdd' y tablas"
    if ! (echo > /dev/tcp/localhost/10000) 2>/dev/null; then
        err "HiveServer2 no responde en :10000. Arranca con: bash 0_infra/start_stack.sh hive"
        return 1
    fi

    # Aseguramos que existe el directorio de warehouse en HDFS (por si ha cambiado permisos)
    hdfs dfs -mkdir -p /user/hive/warehouse 2>/dev/null
    hdfs dfs -chmod g+w /user/hive/warehouse 2>/dev/null

    beeline -u jdbc:hive2://localhost:10000 -n hadoop \
        --hiveconf hive.cli.errors.ignore=false \
        -f "$ROOT/4_batch_layer/hive_schema.sql" \
        2>&1 | grep -vE '^(INFO|SLF4J|Beeline|Connecting|Connected|Driver|Transaction|Closing)' \
             | grep -v '^$' \
             | sed 's/^/    /'

    ok "Hive: DB gaming_kdd lista"
}

# ── CASSANDRA ────────────────────────────────────────────────────────────────
init_cassandra() {
    log "Cassandra → creando keyspace 'gaming_kdd' y tablas"
    if ! (echo > /dev/tcp/localhost/9042) 2>/dev/null; then
        err "Cassandra no responde en :9042. Arranca con: bash 0_infra/start_stack.sh cassandra"
        return 1
    fi

    cqlsh -f "$ROOT/5_serving_layer/cassandra_schema.cql" 2>&1 | sed 's/^/    /'
    ok "Cassandra: keyspace gaming_kdd listo"
}

# ── KAFKA ────────────────────────────────────────────────────────────────────
init_kafka() {
    log "Kafka → creando topics 'gaming.*'"
    if ! (echo > /dev/tcp/localhost/9092) 2>/dev/null; then
        err "Kafka no responde en :9092. Arranca con: bash 0_infra/start_stack.sh kafka"
        return 1
    fi

    bash "$ROOT/2_kafka/setup_topics.sh" 2>&1 | sed 's/^/    /'
    ok "Kafka: topics gaming.* creados"
}

# ── Dispatcher ───────────────────────────────────────────────────────────────
case "${1:-all}" in
    --check|check) check_all ;;
    hive)          init_hive ;;
    cassandra)     init_cassandra ;;
    kafka)         init_kafka ;;
    all)
        check_all
        echo ""
        init_kafka
        init_cassandra
        init_hive
        echo ""
        log "Estado final:"
        check_all
        ;;
    *)
        echo "Uso: $0 [all|hive|cassandra|kafka|--check]"
        exit 1
        ;;
esac
