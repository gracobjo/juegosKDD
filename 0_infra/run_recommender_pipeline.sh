#!/usr/bin/env bash
# ============================================================================
# run_recommender_pipeline.sh — Comprueba servicios y ejecuta el stack
# recomendador (validación CSV → Hive → pipeline ALS+TF-IDF → Cassandra).
#
# Uso:
#   bash 0_infra/run_recommender_pipeline.sh              # todo (validar+cargar+kdd)
#   bash 0_infra/run_recommender_pipeline.sh --kdd-only   # solo kdd_pipeline (Hive ya cargado)
#   bash 0_infra/run_recommender_pipeline.sh --hive-only  # solo loader Hive
#   bash 0_infra/run_recommender_pipeline.sh --check      # solo comprobaciones
#   bash 0_infra/run_recommender_pipeline.sh --download   # solo descarga Kaggle
#   bash 0_infra/run_recommender_pipeline.sh --fetch      # descarga + validar + Hive + KDD (si hay kaggle.json)
#
# Requiere: venv con dependencias, Spark en PATH. Los CSV deben estar en 0_data/raw/
#            (véase 0_data/raw/README.txt) o usar --download / --fetch con Kaggle.
# ============================================================================

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="${ROOT}/venv"
PYTHON="${VENV}/bin/python"
SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"
HADOOP="${HADOOP_HOME:-/opt/hadoop}/bin/hadoop"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
CASS_HOST="${CASSANDRA_HOST:-localhost}"
CASS_PORT="${CASSANDRA_PORT:-9042}"
export CASSANDRA_HOST="${CASSANDRA_HOST:-$CASS_HOST}"
export CASSANDRA_PORT="${CASSANDRA_PORT:-$CASS_PORT}"

MODE="full"
for a in "$@"; do
  case "$a" in
    --check)       MODE="check" ;;
    --kdd-only)    MODE="kdd" ;;
    --hive-only)   MODE="hive" ;;
    --download)    MODE="download" ;;
    --fetch)       MODE="fetch" ;;
    --full)        MODE="full" ;;
  esac
done

C_BLUE="\033[1;34m"; C_GREEN="\033[1;32m"; C_YELLOW="\033[1;33m"
C_RED="\033[1;31m"; C_RESET="\033[0m"
log() { echo -e "${C_BLUE}▸${C_RESET} $*"; }
ok()  { echo -e "  ${C_GREEN}✓${C_RESET} $*"; }
warn(){ echo -e "  ${C_YELLOW}!${C_RESET} $*"; }
err() { echo -e "  ${C_RED}✗${C_RESET} $*"; }

if [[ -x "$PYTHON" ]]; then
  # shellcheck source=/dev/null
  source "$VENV/bin/activate"
  export PATH="$VENV/bin:$PATH"
else
  PYTHON="python3"
  warn "No hay venv en $VENV — usando $PYTHON del sistema"
fi

check_tcp() {
  local host="$1" port="$2" name="$3"
  if (echo >"/dev/tcp/$host/$port") &>/dev/null; then
    ok "$name responde en $host:$port"
    return 0
  fi
  err "$name no responde en $host:$port"
  return 1
}

check_services() {
  log "Comprobando servicios…"
  local ok_all=0
  check_tcp "$CASS_HOST" "$CASS_PORT" "Cassandra" || ok_all=1
  check_tcp "localhost" "9092" "Kafka" || ok_all=1
  if [[ -x "$HADOOP" ]]; then
    if "$HADOOP" dfs -ls / &>/dev/null; then
      ok "HDFS accesible"
    else
      err "HDFS no accesible (hadoop dfs -ls /)"
      ok_all=1
    fi
  else
    warn "HADOOP_HOME no encontrado — omito prueba HDFS"
  fi
  if [[ -x "$KAFKA_HOME/bin/kafka-topics.sh" ]]; then
    if "$KAFKA_HOME/bin/kafka-topics.sh" --list --bootstrap-server localhost:9092 2>/dev/null | grep -q "gaming.user.interactions"; then
      ok "Topic Kafka gaming.user.interactions existe"
    else
      err "Falta topic gaming.user.interactions — ejecuta: bash 2_kafka/setup_topics.sh"
      ok_all=1
    fi
  else
    warn "kafka-topics.sh no encontrado — omito lista de topics"
  fi
  if ! "$PYTHON" -c "import cassandra" &>/dev/null; then
    warn "cassandra-driver no disponible en $PYTHON — omito comprobación de keyspace (usa: bash 0_infra/setup_venv.sh)"
  elif "$PYTHON" "$ROOT/0_infra/apply_cassandra_schema.py" --exists gaming_recommender &>/dev/null; then
    ok "Keyspace Cassandra gaming_recommender existe"
  else
    err "Falta keyspace gaming_recommender — ejecuta: $PYTHON $ROOT/0_infra/apply_cassandra_schema.py"
    ok_all=1
  fi
  return "$ok_all"
}

require_raw_csvs() {
  local d="$ROOT/0_data/raw"
  local b="$d/steam_games_behaviors.csv"
  local r="$d/steam_reviews.csv"
  mkdir -p "$d"
  if [[ ! -f "$b" || ! -f "$r" ]]; then
    err "Faltan CSV en $d"
    echo ""
    echo "  Opción A — Kaggle (~/.kaggle/kaggle.json o KAGGLE_USERNAME+KAGGLE_KEY en .env):"
    echo "    $PYTHON $ROOT/0_data/kaggle/download_datasets.py"
    echo "    bash $ROOT/0_infra/run_recommender_pipeline.sh"
    echo ""
    echo "  Opción B — Todo en un paso si ya tienes kaggle.json:"
    echo "    bash $ROOT/0_infra/run_recommender_pipeline.sh --fetch"
    echo ""
    echo "  Opción C — Copia manual los ficheros con estos nombres exactos:"
    echo "    $b"
    echo "    $r"
    echo ""
    echo "  Más detalle: cat $ROOT/0_data/raw/README.txt"
    exit 1
  fi
}

run_validate() {
  require_raw_csvs
  log "Validando CSV en 0_data/raw/…"
  "$PYTHON" "$ROOT/0_data/kaggle/validate_datasets.py"
}

run_download() {
  log "Descargando datasets desde Kaggle…"
  "$PYTHON" "$ROOT/0_data/kaggle/download_datasets.py"
}

run_hive_loader() {
  log "Spark: CSV → Hive (gaming_recommender.*)…"
  "$SPARK_SUBMIT" \
    --master "local[*]" \
    --driver-memory "${SPARK_DRIVER_MEMORY:-4g}" \
    "$ROOT/1_ingesta/kaggle_to_hive/loader.py"
}

run_kdd() {
  log "Spark: pipeline híbrido (ALS + TF-IDF + Cassandra)…"
  bash "$ROOT/4_batch_layer/submit_recommender_kdd.sh" --date "$(date -u +%F)" --alpha 0.7
}

case "$MODE" in
  check)
    check_services
    exit $?
    ;;
  download)
    check_services || exit 1
    run_download
    ok "Descarga finalizada (revisa 0_data/raw/). Siguiente: bash 0_infra/run_recommender_pipeline.sh"
    ;;
  fetch)
    check_services || exit 1
    run_download || exit 1
    run_validate
    run_hive_loader
    run_kdd
    ok "Fetch + Hive + KDD completados"
    ;;
  hive)
    check_services || exit 1
    run_validate
    run_hive_loader
    ok "Carga Hive finalizada"
    ;;
  kdd)
    check_services || exit 1
    run_validate
    run_kdd
    ok "Pipeline KDD recomendador finalizado"
    ;;
  full)
    check_services || exit 1
    run_validate
    run_hive_loader
    run_kdd
    ok "Pipeline completo (Hive + KDD + Cassandra)"
    ;;
esac

exit 0
