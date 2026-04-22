#!/bin/bash
# ============================================================================
# setup_venv.sh — Crea un venv Python aislado para este proyecto
# ----------------------------------------------------------------------------
# Crea ~/juegosKDD/venv, instala requirements.txt y prepara un .env si no
# existe (partiendo de .env.example). No toca otros venvs del sistema.
# ============================================================================

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$ROOT/venv"

C_BLUE="\033[1;34m"; C_GREEN="\033[1;32m"; C_YELLOW="\033[1;33m"
C_RED="\033[1;31m"; C_RESET="\033[0m"
log()  { echo -e "${C_BLUE}▸${C_RESET} $*"; }
ok()   { echo -e "  ${C_GREEN}✓${C_RESET} $*"; }
warn() { echo -e "  ${C_YELLOW}⚠${C_RESET} $*"; }
err()  { echo -e "  ${C_RED}✗${C_RESET} $*"; }

# 1) Venv
if [ -d "$VENV" ]; then
    ok "venv ya existe en $VENV"
else
    log "Creando venv en $VENV..."
    python3 -m venv "$VENV" || { err "python3 -m venv falló (¿falta python3-venv?)"; exit 1; }
    ok "venv creado"
fi

# 2) Actualizar pip + instalar requirements
log "Instalando dependencias..."
"$VENV/bin/pip" install --upgrade pip wheel setuptools >/dev/null 2>&1
"$VENV/bin/pip" install -r "$ROOT/requirements.txt" || { err "Fallo instalando dependencias"; exit 1; }
ok "Dependencias instaladas (FastAPI, pandas, scikit-learn, kaggle, kagglehub — ver requirements.txt)"

# 2b) Airflow (aislado en ESTE venv, con constraints oficiales)
# --------------------------------------------------------------
# Airflow 3.x arrastra ~200 dependencias con versiones frágiles; por eso se
# instala SEPARADO usando el fichero constraints oficial de la versión
# elegida. Así se evita que `pip install -r requirements.txt` de otros
# proyectos rompa este Airflow (problema real que tuvimos con pydantic).
#
# Variables:
#   AIRFLOW_SKIP=1      → omite este paso (p. ej. si no se va a usar Airflow)
#   AIRFLOW_VERSION     → por defecto 3.1.8 (coincide con requirements-airflow.txt)
if [ "${AIRFLOW_SKIP:-0}" != "1" ]; then
    AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.1.8}"
    PY_VER="$("$VENV/bin/python" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VER}.txt"
    log "Instalando Airflow ${AIRFLOW_VERSION} (Python ${PY_VER}) con constraints oficiales..."
    if "$VENV/bin/pip" install -r "$ROOT/0_infra/requirements-airflow.txt" \
           --constraint "$CONSTRAINTS_URL"; then
        ok "Airflow ${AIRFLOW_VERSION} instalado en $VENV"
    else
        warn "Fallo instalando Airflow. Puedes reintentar manualmente o usar AIRFLOW_SKIP=1."
    fi
else
    warn "AIRFLOW_SKIP=1 → Airflow NO instalado en este venv"
fi

# 3) Preparar .env si no existe
if [ ! -f "$ROOT/.env" ]; then
    if [ -f "$ROOT/.env.example" ]; then
        cp "$ROOT/.env.example" "$ROOT/.env"
        ok ".env creado a partir de .env.example"
        warn "Edita $ROOT/.env (p. ej. STEAM_API_KEY, KAFKA_BOOTSTRAP, CASSANDRA_HOST)"
    else
        warn ".env.example no existe, saltando"
    fi
else
    ok ".env ya existe"
fi

# 4) Resumen
echo ""
log "Venv listo para usar:"
echo "    source $VENV/bin/activate"
echo ""
log "Paquetes principales instalados:"
"$VENV/bin/pip" list 2>/dev/null | grep -iE 'kafka|requests|fastapi|uvicorn|cassandra|dotenv|pydantic|pandas|scikit|kaggle|kagglehub|numpy' | sed 's/^/    /'
