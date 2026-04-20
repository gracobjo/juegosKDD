#!/bin/bash
# ============================================================================
# setup_venv.sh — Crea un venv Python aislado para este proyecto
# ----------------------------------------------------------------------------
# Crea ~/juegosKDD/venv, instala requirements.txt y prepara un .env si no
# existe (partiendo de .env.example). No toca otros venvs del sistema.
# ============================================================================

set -u

ROOT="/home/hadoop/juegosKDD"
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
ok "Dependencias instaladas"

# 3) Preparar .env si no existe
if [ ! -f "$ROOT/.env" ]; then
    if [ -f "$ROOT/.env.example" ]; then
        cp "$ROOT/.env.example" "$ROOT/.env"
        ok ".env creado a partir de .env.example"
        warn "Edita $ROOT/.env si necesitas añadir STEAM_API_KEY o ANTHROPIC_API_KEY"
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
"$VENV/bin/pip" list 2>/dev/null | grep -iE 'kafka|requests|fastapi|uvicorn|cassandra|dotenv|pydantic' | sed 's/^/    /'
