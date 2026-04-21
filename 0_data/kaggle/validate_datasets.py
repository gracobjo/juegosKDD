#!/usr/bin/env python3
"""
Valida presencia, columnas mínimas y volumen de los CSV de Kaggle en 0_data/raw/.
Solo biblioteca estándar (sin pandas) para poder ejecutarlo con python3 del sistema.

Uso: python3 0_data/kaggle/validate_datasets.py
"""

import csv
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
RAW = ROOT / "0_data" / "raw"

CHECKS = [
    {
        "path": RAW / "steam_games_behaviors.csv",
        "rows_min": 180_000,
        "header": False,
        "min_cols": 4,
    },
    {
        "path": RAW / "steam_reviews.csv",
        "rows_min": 400_000,
        "header": True,
        "required_cols": [
            "username",
            "product_id",
            "product_title",
            "review_text",
            "recommended",
            "hours",
        ],
    },
]


def count_lines(path: Path) -> int:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return max(0, sum(1 for _ in f) - 1)


def main() -> int:
    ok_all = True
    for spec in CHECKS:
        p = spec["path"]
        if not p.exists():
            log.error("Falta archivo: %s", p)
            ok_all = False
            continue
        rows = count_lines(p)
        log.info("%s → %s filas", p.name, f"{rows:,}")
        if rows < spec["rows_min"]:
            log.warning("Menos filas de lo esperado (%s)", f"{spec['rows_min']:,}")

        try:
            if spec.get("header"):
                with open(p, "r", encoding="utf-8", errors="replace", newline="") as f:
                    dr = csv.DictReader(f)
                    names = dr.fieldnames or []
                log.info("Columnas encontradas: %s", names)
                missing = set(spec["required_cols"]) - set(names)
                if missing:
                    log.error("Faltan columnas: %s", missing)
                    ok_all = False
                else:
                    log.info("Columnas OK (reviews)")
            else:
                with open(p, "r", encoding="utf-8", errors="replace", newline="") as f:
                    r = csv.reader(f)
                    first = next(r, None)
                if not first or len(first) < spec["min_cols"]:
                    log.error(
                        "Se esperaban al menos %s columnas en la primera fila: %s",
                        spec["min_cols"],
                        first,
                    )
                    ok_all = False
                else:
                    log.info("Muestra behaviors (primera fila): %s", first[: spec["min_cols"]])
        except Exception as e:
            log.error("Error leyendo %s: %s", p, e)
            ok_all = False

    if not ok_all:
        log.error(
            "Sin datos válidos en 0_data/raw/. "
            "Ejecuta: python3 0_data/kaggle/download_datasets.py (Kaggle) "
            "o copia los CSV; detalle en 0_data/raw/README.txt"
        )
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(main())
