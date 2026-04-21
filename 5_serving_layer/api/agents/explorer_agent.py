"""
Agente Explorador - valida estado de los datasets Kaggle y HDFS/Hive.
Endpoint: GET /api/agent/explorer
"""
from __future__ import annotations

import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from ._claude import call_claude, llm_available

ROOT = Path(__file__).resolve().parent.parent.parent.parent
RAW = ROOT / "0_data" / "raw"


def _file_stats(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False}
    return {
        "exists": True,
        "size_mb": round(path.stat().st_size / 1e6, 2),
        "rows_approx": _count_lines(path),
        "modified": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
    }


def _count_lines(path: Path, cap: int = 500_000) -> int:
    """Cuenta lineas hasta un tope (para archivos grandes)."""
    n = 0
    try:
        with open(path, "rb") as f:
            for _ in f:
                n += 1
                if n >= cap:
                    return cap
    except Exception:
        return 0
    return n


def run_explorer_agent() -> dict[str, Any]:
    behaviors = _file_stats(RAW / "steam_games_behaviors.csv")
    reviews = _file_stats(RAW / "steam_reviews.csv")
    is_synthetic = (RAW / "_SYNTHETIC").exists()

    # Hive status via spark-sql cli (best effort, no lo llamamos: dejamos placeholder)
    result = {
        "ts": datetime.utcnow().isoformat(),
        "datasets": {
            "steam_games_behaviors.csv": behaviors,
            "steam_reviews.csv": reviews,
        },
        "is_synthetic": is_synthetic,
        "data_dir": str(RAW),
    }

    # Diagnostico con IA (o heuristico)
    context = (
        f"behaviors_exists={behaviors.get('exists')} size={behaviors.get('size_mb')}MB "
        f"rows={behaviors.get('rows_approx')} | "
        f"reviews_exists={reviews.get('exists')} size={reviews.get('size_mb')}MB "
        f"rows={reviews.get('rows_approx')} | synthetic={is_synthetic}"
    )
    if llm_available():
        diag = call_claude(
            f"""Eres un agente explorador del pipeline KDD de recomendacion.
Estado de los datasets:
{context}

Di en 2-3 frases en espanol si los datos son suficientes para entrenar el modelo
hibrido (ALS + TF-IDF) y, si son sinteticos, recuerda al usuario que puede
configurar ~/.kaggle/kaggle.json para obtener datos reales.""",
            max_tokens=200,
        )
    else:
        if not behaviors.get("exists") or not reviews.get("exists"):
            diag = "FALTAN datasets. Ejecuta: python 0_data/kaggle/download_datasets.py"
        elif is_synthetic:
            diag = (
                "Datos SINTETICOS generados localmente (~"
                f"{behaviors.get('rows_approx',0):,} behaviors, {reviews.get('rows_approx',0):,} reviews). "
                "Suficiente para entrenar. Configura ~/.kaggle/kaggle.json para reemplazar por datos reales."
            )
        else:
            diag = (
                f"Datasets OK: behaviors={behaviors.get('rows_approx',0):,} filas, "
                f"reviews={reviews.get('rows_approx',0):,} filas. Listos para KDD."
            )

    result["diagnosis"] = diag
    result["llm_available"] = llm_available()
    return result
