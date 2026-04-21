#!/usr/bin/env python3
"""
Descarga automática de datasets Kaggle (Steam behaviors + reviews).

Preferencia: **kagglehub** (dataset_download). Respaldo: CLI `kaggle datasets download`.

Credenciales (cualquiera):
  · KAGGLE_API_TOKEN en entorno o .env (recomendado para kagglehub)
  · ~/.kaggle/kaggle.json
  · KAGGLE_USERNAME + KAGGLE_KEY en .env (se puede generar kaggle.json si falta)
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = ROOT / "0_data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)
TMP_HUB = RAW_DIR / "tmp_hub"


def _load_dotenv(*, override: bool = False) -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(ROOT / ".env", override=override)
    except ImportError:
        pass


DATASETS = [
    {
        "name": "tamber/steam-video-games",
        "files": ["steam-200k.csv"],
        "target": "steam_games_behaviors.csv",
        "desc": "200K interacciones usuario-juego",
        "rows_min": 180_000,
        "columns": ["user_id", "game_title", "behavior_name", "value"],
    },
    {
        "name": "andrewmvd/steam-reviews",
        "files": ["steam_reviews.csv", "dataset.csv", "SteamReviews.csv"],
        "target": "steam_reviews.csv",
        "desc": "Reviews Steam (andrewmvd) → CSV unificado para Hive",
        "rows_min": 400_000,
        "normalize": "steam_reviews_loader",
        "columns": [
            "username",
            "product_id",
            "product_title",
            "review_text",
            "recommended",
            "hours",
        ],
    },
]


def check_kaggle_credentials() -> None:
    """
    Carga .env del repo. Si en el shell hay KAGGLE_* definidas pero vacías,
    python-dotenv no las sobrescribe (override=False); se reintenta con override=True.
    """
    env_path = ROOT / ".env"
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_json = kaggle_dir / "kaggle.json"

    for attempt, override in enumerate((False, True)):
        if attempt == 1 and env_path.exists():
            log.info(
                "Releyendo %s con override=True (p. ej. variables Kaggle vacías en el entorno).",
                env_path,
            )
        _load_dotenv(override=override)

        api_token = os.environ.get("KAGGLE_API_TOKEN", "").strip()
        user = os.environ.get("KAGGLE_USERNAME", "").strip()
        key = os.environ.get("KAGGLE_KEY", "").strip()

        if api_token:
            log.info("Autenticación: KAGGLE_API_TOKEN (kagglehub / API)")
            return

        if user and key and not kaggle_json.exists():
            kaggle_dir.mkdir(parents=True, exist_ok=True)
            kaggle_json.write_text(
                json.dumps({"username": user, "key": key}),
                encoding="utf-8",
            )
            os.chmod(kaggle_json, 0o600)
            log.info("Creado ~/.kaggle/kaggle.json a partir de KAGGLE_USERNAME / KAGGLE_KEY")

        if kaggle_json.exists():
            log.info("Autenticación: %s", kaggle_json)
            return

    log.error("No hay credenciales Kaggle.")
    log.error("  A) KAGGLE_API_TOKEN en %s (recomendado para kagglehub)", env_path)
    log.error("  B) ~/.kaggle/kaggle.json — https://www.kaggle.com/settings → API")
    log.error("  C) KAGGLE_USERNAME + KAGGLE_KEY en .env (genera kaggle.json si no existe)")
    if env_path.exists():
        log.error(
            "Existe %s: comprueba que al menos una opción tenga valor real (no solo la plantilla vacía).",
            env_path,
        )
    else:
        log.error("No existe %s — copia desde .env.example y rellena Kaggle.", env_path)
    if (
        "KAGGLE_API_TOKEN" in os.environ
        and not os.environ.get("KAGGLE_API_TOKEN", "").strip()
    ):
        log.error(
            "KAGGLE_API_TOKEN está en el entorno pero vacío; prueba: unset KAGGLE_API_TOKEN"
        )
    sys.exit(1)


def ensure_kagglehub() -> bool:
    try:
        import kagglehub  # noqa: F401

        return True
    except ImportError:
        log.info("Instalando kagglehub…")
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "kagglehub", "-q"],
            check=True,
        )
        return True


def ensure_kaggle_cli() -> None:
    try:
        import kaggle  # noqa: F401
    except ImportError:
        log.info("Instalando kaggle (CLI)…")
        subprocess.run([sys.executable, "-m", "pip", "install", "kaggle", "-q"], check=True)


def _locate_downloaded_file(root: Path, filename: str) -> Path | None:
    if not root.exists():
        return None
    if root.is_file() and root.name == filename:
        return root
    for p in root.rglob(filename):
        if p.is_file():
            return p
    return None


def _find_source_csv(root: Path, filenames: list[str]) -> Path | None:
    for name in filenames:
        found = _locate_downloaded_file(root, name)
        if found is not None:
            return found
    return None


LOADER_REVIEW_COLUMNS = [
    "username",
    "product_id",
    "product_title",
    "review_text",
    "recommended",
    "hours",
]


def _lower_key_map(fieldnames: list[str] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for n in fieldnames or []:
        if n is None:
            continue
        k = (n or "").strip().lower()
        if k and k not in out:
            out[k] = n
    return out


def _pick_col(lk: dict[str, str], *candidates: str) -> str | None:
    for c in candidates:
        if c.lower() in lk:
            return lk[c.lower()]
    return None


def _to_recommended_str(val) -> str:
    if val is None or val == "":
        return "False"
    s = str(val).strip().lower()
    if s in ("1", "true", "yes", "recommended", "positive", "t"):
        return "True"
    if s in ("0", "false", "no", "f"):
        return "False"
    try:
        return "True" if float(str(val).strip()) != 0.0 else "False"
    except ValueError:
        return "False"


def _to_hours_float(val) -> float:
    if val is None or val == "":
        return 0.0
    try:
        return float(str(val).replace(",", "."))
    except ValueError:
        return 0.0


def normalize_reviews_for_loader(src: Path, dest: Path) -> None:
    """
    Convierte variantes (p. ej. andrewmvd/steam-reviews) al esquema del loader Hive.
    Salida: username, product_id, product_title, review_text, recommended (True/False), hours
    """
    import csv

    with open(src, "r", encoding="utf-8", errors="replace", newline="") as fin:
        reader = csv.DictReader(fin)
        lk = _lower_key_map(reader.fieldnames)
        need = {c.lower() for c in LOADER_REVIEW_COLUMNS}
        if need <= set(lk.keys()):
            shutil.copy2(src, dest)
            log.info("Reviews: CSV ya compatible con el loader; copia directa.")
            return

        c_user = _pick_col(
            lk,
            "username",
            "author",
            "author_steamid",
            "steamid",
            "user_id",
            "player_steamid",
            "reviewer",
        )
        c_pid = _pick_col(lk, "product_id", "app_id", "appid", "game_id", "steam_app_id")
        c_title = _pick_col(
            lk,
            "product_title",
            "title",
            "app_name",
            "game_name",
            "game",
            "name",
        )
        c_text = _pick_col(lk, "review_text", "review", "text", "content", "body")
        c_rec = _pick_col(
            lk,
            "recommended",
            "recommendation",
            "voted_up",
            "is_recommended",
            "positive",
        )
        c_hours = _pick_col(
            lk,
            "hours",
            "hour",
            "hourplay",
            "hourplayed",
            "playtime_forever",
            "playtime",
            "author.playtime_forever",
        )
        if not c_text:
            raise ValueError(
                "No hay columna de texto (review/review_text/…). "
                f"Cabeceras: {list(reader.fieldnames or [])}"
            )

    with open(src, "r", encoding="utf-8", errors="replace", newline="") as fin:
        reader = csv.DictReader(fin)
        with open(dest, "w", encoding="utf-8", newline="") as fout:
            writer = csv.DictWriter(fout, fieldnames=LOADER_REVIEW_COLUMNS)
            writer.writeheader()
            for row in reader:
                uid = (row.get(c_user) or "").strip() if c_user else ""
                pid = (row.get(c_pid) or "").strip() if c_pid else ""
                title = (row.get(c_title) or "").strip() if c_title else ""
                if not title and pid:
                    title = f"app_{pid}"
                text = (row.get(c_text) or "").strip() if c_text else ""
                rec_raw = row.get(c_rec) if c_rec else None
                rec = _to_recommended_str(rec_raw)
                hrs = _to_hours_float(row.get(c_hours)) if c_hours else 0.0
                writer.writerow(
                    {
                        "username": uid or "unknown",
                        "product_id": pid or "0",
                        "product_title": title or "unknown",
                        "review_text": text,
                        "recommended": rec,
                        "hours": f"{hrs}",
                    }
                )


def _finalize_dataset_file(src: Path, dataset: dict) -> None:
    """Mueve o normaliza el CSV descargado hacia 0_data/raw/<target>."""
    target_path = RAW_DIR / dataset["target"]
    if dataset.get("normalize") == "steam_reviews_loader":
        normalize_reviews_for_loader(src, target_path)
        if src.resolve() != target_path.resolve():
            try:
                src.unlink()
            except OSError:
                pass
        return
    if src.resolve() != target_path.resolve():
        shutil.move(str(src), str(target_path))


def download_dataset_hub(dataset: dict) -> bool:
    """Descarga con kagglehub; deja el CSV final en 0_data/raw/."""
    import kagglehub

    target_path = RAW_DIR / dataset["target"]
    if target_path.exists():
        log.info(
            "Ya existe: %s (%.1f MB)",
            dataset["target"],
            target_path.stat().st_size / 1e6,
        )
        return True

    # kagglehub exige output_dir vacío: un subcarpeta por dataset (no compartir tmp_hub entre CSV).
    staging = TMP_HUB / dataset["name"].replace("/", "__")
    if staging.exists():
        shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True, exist_ok=True)
    log.info("kagglehub.dataset_download(%r) → %s", dataset["name"], dataset["desc"])

    try:
        local_path = kagglehub.dataset_download(
            dataset["name"],
            output_dir=str(staging),
        )
    except TypeError:
        local_path = kagglehub.dataset_download(dataset["name"])

    root = Path(local_path)
    if root.is_file():
        root = root.parent

    src = _find_source_csv(root, dataset["files"])
    if src is None:
        log.error(
            "kagglehub: ningún archivo de %s bajo %s (listar manualmente el dataset)",
            dataset["files"],
            root,
        )
        return False
    try:
        _finalize_dataset_file(src, dataset)
    except Exception as e:
        log.error("kagglehub: error al preparar %s: %s", dataset["target"], e)
        return False
    log.info(
        "Guardado: %s (%.1f MB)",
        target_path,
        target_path.stat().st_size / 1e6,
    )
    shutil.rmtree(staging, ignore_errors=True)
    return True


def download_dataset_cli(dataset: dict) -> bool:
    """Respaldo: CLI kaggle datasets download."""
    target_path = RAW_DIR / dataset["target"]
    if target_path.exists():
        log.info(
            "Ya existe: %s (%.1f MB)",
            dataset["target"],
            target_path.stat().st_size / 1e6,
        )
        return True

    log.info("CLI kaggle → %s — %s", dataset["name"], dataset["desc"])
    tmp_dir = RAW_DIR / "tmp_download"
    tmp_dir.mkdir(exist_ok=True)

    result = subprocess.run(
        [
            "kaggle",
            "datasets",
            "download",
            "-d",
            dataset["name"],
            "-p",
            str(tmp_dir),
            "--unzip",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        log.error("Error descargando %s: %s", dataset["name"], result.stderr)
        return False

    src = _find_source_csv(tmp_dir, dataset["files"])
    if src is None:
        log.error("CLI: ningún archivo de %s en %s", dataset["files"], tmp_dir)
        return False
    try:
        _finalize_dataset_file(src, dataset)
    except Exception as e:
        log.error("CLI: error al preparar %s: %s", dataset["target"], e)
        return False
    log.info(
        "Guardado: %s (%.1f MB)",
        target_path,
        target_path.stat().st_size / 1e6,
    )
    return True


def download_dataset(dataset: dict, use_hub: bool) -> bool:
    if use_hub:
        try:
            return download_dataset_hub(dataset)
        except Exception as e:
            log.warning("kagglehub falló (%s); reintentando con CLI kaggle…", e)
            ensure_kaggle_cli()
            return download_dataset_cli(dataset)
    ensure_kaggle_cli()
    return download_dataset_cli(dataset)


def validate_dataset(dataset: dict) -> bool:
    import csv

    target_path = RAW_DIR / dataset["target"]
    if not target_path.exists():
        log.error("No existe: %s", target_path)
        return False

    log.info("Validando: %s", dataset["target"])
    try:
        with open(target_path, "r", encoding="utf-8", errors="replace") as f:
            row_count = max(0, sum(1 for _ in f) - 1)
        log.info("Total filas: %s", f"{row_count:,}")

        if row_count < dataset["rows_min"]:
            log.warning(
                "Pocas filas: %s < %s esperadas",
                f"{row_count:,}",
                f"{dataset['rows_min']:,}",
            )

        with open(target_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            if dataset["target"] == "steam_reviews.csv":
                cols = csv.DictReader(f).fieldnames or []
                log.info("Columnas encontradas: %s", list(cols))
                missing = set(dataset["columns"]) - set(cols)
                if missing:
                    log.error("Faltan columnas: %s", missing)
                    return False
            else:
                row = next(csv.reader(f), None)
                if not row or len(row) < 4:
                    log.error("CSV behaviors: se esperaban 4 columnas, fila=%s", row)
                    return False
                log.info("Primera fila behaviors: %s", row[:4])

        log.info("Dataset válido: %s", dataset["target"])
        return True
    except Exception as e:
        log.error("Error validando %s: %s", dataset["target"], e)
        return False


def main() -> int:
    log.info("=" * 60)
    log.info("Descarga de datasets Kaggle — Recomendador híbrido")
    log.info("=" * 60)

    check_kaggle_credentials()

    use_hub = os.environ.get("USE_KAGGLE_CLI_ONLY", "").strip().lower() not in ("1", "true", "yes")
    if use_hub:
        ensure_kagglehub()
    else:
        log.info("USE_KAGGLE_CLI_ONLY: solo CLI kaggle")
        ensure_kaggle_cli()

    if use_hub:
        try:
            import kagglehub  # noqa: F401
        except ImportError:
            use_hub = False

    results: dict[str, str] = {}
    for ds in DATASETS:
        if download_dataset(ds, use_hub=use_hub):
            results[ds["target"]] = "OK" if validate_dataset(ds) else "INVALID"
        else:
            results[ds["target"]] = "FAILED"

    log.info("\n=== RESUMEN ===")
    for filename, status in results.items():
        icon = "✓" if status == "OK" else "✗"
        log.info("  %s %s: %s", icon, filename, status)

    if all(s == "OK" for s in results.values()):
        log.info(
            "\nListo. Siguiente (sin YARN, modo local): "
            "spark-submit --master local[*] 1_ingesta/kaggle_to_hive/loader.py"
        )
        return 0
    log.error("\nAlgunos datasets fallaron.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
