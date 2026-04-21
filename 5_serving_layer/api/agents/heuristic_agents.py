"""
Agentes sin LLM: explicaciones y monitorización basadas en reglas y datos de Cassandra.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List

import requests
from cassandra.cluster import Cluster

log = logging.getLogger(__name__)

CASS = os.getenv("CASSANDRA_HOST", "localhost")
KS = "gaming_recommender"


def _session():
    return Cluster([CASS]).connect(KS)


def _row_dict(r) -> Dict[str, Any]:
    try:
        return r._asdict()
    except Exception:
        return {name: r[name] for name in r._fields}


def explain_rule_based(
    user_id: str,
    game: str,
    cf_score: float,
    cb_score: float,
    history_games: List[str],
    alpha: float,
) -> str:
    hist = ", ".join(history_games[:5]) if history_games else "sin historial reciente"
    if cf_score >= cb_score:
        foco = "usuarios con patrones de juego parecidos (componente colaborativo)"
    else:
        foco = "similitud de contenido con juegos que ya has jugado mucho"
    return (
        f"Se recomienda «{game}» porque encaja con tu perfil (α={alpha:.2f}): "
        f"puntuación CF={cf_score:.3f}, CB={cb_score:.3f}. "
        f"Juegos que más pesan en el contexto: {hist}. "
        f"El sistema prioriza hoy más el componente de {foco}."
    )


def run_recommender_agent(user_id: str) -> Dict[str, Any]:
    sess = _session()
    rec_rows = sess.execute(
        "SELECT recommended_game, hybrid_score, cf_component, cb_component "
        "FROM user_recommendations WHERE user_id = %s LIMIT 10",
        (user_id,),
    )
    recs = [_row_dict(r) for r in rec_rows]
    if not recs:
        pop = sess.execute("SELECT recommended_game, hybrid_score FROM popular_games_fallback LIMIT 10")
        popular = [_row_dict(r) for r in pop]
        return {
            "user_id": user_id,
            "recommendations": [
                {
                    "game": p["recommended_game"],
                    "hybrid_score": p.get("hybrid_score", 0),
                    "cf_component": 0.0,
                    "cb_component": 0.0,
                    "explanation": "Cold start: se muestran títulos populares del dataset Steam.",
                }
                for p in popular
            ],
            "cold_start": True,
            "model_info": {
                "type": "hybrid_heuristic",
                "cf": "ALS (Spark MLlib)",
                "cb": "TF-IDF + coseno",
            },
        }

    try:
        hist_rows = sess.execute(
            "SELECT game_title FROM user_history_cache WHERE user_id = %s LIMIT 10",
            (user_id,),
        )
        history = [r.game_title for r in hist_rows]
    except Exception:
        history = []

    enriched = []
    for r in recs[:5]:
        cf_c = float(r.get("cf_component") or 0.0)
        cb_c = float(r.get("cb_component") or 0.0)
        alpha = float(r.get("alpha_used") or 0.7) if "alpha_used" in r else 0.7
        enriched.append(
            {
                "game": r["recommended_game"],
                "hybrid_score": float(r.get("hybrid_score") or 0.0),
                "cf_component": cf_c,
                "cb_component": cb_c,
                "explanation": explain_rule_based(
                    user_id, r["recommended_game"], cf_c, cb_c, history, alpha
                ),
            }
        )

    return {
        "user_id": user_id,
        "recommendations": enriched,
        "cold_start": False,
        "model_info": {
            "type": "hybrid_heuristic",
            "cf": "ALS (Spark MLlib)",
            "cb": "TF-IDF + coseno",
            "layer": "Cassandra gaming_recommender",
        },
    }


def _get_metrics_rows(sess) -> List[Dict[str, Any]]:
    rows = sess.execute("SELECT * FROM model_metrics LIMIT 50 ALLOW FILTERING")
    out = []
    for r in rows:
        d = _row_dict(r)
        for k, v in list(d.items()):
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
        out.append(d)
    out.sort(key=lambda x: x.get("evaluated_at") or "", reverse=True)
    return out


def detect_drift(metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
    if len(metrics) < 2:
        return {"drift_detected": False, "reasons": [], "message": "Métricas insuficientes"}

    latest = metrics[0]
    baseline_rmse = sum(float(m.get("rmse") or 0) for m in metrics[1:]) / max(len(metrics) - 1, 1)
    baseline_prec = sum(float(m.get("precision_at_k") or 0) for m in metrics[1:]) / max(
        len(metrics) - 1, 1
    )

    lr = float(latest.get("rmse") or 0)
    lp = float(latest.get("precision_at_k") or 0)

    reasons = []
    if baseline_rmse > 1e-6 and (lr - baseline_rmse) / baseline_rmse > 0.15:
        reasons.append(f"RMSE subió respecto a la media histórica ({lr:.4f} vs {baseline_rmse:.4f})")
    if baseline_prec > 1e-6 and (baseline_prec - lp) / baseline_prec > 0.10:
        reasons.append("Precision@K cayó >10% frente a la media histórica")

    return {
        "drift_detected": bool(reasons),
        "reasons": reasons,
        "latest_rmse": lr,
        "baseline_rmse": baseline_rmse,
        "latest_prec": lp,
        "baseline_prec": baseline_prec,
    }


def trigger_retraining(reason: str) -> bool:
    url = os.getenv("AIRFLOW_URL", "http://localhost:8080")
    user = os.getenv("AIRFLOW_USER", "airflow")
    pwd = os.getenv("AIRFLOW_PASS", "airflow")
    dag_id = os.getenv("AIRFLOW_RECOMMENDER_DAG_ID", "gaming_recommender_kdd_pipeline")
    try:
        resp = requests.post(
            f"{url}/api/v1/dags/{dag_id}/dagRuns",
            json={"conf": {"triggered_by": "monitor_agent", "reason": reason}},
            auth=(user, pwd),
            timeout=8,
        )
        return resp.status_code in (200, 201)
    except Exception as e:
        log.warning("Airflow no disponible: %s", e)
        return False


def run_monitor_agent() -> Dict[str, Any]:
    sess = _session()
    metrics = _get_metrics_rows(sess)
    drift = detect_drift(metrics)

    if drift["drift_detected"]:
        diag = (
            "Se detectó posible degradación del modelo. "
            + " ".join(drift["reasons"])
            + " Conviene revisar datos de entrada o reentrenar."
        )
    else:
        diag = "Métricas estables: no hay señales claras de drift en RMSE/Precision recientes."

    retraining = False
    if drift["drift_detected"]:
        retraining = trigger_retraining("; ".join(drift["reasons"]))

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "drift_detected": drift["drift_detected"],
        "drift_reasons": drift.get("reasons", []),
        "diagnosis": diag,
        "retraining_triggered": retraining,
        "metrics_summary": {
            "latest_rmse": drift.get("latest_rmse", 0),
            "baseline_rmse": drift.get("baseline_rmse", 0),
            "latest_prec": drift.get("latest_prec", 0),
            "baseline_prec": drift.get("baseline_prec", 0),
        },
        "metrics_sample": metrics[:5],
    }
