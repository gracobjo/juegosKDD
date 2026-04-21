"""
Agente Monitor - detecta drift y (opcionalmente) dispara reentrenamiento en
Airflow. Endpoint: GET /api/agent/monitor
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import requests

from ._claude import call_claude, llm_available

log = logging.getLogger(__name__)

AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8090")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "airflow")
DAG_ID = os.getenv("RECOMMENDER_DAG_ID", "gaming_recommender_batch_pipeline")


def _fetch_metrics(session) -> list[dict[str, Any]]:
    try:
        rows = session.execute(
            "SELECT * FROM gaming_recommender.model_metrics "
            "WHERE model_name = 'hybrid_als_tfidf' "
            "ORDER BY evaluated_at DESC LIMIT 20 ALLOW FILTERING"
        )
        return [dict(r._asdict()) for r in rows]
    except Exception as exc:
        log.warning("monitor: no metrics (%s)", exc)
        return []


def _detect_drift(metrics: list[dict[str, Any]]) -> dict[str, Any]:
    if len(metrics) < 2:
        return {"drift_detected": False, "reason": "historia insuficiente", "n_metrics": len(metrics)}

    latest = metrics[0]
    baseline = metrics[1:]
    avg = lambda k: sum((m.get(k) or 0) for m in baseline) / max(len(baseline), 1)
    base_rmse = avg("rmse")
    base_prec = avg("precision_at_k")

    reasons = []
    if base_rmse > 0:
        rmse_delta = ((latest.get("rmse", 0) or 0) - base_rmse) / base_rmse
        if rmse_delta > 0.15:
            reasons.append(f"RMSE subio {rmse_delta:.1%} ({latest['rmse']:.4f} vs baseline {base_rmse:.4f})")
    if base_prec > 0:
        prec_delta = (base_prec - (latest.get("precision_at_k", 0) or 0)) / base_prec
        if prec_delta > 0.10:
            reasons.append(f"Precision@K bajo {prec_delta:.1%}")

    if (latest.get("users_with_recs") or 0) < 10:
        reasons.append(f"muy pocos usuarios con recomendaciones ({latest.get('users_with_recs')})")

    return {
        "drift_detected": bool(reasons),
        "reasons": reasons,
        "latest_rmse": latest.get("rmse"),
        "baseline_rmse": base_rmse,
        "latest_precision_at_k": latest.get("precision_at_k"),
        "baseline_precision_at_k": base_prec,
        "n_metrics": len(metrics),
    }


def _trigger_airflow(reason: str) -> dict[str, Any]:
    url = f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns"
    try:
        resp = requests.post(
            url,
            json={"conf": {"triggered_by": "monitor_agent", "reason": reason}},
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=10,
        )
        return {
            "triggered": resp.status_code in (200, 201),
            "status_code": resp.status_code,
            "response": resp.text[:300],
        }
    except Exception as exc:
        return {"triggered": False, "error": str(exc)}


def run_monitor_agent(session, auto_trigger: bool = False) -> dict[str, Any]:
    metrics = _fetch_metrics(session)
    drift = _detect_drift(metrics)

    if llm_available():
        diagnosis = call_claude(
            f"""Eres un agente monitor de un sistema KDD + Lambda de recomendacion.
Metricas (mas reciente primero): {metrics[:5]}
Analisis drift: {drift}

En espanol (4 frases maximo): (1) estado del modelo, (2) causa probable del
drift si existe, (3) accion recomendada, (4) si no hay drift, confirmacion.""",
            max_tokens=300,
        )
    else:
        if drift["drift_detected"]:
            diagnosis = (
                "DRIFT detectado: " + "; ".join(drift["reasons"])
                + ". Recomendacion: relanzar el pipeline KDD con los datos mas recientes "
                "(bash 4_batch_layer/recommender/submit_kdd.sh 0.7)."
            )
        elif not metrics:
            diagnosis = "Sin metricas registradas. Ejecuta el pipeline batch para inicializar."
        else:
            diagnosis = (
                f"Modelo estable tras {len(metrics)} ejecuciones. "
                f"RMSE actual={drift.get('latest_rmse', 0):.4f} (baseline={drift.get('baseline_rmse', 0):.4f}). "
                "No hay drift."
            )

    airflow_result = None
    if auto_trigger and drift["drift_detected"]:
        airflow_result = _trigger_airflow("; ".join(drift["reasons"]))

    return {
        "ts": datetime.utcnow().isoformat(),
        "drift": drift,
        "diagnosis": diagnosis,
        "airflow": airflow_result,
        "llm_available": llm_available(),
        "dag_id": DAG_ID,
    }
