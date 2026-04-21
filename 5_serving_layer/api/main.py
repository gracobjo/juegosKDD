#!/usr/bin/env python3
"""
FastAPI - Serving Layer unificado del proyecto KDD Gaming.
-----------------------------------------------------------------------------
Sirve dos keyspaces en paralelo:
  * gaming_kdd           : pipeline original Steam live (snapshots, insights).
  * gaming_recommender   : sistema hibrido CF + CB + agentes IA.

Arrancar:
  cd 5_serving_layer/api
  pip install -r requirements.txt
  python main.py
Docs: http://localhost:8000/docs
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from cassandra.cluster import Cluster
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

# Permitir "from agents.X" aunque no se arranque con cwd en esta carpeta.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from agents.explorer_agent import run_explorer_agent
from agents.kdd_agent import run_kdd_agent
from agents.monitor_agent import run_monitor_agent
from agents.recommender_agent import run_recommender_agent

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("api")

app = FastAPI(
    title="KDD Gaming API",
    version="2.0",
    description=(
        "Serving Layer unificado: pipeline gaming_kdd (Steam live) + "
        "sistema hibrido de recomendacion gaming_recommender."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
    ],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Conexion perezosa a Cassandra ────────────────────────────────────────────
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
KS_KDD = os.getenv("CASSANDRA_KEYSPACE", "gaming_kdd")
KS_REC = "gaming_recommender"

_cluster: Optional[Cluster] = None
_session_kdd = None
_session_rec = None


def _cluster_get() -> Cluster:
    global _cluster
    if _cluster is None:
        _cluster = Cluster([CASSANDRA_HOST])
    return _cluster


def get_session_kdd():
    global _session_kdd
    if _session_kdd is None:
        try:
            _session_kdd = _cluster_get().connect(KS_KDD)
        except Exception as exc:
            log.warning("No se pudo conectar a keyspace %s: %s", KS_KDD, exc)
            raise HTTPException(503, f"Cassandra {KS_KDD} no disponible: {exc}")
    return _session_kdd


def get_session_rec():
    global _session_rec
    if _session_rec is None:
        try:
            _session_rec = _cluster_get().connect(KS_REC)
        except Exception as exc:
            log.warning("No se pudo conectar a keyspace %s: %s", KS_REC, exc)
            raise HTTPException(503, f"Cassandra {KS_REC} no disponible: {exc}")
    return _session_rec


# ── Utilidades ────────────────────────────────────────────────────────────────
def _safe_execute(session, *args, **kwargs):
    try:
        return session.execute(*args, **kwargs)
    except Exception as exc:
        log.warning("Query fallida: %s", exc)
        return []


# ── ENDPOINTS BASICOS ─────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status": "ok",
        "ts": datetime.utcnow().isoformat(),
        "version": "2.0",
        "keyspaces": [KS_KDD, KS_REC],
    }


@app.get("/api/keyspaces")
def keyspaces():
    """Diagnostico: que keyspaces estan disponibles."""
    status = {}
    for ks in (KS_KDD, KS_REC):
        try:
            _cluster_get().connect(ks)
            status[ks] = "available"
        except Exception as exc:
            status[ks] = f"unavailable ({exc})"
    return status


# ── ENDPOINTS DEL PIPELINE ORIGINAL (gaming_kdd) ─────────────────────────────
@app.get("/api/realtime")
def get_realtime(limit: int = Query(20, le=100)):
    """Ultimas ventanas del Speed Layer (Steam live)."""
    session = get_session_kdd()
    games_rows = _safe_execute(
        session, "SELECT DISTINCT game FROM player_windows LIMIT 20"
    )
    result = []
    for row in games_rows:
        windows = _safe_execute(
            session,
            "SELECT * FROM player_windows WHERE game = %s LIMIT %s",
            (row.game, limit),
        )
        for w in windows:
            result.append({
                "game": w.game,
                "appid": w.appid,
                "window_start": w.window_start.isoformat() if w.window_start else None,
                "avg_players": w.avg_players,
                "max_players": w.max_players,
                "min_players": w.min_players,
                "avg_review_score": w.avg_review_score,
                "avg_health_score": w.avg_health_score,
                "snapshots": w.snapshots,
                "player_tier": w.player_tier,
            })
    result.sort(key=lambda x: x["window_start"] or "", reverse=True)
    return result[:limit]


@app.get("/api/historical")
def get_historical(days: int = Query(7, le=30)):
    session = get_session_kdd()
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = _safe_execute(session, "SELECT * FROM game_stats_daily")
    result = [
        {
            "appid": r.appid, "dt": r.dt, "game": r.game, "genre": r.genre,
            "avg_players": r.avg_players, "max_players": r.max_players,
            "avg_review_score": r.avg_review_score, "avg_health_score": r.avg_health_score,
            "growth_rate": r.growth_rate, "player_rank": r.player_rank,
            "total_snapshots": r.total_snapshots,
        }
        for r in rows
        if r.dt and r.dt >= since
    ]
    result.sort(key=lambda x: (x["dt"], x["player_rank"] or 99))
    return result


@app.get("/api/insights")
def get_insights(
    severity: Optional[str] = None,
    limit: int = Query(20, le=100),
    source: str = Query("kdd", pattern="^(kdd|recommender|both)$"),
):
    """Insights del pipeline original (kdd), del recomendador o ambos."""
    data = []
    if source in ("kdd", "both"):
        session = get_session_kdd()
        q = "SELECT * FROM kdd_insights WHERE severity = %s ALLOW FILTERING" if severity else f"SELECT * FROM kdd_insights LIMIT {limit}"
        rows = _safe_execute(session, q, (severity,)) if severity else _safe_execute(session, q)
        for r in rows:
            data.append({
                "insight_id": str(r.insight_id),
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "game": getattr(r, "game", None),
                "appid": getattr(r, "appid", None),
                "insight_type": r.insight_type,
                "message": r.message,
                "severity": r.severity,
                "metric_value": getattr(r, "metric_value", None),
                "metric_name": getattr(r, "metric_name", None),
                "layer": r.layer,
                "source": "gaming_kdd",
            })

    if source in ("recommender", "both"):
        try:
            session = get_session_rec()
            q = "SELECT * FROM kdd_insights WHERE severity = %s ALLOW FILTERING" if severity else f"SELECT * FROM kdd_insights LIMIT {limit}"
            rows = _safe_execute(session, q, (severity,)) if severity else _safe_execute(session, q)
            for r in rows:
                data.append({
                    "insight_id": str(r.insight_id),
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                    "game": getattr(r, "game_title", None),
                    "user_id": getattr(r, "user_id", None),
                    "insight_type": r.insight_type,
                    "message": r.message,
                    "severity": r.severity,
                    "metric_value": getattr(r, "metric_value", None),
                    "metric_name": getattr(r, "metric_name", None),
                    "layer": r.layer,
                    "source": "gaming_recommender",
                })
        except Exception as exc:
            log.warning("insights recommender: %s", exc)

    data.sort(key=lambda x: x["created_at"] or "", reverse=True)
    return data[:limit]


@app.get("/api/kdd/summary")
def get_kdd_summary():
    session = get_session_kdd()
    def _count(tbl: str) -> int:
        try:
            return int(session.execute(f"SELECT COUNT(*) FROM {tbl}").one()[0])
        except Exception:
            return 0
    return {
        "pipeline_version": "2.0",
        "layers": {
            "speed": {"records": _count("player_windows"), "table": "player_windows"},
            "batch": {"records": _count("game_stats_daily"), "table": "game_stats_daily"},
            "serving": {"insights": _count("kdd_insights"), "table": "kdd_insights"},
        },
        "kdd_stages": [
            "selection", "preprocessing", "transformation", "mining", "evaluation"
        ],
        "last_updated": datetime.utcnow().isoformat(),
    }


# ── ENDPOINTS DEL RECOMENDADOR (gaming_recommender) ──────────────────────────
@app.get("/api/recommendations/{user_id}")
def api_get_recommendations(user_id: str, limit: int = Query(10, le=50)):
    """Top-K recomendaciones hibridas para un usuario."""
    session = get_session_rec()
    rows = _safe_execute(
        session,
        "SELECT recommended_game, hybrid_score, cf_component, cb_component, "
        "recommendation_rank, alpha_used, source "
        "FROM user_recommendations WHERE user_id = %s LIMIT %s",
        (user_id, limit),
    )
    recs = [
        {
            "recommended_game": r.recommended_game,
            "hybrid_score": float(r.hybrid_score or 0.0),
            "cf_component": float(r.cf_component or 0.0),
            "cb_component": float(r.cb_component or 0.0),
            "rank": int(r.recommendation_rank or 0),
            "alpha_used": float(r.alpha_used or 0.0),
            "source": r.source,
        }
        for r in rows
    ]
    if not recs:
        return {"user_id": user_id, "cold_start": True, "recommendations": _get_popular_fallback(session, limit)}
    return {"user_id": user_id, "cold_start": False, "recommendations": recs}


def _get_popular_fallback(session, limit: int):
    rows = _safe_execute(
        session,
        "SELECT recommended_game, hybrid_score, total_hours, n_players "
        "FROM popular_games_fallback LIMIT %s",
        (limit,),
    )
    return [
        {
            "recommended_game": r.recommended_game,
            "hybrid_score": float(r.hybrid_score or 0.0),
            "total_hours": float(r.total_hours or 0.0),
            "n_players": int(r.n_players or 0),
            "source": "popular_fallback",
        }
        for r in rows
    ]


@app.get("/api/recommendations/popular")
def api_get_popular(limit: int = Query(10, le=50)):
    session = get_session_rec()
    return _get_popular_fallback(session, limit)


@app.get("/api/similar/{game_title}")
def api_get_similar(game_title: str, limit: int = Query(10, le=20)):
    """Juegos similares por contenido (TF-IDF cosine)."""
    session = get_session_rec()
    rows = _safe_execute(
        session,
        "SELECT game_b, similarity FROM game_similarity "
        "WHERE game_a = %s LIMIT %s",
        (game_title.strip().lower(), limit),
    )
    return [
        {"game": r.game_b, "similarity": round(float(r.similarity or 0.0), 4)}
        for r in rows
    ]


@app.get("/api/recommender/metrics")
def api_get_metrics(limit: int = Query(20, le=100)):
    """Metricas historicas del modelo hibrido."""
    session = get_session_rec()
    rows = _safe_execute(
        session,
        "SELECT * FROM model_metrics WHERE model_name='hybrid_als_tfidf' "
        "ORDER BY evaluated_at DESC LIMIT %s ALLOW FILTERING",
        (limit,),
    )
    return [
        {
            "run_id": str(getattr(r, "run_id", "")),
            "evaluated_at": r.evaluated_at.isoformat() if r.evaluated_at else None,
            "dt": r.dt,
            "rmse": float(getattr(r, "rmse", 0) or 0),
            "precision_at_k": float(getattr(r, "precision_at_k", 0) or 0),
            "recall_at_k": float(getattr(r, "recall_at_k", 0) or 0),
            "k": int(getattr(r, "k", 10) or 10),
            "alpha": float(getattr(r, "alpha", 0) or 0),
            "users_with_recs": int(getattr(r, "users_with_recs", 0) or 0),
            "games_covered": int(getattr(r, "games_covered", 0) or 0),
        }
        for r in rows
    ]


@app.get("/api/recommender/realtime")
def api_realtime_rec(limit: int = Query(20, le=100)):
    """Ventanas del Speed Layer del recomendador (player_windows en gaming_recommender)."""
    session = get_session_rec()
    rows = _safe_execute(
        session, "SELECT * FROM player_windows LIMIT %s", (limit,)
    )
    return [
        {
            "user_id": r.user_id,
            "window_start": r.window_start.isoformat() if r.window_start else None,
            "events_in_batch": int(r.events_in_batch or 0),
            "avg_engagement": float(r.avg_engagement or 0.0),
            "avg_hours": float(r.avg_hours or 0.0),
        }
        for r in rows
    ]


# ── AGENTES IA ────────────────────────────────────────────────────────────────
@app.get("/api/agent/explorer")
def api_agent_explorer():
    """Agente Explorador: estado de datasets y Hive."""
    return run_explorer_agent()


@app.get("/api/agent/kdd")
def api_agent_kdd():
    """Agente KDD: resumen del pipeline completo."""
    try:
        session = get_session_rec()
    except HTTPException:
        session = None
    if session is None:
        return {"error": "Cassandra gaming_recommender no disponible"}
    return run_kdd_agent(session)


@app.get("/api/agent/recommend/{user_id}")
def api_agent_recommend(user_id: str, top_k: int = Query(5, le=20)):
    """Agente Recomendador: top-K con explicaciones."""
    session = get_session_rec()
    return run_recommender_agent(session, user_id, top_k=top_k)


@app.get("/api/agent/monitor")
def api_agent_monitor(auto_trigger: bool = Query(False)):
    """Agente Monitor: drift + (opcional) trigger Airflow."""
    session = get_session_rec()
    return run_monitor_agent(session, auto_trigger=auto_trigger)


# ── INGESTA EN TIEMPO REAL VIA API ────────────────────────────────────────────
class Interaction(BaseModel):
    user_id: str
    game_title: str
    event_type: str = "play"  # play | purchase | review
    hours: float = 0.0
    recommended: bool = False


@app.post("/api/ingest/interaction")
def api_ingest_interaction(ev: Interaction):
    """Publica una interaccion en Kafka gaming.user.interactions."""
    try:
        from kafka import KafkaProducer
        import json as _json
        prod = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            value_serializer=lambda v: _json.dumps(v).encode("utf-8"),
        )
        payload = ev.model_dump()
        payload["ts"] = int(time.time() * 1000)
        prod.send("gaming.user.interactions", value=payload)
        prod.flush()
        prod.close()
        return {"status": "sent", "payload": payload}
    except Exception as exc:
        raise HTTPException(500, f"No se pudo enviar a Kafka: {exc}")


if __name__ == "__main__":
    import uvicorn
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run("main:app", host=host, port=port, reload=True)
