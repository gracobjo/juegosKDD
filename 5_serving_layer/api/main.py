#!/usr/bin/env python3
"""
FastAPI — Serving Layer (gaming_kdd + gaming_recommender).
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional

from cassandra.cluster import Cluster
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from agents.heuristic_agents import run_monitor_agent, run_recommender_agent

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(title="KDD Gaming API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
KS_ANALYTICS = os.getenv("CASSANDRA_KEYSPACE", "gaming_kdd")
KS_REC = os.getenv("CASSANDRA_REC_KEYSPACE", "gaming_recommender")

cluster = Cluster([CASSANDRA_HOST])
session_analytics = cluster.connect(KS_ANALYTICS)
session_rec = cluster.connect(KS_REC)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "ts": datetime.utcnow().isoformat(),
        "version": "2.0",
        "keyspaces": [KS_ANALYTICS, KS_REC],
    }


# ── Steam / catálogo (gaming_kdd) ───────────────────────────────────────────


@app.get("/api/realtime")
def get_realtime(limit: int = Query(20, le=100)):
    games_rows = session_analytics.execute(
        "SELECT DISTINCT game FROM player_windows LIMIT 20"
    )
    result = []
    for row in games_rows:
        windows = session_analytics.execute(
            "SELECT * FROM player_windows WHERE game = %s LIMIT %s",
            (row.game, limit),
        )
        for w in windows:
            result.append(
                {
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
                }
            )
    result.sort(key=lambda x: x["window_start"] or "", reverse=True)
    return result[:limit]


@app.get("/api/historical")
def get_historical(days: int = Query(7, le=30)):
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = session_analytics.execute("SELECT * FROM game_stats_daily")
    result = [
        {
            "appid": r.appid,
            "dt": r.dt,
            "game": r.game,
            "genre": r.genre,
            "avg_players": r.avg_players,
            "max_players": r.max_players,
            "avg_review_score": r.avg_review_score,
            "avg_health_score": r.avg_health_score,
            "growth_rate": r.growth_rate,
            "player_rank": r.player_rank,
            "total_snapshots": r.total_snapshots,
        }
        for r in rows
        if r.dt and r.dt >= since
    ]
    result.sort(key=lambda x: (x["dt"], x["player_rank"] or 99))
    return result


@app.get("/api/insights")
def get_insights(severity: Optional[str] = None, limit: int = Query(20, le=100)):
    if severity:
        rows = session_analytics.execute(
            "SELECT * FROM kdd_insights WHERE severity = %s ALLOW FILTERING",
            (severity,),
        )
    else:
        rows = session_analytics.execute("SELECT * FROM kdd_insights LIMIT %s", (limit,))

    result = [
        {
            "insight_id": str(r.insight_id),
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "game": r.game,
            "appid": r.appid,
            "insight_type": r.insight_type,
            "message": r.message,
            "severity": r.severity,
            "metric_value": r.metric_value,
            "metric_name": r.metric_name,
            "layer": r.layer,
        }
        for r in rows
    ]
    result.sort(key=lambda x: x["created_at"] or "", reverse=True)
    return result[:limit]


@app.get("/api/kdd/summary")
def get_kdd_summary():
    raw_count = session_analytics.execute("SELECT COUNT(*) FROM player_windows").one()[0]
    insights_count = session_analytics.execute("SELECT COUNT(*) FROM kdd_insights").one()[0]
    stats_count = session_analytics.execute("SELECT COUNT(*) FROM game_stats_daily").one()[0]
    try:
        rec_count = session_rec.execute(
            "SELECT COUNT(*) FROM user_recommendations"
        ).one()[0]
    except Exception:
        rec_count = 0
    return {
        "pipeline_version": "2.0",
        "layers": {
            "speed": {"records": raw_count, "table": "player_windows"},
            "batch": {"records": stats_count, "table": "game_stats_daily"},
            "serving": {"insights": insights_count, "table": "kdd_insights"},
            "recommender": {"records": rec_count, "table": "user_recommendations"},
        },
        "kdd_stages": [
            "selection",
            "preprocessing",
            "transformation",
            "mining",
            "evaluation",
        ],
        "last_updated": datetime.utcnow().isoformat(),
    }


# ── Recomendador híbrido (gaming_recommender) ────────────────────────────────


def _popular_fallback(limit: int):
    rows = session_rec.execute(
        "SELECT recommended_game, hybrid_score FROM popular_games_fallback LIMIT %s",
        (limit,),
    )
    return [
        {
            "recommended_game": r.recommended_game,
            "hybrid_score": r.hybrid_score,
            "cf_component": 0.0,
            "cb_component": 0.0,
            "recommendation_rank": i + 1,
        }
        for i, r in enumerate(rows)
    ]


@app.get("/api/recommendations/{user_id}")
def get_recommendations(user_id: str, limit: int = Query(10, le=50)):
    rows = session_rec.execute(
        "SELECT recommended_game, hybrid_score, cf_component, cb_component, "
        "recommendation_rank FROM user_recommendations WHERE user_id = %s LIMIT %s",
        (user_id, limit),
    )
    out = [
        {
            "recommended_game": r.recommended_game,
            "hybrid_score": r.hybrid_score,
            "cf_component": r.cf_component,
            "cb_component": r.cb_component,
            "recommendation_rank": r.recommendation_rank,
        }
        for r in rows
    ]
    return out or _popular_fallback(limit)


@app.get("/api/similar/{game_title}")
def get_similar_games(game_title: str, limit: int = Query(10, le=20)):
    rows = session_rec.execute(
        "SELECT game_b, similarity FROM game_similarity WHERE game_a = %s LIMIT %s",
        (game_title, limit),
    )
    return [{"game": r.game_b, "similarity": round(float(r.similarity), 4)} for r in rows]


@app.get("/api/recommender/realtime")
def get_recommender_realtime(limit: int = Query(20, le=100)):
    rows = session_rec.execute(
        "SELECT user_id, window_start, events_in_batch, avg_engagement, avg_hours "
        "FROM player_windows LIMIT %s ALLOW FILTERING",
        (limit,),
    )
    res = []
    for r in rows:
        res.append(
            {
                "user_id": r.user_id,
                "window_start": r.window_start.isoformat() if r.window_start else None,
                "events_in_batch": r.events_in_batch,
                "avg_engagement": float(r.avg_engagement or 0),
                "avg_hours": float(r.avg_hours or 0),
            }
        )
    res.sort(key=lambda x: x["window_start"] or "", reverse=True)
    return res[:limit]


@app.get("/api/recommender/insights")
def get_recommender_insights(severity: Optional[str] = None, limit: int = Query(20, le=100)):
    if severity:
        rows = session_rec.execute(
            "SELECT * FROM kdd_insights WHERE severity = %s ALLOW FILTERING LIMIT %s",
            (severity, limit),
        )
    else:
        rows = session_rec.execute("SELECT * FROM kdd_insights LIMIT %s", (limit,))
    out = []
    for r in rows:
        out.append(
            {
                "insight_id": str(r.insight_id),
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "insight_type": r.insight_type,
                "severity": r.severity,
                "message": r.message,
                "layer": r.layer,
            }
        )
    return out


@app.get("/api/metrics")
def get_model_metrics():
    rows = session_rec.execute("SELECT * FROM model_metrics LIMIT %s ALLOW FILTERING", (40,))
    items = []
    for r in rows:
        items.append(
            {
                "run_id": str(r.run_id),
                "evaluated_at": r.evaluated_at.isoformat() if r.evaluated_at else None,
                "dt": r.dt,
                "rmse": float(r.rmse or 0),
                "precision_at_k": float(r.precision_at_k or 0),
                "recall_at_k": float(r.recall_at_k or 0),
                "k": r.k,
                "alpha": float(r.alpha or 0),
                "users_with_recs": int(r.users_with_recs or 0),
                "model_path": r.model_path,
            }
        )
    items.sort(key=lambda x: x["evaluated_at"] or "", reverse=True)
    return items


@app.get("/api/agent/recommend/{user_id}")
def agent_recommend(user_id: str):
    """Explicaciones heurísticas (sin LLM)."""
    return run_recommender_agent(user_id)


@app.get("/api/agent/monitor")
def agent_monitor():
    """Drift + disparo opcional de DAG Airflow (si está configurado)."""
    return run_monitor_agent()


@app.post("/api/agent/interaction")
def record_interaction(
    user_id: str = Query(..., description="Identificador de usuario"),
    game_title: str = Query(..., description="Título del juego"),
    event_type: str = Query(..., description="play | purchase | review"),
    hours: float = Query(0.0),
    recommended: bool = Query(False),
):
    from kafka import KafkaProducer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    event = {
        "user_id": user_id,
        "game_title": game_title,
        "event_type": event_type,
        "hours": hours,
        "recommended": recommended,
        "ts": int(time.time() * 1000),
    }
    producer.send("gaming.user.interactions", value=event)
    producer.flush()
    producer.close()
    return {"status": "ok", "event": event}


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run("main:app", host=host, port=port, reload=True)
