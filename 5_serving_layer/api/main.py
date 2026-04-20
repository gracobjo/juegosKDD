#!/usr/bin/env python3
"""
FastAPI — REST API del Serving Layer
-----------------------------------------------------------------------------
Sirve datos desde Cassandra al dashboard React (puerto 8000).

Arrancar:
  cd 5_serving_layer/api
  pip install -r requirements.txt
  python main.py
Swagger UI: http://localhost:8000/docs
"""

import os
from datetime import datetime, timedelta
from typing import Optional

from cassandra.cluster import Cluster
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

app = FastAPI(title="KDD Gaming API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Conexión a Cassandra ──────────────────────────────────────────────────────
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_KS = os.getenv("CASSANDRA_KEYSPACE", "gaming_kdd")

cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect(CASSANDRA_KS)


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "ts": datetime.utcnow().isoformat()}


@app.get("/api/realtime")
def get_realtime(limit: int = Query(20, le=100)):
    """Últimas ventanas del Speed Layer para el dashboard en tiempo real."""
    games_rows = session.execute(
        "SELECT DISTINCT game FROM player_windows LIMIT 20"
    )
    result = []
    for row in games_rows:
        windows = session.execute(
            "SELECT * FROM player_windows WHERE game = %s LIMIT %s",
            (row.game, limit),
        )
        for w in windows:
            result.append({
                "game":             w.game,
                "appid":            w.appid,
                "window_start":     w.window_start.isoformat() if w.window_start else None,
                "avg_players":      w.avg_players,
                "max_players":      w.max_players,
                "min_players":      w.min_players,
                "avg_review_score": w.avg_review_score,
                "avg_health_score": w.avg_health_score,
                "snapshots":        w.snapshots,
                "player_tier":      w.player_tier,
            })
    result.sort(key=lambda x: x["window_start"] or "", reverse=True)
    return result[:limit]


@app.get("/api/historical")
def get_historical(days: int = Query(7, le=30)):
    """Resumen histórico del Batch Layer (últimos N días)."""
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = session.execute("SELECT * FROM game_stats_daily")
    result = [
        {
            "appid":            r.appid,
            "dt":               r.dt,
            "game":             r.game,
            "genre":            r.genre,
            "avg_players":      r.avg_players,
            "max_players":      r.max_players,
            "avg_review_score": r.avg_review_score,
            "avg_health_score": r.avg_health_score,
            "growth_rate":      r.growth_rate,
            "player_rank":      r.player_rank,
            "total_snapshots":  r.total_snapshots,
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
):
    """Insights KDD generados por Speed y Batch Layer."""
    if severity:
        rows = session.execute(
            "SELECT * FROM kdd_insights WHERE severity = %s ALLOW FILTERING",
            (severity,),
        )
    else:
        rows = session.execute("SELECT * FROM kdd_insights LIMIT %s", (limit,))

    result = [
        {
            "insight_id":   str(r.insight_id),
            "created_at":   r.created_at.isoformat() if r.created_at else None,
            "game":         r.game,
            "appid":        r.appid,
            "insight_type": r.insight_type,
            "message":      r.message,
            "severity":     r.severity,
            "metric_value": r.metric_value,
            "metric_name":  r.metric_name,
            "layer":        r.layer,
        }
        for r in rows
    ]
    result.sort(key=lambda x: x["created_at"] or "", reverse=True)
    return result[:limit]


@app.get("/api/kdd/summary")
def get_kdd_summary():
    """Resumen del estado actual del pipeline KDD."""
    raw_count      = session.execute("SELECT COUNT(*) FROM player_windows").one()[0]
    insights_count = session.execute("SELECT COUNT(*) FROM kdd_insights").one()[0]
    stats_count    = session.execute("SELECT COUNT(*) FROM game_stats_daily").one()[0]

    return {
        "pipeline_version": "1.0",
        "layers": {
            "speed":   {"records": raw_count,       "table": "player_windows"},
            "batch":   {"records": stats_count,     "table": "game_stats_daily"},
            "serving": {"insights": insights_count, "table": "kdd_insights"},
        },
        "kdd_stages": [
            "selection", "preprocessing", "transformation", "mining", "evaluation"
        ],
        "last_updated": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run("main:app", host=host, port=port, reload=True)
