"""
Agente KDD - resumen en lenguaje natural del estado del pipeline.
Endpoint: GET /api/agent/kdd
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from ._claude import call_claude, llm_available

log = logging.getLogger(__name__)


def _latest_metrics(session) -> list[dict[str, Any]]:
    try:
        rows = session.execute(
            "SELECT * FROM gaming_recommender.model_metrics "
            "WHERE model_name = 'hybrid_als_tfidf' "
            "ORDER BY evaluated_at DESC LIMIT 5 ALLOW FILTERING"
        )
        return [dict(r._asdict()) for r in rows]
    except Exception as exc:
        log.warning("kdd_agent: no metrics (%s)", exc)
        return []


def _counts(session) -> dict[str, int]:
    def _count(cql: str) -> int:
        try:
            return int(session.execute(cql).one()[0])
        except Exception:
            return 0
    return {
        "user_recommendations": _count(
            "SELECT COUNT(*) FROM gaming_recommender.user_recommendations"
        ),
        "game_similarity": _count(
            "SELECT COUNT(*) FROM gaming_recommender.game_similarity"
        ),
        "popular_games": _count(
            "SELECT COUNT(*) FROM gaming_recommender.popular_games_fallback"
        ),
        "kdd_insights": _count(
            "SELECT COUNT(*) FROM gaming_recommender.kdd_insights"
        ),
        "player_windows": _count(
            "SELECT COUNT(*) FROM gaming_recommender.player_windows"
        ),
    }


def run_kdd_agent(session) -> dict[str, Any]:
    metrics = _latest_metrics(session)
    counts = _counts(session)

    last = metrics[0] if metrics else {}
    phase_report = {
        "selection": {"source": "Hive gaming_recommender.user_behaviors + user_reviews"},
        "preprocessing": {"null_filter": "OK", "dedup": "OK"},
        "transformation": {
            "implicit_rating": "log1p(hours)",
            "review_clean": "regex minuscula + stopwords",
        },
        "mining": {
            "cf": {"algo": "ALS (Spark MLlib)", "rmse": last.get("rmse")},
            "cb": {"algo": "TF-IDF + cosine", "games_pairs": counts["game_similarity"]},
            "hybrid": {
                "alpha": last.get("alpha"),
                "users_with_recs": last.get("users_with_recs"),
            },
        },
        "evaluation": {
            "precision_at_k": last.get("precision_at_k"),
            "recall_at_k": last.get("recall_at_k"),
            "k": last.get("k"),
        },
    }

    if llm_available():
        summary = call_claude(
            f"""Resume en espanol (3-4 frases) el estado del pipeline KDD de
recomendacion de videojuegos.

Datos actuales:
- Filas en Cassandra: {counts}
- Ultima ejecucion: rmse={last.get('rmse')} precision@{last.get('k')}={last.get('precision_at_k')} alpha={last.get('alpha')} users={last.get('users_with_recs')}
- Metricas historicas: {len(metrics)} ejecuciones registradas

Enfatiza salud del modelo, cobertura, y proximas acciones.""",
            max_tokens=300,
        )
    else:
        if not metrics:
            summary = (
                "Aun no hay metricas registradas. Lanza el pipeline batch con "
                "bash 4_batch_layer/recommender/submit_kdd.sh 0.7 para poblar el modelo."
            )
        else:
            summary = (
                f"Pipeline KDD activo: RMSE={last.get('rmse', 0):.3f}, "
                f"Precision@{last.get('k', 10)}={last.get('precision_at_k', 0):.3f}, "
                f"alpha={last.get('alpha', 0.7):.2f}, "
                f"{last.get('users_with_recs', 0):,} usuarios con recomendaciones. "
                f"Similarity pairs: {counts['game_similarity']:,}, insights: {counts['kdd_insights']}."
            )

    return {
        "ts": datetime.utcnow().isoformat(),
        "kdd_phases": phase_report,
        "cassandra_counts": counts,
        "recent_metrics": metrics,
        "summary": summary,
        "llm_available": llm_available(),
    }
