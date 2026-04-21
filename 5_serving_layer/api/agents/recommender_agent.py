"""
Agente Recomendador - devuelve top-K recomendaciones con explicaciones
generadas en lenguaje natural (Claude API o fallback heuristico).
Endpoint: GET /api/agent/recommend/{user_id}
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from ._claude import call_claude, llm_available

log = logging.getLogger(__name__)


def _get_recommendations(session, user_id: str, limit: int = 5) -> list[dict[str, Any]]:
    try:
        rows = session.execute(
            "SELECT recommended_game, hybrid_score, cf_component, cb_component, "
            "recommendation_rank, alpha_used, source "
            "FROM gaming_recommender.user_recommendations "
            "WHERE user_id = %s LIMIT %s",
            (user_id, limit),
        )
        return [dict(r._asdict()) for r in rows]
    except Exception as exc:
        log.warning("recommender_agent: no recs para %s (%s)", user_id, exc)
        return []


def _get_history(session, user_id: str, limit: int = 10) -> list[dict[str, Any]]:
    try:
        rows = session.execute(
            "SELECT game_title, hours_played FROM gaming_recommender.user_history_cache "
            "WHERE user_id = %s LIMIT %s",
            (user_id, limit),
        )
        return [{"game": r.game_title, "hours": float(r.hours_played)} for r in rows]
    except Exception:
        return []


def _get_similar(session, game: str, limit: int = 5) -> list[dict[str, Any]]:
    try:
        rows = session.execute(
            "SELECT game_b, similarity FROM gaming_recommender.game_similarity "
            "WHERE game_a = %s LIMIT %s",
            (game, limit),
        )
        return [{"game": r.game_b, "similarity": float(r.similarity)} for r in rows]
    except Exception:
        return []


def _get_popular(session, limit: int = 10) -> list[dict[str, Any]]:
    try:
        rows = session.execute(
            "SELECT recommended_game, hybrid_score, total_hours, n_players "
            "FROM gaming_recommender.popular_games_fallback LIMIT %s",
            (limit,),
        )
        return [
            {
                "game": r.recommended_game,
                "hybrid_score": float(r.hybrid_score or 0.0),
                "total_hours": float(r.total_hours or 0.0),
                "n_players": int(r.n_players or 0),
                "explanation": "Top global (cold-start)",
            }
            for r in rows
        ]
    except Exception:
        return []


def _heuristic_explanation(
    game: str,
    cf: float,
    cb: float,
    history: list[dict[str, Any]],
    similar: list[dict[str, Any]],
) -> str:
    parts = [f"Te recomendamos '{game}'"]
    if cf > cb:
        parts.append(
            f"porque jugadores con gustos parecidos (CF={cf:.2f}) lo han valorado muy alto."
        )
    else:
        parts.append(
            f"porque encaja en tu estilo de juego (CB={cb:.2f})."
        )
    if similar:
        names = ", ".join(s["game"] for s in similar[:2])
        parts.append(f"Es similar a: {names}.")
    if history:
        names = ", ".join(h["game"] for h in history[:2])
        parts.append(f"Basado en tu historial con {names}.")
    return " ".join(parts)


def run_recommender_agent(session, user_id: str, top_k: int = 5) -> dict[str, Any]:
    recs = _get_recommendations(session, user_id, limit=top_k)
    history = _get_history(session, user_id)

    if not recs:
        popular = _get_popular(session, limit=top_k)
        return {
            "user_id": user_id,
            "cold_start": True,
            "recommendations": popular,
            "message": "Usuario sin historial conocido, mostrando top global.",
            "llm_available": llm_available(),
            "ts": datetime.utcnow().isoformat(),
        }

    enriched = []
    for r in recs:
        game = r["recommended_game"]
        cf = float(r.get("cf_component") or 0.0)
        cb = float(r.get("cb_component") or 0.0)
        similar = _get_similar(session, game)

        explanation = ""
        if llm_available():
            explanation = call_claude(
                f"""Eres un agente de recomendacion de videojuegos hibrido
(ALS + TF-IDF). Genera en espanol (3 frases como maximo) una explicacion
personalizada y concreta sobre por que recomendar "{game}" al usuario {user_id}.

Contexto:
- Historial top: {history[:3]}
- Score CF (jugadores similares): {cf:.3f}
- Score CB (contenido): {cb:.3f}
- Juegos similares por contenido: {[s['game'] for s in similar]}
- Peso alpha usado: {r.get('alpha_used')}

Menciona 1 juego del historial (si existe) y 1 juego similar.""",
                max_tokens=220,
            )

        if not explanation:
            explanation = _heuristic_explanation(game, cf, cb, history, similar)

        enriched.append({
            "game": game,
            "hybrid_score": float(r.get("hybrid_score") or 0.0),
            "cf_component": cf,
            "cb_component": cb,
            "rank": int(r.get("recommendation_rank") or 0),
            "alpha_used": float(r.get("alpha_used") or 0.0),
            "source": r.get("source") or "batch",
            "similar_games": similar,
            "explanation": explanation,
        })

    return {
        "user_id": user_id,
        "cold_start": False,
        "recommendations": enriched,
        "history_preview": history[:5],
        "model": {
            "type": "hybrid",
            "cf": "ALS (Spark MLlib)",
            "cb": "TF-IDF cosine similarity",
            "alpha_default": 0.7,
        },
        "llm_available": llm_available(),
        "ts": datetime.utcnow().isoformat(),
    }
