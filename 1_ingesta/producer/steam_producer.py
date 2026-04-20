#!/usr/bin/env python3
"""
Steam API → Kafka Producer
Ingesta real de datos de Steam + SteamSpy hacia el topic gaming.events.raw.

Modos:
  python steam_producer.py                 # loop continuo (polling cada 60s)
  python steam_producer.py --mode=batch    # ejecuta un único batch y sale
"""

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP,
    PLAYER_TIERS,
    POLL_INTERVAL_S,
    STEAM_API_KEY,
    STEAM_APPS,
    TOPIC_RAW,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ── Inicializar producer Kafka ────────────────────────────────────────────────
def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


# ── Funciones de fetching ─────────────────────────────────────────────────────
def fetch_steam_players(appid: str) -> dict:
    """Obtiene jugadores actuales de Steam. Este endpoint es público y
    funciona sin API key (la key solo se añade si existe)."""
    params = {"appid": appid}
    if STEAM_API_KEY:
        params["key"] = STEAM_API_KEY
    try:
        r = requests.get(
            "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/",
            params=params,
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        return {
            "current_players": data.get("response", {}).get("player_count", 0),
            "steam_ok": True,
        }
    except Exception as e:
        log.warning(f"Steam API error appid={appid}: {e}")
        return {"current_players": 0, "steam_ok": False}


def fetch_steamspy(appid: str) -> dict:
    """Obtiene datos de SteamSpy (sin API key, gratis — respetar 1 req/s)."""
    try:
        r = requests.get(
            "https://steamspy.com/api.php",
            params={"request": "appdetails", "appid": appid},
            timeout=8,
        )
        r.raise_for_status()
        d = r.json()
        return {
            "owners":            d.get("owners", "0 .. 0"),
            "average_playtime":  d.get("average_forever", 0),
            "median_playtime":   d.get("median_forever", 0),
            "positive_reviews":  d.get("positive", 0),
            "negative_reviews":  d.get("negative", 0),
            "price":             int(d.get("price", 0) or 0),
            "genre":             d.get("genre", "") or "",
            "tags":              list((d.get("tags") or {}).keys())[:5],
            "steamspy_ok":       True,
        }
    except Exception as e:
        log.warning(f"SteamSpy error appid={appid}: {e}")
        return {
            "owners": "0 .. 0", "average_playtime": 0,
            "median_playtime": 0, "positive_reviews": 0,
            "negative_reviews": 0, "price": 0,
            "genre": "", "tags": [], "steamspy_ok": False,
        }


def fetch_steam_achievements(appid: str) -> dict:
    """Porcentajes globales de logros (top 5). Endpoint público sin key obligatoria.
    Si no hay key, se llama igual pero algunos juegos pueden devolver 403."""
    params = {"gameid": appid}
    if STEAM_API_KEY:
        params["key"] = STEAM_API_KEY
    try:
        r = requests.get(
            "https://api.steampowered.com/ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2/",
            params=params,
            timeout=8,
        )
        r.raise_for_status()
        achievements = r.json().get("achievementpercentages", {}).get("achievements", [])
        top = sorted(achievements, key=lambda x: x.get("percent", 0), reverse=True)[:5]
        return {
            "top_achievements": [
                {"name": a["name"], "percent": round(a["percent"], 1)}
                for a in top
            ],
            "total_achievements": len(achievements),
        }
    except Exception as e:
        log.debug(f"Achievements unavailable appid={appid}: {e}")
        return {"top_achievements": [], "total_achievements": 0}


# ── Clasificación KDD (fase Transformation embebida en ingesta) ───────────────
def classify_player_tier(count: int) -> str:
    for tier, threshold in PLAYER_TIERS.items():
        if count >= threshold:
            return tier
    return "niche"


def calculate_health_score(steam: dict, spy: dict) -> float:
    """Score de salud del juego 0-100 basado en reviews y actividad."""
    pos = spy.get("positive_reviews", 0)
    neg = spy.get("negative_reviews", 0)
    total_reviews = pos + neg
    review_ratio = (pos / total_reviews) if total_reviews > 0 else 0.5
    player_score = min(steam.get("current_players", 0) / 50_000, 1.0)
    return round((review_ratio * 0.7 + player_score * 0.3) * 100, 1)


# ── Loop principal de ingesta ─────────────────────────────────────────────────
def build_event(appid: str, game_name: str, now: datetime,
                batch_id: str, ts_ms: int) -> dict:
    # Rate-limit amigable con SteamSpy
    time.sleep(1.1)

    steam  = fetch_steam_players(appid)
    spy    = fetch_steamspy(appid)
    achiev = fetch_steam_achievements(appid)

    pos = spy.get("positive_reviews", 0)
    neg = spy.get("negative_reviews", 0)
    review_score = round(pos / max(pos + neg, 1) * 100, 1)

    return {
        # Identificadores
        "event_id":          str(uuid.uuid4()),
        "batch_id":          batch_id,
        "ts":                ts_ms,
        "dt":                now.strftime("%Y-%m-%d"),
        "hour":              now.hour,

        # Dimensiones del juego
        "appid":             appid,
        "game":              game_name,
        "genre":             spy.get("genre", ""),
        "tags":              spy.get("tags", []),
        "price_usd":         spy.get("price", 0) / 100.0,

        # Métricas de actividad (Speed Layer)
        "current_players":   steam.get("current_players", 0),
        "player_tier":       classify_player_tier(steam.get("current_players", 0)),

        # Métricas de engagement (Batch Layer)
        "owners_range":      spy.get("owners", "0 .. 0"),
        "average_playtime":  spy.get("average_playtime", 0),
        "median_playtime":   spy.get("median_playtime", 0),

        # Métricas de calidad
        "positive_reviews":  pos,
        "negative_reviews":  neg,
        "review_score":      review_score,
        "health_score":      calculate_health_score(steam, spy),

        # Logros
        "total_achievements": achiev.get("total_achievements", 0),
        "top_achievements":   achiev.get("top_achievements", []),

        # Metadatos de pipeline
        "source":            "steam+steamspy",
        "event_type":        "player_snapshot",
        "pipeline_version":  "1.0",
        "steam_api_ok":      steam.get("steam_ok", False),
        "steamspy_api_ok":   spy.get("steamspy_ok", False),
    }


def produce_batch(producer: KafkaProducer) -> int:
    """Fetch de todas las apps configuradas y producción a Kafka."""
    now = datetime.now(timezone.utc)
    ts_ms = int(now.timestamp() * 1000)
    batch_id = str(uuid.uuid4())[:8]
    success = 0

    for appid, game_name in STEAM_APPS.items():
        event = build_event(appid, game_name, now, batch_id, ts_ms)
        producer.send(TOPIC_RAW, key=appid, value=event)
        log.info(
            "✓ %-25s | players=%7d | tier=%-8s | health=%5.1f",
            game_name,
            event["current_players"],
            event["player_tier"],
            event["health_score"],
        )
        success += 1

    producer.flush()
    log.info(f"Batch {batch_id} completado: {success}/{len(STEAM_APPS)} apps")
    return success


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["stream", "batch"], default="stream",
                        help="stream = loop continuo; batch = un único batch y sale")
    parser.add_argument("--date", default=None, help="Fecha ISO para modo batch (informativa)")
    parser.add_argument("--output", default=None, help="Ruta HDFS (informativa, solo se usa en Airflow)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  KDD Gaming — Steam API Producer arrancando")
    log.info(f"  Kafka: {KAFKA_BOOTSTRAP} → topic: {TOPIC_RAW}")
    log.info(f"  Apps monitoreadas: {len(STEAM_APPS)}")
    log.info(f"  Modo: {args.mode}")
    log.info("=" * 60)

    producer = build_producer()

    try:
        if args.mode == "batch":
            produce_batch(producer)
            return

        while True:
            try:
                produce_batch(producer)
            except Exception as e:
                log.error(f"Error en batch: {e}", exc_info=True)
            log.info(f"Esperando {POLL_INTERVAL_S}s hasta el siguiente poll...")
            time.sleep(POLL_INTERVAL_S)
    except KeyboardInterrupt:
        log.info("Producer detenido por el usuario.")
    finally:
        producer.close()


if __name__ == "__main__":
    sys.exit(main())
