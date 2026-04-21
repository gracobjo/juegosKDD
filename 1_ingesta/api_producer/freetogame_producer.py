#!/usr/bin/env python3
"""
FreeToGame API (sin API key) → eventos JSON → Kafka gaming.events.raw
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

from config import FREETOGAME_URL, KAFKA_BOOTSTRAP, POLL_INTERVAL_SEC, TOPIC_RAW

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def fetch_games():
    r = requests.get(FREETOGAME_URL, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    log.info("FreeToGame producer → %s topic=%s", KAFKA_BOOTSTRAP, TOPIC_RAW)
    while True:
        try:
            games = fetch_games()
            ts = int(datetime.now(timezone.utc).timestamp() * 1000)
            for g in games[:200]:
                evt = {
                    "source": "freetogame",
                    "ts": ts,
                    "game": g.get("title"),
                    "genre": g.get("genre"),
                    "platform": g.get("platform"),
                    "release_date": g.get("release_date"),
                    "id": g.get("id"),
                }
                producer.send(TOPIC_RAW, value=evt)
            producer.flush()
            log.info("Enviados %s eventos FreeToGame", len(games[:200]))
        except Exception as e:
            log.error("Error ciclo FreeToGame: %s", e)
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
