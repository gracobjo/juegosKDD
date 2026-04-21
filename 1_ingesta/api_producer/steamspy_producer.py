#!/usr/bin/env python3
"""
SteamSpy API (sin key) → resumen por appid → Kafka gaming.events.raw
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP, POLL_INTERVAL_SEC, TOPIC_RAW, STEAMSPY_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# AppIDs populares para polling (CS2, Dota2, TF2, PUBG, etc.)
DEFAULT_APPIDS = [730, 570, 440, 578080, 271590, 1174180, 1245620, 1091500]


def fetch_app(appid: int):
    r = requests.get(STEAMSPY_URL, params={"request": "appdetails", "appid": appid}, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    log.info("SteamSpy producer → %s topic=%s", KAFKA_BOOTSTRAP, TOPIC_RAW)
    while True:
        ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        for appid in DEFAULT_APPIDS:
            try:
                data = fetch_app(appid)
                name = data.get("name") if isinstance(data, dict) else None
                owners = data.get("owners") if isinstance(data, dict) else None
                evt = {
                    "source": "steamspy",
                    "ts": ts,
                    "appid": str(appid),
                    "game": name,
                    "owners_range": owners,
                    "average_forever": data.get("average_forever") if isinstance(data, dict) else None,
                }
                producer.send(TOPIC_RAW, value=evt)
            except Exception as e:
                log.warning("SteamSpy appid=%s: %s", appid, e)
        producer.flush()
        log.info("Ciclo SteamSpy completado (%s appids)", len(DEFAULT_APPIDS))
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
