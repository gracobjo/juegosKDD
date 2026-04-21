#!/usr/bin/env python3
"""
Producer de interacciones con user_ids REALES del dataset.

Lee una muestra de user_ids / juegos de Hive (via Parquet en HDFS) y emite
eventos user-game continuamente a `gaming.user.interactions`. Sirve para
demostrar que el Speed Layer actualiza recomendaciones en vivo para usuarios
que ya existen en el modelo ALS.

Uso:
    python synthetic_interactions_producer.py \
        [--rate 20]                 # eventos/segundo
        [--users 500]               # subset de users reales a reciclar
        [--games 500]               # subset de games reales a reciclar
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import subprocess
import sys
import time
from typing import List

from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP, TOPIC_USER_INTERACTIONS, USER_AGENT  # noqa: F401

logging.basicConfig(level=logging.INFO, format="%(asctime)s [synth-interactions] %(message)s")
log = logging.getLogger(__name__)

USER_MAP_HDFS = "hdfs:///user/hive/warehouse/gaming_recommender.db/user_index_map"
GAME_MAP_HDFS = "hdfs:///user/hive/warehouse/gaming_recommender.db/game_index_map"


def _hdfs_cat_parquet_sample(path: str, n_rows: int, col: str) -> List[str]:
    """
    Pequeño helper para leer un Parquet en HDFS sin arrancar Spark.
    Copia el directorio a local via `hdfs dfs -get`, usa pyarrow y lo borra.
    """
    import tempfile
    import pyarrow.parquet as pq

    tmp = tempfile.mkdtemp(prefix="hio_")
    try:
        subprocess.run(
            ["hdfs", "dfs", "-get", path, tmp + "/data"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        table = pq.read_table(tmp + "/data", columns=[col])
        values = table.column(col).to_pylist()
        random.shuffle(values)
        return [str(v) for v in values[:n_rows]]
    finally:
        subprocess.run(["rm", "-rf", tmp], check=False)


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v else None,
        linger_ms=100,
        acks="all",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=20.0, help="eventos por segundo")
    parser.add_argument("--users", type=int, default=500, help="subset de users reales")
    parser.add_argument("--games", type=int, default=500, help="subset de games reales")
    args = parser.parse_args()

    log.info("Muestreando %d users y %d games reales desde HDFS...", args.users, args.games)
    users = _hdfs_cat_parquet_sample(USER_MAP_HDFS, args.users, "user_id")
    games = _hdfs_cat_parquet_sample(GAME_MAP_HDFS, args.games, "game_title")
    if not users or not games:
        log.error("No pude muestrear users/games reales (mapas vacios?)")
        return 1
    log.info("OK: %d users x %d games", len(users), len(games))

    producer = build_producer()
    rng = random.Random()

    stop = False

    def handler(signum, frame):  # noqa: ARG001
        nonlocal stop
        log.info("Senal %s recibida, cerrando...", signum)
        stop = True

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    delay = 1.0 / max(args.rate, 0.1)
    sent = 0
    t_report = time.time()

    log.info(
        "Produciendo a topic=%s rate=%.1f ev/s (Ctrl+C para salir)",
        TOPIC_USER_INTERACTIONS,
        args.rate,
    )
    while not stop:
        event = {
            "user_id": rng.choice(users),
            "game_title": rng.choice(games),
            "event_type": rng.choices(
                ["play", "purchase", "review"], weights=[0.7, 0.2, 0.1]
            )[0],
            "hours": round(min(200.0, rng.lognormvariate(1.8, 1.2)), 2),
            "recommended": rng.random() < 0.65,
            "ts": int(time.time() * 1000),
        }
        producer.send(TOPIC_USER_INTERACTIONS, value=event, key=event["user_id"])
        sent += 1
        if time.time() - t_report >= 10:
            log.info("  emitidos=%d (last user=%s game=%s)", sent, event["user_id"], event["game_title"])
            t_report = time.time()
        time.sleep(delay)

    producer.flush()
    producer.close()
    log.info("Cerrado limpio. Total=%d eventos", sent)
    return 0


if __name__ == "__main__":
    sys.exit(main())
