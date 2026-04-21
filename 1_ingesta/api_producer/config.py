"""Configuración compartida para productores API → Kafka."""

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = os.getenv("GAMING_RAW_TOPIC", "gaming.events.raw")
TOPIC_INTERACTIONS = os.getenv("GAMING_INTERACTIONS_TOPIC", "gaming.user.interactions")

FREETOGAME_URL = "https://www.freetogame.com/api/games"
STEAMSPY_URL = "https://steamspy.com/api.php"

POLL_INTERVAL_SEC = int(os.getenv("API_PRODUCER_INTERVAL", "120"))
