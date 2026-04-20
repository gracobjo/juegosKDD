"""Configuración central del Steam → Kafka producer."""

import os
from dotenv import load_dotenv

load_dotenv()

STEAM_API_KEY = os.getenv("STEAM_API_KEY", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = "gaming.events.raw"
POLL_INTERVAL_S = 60  # segundos entre polls a la API

# AppIDs de Steam a monitorear
STEAM_APPS = {
    "730":     "Counter-Strike 2",
    "570":     "Dota 2",
    "1172470": "Apex Legends",
    "1091500": "Cyberpunk 2077",
    "1245620": "Elden Ring",
    "2357570": "Helldivers 2",
    "413150":  "Stardew Valley",
    "1145360": "Hades",
}

# Umbrales KDD para clasificación en tiers (orden importante: de mayor a menor)
PLAYER_TIERS = {
    "massive":  100_000,
    "popular":   10_000,
    "active":     1_000,
    "niche":          0,
}
