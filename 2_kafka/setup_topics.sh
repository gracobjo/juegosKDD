#!/bin/bash
# setup_topics.sh — Crea los topics Kafka necesarios para el pipeline KDD Gaming.
# Uso: bash setup_topics.sh
# Requiere: $KAFKA_HOME apuntando a la instalación (default: /opt/kafka)

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"

if [ ! -x "$KAFKA_HOME/bin/kafka-topics.sh" ]; then
    echo "ERROR: no se encuentra $KAFKA_HOME/bin/kafka-topics.sh"
    echo "Ajusta la variable KAFKA_HOME a tu instalación de Kafka."
    exit 1
fi

echo "=== Creando topics Kafka para KDD Gaming ==="
echo "   KAFKA_HOME : $KAFKA_HOME"
echo "   BOOTSTRAP  : $BOOTSTRAP"
echo ""

# Usamos arrays paralelos (compatible con bash antiguo)
TOPIC_NAMES=(
    "gaming.events.raw"
    "gaming.events.processed"
    "gaming.kdd.insights"
    "gaming.batch.trigger"
    "gaming.user.interactions"
    "gaming.recommendations"
    "gaming.model.updates"
)
TOPIC_PARTITIONS=(
    "3"
    "3"
    "1"
    "1"
    "3"
    "1"
    "1"
)

for i in "${!TOPIC_NAMES[@]}"; do
    TOPIC="${TOPIC_NAMES[$i]}"
    PARTITIONS="${TOPIC_PARTITIONS[$i]}"
    "$KAFKA_HOME/bin/kafka-topics.sh" \
        --create \
        --bootstrap-server "$BOOTSTRAP" \
        --replication-factor 1 \
        --partitions "$PARTITIONS" \
        --topic "$TOPIC" \
        --if-not-exists
    echo "✓ Topic creado: $TOPIC (partitions=$PARTITIONS)"
done

echo ""
echo "=== Topics disponibles ==="
"$KAFKA_HOME/bin/kafka-topics.sh" --list --bootstrap-server "$BOOTSTRAP"

echo ""
echo "=== Setup completado ==="
