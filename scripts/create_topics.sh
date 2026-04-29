#!/usr/bin/env bash
# Crea los 4 topics del proyecto (R2.1).
# El contenedor 'kafka' tiene que estar corriendo antes de ejecutar esto.

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9094}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION="${REPLICATION:-1}"
RETENTION_MS="${RETENTION_MS:-86400000}"   # 24h

TOPICS=(
  "transactions-online:${RETENTION_MS}"
  "transactions-pos:${RETENTION_MS}"
  "alerts:${RETENTION_MS}"
  "transactions-dlq:${RETENTION_MS}"
)

echo ">> Verificando que el contenedor '${KAFKA_CONTAINER}' este corriendo..."
if ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
  echo "ERROR: contenedor '${KAFKA_CONTAINER}' no encontrado." >&2
  echo "       Ejecuta primero: docker compose up -d" >&2
  exit 1
fi

echo ">> Creando topics en ${BOOTSTRAP}..."
for entry in "${TOPICS[@]}"; do
  topic="${entry%%:*}"
  retention="${entry##*:}"

  echo ""
  echo "   -> ${topic} (particiones=${PARTITIONS}, retencion=${retention}ms)"

  docker exec "${KAFKA_CONTAINER}" kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}" \
    --config "retention.ms=${retention}" \
    --config "cleanup.policy=delete"
done

echo ""
echo ">> Topics existentes en el cluster:"
docker exec "${KAFKA_CONTAINER}" kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --list

echo ""
echo ">> Detalle del topic 'transactions-online':"
docker exec "${KAFKA_CONTAINER}" kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --describe \
  --topic transactions-online

echo ""
echo "OK. Los 4 topics estan listos."