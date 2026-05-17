#!/bin/sh

echo "Esperando Kafka Connect..."

until curl -s http://kafka-connect:8083/connectors >/dev/null; do
  sleep 5
done

echo "Kafka Connect listo"

echo "Registrando connectors..."

curl -X POST http://kafka-connect:8083/connectors \
-H "Content-Type: application/json" \
-d @/bootstrap/transactions-es-sink.json

echo "Connectors creados"