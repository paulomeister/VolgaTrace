#!/bin/bash

echo "Esperando Kafka Connect..."

sleep 20

curl -X POST http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @/connectors/elasticsearch-sink.json

echo "Connector creado"
