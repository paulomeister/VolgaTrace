#!/bin/sh

echo "Esperando Elasticsearch..."

until curl -s http://elasticsearch:9200 >/dev/null; do
  sleep 5
done

echo "Elasticsearch listo"

echo "Creando index template..."

curl -X PUT "http://elasticsearch:9200/_index_template/transactions-template" \
-H "Content-Type: application/json" \
-d @/bootstrap/transactions-template.json

echo "Template aplicado"