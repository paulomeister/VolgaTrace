#!/bin/sh
set -euo pipefail

echo "============================="
echo " VOLGATRACE BOOTSTRAP START "
echo "============================="

# 1. Elasticsearch ready real check
echo "Esperando Elasticsearch..."
until curl -s http://elasticsearch:9200 >/dev/null; do
  sleep 3
done

echo "Elasticsearch OK"

# 2. Crear template
echo "Creando index template..."
curl -X PUT "http://elasticsearch:9200/_index_template/transactions-template" \
-H "Content-Type: application/json" \
-d @/init/transactions-template.json

# 3. Esperar Kafka Connect
echo "Esperando Kafka Connect..."
until curl -s http://kafka-connect:8083/connectors >/dev/null; do
  sleep 3
done

echo "Kafka Connect OK"

# 4. Crear connectors
echo "Registrando connectors..."
sh /init/connectors.sh

# 5. Esperar datos en ES (CRÍTICO)
echo "Esperando datos en Elasticsearch..."
until curl -s "http://elasticsearch:9200/transactions-*/_count" | grep -q '"count":[1-9]'; do
  sleep 5
done

echo "Datos detectados en ES"

# 6. Kibana ready
echo "Esperando Kibana..."
until curl -s http://kibana:5601/api/status | grep -q '"level":"available"'; do
  sleep 3
done

echo "Kibana OK"

# 7. Crear data view SOLO cuando hay datos
echo "Creando Data View..."

curl -X POST "http://kibana:5601/api/data_views/data_view" \
-H "kbn-xsrf: true" \
-H "Content-Type: application/json" \
-d '{
  "data_view": {
    "name": "transactions-*",
    "title": "transactions-*",
    "timeFieldName": "event_timestamp"
  }
}' || true

# 8. Import dashboard
echo "Importando dashboard..."

curl -X POST "http://kibana:5601/api/saved_objects/_import?overwrite=true" \
-H "kbn-xsrf: true" \
--form file=@/dashboard/export.ndjson

echo "BOOTSTRAP COMPLETO"