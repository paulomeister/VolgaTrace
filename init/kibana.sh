#!/bin/sh

echo "Esperando Elasticsearch..."

until curl -s http://elasticsearch:9200 >/dev/null; do
  sleep 5
done

echo "Esperando Kibana..."

until curl -s http://kibana:5601/api/status | grep -q '"level":"available"'; do
  sleep 5
done

echo "Kibana listo"

echo "Aplicando index template..."

curl -X PUT "http://elasticsearch:9200/_index_template/volgatrace-template" \
-H "Content-Type: application/json" \
-d @/template.json

# =========================
# 1. DATA VIEWS (CREAR SOLO SI NO EXISTEN)
# =========================

echo "Creando Data Views..."

EXISTS=$(curl -s -H "kbn-xsrf: true" \
http://kibana:5601/api/data_views | grep transactions-\* || true)

if [ -z "$EXISTS" ]; then
  curl -X POST "http://kibana:5601/api/data_views/data_view" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "data_view": {
      "name": "transactions-*",
      "title": "transactions-*",
      "timeFieldName": "event_timestamp"
    }
  }'
fi

ALERTS_EXISTS=$(curl -s -H "kbn-xsrf: true" \
http://kibana:5601/api/data_views | grep alerts || true)

if [ -z "$ALERTS_EXISTS" ]; then
  curl -X POST "http://kibana:5601/api/data_views/data_view" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "data_view": {
      "name": "alerts",
      "title": "alerts",
      "timeFieldName": "triggered_at"
    }
  }'
fi

# =========================
# 2. IMPORT DASHBOARD
# =========================

echo "Importando dashboard..."

curl -X POST "http://kibana:5601/api/saved_objects/_import?overwrite=true" \
-H "kbn-xsrf: true" \
--form file=@/dashboard/export.ndjson

echo "Kibana bootstrap completado"
