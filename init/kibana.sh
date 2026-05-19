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

# =========================
# 1. DATA VIEW (CREAR SOLO SI NO EXISTE)
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

# =========================
# 2. IMPORT DASHBOARD
# =========================

echo "Importando dashboard..."

curl -X POST "http://kibana:5601/api/saved_objects/_import?overwrite=true" \
-H "kbn-xsrf: true" \
--form file=@/dashboard/export.ndjson

echo "Kibana bootstrap completado"