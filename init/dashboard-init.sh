#!/bin/sh

echo "Esperando Elasticsearch..."

until curl -s http://elasticsearch:9200 >/dev/null; do
  sleep 5
done

echo "Creando index template..."

curl -s -X PUT "http://elasticsearch:9200/_index_template/transactions-template" \
  -H "Content-Type: application/json" \
  -d @/init/template.json

echo "Esperando Kibana..."

until curl -s http://kibana:5601/api/status | grep -q '"level":"available"'; do
  sleep 5
done

echo "Eliminando Data View viejo..."

curl -s -X DELETE \
  "http://kibana:5601/api/data_views/data_view/transactions-online*" \
  -H "kbn-xsrf: true"

echo "Creando Data View limpio..."

curl -s -X POST "http://kibana:5601/api/data_views/data_view" \
  -H "Content-Type: application/json" \
  -H "kbn-xsrf: true" \
  -d '{
    "data_view": {
      "name": "transactions-online",
      "title": "transactions-*",
      "timeFieldName": "event_timestamp"
    }
  }'

echo "Importando dashboard..."

curl -s -X POST \
  "http://kibana:5601/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  --form file=@/dashboard/export.ndjson

echo "DONE"