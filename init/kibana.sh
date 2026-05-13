#!/bin/sh

echo "Esperando Kibana..."

until curl -s http://kibana:5601/api/status | grep -q "available"
do
  echo "Kibana aún no está lista..."
  sleep 10
done

echo "Kibana lista"

echo "Importando dashboard..."

curl -X POST "http://kibana:5601/api/saved_objects/_import?overwrite=true" \
-H "kbn-xsrf: true" \
--form file=@/dashboard/export.ndjson

echo "Dashboard importado correctamente"
