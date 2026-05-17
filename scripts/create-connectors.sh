#!/bin/sh

echo "Waiting for services..."
sleep 40

echo "Deleting old indexes..."
curl -X DELETE http://elasticsearch:9200/transactions-* || true

echo "Creating Elasticsearch index template..."
curl -X PUT http://elasticsearch:9200/_index_template/transactions_template \
-H "Content-Type: application/json" \
-d @/init/template.json

echo "Creating Elasticsearch sink connector..."

curl -X POST http://kafka-connect:8083/connectors \
-H "Content-Type: application/json" \
-d '{
  "name": "elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "1",
    "topics": "transactions-online",
    "connection.url": "http://elasticsearch:9200",
    "key.ignore": "true",
    "schema.ignore": "true",
    "type.name": "_doc",

    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable": "false"
  }
}'

echo ""
echo "Importing Kibana dashboard..."

curl -X POST "http://kibana:5601/api/saved_objects/_import?overwrite=true" \
-H "kbn-xsrf: true" \
--form file=@/dashboard/export.ndjson

echo ""
echo "Initialization completed."