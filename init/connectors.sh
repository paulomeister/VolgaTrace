#!/bin/sh

echo "Esperando Kafka Connect..."

until curl -s http://kafka-connect:8083/connectors; do
  sleep 5
done

echo "Kafka Connect listo"

echo "Recreando connector Elasticsearch..."

curl -s -X DELETE http://kafka-connect:8083/connectors/elasticsearch-results-sink >/dev/null || true

curl -X POST http://kafka-connect:8083/connectors \
-H "Content-Type: application/json" \
-d '{
  "name": "elasticsearch-results-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "1",

    "topics": "transactions-aggregated,alerts",

    "connection.url": "http://elasticsearch:9200",

    "key.ignore": "true",
    "schema.ignore": "true",

    "type.name": "_doc",
    "behavior.on.malformed.documents": "IGNORE",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "transactions-dlq",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }
}'

echo ""
echo "Connector elasticsearch-results-sink creado para topics: transactions-aggregated, alerts"
