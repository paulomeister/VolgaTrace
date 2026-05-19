# Dashboard y Elasticsearch

El dashboard se importa automaticamente al levantar el stack con Docker Compose.

## URLs

- Kibana: <http://localhost:5601>
- Dashboard directo: <http://localhost:5601/app/dashboards#/view/f6b39b24-592b-481f-adb6-b0350771de64>
- Kafka UI: <http://localhost:8080>
- Flink UI: <http://localhost:8081>

## Flujo hacia Elasticsearch

Elasticsearch debe persistir resultados procesados, no eventos crudos de entrada.

Kafka Connect crea el connector `elasticsearch-results-sink` y consume:

- `transactions-aggregated`: resultados del job Flink SQL con ventana tumbling de 1 minuto.
- `alerts`: alertas emitidas por el job Flink de anomalias.

Indices esperados en Elasticsearch:

- `transactions-aggregated`
- `alerts`

Data views creados en Kibana:

- `transactions-*`, con `event_timestamp` como campo temporal.
- `alerts`, con `triggered_at` como campo temporal.

## Importacion manual opcional

Si necesitas reimportar el dashboard manualmente:

1. Abre Kibana.
2. Ve a `Stack Management` -> `Saved Objects`.
3. Selecciona `Import`.
4. Usa `dashboard/export.ndjson`.
