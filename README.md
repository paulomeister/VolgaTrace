# VolgaTrace
Inspired by Europe’s most powerful river by discharge, VolgaTrace is a real-time analytics platform that uses Apache Kafka for event ingestion and Apache Flink for stateful stream processing, enabling continuous anomaly detection across financial transactions.

## Requisitos Previos

Antes de ejecutar el proyecto, asegúrate de tener instalado:

- Git
- Docker Engine o Docker Desktop
- Docker Compose v2
- Al menos 8 GB de RAM disponibles para Docker
- Conexión a internet para descargar imágenes Docker y dependencias

Python no es obligatorio para correr el sistema completo, porque los productores se ejecutan dentro de contenedores Docker. Solo necesitas Python si quieres ejecutar productores o consumidores manualmente desde tu máquina.

## Instalación Y Ejecución

1. Clona el repositorio:

```bash
git clone https://github.com/paulomeister/VolgaTrace.git
cd VolgaTrace/
```

2. Crea el archivo de variables de entorno:

```bash
cp .env.template .env
```
O puedes crear el archivo a mano y copiar las variables del archivo .env.template.

3. Revisa o ajusta `.env`:

```env
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_UI_PORT=8080
PRODUCER_RATE=100
```

`PRODUCER_RATE` define la tasa objetivo por productor. Con `100`, el sistema intenta generar aproximadamente 100 eventos/segundo en `producer-online` y 100 eventos/segundo en `producer-pos`.

4. Levanta el stack completo:

```bash
docker compose up -d --build
```

Este comando levanta Kafka, Flink, Kafka Connect, Elasticsearch, Kibana, Kafka UI, los productores y los jobs de Flink. Todo en modo detached. En caso de no quererlo de este modo quita la bandera **-d** del comando anterior.

5. Verifica que los servicios estén activos:

```bash
docker compose ps
```

Los servicios principales que deben quedar arriba son:

- `kafka`
- `kafka-ui`
- `flink-jobmanager`
- `flink-taskmanager`
- `kafka-connect`
- `elasticsearch`
- `kibana`
- `producer-online`
- `producer-pos`

Algunos servicios son de inicialización y es normal que terminen en estado `Exited (0)`, por ejemplo `kafka-init`, `dashboard-init`, `connect-init`, `flink-aggregation-job` y `flink-anomalies-job`.

## UIs Y Dashboards

Cuando el stack esté corriendo, puedes entrar a:

- Kibana: <http://localhost:5601>
- Dashboard principal: <http://localhost:5601/app/dashboards#/view/f6b39b24-592b-481f-adb6-b0350771de64>
- Kafka UI: <http://localhost:8080>
- Flink UI: <http://localhost:8081>
- Elasticsearch API: <http://localhost:9200>
- Kafka Connect API: <http://localhost:8083/connectors>

En Kibana, el dashboard importado se llama `VolgaTrace Streaming Dashboard`.

## Qué Debes Ver

En Kafka UI deben existir estos topics:

- `transactions-online`
- `transactions-pos`
- `transactions-aggregated`
- `alerts`
- `transactions-dlq`

En Flink UI deben verse dos jobs en ejecución:

- Job de agregaciones con ventana tumbling de 1 minuto
- Job de detección de anomalías

En Elasticsearch deben crearse estos índices:

- `transactions-aggregated`
- `alerts`

En Kibana, el dashboard muestra:

- Contador de transacciones procesadas
- Serie de tiempo de transacciones por ventana
- Distribución de montos máximos
- Heatmap por canal y categoría de comercio
- Alertas agrupadas por tipo

## Reiniciar Desde Cero

Para detener todo sin borrar datos:

```bash
docker compose down
```

Para detener todo y borrar la data persistida de Kafka y Elasticsearch:

```bash
docker compose down -v --remove-orphans
```

Después puedes levantar nuevamente con:

```bash
docker compose up -d --build
```
