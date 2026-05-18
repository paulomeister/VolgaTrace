# VolgaTrace
Inspired by Europe’s most powerful river by discharge, VolgaTrace is a real-time analytics platform that uses Apache Kafka for event ingestion and Apache Flink for stateful stream processing, enabling continuous anomaly detection across financial transactions.

## Prerequisites

Before running the project, make sure you have installed:

- Git
- Docker Engine or Docker Desktop
- Docker Compose v2
- At least 8 GB of RAM available for Docker
- Internet connection to download Docker images and dependencies

Python is not required to run the full system because the producers run inside Docker containers. You only need Python if you want to run producers or consumers manually from your machine.

## Installation And Execution

1. Clone the repository:

```bash
git clone https://github.com/paulomeister/VolgaTrace.git
cd VolgaTrace/
```

2. Create the environment variables file:

```bash
cp .env.template .env
```
Or you can create the file manually and copy the variables from `.env.template`.

3. Review or adjust `.env`:

```env
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_UI_PORT=8080
PRODUCER_RATE=100
```

`PRODUCER_RATE` defines the target rate per producer. With `100`, the system attempts to generate approximately 100 events/second in `producer-online` and 100 events/second in `producer-pos`.

4. Start the full stack:

```bash
docker compose up -d --build
```

This command starts Kafka, Flink, Kafka Connect, Elasticsearch, Kibana, Kafka UI, the producers, and the Flink jobs. Everything runs in detached mode. If you do not want detached mode, remove the **-d** flag from the previous command.

5. Verify that the services are active:

```bash
docker compose ps
```

The main services that should remain running are:

- `kafka`
- `kafka-ui`
- `flink-jobmanager`
- `flink-taskmanager`
- `kafka-connect`
- `elasticsearch`
- `kibana`
- `producer-online`
- `producer-pos`

Some services are initialization jobs, so it is normal for them to finish with status `Exited (0)`, for example `kafka-init`, `dashboard-init`, `connect-init`, `flink-aggregation-job`, and `flink-anomalies-job`.

## UIs And Dashboards

When the stack is running, you can open:

- Kibana: <http://localhost:5601>
- Main dashboard: <http://localhost:5601/app/dashboards#/view/f6b39b24-592b-481f-adb6-b0350771de64>
- Kafka UI: <http://localhost:8080>
- Flink UI: <http://localhost:8081>
- Elasticsearch API: <http://localhost:9200>
- Kafka Connect API: <http://localhost:8083/connectors>

In Kibana, the imported dashboard is named `VolgaTrace Streaming Dashboard`.

## What You Should See

In Kafka UI, these topics should exist:

- `transactions-online`
- `transactions-pos`
- `transactions-aggregated`
- `alerts`
- `transactions-dlq`

In Flink UI, you should see two running jobs:

- Aggregation job with a 1-minute tumbling window
- Anomaly detection job

In Elasticsearch, these indices should be created:

- `transactions-aggregated`
- `alerts`

In Kibana, the dashboard shows:

- Processed transactions counter
- Time series of transactions by window
- Maximum amount distribution
- Heatmap by channel and merchant category
- Alerts grouped by type

## Restart From Scratch

To stop everything without deleting data:

```bash
docker compose down
```

To stop everything and delete persisted Kafka and Elasticsearch data:

```bash
docker compose down -v --remove-orphans
```

Then you can start the stack again with:

```bash
docker compose up -d --build
```
