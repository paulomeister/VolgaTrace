# Kafka Module — Real-time Transaction Pipeline

Ingestion layer of the project: Kafka broker in KRaft mode, Python producers
for simulated transactions (online and POS), and inspection tools.

Covers requirements **R2.1** and **R2.2**.

## Requirements

- Docker and Docker Compose v2
- Python 3.10+
- 4 GB of free RAM

## Quick Start

```bash
# 1. Start Kafka + create topics + Kafka UI (single command)
docker compose up -d
```

> **Reproducibility note:** if you pulled changes or cleaned images, use:
> `docker compose up --build -d`

```bash
# 2. Verify everything is healthy (kafka should show "healthy",
#    kafka-init "exited (0)", and kafka-ui "Up")
docker compose ps

# 3. Install Python dependencies
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 4. Run the producers in two separate terminals
python producers/producer_online.py --rate 50
python producers/producer_pos.py    --rate 50
```

Combined total: 100 events/second, meets R2.2.

> **Note:** Topics are created automatically when the stack starts via the
> `kafka-init` service. It is one-shot: it runs, creates the 4 topics with
> 24h retention, and shuts down (state `exited (0)`). It is idempotent, so
> running `docker compose up -d` again won't fail even if the topics already exist.

## Verification

### Kafka UI

Open [http://localhost:8080](http://localhost:8080).

- Under **Topics**: all 4 topics should appear with incoming messages.
- Per topic: rising message counter, retention.ms = 86400000.
- Under **Messages** of a topic: JSON payloads visible.

### Test Consumer (CLI)

```bash
python producers/consumer_test.py --topic transactions-online
python producers/consumer_test.py --topic transactions-pos --from-beginning
```

### Command-line Listing

```bash
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9094 --list
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9094 \
  --describe --topic transactions-online
```

## Structure

```
.
├── docker-compose.yml          # Kafka (KRaft) + kafka-init + Kafka UI
├── producers/
│   ├── schema.py               # Event schema (shared contract)
│   ├── producer_online.py      # Online channel producer
│   ├── producer_pos.py         # POS channel producer
│   └── consumer_test.py        # Local validation (not deliverable)
├── scripts/
│   └── create_topics.sh        # Optional utility (not required)
└── docs/
    └── contract.md             # Contract for the Flink team
```

## Topics

| Topic | Partitions | Retention | Writer | Reader |
|---|---|---|---|---|
| `transactions-online` | 3 | 24 h | producer_online.py | Flink jobs |
| `transactions-pos` | 3 | 24 h | producer_pos.py | Flink jobs |
| `alerts` | 3 | 24 h | Flink anomaly job | Dashboard, notifiers |
| `transactions-dlq` | 3 | 24 h | Flink jobs (errors) | Manual inspection |

To add or modify a topic, edit the `TOPICS` variable of the `kafka-init`
service in `docker-compose.yml`. Format: `name:partitions:retention_ms`.

## Recreating Topics Manually

If you need to recreate topics without stopping Kafka, there are two options:

```bash
# A) Re-run only the init service
docker compose up kafka-init

# B) Use the utility script (same effect)
./scripts/create_topics.sh
```

## Stop Everything

```bash
docker compose down            # stop containers
docker compose down -v         # stop and delete volume (removes messages)
```

## For more details on the message contract see `docs/contract.md`.

## Flink Anomalies Job (Java)

Technical summary of the streaming job implemented in `flink/anomalies-job`:

- Sources Kafka topics `transactions-online` and `transactions-pos` and deserializes into `IngestRecord`.
- Validates records, routes invalid ones to a side output, and publishes them to `transactions-dlq`.
- Uses event-time with bounded out-of-orderness watermarks and idleness handling.
- Keys by `card_id` and detects:
  - High-amount anomalies via `AmountAnomalyFn`.
  - Declined pattern via CEP: `REJECTED` x3 followed by `APPROVED` within 10 minutes.
  - Burst activity using a 1-minute sliding window with 10-second slide.
- Emits `Alert` events to the `alerts` topic.

Execution considerations:

- The anomalies job is auto-submitted by Docker Compose via the `flink-anomalies-job` service.
- The submitter builds the JAR inside the container using Maven and then runs:
  `flink run -c com.streaming.fraud.AnomaliesJob` against the JobManager.
- The JAR path used by the submitter is:
  `flink/anomalies-job/target/anomalies-job-1.0-SNAPSHOT.jar` (inside the container volume).
- Kafka bootstrap servers are resolved from `KAFKA_BOOTSTRAP_SERVERS` (env) or
  `--bootstrap.servers` (CLI), with a default of `localhost:9092`.

## Flink Aggregation Job (SQL)

Execution considerations:

- The SQL aggregation job is auto-submitted by Docker Compose via
  the `flink-aggregation-job` service.
- The submitter runs the Flink SQL client with
  `flink/aggregation.sql` mounted in `/opt/flink/usrlib/`.
