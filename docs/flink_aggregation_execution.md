# Flink Aggregation Job — Execution Guide

## Project: VolgaTrace — Real-time Financial Transaction Pipeline

This document explains how to execute and validate the Flink aggregation module implemented for the real-time transaction processing pipeline.

The aggregation job consumes transaction events from Kafka, applies a 1-minute tumbling window, calculates transaction metrics, and publishes the processed results to a Kafka output topic.

This module supports requirement **R2.3** of the project:

> Create a Flink job that consumes Kafka events, applies a 1-minute tumbling window, and calculates aggregations such as count, average, and maximum.

---

## 1. Module Objective

The objective of this module is to process financial transaction events in real time using Apache Flink.

The job reads events from the following Kafka topics:

- `transactions-online`
- `transactions-pos`

Then it calculates aggregated metrics every 1 minute and writes the results to:

- `transactions-aggregated`

---

## 2. Technologies Used

- Apache Kafka
- Apache Flink
- Docker Compose
- Python producers
- Kafka UI
- Flink SQL Client

---

## 3. Input Topics

The Flink aggregation job consumes data from:

| Topic | Description |
|---|---|
| `transactions-online` | Online transaction events |
| `transactions-pos` | POS / physical payment transaction events |

The services inside Docker use the internal Kafka address:

```text
kafka:9094

Local Python scripts use:
    localhost:9092

4.Output Topic:
    transactions-aggregated

5.Input Message Schema:
    {
        "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
        "card_id": "4532015112830366",
        "event_timestamp": "2026-01-15T14:30:22.123456+00:00",
        "produced_at": "2026-01-15T14:30:22.124001+00:00",
        "channel": "ONLINE",
        "amount": 142.50,
        "currency": "COP",
        "status": "APPROVED",
        "merchant_name": "Tienda Andina S.A.S.",
        "merchant_category": "groceries",
        "city": "Bogota",
        "country": "CO"
    }

6.Aggregation Logic

    The Flink job applies a 1-minute tumbling window based on the event timestamp.

    The events are grouped by:

    channel 
    currency
    merchant_category
    city

    For each group, the job calculates:

    Metric	Description
    transaction_count	Total number of transactions
    approved_count	Number of approved transactions
    rejected_count	Number of rejected transactions
    pending_count	Number of pending transactions
    avg_amount	Average transaction amount
    max_amount	Maximum transaction amount

7. Project Files Related to This Module
    flink/
        ├── Dockerfile
        └── aggregation.sql

flink/Dockerfile
    Builds a custom Flink image with the Kafka SQL connector required to read and write Kafka topics.

flink/aggregation.sql
    Contains the SQL statements that:
    Create the Kafka source table.
    Create the Kafka sink table.
    Execute the tumbling window aggregation job.

8. Execution Steps
    Step 1 — Open the project folder

        From Windows PowerShell or CMD:

            cd C:\University\Github\VolgaTrace
    Step 2 — Start the full stack

        Run:

        docker compose up -d --build

            This command starts:

            Kafka
            Kafka UI
            Flink JobManager
            Flink TaskManager
            Elasticsearch
            Kibana
    Step 3 — Verify running containers

        Run:

        docker compose ps

            Expected services:

                kafka
                kafka-ui
                flink-jobmanager
                flink-taskmanager
                elasticsearch
                kibana

        The Kafka container should appear as healthy.

    Step 4 — Create and activate the Python virtual environment

        If the virtual environment does not exist yet, create it:

            py -m venv .venv

        Activate it:

            .\.venv\Scripts\Activate.ps1

        Install dependencies:

            py -m pip install -r requirements.txt

    Step 5 — Run the ONLINE producer

        Open a terminal and run:

            python producers\producer_online.py --rate 60

        This producer sends online transaction events to:

         transactions-online

    Step 6 — Run the POS producer

        Open another terminal and run:

            python producers\producer_pos.py --rate 60

        This producer sends POS transaction events to:

            transactions-pos

        Running both producers at 60 events per second helps ensure the project exceeds the required total rate of 100 events per second.

    Step 7 — Execute the Flink aggregation job

        Open another terminal and run:

            docker compose exec flink-jobmanager ./bin/sql-client.sh -f /opt/flink/usrlib/aggregation.sql

        If the job starts correctly, the terminal should show a message similar to:

            SQL update statement has been successfully submitted to the cluster:
            Job ID: b7d129c2a1ebd56b984a7a2790c0086d

    Step 8 — Validate the output topic

        Wait at least 1 minute because the job uses a 1-minute tumbling window.

        Then run:

            docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9094 --topic transactions-aggregated --from-beginning --max-messages 10

        Expected output example:

            {
            "window_start": "2026-04-30 02:11:00.000",
            "window_end": "2026-04-30 02:12:00.000",
            "channel": "ONLINE",
            "currency": "COP",
            "merchant_category": "restaurants",
            "city": "San Pablo de Borbur",
            "transaction_count": 1,
            "approved_count": 1,
            "rejected_count": 0,
            "pending_count": 0,
            "avg_amount": 72.38,
            "max_amount": 72.38
            }

        If JSON messages appear in transactions-aggregated, the aggregation job is working correctly.

9. Web Interfaces
    Kafka UI

    Open:

        http://localhost:8080

        Use Kafka UI to inspect:

        transactions-online
        transactions-pos
        transactions-aggregated
        alerts
        transactions-dlq
        Flink UI

    Open:

        http://localhost:8081

        Use Flink UI to verify that the aggregation job is running.

        Kibana

    Open:

        http://localhost:5601

        Used by the visualization layer.

10. Important Notes
    Topic names

    This project uses hyphens in topic names:

        transactions-online
        transactions-pos
        transactions-aggregated

    Do not use old names with underscores such as:

        transactions_online
        transactions_pos
        transactions_aggregated
        Kafka bootstrap addresses

    Use this address inside Docker containers:

        kafka:9094

    Use this address from local Python scripts:

        localhost:9092
        Timestamp handling

    The original event timestamp arrives as a string with microseconds:

        2026-04-29T21:12:05.123456+00:00

        The Flink SQL job converts it into a valid TIMESTAMP(3) field called event_time.

        This conversion is necessary because Flink watermarks only support timestamp precision from 0 to 3.

11. Troubleshooting
    Error: TIMESTAMP_LTZ(6) is not supported for watermark

    Problem:

        Invalid data type of time field for watermark definition.
        The supported precision is from 0 to 3.

    Solution:

        Use event_timestamp as STRING and create a computed field:

            event_time AS CAST(
                REPLACE(SUBSTRING(event_timestamp, 1, 23), 'T', ' ')
                AS TIMESTAMP(3)
            )

        Then define the watermark on event_time.

    Error: no messages in transactions-aggregated

        Possible causes:

            The producers are not running.
            Less than 1 minute has passed.
            The Flink job is not running.
            The topic name is incorrect.
            The Kafka bootstrap server is incorrect.

            Recommended checks:

            docker compose ps
            docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9094 --list
            docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9094 --topic transactions-online --from-beginning --max-messages 5
            docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9094 --topic transactions-pos --from-beginning --max-messages 5
            Error: producer rate below 50 events/second

            If each producer shows around 48 events/second, run them with a higher target rate:

            python producers\producer_online.py --rate 60
            python producers\producer_pos.py --rate 60

12. Commands Summary

    Start the stack:

        docker compose up -d --build

    Check services:

        docker compose ps

    Activate virtual environment:

        .\.venv\Scripts\Activate.ps1

    Run online producer:

        python producers\producer_online.py --rate 60

    Run POS producer:

        python producers\producer_pos.py --rate 60

    Run Flink aggregation job:

        docker compose exec flink-jobmanager ./bin/sql-client.sh -f /opt/flink/usrlib/aggregation.sql

    Validate aggregated results:

        docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9094 --topic transactions-aggregated --from-beginning --max-messages 10

    Stop all containers:

        docker compose down

    Stop and delete volumes:

        docker compose down -v