"""
Consumidor de prueba para verificar que los productores escriben correctamente.

NO es un entregable — es solo herramienta de validacion local.
El job real de consumo lo hara Flink.

Uso:
    python producers/consumer_test.py
    python producers/consumer_test.py --topic transactions-pos
    python producers/consumer_test.py --from-beginning
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys

from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] consumer-test: %(message)s",
)
log = logging.getLogger(__name__)

RUNNING = True


def shutdown(signum, frame) -> None:
    global RUNNING
    log.info("Cerrando consumidor...")
    RUNNING = False


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Consumidor de prueba.")
    p.add_argument("--topic", type=str, default="transactions-online",
                   help="Topic a consumir")
    p.add_argument("--bootstrap", type=str,
                   default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--group", type=str, default="test-consumer-group")
    p.add_argument("--from-beginning", action="store_true",
                   help="Leer desde el inicio del topic")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap.split(","),
        group_id=args.group,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,   # timeout para poder chequear RUNNING en cada ciclo
    )

    log.info("Suscrito a %s, esperando mensajes... (Ctrl+C para salir)", args.topic)
    received = 0

    try:
        while RUNNING:
            for msg in consumer:
                if not RUNNING:
                    break
                received += 1
                log.info(
                    "p=%d off=%d key=%s value=%s",
                    msg.partition,
                    msg.offset,
                    msg.key,
                    json.dumps(msg.value, ensure_ascii=False),
                )
    finally:
        log.info("Cerrando. Total recibidos=%d", received)
        consumer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())