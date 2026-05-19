"""
Productor de transacciones en datafono (POS) para 'transactions-pos'.

Cumple R2.2: tasa minima de 50 eventos/segundo.
Mismo diseno que producer_online.py; solo cambia el canal y el topic default.

Uso:
    python producers/producer_pos.py
    python producers/producer_pos.py --rate 100 --duration 600
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from schema import TransactionGenerator   # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] producer-pos: %(message)s",
)
log = logging.getLogger(__name__)

RUNNING = True


def shutdown(signum, frame) -> None:
    global RUNNING
    log.info("Senal recibida (%s). Cerrando productor...", signum)
    RUNNING = False


def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        client_id="producer-pos",
        linger_ms=10,
        batch_size=32 * 1024,
        compression_type="lz4",
        acks="all",
        retries=5,
        retry_backoff_ms=200,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Productor de transacciones POS.")
    p.add_argument("--rate", type=int, default=50)
    p.add_argument("--duration", type=int, default=0)
    p.add_argument("--bootstrap", type=str,
                   default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--topic", type=str,
                   default=os.getenv("KAFKA_TOPIC", "transactions-pos"))
    return p.parse_args()


def main() -> int:
    args = parse_args()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log.info(
        "Iniciando: bootstrap=%s topic=%s rate=%d ev/s",
        args.bootstrap, args.topic, args.rate,
    )

    try:
        producer = build_producer(args.bootstrap)
    except KafkaError as e:
        log.error("No se pudo conectar al broker %s: %s", args.bootstrap, e)
        return 1

    generator = TransactionGenerator(channel="POS")
    if args.rate <= 0:
        log.error("rate debe ser mayor que 0")
        return 1

    period = 1.0 / args.rate

    sent = 0
    failures = 0
    start = time.monotonic()
    last_log = start
    next_send = start

    def on_send_error(exc):
        nonlocal failures
        failures += 1
        log.warning("Error entregando mensaje: %s", exc)

    try:
        while RUNNING:
            if args.duration and (time.monotonic() - start) >= args.duration:
                log.info("Duracion alcanzada (%ds). Saliendo.", args.duration)
                break

            key, value = generator.next_event()

            future = producer.send(args.topic, key=key, value=value)
            future.add_errback(on_send_error)
            sent += 1

            now = time.monotonic()
            if now - last_log >= 5.0:
                elapsed = now - start
                rate_real = sent / elapsed if elapsed > 0 else 0
                log.info(
                    "Enviados=%d fallos=%d tasa_real=%.1f ev/s",
                    sent, failures, rate_real,
                )
                last_log = now

            next_send += period
            sleep_for = next_send - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_send = time.monotonic()
    finally:
        log.info("Vaciando buffer pendiente...")
        producer.flush(timeout=10)
        producer.close(timeout=5)

    elapsed = time.monotonic() - start
    log.info(
        "Fin. Total enviados=%d duracion=%.1fs tasa_promedio=%.1f ev/s",
        sent, elapsed, sent / elapsed if elapsed > 0 else 0,
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
