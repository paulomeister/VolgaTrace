import requests
import random
import uuid
from datetime import datetime, timedelta

ELASTIC_URL = "http://localhost:9200"
INDEX = "transactions"
TOTAL = 10000

NORMAL_MERCHANTS = [
    "Amazon", "Nike", "Walmart", "Apple", "Spotify", "Steam"
]

RISKY_MERCHANTS = [
    "CryptoX", "BetFast", "UnknownStore"
]


def generate_hour():
    # Horario normal más frecuente
    if random.random() < 0.8:
        return random.randint(8, 22)
    return random.randint(0, 5)


def generate_amount(is_fraud):
    if is_fraud:
        return round(random.uniform(5000, 15000), 2)
    return round(random.uniform(5, 1200), 2)


def generate_status(is_fraud):
    if is_fraud:
        return random.choice(["REJECTED", "APPROVED"])
    return random.choice(["APPROVED", "APPROVED", "APPROVED", "REJECTED"])


def generate_transaction():
    fraud = random.random() < 0.12   # 12% sospechosas

    merchant = random.choice(RISKY_MERCHANTS if fraud else NORMAL_MERCHANTS)

    hour = generate_hour()

    now = datetime.utcnow()
    event_time = now - timedelta(
        days=random.randint(0, 3),
        hours=(now.hour - hour),
        minutes=random.randint(0, 59)
    )

    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": str(random.randint(1000000000000000,9999999999999999)),
        "event_timestamp": event_time.isoformat(),
        "channel": random.choice(["ONLINE", "POS"]),
        "amount": generate_amount(fraud),
        "currency": "USD",
        "status": generate_status(fraud),
        "merchant": merchant,
        "risk_level": "HIGH" if fraud else "LOW"
    }


def load():
    for _ in range(TOTAL):
        data = generate_transaction()
        requests.post(
            f"{ELASTIC_URL}/{INDEX}/_doc",
            json=data
        )

    print(f"{TOTAL} transactions inserted")


if __name__ == "__main__":
    load()