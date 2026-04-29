"""
Esquema del evento de transaccion y generador de datos sinteticos.

Contrato compartido con Flink — coordinar cualquier cambio de campos
con quien consuma los topics antes de tocar esto.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Literal, Optional

from faker import Faker

fake = Faker("es_CO")  # nombres de comercios en colombiano
Faker.seed(42)
random.seed(42)

# -----------------------------------------------------------------------------
# Tipos permitidos
# -----------------------------------------------------------------------------
Channel = Literal["online", "pos"]
Status = Literal["approved", "declined", "pending"]
Currency = Literal["COP", "USD", "EUR"]

# Pool pequeño a proposito: facilita ver anomalias por tarjeta en Flink.
CARD_POOL: list[str] = [f"card_{i:05d}" for i in range(1, 501)]   # 500 tarjetas

MERCHANT_CATEGORIES = [
  "groceries", "restaurants", "fuel", "electronics",
  "clothing", "travel", "entertainment", "health",
]


# -----------------------------------------------------------------------------
# Evento
# -----------------------------------------------------------------------------
@dataclass
class TransactionEvent:
    """Representa una transaccion financiera. produced_at es para R2.7."""
    transaction_id: str
    card_id: str
    event_timestamp: str
    produced_at: str
    channel: Channel
    amount: float
    currency: Currency
    status: Status
    merchant_name: str
    merchant_category: str
    city: str
    country: str

    def to_dict(self) -> dict:
        return asdict(self)


# -----------------------------------------------------------------------------
# Generador
# -----------------------------------------------------------------------------
class TransactionGenerator:
    """Genera eventos sinteticos con anomalias inyectadas a proposito (R2.4)."""

    def __init__(
        self,
        channel: Channel,
        anomaly_rate_high_amount: float = 0.05,
        anomaly_rate_burst: float = 0.03,
        invalid_message_rate: float = 0.01,
    ) -> None:
        self.channel = channel
        self.anomaly_rate_high_amount = anomaly_rate_high_amount
        self.anomaly_rate_burst = anomaly_rate_burst
        self.invalid_message_rate = invalid_message_rate
        self._burst_card: Optional[str] = None
        self._burst_remaining: int = 0

    def next_event(self) -> tuple[str, dict]:
        """Retorna (card_id, event_dict). Puede devolver un mensaje roto (DLQ, R2.8)."""
        if random.random() < self.invalid_message_rate:
            return self._invalid_event()

        # rafaga: varias transacciones seguidas con la misma tarjeta
        if self._burst_remaining > 0:
            card_id = self._burst_card  # type: ignore[assignment]
            self._burst_remaining -= 1
        else:
            card_id = random.choice(CARD_POOL)
            if random.random() < self.anomaly_rate_burst:
                self._burst_card = card_id
                self._burst_remaining = 4

        # 5% de los montos son anomalamente altos
        if random.random() < self.anomaly_rate_high_amount:
            amount = round(random.uniform(10_000, 50_000), 2)
        else:
            amount = round(random.uniform(5, 500), 2)

        status: Status = random.choices(
            ["approved", "declined", "pending"],
            weights=[85, 12, 3],
            k=1,
        )[0]

        now = datetime.now(timezone.utc)

        event = TransactionEvent(
            transaction_id=str(uuid.uuid4()),
            card_id=card_id,
            event_timestamp=now.isoformat(),
            produced_at=now.isoformat(),
            channel=self.channel,
            amount=amount,
            currency=random.choices(
                ["COP", "USD", "EUR"], weights=[80, 15, 5], k=1
            )[0],
            status=status,
            merchant_name=fake.company(),
            merchant_category=random.choice(MERCHANT_CATEGORIES),
            city=fake.city(),
            country="CO",
        )

        # card_id como key → misma tarjeta siempre va a la misma particion
        return card_id, event.to_dict()

    @staticmethod
    def _invalid_event() -> tuple[str, dict]:
        """Evento roto a proposito: le falta card_id, amount es string."""
        broken = {
            "transaction_id": str(uuid.uuid4()),
            "amount": "not-a-number",
            "broken": True,
        }
        return "invalid", broken