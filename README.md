# Modulo Kafka — Pipeline de transacciones en tiempo real

Capa de ingesta del proyecto: broker Kafka en modo KRaft, productores Python
de transacciones simuladas (online y POS), y herramientas de inspeccion.

Cubre los requerimientos **R2.1** y **R2.2**.

## Requisitos

- Docker y Docker Compose v2
- Python 3.10+
- 4 GB de RAM libre

## Arranque rapido

```bash
# 1. Levantar Kafka + crear topics + Kafka UI (un solo comando)
docker compose up -d

# 2. Verificar que todo este sano (debe verse "healthy" en kafka,
#    "exited (0)" en kafka-init, y "Up" en kafka-ui)
docker compose ps

# 3. Instalar dependencias Python
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 4. Ejecutar los productores en dos terminales distintas
python producers/producer_online.py --rate 50
python producers/producer_pos.py    --rate 50
```

Total combinado: 100 eventos/segundo, cumple R2.2.

> **Nota:** los topics se crean automaticamente al levantar el stack mediante
> el servicio `kafka-init`. Es one-shot: corre, crea los 4 topics con su
> retencion de 24h, y se apaga (estado `exited (0)`). Es idempotente, asi que
> volver a ejecutar `docker compose up -d` no falla aunque los topics ya existan.

## Verificacion

### Kafka UI

Abrir [http://localhost:8080](http://localhost:8080).

- En **Topics**: deben aparecer los 4 topics con mensajes entrando.
- En cada topic: contador de mensajes en aumento, retention.ms = 86400000.
- En **Messages** de un topic: payloads JSON visibles.

### Consumidor de prueba (CLI)

```bash
python producers/consumer_test.py --topic transactions-online
python producers/consumer_test.py --topic transactions-pos --from-beginning
```

### Listado por linea de comandos

```bash
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9094 --list
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9094 \
  --describe --topic transactions-online
```

## Estructura

```
.
├── docker-compose.yml          # Kafka (KRaft) + kafka-init + Kafka UI
├── producers/
│   ├── schema.py               # Esquema del evento (contrato compartido)
│   ├── producer_online.py      # Productor canal online
│   ├── producer_pos.py         # Productor canal POS
│   └── consumer_test.py        # Validacion local (no entregable)
├── scripts/
│   └── create_topics.sh        # Utilidad opcional (no requerida)
└── docs/
    └── contract.md             # Contrato para el equipo de Flink
```

## Topics

| Topic | Particiones | Retencion | Quien escribe | Quien lee |
|---|---|---|---|---|
| `transactions-online` | 3 | 24 h | producer_online.py | Jobs de Flink |
| `transactions-pos` | 3 | 24 h | producer_pos.py | Jobs de Flink |
| `alerts` | 3 | 24 h | Job Flink anomalias | Dashboard, notificadores |
| `transactions-dlq` | 3 | 24 h | Jobs Flink (errores) | Inspeccion manual |

Para anadir o modificar un topic, editar la variable `TOPICS` del servicio
`kafka-init` en `docker-compose.yml`. Formato: `nombre:particiones:retencion_ms`.

## Recrear topics manualmente

Si necesitas recrear los topics sin tumbar Kafka, hay dos formas:

```bash
# A) Volver a correr solo el servicio de inicializacion
docker compose up kafka-init

# B) Usar el script utilitario (mismo efecto)
./scripts/create_topics.sh
```

## Parar todo

```bash
docker compose down            # detiene contenedores
docker compose down -v         # detiene y borra volumen (elimina mensajes)
```

## Para mas detalle del contrato de mensajes ver `docs/contract.md`.