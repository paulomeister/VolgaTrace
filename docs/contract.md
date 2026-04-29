# Contrato de mensajes — equipo de ingesta ↔ equipo de Flink
 
Este documento es la fuente de verdad sobre los topics, claves y formato de
mensajes que el modulo de Kafka publica. Cualquier cambio aqui debe acordarse en equipo.
 
## Bootstrap servers
 
| Origen del consumidor | Direccion |
|---|---|
| Servicio dentro de Docker (Flink, conectores, etc.) | `kafka:9094` |
| Script o herramienta fuera de Docker (PC) | `localhost:9092` |
 
## Topics
 
| Topic | Particiones | Retencion (ms) | Key |
|---|---|---|---|
| `transactions-online` | 3 | 86 400 000 | `card_id` |
| `transactions-pos` | 3 | 86 400 000 | `card_id` |
| `alerts` | 3 | 86 400 000 | `card_id` |
| `transactions-dlq` | 3 | 86 400 000 | original (puede faltar) |
 
**Particionamiento por `card_id`**: garantiza que todos los eventos de la misma
tarjeta caen en la misma particion. Esto es necesario para detectar patrones
con orden temporal (por ejemplo, "3 rechazos seguidos + 1 aprobacion").
 
## Esquema del evento de transaccion
 
Todos los mensajes en `transactions-online` y `transactions-pos` siguen este
formato. Codificacion: JSON UTF-8.
 
```json
{
  "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
  "card_id": "card_00123",
  "event_timestamp": "2026-01-15T14:30:22.123456+00:00",
  "produced_at":     "2026-01-15T14:30:22.124001+00:00",
  "channel": "online",
  "amount": 142.50,
  "currency": "COP",
  "status": "approved",
  "merchant_name": "Tienda Andina S.A.S.",
  "merchant_category": "groceries",
  "city": "Bogota",
  "country": "CO"
}
```
 
### Campos
 
| Campo | Tipo | Notas |
|---|---|---|
| `transaction_id` | string (UUID v4) | Unico por evento |
| `card_id` | string | Formato `card_NNNNN` (5 digitos) |
| `event_timestamp` | string (ISO 8601, UTC) | Hora del evento "real" |
| `produced_at` | string (ISO 8601, UTC) | Hora en que el productor publico — usar para medir latencia E2E (R2.7) |
| `channel` | enum | `"online"` o `"pos"` |
| `amount` | float | Positivo, dos decimales |
| `currency` | enum | `"COP"`, `"USD"`, `"EUR"` |
| `status` | enum | `"approved"`, `"declined"`, `"pending"` |
| `merchant_name` | string | Nombre del comercio |
| `merchant_category` | enum | `groceries`, `restaurants`, `fuel`, `electronics`, `clothing`, `travel`, `entertainment`, `health` |
| `city` | string | Ciudad de la transaccion |
| `country` | string | ISO-3166 alpha-2 (`"CO"`) |
 
## Anomalias inyectadas (para R2.4)
 
El generador inyecta deliberadamente patrones detectables:
 
1. **Monto inusualmente alto** (~5%): `amount` entre 10 000 y 50 000.
2. **Rafaga de la misma tarjeta** (~3%): 5 transacciones seguidas con el
   mismo `card_id` en un intervalo corto.
3. **Transacciones rechazadas** (~12%): `status="declined"` en distribucion
   normal — combinado con el orden por particion permite detectar la regla
   "3 rechazos + 1 aprobacion".
## Mensajes invalidos para la DLQ (para R2.8)
 
El ~1% de los mensajes producidos son intencionalmente mal formados (campos
faltantes, tipos incorrectos). El job de Flink debe:
 
1. Detectarlos al deserializar.
2. Reenviarlos a `transactions-dlq`.
3. NO detener el pipeline.
Un mensaje invalido tipico:
 
```json
{
  "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
  "amount": "not-a-number",
  "broken": true
}
```
 
## Esquema sugerido para el topic `alerts`
 
(Esto lo decide el rol de anomalias, pero deja una propuesta como base.)
 
```json
{
  "alert_id": "uuid",
  "card_id": "card_00123",
  "alert_type": "HIGH_AMOUNT | BURST | DECLINED_PATTERN",
  "severity": "low | medium | high",
  "triggered_at": "2026-01-15T14:30:25Z",
  "source_transaction_ids": ["uuid1", "uuid2", ...],
  "details": { "...": "..." }
}
```