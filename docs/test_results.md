## CP-01 — Producción de eventos ONLINE válidos

**Bloque:** Pruebas funcionales  
**Estado:** Aprobada  
**Fecha de ejecución:** 2026-05-18  
**Entorno:** Ejecución local sobre Docker Compose, con Kafka y Kafka UI activos

### Objetivo

Verificar que el productor del canal `ONLINE` publique mensajes en el topic `transactions-online` usando el contrato de datos definido para el proyecto.

### Precondiciones

- Stack levantado con Docker Compose.
- Servicio Kafka activo.
- Kafka UI disponible.
- Topic `transactions-online` disponible.
- Productor `ONLINE` disponible en el repositorio.
- Carpeta `docs/evidence` disponible para almacenar logs, resumen y capturas.

### Procedimiento ejecutado

1. Se verificó que Kafka y Kafka UI estuvieran disponibles.
2. Se ejecutó el script automatizado de CP-01.
3. El script levantó o verificó los servicios necesarios para la prueba:
   - `kafka`
   - `kafka-ui`
4. El script registró los offsets iniciales del topic `transactions-online`.
5. Se ejecutó el productor `ONLINE` con una tasa configurada de `50 eventos/s`.
6. El productor se ejecutó durante `60 segundos`.
7. Se registró el log de salida del productor.
8. El script registró los offsets finales del topic `transactions-online`.
9. Se calculó la diferencia de offsets para confirmar cuántos mensajes nuevos llegaron al topic.
10. Se verificó visualmente el resultado en Kafka UI.

### Comando ejecutado

```powershell
.\scripts\run_cp01_online_producer_test.ps1
```

### Configuración de la prueba

```text
Productor: producers/producer_online.py
Topic destino: transactions-online
Rate configurado: 50 eventos/s
Duración: 60 segundos
```

### Evidencia observada

El script finalizó con el siguiente resultado:

```text
CP-01 validation: PASSED
ONLINE producer published messages to transactions-online.
```

El productor `ONLINE` reportó:

```text
Total enviados: 2810
Fallos: 0
Tasa promedio: 46.8 eventos/s
Duración: 60.0 segundos
```

La línea final del productor fue:

```text
2026-05-18 17:28:48,649 [INFO] producer-online: Fin. Total enviados=2810 fallos=0 duracion=60.0s tasa_promedio=46.8 ev/s
```

La validación por offsets de Kafka mostró:

```text
Offset inicial total: 175681
Offset final total: 178491
Delta: 2810 mensajes nuevos
```

En Kafka UI se observó:

```text
transactions-online: 2810 mensajes
transactions-pos: 0 mensajes
alerts: 0 mensajes
transactions-dlq: 0 mensajes
transactions-aggregated: 0 mensajes
```

Esto confirma que los mensajes fueron publicados únicamente en el topic esperado para el productor `ONLINE`.

### Resultado esperado

- El productor `ONLINE` publica mensajes en el topic `transactions-online`.
- Kafka recibe nuevos mensajes en el topic `transactions-online`.
- El productor finaliza sin errores críticos.
- No se presentan fallos visibles de publicación que impidan el envío al topic.
- La cantidad de mensajes reportada por el productor coincide con el incremento observado en Kafka.
- Los mensajes se publican en el topic correcto.

### Resultado obtenido

La prueba fue ejecutada correctamente.

El productor `ONLINE` publicó `2810` mensajes durante una ejecución de `60 segundos`. El productor finalizó con `0` fallos y una tasa promedio de `46.8 eventos/s`.

La validación por offsets confirmó que Kafka recibió `2810` mensajes nuevos en el topic `transactions-online`, coincidiendo con el total reportado por el productor.

Kafka UI mostró mensajes únicamente en `transactions-online`, mientras que `transactions-pos`, `alerts`, `transactions-dlq` y `transactions-aggregated` permanecieron en `0` para esta ejecución, lo cual confirma que la prueba estuvo aislada al canal `ONLINE`.

### Conclusión

La prueba CP-01 fue aprobada.

Se comprobó que el productor del canal `ONLINE` publica correctamente mensajes en el topic `transactions-online`. El productor finalizó sin errores críticos y Kafka registró el mismo número de mensajes reportado por el productor.

El resultado demuestra que la producción de eventos `ONLINE` funciona correctamente y que el topic destino recibe los eventos esperados.

### Observación técnica

La tasa configurada fue de `50 eventos/s`, pero la tasa promedio real observada fue de `46.8 eventos/s`. Esta diferencia no invalida la prueba, ya que el objetivo de CP-01 es validar la publicación correcta de mensajes en el topic `transactions-online`, no medir throughput mínimo.

La validación de estructura detallada de mensajes se respaldó visualmente desde Kafka UI y por el comportamiento esperado del productor. La validación automatizada de CP-01 se enfocó en confirmar ejecución exitosa del productor, ausencia de fallos y recepción de mensajes en Kafka mediante offsets.

### Evidencia asociada

- [Log general de ejecución de CP-01](evidence/cp01_terminal_log.txt)
- [Log del productor ONLINE CP-01](evidence/cp01_online_producer_log.txt)
- [Captura de Kafka UI mostrando mensajes en transactions-online](evidence/cp01_kafka_ui_transactions_online.png)

---

## CP-02 — Producción de eventos POS válidos

**Bloque:** Pruebas funcionales  
**Estado:** Aprobada  
**Fecha de ejecución:** 2026-05-18  
**Entorno:** Ejecución local sobre Docker Compose, con Kafka y Kafka UI activos

### Objetivo

Verificar que el productor del canal `POS` publique mensajes en el topic `transactions-pos` usando el contrato de datos definido para el proyecto.

### Precondiciones

- Stack levantado con Docker Compose.
- Servicio Kafka activo.
- Kafka UI disponible.
- Topic `transactions-pos` disponible.
- Productor `POS` disponible en el repositorio.
- Carpeta `docs/evidence` disponible para almacenar logs, resumen y capturas.

### Procedimiento ejecutado

1. Se verificó que Kafka y Kafka UI estuvieran disponibles.
2. Se ejecutó el script automatizado de CP-02.
3. El script levantó o verificó los servicios necesarios para la prueba:
   - `kafka`
   - `kafka-ui`
4. El script registró los offsets iniciales del topic `transactions-pos`.
5. Se ejecutó el productor `POS` con una tasa configurada de `50 eventos/s`.
6. El productor se ejecutó durante `60 segundos`.
7. Se registró el log de salida del productor.
8. El script registró los offsets finales del topic `transactions-pos`.
9. Se calculó la diferencia de offsets para confirmar cuántos mensajes nuevos llegaron al topic.
10. Se verificó visualmente el resultado en Kafka UI.

### Comando ejecutado

```powershell
.\scripts\run_cp02_pos_producer_test.ps1
```

### Configuración de la prueba

```text
Productor: producers/producer_pos.py
Topic destino: transactions-pos
Rate configurado: 50 eventos/s
Duración: 60 segundos
```

### Evidencia observada

El script finalizó con el siguiente resultado:

```text
CP-02 validation: PASSED
POS producer published messages to transactions-pos.
```

El productor `POS` reportó:

```text
Total enviados: 2808
Fallos: N/A
Tasa promedio: 46.8 eventos/s
Duración: 60.0 segundos
```

La línea final del productor fue:

```text
2026-05-18 17:43:25,038 [INFO] producer-pos: Fin. Total enviados=2808 duracion=60.0s tasa_promedio=46.8 ev/s
```

La validación por offsets de Kafka mostró:

```text
Offset inicial total: 150199
Offset final total: 153007
Delta: 2808 mensajes nuevos
```

En Kafka UI se observó:

```text
transactions-pos: 2808 mensajes
transactions-online: 0 mensajes
alerts: 0 mensajes
transactions-dlq: 0 mensajes
transactions-aggregated: 0 mensajes
```

Esto confirma que los mensajes fueron publicados únicamente en el topic esperado para el productor `POS`.

### Resultado esperado

- El productor `POS` publica mensajes en el topic `transactions-pos`.
- Kafka recibe nuevos mensajes en el topic `transactions-pos`.
- El productor finaliza sin errores críticos.
- No se presentan fallos visibles de publicación que impidan el envío al topic.
- La cantidad de mensajes reportada por el productor coincide con el incremento observado en Kafka.
- Los mensajes se publican en el topic correcto.

### Resultado obtenido

La prueba fue ejecutada correctamente.

El productor `POS` publicó `2808` mensajes durante una ejecución de `60 segundos`. El productor finalizó sin errores críticos y alcanzó una tasa promedio de `46.8 eventos/s`.

La validación por offsets confirmó que Kafka recibió `2808` mensajes nuevos en el topic `transactions-pos`, coincidiendo con el total reportado por el productor.

Kafka UI mostró mensajes únicamente en `transactions-pos`, mientras que `transactions-online`, `alerts`, `transactions-dlq` y `transactions-aggregated` permanecieron en `0` para esta ejecución, lo cual confirma que la prueba estuvo aislada al canal `POS`.

### Conclusión

La prueba CP-02 fue aprobada.

Se comprobó que el productor del canal `POS` publica correctamente mensajes en el topic `transactions-pos`. El productor finalizó sin errores críticos y Kafka registró el mismo número de mensajes reportado por el productor.

El resultado demuestra que la producción de eventos `POS` funciona correctamente y que el topic destino recibe los eventos esperados.

### Observación técnica

La tasa configurada fue de `50 eventos/s`, pero la tasa promedio real observada fue de `46.8 eventos/s`. Esta diferencia no invalida la prueba, ya que el objetivo de CP-02 es validar la publicación correcta de mensajes en el topic `transactions-pos`, no medir throughput mínimo.

El campo `Fallos` aparece como `N/A` porque la línea final del productor `POS` no imprime explícitamente el valor `fallos=0`, a diferencia de otros productores o ejecuciones. Sin embargo, el productor finalizó correctamente, Kafka recibió los mensajes esperados y no se observaron errores críticos durante la ejecución.

### Evidencia asociada

- [Log general de ejecución de CP-02](evidence/cp02_terminal_log.txt)
- [Log del productor POS CP-02](evidence/cp02_pos_producer_log.txt)
- [Captura de Kafka UI mostrando mensajes en transactions-pos](evidence/cp02_kafka_ui_transactions_pos.png)

---

## CP-03 — Validación de agregaciones en `transactions-aggregated`

**Bloque:** Pruebas funcionales  
**Estado:** Aprobada  
**Fecha de ejecución:** 2026-05-16  
**Entorno:** Ejecución local sobre Docker Compose, con Kafka, Kafka UI y Apache Flink activos

### Objetivo

Verificar que el job de agregación de Apache Flink consuma eventos desde los tópicos `transactions-online` y `transactions-pos`, procese las transacciones mediante ventanas de agregación y publique los resultados en el tópico `transactions-aggregated`.

### Precondiciones

- Stack levantado con Docker Compose.
- Servicio Kafka activo.
- Kafka UI disponible en `http://localhost:8080`.
- Flink UI disponible en `http://localhost:8081`.
- Topics `transactions-online`, `transactions-pos` y `transactions-aggregated` disponibles.
- Entorno virtual de Python configurado con dependencias instaladas desde `requirements.txt`.
- Job de agregación disponible en Apache Flink.
- Configuración del source Kafka ajustada para consumir explícitamente desde los topics `transactions-online` y `transactions-pos`.

### Procedimiento ejecutado

1. Se cancelaron jobs activos previos en Flink para evitar ejecuciones duplicadas.
2. Se limpiaron los topics `transactions-online`, `transactions-pos` y `transactions-aggregated` para iniciar la prueba desde cero.
3. Se verificó que la configuración del job de agregación consumiera explícitamente desde ambos topics de entrada:

   ```sql
   'topic' = 'transactions-online;transactions-pos'
   ```

4. Se ejecutó el script automatizado de CP-03.
5. El script levantó o verificó los servicios necesarios para la prueba:
   - `kafka`
   - `kafka-ui`
   - `flink-jobmanager`
   - `flink-taskmanager`
   - `flink-aggregation-job`
6. El script ejecutó los productores ONLINE y POS durante 150 segundos.
7. Se esperó el cierre de ventana y avance de watermark.
8. Se validó la salida en Kafka UI y Flink UI.

### Comando ejecutado

```powershell
.\scripts\run_cp03_aggregation_test.ps1
```

### Evidencia observada

En Kafka UI se observaron mensajes publicados en los topics de entrada y salida:

- `transactions-online`: 1478 mensajes.
- `transactions-pos`: 1478 mensajes.
- `transactions-aggregated`: 134 mensajes.

En Flink UI se observó el job de agregación en estado `RUNNING`.

También se observó procesamiento de registros en los operadores del job:

- Operador Source:
  - `Records Sent`: 212.
  - `Bytes Sent`: 192 KB.

- Operador global de agregación:
  - `Records Received`: 212.
  - `Bytes Received`: 206 KB.

Además, el script generó los archivos de log de ejecución de los productores ONLINE y POS, junto con el log de validación automática del topic `transactions-aggregated`.

### Resultado esperado

- El productor ONLINE publica eventos en `transactions-online`.
- El productor POS publica eventos en `transactions-pos`.
- El job de agregación permanece en estado `RUNNING`.
- El Source de Flink consume registros desde los topics de entrada.
- El operador de agregación procesa los registros recibidos.
- El topic `transactions-aggregated` recibe mensajes agregados.
- No se presentan errores visibles que impidan el procesamiento o la publicación de resultados.

### Resultado obtenido

La prueba fue ejecutada correctamente.

Los productores ONLINE y POS generaron eventos en sus respectivos topics. Kafka UI mostró mensajes en `transactions-online` y `transactions-pos`. Flink UI confirmó que el job de agregación estaba en estado `RUNNING` y que los operadores del pipeline procesaron registros. Finalmente, Kafka UI mostró mensajes publicados en `transactions-aggregated`.

Durante el diagnóstico previo se identificó que la configuración inicial con:

```sql
'topic-pattern' = 'transactions-(online|pos)'
```

dejaba el Source de Flink sin consumir registros en el entorno local. Para estabilizar la ejecución se reemplazó por una suscripción explícita:

```sql
'topic' = 'transactions-online;transactions-pos'
```

Con este ajuste, el pipeline de agregación funcionó correctamente.

### Conclusión

La prueba CP-03 fue aprobada.

Se comprobó que el job de agregación de Apache Flink consume eventos desde `transactions-online` y `transactions-pos`, procesa los registros mediante ventanas de agregación y publica resultados en `transactions-aggregated`.

Se deja como observación técnica que, para el entorno local de pruebas, fue necesario reemplazar el uso de `topic-pattern` por la declaración explícita de ambos topics mediante `topic = 'transactions-online;transactions-pos'`.

### Evidencia asociada

- [Log de ejecución del productor ONLINE usado en CP-03](evidence/cp03_online_terminal_log.txt)
- [Log de ejecución del productor POS usado en CP-03](evidence/cp03_pos_terminal_log.txt)
- [Log de validación automática sobre `transactions-aggregated`](evidence/cp03_aggregated_validation_log.txt)
- [Captura de Kafka UI mostrando mensajes en `transactions-online`, `transactions-pos` y `transactions-aggregated`](evidence/cp03_kafka_ui_aggregated.png)
- [Captura de Flink UI mostrando el job de agregación en ejecución y procesamiento de registros](evidence/cp03_flink_job.png)

---

## CP-04 — Validación funcional de alerta por monto inusual

**Bloque:** Pruebas funcionales  
**Estado:** Aprobada  
**Fecha de ejecución:** 2026-05-16  
**Entorno:** Ejecución local sobre Docker Compose, con Kafka, Kafka UI y Apache Flink activos

### Objetivo

Verificar que el job de anomalías de Apache Flink detecte una transacción con monto inusual y publique una alerta funcional en el tópico `alerts`.

### Precondiciones

- Stack levantado con Docker Compose.
- Servicio Kafka activo.
- Kafka UI disponible en `http://localhost:8080`.
- Flink UI disponible en `http://localhost:8081`.
- Topic `transactions-online` disponible.
- Topic `alerts` disponible.
- Job de anomalías disponible y ejecutándose en Apache Flink.
- Corrección de despliegue del job de anomalías integrada en `main`, permitiendo ejecutar el job desde el JAR generado.
- Entorno local actualizado con los cambios de la PR que corrige la ejecución de `flink-anomalies-job`.

### Procedimiento ejecutado

1. Se verificó que los cambios de la PR correctiva estuvieran disponibles en la rama `main` local.
2. Se limpiaron los topics `transactions-online` y `alerts` para iniciar la prueba desde cero.
3. Se ejecutó el script automatizado de CP-04.
4. El script levantó o verificó los servicios necesarios para la prueba:
   - `kafka`
   - `kafka-ui`
   - `flink-jobmanager`
   - `flink-taskmanager`
   - `flink-anomalies-job`
5. El script publicó una transacción controlada en el topic `transactions-online`.
6. La transacción enviada tenía un monto de `20000.00` en moneda `USD`, superando el umbral definido para alertas de tipo `HIGH_AMOUNT`.
7. Se esperó el procesamiento del evento por parte del job de anomalías.
8. Se validó la salida en Kafka UI y Flink UI.
9. Se confirmó la generación de una alerta en el topic `alerts`.

### Comando ejecutado

```powershell
.\scripts\run_cp04_high_amount_alert_test.ps1
```

### Evento de prueba enviado

```json
{
  "transaction_id": "cp04-high-amount-001",
  "card_id": "4532015112830366",
  "event_timestamp": "2026-05-16T17:30:00.000000+00:00",
  "produced_at": "2026-05-16T17:30:00.000000+00:00",
  "channel": "ONLINE",
  "amount": 20000.00,
  "currency": "USD",
  "status": "APPROVED",
  "merchant_name": "CP04 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

### Evidencia observada

En Kafka UI se observó el evento publicado en el topic `transactions-online`:

- `transactions-online`: 1 mensaje.
- `transaction_id`: `cp04-high-amount-001`.
- `card_id`: `4532015112830366`.
- `amount`: `20000.00`.
- `currency`: `USD`.

En Kafka UI se observó una alerta publicada en el topic `alerts`:

- `alerts`: 1 mensaje.
- `alert_type`: `HIGH_AMOUNT`.
- `card_id`: `4532015112830366`.
- `source_transaction_id`: `cp04-high-amount-001`.

La alerta observada fue:

```json
{
  "alert_id": "8a4bffdc-6a6b-4b72-abc1-709dbec27bd3",
  "card_id": "4532015112830366",
  "alert_type": "HIGH_AMOUNT",
  "triggered_at": 1778976411.170874720,
  "source_transaction_id": "cp04-high-amount-001"
}
```

En Flink UI se observó el job de anomalías en estado `RUNNING`:

- Job: `Fraud Detection, Anomalies Job`.
- Estado: `RUNNING`.

También se observó procesamiento de registros en los operadores del job:

- Source Kafka:
  - `Records Sent`: 1.
  - `Bytes Sent`: 537 B.

- Operador `KeyedProcess`:
  - `Records Received`: 1.
  - `Records Sent`: 1.
  - `Bytes Received`: 677 B.
  - `Bytes Sent`: 280 B.

- Sink hacia salida:
  - `Records Received`: 1.
  - `Bytes Received`: 1.64 KB.

Además, el script generó los archivos de evidencia automática de la prueba.

### Resultado esperado

- El evento de prueba se publica correctamente en `transactions-online`.
- El job de anomalías permanece en estado `RUNNING`.
- El Source de Flink consume el evento desde Kafka.
- El operador de detección de anomalías procesa el evento.
- El job detecta que el monto `20000.00 USD` supera el umbral configurado.
- Se genera una alerta de tipo `HIGH_AMOUNT`.
- La alerta se publica en el topic `alerts`.
- La alerta contiene el mismo `card_id` y el mismo `source_transaction_id` del evento enviado.

### Resultado obtenido

La prueba fue ejecutada correctamente.

El evento controlado fue publicado en `transactions-online`. El job de anomalías de Flink permaneció en estado `RUNNING` y procesó el registro. Kafka UI confirmó la publicación de una alerta en el topic `alerts` con tipo `HIGH_AMOUNT`, asociada al `card_id` `4532015112830366` y al `source_transaction_id` `cp04-high-amount-001`.

Durante la preparación de la prueba se identificó que el servicio `flink-anomalies-job` no podía ejecutarse porque intentaba compilar el job Java en tiempo de ejecución usando Maven, pero la imagen no tenía `mvn` disponible. La PR correctiva integró el uso de una imagen específica para anomalías y la ejecución del JAR ya generado, permitiendo que el job `Fraud Detection, Anomalies Job` quedara correctamente desplegado en Flink.

Con esta corrección integrada en `main`, la prueba CP-04 fue ejecutada exitosamente.

### Conclusión

La prueba CP-04 fue aprobada.

Se comprobó que el job de anomalías detecta correctamente una transacción con monto inusual y publica una alerta funcional en el topic `alerts`.

El evento enviado con `amount = 20000.00` y `currency = USD` superó el umbral definido para la regla de monto alto, generando una alerta `HIGH_AMOUNT` asociada a la transacción `cp04-high-amount-001`.

### Evidencia asociada

- [Log de ejecución de la prueba CP-04](evidence/cp04_terminal_log.txt)
- [Evento de entrada usado para la prueba CP-04](evidence/cp04_input_event.txt)
- [Log de validación automática sobre el topic `alerts`](evidence/cp04_alerts_validation_log.txt)
- [Captura de Kafka UI mostrando el evento en `transactions-online`](evidence/cp04_kafka_ui_input.png)
- [Captura de Kafka UI mostrando la alerta `HIGH_AMOUNT` en `alerts`](evidence/cp04_kafka_ui_alerts.png)
- [Captura de Flink UI mostrando el job de anomalías en ejecución](evidence/cp04_flink_job.png)

---

## CP-05 — Validación funcional de alerta por alta frecuencia

**Bloque:** Pruebas funcionales  
**Estado:** Aprobada  
**Fecha de ejecución:** 2026-05-16  
**Entorno:** Ejecución local sobre Docker Compose, con Kafka, Kafka UI y Apache Flink activos

### Objetivo

Verificar que el job de anomalías de Apache Flink detecte una alta frecuencia de transacciones asociadas a una misma tarjeta y publique una alerta funcional en el tópico `alerts`.

### Precondiciones

- Stack levantado con Docker Compose.
- Servicio Kafka activo.
- Kafka UI disponible en `http://localhost:8080`.
- Flink UI disponible en `http://localhost:8081`.
- Topic `transactions-online` disponible.
- Topic `alerts` disponible.
- Job de anomalías disponible y ejecutándose en Apache Flink.
- Servicio `flink-anomalies-job` corregido y operativo.
- Entorno local actualizado con los cambios necesarios para ejecutar correctamente el job `Fraud Detection, Anomalies Job`.

### Procedimiento ejecutado

1. Se verificó que el job `Fraud Detection, Anomalies Job` estuviera en estado `RUNNING` en Flink UI.
2. Se limpiaron los topics `transactions-online` y `alerts` para iniciar la prueba desde cero.
3. Se ejecutó el script automatizado de CP-05.
4. El script levantó o verificó los servicios necesarios para la prueba:
   - `kafka`
   - `kafka-ui`
   - `flink-jobmanager`
   - `flink-taskmanager`
   - `flink-anomalies-job`
5. El script publicó 6 transacciones controladas en el topic `transactions-online`.
6. Las 6 transacciones fueron enviadas con el mismo `card_id`, usando timestamps dentro de una ventana de 1 minuto.
7. Se envió un evento adicional de avance de watermark para permitir el cierre de la ventana de event-time.
8. Se esperó el procesamiento del evento por parte del job de anomalías.
9. Se validó la salida en Kafka UI y Flink UI.
10. Se confirmó la generación de una alerta en el topic `alerts`.

### Comando ejecutado

```powershell
.\scripts\run_cp05_burst_alert_test.ps1
```

### Eventos de prueba enviados

Se enviaron 6 transacciones con el mismo `card_id` para superar el umbral de alta frecuencia.

```json
{
  "transaction_id": "cp05-burst-001",
  "card_id": "4532015112830999",
  "event_timestamp": "2026-05-16T18:00:01.000000+00:00",
  "produced_at": "2026-05-16T18:00:01.000000+00:00",
  "channel": "ONLINE",
  "amount": 100.00,
  "currency": "COP",
  "status": "APPROVED",
  "merchant_name": "CP05 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

```json
{
  "transaction_id": "cp05-burst-002",
  "card_id": "4532015112830999",
  "event_timestamp": "2026-05-16T18:00:02.000000+00:00",
  "produced_at": "2026-05-16T18:00:02.000000+00:00",
  "channel": "ONLINE",
  "amount": 101.00,
  "currency": "COP",
  "status": "APPROVED",
  "merchant_name": "CP05 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

```json
{
  "transaction_id": "cp05-burst-003",
  "card_id": "4532015112830999",
  "event_timestamp": "2026-05-16T18:00:03.000000+00:00",
  "produced_at": "2026-05-16T18:00:03.000000+00:00",
  "channel": "ONLINE",
  "amount": 102.00,
  "currency": "COP",
  "status": "APPROVED",
  "merchant_name": "CP05 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

```json
{
  "transaction_id": "cp05-burst-004",
  "card_id": "4532015112830999",
  "event_timestamp": "2026-05-16T18:00:04.000000+00:00",
  "produced_at": "2026-05-16T18:00:04.000000+00:00",
  "channel": "ONLINE",
  "amount": 103.00,
  "currency": "COP",
  "status": "APPROVED",
  "merchant_name": "CP05 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

```json
{
  "transaction_id": "cp05-burst-005",
  "card_id": "4532015112830999",
  "event_timestamp": "2026-05-16T18:00:05.000000+00:00",
  "produced_at": "2026-05-16T18:00:05.000000+00:00",
  "channel": "ONLINE",
  "amount": 104.00,
  "currency": "COP",
  "status": "APPROVED",
  "merchant_name": "CP05 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

```json
{
  "transaction_id": "cp05-burst-006",
  "card_id": "4532015112830999",
  "event_timestamp": "2026-05-16T18:00:06.000000+00:00",
  "produced_at": "2026-05-16T18:00:06.000000+00:00",
  "channel": "ONLINE",
  "amount": 105.00,
  "currency": "COP",
  "status": "APPROVED",
  "merchant_name": "CP05 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

Adicionalmente, se envió un evento de avance de watermark para cerrar la ventana de event-time y permitir que Flink emitiera la alerta.

```json
{
  "transaction_id": "cp05-watermark-advance-001",
  "card_id": "9999999999999999",
  "event_timestamp": "2026-05-16T18:02:00.000000+00:00",
  "produced_at": "2026-05-16T18:02:00.000000+00:00",
  "channel": "ONLINE",
  "amount": 100.00,
  "currency": "COP",
  "status": "APPROVED",
  "merchant_name": "CP05 Watermark Event",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

### Evidencia observada

En Kafka UI se observaron los eventos publicados en el topic `transactions-online`:

- `transactions-online`: 7 mensajes.
- 6 eventos de prueba con `transaction_id` desde `cp05-burst-001` hasta `cp05-burst-006`.
- 1 evento adicional `cp05-watermark-advance-001` para avanzar el watermark.
- Los 6 eventos de prueba usaron el mismo `card_id`: `4532015112830999`.

En Kafka UI se observó una alerta publicada en el topic `alerts`:

- `alerts`: 1 mensaje.
- `alert_type`: `BURST`.
- `card_id`: `4532015112830999`.
- `source_transaction_id`: `cp05-burst-006`.

La alerta observada fue:

```json
{
  "alert_id": "b7dbf7d0-6d8a-4867-b41a-5b63aa817b88",
  "card_id": "4532015112830999",
  "alert_type": "BURST",
  "triggered_at": 1778979938.578011222,
  "source_transaction_id": "cp05-burst-006"
}
```

En Flink UI se observó el job de anomalías en estado `RUNNING`:

- Job: `Fraud Detection, Anomalies Job`.
- Estado: `RUNNING`.

También se observó procesamiento de registros en los operadores del job:

- Source Kafka:
  - `Records Sent`: 7.
  - `Bytes Sent`: 3.24 KB.

- Operador `KeyedProcess`:
  - `Records Received`: 7.
  - `Bytes Received`: 1.42 KB.

- Operador `CepOperator`:
  - `Records Received`: 7.
  - `Bytes Received`: 1.42 KB.

- Operador `SlidingEventTimeWindows`:
  - `Records Received`: 7.
  - `Records Sent`: 1.
  - `Bytes Received`: 1.42 KB.
  - `Bytes Sent`: 282 B.

- Sink hacia salida:
  - `Records Received`: 1.
  - `Bytes Received`: 1.22 KB.

Además, el script generó los archivos de evidencia automática de la prueba.

### Resultado esperado

- Los 6 eventos de prueba se publican correctamente en `transactions-online`.
- Todos los eventos de prueba usan el mismo `card_id`.
- Los eventos caen dentro de una ventana de event-time de 1 minuto.
- El job de anomalías permanece en estado `RUNNING`.
- El Source de Flink consume los eventos desde Kafka.
- El operador de ventana procesa los eventos recibidos.
- Al superar el umbral de más de 5 eventos para la misma tarjeta, se genera una alerta de tipo `BURST`.
- La alerta se publica en el topic `alerts`.
- La alerta contiene el `card_id` esperado.
- No se presentan errores visibles que impidan el procesamiento o la publicación de resultados.

### Resultado obtenido

La prueba fue ejecutada correctamente.

Los 6 eventos controlados fueron publicados en `transactions-online` con el mismo `card_id`. El job de anomalías de Flink permaneció en estado `RUNNING` y procesó los registros. Inicialmente la alerta no se generó hasta que se envió un evento adicional con timestamp posterior para avanzar el watermark y permitir el cierre de la ventana de event-time.

Después del evento de avance de watermark, Kafka UI confirmó la publicación de una alerta en el topic `alerts` con tipo `BURST`, asociada al `card_id` `4532015112830999` y al `source_transaction_id` `cp05-burst-006`.

El script final de CP-05 incorporó este comportamiento enviando automáticamente el evento adicional de avance de watermark, permitiendo validar correctamente la alerta de alta frecuencia.

### Conclusión

La prueba CP-05 fue aprobada.

Se comprobó que el job de anomalías detecta correctamente una alta frecuencia de transacciones asociadas a una misma tarjeta y publica una alerta funcional en el topic `alerts`.

La alerta `BURST` fue generada cuando se enviaron más de 5 eventos con el mismo `card_id` dentro de una ventana de 1 minuto. Se documentó además que, por tratarse de una ventana basada en event-time, fue necesario enviar un evento adicional con timestamp posterior para avanzar el watermark y cerrar la ventana.

### Evidencia asociada

- [Log de ejecución de la prueba CP-05](evidence/cp05_terminal_log.txt)
- [Eventos de entrada usados para la prueba CP-05](evidence/cp05_input_events.txt)
- [Log de validación automática sobre el topic `alerts`](evidence/cp05_alerts_validation_log.txt)
- [Captura de Kafka UI mostrando los eventos en `transactions-online`](evidence/cp05_kafka_ui_input.png)
- [Captura de Kafka UI mostrando la alerta `BURST` en `alerts`](evidence/cp05_kafka_ui_alerts.png)
- [Captura de Flink UI mostrando el job de anomalías en ejecución](evidence/cp05_flink_job.png)

---

## CP-06 — Validación funcional de alerta por fuerza bruta

**Bloque:** Pruebas funcionales  
**Estado:** Aprobada  
**Fecha de ejecución:** 2026-05-16  
**Entorno:** Ejecución local sobre Docker Compose, con Kafka, Kafka UI y Apache Flink activos

### Objetivo

Verificar que el job de anomalías de Apache Flink detecte un patrón de fuerza bruta basado en múltiples transacciones rechazadas seguidas de una transacción aprobada, y publique una alerta funcional en el tópico `alerts`.

### Precondiciones

- Stack levantado con Docker Compose.
- Servicio Kafka activo.
- Kafka UI disponible en `http://localhost:8080`.
- Flink UI disponible en `http://localhost:8081`.
- Topic `transactions-online` disponible.
- Topic `alerts` disponible.
- Job de anomalías disponible y ejecutándose en Apache Flink.
- Servicio `flink-anomalies-job` corregido y operativo.
- Entorno local actualizado con los cambios necesarios para ejecutar correctamente el job `Fraud Detection, Anomalies Job`.

### Procedimiento ejecutado

1. Se verificó que el job `Fraud Detection, Anomalies Job` estuviera en estado `RUNNING` en Flink UI.
2. Se limpiaron los topics `transactions-online` y `alerts` para iniciar la prueba desde cero.
3. Se ejecutó el script automatizado de CP-06.
4. El script levantó o verificó los servicios necesarios para la prueba:
   - `kafka`
   - `kafka-ui`
   - `flink-jobmanager`
   - `flink-taskmanager`
   - `flink-anomalies-job`
5. El script publicó una secuencia controlada de eventos en el topic `transactions-online`.
6. La secuencia enviada incluyó tres eventos con estado `REJECTED`, seguidos por un evento con estado `APPROVED`, todos asociados al mismo `card_id`.
7. Se envió un quinto evento adicional para avanzar el watermark, debido a que durante la ejecución manual se observó que la alerta requería progreso de event-time para emitirse correctamente.
8. Se esperó el procesamiento de los eventos por parte del job de anomalías.
9. Se validó la salida en Kafka UI y Flink UI.
10. Se confirmó la generación de una alerta en el topic `alerts`.

### Comando ejecutado

```powershell
.\scripts\run_cp06_bruteforce_alert_test.ps1
```

### Eventos de prueba enviados

Se enviaron cuatro eventos principales con el mismo `card_id`, siguiendo el patrón esperado de fuerza bruta:

```text
REJECTED → REJECTED → REJECTED → APPROVED
```

#### Evento 1

```json
{
  "transaction_id": "cp06-bruteforce-001",
  "card_id": "4532015112830666",
  "event_timestamp": "2026-05-16T18:10:01.000000+00:00",
  "produced_at": "2026-05-16T18:10:01.000000+00:00",
  "channel": "ONLINE",
  "amount": 100.00,
  "currency": "COP",
  "status": "REJECTED",
  "merchant_name": "CP06 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

#### Evento 2

```json
{
  "transaction_id": "cp06-bruteforce-002",
  "card_id": "4532015112830666",
  "event_timestamp": "2026-05-16T18:10:02.000000+00:00",
  "produced_at": "2026-05-16T18:10:02.000000+00:00",
  "channel": "ONLINE",
  "amount": 101.00,
  "currency": "COP",
  "status": "REJECTED",
  "merchant_name": "CP06 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

#### Evento 3

```json
{
  "transaction_id": "cp06-bruteforce-003",
  "card_id": "4532015112830666",
  "event_timestamp": "2026-05-16T18:10:03.000000+00:00",
  "produced_at": "2026-05-16T18:10:03.000000+00:00",
  "channel": "ONLINE",
  "amount": 102.00,
  "currency": "COP",
  "status": "REJECTED",
  "merchant_name": "CP06 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

#### Evento 4

```json
{
  "transaction_id": "cp06-bruteforce-004",
  "card_id": "4532015112830666",
  "event_timestamp": "2026-05-16T18:10:04.000000+00:00",
  "produced_at": "2026-05-16T18:10:04.000000+00:00",
  "channel": "ONLINE",
  "amount": 103.00,
  "currency": "COP",
  "status": "APPROVED",
  "merchant_name": "CP06 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

Adicionalmente, se envió un evento de avance de watermark para permitir que Flink completara el procesamiento basado en event-time.

```json
{
  "transaction_id": "cp06-watermark-advance-001",
  "card_id": "4532015112830666",
  "event_timestamp": "2026-05-16T18:10:20.000000+00:00",
  "produced_at": "2026-05-16T18:10:20.000000+00:00",
  "channel": "ONLINE",
  "amount": 104.00,
  "currency": "COP",
  "status": "APPROVED",
  "merchant_name": "CP06 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

### Evidencia observada

En Kafka UI se observaron los eventos publicados en el topic `transactions-online`:

- `transactions-online`: 5 mensajes.
- 4 eventos principales con `transaction_id` desde `cp06-bruteforce-001` hasta `cp06-bruteforce-004`.
- 1 evento adicional `cp06-watermark-advance-001` para avanzar el watermark.
- Todos los eventos usaron el mismo `card_id`: `4532015112830666`.

En Kafka UI se observó una alerta publicada en el topic `alerts`:

- `alerts`: 1 mensaje.
- `alert_type`: `DECLINED_PATTERN`.
- `card_id`: `4532015112830666`.
- `source_transaction_id`: `cp06-bruteforce-004`.

La alerta observada fue:

```json
{
  "alert_id": "11805988-bd68-48e4-a1a7-0ccbe0ed6215",
  "card_id": "4532015112830666",
  "alert_type": "DECLINED_PATTERN",
  "triggered_at": 1778984709.659518615,
  "source_transaction_id": "cp06-bruteforce-004"
}
```

En Flink UI se observó el job de anomalías en estado `RUNNING`:

- Job: `Fraud Detection, Anomalies Job`.
- Estado: `RUNNING`.

También se observó procesamiento de registros en los operadores del job:

- Source Kafka:
  - `Records Sent`: 5.
  - `Bytes Sent`: 2.37 KB.

- Operador `KeyedProcess`:
  - `Records Received`: 5.
  - `Bytes Received`: 965 B.

- Operador `CepOperator`:
  - `Records Received`: 5.
  - `Records Sent`: 1.
  - `Bytes Received`: 965 B.
  - `Bytes Sent`: 288 B.

- Operador `SlidingEventTimeWindows`:
  - `Records Received`: 5.
  - `Bytes Received`: 965 B.

- Sink hacia salida:
  - `Records Received`: 1.
  - `Bytes Received`: 656 B.

Además, el script generó los archivos de evidencia automática de la prueba.

### Resultado esperado

- Los eventos de prueba se publican correctamente en `transactions-online`.
- Los eventos usan el mismo `card_id`.
- La secuencia de estados enviada corresponde al patrón `REJECTED`, `REJECTED`, `REJECTED`, `APPROVED`.
- Los eventos caen dentro del límite temporal de 10 minutos definido para el patrón CEP.
- El job de anomalías permanece en estado `RUNNING`.
- El Source de Flink consume los eventos desde Kafka.
- El operador CEP procesa la secuencia recibida.
- Al detectar el patrón, se genera una alerta de tipo `DECLINED_PATTERN`.
- La alerta se publica en el topic `alerts`.
- La alerta contiene el `card_id` esperado y referencia como `source_transaction_id` al evento aprobado `cp06-bruteforce-004`.
- No se presentan errores visibles que impidan el procesamiento o la publicación de resultados.

### Resultado obtenido

La prueba fue ejecutada correctamente.

Los eventos controlados fueron publicados en `transactions-online` con el mismo `card_id`. El job de anomalías de Flink permaneció en estado `RUNNING` y procesó los registros. Se envió un evento adicional con timestamp posterior para avanzar el watermark, ya que durante la ejecución manual se observó que era necesario para que el job emitiera la alerta.

Kafka UI confirmó la publicación de una alerta en el topic `alerts` con tipo `DECLINED_PATTERN`, asociada al `card_id` `4532015112830666` y al `source_transaction_id` `cp06-bruteforce-004`.

El script de CP-06 incorporó este comportamiento enviando automáticamente el evento adicional de avance de watermark, permitiendo validar correctamente la alerta de fuerza bruta.

### Conclusión

La prueba CP-06 fue aprobada.

Se comprobó que el job de anomalías detecta correctamente el patrón de fuerza bruta compuesto por tres transacciones rechazadas seguidas de una transacción aprobada para la misma tarjeta, y publica una alerta funcional en el topic `alerts`.

La alerta `DECLINED_PATTERN` fue generada correctamente y asociada al evento `cp06-bruteforce-004`.

### Evidencia asociada

- [Log de ejecución de la prueba CP-06](evidence/cp06_terminal_log.txt)
- [Eventos de entrada usados para la prueba CP-06](evidence/cp06_input_events.txt)
- [Log de validación automática sobre el topic `alerts`](evidence/cp06_alerts_validation_log.txt)
- [Captura de Kafka UI mostrando los eventos en `transactions-online`](evidence/cp06_kafka_ui_input.png)
- [Captura de Kafka UI mostrando la alerta `DECLINED_PATTERN` en `alerts`](evidence/cp06_kafka_ui_alerts.png)
- [Captura de Flink UI mostrando el job de anomalías en ejecución](evidence/cp06_flink_job.png)

---

## CP-07 — Validación de envío de mensajes inválidos a DLQ

**Bloque:** Pruebas de manejo de errores  
**Estado:** Aprobada  
**Fecha de ejecución:** 2026-05-16  
**Entorno:** Ejecución local sobre Docker Compose, con Kafka, Kafka UI y Apache Flink activos

### Objetivo

Verificar que el sistema detecte un mensaje inválido publicado en un topic de entrada y lo redirija correctamente al tópico `transactions-dlq`, sin afectar la ejecución del job de anomalías.

### Precondiciones

- Stack levantado con Docker Compose.
- Servicio Kafka activo.
- Kafka UI disponible en `http://localhost:8080`.
- Flink UI disponible en `http://localhost:8081`.
- Topic `transactions-online` disponible.
- Topic `transactions-dlq` disponible.
- Job de anomalías disponible y ejecutándose en Apache Flink.
- Servicio `flink-anomalies-job` corregido y operativo.
- Entorno local actualizado con los cambios necesarios para ejecutar correctamente el job `Fraud Detection, Anomalies Job`.

### Procedimiento ejecutado

1. Se verificó que el job `Fraud Detection, Anomalies Job` estuviera en estado `RUNNING` en Flink UI.
2. Se limpiaron los topics `transactions-online` y `transactions-dlq` para iniciar la prueba desde cero.
3. Se ejecutó el script automatizado de CP-07.
4. El script levantó o verificó los servicios necesarios para la prueba:
   - `kafka`
   - `kafka-ui`
   - `flink-jobmanager`
   - `flink-taskmanager`
   - `flink-anomalies-job`
5. El script publicó un mensaje inválido en el topic `transactions-online`.
6. El mensaje inválido fue procesado por el job de anomalías.
7. El mensaje fue rechazado por validación y enviado al topic `transactions-dlq`.
8. Se validó la salida en Kafka UI y Flink UI.
9. Se confirmó que el job de anomalías permaneció en estado `RUNNING`.

### Comando ejecutado

```powershell
.\scripts\run_cp07_dlq_invalid_message_test.ps1
```

### Mensaje inválido enviado

```text
cp07-invalid-key|{"transaction_id":"cp07-invalid-001","amount":"not-a-number","broken":true}
```

El mensaje fue enviado con la key:

```text
cp07-invalid-key
```

Y con el siguiente contenido:

```json
{
  "transaction_id": "cp07-invalid-001",
  "amount": "not-a-number",
  "broken": true
}
```

### Motivo de invalidez

El mensaje enviado no cumple el esquema mínimo esperado por el job de anomalías.

El error principal detectado fue:

```text
missing-or-invalid-field: card_id
```

Esto ocurrió porque el mensaje no incluye el campo obligatorio `card_id`.

Adicionalmente, el campo `amount` fue enviado como texto no numérico:

```json
"amount": "not-a-number"
```

El campo `amount` debería ser un valor numérico, por ejemplo:

```json
"amount": 100.00
```

Por estas razones, el mensaje no pudo convertirse en una transacción válida y fue enviado al topic `transactions-dlq`.

### Evidencia observada

En Kafka UI se observó el mensaje inválido publicado en el topic `transactions-online`:

- `transactions-online`: 1 mensaje.
- Key: `cp07-invalid-key`.
- `transaction_id`: `cp07-invalid-001`.
- `amount`: `not-a-number`.
- Campo adicional: `broken = true`.

En Kafka UI se observó un registro publicado en el topic `transactions-dlq`:

- `transactions-dlq`: 1 mensaje.
- `raw`: contiene el mensaje original inválido.
- `error`: `missing-or-invalid-field: card_id`.
- `topic`: `transactions-online`.
- `partition`: `1`.
- `offset`: `8786`.
- `key`: `cp07-invalid-key`.
- `receivedAt`: `2026-05-17T03:05:46.213599951Z`.

El registro observado en DLQ fue:

```json
{
  "raw": "{\"transaction_id\":\"cp07-invalid-001\",\"amount\":\"not-a-number\",\"broken\":true}",
  "error": "missing-or-invalid-field: card_id",
  "topic": "transactions-online",
  "partition": 1,
  "offset": 8786,
  "key": "cp07-invalid-key",
  "receivedAt": "2026-05-17T03:05:46.213599951Z"
}
```

En Flink UI se observó el job de anomalías en estado `RUNNING`:

- Job: `Fraud Detection, Anomalies Job`.
- Estado: `RUNNING`.

También se observó que el job no falló ni se reinició después de recibir el mensaje inválido. Los operadores principales permanecieron en ejecución. Para este caso es esperado que operadores del flujo normal, como `KeyedProcess`, `CepOperator` o `SlidingEventTimeWindows`, no procesen el mensaje inválido como una transacción válida, ya que el registro es redirigido hacia DLQ antes de entrar al flujo normal de anomalías.

Además, el script generó los archivos de evidencia automática de la prueba.

### Resultado esperado

- El mensaje inválido se publica correctamente en `transactions-online`.
- El job de anomalías permanece en estado `RUNNING`.
- El deserializador o validador detecta que el mensaje no cumple el esquema requerido.
- El mensaje inválido no ingresa al flujo normal de detección de anomalías.
- El mensaje inválido se redirige al topic `transactions-dlq`.
- El registro en `transactions-dlq` conserva el mensaje original en el campo `raw`.
- El registro en `transactions-dlq` incluye la razón del error.
- El registro en `transactions-dlq` incluye metadatos como `topic`, `partition`, `offset`, `key` y `receivedAt`.
- No se presentan errores visibles que detengan el job de anomalías.

### Resultado obtenido

La prueba fue ejecutada correctamente.

El mensaje inválido fue publicado en `transactions-online` y posteriormente enviado al topic `transactions-dlq`. Kafka UI confirmó la existencia del registro DLQ con el contenido original del mensaje, la key `cp07-invalid-key` y el error `missing-or-invalid-field: card_id`.

El job `Fraud Detection, Anomalies Job` permaneció en estado `RUNNING`, sin fallos ni reinicios visibles. El mensaje inválido no fue procesado por el flujo normal de anomalías, lo cual corresponde al comportamiento esperado para registros inválidos.

El script finalizó con el resultado:

```text
CP-07 validation: PASSED
Invalid message was routed to transactions-dlq.
```

### Conclusión

La prueba CP-07 fue aprobada.

Se comprobó que el sistema detecta correctamente mensajes inválidos publicados en `transactions-online` y los redirige al topic `transactions-dlq`, conservando el mensaje original, la razón del error y los metadatos de origen.

El comportamiento observado confirma que el pipeline maneja errores de entrada sin interrumpir el job de anomalías ni contaminar el flujo principal de procesamiento.

### Evidencia asociada

- [Log de ejecución de la prueba CP-07](evidence/cp07_terminal_log.txt)
- [Mensaje inválido usado para la prueba CP-07](evidence/cp07_input_invalid_message.txt)
- [Log de validación automática sobre el topic `transactions-dlq`](evidence/cp07_dlq_validation_log.txt)
- [Captura de Kafka UI mostrando el mensaje inválido en `transactions-online`](evidence/cp07_kafka_ui_input.png)
- [Captura de Kafka UI mostrando el registro en `transactions-dlq`](evidence/cp07_kafka_ui_dlq.png)
- [Captura de Flink UI mostrando el job de anomalías en ejecución](evidence/cp07_flink_job.png)

----

## CP-08 — Continuidad del pipeline después de errores

**Bloque:** Pruebas de manejo de errores  
**Estado:** Aprobada  
**Fecha de ejecución:** 2026-05-17  
**Entorno:** Ejecución local sobre Docker Compose, con Kafka, Kafka UI y Apache Flink activos

### Objetivo

Verificar que el pipeline continúe funcionando después de recibir un mensaje inválido, aislando el error en `transactions-dlq` y procesando correctamente mensajes válidos posteriores.

### Precondiciones

- Stack levantado con Docker Compose.
- Servicio Kafka activo.
- Kafka UI disponible en `http://localhost:8080`.
- Flink UI disponible en `http://localhost:8081`.
- Topic `transactions-online` disponible.
- Topic `transactions-dlq` disponible.
- Topic `alerts` disponible.
- Job de anomalías disponible y ejecutándose en Apache Flink.
- Servicio `flink-anomalies-job` corregido y operativo.
- Entorno local actualizado con los cambios necesarios para ejecutar correctamente el job `Fraud Detection, Anomalies Job`.

### Procedimiento ejecutado

1. Se verificó que el job `Fraud Detection, Anomalies Job` estuviera en estado `RUNNING` en Flink UI.
2. Se limpiaron los topics `transactions-online`, `transactions-dlq` y `alerts` para iniciar la prueba desde cero.
3. Se ejecutó el script automatizado de CP-08.
4. El script levantó o verificó los servicios necesarios para la prueba:
   - `kafka`
   - `kafka-ui`
   - `flink-jobmanager`
   - `flink-taskmanager`
   - `flink-anomalies-job`
5. El script publicó una primera transacción válida de alto monto en `transactions-online`.
6. El script publicó un mensaje inválido en `transactions-online`.
7. El script publicó una segunda transacción válida de alto monto en `transactions-online`.
8. Se validó que el mensaje inválido fuera enviado a `transactions-dlq`.
9. Se validó que las transacciones válidas generaran alertas `HIGH_AMOUNT` en `alerts`.
10. Se confirmó que el job de anomalías permaneció en estado `RUNNING`.

### Comando ejecutado

```powershell
.\scripts\run_cp08_pipeline_continuity_test.ps1
```

### Mensajes enviados

La prueba ejecutó la siguiente secuencia:

```text
mensaje válido antes del error → mensaje inválido → mensaje válido después del error
```

#### Mensaje válido antes del error

```json
{
  "transaction_id": "cp08-valid-before-error-001",
  "card_id": "4532015112830877",
  "event_timestamp": "2026-05-16T18:30:01.000000+00:00",
  "produced_at": "2026-05-16T18:30:01.000000+00:00",
  "channel": "ONLINE",
  "amount": 20000.00,
  "currency": "USD",
  "status": "APPROVED",
  "merchant_name": "CP08 Valid Before Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

#### Mensaje inválido

```text
cp08-invalid-key|{"transaction_id":"cp08-invalid-001","amount":"not-a-number","broken":true}
```

Contenido inválido:

```json
{
  "transaction_id": "cp08-invalid-001",
  "amount": "not-a-number",
  "broken": true
}
```

#### Mensaje válido después del error

```json
{
  "transaction_id": "cp08-valid-after-error-001",
  "card_id": "4532015112830888",
  "event_timestamp": "2026-05-16T18:30:10.000000+00:00",
  "produced_at": "2026-05-16T18:30:10.000000+00:00",
  "channel": "ONLINE",
  "amount": 20000.00,
  "currency": "USD",
  "status": "APPROVED",
  "merchant_name": "CP08 Valid After Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

### Motivo de invalidez del mensaje intermedio

El mensaje inválido no cumple el esquema mínimo esperado por el job de anomalías.

El error principal detectado fue:

```text
missing-or-invalid-field: card_id
```

Esto ocurrió porque el mensaje no incluye el campo obligatorio `card_id`.

Adicionalmente, el campo `amount` fue enviado como texto no numérico:

```json
"amount": "not-a-number"
```

Por estas razones, el mensaje no pudo convertirse en una transacción válida y fue enviado al topic `transactions-dlq`.

### Evidencia observada

En Kafka UI se observaron los tres mensajes publicados en el topic `transactions-online`:

- `cp08-valid-before-error-001`
- `cp08-invalid-001`
- `cp08-valid-after-error-001`

En Kafka UI se observó un registro publicado en el topic `transactions-dlq`:

- `transactions-dlq`: 1 mensaje.
- `raw`: contiene el mensaje original inválido.
- `error`: `missing-or-invalid-field: card_id`.
- `topic`: `transactions-online`.
- `partition`: `2`.
- `offset`: `11304`.
- `key`: `cp08-invalid-key`.
- `receivedAt`: `2026-05-17T17:31:46.532604462Z`.

El registro observado en DLQ fue:

```json
{
  "raw": "{\"transaction_id\":\"cp08-invalid-001\",\"amount\":\"not-a-number\",\"broken\":true}",
  "error": "missing-or-invalid-field: card_id",
  "topic": "transactions-online",
  "partition": 2,
  "offset": 11304,
  "key": "cp08-invalid-key",
  "receivedAt": "2026-05-17T17:31:46.532604462Z"
}
```

En Kafka UI se observaron dos alertas publicadas en el topic `alerts`:

#### Alerta antes del error

```json
{
  "alert_id": "d1838369-4e08-4f00-ba6d-400200c59550",
  "card_id": "4532015112830877",
  "alert_type": "HIGH_AMOUNT",
  "triggered_at": 1779039106.627632920,
  "source_transaction_id": "cp08-valid-before-error-001"
}
```

#### Alerta después del error

```json
{
  "alert_id": "2a72e4cb-11f1-4b68-b712-9c5415a33066",
  "card_id": "4532015112830888",
  "alert_type": "HIGH_AMOUNT",
  "triggered_at": 1779039106.628170681,
  "source_transaction_id": "cp08-valid-after-error-001"
}
```

En Flink UI se observó el job de anomalías en estado `RUNNING`:

- Job: `Fraud Detection, Anomalies Job`.
- Estado: `RUNNING`.

También se observó procesamiento de registros en los operadores del job:

- Source Kafka:
  - `Records Sent`: 2.
  - `Bytes Sent`: 1.07 KB.

- Operador `KeyedProcess`:
  - `Records Received`: 2.
  - `Records Sent`: 2.
  - `Bytes Received`: 1.03 KB.
  - `Bytes Sent`: 542 B.

- Operador `CepOperator`:
  - `Records Received`: 2.
  - `Bytes Received`: 1.03 KB.

- Operador `SlidingEventTimeWindows`:
  - `Records Received`: 2.
  - `Bytes Received`: 1.03 KB.

- Sink hacia salida:
  - `Records Received`: 2.
  - `Bytes Received`: 2.32 KB.

El mensaje inválido no ingresó al flujo normal de anomalías, sino que fue redirigido hacia `transactions-dlq`, lo cual corresponde al comportamiento esperado.

Además, el script generó los archivos de evidencia automática de la prueba.

### Resultado esperado

- El primer mensaje válido se publica correctamente en `transactions-online`.
- El primer mensaje válido genera una alerta `HIGH_AMOUNT` en `alerts`.
- El mensaje inválido se publica correctamente en `transactions-online`.
- El mensaje inválido es detectado como inválido y se redirige a `transactions-dlq`.
- El mensaje inválido no detiene ni reinicia el job de anomalías.
- El segundo mensaje válido se publica correctamente en `transactions-online`.
- El segundo mensaje válido genera una nueva alerta `HIGH_AMOUNT` en `alerts`.
- El job `Fraud Detection, Anomalies Job` permanece en estado `RUNNING`.
- El pipeline continúa funcionando después del error.

### Resultado obtenido

La prueba fue ejecutada correctamente.

El pipeline procesó una transacción válida antes del error y generó una alerta `HIGH_AMOUNT`. Luego recibió un mensaje inválido, lo redirigió correctamente al topic `transactions-dlq` con el error `missing-or-invalid-field: card_id`, y posteriormente procesó una nueva transacción válida, generando otra alerta `HIGH_AMOUNT`.

El job `Fraud Detection, Anomalies Job` permaneció en estado `RUNNING` durante toda la prueba, sin fallos ni reinicios visibles.

El script finalizó con el resultado:

```text
CP-08 validation: PASSED
Pipeline processed valid event before error, routed invalid message to DLQ, and processed valid event after error.
```

### Conclusión

La prueba CP-08 fue aprobada.

Se comprobó que el pipeline mantiene continuidad operativa después de recibir un mensaje inválido. El sistema aisló correctamente el error en `transactions-dlq` y continuó procesando mensajes válidos posteriores, generando alertas funcionales en `alerts`.

Este comportamiento confirma que el manejo de errores no interrumpe el flujo principal del job de anomalías.

### Evidencia asociada

- [Log de ejecución de la prueba CP-08](evidence/cp08_terminal_log.txt)
- [Mensajes usados para la prueba CP-08](evidence/cp08_input_messages.txt)
- [Log de validación automática sobre el topic `transactions-dlq`](evidence/cp08_dlq_validation_log.txt)
- [Log de validación automática sobre el topic `alerts`](evidence/cp08_alerts_validation_log.txt)
- [Captura de Kafka UI mostrando los mensajes en `transactions-online`](evidence/cp08_kafka_ui_input.png)
- [Captura de Kafka UI mostrando el registro en `transactions-dlq`](evidence/cp08_kafka_ui_dlq.png)
- [Captura de Kafka UI mostrando las alertas `HIGH_AMOUNT` en `alerts`](evidence/cp08_kafka_ui_alerts.png)
- [Captura de Flink UI mostrando el job de anomalías en ejecución](evidence/cp08_flink_job.png)

---

## CP-09 — Throughput mínimo sostenido

**Bloque:** Pruebas de rendimiento  
**Estado:** Aprobada  
**Fecha de ejecución:** 2026-05-18  
**Entorno:** Ejecución local sobre Docker Compose, con Kafka, Kafka UI y Apache Flink activos

### Objetivo

Verificar que el sistema sostenga un throughput mínimo combinado de **100 eventos por segundo** entre los productores `ONLINE` y `POS`, cumpliendo el requerimiento de generación mínima del proyecto.

### Precondiciones

- Stack levantado con Docker Compose.
- Servicio Kafka activo.
- Kafka UI disponible en `http://localhost:8080`.
- Flink UI disponible en `http://localhost:8081`.
- Productores `ONLINE` y `POS` disponibles.
- Topics `transactions-online` y `transactions-pos` disponibles.
- Jobs de Flink activos para observar el comportamiento del pipeline durante la carga.
- Carpeta `docs/evidence` disponible para almacenar logs, resumen y capturas.

### Procedimiento ejecutado

1. Se limpiaron los topics principales para iniciar la prueba con evidencia clara.
2. Se verificó que Kafka, Kafka UI y los jobs de Flink estuvieran activos.
3. Se ejecutó el script automatizado de CP-09.
4. El script levantó o verificó los servicios necesarios para la prueba:
   - `kafka`
   - `kafka-ui`
   - `flink-jobmanager`
   - `flink-taskmanager`
   - `flink-anomalies-job`
   - `flink-aggregation-job`
5. El script registró los offsets iniciales de los topics:
   - `transactions-online`
   - `transactions-pos`
6. El script ejecutó en paralelo los productores:
   - `producer_online.py`
   - `producer_pos.py`
7. Cada productor fue configurado con una tasa de `60 eventos/s`.
8. La duración de la prueba fue de `120 segundos`.
9. Se registraron los logs de salida de ambos productores.
10. Se registraron los offsets finales de Kafka.
11. Se calculó el throughput combinado observado.
12. Se validó el resultado en Kafka UI y Flink UI.

### Comando ejecutado

```powershell
.\scripts\run_cp09_throughput_minimum_test.ps1
```

### Configuración de la prueba

```text
Productor ONLINE:
rate configurado: 60 eventos/s
duration: 120 segundos

Productor POS:
rate configurado: 60 eventos/s
duration: 120 segundos

Throughput configurado total:
120 eventos/s

Criterio mínimo requerido:
100 eventos/s combinados
```

### Evidencia observada

En Kafka UI se observaron mensajes publicados en los topics de entrada:

- `transactions-online`: 6590 mensajes.
- `transactions-pos`: 6593 mensajes.

Total de eventos observados en Kafka:

```text
6590 + 6593 = 13183 eventos
```

Throughput combinado calculado con base en Kafka UI:

```text
13183 eventos / 120 segundos = 109.85 eventos/s
```

Resultado del script:

```text
CP-09 validation: PASSED
Combined throughput is >= 100 events/s.
```

Durante la prueba también se observaron mensajes en otros topics:

- `alerts`: 681 mensajes.
- `transactions-dlq`: 148 mensajes.
- `transactions-aggregated`: 86 mensajes.

Esto ocurrió porque los jobs de Flink estaban activos y los productores generan tráfico realista con una proporción pequeña de anomalías e inválidos. Este comportamiento no invalida la prueba, ya que el objetivo principal de CP-09 era validar el throughput mínimo sostenido de entrada en `transactions-online` y `transactions-pos`.

### Resultado esperado

- El productor `ONLINE` publica eventos en `transactions-online`.
- El productor `POS` publica eventos en `transactions-pos`.
- Kafka recibe mensajes en ambos topics durante toda la prueba.
- El throughput combinado real observado es mayor o igual a `100 eventos/s`.
- No se presentan fallos críticos en los productores.
- No se presenta caída visible de Kafka, Kafka UI o Flink.
- Los jobs de Flink permanecen activos durante la prueba.
- Se genera evidencia numérica del volumen enviado y recibido.

### Resultado obtenido

La prueba fue ejecutada correctamente.

Los productores `ONLINE` y `POS` generaron carga de manera sostenida durante 120 segundos. Kafka recibió mensajes en los topics `transactions-online` y `transactions-pos`. El total observado fue de 13.183 eventos, equivalente a un throughput combinado aproximado de `109.85 eventos/s`.

El valor obtenido supera el mínimo requerido de `100 eventos/s`, por lo que la prueba fue aprobada.

Además, los jobs de Flink procesaron parte de la carga recibida, generando mensajes en `transactions-aggregated`, `alerts` y `transactions-dlq`, lo cual corresponde al comportamiento esperado cuando el pipeline completo está activo.

### Conclusión

La prueba CP-09 fue aprobada.

Se comprobó que el sistema sostiene un throughput combinado superior a `100 eventos/s` entre los productores `ONLINE` y `POS`. El resultado obtenido demuestra que el pipeline cumple con el requerimiento mínimo de generación de eventos establecido para el proyecto.

### Evidencia asociada

- [Log general de ejecución de la prueba CP-09](evidence/cp09_terminal_log.txt)
- [Log del productor ONLINE usado en CP-09](evidence/cp09_online_terminal_log.txt)
- [Log del productor POS usado en CP-09](evidence/cp09_pos_terminal_log.txt)
- [Offsets de Kafka antes y después de CP-09](evidence/cp09_kafka_offsets_before_after.txt)
- [Resumen de throughput de CP-09](evidence/cp09_throughput_summary.txt)
- [Captura de Kafka UI mostrando los mensajes en los topics](evidence/cp09_kafka_ui_topics.png)

---

## CP-10 — Latencia end-to-end de alertas generadas por transacciones válidas

**Bloque:** Pruebas de rendimiento  
**Estado:** Aprobada  
**Fecha de ejecución:** 2026-05-18  
**Entorno:** Ejecución local sobre Docker Compose, con Kafka, Kafka UI y Apache Flink activos

### Objetivo

Medir la latencia end-to-end del pipeline desde que se produce una transacción válida que genera alerta hasta que dicha alerta aparece en el topic `alerts`, verificando que la latencia promedio sea menor a **3 segundos**.

### Precondiciones

- Stack levantado con Docker Compose.
- Servicio Kafka activo.
- Kafka UI disponible en `http://localhost:8080`.
- Flink UI disponible en `http://localhost:8081`.
- Topic `transactions-online` disponible.
- Topic `alerts` disponible.
- Job `Fraud Detection, Anomalies Job` disponible y ejecutándose en Apache Flink.
- Carpeta `docs/evidence` disponible para almacenar logs, resultados y capturas.
- Topics limpiados o aislados antes de la ejecución para facilitar la correlación de resultados.

### Procedimiento ejecutado

1. Se limpiaron los topics `transactions-online` y `alerts` para iniciar la prueba con evidencia clara.
2. Se verificó que el job `Fraud Detection, Anomalies Job` estuviera en estado `RUNNING`.
3. Se ejecutó el script automatizado de CP-10.
4. El script levantó o verificó los servicios necesarios para la prueba:
   - `kafka`
   - `kafka-ui`
   - `flink-jobmanager`
   - `flink-taskmanager`
   - `flink-anomalies-job`
5. El script generó 20 transacciones válidas con monto alto.
6. Cada transacción fue publicada en el topic `transactions-online`.
7. Cada evento incluyó un `transaction_id` único y un campo `produced_at` generado justo antes del envío.
8. El job de anomalías procesó las transacciones y generó alertas `HIGH_AMOUNT`.
9. El script consumió las alertas desde el topic `alerts`.
10. Se correlacionó cada alerta con su evento original usando el campo `source_transaction_id`.
11. Se calculó la latencia por evento usando la fórmula:

```text
latencia_ms = (triggered_at - produced_at_epoch) * 1000
```

12. Se calcularon las métricas de latencia mínima, promedio y máxima.
13. Se validó el resultado en Kafka UI y Flink UI.

### Comando ejecutado

```powershell
.\scripts\run_cp10_end_to_end_latency_alerts_test.ps1
```

### Configuración de la prueba

```text
Cantidad de muestras: 20
Topic de entrada: transactions-online
Topic de salida observable: alerts
Tipo de alerta esperada: HIGH_AMOUNT
Monto usado por evento: 20000.00
Moneda: USD
Estado de transacción: APPROVED
Criterio de aceptación: latencia promedio < 3000 ms
```

### Eventos de prueba enviados

Se enviaron 20 transacciones válidas con el siguiente patrón:

```json
{
  "transaction_id": "cp10-latency-001",
  "card_id": "4532015112831001",
  "event_timestamp": "timestamp generado al momento de envío",
  "produced_at": "timestamp generado al momento de envío",
  "channel": "ONLINE",
  "amount": 20000.00,
  "currency": "USD",
  "status": "APPROVED",
  "merchant_name": "CP10 Test Merchant",
  "merchant_category": "electronics",
  "city": "Bogota",
  "country": "CO"
}
```

Los identificadores usados fueron:

```text
cp10-latency-001
cp10-latency-002
cp10-latency-003
...
cp10-latency-020
```

### Evidencia observada

En Kafka UI se observaron mensajes publicados en los topics:

- `transactions-online`: 20 mensajes.
- `alerts`: 20 mensajes.
- `transactions-dlq`: 0 mensajes.
- `transactions-pos`: 0 mensajes.

Esto confirma que las transacciones fueron válidas, no fueron enviadas a DLQ y generaron alertas en el topic esperado.

En Flink UI se observó el job de anomalías en estado `RUNNING`:

- Job: `Fraud Detection, Anomalies Job`.
- Estado: `RUNNING`.

También se observó procesamiento de registros en los operadores del job:

- Source Kafka:
  - `Records Sent`: 20.
  - `Bytes Sent`: 9.75 KB.

- Operador `KeyedProcess`:
  - `Records Received`: 20.
  - `Records Sent`: 20.
  - `Bytes Received`: 3.51 KB.
  - `Bytes Sent`: 4.98 KB.

- Operador `CepOperator`:
  - `Records Received`: 20.
  - `Bytes Received`: 3.51 KB.

- Operador `SlidingEventTimeWindows`:
  - `Records Received`: 20.
  - `Bytes Received`: 3.51 KB.

- Sink hacia salida:
  - `Records Received`: 20.
  - `Bytes Received`: 3.81 KB.

El resumen de latencia generado por el script fue:

```text
CP-10 - Latency summary

Samples expected: 20
Samples matched: 20
Min latency ms: 1346.84
Avg latency ms: 1877.70
Max latency ms: 4013.55
Criterion: average latency < 3000 ms
Result: PASSED
```

### Resultado esperado

- Las 20 transacciones válidas se publican correctamente en `transactions-online`.
- Cada transacción genera una alerta `HIGH_AMOUNT` en `alerts`.
- Cada alerta se puede correlacionar con su transacción original mediante `source_transaction_id`.
- El job de anomalías permanece en estado `RUNNING`.
- No se generan mensajes en `transactions-dlq`.
- Se calcula la latencia end-to-end para cada muestra.
- La latencia promedio calculada es menor a `3000 ms`.

### Resultado obtenido

La prueba fue ejecutada correctamente.

Se enviaron 20 transacciones válidas de alto monto al topic `transactions-online`. El job `Fraud Detection, Anomalies Job` procesó los eventos y generó 20 alertas `HIGH_AMOUNT` en el topic `alerts`.

Todas las alertas fueron correlacionadas correctamente con sus eventos originales mediante `source_transaction_id`.

La latencia promedio end-to-end fue de `1877.70 ms`, equivalente a aproximadamente `1.88 segundos`, por debajo del umbral requerido de `3000 ms`.

Aunque la latencia máxima observada fue de `4013.55 ms`, el criterio de aceptación definido para esta prueba corresponde a la latencia promedio, por lo cual la prueba fue aprobada.

El script finalizó con el resultado:

```text
CP-10 validation: PASSED
Average end-to-end latency is below 3000 ms.
```

### Conclusión

La prueba CP-10 fue aprobada.

Se comprobó que el pipeline genera alertas `HIGH_AMOUNT` a partir de transacciones válidas y que la latencia promedio end-to-end entre la producción del evento y la emisión de la alerta es menor a 3 segundos.

El resultado obtenido cumple con el criterio de rendimiento definido para el proyecto.

### Observación técnica

La latencia se midió sobre transacciones válidas que generan alerta `HIGH_AMOUNT`, ya que esta salida es inmediata y correlacionable mediante `source_transaction_id`.

Para esta prueba no se usaron transacciones normales sin alerta, porque en el estado actual del pipeline la salida individual observable para este tipo de transacciones depende de la integración de persistencia o visualización. Por ello, CP-10 se enfocó en una salida observable inmediata del job de anomalías.

### Evidencia asociada

- [Log general de ejecución de la prueba CP-10](evidence/cp10_terminal_log.txt)
- [Eventos de entrada usados en CP-10](evidence/cp10_input_events.txt)
- [Log de validación de alertas CP-10](evidence/cp10_alerts_validation_log.txt)
- [Resultados detallados de latencia CP-10](evidence/cp10_latency_results.csv)
- [Resumen de latencia CP-10](evidence/cp10_latency_summary.txt)
- [Captura de Kafka UI mostrando las alertas generadas](evidence/cp10_kafka_ui_alerts.png)
- [Captura de Flink UI mostrando el job de anomalías en ejecución](evidence/cp10_flink_job.png)

---

## CP-11 — Throughput incremental

**Bloque:** Pruebas de rendimiento  
**Estado:** Aprobada  
**Fecha de ejecución:** 2026-05-18  
**Entorno:** Ejecución local sobre Docker Compose, con Kafka, Kafka UI y Apache Flink activos

### Objetivo

Identificar el comportamiento del sistema bajo carga incremental, midiendo el throughput sostenido alcanzado en diferentes niveles de carga y determinando el punto donde empieza a presentarse degradación.

### Precondiciones

- Stack levantado con Docker Compose.
- Servicio Kafka activo.
- Kafka UI disponible en `http://localhost:8080`.
- Flink UI disponible en `http://localhost:8081`.
- Productores `ONLINE` y `POS` disponibles.
- Topics `transactions-online` y `transactions-pos` disponibles.
- Jobs de Flink activos:
  - `Fraud Detection, Anomalies Job`.
  - `insert-into_default_catalog.default_database.aggregation_results`.
- Carpeta `docs/evidence` disponible para almacenar logs, resultados y capturas.
- Topics limpiados o aislados antes de la ejecución para facilitar la lectura de resultados por escalón.

### Procedimiento ejecutado

1. Se limpiaron los topics principales para iniciar la prueba con evidencia clara.
2. Se verificó que Kafka, Kafka UI y los jobs de Flink estuvieran activos.
3. Se ejecutó el script automatizado de CP-11.
4. El script levantó o verificó los servicios requeridos:
   - `kafka`
   - `kafka-ui`
   - `flink-jobmanager`
   - `flink-taskmanager`
   - `flink-anomalies-job`
   - `flink-aggregation-job`
5. Se ejecutaron tres escalones de carga incremental.
6. En cada escalón se ejecutaron en paralelo los productores:
   - `producer_online.py`
   - `producer_pos.py`
7. Se registraron los logs de salida de ambos productores por escalón.
8. Se calcularon los eventos enviados, la tasa promedio por productor, el throughput combinado y el porcentaje de cumplimiento frente a la tasa configurada.
9. Se observaron los topics en Kafka UI para confirmar la recepción de mensajes.
10. Se observaron los jobs en Flink UI para confirmar continuidad de procesamiento y estado `RUNNING`.
11. Se documentó el throughput máximo sostenido observado y el punto de degradación.

### Comando ejecutado

```powershell
.\scripts\run_cp11_incremental_throughput_test.ps1
```

### Configuración de la prueba

```text
Escalón 1:
ONLINE: 100 eventos/s
POS: 100 eventos/s
Total configurado: 200 eventos/s

Escalón 2:
ONLINE: 150 eventos/s
POS: 150 eventos/s
Total configurado: 300 eventos/s

Escalón 3:
ONLINE: 200 eventos/s
POS: 200 eventos/s
Total configurado: 400 eventos/s

Duración por escalón: 120 segundos
```

### Resultados obtenidos

#### Escalón 1

```text
Configurado ONLINE: 100 eventos/s
Configurado POS: 100 eventos/s
Configurado total: 200 eventos/s
Duración: 120 segundos

ONLINE:
Total enviados: 10823
Fallos: 0
Tasa promedio: 90.0 eventos/s

POS:
Total enviados: 10812
Fallos: N/A
Tasa promedio: 89.9 eventos/s

Total combinado:
Total eventos: 21635
Throughput por suma de tasas: 179.9 eventos/s
Throughput por total/duración: 180.29 eventos/s
Cumplimiento frente a tasa configurada: 89.95 %
Diferencia frente a tasa configurada: 20.1 eventos/s
Estado: estable con degradación moderada
```

#### Escalón 2

```text
Configurado ONLINE: 150 eventos/s
Configurado POS: 150 eventos/s
Configurado total: 300 eventos/s
Duración: 120 segundos

ONLINE:
Total enviados: 13489
Fallos: 0
Tasa promedio: 112.4 eventos/s

POS:
Total enviados: 13494
Fallos: N/A
Tasa promedio: 112.4 eventos/s

Total combinado:
Total eventos: 26983
Throughput por suma de tasas: 224.8 eventos/s
Throughput por total/duración: 224.86 eventos/s
Cumplimiento frente a tasa configurada: 74.93 %
Diferencia frente a tasa configurada: 75.2 eventos/s
Estado: degradado
```

#### Escalón 3

```text
Configurado ONLINE: 200 eventos/s
Configurado POS: 200 eventos/s
Configurado total: 400 eventos/s
Duración: 120 segundos

ONLINE:
Total enviados: 16920
Fallos: 0
Tasa promedio: 140.9 eventos/s

POS:
Total enviados: 16914
Fallos: N/A
Tasa promedio: 140.9 eventos/s

Total combinado:
Total eventos: 33834
Throughput por suma de tasas: 281.8 eventos/s
Throughput por total/duración: 281.95 eventos/s
Cumplimiento frente a tasa configurada: 70.45 %
Diferencia frente a tasa configurada: 118.2 eventos/s
Estado: degradado
```

### Comparación general

| Escalón | Carga configurada | Throughput real observado | Cumplimiento | Diferencia | Estado |
|---|---:|---:|---:|---:|---|
| Escalón 1 | 200 eventos/s | 179.9 eventos/s | 89.95 % | 20.1 eventos/s | Estable con degradación moderada |
| Escalón 2 | 300 eventos/s | 224.8 eventos/s | 74.93 % | 75.2 eventos/s | Degradado |
| Escalón 3 | 400 eventos/s | 281.8 eventos/s | 70.45 % | 118.2 eventos/s | Degradado |

### Evidencia observada

En Kafka UI se observaron incrementos progresivos en los topics principales:

- `transactions-online`.
- `transactions-pos`.

También se observaron mensajes en:

- `alerts`.
- `transactions-dlq`.
- `transactions-aggregated`.

Esto es esperado porque los productores generan tráfico realista, incluyendo una pequeña proporción de anomalías y registros inválidos. Por ello, los jobs de Flink generan alertas, registros DLQ y resultados agregados durante la prueba.

En Flink UI se observó que los jobs permanecieron en estado `RUNNING` durante los escalones:

- `Fraud Detection, Anomalies Job`.
- `insert-into_default_catalog.default_database.aggregation_results`.

En los escalones de mayor carga se observaron señales visuales de presión o degradación en el job de anomalías, especialmente en operadores del flujo. Sin embargo, el job no falló ni se canceló durante la ejecución.

### Resultado esperado

- El sistema debe recibir carga creciente desde los productores `ONLINE` y `POS`.
- Kafka debe recibir mensajes en `transactions-online` y `transactions-pos` en cada escalón.
- Los productores deben finalizar sin errores críticos.
- Los jobs de Flink deben mantenerse en estado `RUNNING`.
- La prueba debe permitir identificar el throughput máximo sostenido aproximado.
- La prueba debe permitir identificar el punto de degradación si ocurre.
- Se debe generar evidencia numérica y visual del comportamiento incremental.

### Resultado obtenido

La prueba fue ejecutada correctamente.

El sistema procesó los tres escalones de carga incremental. En todos los escalones los productores finalizaron sin fallos críticos y los jobs de Flink permanecieron en estado `RUNNING`.

El mayor throughput sostenido observado fue de **281.8 eventos/s** durante el Escalón 3.

Sin embargo, se identificó degradación progresiva a medida que aumentó la carga configurada. El punto de degradación clara apareció desde el Escalón 2, donde el cumplimiento frente a la tasa configurada bajó a **74.93 %**. En el Escalón 3 la degradación fue mayor, con un cumplimiento de **70.45 %** y una diferencia de **118.2 eventos/s** frente a los 400 eventos/s configurados.

El script finalizó con el resultado:

```text
CP-11 validation: PASSED
Maximum sustained throughput observed: 281.8 events/s.
```

### Conclusión

La prueba CP-11 fue aprobada.

Se comprobó que el sistema responde al incremento de carga y mantiene operación continua durante los tres escalones evaluados. La prueba permitió identificar un throughput máximo sostenido aproximado de **281.8 eventos/s** en el entorno local.

También se identificó degradación progresiva a partir del Escalón 2. Aunque el sistema continuó funcionando y los jobs permanecieron en estado `RUNNING`, la tasa real alcanzada quedó por debajo de la tasa configurada, especialmente en los escalones de 300 y 400 eventos/s.

Por lo tanto, CP-11 cumple su objetivo: documentar la capacidad incremental del sistema, el throughput máximo observado y el punto donde comienza la degradación.

### Observación técnica

La degradación observada puede estar influenciada tanto por la capacidad del pipeline como por las limitaciones del entorno local y de los productores usados para generar carga.

Los productores generan tráfico realista con una proporción de anomalías y registros inválidos. Por esta razón, durante la prueba también se generaron mensajes en `alerts`, `transactions-dlq` y `transactions-aggregated`. Este comportamiento no invalida CP-11, ya que el propósito de la prueba era evaluar el comportamiento incremental bajo carga y no aislar únicamente transacciones normales.

### Evidencia asociada

- [Log general de ejecución de CP-11](evidence/cp11_terminal_log.txt)
- [Resumen de resultados incrementales CP-11](evidence/cp11_incremental_results.txt)
- [Resultados incrementales en CSV CP-11](evidence/cp11_incremental_results.csv)
- [Log ONLINE Escalón 1](evidence/cp11_step1_online_log.txt)
- [Log POS Escalón 1](evidence/cp11_step1_pos_log.txt)
- [Log ONLINE Escalón 2](evidence/cp11_step2_online_log.txt)
- [Log POS Escalón 2](evidence/cp11_step2_pos_log.txt)
- [Log ONLINE Escalón 3](evidence/cp11_step3_online_log.txt)
- [Log POS Escalón 3](evidence/cp11_step3_pos_log.txt)
- [Captura Kafka UI Escalón 1](evidence/cp11_kafka_ui_step1.png)
- [Captura Kafka UI Escalón 2](evidence/cp11_kafka_ui_step2.png)
- [Captura Kafka UI Escalón 3](evidence/cp11_kafka_ui_step3.png)
- [Captura Flink anomalías Escalón 1](evidence/cp11_flink_anomalies_step1.png)
- [Captura Flink anomalías Escalón 2](evidence/cp11_flink_anomalies_step2.png)
- [Captura Flink anomalías Escalón 3](evidence/cp11_flink_anomalies_step3.png)
- [Captura Flink agregación Escalón 1](evidence/cp11_flink_aggregation_step1.png)
- [Captura Flink agregación Escalón 2](evidence/cp11_flink_aggregation_step2.png)
- [Captura Flink agregación Escalón 3](evidence/cp11_flink_aggregation_step3.png)

