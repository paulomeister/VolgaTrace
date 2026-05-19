# Plan de Casos de Prueba 

## Objetivo general
Definir los casos de prueba del proyecto para validar la funcionalidad del pipeline, el manejo de errores y el rendimiento del sistema de procesamiento de transacciones en tiempo real.

## Organización del plan de pruebas
Los casos de prueba se agrupan en tres bloques:

- **Pruebas funcionales**
- **Pruebas de manejo de errores**
- **Pruebas de rendimiento**

### ¿Por qué se agrupan así?
Esta organización permite evaluar el sistema de forma progresiva y lógica:

- Las **pruebas funcionales** validan que los productores, el procesamiento y la generación de alertas operen como se espera.
- Las **pruebas de manejo de errores** verifican que el sistema trate correctamente mensajes inválidos sin interrumpir el flujo normal.
- Las **pruebas de rendimiento** permiten medir capacidad, latencia y estabilidad bajo carga.

---

## 1. Pruebas funcionales

### Idea general del bloque
Este bloque busca comprobar que el sistema cumple con el flujo funcional esperado: publicación de eventos, generación de agregaciones y detección de anomalías. La idea es verificar el comportamiento observable del pipeline sin intervenir en la implementación interna.

---

### CP-01 — Producción de eventos ONLINE válidos

**Objetivo**  
Verificar que el productor del canal ONLINE publique mensajes en el tópico `transactions-online` usando el contrato de datos definido para el proyecto.

**Precondiciones**
- Docker Compose levantado.
- Kafka operativo.
- Kafka UI disponible.
- Topic `transactions-online` disponible.
- Productor ONLINE accesible desde el repositorio.
- Script de prueba `scripts/run_cp01_online_producer_test.ps1` disponible.
- Carpeta `docs/evidence` disponible para almacenar evidencias.

**Entrada / estímulo**  
Ejecución automatizada del productor ONLINE mediante script.

```powershell
.\scripts\run_cp01_online_producer_test.ps1
```

El script ejecuta el productor ONLINE con una tasa controlada:

```powershell
python .\producers\producer_online.py --rate 50 --duration 60
```

**Procedimiento**
1. Ejecutar el script automatizado de CP-01.
2. El script levanta o verifica los servicios requeridos: Kafka y Kafka UI.
3. El script registra los offsets iniciales del tópico `transactions-online`.
4. El script ejecuta el productor ONLINE con la tasa y duración definidas.
5. El script registra el log de salida del productor.
6. El script registra los offsets finales del tópico `transactions-online`.
7. El script calcula la diferencia de offsets para confirmar cuántos mensajes nuevos fueron publicados.
8. Verificar visualmente en Kafka UI que los mensajes aparecen en `transactions-online`.
9. Guardar las evidencias generadas en `docs/evidence`.

**Resultado esperado**
- Se publican mensajes en `transactions-online`.
- El productor finaliza sin errores críticos.
- Kafka registra nuevos mensajes en el tópico esperado.
- La cantidad de mensajes reportada por el productor coincide con el incremento observado por offsets en Kafka.
- No se presentan errores visibles de publicación que impidan el envío al tópico.
- En una ejecución aislada, no se esperan mensajes nuevos en tópicos no relacionados.

**Criterio de aceptación**  
La prueba es satisfactoria si el script finaliza correctamente, el productor ONLINE reporta mensajes enviados y Kafka registra nuevos mensajes en el tópico `transactions-online`. La validación se respalda con el log del productor, el resumen de validación, los offsets antes/después y la captura de Kafka UI.

---

### CP-02 — Producción de eventos POS válidos

**Objetivo**  
Verificar que el productor del canal POS publique mensajes en el tópico `transactions-pos` usando el contrato de datos definido para el proyecto.

**Precondiciones**
- Docker Compose levantado.
- Kafka operativo.
- Kafka UI disponible.
- Topic `transactions-pos` disponible.
- Productor POS accesible desde el repositorio.
- Script de prueba `scripts/run_cp02_pos_producer_test.ps1` disponible.
- Carpeta `docs/evidence` disponible para almacenar evidencias.

**Entrada / estímulo**  
Ejecución automatizada del productor POS mediante script.

```powershell
.\scripts\run_cp02_pos_producer_test.ps1
```

El script ejecuta el productor POS con una tasa controlada:

```powershell
python .\producers\producer_pos.py --rate 50 --duration 60
```

**Procedimiento**
1. Ejecutar el script automatizado de CP-02.
2. El script levanta o verifica los servicios requeridos: Kafka y Kafka UI.
3. El script registra los offsets iniciales del tópico `transactions-pos`.
4. El script ejecuta el productor POS con la tasa y duración definidas.
5. El script registra el log de salida del productor.
6. El script registra los offsets finales del tópico `transactions-pos`.
7. El script calcula la diferencia de offsets para confirmar cuántos mensajes nuevos fueron publicados.
8. Verificar visualmente en Kafka UI que los mensajes aparecen en `transactions-pos`.
9. Guardar las evidencias generadas en `docs/evidence`.

**Resultado esperado**
- Se publican mensajes en `transactions-pos`.
- El productor finaliza sin errores críticos.
- Kafka registra nuevos mensajes en el tópico esperado.
- La cantidad de mensajes reportada por el productor coincide con el incremento observado por offsets en Kafka.
- No se presentan errores visibles de publicación que impidan el envío al tópico.
- En una ejecución aislada, no se esperan mensajes nuevos en tópicos no relacionados.

**Criterio de aceptación**  
La prueba es satisfactoria si el script finaliza correctamente, el productor POS reporta mensajes enviados y Kafka registra nuevos mensajes en el tópico `transactions-pos`. La validación se respalda con el log del productor, el resumen de validación, los offsets antes/después y la captura de Kafka UI.

---

### CP-03 — Validación funcional de salida agregada en `transactions-aggregated`

**Objetivo**  
Verificar que el job de agregaciones produzca resultados en el tópico `transactions-aggregated`, conforme al requerimiento de agregaciones por ventana de 1 minuto.

**Precondiciones**
- Stack levantado.
- Job de agregaciones desplegado y en ejecución.
- Topic `transactions-aggregated` disponible.
- Productores accesibles desde el repositorio.

**Entrada / estímulo**  
Ejecución de los productores con generación de transacciones válidas durante un intervalo suficiente para que se consolide al menos una ventana de agregación.

**Procedimiento**
1. Ejecutar los productores disponibles.
2. Mantener flujo de eventos durante el tiempo requerido para generar agregaciones.
3. Revisar en Kafka UI el tópico `transactions-aggregated`.
4. Confirmar que se generan mensajes agregados como salida del procesamiento.

**Resultado esperado**
- Se publican mensajes en `transactions-aggregated`.
- Los mensajes agregados reflejan el procesamiento de los eventos de entrada.
- La salida es consistente con el comportamiento esperado de una ventana de agregación de 1 minuto.

**Criterio de aceptación**  
La prueba es satisfactoria si se observa salida agregada en el tópico correcto y dicha salida es coherente con los eventos procesados.

---

### CP-04 — Validación funcional de alerta por monto inusual

**Objetivo**  
Verificar que una transacción válida con monto suficientemente alto genere una alerta funcional en el tópico `alerts`.

**Precondiciones**
- Job de anomalías desplegado.
- Topic `alerts` disponible.
- Capacidad de publicar una transacción controlada válida en un tópico de entrada.

**Entrada / estímulo**  
Publicación de una transacción válida con monto que cumpla una condición detectada por la lógica actual de monto alto.

**Procedimiento**
1. Publicar una transacción controlada válida en `transactions-online` o `transactions-pos`.
2. Usar un monto que garantice detección según la lógica actual del sistema.
3. Esperar su procesamiento en Flink.
4. Revisar el tópico `alerts`.
5. Confirmar la generación de una alerta asociada a la transacción enviada.

**Resultado esperado**
- Se emite una alerta en `alerts`.
- La alerta corresponde a la transacción enviada y queda asociada por `card_id` y `source_transaction_id`.
- El pipeline continúa operando normalmente.

**Criterio de aceptación**  
La prueba pasa si la alerta se genera para la transacción controlada y puede relacionarse de forma trazable con el evento de entrada.

---

### CP-05 — Validación funcional de alerta por alta frecuencia

**Objetivo**  
Verificar que una secuencia rápida de más de 5 transacciones válidas del mismo `card_id` dentro de una ventana de 1 minuto produzca una alerta en el tópico `alerts`.

**Precondiciones**
- Job de anomalías activo.
- Topic `alerts` disponible.
- Posibilidad de enviar múltiples eventos controlados para una misma tarjeta.

**Entrada / estímulo**  
Secuencia rápida de más de 5 transacciones válidas con el mismo `card_id` dentro de una ventana temporal compatible con la lógica actual del sistema.

**Procedimiento**
1. Enviar más de 5 transacciones válidas del mismo `card_id` en un intervalo corto.
2. Esperar el procesamiento del job de anomalías.
3. Revisar el tópico `alerts`.
4. Verificar la aparición de una alerta asociada al `card_id` usado en la ráfaga.

**Resultado esperado**
- Se genera una alerta en `alerts` de tipo `BURST`.
- La alerta corresponde al patrón enviado para la misma tarjeta.

**Criterio de aceptación**  
La prueba se acepta si la ráfaga controlada produce una alerta trazable al `card_id` utilizado.

---

### CP-06 — Validación funcional de alerta por fuerza bruta

**Objetivo**  
Verificar que la secuencia de tres transacciones `REJECTED` consecutivas seguidas por una `APPROVED`, para la misma tarjeta, genere una alerta en el tópico `alerts`.

**Precondiciones**
- Job de anomalías activo.
- Topic `alerts` disponible.
- Posibilidad de enviar una secuencia controlada de eventos para un mismo `card_id`.

**Entrada / estímulo**  
Secuencia para un mismo `card_id`, en este orden:
1. `REJECTED`
2. `REJECTED`
3. `REJECTED`
4. `APPROVED`

**Procedimiento**
1. Publicar la secuencia en el orden definido para el mismo `card_id`.
2. Asegurar que la secuencia ocurra dentro de una ventana compatible con la lógica actual del sistema.
3. Esperar el procesamiento por Flink.
4. Revisar el tópico `alerts`.
5. Validar la generación de una alerta asociada al patrón enviado.

**Resultado esperado**
- Se produce una alerta en `alerts` de tipo `DECLINED_PATTERN`.
- La alerta corresponde a la secuencia definida para la misma tarjeta.

**Criterio de aceptación**  
La prueba es satisfactoria si el sistema detecta la secuencia completa y emite una alerta trazable al `card_id` utilizado.

---

## 2. Pruebas de manejo de errores

### Idea general del bloque
Este bloque busca verificar que el sistema sea capaz de tratar entradas inválidas de manera controlada. La idea es confirmar que los errores no rompan el pipeline, sino que sean aislados correctamente en la DLQ mientras el flujo principal continúa operando.

---

### CP-07 — Validación de envío de mensajes inválidos a DLQ

**Objetivo**  
Verificar que los mensajes inválidos consumidos por el job de anomalías sean redirigidos al tópico `transactions-dlq` sin entrar al flujo normal de procesamiento.

**Precondiciones**
- Job de anomalías activo.
- Topic `transactions-dlq` disponible.
- Capacidad de enviar mensajes inválidos a los tópicos de entrada.

**Entrada / estímulo**  
Mensajes inválidos en estas variantes:
- campos faltantes
- tipos inválidos
- timestamp inválido
- mensaje vacío o en blanco
- JSON malformado

**Procedimiento**
1. Enviar una muestra de cada variante inválida a `transactions-online` o `transactions-pos`.
2. Observar el comportamiento del job durante la prueba.
3. Revisar el tópico `transactions-dlq`.
4. Confirmar que los mensajes inválidos son enviados allí y no continúan como eventos válidos.

**Resultado esperado**
- Los mensajes inválidos no se procesan como válidos.
- Se redirigen a `transactions-dlq`.
- El job continúa operando durante la prueba.

**Criterio de aceptación**  
La prueba pasa si las variantes inválidas terminan en DLQ y no interrumpen el flujo normal de procesamiento de los mensajes válidos.

---

### CP-08 — Continuidad del pipeline después de errores

**Objetivo**  
Verificar que, tras recibir mensajes inválidos, el job de procesamiento continúe procesando correctamente los mensajes válidos posteriores.

**Precondiciones**
- Job de anomalías activo.
- Topic `transactions-dlq` disponible.
- Posibilidad de enviar mezcla de mensajes válidos e inválidos a los tópicos de entrada.

**Entrada / estímulo**  
Secuencia mixta:
- mensajes válidos
- mensajes inválidos
- nuevamente mensajes válidos

**Procedimiento**
1. Enviar mensajes válidos a `transactions-online` o `transactions-pos`.
2. Introducir mensajes inválidos en los mismos tópicos.
3. Continuar con mensajes válidos.
4. Observar el tópico `transactions-dlq` y el flujo de salida esperado para mensajes válidos.
5. Verificar continuidad del procesamiento.

**Resultado esperado**
- Los mensajes inválidos se envían a `transactions-dlq`.
- Los mensajes válidos posteriores siguen procesándose normalmente.
- El job no se detiene ni interrumpe el flujo general de procesamiento.

**Criterio de aceptación**  
La prueba se acepta si el sistema aísla los mensajes inválidos en DLQ y mantiene el procesamiento de los mensajes válidos posteriores.

---

## 3. Pruebas de rendimiento

### Idea general del bloque
Este bloque está orientado a medir el comportamiento del sistema bajo carga. La idea es verificar si el pipeline cumple los requisitos de throughput y latencia, además de identificar su estabilidad y punto de degradación en condiciones de uso intensivo.

---

### CP-09 — Throughput mínimo sostenido

**Objetivo**  
Verificar que el sistema sostenga un throughput mínimo de 100 eventos por segundo combinados entre ONLINE + POS, conforme al requerimiento del proyecto.

**Precondiciones**
- Pipeline completo en ejecución.
- Kafka operativo.
- Kafka UI disponible.
- Jobs de Flink en estado `RUNNING`.
- Productores ONLINE y POS disponibles.
- Topics `transactions-online` y `transactions-pos` disponibles.
- Script de prueba `scripts/run_cp09_minimum_throughput_test.ps1` disponible.
- Mecanismo de medición mediante logs de productores y observación en Kafka/Flink.
- Carpeta `docs/evidence` disponible para almacenar evidencias.

**Entrada / estímulo**  
Ejecución automatizada de carga sostenida con ambos productores.

```powershell
.\scripts\run_cp09_minimum_throughput_test.ps1
```

La carga configurada para la prueba es:

```text
ONLINE: 60 eventos/s
POS: 60 eventos/s
Total configurado: 120 eventos/s
Duración: 120 segundos
```

**Procedimiento**
1. Ejecutar el script automatizado de CP-09.
2. El script levanta o verifica Kafka, Kafka UI y los jobs de Flink requeridos.
3. El script ejecuta en paralelo los productores ONLINE y POS con la tasa configurada.
4. El script registra los logs de ambos productores.
5. El script calcula el throughput real combinado a partir de los eventos enviados y la duración.
6. El script valida que el throughput combinado sea mayor o igual a 100 eventos/s.
7. Verificar visualmente en Kafka UI la llegada de mensajes a `transactions-online` y `transactions-pos`.
8. Verificar en Flink UI que los jobs permanezcan en estado `RUNNING`.
9. Guardar las evidencias generadas en `docs/evidence`.

**Resultado esperado**
- El sistema mantiene procesamiento estable durante la prueba.
- Los productores finalizan sin fallos críticos.
- Kafka recibe mensajes en `transactions-online` y `transactions-pos`.
- Los jobs de Flink permanecen en estado `RUNNING`.
- El throughput real combinado alcanza al menos 100 eventos/s.
- Pueden generarse mensajes en `alerts`, `transactions-dlq` y `transactions-aggregated`, debido a que los productores generan tráfico realista con anomalías e inválidos en baja proporción.

**Criterio de aceptación**  
La prueba pasa si el throughput combinado real entre ONLINE + POS es mayor o igual a 100 eventos/s, sin caída crítica de servicios ni interrupción del pipeline durante el intervalo definido.

---

### CP-10 — Latencia end-to-end de alertas generadas por transacciones válidas

**Objetivo**  
Medir la latencia end-to-end desde `produced_at` en una transacción válida hasta la generación de su alerta correspondiente en el tópico `alerts`, verificando que la latencia promedio sea menor a 3 segundos.

**Precondiciones**
- Pipeline funcional extremo a extremo.
- Kafka operativo.
- Kafka UI disponible.
- Job `Fraud Detection, Anomalies Job` en estado `RUNNING`.
- Topic `transactions-online` disponible.
- Topic `alerts` disponible.
- Eventos con campo `produced_at`.
- Salida observable y correlacionable mediante `source_transaction_id`.
- Script de prueba `scripts/run_cp10_end_to_end_latency_alerts_test.ps1` disponible.
- Carpeta `docs/evidence` disponible para almacenar evidencias.

**Entrada / estímulo**  
Ejecución automatizada de transacciones válidas controladas que generan alerta `HIGH_AMOUNT`.

```powershell
.\scripts\run_cp10_end_to_end_latency_alerts_test.ps1
```

Características de los eventos generados:

```text
channel: ONLINE
amount: 20000.00
currency: USD
status: APPROVED
transaction_id: único por evento
card_id: único por evento
produced_at: generado justo antes del envío
event_timestamp: generado justo antes del envío
```

**Procedimiento**
1. Ejecutar el script automatizado de CP-10.
2. El script levanta o verifica Kafka, Kafka UI y el job de anomalías.
3. El script genera transacciones válidas `HIGH_AMOUNT` con `produced_at` actualizado justo antes del envío.
4. El script publica los eventos en `transactions-online`.
5. El script consume las alertas generadas desde el topic `alerts`.
6. El script correlaciona cada alerta con su evento original usando `source_transaction_id`.
7. El script calcula la latencia por evento usando la diferencia entre `triggered_at` y `produced_at`.
8. El script calcula latencia mínima, promedio y máxima.
9. El script valida que la latencia promedio sea menor a 3000 ms.
10. Verificar visualmente en Kafka UI que se generaron las alertas.
11. Verificar en Flink UI que el job permaneció en estado `RUNNING`.
12. Guardar las evidencias generadas en `docs/evidence`.

**Resultado esperado**
- Se generan alertas `HIGH_AMOUNT` para las transacciones válidas enviadas.
- Cada alerta se puede correlacionar con su evento original mediante `source_transaction_id`.
- Se obtiene una medición consistente de latencia end-to-end.
- La latencia promedio medida es menor a 3000 ms.
- El job de anomalías permanece en estado `RUNNING`.
- No se generan mensajes en DLQ para estos eventos controlados.

**Criterio de aceptación**  
La prueba se acepta si el script logra generar y correlacionar las alertas esperadas, y la latencia promedio end-to-end calculada es inferior a 3000 ms.

**Observación**  
La medición se realiza sobre alertas `HIGH_AMOUNT` porque esta salida es inmediata y correlacionable. Las transacciones normales sin alerta no se usan como salida principal de CP-10 mientras no exista una salida individual persistida y observable, como una integración completa con Elasticsearch/Kibana.

---

### CP-11 — Throughput incremental

**Objetivo**  
Identificar el throughput máximo sostenido aproximado del sistema mediante incrementos progresivos de carga y determinar el punto donde comienza la degradación.

**Precondiciones**
- Pipeline estable.
- Kafka operativo.
- Kafka UI disponible.
- Jobs de Flink en estado `RUNNING`.
- Productores ONLINE y POS disponibles.
- Script de prueba `scripts/run_cp11_incremental_throughput_test.ps1` disponible.
- Mecanismo de monitoreo mediante logs, Kafka UI y Flink UI.
- Carpeta `docs/evidence` disponible para almacenar evidencias.

**Entrada / estímulo**  
Ejecución automatizada de tres escalones de carga incremental.

```powershell
.\scripts\run_cp11_incremental_throughput_test.ps1
```

Escalones definidos:

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
```

Cada escalón se ejecuta durante un intervalo definido en el script.

**Procedimiento**
1. Ejecutar el script automatizado de CP-11.
2. El script levanta o verifica Kafka, Kafka UI y los jobs de Flink requeridos.
3. El script ejecuta el primer escalón de carga con productores ONLINE y POS en paralelo.
4. El script registra logs de ambos productores.
5. El script calcula throughput real combinado y porcentaje de cumplimiento frente a la tasa configurada.
6. El script repite el proceso para el segundo escalón.
7. El script repite el proceso para el tercer escalón.
8. El script identifica el throughput máximo sostenido observado.
9. El script identifica el punto de degradación.
10. Verificar visualmente Kafka UI y Flink UI por escalón.
11. Guardar logs, resumen, CSV y capturas en `docs/evidence`.

**Resultado esperado**
- El sistema procesa carga creciente en los tres escalones.
- Los productores finalizan sin errores críticos.
- Kafka recibe mensajes en `transactions-online` y `transactions-pos`.
- Los jobs de Flink permanecen en estado `RUNNING`.
- Se determina una capacidad sostenida aproximada.
- Se identifica el punto de degradación si ocurre.
- Pueden generarse mensajes en `alerts`, `transactions-dlq` y `transactions-aggregated` como parte del tráfico realista generado por los productores.

**Criterio de aceptación**  
La prueba se considera satisfactoria si permite documentar con evidencia numérica el throughput máximo sostenido aproximado y el punto de degradación, manteniendo estabilidad al menos en el primer escalón de carga incremental.

**Clasificación de estabilidad**
- Un escalón se considera **estable** si los productores finalizan sin fallos críticos, Kafka recibe mensajes y los jobs de Flink permanecen en estado `RUNNING`.
- Un escalón se considera **degradado** si el sistema sigue funcionando, pero el throughput real queda significativamente por debajo del configurado.
- Un escalón se considera **no estable** si hay caída de jobs, fallos críticos de productores, bloqueo del procesamiento o pérdida de continuidad del pipeline.

---

### CP-12 — Carga sostenida, persistencia y visualización

**Objetivo**  
Verificar la estabilidad del sistema bajo carga continua durante un periodo prolongado y validar que los resultados procesados sean persistidos y visualizables mediante Elasticsearch/Kibana.

**Precondiciones**
- Pipeline estable.
- Kafka operativo.
- Kafka UI disponible.
- Jobs de Flink en estado `RUNNING`.
- Productores ONLINE y POS disponibles.
- Elasticsearch integrado y en ejecución.
- Kibana integrado y disponible.
- Sink o mecanismo real de escritura hacia Elasticsearch disponible.
- Índices o data views configurados para consultar los datos procesados.
- Script de prueba `scripts/run_cp12_sustained_load_persistence_visualization_test.ps1` disponible.
- Carpeta `docs/evidence` disponible para almacenar evidencias.

**Entrada / estímulo**  
Ejecución automatizada de carga sostenida con ambos productores.

```powershell
.\scripts\run_cp12_sustained_load_persistence_visualization_test.ps1
```

Escenario recomendado:

```text
ONLINE: 60 eventos/s
POS: 60 eventos/s
Total configurado: 120 eventos/s
Duración: 10 minutos
```

Comandos base esperados dentro del script:

```powershell
python .\producers\producer_online.py --rate 60 --duration 600
python .\producers\producer_pos.py --rate 60 --duration 600
```

**Procedimiento**
1. Ejecutar el script automatizado de CP-12 cuando la integración Elasticsearch/Kibana esté disponible.
2. El script levanta o verifica Kafka, Kafka UI, Flink, Elasticsearch y Kibana.
3. El script confirma que existe un sink o mecanismo real escribiendo datos hacia Elasticsearch.
4. El script registra el estado inicial de índices o conteo de documentos en Elasticsearch.
5. El script ejecuta carga sostenida con productores ONLINE y POS.
6. El script registra logs de ambos productores.
7. El script monitorea o registra la continuidad del pipeline durante la carga.
8. El script consulta Elasticsearch para validar incremento de documentos indexados.
9. Se valida visualmente en Kibana que los datos sean consultables.
10. Se guardan logs, conteos, consultas y capturas en `docs/evidence`.

**Resultado esperado**
- El sistema mantiene operación continua durante la prueba.
- Kafka recibe eventos de forma sostenida.
- Los jobs de Flink permanecen en estado `RUNNING`.
- No se presentan fallos acumulativos críticos.
- Elasticsearch recibe y persiste documentos procesados.
- Kibana permite visualizar los datos persistidos.
- Se documenta el comportamiento del sistema durante la carga sostenida.

**Criterio de aceptación**  
La prueba pasa si el sistema soporta la carga sostenida sin interrupciones graves, mantiene el pipeline operativo, persiste datos en Elasticsearch y permite visualizarlos en Kibana.