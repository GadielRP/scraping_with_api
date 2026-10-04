# Validación de ejecución y memoria

Fecha: 2026-10-03. Implementación: `execution-and-memory-plan.md`. Las pruebas usan bases desechables y proveedores sintéticos; no modifican datos productivos ni envían alertas.

## Comportamiento implementado

- `app/runtime.py` compone los casos de uso. El scheduler solo traduce horarios y despacha solicitudes a tres canales: pre-start, cierre T−1 y mantenimiento. Cada canal tiene un hilo; mantenimiento admite como máximo ocho solicitudes entre activa y pendientes.
- Los ticks de pre-start se consolidan en una siguiente ejecución. T−1 conserva su instante original y vence cuando empieza el partido. Daily/midnight conservan fecha y slot aunque se retrasen. Un diferimiento de mantenimiento reintenta la misma unidad después de 60 segundos.
- Daily lee respuestas a disco y construye un elemento JSON por vez. Persiste batches de 100; deduplicación, mappings y conteos del run viven en SQLite temporal con cache de 2 MiB. Un deporte con ingestión/odds incompletos queda reintentable.
- Resultados selecciona páginas por ID, con límite superior fijo y proyecciones de identidad; libera la sesión antes de HTTP. Escritura de metadata, resultados y eliminación son unidades separadas e idempotentes. Resultados, sus observaciones y la invalidación de reporting comparten una transacción. Los contadores usan IDs realmente confirmados.
- Un deadline durante una página confirma primero sus unidades resueltas. `ResultBatchDeferred` conserva esos contadores; los outcomes distinguen `deferred_budget` de `deferred_unresolved`.
- Reporting conserva generaciones pendientes en PostgreSQL, con recuperación por vista y backoff. Refresca dos vistas con `CONCURRENTLY`, índices únicos, allowlist, rol limitado y timeout; no bloquea lectores con un refresh ordinario. El polling es de 60 segundos y consolida invalidaciones durante un mínimo de 30 minutos entre refresh exitosos; solo el refresh manual puede forzarlo. Cada transacción de refresh se excluye de pre-start, T−1 y los ciclos de OddsPortal dentro del proceso; el resto de las cargas conserva su concurrencia. La configuración vive en `infrastructure/settings/job_execution.py`, sin overrides de entorno.
- Los pools internos limitan futures pendientes y propagan el contexto. HTTP comparte su límite de solicitudes y prioriza T−1, pre-start y mantenimiento, en ese orden. Esto no multiplica el throughput permitido por el proveedor.
- OddsPortal conserva la propiedad de un ciclo activo incluso durante ticks vacíos. Un ciclo ocupado rechaza el siguiente y libera sus señales de espera. El cierre señaliza cancelación y limita la espera total mediante `JobExecutionSettings.shutdown_grace_seconds=30`; un hilo que no coopera se registra. No se fuerza la interrupción de transacciones ni de llamadas de terceros.

Los defaults y su validación están en `infrastructure/settings/job_execution.py`; `.env.example` documenta overrides. La política de descartes sigue configurada en su propio paquete: `canceled`, tres días y limpieza antes de cada daily heartbeat, incluido un heartbeat cuyo slot ya esté completado.

## Contratos verificados

La batería local final pasó 220 pruebas, con cuatro omisiones que requieren PostgreSQL. Se excluyó un caso de representación de OddsPortal cuyo contrato `ExternalMarketQuoteBlock` pertenece a cambios paralelos de pilares fuera de este refactor. La suite completa no se declara aprobada: se comprobó su colección sin errores, con 1.083 pruebas encontradas.

En el entorno con PostgreSQL 15 real, app y DB limitadas individualmente a 384 MiB y swap desactivado, pasaron 53 pruebas. Incluyen:

- Cola limitada, canales independientes, coalescencia, slots vencidos, prioridad HTTP y cierre de un action que no coopera.
- Daily con duplicados, mapping de varios IDs a un evento, JSON truncado después de batches confirmados y reintento sin duplicar eventos.
- Resultados parciales, IDs remapeados/eliminados, locks concurrentes, respuesta cancelada con identidad incorrecta, rollback de observaciones y flush antes de diferir.
- Invalidación durante refresh, recuperación tras fallo, backoff, throttling y migración upgrade/downgrade/upgrade. La función privilegiada se ejecutó con un rol distinto del propietario de las vistas.
- Lease de scratch temporal, limpieza de runs abandonados, límite de tokens JSON antes de construir escalares grandes, profundidad máxima y escapes fragmentados entre lecturas.

La prueba Linux usa el backend Python de `ijson`; la prueba Windows usa el backend instalado en el virtualenv. No se contactó a SofaScore/Oddspapi ni se lanzó Chromium.

## Memoria incremental: 1k, 10k y 50k

`scripts/benchmarks/discovery_memory.py` compara `json.load` con lectura incremental y registro en el mismo almacén temporal. Cada medición corre en un proceso nuevo. Ambos procesan el mismo fixture; se mide el parsing/agrupado, no todo el programa.

| Eventos | Asignación Python materializada (MiB) | Asignación incremental (MiB) | Pico RSS materializado (MiB) | Pico RSS incremental (MiB) |
|---:|---:|---:|---:|---:|
| 1.000 | 2,045 | 0,684 | 77,2 | 75,9 |
| 10.000 | 20,237 | 0,757 | 103,2 | 76,5 |
| 50.000 | 101,146 | 0,874 | 216,0 | 77,8 |

El volumen ya no exige retener la colección completa de daily en Python. El almacén temporal y los archivos sí crecen en disco con el volumen. RSS incluye intérprete/imports/allocator y no es consumo exclusivo de un job.

El coste medido bajo `tracemalloc`, validación de tokens y escritura temporal fue mayor: 50k tardaron 5,003 segundos materializados y 35,522 incrementales. Es una reducción de memoria con trabajo adicional de CPU/disco; no demuestra una aceleración del parsing. El tiempo de red y persistencia real se mide aparte.

Reproducción:

```powershell
.venv/Scripts/python.exe scripts/benchmarks/discovery_memory.py --output scratch/discovery-memory-benchmark.json
```

## Carga integrada con PostgreSQL

`scripts/benchmarks/execution_load.py` exige una base vacía cuyo nombre empiece por `execution_load_`. Usa los escritores y definiciones de vistas reales, con fixtures del proveedor.

Dataset: 20.000 eventos históricos y 60.000 choices/quotes/snapshots, seguidos de 10.000 eventos nuevos del día anterior. Durante mantenimiento se ejecutan lectores SQL en los canales pre-start y T−1.

| Medida | Resultado |
|---|---:|
| Daily: eventos insertados / fallos | 10.000 / 0 |
| Resultados confirmados / fallos | 10.000 / 0 |
| Vistas refrescadas / pendientes al terminar | 2 / 0 |
| Ejecuciones de lectores críticos | 340 |
| Mayor demora de despacho | 51,1 ms |
| Mayor lectura concurrente de vista | 31,2 ms |
| Pico RSS de app muestreado | 107,5 MiB |
| Pico del cgroup de app reportado por kernel | 94,6 MiB |
| OOM / OOM kill de app | 0 / 0 |
| Duración total sintética | 34,26 s |

La latencia de proveedor es simulada y el fixture no incluye cuotas nuevas en daily. La prueba ejercita persistencia de eventos/resultados, reporting y lectores concurrentes; no ejecuta toda la evaluación de alertas ni demuestra que una ingesta real de 10k termine en 34 segundos. RSS y contabilidad de cgroup provienen de mecanismos distintos y no deben tratarse como medidas idénticas.

El contenedor PostgreSQL alcanzó 342,1 MiB durante su vida, que incluyó preparación y otras pruebas. No es un pico aislado del job de carga. No hubo OOM observado en esta validación. Los límites suman 768 MiB; la prueba se realizó en una VM Docker mayor y **no certifica el presupuesto conjunto de un host de 1 GB**.

Reproducción: crear una base PostgreSQL desechable y vacía, establecer `EXECUTION_LOAD_DATABASE_URL` con sus credenciales locales y ejecutar:

```powershell
.venv/Scripts/python.exe scripts/benchmarks/execution_load.py --events 10000 --history 20000 --output scratch/execution-load.json
```

Nunca reutilizar la base de la aplicación para este benchmark. El script rechaza nombres fuera del prefijo y bases con tablas.

## Planes SQL

Se capturó `EXPLAIN (ANALYZE, BUFFERS)` en el dataset desechable, con `work_mem=4MB` y sin workers paralelos. Para probar pendientes se dejó incompleto el score visitante de los 10k eventos nuevos.

| Consulta | Filas | Ejecución | Índices observados |
|---|---:|---:|---|
| Página de resultados pendientes | 100 | 7,458 ms | PK de events/results e índice de mapping por event_id |
| SELECT subyacente de mv_alert_events | 20.000 | 327,717 ms | Índices de quotes, snapshots por quote/fecha, choices y PKs |
| SELECT subyacente de mv_p5_price_memory | 20.000 | 332,225 ms | Índices de quotes, snapshots por quote/fecha, choices y PKs |

La selección parte del ID mínimo elegible de la fecha; evitó recorrer un prefijo histórico irrelevante. No se añadieron índices duplicados por intuición. El fixture usa cache caliente y cardinalidades menores que producción: estas cifras no sustituyen revisar los planes reales cuando crezcan quotes/snapshots.

## Validación operativa pendiente y despliegue

1. Instalar las requirements actualizadas, incluida `ijson`, y aplicar `alembic upgrade head` con el rol de migración y `APP_DB_ROLE` correctos. La nueva revisión es `20261003_01`; el arranque comprueba la tabla de generaciones y la nueva función. Los callers antiguos de refresh SQL fueron retirados por esa migración.
2. Usar `compose.memory-test.yaml` únicamente sobre una base/copia de prueba. Limita ambos servicios a 384 MiB, desactiva swap y tokens de alertas. Para reproducir presión conjunta, ejecutarlo en una VM dedicada de 1 GiB o establecer `EXECUTION_MEMORY_CGROUP_PARENT` con el mismo cgroup padre de 1 GiB previamente provisionado para app y PostgreSQL. El perfil no crea ni modifica cgroups del host por sí solo.
3. Con proveedores reales, observar varios ciclos completos, latencias/errores HTTP, headroom del host, app/DB/browser, disco temporal, lag de despacho y finalización de los momentos críticos. El muestreo de RSS no incluye automáticamente la memoria de procesos hijos de Chromium; el cgroup sí la incluye cuando comparten el grupo.
4. Examinar diferimientos repetidos. Una app que conserva menos de 96 MiB disponibles en su cgroup/host difiere mantenimiento entre unidades; no se corta una transacción. Si la presión persiste, aumentar capacidad o reducir alcance según las medidas. La política no garantiza progresar sobre un presupuesto que ya no alcanza para una unidad.
5. Evaluar OddsPortal y los feeds históricos/materializados restantes antes de aumentar concurrencia. Las responsabilidades pendientes están reunidas en `docs/maintenance/execution-legacy-cleanup.md` como una única tarea posterior.

La separación de canales elimina la espera de pre-start detrás de un daily/midnight dentro del mismo worker. Todos comparten proceso, CPU, proveedor y PostgreSQL: un OOM todavía termina el proceso completo y separar hilos no garantiza deadlines ante cualquier carga externa.

## Validación de la limpieza posterior

La limpieza de CLI, calendario, discovery y repositorios pasó 162 pruebas locales, con tres omisiones que requieren PostgreSQL y el mismo caso ajeno de representación de OddsPortal excluido. Incluye políticas de admisión de eventos/cuotas, una única consulta previa de descartes por lote, propagación de pausas durante normalización, cierre del almacén temporal al interrumpir un feed, fecha local del comando `results`, streaming/reintentos, identidad y canales de ejecución. Los símbolos retirados no conservan aliases ni consumidores activos.

Las mediciones de carga y PostgreSQL anteriores corresponden a la implementación previa a esta limpieza; no se repitieron como parte de estos cambios. Esta validación no certifica nuevamente memoria de producción, el presupuesto conjunto de 1 GB ni proveedores reales.
