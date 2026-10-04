# Memoria de descartes y escritura de eventos por lotes

## Comportamiento

Discovery sigue admitiendo eventos pasados y futuros dentro de su alcance actual.
Cuando un consumidor decide eliminar un evento, entrega un `DeletionBatch` con
la clasificación de `results_parser`, la identidad del proveedor y su evidencia.
El repositorio registra la memoria y elimina el evento en la misma transacción.
Un fallo revierte ambas operaciones. El parser sigue clasificando sin acceder a la DB.

Por defecto se memoriza únicamente `canceled`. Esto incluye los estados que el
parser ya clasifica así, como postponed y walkover. `not_started`,
`finished_empty_score` y las respuestas 404 siguen sus reglas de eliminación
anteriores, pero no generan memoria por defecto. Una lista de IDs sin evidencia
es una eliminación administrativa: no se infiere que sus eventos sean canceled.

La tabla `event_discard_memory` tiene clave `(source, source_event_id)` y no tiene
FK al evento eliminado. Conserva el ID canónico original, kind, motivo, origen,
fechas de observación y descarte, versión de política y un snapshot JSON con
`status` (code, description, type), `homeScore`, `awayScore`, `winnerCode` y
`startTimestamp`, si esos campos estaban presentes. No convierte campos ausentes
en null ni modifica sus valores originales.

La memoria bloquea esa identidad incluso si discovery muestra otro estado.
No bloquea IDs iguales de otro proveedor ni interpreta sus estados.
Los encuentros posteriores no renuevan `discarded_at`. No recupera descartes
históricos ni elimina retroactivamente los eventos recreados antes de instalarla.

## Configuración

Las variables se cargan al iniciar el proceso. Reiniciarlo aplica sus cambios.
`Config` es la fuente única de valores por defecto; `DiscardSettings` crea una
instantánea tipada y valida que los valores positivos y los kinds sean elegibles.

| Variable | Valor por defecto | Función |
| --- | --- | --- |
| `EVENT_DISCARD_MEMORY_ENABLED` | `true` | Registrar y consultar la memoria |
| `EVENT_DISCARD_MEMORY_KINDS` | `canceled` | CSV de kinds que se registran y bloquean |
| `EVENT_DISCARD_MEMORY_RETENTION_DAYS` | `3` | Antigüedad mínima desde el descarte para limpiar |
| `EVENT_DISCARD_MEMORY_CLEANUP_ENABLED` | `true` | Habilitar eliminación de filas de memoria |
| `EVENT_DISCARD_MEMORY_CLEANUP_BATCH_SIZE` | `1000` | Máximo de filas eliminadas por ejecución |

El tamaño de las transacciones de eventos se configura mediante `JobExecutionSettings.event_write_batch_size` en `infrastructure/settings/job_execution.py` (100 por defecto), sin override de entorno.

También se pueden configurar `not_started` y `finished_empty_score` como kinds.
No se admite `finished`: la memoria representa descartes, no resultados válidos.

La limpieza se ejecuta al comienzo de cada invocación de `run_daily_discovery_job`,
antes de comprobar la ranura del día y si quedan deportes pendientes. Por eso
también corre en heartbeats fuera de la ranura o cuando la caché indica que el
trabajo ya terminó. El scheduler programa heartbeats en horarios fijos y reintentos.

**La caducidad depende de la limpieza física.** Una fila con más de tres días
continúa bloqueando hasta que el job la elimine. Con cleanup desactivado no se
limpia ni se ignora por su edad. Al reactivarlo se limpia progresivamente. Cada
invocación procesa como máximo un lote; una cola grande puede necesitar varias
invocaciones de daily discovery. Con memoria desactivada se permite ingerir y no
se registran nuevos descartes; la limpieza mantiene su toggle independiente.

## Transacciones, consultas y concurrencia

`EventRepository.batch_upsert_events` acepta un iterable y hace transacciones
acotadas. Cada lote consulta mappings, eventos, participantes y descartes en
conjunto, escribe referencias y eventos en grupo y confirma una sola vez.
`upsert_event` delega al mismo camino para un solo elemento. El resultado distingue
eventos persistidos, IDs descartados e IDs inválidos.

Daily discovery filtra por alcance y memoria antes de normalizar. Procesa y
consume los resultados por lote; las llamadas HTTP y la persistencia de cuotas
quedan fuera de la transacción de eventos. Los helpers secundarios también usan
el repositorio por lotes. No se carga toda la tabla de memoria ni se añade una
consulta de memoria por evento. El resultado del API batch retiene los eventos
que devuelve (O(n)); sus consultas y operaciones intermedias están acotadas por
lote. Los clientes que no necesitan retenerlos pueden consumir lotes como daily.

PostgreSQL usa advisory locks transaccionales estables por identidad, en orden
determinista, tanto al crear como al borrar. La comprobación final de memoria se
hace después del lock. Así se evita que una ingesta concurrente recree el evento
entre la consulta anticipada y el commit. SQLite sirve para pruebas funcionales;
la prueba de concurrencia se ejecuta contra PostgreSQL.

Un descarte con evidencia se omite si cambió la identidad, si el evento fue
actualizado después de la observación o si ya tiene un resultado persistido.
Se registra el conflicto para una evaluación posterior. Los fallos de integridad
o datos dividen el lote para aislar la fila inválida; un fallo de infraestructura
se propaga. Los deadlocks o conflictos de serialización de escritura se reintentan
hasta dos veces. Una operación de varios lotes puede quedar parcialmente confirmada.

El alcance sigue usando `tracked_competitions`: argumento omitido resuelve la
configuración; `None` explícito desactiva el filtro; un conjunto vacío no permite
ninguna competición. La reconciliación de temporadas y las listas independientes
por proveedor/job quedan fuera de este cambio.

## Logs operativos

Los contadores de escritura se emiten después del commit. `memory_inserted`
cuenta filas realmente devueltas por `INSERT ... ON CONFLICT DO NOTHING RETURNING`;
`memory_existing` cuenta los conflictos de identidad que no insertaron otra fila.
`memory_eligible` identifica los descartes cubiertos por la política y
`deleted_without_memory` las eliminaciones sin registro de memoria. El log incluye
kind, conflictos de eliminación, IDs canónicos y duración del lote.

Discovery registra los IDs externos bloqueados antes de normalizar y los que
bloquea la comprobación final dentro de la transacción. Los resúmenes distinguen
`events_persisted`, `events_inserted`, `events_updated` y `events_discarded`.
`events_inserted` ahora significa exclusivamente eventos nuevos; anteriormente
incluía todos los upserts. `updated` cuenta eventos existentes escritos, aunque
sus valores de negocio no cambien; los contadores reflejan operaciones y no un
conteo global de IDs únicos entre lotes. No se carga la tabla completa ni se
añaden consultas por evento para calcularlos.

## Instalación y validación

Aplicar la migración antes de arrancar procesos con este código:

```powershell
.venv/Scripts/python.exe -m alembic upgrade head
```

La revisión `20261001_01` crea solamente la tabla y su índice por fecha; su padre
es `20260925_01`. El downgrade elimina la memoria y pierde su historial. La
implementación se validó en una DB aislada; no aplica esta migración a producción.

`tests/test_event_discard_memory.py` usa SQLite por defecto. Para verificar locks,
rollback, migración y errores de PostgreSQL, configurar
`DISCARD_TEST_DATABASE_URL` con una base **desechable**: el fixture crea y elimina
las tablas. Nunca usar la DB de la aplicación para ese test.

Una medición local con PostgreSQL 17, 100 eventos nuevos y referencias compartidas
comparó el upsert individual anterior con el batch actual: 802 frente a 9 SELECT,
200 frente a 1 commits y 1107 frente a 14 ejecuciones SQL. El tiempo fue 0.889 s
frente a 0.105 s en esa ejecución. Son medidas de persistencia local, no una
predicción del rendimiento total de discovery ni de las llamadas al proveedor.
