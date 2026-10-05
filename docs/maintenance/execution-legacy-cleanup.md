# Única tarea posterior de limpieza de ejecución

Esta lista es una tarea de código y datos. No crea un job periódico. El refactor actual elimina de inmediato las rutas sustituidas; solo se difieren responsabilidades que mantienen consumidores/datos válidos.

| ID | Símbolo y consumidor comprobado | Razón y condición de retirada |
|---|---|---|
| EXEC-001 | `EventRepository._build_event_data_with_legacy_fallback`; selección pre-start y simuladores en `scripts/development/` | Datos históricos pueden carecer de relaciones Participant/Competition. Verificar el backfill completo, migrar proyecciones y retirar los textos/fallback juntos. No asumir que los nuevos eventos prueban cobertura histórica. |
| EXEC-004 | `EventRepository.upsert_event`; `modules/sofascore/event_details.py`, backfills de metadata/entidades/competiciones y `scripts/sport_seasons_processing.py` | Todos usan el escritor común, pero algunos consumidores aún operan una respuesta por vez. Migrar los backfills a unidades acotadas y revisar el contrato de actualización tardía antes de retirar la API singular. No duplicar otro escritor. |
| EXEC-005 | Feeds materializados de dropping/secondary en `modules/sofascore/discovery_feeds.py` y sus coordinadores | La adquisición conserva listas/diccionarios completos; el procesamiento posterior ya limita futures y batches. Migrar estos feeds a una fuente incremental con su política de deduplicación, sin alterar qué eventos se ingieren. |
| EXEC-006 | Respuestas históricas Oddspapi y ciclo de navegador OddsPortal en pre-start | Contexto/deadline y pools ya se propagan, pero payloads históricos y procesos de Chromium requieren una prueba propia de volumen/lifecycle. Acotar lectura/ticks/retención con contratos del proveedor; comprobar cierre y presupuesto conjunto antes de ampliar su concurrencia. |

EXEC-003 (funciones SQL antiguas de refresh) queda cerrado en esta implementación: se eliminaron por migración y el schema guard/CLI/backfills usan la nueva función/coordinación. Las migraciones históricas se conservan como historia y para downgrade.

EXEC-002 queda cerrado: los slots ahora describen su fecha UTC objetivo (`current_utc_day` y `next_utc_day`), la migración `20261005_01` conserva el significado de las filas históricas, y las variables de entorno AM/PM siguen aceptándose como alias para no invalidar configuraciones existentes.

Retirados: MaintenanceExecutor, singleton del scheduler, wrappers CLI del scheduler, DailyDiscoveryExtractor/extractor/persistence/odds_parser, retry wrapper diario, pseudo batch_upsert_events de parallelism, process_with_parallel_db_ops, aliases de spelling de OddsPortal, helpers EventRepository.delete_event/get_events_starting_soon_with_odds sin consumidores, upsert singular de observaciones sin consumidores y export NBA_SEASONS de repositorios. También se eliminaron la cadena de launchers y timeout de ciclo anterior de OddsPortal, el argumento singular de cooldown de fixtures en Oddspapi, retornos booleanos constantes de DailyDiscoveryRepository y filtros post-captura de upcoming cuyo resultado ya no se consumía. `NBA_SEASONS` permanece en su dueño real, `modules/events/round_policy.py`.

Al cerrar esta tarea, comprobar consumidores con Graphify y búsqueda de símbolos, verificar datos necesarios, retirar los marcadores LEGACY(EXEC-xxx), ejecutar los contratos pertinentes y actualizar el grafo. No borrar migraciones antiguas ni módulos de otros cambios concurrentes.

## Limpieza posterior del código de ejecución

Se eliminaron los wrappers directos de CLI y refresh, los tres runners redundantes de resultados, el wrapper de daily discovery, el paquete `parallelism` y sus recomendaciones de rendimiento sin consumidores. La colección queda en un único caso de uso, con selección explícita por fecha.

Discovery separa consulta (`discovery/fetching.py`), políticas de admisión y persistencia (`discovery/persistence.py`) y presentación de logs (`discovery/summary.py`). Se retiró el wrapper `DiscoveryPersistenceSummary`: los coordinadores usan directamente el almacén temporal y garantizan su cierre. Se eliminó la segunda consulta de descartes del camino odds-first; el escritor mantiene la comprobación transaccional bajo lock. Las políticas se conservan: team streaks exige una respuesta de cuotas válida; dropping/winning persiste los eventos elegibles aunque falten cuotas; daily persiste el calendario completo de su alcance.

Se retiraron el campo temporal `members.starts_at`, la transacción duplicada al registrar membresía, el código de cuotas opcionales inalcanzable en la persistencia diaria, cuatro helpers de proyección normalizada sin consumidores y `get_todays_events` sin consumidores. También se eliminaron siete capturas que solo repetían el log y relanzaban el mismo error. El calendario se construye sin workers para `status`; la aplicación enlaza cada ocurrencia con su caso de uso antes del despacho.

Las pausas por presupuesto ya no se absorben como fallos ordinarios en normalización diaria ni en los coordinadores secundarios/dropping. Los feeds secundarios se adquieren y persisten secuencialmente para liberar cada respuesta antes de obtener la siguiente. Los límites de cola, identidad, memoria y transacciones se mantienen: responden a fallos observados y requisitos de integridad.
