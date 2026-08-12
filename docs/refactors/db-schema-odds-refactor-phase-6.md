# Fase 6 — `market_choice_snapshots` slim

**Estado:** implementación y migración local completadas; servidor pendiente de ventana operativa.
**Commit base:** `67b1d3f`.
**Fecha de inicio:** 2026-08-12.

## 1. Resultado buscado

`market_choice_snapshots` debe volver a ser un log temporal, no una segunda
copia de la identidad de una quote. El contrato final es:

```text
market_choice_snapshots
├─ snapshot_id          PK
├─ quote_id             FK NOT NULL → market_choice_quotes.quote_id
├─ odds_value           valor observado
├─ collected_at         hora de persistencia
├─ source_collected_at  hora informada por el provider
├─ source_limit         dato propio del tick
└─ exchange_size        dato propio del tick
```

La navegación completa queda así:

```text
snapshot
  └─ quote_id → market_choice_quotes
                  ├─ source / side / level / metadata de identidad
                  └─ choice_id → market_choices → markets → events/bookies
```

## 2. Fuera de alcance

- No eliminar todavía `MarketChoice.initial_odds/current_odds/change`; Fase 7.
- No borrar toda la fachada/readers legacy; Fase 8.
- No limpiar automáticamente markets/choices Back/Lay históricos.
- No aplicar el DDL al servidor antes de cerrar la observación operativa de
  Fase 5.
- No usar `CASCADE` para forzar el DROP.

## 3. Baseline local

Preflight inicial sobre `public.market_choice_snapshots`:

| Métrica | Valor |
|---|---:|
| Filas | 2,762,285 |
| `quote_id IS NULL` | 0 |
| Heap | 218,112,000 bytes |
| Índices | 433,004,544 bytes |
| Total | 651,206,656 bytes |

Índices legacy que desaparecerán junto a las columnas:

- `idx_choice_collected`;
- `idx_market_choice_snapshots_choice_collected_desc`;
- `idx_market_choice_snapshots_source`;
- `idx_market_choice_snapshots_source_collected`;
- `idx_market_choice_snapshots_source_market`.

Se conserva `idx_market_choice_snapshots_quote_collected` y la PK.

## 4. Mapa de responsabilidades

| Pieza | Responsabilidad en Fase 6 |
|---|---|
| `models.py::MarketChoiceSnapshot` | Declarar únicamente el contrato slim |
| `market_choice_snapshot_writer.py` | Escribir tick + `quote_id`; no copiar identidad |
| `market_choice_snapshot_slim.py` | Auditar, producir plan DDL y aplicar fail-closed |
| `migrate_market_choice_snapshots_slim.py` | Único CLI autorizado para dry-run, commit, compactación y evidencia JSON |
| `market_choice_snapshot_slim_postflight.py` | Probar vistas/MV quote y confirmar que no sobreviven variantes dual/trajectory retiradas |
| builders de vistas en `models.py` | Resolver choice/identidad por join con `mcq` |
| readiness de Fase 5 | Auditar lineage por `quote_id`, sin exigir `choice_id` snapshot |
| scripts de simulación/mantenimiento | Navegar snapshot → quote → choice |
| guard estático | Rechazar identidad o `choice_id` leídos desde snapshots |

El migrador no contiene reglas de alertas, trajectory o backfill. Los builders
de vista sólo producen contratos SQL reutilizables. El CLI no reimplementa
schema: orquesta migrador, preparación de vistas y postflight.

**Regla operativa:** ningún operador debe copiar/pegar los `ALTER`, `DROP
INDEX`, `DROP COLUMN` o `VACUUM FULL`. Todas las mutaciones de Fase 6 son
responsabilidad exclusiva del CLI versionado para que local y servidor
ejecuten exactamente el mismo procedimiento auditable.

## 5. Subfases y orden estricto

### 6A — Preparación del release

1. Hacer de `v_dual_process_event_odds` y
   `v_pre_start_odds_trajectory` las únicas vistas canónicas quote-aware.
2. Cambiar readers, readiness y scripts activos a joins por quote.
3. Adelgazar el writer: deja de persistir identidad duplicada.
4. Extender el guard para incluir `MarketChoiceSnapshot.choice_id`.
5. El script puede ejecutarse contra el schema expandido, pero la aplicación
   Fase 6 no debe reanudar jobs antes del DROP: el writer slim ya no llena
   `choice_id`. El startup falla con un mensaje accionable si detecta el schema
   expandido y nunca intenta migrarlo automáticamente.

### 6B — Migración de schema

Precondiciones dentro de la misma transacción:

- tabla y columnas esperadas existen;
- `quote_id IS NULL = 0`;
- cero `quote_id` huérfanos;
- mientras exista `choice_id`, cero mismatch contra `mcq.choice_id`;
- las únicas dependencias permitidas ya usan columnas slim;
- los índices slim existen.

DDL PostgreSQL:

1. `LOCK TABLE public.market_choice_snapshots IN ACCESS EXCLUSIVE MODE`;
2. repetir invariantes;
3. `ALTER COLUMN quote_id SET NOT NULL`;
4. retirar índices legacy explícitamente;
5. retirar las ocho columnas redundantes, una lista exacta y sin `CASCADE`;
6. confirmar por introspección el conjunto exacto de columnas.

El DDL destructivo está soportado únicamente para PostgreSQL. Las bases SQLite
nuevas de tests nacen slim desde metadata; los fixtures históricos expandidos
sólo ejercitan auditoría/preflight y nunca aplican el DROP.

El DDL anterior describe el contrato interno del migrador; **no es un runbook
para ejecutar SQL a mano**.

### 6C — Compactación y evidencia

En PostgreSQL, `DROP COLUMN` no reduce por sí solo el heap. Después del commit:

1. ejecutar opcionalmente `VACUUM (FULL, ANALYZE)` en ventana local/operativa;
2. medir heap, índices y total;
3. verificar conteo, `MIN/MAX(snapshot_id)` y checksum de las siete columnas
   slim;
4. recrear/refrescar dependencias y ejecutar suites focales.

## 6. Seguridad y rollback

La migración es destructiva y no tiene down migration sintética: reconstruir
la identidad antigua desde quotes produciría valores, pero no restauraría
necesariamente los bytes históricos originales de cada columna.

- Antes del DROP: rollback = desplegar el commit de Fase 5; no hubo pérdida.
- Después del DROP: rollback estructural = restaurar backup/copia pre-6.
- Los flags legacy/shadow no restauran columnas. Dual process y trajectory ya
  no tienen flags ni variantes legacy.
- Un lock, timeout, fila sin lineage o dependencia desconocida aborta toda la
  transacción.
- Nunca se usa `DROP ... CASCADE`.

## 7. Gates de aceptación

- Schema exacto de siete columnas; `quote_id NOT NULL` con FK.
- Cero referencias activas a identidad/choice desde snapshot.
- Writer sólo llena campos slim.
- Counts, IDs y checksum de payload temporal iguales antes/después.
- Vistas trajectory/dual, alertas, drift y Pilar 5 verdes.
- Guard estático verde sin allowlist de identidad snapshot.
- Tamaño antes/después registrado; compactación reportada por separado.
- Backfill 4b/4c claramente bloqueado/retirado en schema slim.
- Documentación actualizada con evidencia real, no estimaciones.

## 8. Estado operativo

La copia local satisface el gate principal de lineage (`quote_id IS NULL = 0`).
El DDL del servidor permanece bloqueado hasta completar la ventana de
observación de Fase 5. Este bloqueo no impide desarrollar y probar 6A/6B en la
copia local.

## 9. Runbook único local/servidor

Prerrequisitos: release Fase 6 disponible, jobs detenidos, backup/copia
pre-6 confirmado y los modos configurables restantes en `quotes`.

```powershell
# 1. Inspección read-only. Exit 0 = expanded listo o slim idempotente.
python -m scripts.maintenance.migrate_market_choice_snapshots_slim `
  --output-json logs/debug/phase6/market_choice_snapshots_slim_dry_run.json

# 2. Migración lógica + preparación/postflight de readers + refresh de MV.
python -m scripts.maintenance.migrate_market_choice_snapshots_slim `
  --commit --confirm-destructive `
  --output-json logs/debug/phase6/market_choice_snapshots_slim_commit.json

# 3. Opcional y en ventana con lock: compactación física.
python -m scripts.maintenance.migrate_market_choice_snapshots_slim `
  --commit --confirm-destructive --compact `
  --output-json logs/debug/phase6/market_choice_snapshots_slim_compact.json
```

No arrancar la aplicación entre el deploy del código Fase 6 y el paso 2. El
paso 2 es idempotente: instala las vistas canónicas quote-aware, elimina las
variantes dual y trajectory con `DROP VIEW IF EXISTS` y sin `CASCADE`, refresca
`mv_alert_events` y ejecuta el postflight. También consulta `event_all_odds`.
Por defecto usa
`158955` y `169158`; se puede repetir
`--reference-event-id` para sustituir ese scope.

Existen dos caminos soportados por el mismo script:

- Local/staging que ejecutó la Fase 5 anterior: las cuatro vistas privadas
  dual/trajectory pueden existir y se retiran.
- Servidor sin Fases 5/6 en DB: se crea directamente la vista dual canónica y
  ambos `DROP VIEW IF EXISTS` son no-op. La migración nunca exige que las
  variantes hayan existido.

El bloque `RETIRED_ODDS_READ_VIEWS`/
`retire_odds_read_variants_postgresql` está marcado `PHASE8_CLEANUP`: sólo
compatibiliza bases que ya recibieron el rollout local anterior y se elimina
cuando todos los entornos hayan cruzado Fase 6.

## 10. Evidencia local final

Migración ejecutada el 2026-08-12 únicamente mediante el CLI anterior:

| Métrica | Antes | Después del DROP | Después de compactar |
|---|---:|---:|---:|
| Filas | 2,762,285 | 2,762,285 | 2,762,285 |
| `MIN/MAX(snapshot_id)` | 1 / 4,771,164 | igual | igual |
| Checksum payload | `8fec7e3fb72e38a910a84d657b7f1784` | igual | igual |
| Heap | 218,112,000 | 218,112,000 | 192,921,600 |
| Índices | 433,004,544 | 174,071,808 | 174,071,808 |
| Total | 651,206,656 | 392,273,920 | 366,993,408 |

Reducción total: 284,213,248 bytes (43.6%). El schema final tiene exactamente
siete columnas, `quote_id NOT NULL`, FK vigente, cero nulos/huérfanos,
únicamente el índice quote+tiempo y ninguna dependencia sobre columnas
retiradas.

Postflight idempotente:

- readiness `158955`/`169158`: `ready=true`, cero issues;
- dual canónica quote-aware: 2 filas en el contrato;
- vistas dual privadas legacy/quotes: ausentes;
- trajectory canónica quote-aware: 100 filas en el contrato;
- variantes privadas trajectory legacy/quotes: ausentes;
- `mv_alert_events`: 2 filas para el scope;
- refresh de `mv_alert_events`: ejecutado por el CLI y confirmado en el JSON
  final (`materialized_views_refreshed=true`);
- ambas vistas canónicas usan directamente `market_choice_quotes`; ya no son
  wrappers;
- tests: 284 passed, 12 skipped del backfill 4b/4c retirado;
- guard de lecturas legacy: cero violaciones no allowlisted.

Reejecución local tras fijar dual process en quotes (2026-08-12):

- schema ya slim, antes/después idénticos: 2,762,327 filas, IDs 1 / 4,771,210
  y checksum `76e142752346872caf82d97192f7243f`;
- `compacted=false`: esta limpieza no volvió a compactar ni alteró snapshots;
- `v_dual_process_event_odds` lee directamente `market_choice_quotes` y produjo
  2/2 eventos de referencia;
- `v_dual_process_event_odds_legacy` y
  `v_dual_process_event_odds_quotes` quedaron ausentes;
- `mv_alert_events` se refrescó y devolvió 2/2 eventos de referencia;
- trajectory quedó validada funcionalmente y posteriormente se fijó como una
  única vista canónica quote-aware.

Reejecución local final tras fijar trajectory (2026-08-12):

- schema y payload temporal permanecieron idénticos: 2,762,327 filas,
  IDs 1 / 4,771,210 y checksum `76e142752346872caf82d97192f7243f`;
- `v_pre_start_odds_trajectory` produjo 100 filas para los eventos de
  referencia y su definición contiene el CTE quote-aware `eligible_quotes`;
- las cuatro variantes privadas dual/trajectory quedaron ausentes;
- `mv_alert_events` volvió a refrescarse correctamente;
- `compacted=false`: no hubo reescritura física ni cambio de snapshots.

### Orden de despliegue en servidor sin Fases 5/6

Inicializar la aplicación no sustituye esta migración. El startup crea vistas,
pero no ejecuta el DDL destructivo de Fase 6. El orden requerido es:

1. detener aplicación/jobs y confirmar backup;
2. desplegar el código final;
3. ejecutar dry-run del CLI de Fase 6;
4. ejecutar `--commit --confirm-destructive` (y `--compact` sólo en su ventana);
5. revisar `ok=true`, readers canónicos y variantes retiradas;
6. iniciar la aplicación.

Así el servidor instala directamente las vistas finales de Fase 5 y el schema
de Fase 6 en una sola campaña versionada, aunque nunca haya tenido los wrappers
o vistas privadas usados durante las pruebas locales.

El preflight permite como dependencias transitorias exclusivamente
`v_dual_process_event_odds` y `v_pre_start_odds_trajectory`: en un servidor sin
Fase 5 todavía pueden leer columnas snapshot legacy, pero el CLI las reemplaza
antes del lock/DDL. Cualquier otra vista o dependencia continúa bloqueando la
migración.

### Ensayo con copia nueva del servidor

La campaña se repitió contra una copia descargada del servidor que no tenía
Fases 5/6 aplicadas:

- preflight expandido: 2,762,285 filas, cero quotes nulas/huérfanas/mismatch,
  checksum `8fec7e3fb72e38a910a84d657b7f1784` y 651,206,656 bytes;
- las únicas dependencias legacy fueron las dos vistas canónicas administradas
  por el CLI; fueron reemplazadas antes del DDL;
- post-migración: siete columnas, `quote_id NOT NULL`, FK con
  `ON DELETE CASCADE`, PK y único índice adicional
  `idx_market_choice_snapshots_quote_collected`;
- conteo, rango de IDs `1..4,771,164` y checksum permanecieron idénticos;
- tamaño compactado final: 366,985,216 bytes (reducción de 284,221,440 bytes,
  43.65%);
- sólo existen `v_dual_process_event_odds`,
  `v_pre_start_odds_trajectory`, `event_all_odds` y `mv_alert_events`; las
  cuatro variantes privadas están ausentes;
- `mv_alert_events` contiene 123,051 filas y ambos eventos de referencia;
- reporte final: `logs/debug/phase6/server_copy_commit_compact.json`,
  `ok=true`, sin errores de readers.

Durante la primera ejecución, el modo fail-closed detectó una diferencia de
tipo preexistente entre las vistas trajectory (`integer` legacy vs `smallint`
quotes) y abortó antes del DROP. El builder conserva ahora ambos contratos y
la ejecución posterior fue exitosa.

## 11. Legacy y cleanup futuro

- `market_choice_quote_backfill_repository.py` y su servicio 4b/4c conservan
  código que referencia las columnas expandidas. Están **retirados**: su
  preflight bloquea el schema slim y sus tests históricos quedan marcados como
  skip hasta eliminarlos en Fase 8.
- `backfill_sofascore_choice_names_and_groups.py` y
  `backfill_sofascore_canonical_markets.py` todavía fusionan choices mediante
  `snapshot.choice_id`; su preflight exige esa columna y los bloquea bajo
  Fase 6. Portar a quote lineage o eliminar en Fase 8.
- `DatabaseManager.check_and_migrate_schema` sigue siendo un orquestador
  monolítico de migraciones históricas. Fase 6 retiró de él toda mutación de
  snapshots, pero su extracción a migraciones versionadas queda como cleanup.
