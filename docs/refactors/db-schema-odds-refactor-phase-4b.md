# Fase 4b — Plan de implementación del backfill de quotes

**Estado:** listo para implementación  
**Prerrequisito:** Fase 4a desplegada y verificada  
**Documento maestro:** [db-schema-odds-refactor.md](./db-schema-odds-refactor.md)  
**Resultado de 4b:** herramienta segura, idempotente y probada; la ejecución
completa en staging/producción pertenece a Fase 4c.

## 1. Objetivo

Construir el backfill que:

1. Cree o complete una `MarketChoiceQuote` por identidad exacta
   `(choice_id canónico, source, exchange_side, exchange_level)`.
2. Enlace cada `MarketChoiceSnapshot` histórico clasificable mediante
   `snapshot.quote_id`.
3. Migre el estado legacy de `MarketChoice.initial_odds/current_odds/change`
   únicamente cuando el ownership de source/side sea demostrable.
4. Nunca degrade una quote escrita por la ingesta nueva.
5. Produzca evidencia auditable de todo lo migrado, omitido o rechazado.

La fase termina cuando el script puede ejecutarse en dry-run y commit de forma
determinista, reanudable e idempotente. No incluye el cutover de lectores.

## 2. Fuera de alcance

- No migrar alertas, trajectory, drift ni dual-process; corresponde a Fase 5.
- No hacer `quote_id NOT NULL`; corresponde a Fase 6.
- No borrar columnas de `market_choice_snapshots` ni `market_choices`.
- No eliminar markets/choices legacy Back/Lay.
- No crear otro writer de quotes ni duplicar sus reglas en SQL del script.
- No ejecutar migraciones desde el backfill.
- No resolver evidencia ambigua con heurísticas silenciosas.
- No habilitar ejecución online junto a la ingesta hasta tener locking
  compartido y upsert condicional a nivel de base de datos.
- **No** “arreglar” el gate live de opening snapshots
  (`_opening_gate_side_and_level` / `initial_was_set`): eso es path de
  ingesta canónica, ya corregido fuera de 4b. Ver maestro §11.7 gotcha 8.
- **No** dejar de escribir ni borrar la quote Oddspapi `exchange_side=NULL`
  de Betfair: es cleanup post–Fase 5 (maestro §11.7 gotcha 9). 4b **sí** debe
  clasificar evidencia histórica `NULL` vs `back`/`lay` sin fusionarlos
  silenciosamente (ver §6.2).

## 3. Invariantes

1. `MarketChoiceQuoteWriter` continúa siendo el único responsable del merge
   de estado de una quote.
2. El backfill solo enlaza snapshots cuyo `quote_id IS NULL`; nunca reasigna
   silenciosamente un snapshot ya enlazado.
3. Un snapshot clasificable tiene exactamente una identidad de quote.
4. `odds_value`, timestamps y `choice_id` legacy de snapshots son read-only.
5. Una quote live más nueva siempre gana frente a un candidato histórico.
6. Un conflicto se reporta con IDs y evidencia; no se convierte en una
   decisión por precedencia.
7. Dry-run no ejecuta `INSERT`, `UPDATE`, DDL ni funciones de migración. Hacer
   rollback al final no es suficiente: en PostgreSQL una secuencia puede
   avanzar aunque la transacción se revierta.
8. Cada batch commitido es atómico y su cursor se publica solo después del
   commit.

## 4. Arquitectura SRP

La CLI no contendrá reglas de negocio y `market_repository.py` no recibirá más
responsabilidades.

| Pieza | Responsabilidad única |
|---|---|
| `scripts/maintenance/backfill_market_choice_quotes.py` | Parsear CLI, configurar logging, invocar el servicio y traducir resultado a exit code. |
| `modules/odds_ingestion/backfill/__init__.py` | Declarar el paquete sin lógica ni side effects de inicialización. |
| `modules/odds_ingestion/backfill/market_choice_quote_backfill.py` | Orquestar preflight, keyset batches, clasificación, aplicación y reportes. Contiene contratos inmutables del backfill. |
| `infrastructure/persistence/repositories/market/market_choice_quote_backfill_repository.py` | Consultas set-based, precargas y bulk update de `snapshot.quote_id`; no decide source/side ni políticas temporales. |
| `infrastructure/persistence/repositories/market/market_choice_quote_merge_policy.py` | Calcular un `QuoteMergeDecision` puro e inmutable; lo comparten dry-run y writer para no duplicar reglas. |
| `MarketChoiceQuoteWriter.upsert` | Aplicar el `QuoteMergeDecision` sobre una quote precargada; cero SELECT propios. |
| `MarketChoiceSnapshotWriter` | No participa: el backfill enlaza snapshots existentes, no crea ticks nuevos. |

Contratos mínimos del orquestador:

- `QuoteIdentity`: choice canónico, source normalizado, side nullable y level.
- `BackfillCandidate`: evidencia legacy inmutable leída de DB.
- `ClassificationDecision`: `resolved`, `ambiguous`, `conflict` o `invalid`,
  con reason code y evidencia.
- `QuoteStateCandidate`: initial/current, timestamps y metadata candidata.
- `QuoteMergeMode`: `live` o `backfill_fill_only`.
- `QuoteMergeDecision`: cambios permitidos, no-ops, stale fields y conflictos.
- `BatchReport`: cursor, métricas, rechazos y checksum del lote.

Estos contratos no deben depender de una `Session`, para poder probar la
clasificación como lógica pura.

## 5. Cambios por archivo

### 5.1. Writer temporal

Crear
`infrastructure/persistence/repositories/market/market_choice_quote_merge_policy.py`
y modificar
`infrastructure/persistence/repositories/market/market_choice_quote_writer.py`:

- La policy recibe estado existente + candidato + modo y devuelve un
  `QuoteMergeDecision`; no conoce SQLAlchemy ni muta objetos.
- Extender el `upsert` existente con una política explícita, sin crear un
  segundo método de persistencia.
- Modos semánticos:
  - `live`: ingesta normal protegida contra payloads fuera de orden.
  - `backfill_fill_only`: completa huecos y nunca pisa evidencia más fuerte.
- Retornar un `QuoteUpsertResult` con la quote y el mismo merge decision:
  campos aplicados, candidatos stale y conflictos. El caller necesita métricas
  auditables.
- Mantener el contrato actual: recibe `session` y `quote_index`; no consulta DB.
- Adaptar los callers live para consumir `QuoteUpsertResult.quote`; el mapa
  `(side, level) → MarketChoiceQuote` de `_upsert_choice_quotes` no cambia.
- Conservar la autoridad de apertura de OddsPortal: el modo temporal no elimina
  la decisión explícita `overwrite_initial`; solo evita retrocesos de current y
  define el comportamiento fill-only del backfill.

Reglas temporales:

| Estado existente | Candidato | `live` | `backfill_fill_only` |
|---|---|---|---|
| current NULL | valor válido | aplicar, incluso sin timestamp | aplicar |
| timestamp candidato mayor | valor válido | aplicar | aplicar |
| timestamp igual + mismo valor | — | no-op | no-op |
| timestamp igual + valor distinto | — | conflicto | conflicto |
| timestamp candidato menor | — | stale/no-op | stale/no-op |
| existente con timestamp, candidato sin timestamp | — | no-op | no-op |
| existente sin timestamp y valor no NULL | candidato timestamped | aplicar en live | no sobrescribir en backfill |
| ambos sin timestamp y valor existente | valor distinto | aplicar por orden de llegada en live | conflicto/no-op |

Reglas para initial y metadata:

- `backfill_fill_only` rellena `initial_odds` solo cuando es NULL.
- Si `initial_odds` ya coincide y `initial_captured_at` es NULL, puede completar
  únicamente el timestamp respaldado por evidencia de apertura.
- Un initial distinto ya existente es conflicto; no se sobreescribe.
- `source`, side y level forman identidad y nunca son mutables.
- `main_line`, `source_market_id`, `source_outcome_id` y
  `bookmaker_outcome_id` se rellenan si son NULL; valores distintos son
  `metadata_conflict`.
- `source_limit` es estado variable: solo acompaña al current que efectivamente
  gana el merge o rellena un NULL.
- `movement` se recalcula únicamente si cambia initial/current.

### 5.2. Repositorio set-based

Crear
`infrastructure/persistence/repositories/market/market_choice_quote_backfill_repository.py`:

- Leer snapshots pendientes por keyset de `snapshot_id` dentro del scope de
  eventos congelado; evitar un sort global por event sobre la tabla histórica.
- Leer en una segunda pasada choices con estado legacy y sin snapshots,
  ordenados por `(event_id, choice_id)`.
- Aplicar filtros de scope antes del `LIMIT`.
- Traer en la misma lectura el contexto de event, market, bookie y choice
  necesario para clasificar; evitar lazy loads.
- Precargar en consultas acotadas:
  - mappings de bookmakers presentes en el batch;
  - markets/choices canónicos candidatos;
  - quotes existentes para las identidades resueltas.
- Ejecutar bulk update de `snapshot.quote_id` mediante `executemany` o
  `UPDATE ... FROM VALUES` según dialecto.
- No hacer commit; la transacción pertenece al orquestador.

Presupuesto de rendimiento por batch:

- Cantidad constante de SELECT para candidatos, referencias y quotes.
- Cero SELECT por snapshot y cero SELECT dentro de los writers.
- Un upsert/merge por identidad de quote, no por snapshot.
- Updates de lineage en chunks; no una llamada DB por fila.
- Memoria `O(batch_size + quote_identities)` y liberación al cerrar el batch.

### 5.3. Clasificador y orquestador

Crear `modules/odds_ingestion/backfill/market_choice_quote_backfill.py`:

1. Validar schema de forma read-only.
2. Adquirir lock de ejecución en commit.
3. Leer un batch por keyset.
4. Clasificar evidencia sin mutar ORM.
5. Agrupar decisiones resueltas por `QuoteIdentity`.
6. Derivar un solo `QuoteStateCandidate` por bucket.
7. En dry-run, evaluar la misma policy pura sin adjuntar objetos a una sesión.
8. En commit, precargar `quote_index`, invocar el writer una vez por bucket y
   hacer bulk link de snapshots.
9. Verificar invariantes dentro de la transacción.
10. Commit, publicar cursor y liberar memoria del batch.

El run tiene dos pasadas explícitas:

1. `snapshots`: crear/completar quotes y enlazar ticks históricos.
2. `choice_states`: completar estado legacy demostrable que no tiene snapshots.

El checkpoint identifica la pasada activa; nunca se mezclan ambos cursores.

No debe capturar excepciones por fila para continuar con un batch parcial. Un
error inesperado revierte el batch completo; los rechazos esperados se modelan
como `ClassificationDecision`.

### 5.4. CLI

Crear `scripts/maintenance/backfill_market_choice_quotes.py` con:

```text
--dry-run                 default implícito
--commit                  habilita escrituras
--event-id ID
--event-id-min ID
--event-id-max ID
--source SOURCE
--pass snapshots|choice-states|all
--batch-size N            filas máximas residentes por batch (default: 200)
--max-events N            eventos distintos máximos de esta ejecución
--max-rows N              filas candidatas máximas de esta ejecución
--after-event-id ID       selecciona el siguiente scope de eventos
--after-snapshot-id ID    cursor PK de la pasada snapshots
--resume-from PATH        reanuda exactamente un checkpoint compatible
--resolution-file PATH
--checkpoint-file PATH
--output-json PATH
--output-rejections PATH  NDJSON/CSV con evidencia por fila
--progress-every N
--confirm-ingestion-paused requerido para commit
```

Validaciones CLI:

- `--commit` y dry-run son mutuamente excluyentes.
- `--event-id` es mutuamente excluyente con el rango min/max.
- `--event-id` ya constituye un scope acotado de un evento. En cualquier otro
  scope es obligatorio indicar `--max-events`, `--max-rows` o ambos; no existe
  un modo unbounded para producción.
- `--max-events` y `--max-rows` deben ser positivos. Si se proporcionan ambos,
  el proceso se detiene al alcanzar primero cualquiera de los dos límites.
- Límites defensivos iniciales para el servidor pequeño: `max-events <= 500`
  y `max-rows <= 100000`. Completar más volumen exige varios runs, no un flag
  para desactivar la protección.
- `--after-event-id` y `--after-snapshot-id` son independientes: el primero
  delimita la selección de event scope y el segundo reanuda filas dentro de un
  scope ya congelado. Para reanudación normal, preferir `--resume-from`.
- Los cursores manuales de snapshots no aplican a `choice_states`; esa pasada
  se reanuda desde checkpoint mediante `choice_id`.
- `batch-size > 0` y con máximo defensivo.
- `batch-size` tiene default conservador de `200` y hard cap de `1000`; subirlo
  exige medir RSS con datos representativos, no solo tiempo de ejecución.
- `--resolution-file` debe tener versión y checksum válidos.
- Un commit exige `--output-json`, `--output-rejections` y checkpoint.
- Un commit exige `--confirm-ingestion-paused`; no usar prompt interactivo.
- `--source` filtra por source **resuelto**, no solo por el valor crudo de
  `snapshot.source`; por tanto la clasificación debe ocurrir antes de descartar
  candidatos con source legacy NULL.

## 6. Clasificación determinista

Identidad final:

```text
(canonical_choice_id, normalized_source, exchange_side|null, exchange_level)
```

### 6.1. Source

Evaluar toda la evidencia y rechazar contradicciones:

1. `LOWER(TRIM(snapshot.source))` válido.
2. Source único probado por IDs de lineage y mappings.
3. Market legacy cuyo `choice_group` codifica Back/Lay: `oddsportal`.
4. `bookie_id = 1`: `sofascore`.
5. Único source en `bookie_source_mappings` para el bookie.
6. Si quedan cero o varios candidatos: `ambiguous_source`.

### 6.2. Side y level

- Side explícito normalizado del snapshot gana si no contradice el resto.
- Si falta, extraer `Back`/`Lay` del `choice_group` legacy de OddsPortal.
- Para ticks Oddspapi side-agnostic de la ventana Fase 2→4a, mapear a `back,0`
  solo cuando una quote/mapping exchange y el contrato top-back lo demuestren.
- Un bookie no exchange conserva `side=NULL, level=0`.
- Level NULL se normaliza a `0`; negativo o no numérico es inválido.

### 6.3. Choice y market canónicos

- Caso normal: mantener el choice actual.
- OddsPortal legacy Back/Lay: resolver el market canónico por
  `(event_id, bookie_id, canonical market name, period, line, is_live)` y el
  choice por nombre normalizado.
- La línea sale únicamente de `Back|Lay` con sufijo explícito.
- Un target existente único se reutiliza.
- Un target inexistente solo puede planificarse para creación cuando todas las
  piezas de identidad son deterministas y pasan el mismo normalizador y
  constraint del path canónico.
- Dos targets, diferencias solo resolubles por heurística o choice ausente son
  `ambiguous_target`; requieren resolution file.

### 6.4. Estado de la quote

- Initial: `MarketChoice.initial_odds` solo con ownership unívoco; no usar el
  primer snapshot como opening inventado.
- Current: último tick del bucket por
  `(COALESCE(source_collected_at, collected_at), collected_at, snapshot_id)`.
- Sin snapshots, `MarketChoice.current_odds` solo con ownership unívoco.
- `initial_captured_at` queda NULL si no existe evidencia temporal real.
- `current_updated_at` usa source time y `collected_at` como fallback.
- El snapshot conserva `source_limit`/`exchange_size`; la quote toma
  `source_limit` del current ganador.

## 7. Resolution file

Las excepciones manuales son datos versionados, no branches ocultas en código.

Ejemplo mínimo:

```json
{
  "version": 1,
  "generated_for": "staging-2026-08-08",
  "decisions": [
    {
      "snapshot_id": 123,
      "canonical_choice_id": 456,
      "source": "oddsportal",
      "exchange_side": "back",
      "exchange_level": 0,
      "reason": "validated_against_raw_export",
      "evidence": "artifact/path-or-ticket"
    }
  ]
}
```

Reglas:

- IDs referenciados deben existir dentro del scope solicitado.
- Una decisión no puede contradecir una FK o side inválido.
- Duplicados con distinto contenido invalidan todo el archivo.
- El SHA-256 se incluye en reportes y checkpoints.
- No se aceptan wildcard ni decisiones por nombre sin IDs estables.

## 8. Batching, transacciones y reanudación

- Seleccionar primero el scope acotado de event IDs y congelarlo en el
  checkpoint.
- Dentro de ese scope, snapshots usan `snapshot_id ASC`; choice states usan
  `choice_id ASC`. Ambos son keysets por PK; nunca `OFFSET`.
- `batch_size` limita snapshots pendientes.
- En la pasada `choice_states`, `batch_size` limita choices.
- `max_rows` limita el total de candidatos **escaneados** durante el run,
  incluidos resueltos, ambiguos e inválidos. Así un histórico sucio no evade
  el límite por producir cero escrituras.
- Antes de procesar, `max_events` congela un scope de hasta N event IDs
  distintos obtenido de la unión set-based de ambas pasadas. Ese scope se
  guarda en el checkpoint y se reutiliza al reanudar.
- Cada query usa `LIMIT min(batch_size, remaining_max_rows)`; ningún batch ni
  run puede sobrepasar el límite configurado.
- Si `max_rows` se alcanza a mitad de un evento, se committea únicamente el
  batch completo actual y el checkpoint permite continuar ese mismo evento.
- Los límites son presupuesto de **cada invocación**. `--resume-from` crea un
  nuevo run relacionado, reinicia el presupuesto de filas y continúa desde el
  último cursor commitido.
- Si el scope de `max_events` quedó incompleto, la reanudación conserva esos
  event IDs. Si ya terminó, selecciona el siguiente scope después del último
  event ID completado.
- Un `--pass all` comparte el mismo contador global de filas. Si el límite se
  consume en `snapshots`, la siguiente ejecución reanuda esa pasada antes de
  avanzar a `choice_states`.
- Un evento puede cruzar batches. Reprocesar un bucket es seguro por
  idempotencia y merge fill-only.
- Una sesión/transacción corta por batch.
- El checkpoint contiene pasada activa, último cursor commitido, run ID,
  filtros, event scope congelado, límites solicitados/consumidos, checksum de
  resolution file y versión del algoritmo.
- El checkpoint se escribe de forma atómica después del commit.
- Tras fallo, repetir el último batch; nunca saltar al cursor observado antes
  de confirmar el commit.
- Commit con advisory lock PostgreSQL. En SQLite/local, garantizar una única
  instancia del proceso.
- La primera ejecución real requiere ingesta pausada aunque exista advisory
  lock, porque los writers live todavía no comparten ese lock.
- Antes de aprobar el batch size, ejecutar `EXPLAIN (ANALYZE, BUFFERS)` en
  staging. La lectura no puede ordenar/materializar todo el histórico. Si el
  planner no usa un recorrido acotado por PK para `quote_id IS NULL`, desplegar
  previamente un índice parcial de cursor mediante la migración de aplicación;
  el script jamás crea índices por sí mismo.

### Perfil de recursos para el servidor de 1 GB

- Configuración inicial recomendada: `--batch-size 200 --max-events 10
  --max-rows 5000`.
- Candidatos se leen como rows/mappings ligeros, no como grafos ORM con
  relaciones cargadas.
- `quote_index`, agrupaciones y resultados pertenecen solo al batch actual.
- Cerrar la sesión y descartar todas las colecciones al terminar cada batch;
  la identity map de SQLAlchemy no puede crecer durante todo el run.
- Rechazos y manifest se escriben incrementalmente en NDJSON. Solo contadores
  agregados permanecen en memoria.
- No cargar previamente todos los snapshot IDs, event IDs históricos ni el
  resolution file completo. La única excepción es el pequeño scope explícito
  de hasta `max_events` IDs, que debe respetar un máximo defensivo.
- El backfill completo se logra mediante ejecuciones repetidas con checkpoint,
  nunca retirando los límites en el servidor pequeño.

## 9. Dry-run y reportes

Dry-run ejecuta exactamente las mismas lecturas, clasificación y simulación de
merge, pero no crea objetos ORM persistentes ni emite DML. Es read-only respecto
a DB; sí escribe los artefactos de reporte solicitados.

Resumen JSON mínimo:

- Identidad de run, ambiente, filtros, cursor inicial/final y checksums.
- `configured_batch_size`, `configured_max_events`, `configured_max_rows`,
  `events_selected`, `events_processed`, `rows_consumed`, `rows_remaining` y
  `stop_reason` (`completed_scope`, `max_events`, `max_rows`, `error`).
- `snapshots_scanned`, `snapshots_already_linked`, `snapshots_linkable`,
  `snapshots_linked`.
- `choice_states_scanned`, `unmigrated_choice_states`.
- `quote_buckets_planned`, `quotes_inserted`, `quotes_updated`,
  `quotes_unchanged`.
- `stale_candidates_ignored`, `metadata_conflicts`.
- `ambiguous_source`, `ambiguous_target`, `ambiguous_choice_state`,
  `contradictory_evidence`, `invalid_side_or_level`.
- `legacy_markets_mapped`, `canonical_markets_planned/created`.
- Duración, filas/segundo, peak batch size y métricas por source/side.

El archivo de rechazos contiene una fila por decisión no resuelta con
`event_id`, `market_id`, `choice_id`, `snapshot_id`, reason code y evidencia.
No incluir payloads completos ni secretos.

Exit codes:

- `0`: ejecución completa sin ambiguos bloqueantes.
- `2`: completó análisis pero quedan decisiones bloqueantes.
- `3`: preflight/schema/configuración inválida.
- `4`: fallo transaccional o invariante rota.

## 10. Preflight read-only

Antes de leer candidatos, validar mediante introspección:

- Existen `market_choice_quotes` y `market_choice_snapshots.quote_id`.
- Tipo físico compatible entre PK/FK.
- FK `snapshot.quote_id → quote.quote_id` presente.
- Índice `idx_market_choice_snapshots_quote_collected` presente.
- Índice único NULL-safe de quotes presente.
- Side legacy `'single'` no existe en quotes.
- Fuentes solicitadas son conocidas.
- Resolution/checkpoint corresponden al ambiente y scope.

El preflight no llama `check_and_migrate_schema()`.

## 11. Plan de implementación incremental

### 4b.1 — Contrato temporal del writer

1. Definir los dos modos de merge.
2. Extender `MarketChoiceQuoteWriter.upsert` y su resultado auditable.
3. Adaptar callers live sin cambiar su semántica válida.
4. Añadir tests de orden temporal, igualdad y metadata conflictiva.

Gate: un current antiguo no puede degradar una quote nueva y la suite de
ingesta existente permanece verde.

### 4b.2 — Clasificador y dry-run

1. Implementar contratos inmutables y clasificación pura.
2. Implementar consultas read-only por keyset.
3. Añadir resolution file, reportes y exit codes.
4. Ejecutar dry-run sobre fixtures representativos.

Gate: dry-run produce decisiones reproducibles y cero DDL/DML, incluyendo
cero avance de secuencias.

### 4b.3 — Aplicación transaccional

1. Añadir quote preload e índice en memoria.
2. Agrupar por identidad y llamar una vez al writer por bucket.
3. Hacer un flush batch de quotes para obtener IDs y bulk link de snapshots.
4. Añadir la pasada separada de choice states sin snapshots.
5. Añadir advisory lock, checkpoint y reanudación.

Gate: segundo commit es no-op y un fallo inyectado revierte solo el batch
activo sin perder el cursor anterior.

### 4b.4 — Verificación y hardening

1. Presupuesto de queries y memoria con batch grande.
2. Verificar límites exactos por eventos/filas y reanudación a mitad de evento.
3. Tests PostgreSQL de constraint NULL-safe, lock y bulk update.
4. Comparar checksums/counts antes y después.
5. Generar runbook de Fase 4c con comandos reales de staging/producción.

Gate: todos los criterios de aceptación de la sección 14 están automatizados
o tienen consulta operativa documentada.

**Runbook de ejecución (Fase 4c):** ver
[db-schema-odds-refactor-phase-4c-runbook.md](./db-schema-odds-refactor-phase-4c-runbook.md).

## 12. Matriz mínima de tests

| Caso | Resultado esperado |
|---|---|
| Sportsbook normal con source | Quote `(choice, source, NULL, 0)` y snapshots enlazados. |
| Oddspapi Back/Lay con varios levels | Quote distinta por side/level; sin colisiones. |
| OddsPortal market legacy `Back 2.5`/`Lay 2.5` | Ambos sides apuntan al mismo choice canónico de línea `2.5`. |
| Snapshot sin source pero bookie SofaScore | Source resuelto a `sofascore`. |
| Dos sources posibles | `ambiguous_source`, cero mutaciones. |
| Quote live más nueva | Candidato stale ignorado. |
| Timestamp igual, precio diferente | Conflicto determinista. |
| Metadata estable contradictoria | `metadata_conflict`, no overwrite. |
| Snapshot ya enlazado correctamente | No-op. |
| Snapshot enlazado a identidad incompatible | Fallo de invariante; no reasignar. |
| Choice con estado y sin snapshots, ownership único | Completar quote sin inventar timestamps. |
| Choice externo sin lineage | `ambiguous_choice_state`. |
| Dry-run | Cero DDL, DML y avance de secuencias. |
| Commit repetido | Segundo pase con cero inserts/updates. |
| Fallo a mitad de batch | Rollback completo del batch y checkpoint intacto. |
| 25+ snapshots del mismo bucket | Un merge de quote; bulk link. |
| Evento mayor que batch size | Memoria acotada y reanudación correcta. |
| `--max-events 3` con más eventos pendientes | Solo tres event IDs entran al scope congelado. |
| `--max-rows 250` con batch de 200 | Consume 200 + 50; nunca lee/procesa 400. |
| Ambos límites configurados | Se detiene en el primero y reporta `stop_reason`. |
| Límite alcanzado a mitad de evento | Commit atómico del último batch y resume sin saltos ni duplicados. |
| Resume después de `max_rows` | Nuevo run y nuevo presupuesto; conserva cursor y scope incompleto. |
| Resume después de completar `max_events` | Selecciona el siguiente scope, sin repetir eventos finalizados. |
| `--pass all` agota filas en snapshots | Checkpoint permanece en snapshots; choice states espera al siguiente run. |
| Ejecución sin límite ni `--event-id` | CLI rechaza antes de abrir una sesión de escritura. |
| Muchos rechazos | NDJSON streaming; memoria no crece con el total de rechazos. |
| Plan SQL sobre histórico grande | Sin sort/materialización global; keyset por PK dentro del event scope. |

Suites de regresión obligatorias:

```bash
python -m pytest -q \
  tests/test_market_choice_quote_model.py \
  tests/test_market_choice_quote_writer.py \
  tests/test_market_choice_snapshot_writer.py \
  tests/test_save_canonical_bookmaker_batches_quotes.py \
  tests/test_oddsportal_canonical_ingestion.py \
  tests/test_oddsportal_betfair_back_lay_quotes.py \
  tests/oddspapi/test_ingestion_service.py
```

Nuevos tests sugeridos:

- `tests/test_market_choice_quote_temporal_merge.py`
- `tests/test_market_choice_quote_backfill_classifier.py`
- `tests/test_market_choice_quote_backfill_repository.py`
- `tests/test_backfill_market_choice_quotes_cli.py`

Como `test_*.py`, `*.md` y `*.json` están ignorados globalmente en este repo,
la implementación debe añadir excepciones explícitas en `.gitignore` para los
tests nuevos, este plan y cualquier resolution file que deba versionarse. Los
resolution files viven bajo `config/backfills/market_choice_quotes/`; reportes,
checkpoints y manifests operativos permanecen fuera de git.

## 13. Runbook de validación de 4b

1. Crear fixtures mínimos por source/side y capturar baseline.
2. Ejecutar dry-run focalizado:

```bash
python -m scripts.maintenance.backfill_market_choice_quotes \
  --event-id 158955 \
  --batch-size 200 \
  --max-rows 5000 \
  --output-json debug/market_choice_quote_backfill/158955-dry-run.json \
  --output-rejections debug/market_choice_quote_backfill/158955-rejections.ndjson
```

3. Revisar todos los ambiguos; crear resolution file solo con evidencia.
4. Ejecutar commit en DB desechable/restaurada desde staging.

Ejemplo de run acotado para el servidor de 1 GB:

```bash
python -m scripts.maintenance.backfill_market_choice_quotes \
  --commit \
  --confirm-ingestion-paused \
  --batch-size 200 \
  --max-events 10 \
  --max-rows 5000 \
  --checkpoint-file debug/market_choice_quote_backfill/checkpoint.json \
  --output-json debug/market_choice_quote_backfill/run-summary.json \
  --output-rejections debug/market_choice_quote_backfill/run-rejections.ndjson
```

Reanudación: conserva scope/cursor y reutiliza los valores configurados, pero
otorga un presupuesto nuevo de filas a la nueva invocación:

```bash
python -m scripts.maintenance.backfill_market_choice_quotes \
  --commit \
  --confirm-ingestion-paused \
  --resume-from debug/market_choice_quote_backfill/checkpoint.json \
  --checkpoint-file debug/market_choice_quote_backfill/checkpoint.json \
  --output-json debug/market_choice_quote_backfill/resume-summary.json \
  --output-rejections debug/market_choice_quote_backfill/resume-rejections.ndjson
```

5. Repetir el mismo commit y exigir cero mutaciones.
6. Repetir dry-run y exigir cero trabajo pendiente clasificable.
7. Comparar counts/checksums de snapshots.
8. Medir queries, throughput y peak RSS; reducir `batch-size` si el proceso
   supera el presupuesto definido para el servidor de 1 GB.
9. Probar una ejecución limitada por eventos y otra limitada por filas,
   reanudando ambas desde checkpoint.

La Fase 4b no ejecuta el backfill completo de producción.

## 14. Criterios de aceptación

- Todo snapshot creado después de 4a conserva `quote_id` no nulo.
- Todo snapshot histórico clasificable proyecta exactamente un `quote_id`.
- Ningún snapshot cambia odds, timestamps o `choice_id` legacy.
- No existen duplicados por identidad NULL-safe de quote.
- Una quote nueva nunca es degradada por estado histórico.
- Dry-run es físicamente read-only.
- Query count no escala con snapshots mediante SELECT N+1.
- Memoria queda acotada por `batch-size`.
- La CLI rechaza scopes no acotados: requiere `--event-id`, `--max-events` o
  `--max-rows`.
- Ninguna ejecución procesa más eventos o filas que los límites solicitados.
- Alcanzar un límite genera checkpoint y `stop_reason`, no se considera error.
- Con `batch-size=200`, la prueba representativa debe dejar margen suficiente
  para SO/DB dentro del servidor de 1 GB; el valor final se fija con peak RSS
  medido y queda documentado en el runbook de 4c.
- Reanudación repite de forma segura el último batch.
- Segundo commit produce cero mutaciones.
- Todos los ambiguos están nominados y tienen reason code.
- Reporte, rechazos, checkpoint y resolution checksum permiten auditar el run.
- Suite de ingesta de los tres providers permanece verde.
- Existe runbook listo para que Fase 4c ejecute staging y producción.

## 15. Rollback y recuperación

El cambio de 4b es aditivo y los lectores legacy siguen intactos. Antes de cada
commit se requiere backup lógico y un manifiesto exacto del run.

El manifiesto debe registrar:

- snapshot IDs cuyo `quote_id` pasó de NULL a un valor;
- quote IDs creados por el run;
- campos NULL de quotes existentes que fueron completados;
- cursor/checksum de cada batch.

Para runs grandes, escribir el detalle incrementalmente como NDJSON comprimible;
no acumular todos los snapshot IDs en memoria hasta el final.

No implementar un rollback destructivo automático en 4b. Si una verificación
falla, pausar, conservar artefactos y restaurar desde backup o preparar una
corrección revisada usando el manifiesto. Nunca borrar quotes por rango o
revertir snapshots sin validar dependencias live creadas después del run.
