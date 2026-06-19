# Events ID Dependency Audit

> **Commit base**: `c14d561`  
> **Date**: 2026-06-19  
> **Author**: Architecture audit updated to reflect implemented changes  
> **Status**: IMPLEMENTED IN CODEBASE

---

## 1. Resumen Ejecutivo

### Estado Actual

La separación de identidad ya quedó implementada en el código base:

1. `events.id` ahora funciona como ID canónico interno autoincremental.
2. Los IDs externos de SofaScore se almacenan en `event_source_mappings`.
3. La migración de identidad corre en batches de 1000, es idempotente y deja un marcador de completitud.
4. Los jobs y scripts que llaman a SofaScore resuelven primero el `sofascore_event_id` desde el mapping.
5. El validador de migración comprueba mappings, huérfanos, duplicados y el default de secuencia.

### Contexto Original

Antes de la migración, `events.id` almacenaba el **ID externo de SofaScore** como primary key. Eso creaba un acoplamiento directo entre la identidad interna del evento y un proveedor externo, lo cual:

1. **Impide integrar otros proveedores** (OddsPapi, Pinnacle, Betradar, Flashscore, OpticOdds, LSports, MollyBet, TxOdds, BetGenius, Oddin) ya que cada proveedor tiene su propio sistema de IDs.
2. **Mezcla semánticas**: el mismo campo `event_id` se usa tanto para llamar APIs externas como para foreign keys internas. No hay distinción explícita.
3. **Rompe si dos proveedores reportan el mismo evento**: no hay mecanismo para mapear IDs de diferentes fuentes al mismo evento canónico.
4. **Hace imposible una deduplicación multi-source**: no se puede saber si el evento 12345 de SofaScore es el mismo que el fixture 98765 de OddsPapi.

### Riesgos de Migrar Sin Auditar

- **FKs rotas**: `results.event_id`, `markets.event_id`, `event_observations.event_id`, `prediction_logs.event_id` dejarían de apuntar a eventos válidos.
- **Vistas SQL rotas**: 7 views/materialized views hacen JOIN con `events.id`.
- **Jobs rotos**: 4 jobs activos usan `event_id` mezclando ID interno con ID externo para API calls.
- **Pérdida de datos**: scripts de backfill asumen `Event.id == SofaScore ID`.

### Módulos Más Críticos (por riesgo)

1. **`EventRepository.upsert_event()`** — CRITICAL: Escribe `event_payload["id"]` (SofaScore ID) directamente en `Event.id`.
2. **`daily_discovery/persistence.py`** — CRITICAL: Usa el mismo ID para upsert y para guardar odds.
3. **`run_pre_start_check_job.py`** — CRITICAL: Usa `event_data["id"]` tanto para SofaScore API calls como para DB operations.
4. **`run_results_collection_job.py`** — HIGH: Usa `event.id` para `api_client.get_event_results()` (SofaScore API) y `ResultRepository.upsert_result()` (DB FK).
5. **`intraday_result_freshness.py`** — HIGH: Usa `event_data["id"]` para llamar `/event/{event_id}` en SofaScore.
6. **SQL views y materialized views** — HIGH: Todas referencian `events.id` en JOINs.

---

## 2. Conceptos Finales de Identidad

### `canonical_event_id`
- **Definición**: ID interno autoincremental de la tabla `events`.
- **Generación**: Asignado por PostgreSQL al insertar un evento nuevo (`SERIAL` / `autoincrement`).
- **Uso**: Primary key de `events`, foreign key en `results`, `markets`, `event_observations`, `prediction_logs`.
- **Regla**: NUNCA debe contener un ID externo de ningún proveedor.

### `source_event_id`
- **Definición**: ID externo asignado por un proveedor a un evento.
- **Almacenamiento**: `event_source_mappings.source_event_id` (tipo `Text`).
- **Ejemplos**: `"12345678"` (SofaScore), `"1300010955308649"` (OddsPapi fixture_id).
- **Regla**: Siempre se almacena como string para soportar IDs alfanuméricos de cualquier proveedor.

### `source`
- **Definición**: Identificador del proveedor externo.
- **Valores válidos**:
  - `sofascore`
  - `oddspapi`
  - `pinnacle`
  - `betradar`
  - `flashscore`
  - `opticodds`
  - `lsports`
  - `mollybet`
  - `txodds`
  - `betgenius`
  - `oddin`

### `provider API ID`
- **Definición**: ID requerido para llamar endpoints específicos de un proveedor externo.
- **Ejemplo**: Para SofaScore, se usa `sofascore_event_id` en `/event/{sofascore_event_id}`.
- **Regla**: Nunca se obtiene de `events.id` directamente; siempre se resuelve via `event_source_mappings`.

### `database FK` (`event_id` en tablas internas)
- **Definición**: `event_id` usado dentro de tablas internas como `results`, `markets`, `event_observations`, `prediction_logs`.
- **Semántica post-migración**: Siempre es `canonical_event_id`.

---

## 3. Tabla Maestra de Dependencias

### A. DB Schema / SQLAlchemy Models

| File | Function/Class | Line/Pattern | Current Meaning | Desired Meaning After Migration | Risk | Required Action |
|------|---------------|-------------|-----------------|-------------------------------|------|-----------------|
| `models.py` | `Event.id` | L61 `Column(Integer, primary_key=True)` | sofascore_external_event_id | canonical_internal_event_id | **CRITICAL** | Change to `autoincrement=True`, stop accepting external ID |
| `models.py` | `Result.event_id` | L123 `ForeignKey('events.id')` | database_foreign_key (points to SofaScore ID) | database_foreign_key (canonical) | SAFE_AFTER_FK_MIGRATION | Keep as canonical FK, update values during migration |
| `models.py` | `EventObservation.event_id` | L137 `ForeignKey('events.id')` | database_foreign_key | database_foreign_key (canonical) | SAFE_AFTER_FK_MIGRATION | Keep as canonical FK |
| `models.py` | `Market.event_id` | L189 `ForeignKey('events.id')` | database_foreign_key | database_foreign_key (canonical) | SAFE_AFTER_FK_MIGRATION | Keep as canonical FK |
| `models.py` | `PredictionLog.event_id` | L281 `ForeignKey('events.id')` | database_foreign_key | database_foreign_key (canonical) | SAFE_AFTER_FK_MIGRATION | Keep as canonical FK |

### B. Repository Methods

| File | Function/Class | Line/Pattern | Current Meaning | Desired Meaning After Migration | Risk | Required Action |
|------|---------------|-------------|-----------------|-------------------------------|------|-----------------|
| `event_repository.py` | `upsert_event()` | L136 `event_payload.get('id')` | sofascore_external_event_id | N/A — must not be used as PK | **CRITICAL** | Receive sofascore_event_id separately, resolve or create canonical via mapping |
| `event_repository.py` | `upsert_event()` | L200 `Event.id == event_id` | sofascore_external_event_id lookup | canonical_internal_event_id lookup | **CRITICAL** | Lookup by mapping first, then by canonical ID |
| `event_repository.py` | `upsert_event()` | L246 `Event(id=event_id, ...)` | sofascore_external_event_id written to PK | MUST NOT pass external ID | **CRITICAL** | Remove `id=event_id`; let autoincrement assign; create mapping after flush |
| `event_repository.py` | `_build_normalized_event_data()` | L56 `"id": event_obj.id` | Returns SofaScore ID in payload | Returns canonical_event_id | HIGH | All consumers must understand this is now canonical |
| `event_repository.py` | `_build_event_data_with_legacy_fallback()` | L98 `"id": event_obj.id` | Returns SofaScore ID in payload | Returns canonical_event_id | HIGH | All consumers must understand this is now canonical |
| `event_repository.py` | `get_event_by_id()` | L277 `Event.id == event_id` | ambiguous (could be SofaScore or canonical) | canonical_internal_event_id | MEDIUM | Add type hint clarification; after migration, always canonical |
| `event_repository.py` | `delete_event()` | L411 `Event.id == event_id` | canonical FK | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `event_repository.py` | `batch_delete_events()` | L439 `Event.id.in_(event_ids)` | canonical FK | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `event_repository.py` | `update_event_starting_time()` | L392 `Event.id == event_id` | canonical FK | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `event_repository.py` | `mark_event_as_alerted()` | L666 `Event.id == event_id` | canonical FK | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `event_repository.py` | `get_events_starting_soon_with_odds()` | L533 `event_obj.id for event_obj` | canonical FK | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `result_repository.py` | `upsert_result()` | L14 `event_id: int` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Ensure callers pass canonical ID |
| `result_repository.py` | `get_result_by_event_id()` | L46 `event_id: int` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `market_repository.py` | `save_markets_from_response()` | L55 `event_id: int` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Ensure callers pass canonical ID |
| `market_repository.py` | `save_markets_from_oddsportal()` | L354 `event_id: int` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `market_repository.py` | `get_markets_for_event()` | L220 `event_id: int` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `market_repository.py` | `delete_markets_for_event()` | L661 `event_id: int` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `dual_process_odds_repository.py` | `get_event_odds()` | L57 `event_id: int` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `dual_process_odds_repository.py` | `get_event_odds_map()` | L64 `event_ids: List[int]` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `odds_trajectory_repository.py` | `get_pre_start_trajectory_map()` | L83 `event_ids: List[int]` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `observation_repository.py` | `upsert_observation()` | L17 `event_id: int` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |

### C. SQL Views / Materialized Views

| File | View/MV Name | Line/Pattern | Current Meaning | Desired Meaning | Risk | Required Action |
|------|-------------|-------------|-----------------|----------------|------|-----------------|
| `models.py` | `v_dual_process_event_odds` | L365 `m.event_id` | database_foreign_key | canonical FK | SAFE_AFTER_FK_MIGRATION | Recreate view after migration |
| `models.py` | `event_all_odds` | L481 `e.id = eo.event_id` | database_foreign_key | canonical FK | SAFE_AFTER_FK_MIGRATION | Recreate view after migration |
| `models.py` | `basketball_results` | L497 `e.id AS event_id` | database_foreign_key | canonical FK | SAFE_AFTER_FK_MIGRATION | Recreate view after migration |
| `models.py` | `mv_alert_events` | L579 `e.id AS event_id` | database_foreign_key | canonical FK | SAFE_AFTER_FK_MIGRATION | Recreate materialized view after migration |
| `models.py` | `season_events_with_results` | L656 `e.id AS event_id` | database_foreign_key | canonical FK | SAFE_AFTER_FK_MIGRATION | Recreate view after migration |
| `models.py` | `v_market_choice_trajectory` | L696 `e.id AS event_id` | database_foreign_key | canonical FK | SAFE_AFTER_FK_MIGRATION | Recreate view after migration |
| `models.py` | `v_pre_start_odds_trajectory` | L720 `e.id AS event_id` | database_foreign_key | canonical FK | SAFE_AFTER_FK_MIGRATION | Recreate view after migration |

### D. SofaScore API Calls

| File | Function/Class | Line/Pattern | Current Meaning | Desired Meaning | Risk | Required Action |
|------|---------------|-------------|-----------------|----------------|------|-----------------|
| `client.py` | `get_event_final_odds()` | L443 `f"/event/{id}/odds/1/all"` | sofascore_external_event_id | EXTERNAL_PROVIDER_ID_REQUIRED | **CRITICAL** | Must resolve sofascore_event_id from mapping before calling |
| `client.py` | `get_event_details()` | L384 `get_event_details(self, event_id)` | sofascore_external_event_id | EXTERNAL_PROVIDER_ID_REQUIRED | **CRITICAL** | Parameter name should be `sofascore_event_id` |
| `client.py` | `get_event_results()` | L461 `event_id: int` | sofascore_external_event_id | EXTERNAL_PROVIDER_ID_REQUIRED | **CRITICAL** | Must receive sofascore_event_id, not canonical |
| `client.py` | `check_and_update_starting_time()` | L506 `event_id: int` | ambiguous_event_id | canonical_internal_event_id for DB, sofascore_event_id for API | **CRITICAL** | Split into DB operations (canonical) and API calls (sofascore_event_id) |
| `client.py` | `_extract_endpoint_event_id()` | L174 `parts[2]` | sofascore_external_event_id | sofascore_external_event_id | LOW | Keep as is — used for error handling only |
| `event_details.py` | `fetch_event_response()` | L20 `f"/event/{event_id}"` | sofascore_external_event_id | EXTERNAL_PROVIDER_ID_REQUIRED | **CRITICAL** | Must receive sofascore_event_id |
| `event_details.py` | `get_event_results()` | L132 `event_id: int` | sofascore_external_event_id | EXTERNAL_PROVIDER_ID_REQUIRED | **CRITICAL** | Must receive sofascore_event_id |

### E. Daily Discovery Flow

| File | Function/Class | Line/Pattern | Current Meaning | Desired Meaning | Risk | Required Action |
|------|---------------|-------------|-----------------|----------------|------|-----------------|
| `daily_discovery/persistence.py` | `persist_event_with_odds()` | L16 `event.get("id")` | sofascore_external_event_id | sofascore_external_event_id (from API) | **CRITICAL** | After upsert, use `db_event.id` (canonical) for DB writes, keep SofaScore ID for API calls |
| `daily_discovery/persistence.py` | `persist_event_with_odds()` | L34-35 `MarketOddsIngestionService.save_from_event_odds_response(event_id, ...)` | sofascore_external_event_id passed as DB FK | Must be canonical_internal_event_id | **CRITICAL** | Replace with `db_event.id` |

### F. Pre-Start Check Flow

| File | Function/Class | Line/Pattern | Current Meaning | Desired Meaning | Risk | Required Action |
|------|---------------|-------------|-----------------|----------------|------|-----------------|
| `run_pre_start_check_job.py` | `run_pre_start_check_job()` | L190 `event["id"]` | ambiguous_event_id (currently SofaScore ID from DB) | canonical_internal_event_id (after migration) | **CRITICAL** | After migration, `event["id"]` becomes canonical; SofaScore API calls need mapping |
| `run_pre_start_check_job.py` | Various | L271 `event_data["id"]` | ambiguous_event_id | canonical_internal_event_id | HIGH | Used for timing, context building — safe after migration IF SofaScore calls are fixed |
| `run_pre_start_check_job.py` | Odds extraction | L311 `api_client.get_event_final_odds(event_data["id"], ...)` | sofascore_external_event_id | EXTERNAL_PROVIDER_ID_REQUIRED | **CRITICAL** | Must resolve sofascore_event_id from mapping first |
| `run_pre_start_check_job.py` | Market save | L317 `MarketOddsIngestionService.save_from_event_odds_response(event_data["id"], ...)` | sofascore_external_event_id as DB FK | canonical_internal_event_id | **CRITICAL** | After migration, event_data["id"] will be canonical — OK |
| `timing.py` | `should_extract_odds_for_event()` | L33 `event_id: int` | ambiguous_event_id | canonical_internal_event_id (for logging), sofascore_event_id (for API calls inside) | HIGH | Internal API calls (`api_client.get_event_results(event_id)`) need sofascore_event_id |
| `timestamp_corrections.py` | `check_and_update_starting_time()` | L33 `event_id: int` | ambiguous_event_id | canonical_internal_event_id for DB + sofascore_event_id for API | HIGH | DB operations use canonical, API calls need mapping resolution |
| `rescheduled_events.py` | `handle_rescheduled_event()` | L42 `api_client.get_event_final_odds(event_id, ...)` | sofascore_external_event_id | EXTERNAL_PROVIDER_ID_REQUIRED | HIGH | Must resolve sofascore_event_id from mapping |

### G. Results Collection Flow

| File | Function/Class | Line/Pattern | Current Meaning | Desired Meaning | Risk | Required Action |
|------|---------------|-------------|-----------------|----------------|------|-----------------|
| `run_results_collection_job.py` | `_collect_results_for_events()` | L22 `ResultRepository.get_result_by_event_id(event.id)` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | `event.id` will be canonical after migration |
| `run_results_collection_job.py` | `_collect_results_for_events()` | L27 `api_client.get_event_results(event.id)` | sofascore_external_event_id | EXTERNAL_PROVIDER_ID_REQUIRED | **CRITICAL** | Must resolve sofascore_event_id from mapping |
| `run_results_collection_job.py` | `_collect_results_for_events()` | L32 `ResultRepository.upsert_result(event.id, ...)` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | `event.id` will be canonical |
| `run_results_collection_job.py` | `run_results_collection_for_date()` | L92 `api_client.get_event_final_odds(event_data.id, ...)` | sofascore_external_event_id | EXTERNAL_PROVIDER_ID_REQUIRED | **CRITICAL** | Must resolve sofascore_event_id from mapping |

### H. Odds Ingestion Flow

| File | Function/Class | Line/Pattern | Current Meaning | Desired Meaning | Risk | Required Action |
|------|---------------|-------------|-----------------|----------------|------|-----------------|
| `market_odds_ingestion_service.py` | `save_from_event_odds_response()` | L31 `event_id: int` | ambiguous_event_id | canonical_internal_event_id | HIGH | Callers must pass canonical_event_id; service passes to MarketRepository (DB FK) |
| `market_odds_ingestion_service.py` | `_save_normalized()` | L65 `MarketRepository.save_markets_from_response(event_id, ...)` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is — receives canonical from caller |

### I. Alerts / Pillars / Prediction Flows

| File | Function/Class | Line/Pattern | Current Meaning | Desired Meaning | Risk | Required Action |
|------|---------------|-------------|-----------------|----------------|------|-----------------|
| `pillars/context.py` | `EventContext.event_id` | L46 | canonical FK (from `event_obj.id`) | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | `event_obj.id` will be canonical after migration |
| `pillars/context.py` | `build_event_context()` | L247 `event_id=getattr(event_obj, "id", 0)` | canonical FK | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `alerts/dual_process/process_2/engine.py` | `DualProcessResult.event_id` | L28 | canonical FK | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `alerts/matchup_streak_analysis/historical_form.py` | Various | L394 `event_id_from_api = event.get('id')` | sofascore_external_event_id (from SofaScore API response) | sofascore_external_event_id | LOW | Used for deduplication within SofaScore API results — no DB write |
| `prediction/prediction_logging.py` | `PredictionLog` upserts | L50-72 `event.id` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | `event.id` will be canonical |

### J. Observations Flow

| File | Function/Class | Line/Pattern | Current Meaning | Desired Meaning | Risk | Required Action |
|------|---------------|-------------|-----------------|----------------|------|-----------------|
| `observations/service.py` | `event_has_observations()` | L20 `event_id: int` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `observations/service.py` | `save_observations_for_event()` | L33 `event_id: int` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |
| `observations/service.py` | `extract_and_save_tennis_ground_type()` | L62 `event_id: int` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |

### K. CLI Scripts

| File | Function/Class | Line/Pattern | Current Meaning | Desired Meaning | Risk | Required Action |
|------|---------------|-------------|-----------------|----------------|------|-----------------|
| `scripts/get_event_odds_by_event_id.py` | `main` | L11 `event_id` arg, L18 `api_client.get_event_final_odds(args.event_id)` | sofascore_external_event_id | Ambiguous — user may not know which ID they're passing | MEDIUM | Rename arg to `sofascore_event_id` or add `--source` flag |
| `scripts/get_event_results.py` | `main` | L9 `event_id` arg, L21 `api_client.get_event_results(args.event_id)` | sofascore_external_event_id | EXTERNAL_PROVIDER_ID_REQUIRED | MEDIUM | Rename arg to `sofascore_event_id` |
| `scripts/get_event_details.py` | `main` | L9 `event_id` arg, L16 `api_client.get_event_details(args.event_id)` | sofascore_external_event_id | EXTERNAL_PROVIDER_ID_REQUIRED | MEDIUM | Rename arg to `sofascore_event_id` |
| `scripts/sport_seasons_processing.py` | Various | L118/432 `event_payload['id']` | sofascore_external_event_id | sofascore_external_event_id (in context of SofaScore API response) | HIGH | After migration, this script must use mappings to write events |

### L. Legacy / Maintenance / Backfill Scripts

| File | Function/Class | Line/Pattern | Current Meaning | Desired Meaning | Risk | Required Action |
|------|---------------|-------------|-----------------|----------------|------|-----------------|
| `scripts/legacy/process_null_seasons_legacy_event_odds.py` | `process_event()` | L168 `event_id: int` | sofascore_external_event_id | sofascore_external_event_id | LOW | Legacy script; update if re-used |
| `scripts/legacy/extract_historical_results_legacy_event_odds.py` | Various | `event_id` throughout | sofascore_external_event_id | sofascore_external_event_id | LOW | Legacy script; update if re-used |
| `scripts/maintenance/backfill_event_entities_from_sofascore.py` | `main()` | L46 `f"/event/{event_id}"` | sofascore_external_event_id (Event.id is currently SofaScore ID) | EXTERNAL_PROVIDER_ID_REQUIRED | HIGH | After migration, must resolve sofascore_event_id from mapping |
| `scripts/backfill/backfill_event_metadata.py` | Various | L423 `f"/event/{event_id}"` | sofascore_external_event_id | EXTERNAL_PROVIDER_ID_REQUIRED | HIGH | After migration, must resolve sofascore_event_id from mapping |
| `scripts/backfill/backfill_event_metadata_with_db.py` | `_apply_updates()` | L515 `WHERE id = :event_id` | database_foreign_key | canonical_internal_event_id | SAFE_AFTER_FK_MIGRATION | Keep as is |

### M. Normalizer / Data Extraction

| File | Function/Class | Line/Pattern | Current Meaning | Desired Meaning | Risk | Required Action |
|------|---------------|-------------|-----------------|----------------|------|-----------------|
| `event_normalizer.py` | `get_event_information()` | L237 `"id": event.get("id")` | sofascore_external_event_id | sofascore_external_event_id | **CRITICAL** | This produces the payload that `upsert_event` uses. After migration, this value must be treated as `source_event_id`, NOT as `Event.id` |
| `sofascore/event_details.py` | `update_event_information_from_response()` | L52 `event_payload.get("id")` | sofascore_external_event_id | sofascore_external_event_id | **CRITICAL** | Must resolve canonical_event_id from mapping before upsert |

---

## 4. Clasificación por Categorías

### A. DB Schema / SQLAlchemy Models
- `Event.id` — **CRITICAL** — Must become autoincremental.
- `Result.event_id`, `Market.event_id`, `EventObservation.event_id`, `PredictionLog.event_id` — **SAFE_AFTER_FK_MIGRATION** — Already correct FKs; just need value migration.

### B. Repository Methods
- `EventRepository.upsert_event()` — **CRITICAL** — Core of the migration.
- All other repository methods (`get_event_by_id`, `delete_event`, `upsert_result`, `save_markets_from_response`, etc.) — **SAFE_AFTER_FK_MIGRATION** — Work with DB FKs, will work correctly once IDs are migrated.

### C. SQL Views / Materialized Views
- All 7 views — **SAFE_AFTER_FK_MIGRATION** — Reference `events.id` via JOINs, which will point to canonical IDs after migration. Must be recreated after migration to pick up any schema changes.

### D. SofaScore API Calls
- `get_event_final_odds()`, `get_event_results()`, `get_event_details()`, `fetch_event_response()` — **CRITICAL** — Must receive `sofascore_event_id` (resolved from mapping), NOT `canonical_event_id`.

### E. Daily Discovery Flow
- `persist_event_with_odds()` — **CRITICAL** — Must separate SofaScore ID from canonical ID after upsert.

### F. Pre-Start Check Flow
- Multiple entry points — **CRITICAL** — SofaScore API calls (`get_event_final_odds`, `get_event_results`) must resolve `sofascore_event_id` from mapping.

### G. Results Collection Flow
- `_collect_results_for_events()` — **CRITICAL** — API calls use `event.id` which will become canonical; must resolve SofaScore ID for API.

### H. Odds Ingestion Flow
- **SAFE_AFTER_FK_MIGRATION** — Receives `event_id` from callers; as long as callers pass canonical, this layer is fine.

### I. Alerts / Pillars / Prediction Flows
- **SAFE_AFTER_FK_MIGRATION** — Use `event_obj.id` from DB; will automatically use canonical after migration.
- **Exception**: `historical_form.py` uses `event.get('id')` from SofaScore API responses for deduplication — this is fine, stays as SofaScore ID (never written to DB as PK).

### J. Observations Flow
- **SAFE_AFTER_FK_MIGRATION** — All methods receive canonical FK.

### K. CLI Scripts
- **MEDIUM** — Need parameter rename for clarity. Currently pass values directly to SofaScore API.

### L. Legacy / Maintenance / Backfill Scripts
- **LOW to HIGH** — Legacy scripts assume `Event.id == SofaScore ID`. Maintenance/backfill scripts that call SofaScore API with `Event.id` need mapping resolution.

---

## 5. Mapa de Flujo Actual

```
SofaScore API Response
  └─> event.get("id")  →  sofascore_event_id (e.g., 12345678)
        │
        ├── get_event_information() puts it in event_data["event"]["id"]
        │
        ├── EventRepository.upsert_event()
        │       └── Event(id=sofascore_event_id)  ← ⚠️ WRITES EXTERNAL ID AS PK
        │       └── events.id = 12345678
        │
        ├── MarketOddsIngestionService.save_from_event_odds_response(sofascore_event_id, ...)
        │       └── markets.event_id = 12345678  ← ⚠️ FK = EXTERNAL ID
        │
        ├── ResultRepository.upsert_result(sofascore_event_id, ...)
        │       └── results.event_id = 12345678  ← ⚠️ FK = EXTERNAL ID
        │
        └── Later jobs read events back:
                event.id = 12345678
                ├── api_client.get_event_final_odds(event.id)  ← Works by coincidence (ID is SofaScore)
                ├── api_client.get_event_results(event.id)  ← Works by coincidence
                └── ResultRepository.upsert_result(event.id, ...)  ← FK matches because PK = SofaScore ID

⚠️ PROBLEM: events.id == sofascore_event_id throughout the entire pipeline
```

---

## 6. Mapa de Flujo Objetivo

```
SofaScore API Response
  └─> event.get("id")  →  sofascore_event_id (e.g., 12345678)
        │
        ├── get_event_information() puts it in event_data["event"]["id"]
        │     (still sofascore_event_id at this point — not yet a canonical ID)
        │
        ├── EventRepository.upsert_event()  (MODIFIED)
        │       ├── Step 1: Look for existing mapping:
        │       │     EventSourceMappingRepository.get_event_id_by_source("sofascore", "12345678")
        │       │
        │       ├── If mapping exists:
        │       │     canonical_event_id = mapping.event_id
        │       │     Load Event by canonical_event_id, update metadata
        │       │
        │       ├── If no mapping:
        │       │     Create Event() WITHOUT passing id (autoincrement)
        │       │     session.flush() → canonical_event_id assigned (e.g., 42)
        │       │     Create EventSourceMapping(event_id=42, source="sofascore", source_event_id="12345678")
        │       │
        │       └── Return Event with id=42 (canonical)
        │
        ├── MarketOddsIngestionService.save_from_event_odds_response(canonical_event_id=42, ...)
        │       └── markets.event_id = 42  ← ✅ CANONICAL FK
        │
        ├── ResultRepository.upsert_result(canonical_event_id=42, ...)
        │       └── results.event_id = 42  ← ✅ CANONICAL FK
        │
        └── Later jobs read events back:
                event.id = 42 (canonical)
                │
                ├── Need to call SofaScore API? RESOLVE FIRST:
                │     sofascore_event_id = EventSourceMappingRepository
                │         .resolve_required_source_event_id(42, "sofascore")
                │     → returns "12345678"
                │
                ├── api_client.get_event_final_odds(sofascore_event_id)  ← ✅ Correct provider ID
                ├── api_client.get_event_results(sofascore_event_id)  ← ✅ Correct provider ID
                └── ResultRepository.upsert_result(canonical_event_id=42, ...)  ← ✅ Canonical FK

✅ CLEAN SEPARATION: canonical_event_id for DB, sofascore_event_id for API
```

### Flujo para llamadas externas:

```
canonical_event_id (42)
  └─> EventSourceMappingRepository.resolve_required_source_event_id(42, "sofascore")
        └─> sofascore_event_id ("12345678")
              └─> SofaScore API: /event/12345678/odds/1/all
```

---

## 7. Lista de Breaking Points

### Breaking Point 1: `EventRepository.upsert_event()` — `Event(id=event_id)`

- **Por qué se rompe**: Actualmente escribe `event_payload["id"]` (SofaScore ID, e.g., `12345678`) como `Event.id`. Si `Event.id` cambia a autoincrement, PostgreSQL asignaría un ID diferente (e.g., `1`).
- **Qué dato espera**: Un entero que es simultáneamente SofaScore ID y PK.
- **Qué dato recibiría**: Un autoincremental que no coincide con el SofaScore ID.
- **Cómo corregirlo**: No pasar `id` al constructor de `Event`. Dejar que autoincrement asigne. Crear `EventSourceMapping` con el SofaScore ID.

### Breaking Point 2: `persist_event_with_odds()` — `MarketOddsIngestionService.save_from_event_odds_response(event_id, ...)`

- **Por qué se rompe**: Usa `event.get("id")` (SofaScore ID) para guardar markets. Después de la migración, `event_id` en markets debe ser canonical.
- **Qué dato espera**: SofaScore ID (que coincide con `events.id` actual).
- **Qué dato recibiría**: SofaScore ID (que ya NO coincide con `events.id`).
- **Cómo corregirlo**: Usar `db_event.id` (canonical) en lugar de `event.get("id")`.

### Breaking Point 3: `api_client.get_event_final_odds(event_data["id"], ...)` en pre-start check

- **Por qué se rompe**: Pasa `event_data["id"]` (que tras migración será canonical) a SofaScore API.
- **Qué dato espera**: SofaScore event ID para endpoint `/event/{id}/odds/1/all`.
- **Qué dato recibiría**: Canonical ID (e.g., `42`) que no existe en SofaScore → 404.
- **Cómo corregirlo**: Resolver `sofascore_event_id` vía mapping antes de la llamada API.

### Breaking Point 4: `api_client.get_event_results(event.id)` en results collection

- **Por qué se rompe**: Pasa `event.id` (canonical) a SofaScore API `/event/{event_id}`.
- **Qué dato espera**: SofaScore ID.
- **Qué dato recibiría**: Canonical ID → 404.
- **Cómo corregirlo**: Resolver `sofascore_event_id` vía mapping.

### Breaking Point 5: `intraday_result_freshness.py` — `api_client._request_json(f"/event/{event_id}")`

- **Por qué se rompe**: Pasa `event_data["id"]` (canonical) a SofaScore API.
- **Qué dato espera**: SofaScore ID.
- **Qué dato recibiría**: Canonical ID → 404.
- **Cómo corregirlo**: Resolver `sofascore_event_id` vía mapping.

### Breaking Point 6: `fetch_event_response()` — deletion on 404

- **Por qué se rompe**: Usa `EventRepository.batch_delete_events([event_id])` con what it thinks is a DB ID after a 404.
- **Qué dato espera**: Canonical ID.
- **Qué dato recibiría**: After migration, if called with SofaScore ID, would try to delete non-existent canonical ID.
- **Cómo corregirlo**: Ensure function receives canonical_event_id for deletion and sofascore_event_id for API call.

### Breaking Point 7: `timing.py` `should_extract_odds_for_event()` — calls `api_client.get_event_results(event_id)`

- **Por qué se rompe**: Internally calls SofaScore API with `event_id` which post-migration would be canonical.
- **Qué dato espera**: SofaScore ID for API call.
- **Qué dato recibiría**: Canonical ID → 404.
- **Cómo corregirlo**: Resolve sofascore_event_id from mapping before API call.

### Breaking Point 8: Maintenance/Backfill scripts — `f"/event/{event_id}"`

- **Por qué se rompe**: All backfill scripts iterate `Event` objects and call SofaScore API with `event.id`.
- **Qué dato espera**: SofaScore ID.
- **Qué dato recibiría**: Canonical ID → 404.
- **Cómo corregirlo**: Resolve sofascore_event_id from mapping.

### Breaking Point 9: `event_normalizer.py` `get_event_information()` — `"id": event.get("id")`

- **Por qué se rompe**: Produces the SofaScore ID in `event_data["event"]["id"]`, which `upsert_event()` currently uses as PK.
- **Qué dato espera**: `upsert_event()` expects this to be the Event PK.
- **Qué dato recibiría**: Still a SofaScore ID, but it must NOT be used as PK anymore.
- **Cómo corregirlo**: `upsert_event()` must treat `event_data["event"]["id"]` as `source_event_id`, not as PK.

---

## 8. Recomendaciones de Naming

### Convención de Nombres

| Concepto | Variable Name | Uso |
|----------|--------------|-----|
| ID interno canónico | `canonical_event_id` o simplemente `event_id` (en contexto interno DB) | PKs, FKs, db queries internas |
| ID externo de SofaScore | `sofascore_event_id` | Llamadas a SofaScore API |
| ID externo de OddsPapi | `oddspapi_fixture_id` | Llamadas a OddsPapi API |
| ID externo de Pinnacle | `pinnacle_event_id` | Llamadas a Pinnacle API |
| ID externo genérico de proveedor | `source_event_id` | En `event_source_mappings`, o cuando el proveedor no está especificado |
| Proveedor | `source` | String identificador del proveedor |

### Regla Principal

> **Dentro del dominio interno (DB, repositories, services), `event_id` siempre significa `canonical_event_id`.** Cuando se necesite un ID de proveedor externo, usar el nombre explícito: `sofascore_event_id`, `oddspapi_fixture_id`, `pinnacle_event_id`, etc. NUNCA usar `event_id` genérico para llamadas a APIs externas.

---

## 9. Schema Final Propuesto

### `Event` (modified)

```python
class Event(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True, autoincrement=True)  # ← CHANGED: autoincrement

    custom_id = Column(Text)
    slug = Column(Text, nullable=False)
    start_time_utc = Column(DateTime, nullable=False)
    sport = Column(Text, nullable=False)

    # LEGACY_EVENT_TEXT_FIELDS: kept for backward compatibility
    competition = Column(Text, nullable=False)
    country = Column(Text)
    home_team = Column(Text, nullable=False)
    away_team = Column(Text, nullable=False)

    gender = Column(String(10), nullable=False, default="unknown")
    discovery_source = Column(String(50), nullable=False, default="dropping_odds")

    season_id = Column(Integer, ForeignKey("seasons.id", ondelete="SET NULL"))
    round = Column(Text)
    alert_sent = Column(Boolean, default=False, nullable=False)

    home_participant_id = Column(Integer, ForeignKey("participants.participant_id", ondelete="SET NULL"))
    away_participant_id = Column(Integer, ForeignKey("participants.participant_id", ondelete="SET NULL"))
    competition_id = Column(Integer, ForeignKey("competitions.competition_id", ondelete="SET NULL"))

    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)

    # Relationships
    result = relationship("Result", back_populates="event", uselist=False, cascade="all, delete-orphan")
    observations = relationship("EventObservation", back_populates="event", cascade="all, delete-orphan")
    prediction_logs = relationship("PredictionLog", back_populates="event", uselist=False, cascade="all, delete-orphan")
    season = relationship("Season", back_populates="events")
    markets = relationship("Market", back_populates="event", cascade="all, delete-orphan")
    home_participant = relationship("Participant", foreign_keys=[home_participant_id])
    away_participant = relationship("Participant", foreign_keys=[away_participant_id])
    competition_ref = relationship("Competition", foreign_keys=[competition_id])
    source_mappings = relationship("EventSourceMapping", back_populates="event", cascade="all, delete-orphan")
```

### `EventSourceMapping` (new)

```python
class EventSourceMapping(Base):
    __tablename__ = "event_source_mappings"

    mapping_id = Column(Integer, primary_key=True, autoincrement=True)

    event_id = Column(Integer, ForeignKey("events.id", ondelete="CASCADE"), nullable=False)

    source = Column(Text, nullable=False)            # "sofascore", "oddspapi", "pinnacle", etc.
    source_event_id = Column(Text, nullable=False)    # External ID as string

    source_sport_id = Column(Text)                    # Provider's sport ID (optional)
    source_tournament_id = Column(Text)               # Provider's tournament ID (optional)
    source_season_id = Column(Text)                   # Provider's season ID (optional)

    match_method = Column(Text, nullable=False, default="direct")  # "direct", "fuzzy_name", "time_window", "legacy_primary_key_migration"
    confidence = Column(Numeric(5, 3))                 # 0.000 to 1.000

    raw_external_providers = Column(JSONB)             # Optional: raw provider payload

    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)

    event = relationship("Event", back_populates="source_mappings")

    __table_args__ = (
        UniqueConstraint("source", "source_event_id", name="unique_event_source_mapping"),
        Index("idx_event_source_mappings_event_id", "event_id"),
        Index("idx_event_source_mappings_source_event_id", "source", "source_event_id"),
        Index("idx_event_source_mappings_source", "source"),
    )
```

### Evaluación de Columnas Adicionales

| Column | Recommendation | Rationale |
|--------|---------------|-----------|
| `provider_name` | **NO** | Redundant with `source` |
| `source_slug` | **NO** | Not needed; slug is in `events.slug` already |
| `is_primary` | **DEFER** | Could be useful for multi-provider dedup, but not needed for initial migration. Add later if needed. |
| `last_seen_at` | **DEFER** | Useful for staleness detection, but `updated_at` covers most cases. Add if monitoring requirements arise. |
| `raw_payload` | **NO** | Too large; `raw_external_providers` (JSONB) already covers structured metadata storage. |

---

## 10. Repositorio de Mappings Propuesto

### Archivo: `infrastructure/persistence/repositories/event_source_mapping_repository.py`

```python
class EventSourceMappingRepository:

    @staticmethod
    def get_event_id_by_source(source: str, source_event_id: str) -> Optional[int]:
        """
        Look up the canonical event_id for a given provider + external ID.

        Receives:
            source: Provider name (e.g., "sofascore")
            source_event_id: External event ID as string (e.g., "12345678")

        Returns:
            canonical_event_id (int) if mapping exists, None otherwise.

        Use cases:
            - During upsert_event(), check if SofaScore event already has a canonical ID.
            - During API response processing, find canonical ID for a known external ID.

        Error handling:
            Returns None if not found. Does not raise.
        """

    @staticmethod
    def get_source_event_id(event_id: int, source: str) -> Optional[str]:
        """
        Get the external provider ID for a canonical event.

        Receives:
            event_id: Canonical event ID (int)
            source: Provider name (e.g., "sofascore")

        Returns:
            source_event_id (str) if mapping exists, None otherwise.

        Use cases:
            - Before calling SofaScore API, resolve the external ID.
            - Before calling any provider API, resolve the external ID.

        Error handling:
            Returns None if not found. Does not raise.
        """

    @staticmethod
    def upsert_mapping(
        event_id: int,
        source: str,
        source_event_id: str,
        source_sport_id: Optional[str] = None,
        source_tournament_id: Optional[str] = None,
        source_season_id: Optional[str] = None,
        match_method: str = "direct",
        confidence: Optional[float] = None,
        raw_external_providers: Optional[dict] = None,
    ) -> EventSourceMapping:
        """
        Insert or update a mapping between a canonical event and an external provider ID.

        Receives:
            event_id: Canonical event ID (must already exist in events table)
            source: Provider name
            source_event_id: External event ID as string
            source_sport_id: Optional provider sport ID
            source_tournament_id: Optional provider tournament ID
            source_season_id: Optional provider season ID
            match_method: How the mapping was established ("direct", "fuzzy_name", "time_window", "legacy_primary_key_migration")
            confidence: Match confidence 0.000 to 1.000
            raw_external_providers: Optional JSONB metadata

        Returns:
            The created or updated EventSourceMapping instance.

        Use cases:
            - During upsert_event() when creating a new event.
            - During batch migration to create initial SofaScore mappings.
            - When integrating a new provider (e.g., OddsPapi).

        Error handling:
            Raises on database errors. Caller should handle.
        """

    @staticmethod
    def resolve_required_source_event_id(event_id: int, source: str) -> str:
        """
        Get the external provider ID, raising an error if not found.

        Receives:
            event_id: Canonical event ID
            source: Provider name

        Returns:
            source_event_id (str).

        Use cases:
            - When calling an external API where the mapping MUST exist.
            - Provides fail-fast semantics instead of silent None.

        Error handling:
            Raises ValueError if mapping not found. This is intentional:
            if we're about to call an external API and don't have the mapping,
            something is fundamentally wrong and we should fail loudly.
        """

    @staticmethod
    def get_mappings_for_event(event_id: int) -> list[EventSourceMapping]:
        """
        Get all source mappings for a canonical event.

        Receives:
            event_id: Canonical event ID

        Returns:
            List of EventSourceMapping instances. Empty list if none found.

        Use cases:
            - Debugging / admin views.
            - Multi-provider cross-reference.

        Error handling:
            Returns empty list on not found. Does not raise.
        """
```

---

## 11. Cambios Conceptuales en `EventRepository.upsert_event()`

### Lógica Actual (Problema)

```python
event_id = event_payload.get('id')           # SofaScore ID
event_obj = session.query(Event).filter(Event.id == event_id).first()
if event_obj:
    # update with SofaScore ID as PK
else:
    event_obj = Event(id=event_id, ...)      # SofaScore ID written as PK
```

### Lógica Propuesta (Post-Migración)

```python
# 1. Extract SofaScore ID from payload
sofascore_event_id = str(event_payload.get('id'))  # Always treat as external

# 2. Check if mapping exists
canonical_event_id = EventSourceMappingRepository.get_event_id_by_source(
    source="sofascore",
    source_event_id=sofascore_event_id,
)

if canonical_event_id:
    # 3a. Mapping exists → load and update
    event_obj = session.query(Event).filter(Event.id == canonical_event_id).first()
    # ... update metadata fields ...
else:
    # 3b. No mapping → create new event WITHOUT passing id
    event_obj = Event(
        # id is NOT set — autoincrement assigns it
        slug=event_payload.get('slug') or sofascore_event_id,
        start_time_utc=datetime.fromtimestamp(event_payload['startTimestamp']),
        sport=event_payload.get('sport') or 'Unknown',
        # ... other fields ...
    )
    session.add(event_obj)
    session.flush()  # → event_obj.id is now the canonical ID

    # 4. Create mapping
    EventSourceMappingRepository.upsert_mapping(
        event_id=event_obj.id,  # canonical
        source="sofascore",
        source_event_id=sofascore_event_id,
        match_method="direct",
        confidence=1.000,
    )

# 5. Return Event with canonical id
return event_obj
```

### Principio Importante

> Después del cambio, cualquier función que necesite llamar a SofaScore **DEBE resolver primero**:
>
> ```python
> sofascore_event_id = EventSourceMappingRepository.resolve_required_source_event_id(
>     event_id=canonical_event_id,
>     source="sofascore",
> )
> response = api_client.get_event_final_odds(sofascore_event_id)
> ```
>
> **NUNCA** llamar a SofaScore con `Event.id` directamente.

---

## 12. Plan Conceptual de Migración

> Este bloque se conserva como referencia de diseño. La implementación actual ya sigue este flujo en `database.py`, con batching, validación y persistencia de estado.

> [!CAUTION]
> Este plan NO debe ejecutarse todavía. Es un diseño para validación y aprobación.

### Paso 0: Backup obligatorio

```sql
-- Full database backup before any changes
pg_dump -Fc -f pre_migration_backup_$(date +%Y%m%d_%H%M%S).dump sofascore_db
```

### Paso 1: Crear tabla `event_source_mappings`

```sql
CREATE TABLE event_source_mappings (
    mapping_id SERIAL PRIMARY KEY,
    event_id INTEGER NOT NULL,
    source TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    source_sport_id TEXT,
    source_tournament_id TEXT,
    source_season_id TEXT,
    match_method TEXT NOT NULL DEFAULT 'direct',
    confidence NUMERIC(5,3),
    raw_external_providers JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_event_source_mapping UNIQUE (source, source_event_id)
);

CREATE INDEX idx_event_source_mappings_event_id ON event_source_mappings (event_id);
CREATE INDEX idx_event_source_mappings_source_event_id ON event_source_mappings (source, source_event_id);
CREATE INDEX idx_event_source_mappings_source ON event_source_mappings (source);
```

> **Nota**: FK constraint a `events.id` se agrega DESPUÉS de la migración de IDs.

### Paso 2: Poblar mappings iniciales desde datos existentes

```sql
-- Every existing event gets a SofaScore mapping
INSERT INTO event_source_mappings (event_id, source, source_event_id, match_method, confidence)
SELECT id, 'sofascore', id::text, 'legacy_primary_key_migration', 1.000
FROM events;
```

### Paso 3: Crear columna temporal y preservar SofaScore IDs

```sql
-- Add temporary column to preserve the original SofaScore ID
ALTER TABLE events ADD COLUMN old_sofascore_id INTEGER;
UPDATE events SET old_sofascore_id = id;
```

### Paso 4: Crear nueva secuencia e IDs canónicos

```sql
-- Create a sequence that starts after the max existing ID
-- to avoid conflicts during transition
CREATE SEQUENCE events_canonical_id_seq START WITH (SELECT MAX(id) + 1 FROM events);

-- Create temporary mapping table for old→new ID translation
CREATE TEMPORARY TABLE id_translation AS
SELECT id AS old_id, nextval('events_canonical_id_seq') AS new_id
FROM events
ORDER BY id;
```

### Paso 5: Actualizar FKs internas

```sql
-- Disable FK constraints temporarily
SET CONSTRAINTS ALL DEFERRED;

-- Update all FK tables to use new canonical IDs
UPDATE results r
SET event_id = t.new_id
FROM id_translation t
WHERE r.event_id = t.old_id;

UPDATE markets m
SET event_id = t.new_id
FROM id_translation t
WHERE m.event_id = t.old_id;

UPDATE event_observations eo
SET event_id = t.new_id
FROM id_translation t
WHERE eo.event_id = t.old_id;

UPDATE prediction_logs pl
SET event_id = t.new_id
FROM id_translation t
WHERE pl.event_id = t.old_id;

-- Update event_source_mappings to reference new IDs
UPDATE event_source_mappings esm
SET event_id = t.new_id
FROM id_translation t
WHERE esm.event_id = t.old_id;

-- Update events table PK
UPDATE events e
SET id = t.new_id
FROM id_translation t
WHERE e.id = t.old_id;
```

### Paso 6: Reconfigurar secuencia y constraints

```sql
-- Set the sequence as the default for events.id
ALTER TABLE events ALTER COLUMN id SET DEFAULT nextval('events_canonical_id_seq');
ALTER SEQUENCE events_canonical_id_seq OWNED BY events.id;

-- Add FK constraint to event_source_mappings
ALTER TABLE event_source_mappings
ADD CONSTRAINT fk_event_source_mappings_event_id
FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE;

-- Re-enable constraints
SET CONSTRAINTS ALL IMMEDIATE;
```

### Paso 7: Limpiar columna temporal

```sql
-- Verify migration before dropping
-- ALTER TABLE events DROP COLUMN old_sofascore_id;
-- (Keep it temporarily for rollback capability)
```

### Paso 8: Recrear views y materialized views

```python
# Call existing functions
create_or_replace_views(engine)
create_or_replace_materialized_views(engine)
```

### Paso 9: Validaciones

```sql
-- Validation queries

-- 1. Event count consistency
SELECT 'events_count' AS check_name,
       (SELECT COUNT(*) FROM events) AS count;

-- 2. Results FK integrity
SELECT 'orphan_results' AS check_name,
       COUNT(*) AS count
FROM results r
LEFT JOIN events e ON r.event_id = e.id
WHERE e.id IS NULL;

-- 3. Markets FK integrity
SELECT 'orphan_markets' AS check_name,
       COUNT(*) AS count
FROM markets m
LEFT JOIN events e ON m.event_id = e.id
WHERE e.id IS NULL;

-- 4. Observations FK integrity
SELECT 'orphan_observations' AS check_name,
       COUNT(*) AS count
FROM event_observations eo
LEFT JOIN events e ON eo.event_id = e.id
WHERE e.id IS NULL;

-- 5. Prediction logs FK integrity
SELECT 'orphan_prediction_logs' AS check_name,
       COUNT(*) AS count
FROM prediction_logs pl
LEFT JOIN events e ON pl.event_id = e.id
WHERE e.id IS NULL;

-- 6. Mapping count should equal event count (at minimum)
SELECT 'mapping_count_vs_events' AS check_name,
       (SELECT COUNT(*) FROM events) AS events_count,
       (SELECT COUNT(*) FROM event_source_mappings WHERE source = 'sofascore') AS sofascore_mappings;

-- 7. Every event must have at least one SofaScore mapping
SELECT 'events_without_sofascore_mapping' AS check_name,
       COUNT(*) AS count
FROM events e
LEFT JOIN event_source_mappings esm ON e.id = esm.event_id AND esm.source = 'sofascore'
WHERE esm.mapping_id IS NULL;

-- 8. No duplicate mappings
SELECT 'duplicate_mappings' AS check_name,
       COUNT(*) AS count
FROM (
    SELECT source, source_event_id, COUNT(*)
    FROM event_source_mappings
    GROUP BY source, source_event_id
    HAVING COUNT(*) > 1
) dups;

-- 9. Views return data
SELECT 'v_dual_process_event_odds_count' AS check_name,
       COUNT(*) AS count FROM v_dual_process_event_odds;

SELECT 'mv_alert_events_count' AS check_name,
       COUNT(*) AS count FROM mv_alert_events;

SELECT 'season_events_with_results_count' AS check_name,
       COUNT(*) AS count FROM season_events_with_results;
```

---

## 13. Preguntas Abiertas

> Varias de estas preguntas ya quedaron resueltas por la implementación actual; se conservan aquí como contexto histórico.

1. **¿Se debe soportar que un mismo proveedor pueda tener múltiples IDs para el mismo evento canónico?** (e.g., SofaScore puede retornar un evento con diferentes IDs si hay re-scheduling). La constraint `UNIQUE(source, source_event_id)` asegura 1:1 dentro de cada proveedor.

2. **¿Qué pasa con los CLI scripts (`get_event_odds_by_event_id.py`, etc.)? ¿Deben aceptar canonical ID o SofaScore ID?** Recomendación: agregar flag `--source sofascore` para especificar que el ID es externo, y sin flag asume canonical.

3. **¿Se deben migrar los IDs existentes a nuevos valores autoincrementales, o mantener los mismos valores numéricos como canonical IDs?** La opción de mantener los mismos valores simplifica la migración (no hay cambio de FKs), pero deja la semántica confusa. Recomendación: **crear nuevos IDs autoincrementales** para separación limpia.

4. **¿La tabla `seasons` tiene el mismo problema?** `seasons.id` actualmente almacena el SofaScore `season_id` directamente (L107 en models.py: `id = Column(Integer, primary_key=True)` con comment "Season ID from SofaScore API"). Esto requerirá una migración similar eventualmente.

5. **¿Cuándo parar el servicio durante la migración?** La migración requiere actualizar FKs en todas las tablas. Se recomienda una ventana de mantenimiento de ~30 minutos.

6. **¿Se debe implementar una capa de compatibilidad temporal?** (e.g., helper que acepta tanto canonical como SofaScore ID y resuelve automáticamente). Esto agrega complejidad pero reduce riesgo de breaking changes durante la transición.
