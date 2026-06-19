# Event Identity Cleanup Audit

> Commit reviewed: `bd53863`  
> Date: 2026-06-19  
> Purpose: identify unreachable, legacy, obsolete, or cleanup-worthy code after the canonical event identity migration.

---

## 1. Immediate bugfix prompt

Use this prompt in the IDE before continuing with OddsPapi integration:

```text
Actúa como senior software engineer. Revisa `infrastructure/persistence/repositories/event_repository.py` en el commit actual.

Corrige solo estos dos puntos, sin refactors adicionales:

1. En `EventRepository.upsert_event()`, dentro de la rama de update, hay un log/debug que usa `event_id`, pero esa variable ya no existe en el flujo canónico. Reemplázala por `event_obj.id` si quieres mostrar el canonical ID, o por `sofascore_event_id` si quieres mostrar el ID externo. Preferencia: loguear ambos explícitamente.

2. Revisa la indentación del bloque que procesa `home_participant_data`. Actualmente parece estar dentro del bloque `if 'season_id' in event_payload and event_payload['season_id']:` mientras `away_participant_data` y `competition_data` están fuera. El procesamiento de home, away y competition debe ocurrir aunque el evento no tenga `season_id`.

Criterios de aceptación:
- `upsert_event()` no referencia variables inexistentes.
- Home participant, away participant y competition se procesan de forma simétrica.
- No se vuelve a escribir `Event(id=sofascore_event_id)`.
- `event_payload['id']` sigue tratándose como `sofascore_event_id` externo.
- `Event.id` sigue siendo canonical/autoincremental.
- Agrega o ajusta tests mínimos si existen tests para `EventRepository.upsert_event()`.
```

---

## 2. Executive summary

The canonical event identity migration appears structurally complete:

- `events.id` is now the canonical internal ID.
- SofaScore external IDs live in `event_source_mappings`.
- Internal FKs now point to canonical IDs.
- Runtime SofaScore flows are expected to resolve `sofascore_event_id` before API calls.

The codebase now has three cleanup categories:

1. **Immediate correctness cleanup**: small issues in `EventRepository.upsert_event()`.
2. **Safe legacy deletion candidates**: old `event_odds` / `odds_snapshot` scripts that are explicitly incompatible with the normalized market architecture.
3. **Deferred cleanup**: one-time migrations and compatibility shims that should remain for a short stabilization window, then be removed or moved out of runtime startup.

---

## 3. Findings by priority

### P0 — Fix before OddsPapi work

#### 3.1 `EventRepository.upsert_event()` stale variable in update branch

**File:** `infrastructure/persistence/repositories/event_repository.py`

**Issue:** In the update branch, a debug log still references `event_id`, but the canonicalized flow now uses `sofascore_event_id` and `event_obj.id`.

**Why it matters:** This can raise a `NameError` only when that branch executes, making it easy to miss during simple runs.

**Action:** Replace with explicit IDs:

```python
logger.debug(
    "Overwrote discovery_source to 'dropping_odds' for canonical_event_id=%s sofascore_event_id=%s (was=%s)",
    event_obj.id,
    sofascore_event_id,
    old_source,
)
```

---

#### 3.2 `home_participant_data` processing appears incorrectly nested

**File:** `infrastructure/persistence/repositories/event_repository.py`

**Issue:** The home participant processing appears nested under the `season_id` block, while away participant and competition processing are outside that block.

**Why it matters:** Events without `season_id` may skip home participant upsert while still processing away participant and competition.

**Action:** Move home participant processing outside the season block so that this logic is symmetrical:

```text
season handling
home participant handling
away participant handling
competition handling
```

---

## 4. Strong deletion/archive candidates

### 4.1 `scripts/legacy/*event_odds*` scripts

These scripts are explicitly marked as incompatible with the final normalized market architecture.

Files found:

- `scripts/legacy/collect_yesterday_odds_legacy_event_odds.py`
- `scripts/legacy/process_null_seasons_legacy_event_odds.py`
- `scripts/legacy/backfill_results_legacy_event_odds.py`
- `scripts/legacy/extract_historical_results_legacy_event_odds.py`
- `scripts/legacy/csv_migration_legacy_event_odds.py`

Evidence:

- Several scripts explicitly say: `LEGACY: old event_odds / odds_snapshot migration script. Not compatible with final market-based odds architecture.`
- Some reference removed or non-exported concepts such as `EventOdds`, `OddsRepository`, `Event.event_odds`, `OddsRepository.upsert_event_odds`, and `OddsRepository.create_odds_snapshot`.
- These scripts also assume older ID semantics in places where `event.id` could be used directly for SofaScore API calls.

Recommended action:

```text
Move to `archive/legacy_event_odds_scripts/` or delete after backup/tag.
```

Preferred staged cleanup:

1. Create a git tag before deletion: `pre-legacy-event-odds-cleanup`.
2. Move scripts to an archive folder or delete them.
3. Remove imports/references to `EventOdds` and `OddsRepository` if no active code needs them.
4. Run `scripts/maintenance/check_no_active_legacy_odds_writes.py`.
5. If clean, remove the checker itself or keep it only as a CI guard for one more cycle.

---

### 4.2 `scripts/legacy/csv_migration_legacy_event_odds.py`

**Issue:** This script imports `EventOdds`, but current `models.py` no longer defines `EventOdds`.

**Risk:** It is likely broken if executed.

**Action:** Delete or archive. If historical CSV migration is still needed, rewrite against:

```text
markets
market_choices
market_choice_snapshots
results
event_source_mappings
```

Do not patch this script in place unless there is a real business need.

---

### 4.3 `scripts/legacy/collect_yesterday_odds_legacy_event_odds.py`

**Issue:** This script imports old modules (`repository`, `models`, `database`) and uses old relationships like `Event.event_odds`.

**Risk:** Very likely unreachable/broken after the project modularization and normalized market migration.

**Action:** Delete or archive.

---

## 5. Keep temporarily, then remove from runtime startup

### 5.1 One-time canonical identity migration inside `DatabaseManager.check_and_migrate_schema()`

**File:** `infrastructure/persistence/database.py`

**Current behavior:** `check_and_migrate_schema()` calls `_migrate_events_to_canonical_identity()` during startup/schema sync.

**Why it exists:** It safely canonicalized event IDs, migrated internal FKs, created SofaScore mappings, restored FKs, ensured sequence default, and validates integrity.

**Cleanup recommendation:** Keep for now, but do not keep forever in the normal startup path.

Suggested lifecycle:

```text
Now: keep enabled.
After 1-2 stable deploys: convert to explicit maintenance command or guard behind env flag.
Later: remove from automatic startup and keep only the validation script.
```

Reason: heavy one-time migrations inside app startup increase startup complexity and future risk, even if they are idempotent.

---

### 5.2 `event_migration_status` table

**File:** `infrastructure/persistence/database.py`

**Purpose:** Durable marker for `event_identity_canonicalization`.

**Cleanup recommendation:** Keep while the startup migration remains. If the one-time migration is removed from runtime startup, either:

- keep the table as migration history, or
- move this information to formal migration tooling/documentation.

Do not delete immediately.

---

### 5.3 `_drop_legacy_odds_tables()` in startup migration flow

**File:** `infrastructure/persistence/database.py`

**Current behavior:** Drops `odds_snapshot` and `event_odds` if they exist.

**Cleanup recommendation:** Keep until legacy scripts are deleted/archived and production DB is verified clean. Then remove from automatic startup and move to a one-time maintenance script if needed.

---

## 6. Compatibility shims to track

### 6.1 Legacy event text fields

**Files:**

- `infrastructure/persistence/models.py`
- `infrastructure/persistence/repositories/event_repository.py`

Fields:

```text
Event.competition
Event.home_team
Event.away_team
```

Current comments say they are kept for backward compatibility with historical rows and old runtime paths.

**Recommendation:** Keep for now. Remove only after:

1. Every active flow reads display names from normalized participants/competitions.
2. All historical rows have `home_participant_id`, `away_participant_id`, and `competition_id`.
3. CLI/reporting/export views no longer depend on text fallback.

Add a tracking issue or checklist before removal.

---

### 6.2 `EventRepository._build_event_data_with_legacy_fallback()`

**Current role:** Keeps runtime stable if normalized participants/competition are missing.

**Recommendation:** Keep temporarily. Once backfill coverage is complete, replace broad fallback with stricter validation or remove it.

---

## 7. Risky naming / semantic cleanup

### 7.1 SofaScore API helper names still use generic `event_id`

**File:** `modules/sofascore/event_details.py`

Examples:

```python
fetch_event_response(client, event_id: int, ...)
get_event_details(client, event_id: int)
get_event_results(client, event_id: int, ...)
```

These functions build `/event/{event_id}` endpoints, so the parameter is actually `sofascore_event_id`, not canonical `event_id`.

**Recommendation:** Rename parameters internally to `sofascore_event_id` and update docstrings. This is not necessarily a functional bug if callers already resolve IDs correctly, but it is a readability and future-safety issue.

Important: Be careful with branches that also perform DB work, observations, deletes, or updates. Those may need both:

```text
canonical_event_id
sofascore_event_id
```

---

### 7.2 Historical/backfill scripts should explicitly state ID type

Files to review:

- `scripts/backfill/backfill_event_metadata.py`
- `scripts/backfill/backfill_event_metadata_with_db.py`
- `scripts/maintenance/backfill_event_entities_from_sofascore.py`
- `scripts/gender_reorganization.py`
- `scripts/maintenance/correct_tennis_sport_classification.py`

Some have already adopted `resolve_sofascore_event_id()`. Others may still need naming/docstring cleanup.

Recommendation:

```text
Any variable passed to SofaScore API must be named `sofascore_event_id`.
Any variable used for DB operations must be named `event_id` or `canonical_event_id`.
```

---

## 8. Maintenance scripts with now-obsolete integrated migrations

### 8.1 `scripts/gender_reorganization.py`

**Issue:** It still contains `ensure_gender_column_exists()` that manually alters `events.gender`.

**Why it is cleanup-worthy:** `events.gender` is now part of the SQLAlchemy model and generic schema migration flow.

**Recommendation:** Keep the gender correction behavior if still useful, but remove or deprecate the embedded schema mutation. Schema changes should live in the central migration system, not in a domain correction script.

---

## 9. OddsPapi exploratory code cleanup plan

The `odds_papi/` folder contains useful exploratory scripts and captured JSON samples. They are not obsolete yet, but they should not become production architecture.

Current role:

```text
API exploration
Response sampling
Manual endpoint probing
```

Future role after OddsPapi integration:

```text
modules/oddspapi/client.py         -> production client
modules/oddspapi/event_resolver.py -> provider-to-canonical resolver
modules/odds_ingestion/adapters/oddspapi_market_adapter.py -> response normalization
```

Recommendation after production client exists:

1. Move useful JSON examples into `tests/fixtures/oddspapi/`.
2. Move manual scripts into `scripts/dev/oddspapi/` or delete them.
3. Do not let production code import from `odds_papi/`.

---

## 10. Proposed cleanup phases

### Phase A — Immediate safe fixes

- Fix stale `event_id` variable in `EventRepository.upsert_event()`.
- Fix `home_participant_data` indentation.
- Add/adjust tests for events with no `season_id`.

### Phase B — Legacy odds script cleanup

- Archive/delete `scripts/legacy/*event_odds*` scripts.
- Confirm no active import of `OddsRepository`, `EventOdds`, `Event.event_odds`.
- Keep `check_no_active_legacy_odds_writes.py` briefly as a guard.

### Phase C — Runtime startup cleanup

After stable deployment:

- Move `_migrate_events_to_canonical_identity()` out of automatic startup or guard it behind an env flag.
- Move `_drop_legacy_odds_tables()` out of automatic startup.
- Keep validation tooling.

### Phase D — Naming cleanup

- Rename SofaScore API-facing parameters from `event_id` to `sofascore_event_id`.
- Split functions that mix API calls and DB writes into explicit `canonical_event_id` and `sofascore_event_id` parameters.

### Phase E — Legacy text fallback cleanup

Only after normalized entity backfill is complete:

- Remove/reduce `LEGACY_EVENT_TEXT_FIELDS` comments.
- Remove `_build_event_data_with_legacy_fallback()` or restrict its use.
- Consider dropping text fields in a future DB migration if no reporting depends on them.

---

## 11. Cleanup acceptance criteria

A cleanup PR should be considered safe only if:

- Current SofaScore runtime flows still pass.
- No active job imports `OddsRepository`, `EventOdds`, or `Event.event_odds`.
- No active SofaScore API call receives canonical `events.id` directly.
- `event_source_mappings` validation passes.
- `scripts/maintenance/validate_event_identity_migration.py` or equivalent validation returns success.
- OddsPapi work remains blocked until P0 fixes are complete.

---

## 12. Notes

This audit intentionally does not recommend deleting all compatibility shims immediately. The priority is to remove code that is clearly incompatible with the current architecture while preserving rollback/debug tools until the canonical identity migration has been stable for at least one or two real runtime cycles.
