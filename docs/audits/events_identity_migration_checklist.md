# Events Identity Migration Checklist

> Status: implemented in codebase  
> Date: 2026-06-19  
> Reference audit: [events_id_dependency_audit.md](C:/Users/gadie/Documents/projects/sofascore/docs/audits/events_id_dependency_audit.md)

---

## Implemented

- [x] Add `EventSourceMapping` to `models.py` to store:
  - `mapping_id`
  - `event_id`
  - `source`
  - `source_event_id`
  - `source_sport_id`
  - `source_tournament_id`
  - `source_season_id`
  - `match_method`
  - `confidence`
  - `raw_external_providers`
  - `created_at`
  - `updated_at`
- [x] Add `Event.source_mappings` relationship with cascade delete behavior
- [x] Add `EventSourceMapping.event` relationship back to `Event`
- [x] Add unique constraint on `(source, source_event_id)`
- [x] Add indexes for `event_id`, `(source, source_event_id)`, and `source`
- [x] Make `events.id` an autoincrement canonical internal identifier in the ORM model
- [x] Add `EventSourceMappingRepository` with:
  - `get_event_id_by_source(source, source_event_id)`
  - `get_source_event_id(event_id, source)`
  - `resolve_required_source_event_id(event_id, source)`
  - `upsert_mapping(...)`
  - `get_mappings_for_event(event_id)`
- [x] Export the mapping repository from the repositories package
- [x] Update `EventRepository.upsert_event()` to resolve SofaScore IDs through the mapping layer before reusing or creating events
- [x] Create mappings automatically for newly discovered events
- [x] Add `modules/sofascore/event_identity.py` to resolve canonical event IDs back to SofaScore event IDs
- [x] Update runtime jobs and maintenance scripts to resolve canonical IDs before calling SofaScore APIs
- [x] Add durable migration state tracking with `event_migration_status`
- [x] Add a batched canonicalization migration that processes events in chunks of 1000
- [x] Backfill SofaScore mappings during migration
- [x] Renumber events into a dense internal `1..N` canonical ID range
- [x] Rewrite dependent references in `results`, `markets`, `event_observations`, `prediction_logs`, and `event_source_mappings`
- [x] Restore the PostgreSQL `events.id` sequence default after migration
- [x] Validate migration state after completion
- [x] Add `scripts/maintenance/validate_event_identity_migration.py`
- [x] Keep startup safe to call repeatedly by making the migration idempotent once completion is recorded

## Migration Flow

- [x] Read the current event IDs from `events`
- [x] Build an old-to-new translation where canonical IDs start at `1`
- [x] Process translation rows in batches of 1000
- [x] Log each batch before and after processing
- [x] Seed or update `event_source_mappings` for SofaScore rows
- [x] Update foreign keys and event mappings to the new canonical IDs
- [x] Restore constraints, defaults, and validation state at the end of the migration
- [x] Record completion so later startups can skip reprocessing

## Validation Criteria

- [x] `events.id` is the canonical internal event identifier
- [x] SofaScore IDs live in `event_source_mappings.source_event_id`
- [x] Every event is expected to have a SofaScore mapping after a successful migration
- [x] No orphan rows remain in dependent tables
- [x] No orphan `event_source_mappings` rows remain
- [x] No duplicate `(source, source_event_id)` mappings exist
- [x] PostgreSQL keeps a sequence-backed default for `events.id`
- [x] The validation script reports sample mappings and fails on critical inconsistencies

## Notes

- Deterministic mappings use `confidence=1.000`
- Legacy backfill mappings use `match_method=legacy_primary_key_migration`
- The migration is safe to run at startup, but after a successful run it should become a no-op except for validation and state refresh
- `events.id` now starts at `1` after canonical renumbering, so legacy placeholder rows no longer reserve the first IDs
