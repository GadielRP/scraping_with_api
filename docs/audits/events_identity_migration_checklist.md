# Events Identity Migration Checklist

> **Commit base**: `c14d561`  
> **Date**: 2026-06-19  
> **Pre-requisite**: Aprobación del [events_id_dependency_audit.md](file:///c:/Users/gadie/Documents/projects/sofascore/docs/audits/events_id_dependency_audit.md)

---

## Fase 0: Preparación (Sin Cambio de Comportamiento)

- [ ] Backup completo de la base de datos producción
- [ ] Crear branch de migración: `feature/canonical-event-identity`
- [ ] Definir modelo SQLAlchemy `EventSourceMapping` en `models.py`
  - [ ] Tabla `event_source_mappings` con columnas: `mapping_id`, `event_id`, `source`, `source_event_id`, `source_sport_id`, `source_tournament_id`, `source_season_id`, `match_method`, `confidence`, `raw_external_providers`, `created_at`, `updated_at`
  - [ ] Unique constraint: `(source, source_event_id)`
  - [ ] Indexes: `event_id`, `(source, source_event_id)`, `source`
  - [ ] Relationship con `Event`: `event = relationship("Event", back_populates="source_mappings")`
  - [ ] Agregar `source_mappings` relationship en `Event`
- [ ] Crear `EventSourceMappingRepository` en `infrastructure/persistence/repositories/event_source_mapping_repository.py`
  - [ ] `get_event_id_by_source(source, source_event_id) -> Optional[int]`
  - [ ] `get_source_event_id(event_id, source) -> Optional[str]`
  - [ ] `resolve_required_source_event_id(event_id, source) -> str` (raises ValueError)
  - [ ] `upsert_mapping(...) -> EventSourceMapping`
  - [ ] `get_mappings_for_event(event_id) -> list[EventSourceMapping]`
- [ ] Exportar repositorio en `__init__.py` de repositories
- [ ] Crear tests unitarios para `EventSourceMappingRepository`
- [ ] Crear Alembic migration: crear tabla `event_source_mappings` (sin FK constraint aún)
- [ ] Aplicar migration en staging
- [ ] Poblar `event_source_mappings` con datos existentes: `INSERT ... SELECT id, 'sofascore', id::text, 'legacy_primary_key_migration', 1.000 FROM events`
- [ ] Validar count: `SELECT COUNT(*) FROM event_source_mappings` = `SELECT COUNT(*) FROM events`

---

## Fase 1: Dual-Write (Compatibilidad Total)

**Objetivo**: Todas las nuevas escrituras crean mappings SIN cambiar `events.id`.

- [ ] Modificar `EventRepository.upsert_event()`:
  - [ ] Después del insert/update, crear `EventSourceMapping(event_id=event.id, source="sofascore", source_event_id=str(event_payload["id"]))` si no existe
  - [ ] Verificar con test: cada nuevo evento tiene un mapping
- [ ] Modificar `daily_discovery/persistence.py`:
  - [ ] Después de `EventRepository.upsert_event()`, verificar que mapping existe
- [ ] Modificar `sport_seasons_processing.py`:
  - [ ] Después de upsert, verificar que mapping existe
- [ ] Deploy a staging
- [ ] Validar que:
  - [ ] Todos los nuevos eventos tienen mappings
  - [ ] Comportamiento funcional idéntico al actual
  - [ ] No hay impacto de performance

---

## Fase 2: Introducir Lookup por Mapping (Sin Romper Nada)

**Objetivo**: Las funciones que llaman SofaScore API resuelven `sofascore_event_id` desde el mapping, pero como fallback usan `event.id` (que todavía es el SofaScore ID).

- [ ] Crear helper `resolve_sofascore_event_id(canonical_event_id: int) -> int`:
  - [ ] Try: `EventSourceMappingRepository.get_source_event_id(event_id, "sofascore")`
  - [ ] Fallback: `canonical_event_id` (en esta fase, son iguales)
  - [ ] Log warning si usa fallback
- [ ] Aplicar helper en:
  - [ ] `run_results_collection_job.py` — `api_client.get_event_results(event.id)` → `api_client.get_event_results(resolve_sofascore_event_id(event.id))`
  - [ ] `run_pre_start_check_job.py` — `api_client.get_event_final_odds(event_data["id"])` → con resolve
  - [ ] `intraday_result_freshness.py` — `api_client._request_json(f"/event/{event_id}")` → con resolve
  - [ ] `timing.py` — `api_client.get_event_results(event_id)` → con resolve
  - [ ] `timestamp_corrections.py` — `api_client.get_event_results(event_id)` → con resolve
  - [ ] `rescheduled_events.py` — `api_client.get_event_final_odds(event_id)` → con resolve
  - [ ] `event_details.py` — `fetch_event_response(client, event_id)` → con resolve
  - [ ] `daily_discovery/persistence.py` — `MarketOddsIngestionService.save_from_event_odds_response(event_id, ...)` → usar `db_event.id`
  - [ ] `maintenance/backfill_event_entities_from_sofascore.py` → con resolve
- [ ] Deploy a staging
- [ ] Validar que:
  - [ ] Warnings de fallback aparecen pero disminuyen con el tiempo
  - [ ] No hay impacto funcional
  - [ ] Logs confirman que resolve funciona

---

## Fase 3: Migración de IDs (Cambio de Schema)

**Objetivo**: Convertir `events.id` de SofaScore external ID a canonical autoincremental.

- [ ] Ventana de mantenimiento programada (~30 minutos)
- [ ] Detener todos los jobs (scheduler, workers)
- [ ] Backup final pre-migración
- [ ] Ejecutar migración SQL (ver plan en audit doc sección 12):
  - [ ] Crear secuencia
  - [ ] Crear tabla temporal de traducción old→new
  - [ ] Actualizar `results.event_id`
  - [ ] Actualizar `markets.event_id`
  - [ ] Actualizar `event_observations.event_id`
  - [ ] Actualizar `prediction_logs.event_id`
  - [ ] Actualizar `event_source_mappings.event_id`
  - [ ] Actualizar `events.id`
  - [ ] Configurar secuencia como default
  - [ ] Agregar FK constraint en `event_source_mappings`
- [ ] Ejecutar validaciones (sección 12 del audit doc):
  - [ ] No orphan results
  - [ ] No orphan markets
  - [ ] No orphan observations
  - [ ] No orphan prediction_logs
  - [ ] Every event has SofaScore mapping
  - [ ] No duplicate mappings
  - [ ] Views return data
- [ ] Recrear views y materialized views
- [ ] Reiniciar jobs
- [ ] Monitorear logs por 24h

---

## Fase 4: Modificar `upsert_event()` para Usar Canonical IDs

**Objetivo**: `upsert_event()` ya no escribe `event_payload["id"]` como PK.

- [ ] Modificar `EventRepository.upsert_event()`:
  - [ ] Extract `sofascore_event_id = str(event_payload.get("id"))`
  - [ ] Lookup `canonical_event_id = EventSourceMappingRepository.get_event_id_by_source("sofascore", sofascore_event_id)`
  - [ ] Si existe: load y update `Event` by `canonical_event_id`
  - [ ] Si no existe: create `Event()` sin `id=`, flush para autoincrement, create mapping
  - [ ] Return `Event` con canonical `id`
- [ ] Actualizar callers que usan el resultado de `upsert_event()`:
  - [ ] `daily_discovery/persistence.py`: usar `db_event.id` (canonical) para `MarketOddsIngestionService`
  - [ ] `sport_seasons_processing.py`: usar `db_event.id` (canonical)
  - [ ] `event_details.py`: usar `db_event.id` (canonical)
- [ ] Remover fallback de `resolve_sofascore_event_id()`
  - [ ] Convertir a `resolve_required_source_event_id()` que raisa error si no hay mapping
- [ ] Deploy a staging
- [ ] Validar:
  - [ ] Nuevos eventos reciben canonical IDs diferentes de SofaScore IDs
  - [ ] Mappings se crean automáticamente
  - [ ] API calls siguen funcionando con resolved SofaScore IDs
  - [ ] FK integrity en todas las tablas

---

## Fase 5: Cleanup y Documentación

- [ ] Eliminar `LEGACY_DB_SHIM_REMOVE_AFTER_SCHEMA_MIGRATION` de `event_repository.py`
- [ ] Actualizar CLI scripts para clarificar tipo de ID:
  - [ ] `get_event_odds_by_event_id.py`: agregar `--source` flag
  - [ ] `get_event_results.py`: agregar `--source` flag
  - [ ] `get_event_details.py`: agregar `--source` flag
- [ ] Actualizar docstrings de métodos con tipo de ID esperado
- [ ] Aplicar naming convention: `event_id` = canonical, `sofascore_event_id` = externo
- [ ] Eliminar columna temporal `old_sofascore_id` (si se usó)
- [ ] Actualizar README con nueva arquitectura de identidad
- [ ] Crear script de verificación post-migración reutilizable

---

## Criterios de Éxito (Checklist Final)

- [ ] `events.id` es autoincremental y NO contiene IDs de SofaScore
- [ ] Toda FK (`results.event_id`, `markets.event_id`, `event_observations.event_id`, `prediction_logs.event_id`) apunta a canonical IDs
- [ ] Toda llamada a SofaScore API resuelve `sofascore_event_id` desde `event_source_mappings`
- [ ] Cada evento en `events` tiene al menos un registro en `event_source_mappings` con `source="sofascore"`
- [ ] No hay orphaned rows en ninguna tabla dependiente
- [ ] Views y materialized views funcionan correctamente
- [ ] Jobs (daily_discovery, pre_start_check, results_collection, intraday_freshness) funcionan correctamente
- [ ] CLI scripts aceptan el tipo correcto de ID
- [ ] No hay degradación de performance
- [ ] La arquitectura soporta agregar un nuevo proveedor (e.g., OddsPapi) sin cambios en schema
