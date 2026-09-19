# 🔍 Auditoría de Calidad: `database.py`

> Nota de vigencia: este documento es una auditoría histórica previa a la
> adopción de Alembic. La Fase 3 retiró el orquestador automático de migraciones
> del runtime; para el estado actual consulta
> `docs/audits/2026-09-18-phase3-market-contract-local.md`.

**Archivo:** [database.py](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py)
**Tamaño:** 1,700 líneas · 84 KB · 1 clase (`DatabaseManager`) · 27 métodos · 1 instancia global

---

## Resumen Ejecutivo

`database.py` tiene un problema arquitectónico serio: **es un "God Object"** que mezcla dos responsabilidades fundamentalmente distintas:

1. **Gestión de conexión/sesión** (~75 líneas) — legítima responsabilidad de un `DatabaseManager`
2. **Migraciones de esquema** (~1,600 líneas) — debería vivir en un módulo separado

El 94% del archivo son migraciones empaquetadas como métodos privados de una clase que debería ser pequeña. Esto es lo que hace que el archivo sea difícil de leer y mantener.

---

## Inventario Método por Método

### 🟢 Métodos Esenciales (Sí Conservar)

| # | Método | Líneas | Usos Externos | Veredicto |
|---|--------|--------|---------------|-----------|
| 1 | [`__init__`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L14-L18) | 14-18 | Instanciación global + tests | ✅ Correcto |
| 2 | [`_setup_engine`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L20-L37) | 20-37 | Solo desde `__init__` | ✅ Bien encapsulado |
| 3 | [`get_session`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L57-L73) | 57-73 | **~80+ usos** en todo el codebase | ✅ Core indispensable |
| 4 | [`test_connection`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L75-L84) | 75-84 | ~10 usos (health checks, scripts, init) | ✅ Útil |
| 5 | [`create_tables`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L39-L46) | 39-46 | 4 usos (init, tests, scripts) | ✅ Útil |

### 🟡 Métodos Cuestionables

| # | Método | Líneas | Usos Externos | Problema |
|---|--------|--------|---------------|----------|
| 6 | [`drop_tables`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L48-L55) | 48-55 | **0 usos en todo el codebase** | ⚠️ **Código muerto.** Nunca se llama fuera de su propia definición. Candidato a eliminación. |
| 7 | [`check_and_migrate_schema`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L86-L220) | 86-220 | 5 usos (init, scripts) | ⚠️ Es el "orquestador" de migraciones. El método en sí está bien, pero debería vivir en otro módulo. 135 líneas de lógica genérica + 13 llamadas a `_migrate_*`. |

### 🔴 Métodos que Deberían Extraerse (Migraciones)

Todos estos métodos son **internos** (`_migrate_*`), llamados **exclusivamente** desde `check_and_migrate_schema()`. Ninguno tiene usos fuera de `database.py` (excepto en tests de migración):

| # | Método | Líneas | Llamadores | Problema Arquitectónico |
|---|--------|--------|------------|------------------------|
| 8 | [`_create_table_and_indexes`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L222-L227) | 222-227 | 4 llamadas internas | Helper válido, pero pertenece al módulo de migraciones |
| 9 | [`_migrate_canonical_market_types`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L229-L246) | 229-246 | 1 (check_and_migrate) | Migración one-time empotrada en la clase |
| 10 | [`_migrate_market_source_mappings`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L248-L266) | 248-266 | 1 | Migración one-time empotrada |
| 11 | [`_migrate_market_outcome_source_mappings`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L268-L282) | 268-282 | 1 | Migración one-time empotrada |
| 12 | [`_migrate_source_catalog_syncs`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L284-L296) | 284-296 | 1 | Migración one-time empotrada |
| 13 | [`_migrate_markets_to_bookies`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L298-L440) | 298-440 | 1 | **143 líneas** de SQL hardcodeado para una migración legacy |
| 14 | [`_migrate_bookie_source_mappings`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L442-L533) | 442-533 | 1 | **92 líneas** de seeding + migración combinados |
| 15 | [`_migrate_market_period_not_null`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L535-L574) | 535-574 | 1 | Migración one-time |
| 16 | [`_migrate_market_choice_snapshot_lineage`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L576-L620) | 576-620 | 1 | Migración one-time |
| 17 | [`_reorder_markets_columns`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L622-L745) | 622-745 | 1 | **124 líneas** para reordenar columnas cosméticamente. Operación destructiva y riesgosa. Posiblemente ya completada y potencialmente eliminable. |
| 18 | [`_migrate_market_period_identity`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L747-L791) | 747-791 | 1 | Migración one-time |
| 19 | [`_deduplicate_markets_for_period_identity`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L793-L867) | 793-867 | 1 (desde `_migrate_market_period_identity`) | **75 líneas de PL/pgSQL** embebido en Python. Solo helper de #18 |
| 20 | [`_migrate_events_to_participants_competitions`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L869-L960) | 869-960 | 1 | Migración one-time |
| 21 | [`_migrate_daily_discovery_log_run_slots`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L962-L1071) | 962-1071 | 1 | **110 líneas** para una migración de run_slots |
| 22 | [`_migrate_events_to_canonical_identity`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L1421-L1646) | 1421-1646 | 1 (+ tests) | **226 líneas** — la migración más grande. Tiene 8 sub-helpers. |

### 🔴 Sub-helpers de `_migrate_events_to_canonical_identity`

Estos **solo existen para servir a un único método padre**. Forman una cadena de llamadas interna:

| # | Método | Líneas | Llamadores |
|---|--------|--------|------------|
| 23 | [`_event_identity_migration_already_applied`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L1073-L1106) | 1073-1106 | 1 interno |
| 24 | [`_ensure_event_migration_status_table`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L1108-L1119) | 1108-1119 | 1 interno |
| 25 | [`_mark_event_identity_migration_completed`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L1121-L1133) | 1121-1133 | 2 internos |
| 26 | [`_ensure_event_source_mappings_table_ready`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L1135-L1171) | 1135-1171 | 1 interno |
| 27 | [`_cleanup_orphan_event_source_mappings`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L1173-L1197) | 1173-1197 | 1 interno |
| 28 | [`_drop_event_identity_foreign_keys`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L1199-L1235) | 1199-1235 | 1 interno |
| 29 | [`_restore_event_identity_foreign_keys`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L1237-L1292) | 1237-1292 | 2 internos |
| 30 | [`_ensure_events_id_default`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L1294-L1317) | 1294-1317 | 3 internos |
| 31 | [`_validate_event_identity_migration`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L1319-L1419) | 1319-1419 | 2 internos |

### Utilidades

| # | Método | Líneas | Llamadores |
|---|--------|--------|------------|
| 32 | [`_get_column_type_sql`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L1660-L1694) | 1660-1694 | 1 (check_and_migrate_schema) |
| 33 | [`_drop_legacy_odds_tables`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py#L1648-L1658) | 1648-1658 | 1 (check_and_migrate_schema) |

---

## Diagnóstico Arquitectónico

### Problema 1: God Object — Violación masiva de SRP

```
DatabaseManager (1,700 líneas)
├── Gestión de conexión     (~75 líneas)  ← ✅ responsabilidad legítima
└── 13 migraciones          (~1,600 líneas) ← ❌ NO pertenece aquí
    └── 9 sub-helpers de una sola migración
```

El `DatabaseManager` debería ser ~100 líneas. Las migraciones son lógica de dominio transitoria que no tiene nada que ver con la gestión de sesiones.

### Problema 2: Migraciones que se ejecutan en CADA arranque

Todas las `_migrate_*` se ejecutan cada vez que el sistema arranca (vía `initialize_system()` → `check_and_migrate_schema()`). Cada una tiene guards de idempotencia, pero:
- **Costo innecesario**: Inspecciones de esquema repetidas en cada arranque
- **Riesgo**: Una migración buggy puede corromper datos en cada reinicio
- **Complejidad cognitiva**: Leer el archivo requiere entender 13 migraciones históricas solo para encontrar `get_session()`

### Problema 3: Código muerto confirmado

| Método | Evidencia |
|--------|-----------|
| `drop_tables` | 0 llamadas en todo el codebase |

### Problema 4: Migraciones potencialmente obsoletas

Estas migraciones one-time probablemente ya se ejecutaron exitosamente hace tiempo y siguen corriendo (con guards) en cada arranque:

| Migración | ¿Probablemente completada? |
|-----------|---------------------------|
| `_migrate_markets_to_bookies` | Sí — transición sofascore_market_id ya no existe |
| `_reorder_markets_columns` | Sí — reordenamiento cosmético de columnas |
| `_migrate_market_period_not_null` | Sí — backfill de NULLs |
| `_drop_legacy_odds_tables` | Sí — tablas legacy ya eliminadas |
| `_migrate_events_to_canonical_identity` | Sí — tiene su propio marker en `event_migration_status` |

### Problema 5: Acoplamiento fuerte con repositorios

Métodos de migración importan repositorios directamente:
- `_migrate_canonical_market_types` → `MarketMappingRepository`
- `_migrate_bookie_source_mappings` → `BookieRepository`

Esto crea dependencias circulares potenciales: `database.py` → `repositories` → `database.py` (vía `db_manager`)

### Problema 6: SQL crudo masivo

~500 líneas de SQL raw hardcodeado en strings Python, incluyendo un bloque PL/pgSQL de 70 líneas (la función `_deduplicate_markets_for_period_identity`). Esto es extremadamente difícil de:
- Leer y revisar
- Debuggear
- Testear unitariamente
- Mantener cuando el esquema cambia

---

## Métricas de Calidad

| Métrica | Valor | Evaluación |
|---------|-------|------------|
| **Líneas totales** | 1,700 | ❌ Excesivo para un gestor de DB |
| **Métodos públicos útiles** | 5 de 6 | ✅ Buena interfaz pública |
| **Código muerto** | `drop_tables` (8 líneas) | ⚠️ Menor |
| **Métodos "solo 1 llamador"** | 25 de 33 (~76%) | ❌ Señal de sobre-fragmentación |
| **Responsabilidades** | 2 (conexión + migraciones) | ❌ Violación SRP |
| **Profundidad de anidamiento** | Hasta 4 niveles (check → migrate → helper → sub-helper) | ❌ Difícil de seguir |
| **Imports internos** | 2 repos importados desde migraciones | ⚠️ Acoplamiento circular |
| **Legibilidad** | Baja para el archivo en conjunto, aceptable por método individual | ⚠️ |
| **Cohesión** | Muy baja — métodos de conexión no tienen relación con migraciones | ❌ |

---

## Recomendación de Refactorización

### Fase 1: Separar responsabilidades (Alto impacto, bajo riesgo)

```
infrastructure/persistence/
├── database.py              ← Solo conexión/sesión (~100 líneas)
├── models.py                ← (sin cambios)
├── migrations/
│   ├── __init__.py
│   ├── runner.py            ← check_and_migrate_schema() + generic column sync
│   ├── market_migrations.py ← _migrate_markets_to_bookies, _migrate_market_*
│   ├── event_migrations.py  ← _migrate_events_to_canonical_identity + helpers
│   └── utils.py             ← _get_column_type_sql, _create_table_and_indexes
└── repositories/
```

### Fase 2: Eliminar código muerto

- Borrar `drop_tables()`

### Fase 3: Graduar migraciones completadas

Después de confirmar que las migraciones ya corrieron en producción:
- Mover migraciones históricas a un archivo `migrations/completed/` como documentación
- Removerlas del flujo de arranque
- Esto podría eliminar ~1,200 líneas del flujo activo

---

## ¿Hay funciones wrapper inútiles?

Tu intuición es correcta, pero no en el sentido clásico de "wrappers que no hacen nada". Lo que hay es:

1. **Sub-helpers excesivos**: 9 métodos (`_ensure_*`, `_validate_*`, `_drop_*`, `_restore_*`, `_cleanup_*`, `_mark_*`) que solo existen para descomponer `_migrate_events_to_canonical_identity`. La descomposición es correcta como técnica, pero **el problema es que todo esto está en la clase equivocada**.

2. **`_create_table_and_indexes`**: Es un helper legítimo reutilizado 4 veces. No es un wrapper inútil.

3. **`_get_column_type_sql`**: Tiene 1 solo llamador. Podría ser una función local dentro de `check_and_migrate_schema`, pero como helper está bien.

**El problema real no es que haya wrappers inútiles, sino que hay ~1,600 líneas de lógica de migración empotradas en una clase de gestión de conexión.** Eso es lo que hace que sea difícil de leer.
