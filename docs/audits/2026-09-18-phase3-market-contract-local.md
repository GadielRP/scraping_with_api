# Fase 3 — contracción del schema de markets (local)

Fecha: 2026-09-18  
Base: `127.0.0.1:5435/sofascore_odds` (contenedor `sofascore-pg`)  
Revisión Alembic: `20260918_04`

## Objetivo

Convertir la identidad de un mercado en la combinación:

```text
(event_id, bookie_id, market_type_id, line_value, is_live)
```

`market_type_id` es la única identidad canónica; `line_value` es un número
decimal nullable (NULL es válido para moneyline y otros mercados sin línea).

## Cambios aplicados

- `markets`: eliminadas `market_name`, `market_group`, `market_period` y
  `choice_group`; eliminadas sus restricciones/índices de identidad textual.
- `market_source_mappings`: eliminadas las copias
  `canonical_market_name/group/period`; se conserva `canonical_market_key`
  como clave de negocio y `market_type_id` como FK numérica.
- `canonical_market_types.requires_choice_group` renombrada a
  `requires_line_value`.
- `pillar_mining_units`: reemplazadas las cuatro columnas textuales duplicadas
  por `market_type_id` y `line_value`, con FK al catálogo.
- Lecturas/escrituras y vistas (`v_dual_process_event_odds`) usan el catálogo
  canónico. Las vistas de reporting conservan aliases de salida
  (`market_name`, `market_group`, `market_period`) para no romper consumidores,
  pero ya no leen columnas duplicadas de `markets`.
- Retirado el migrador DDL automático y el método de compatibilidad de
  `DatabaseManager`; el único camino de cambio de schema es Alembic y la
  aplicación sólo usa `verify_schema_at_head()`.
- Eliminado el script de backfill histórico, porque su trabajo terminó en la
  Fase 2 y sus consultas dependían de columnas eliminadas.

## Verificaciones locales

- `alembic upgrade head`: completó `20260918_03 -> 20260918_04`.
- `alembic_version = 20260918_04`.
- `markets.market_type_id IS NULL`: `0` filas.
- Las vistas `v_dual_process_event_odds` y `event_all_odds` fueron recreadas y
  consultan correctamente.
- `initialize_system()` devolvió `True` con `REQUIRE_ALEMBIC_SCHEMA=true`.
- No quedan columnas legacy en las tablas `markets`,
  `market_source_mappings` ni `pillar_mining_units`.
- Pruebas del contrato canónico: `48 passed` (una prueba de fixture externo
  se dejó fuera porque el archivo `61507_odds_response.json` no está presente
  en el checkout local).
- Pruebas de lectores, escritores, vistas y quotes: `42 passed`.
- Pruebas de trayectoria y pilares después del renombrado interno a
  `line_value`: `116 passed` y persistencia canónica/exchange: `26 passed`.

## Operación posterior

La revisión es una contracción intencional y su `downgrade` falla de forma
explícita: restaurar nombres vacíos no puede recuperar los datos eliminados.
En despliegues se debe ejecutar, antes de levantar la aplicación:

```bash
alembic upgrade head
docker compose up -d app
```
