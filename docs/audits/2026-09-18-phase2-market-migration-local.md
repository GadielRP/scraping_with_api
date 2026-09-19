# Fase 2 — migración local de identidad de markets

Fecha: 2026-09-18  
Entorno: PostgreSQL local `127.0.0.1:5435/sofascore_odds`  
Servidor remoto: no utilizado ni modificado.

## Objetivo

Validar y completar localmente la migración de identidad canónica de markets:

- `canonical_market_types.market_type_id`;
- `market_source_mappings.market_type_id`;
- `markets.market_type_id`;
- `markets.line_value` como línea numérica nullable;
- índices únicos parciales y claves foráneas.

## Procedimiento ejecutado

1. Inspección read-only del esquema y conteos históricos.
2. `alembic stamp 20260918_00`, porque la base preexistente no tenía fila de
   control Alembic y fue verificada como el esquema baseline esperado.
3. `alembic upgrade head`, aplicando `20260918_01`, `20260918_02` y
   `20260918_03`.
4. Auditoría de backfill:

   ```powershell
   python -m scripts.maintenance.backfill_market_type_ids --sample-limit 10
   ```

5. Aplicación del backfill determinista:

   ```powershell
   python -m scripts.maintenance.backfill_market_type_ids --apply --sample-limit 10
   ```

## Resultado de cobertura

| Métrica | Resultado |
|---|---:|
| canonical types | 39 |
| mappings sin `market_type_id` | 0 |
| markets totales | 771,461 |
| markets sin `market_type_id` | 0 |
| exact candidates | 771,461 |
| exact ambiguities | 0 |
| exact identity conflicts | 0 |
| lineage candidates | 36,876 |
| lineage ambiguities | 0 |
| duplicate canonical identities | 0 |
| filas actualizadas por el backfill | 0 |

La base ya había recibido los IDs durante la etapa de compatibilidad de
arranque. El backfill fue ejecutado igualmente y confirmó que no quedaban
filas pendientes; es idempotente.

## Hallazgo de lineage

Se detectaron 456 discrepancias entre el mapping histórico del proveedor y el
tipo canónico de la fila. No se sobrescribieron porque:

- cada fila tiene una coincidencia exacta y única por nombre/grupo/periodo;
- `exact_identity_conflicts = 0`;
- el `market_type_id` actual coincide con esa coincidencia exacta.

Se clasifican como `lineage_conflicts_overridden_by_exact = 456` y
`lineage_conflicts_requiring_review = 0`. Representan drift histórico del
catálogo del proveedor, no una ambigüedad de la identidad persistida.

## Validación de `line_value`

| Métrica | Resultado |
|---|---:|
| `line_value IS NULL` | 425,641 |
| `line_value IS NOT NULL` | 345,820 |
| legacy no vacío sin `line_value` | 0 |
| discrepancias numéricas con `choice_group` | 0 |

Los valores `NULL` corresponden a mercados sin línea o a la representación
legacy vacía. Los valores presentes coinciden numéricamente con el espejo
textual.

## Integridad del esquema

- Alembic quedó en `20260918_03 (head)`.
- `markets.market_type_id` quedó `NOT NULL` en PostgreSQL.
- Existen los índices `uq_markets_canonical_without_line` y
  `uq_markets_canonical_with_line` basados en `line_value`.
- Existe `fk_markets_market_type` en `markets`.
- Existe `fk_market_source_mappings_market_type` en
  `market_source_mappings`.

## Conteos antes/después

Los conteos no cambiaron durante la migración:

| Tabla | Antes | Después |
|---|---:|---:|
| markets | 771,461 | 771,461 |
| market_choices | 1,754,459 | 1,754,459 |
| market_choice_quotes | 1,757,349 | 1,757,349 |
| market_choice_snapshots | 3,852,981 | 3,852,981 |
| canonical_market_types | 39 | 39 |
| market_source_mappings | 2,778 | 2,778 |

## Conclusión

La base local está lista para la contracción desde el punto de vista de
cobertura de `market_type_id` y `line_value`. La eliminación de columnas y
fallbacks legacy aún no se ejecuta. El arranque ya fue cambiado para no
ejecutar DDL ni backfills: en PostgreSQL exige que `alembic_version` esté en
`20260918_03`; la migración se ejecuta fuera del proceso de la aplicación.
La llamada local a `initialize_system()` devolvió `True` contra esta base.
