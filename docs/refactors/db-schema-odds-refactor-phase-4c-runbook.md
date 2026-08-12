# Fase 4c — Runbook de ejecución del backfill de quotes

**Estado:** completada (producción, 2026-08-12)
**Prerrequisito:** Fase 4b desplegada (script + tests verdes).
**Plan de herramienta:** [db-schema-odds-refactor-phase-4b.md](./db-schema-odds-refactor-phase-4b.md)
**Documento maestro:** [db-schema-odds-refactor.md](./db-schema-odds-refactor.md)
**Siguiente:** [Fase 5](./db-schema-odds-refactor-phase-5.md) (cutover de lectores).

**Cierre en producción:** campaña `algorithm_version=4b.7` con
`--until-empty` y todos los purges; artefacto final
`events_selected=0` / advisory lock liberado. Local y server: snapshots
clasificables con `quote_id` ligado. Antes de reanudar ingesta, conviene
repetir la verificación §5 (pase idempotente + query de duplicados).

Esta fase **ejecuta** el backfill en staging/producción. No cambia el código
del clasificador ni del writer.

Los artefactos del run (JSON, NDJSON, checkpoint) van bajo
`logs/debug/market_choice_quote_backfill/`. En Docker eso vive en el volumen
`./logs:/app/logs` de `compose.yaml`, así que sobreviven recreaciones del
contenedor `app`.

## 1. Guardas operativas

1. Pausar ingesta live (pre-start / odds jobs) durante el commit.
2. Tomar backup lógico de `market_choice_quotes` y `market_choice_snapshots`
   (al menos `quote_id`, PKs y odds) antes del primer `--commit`.
3. Confirmar preflight dry-run sin errores de schema.
4. No lanzar dos instancias concurrentes del script.

## 2. Dry-run focalizado (staging)

Artefactos compartidos (siempre los mismos archivos; el detalle por evento
va *dentro* del JSON/NDJSON vía `event_id` / `event_scope`):

- `logs/debug/market_choice_quote_backfill/checkpoint.json`
- `logs/debug/market_choice_quote_backfill/output.json`
- `logs/debug/market_choice_quote_backfill/rejections.ndjson`

En Docker eso vive en el volumen `./logs:/app/logs` de `compose.yaml`.

```bash
python -m scripts.maintenance.backfill_market_choice_quotes \
  --fresh-artifacts \
  --event-id 158955 \
  --batch-size 200 \
  --max-rows 5000
```

Revisar ambiguos; si hace falta, versionar un resolution file bajo
`config/backfills/market_choice_quotes/` y re-ejecutar dry-run con
`--resolution-file`.

## 3. Commit acotado (servidor ~1 GB)

```bash
python -m scripts.maintenance.backfill_market_choice_quotes \
  --commit \
  --confirm-ingestion-paused \
  --fresh-artifacts \
  --batch-size 200 \
  --max-events 10 \
  --max-rows 5000
```

## 4. Reanudación / campaña continua

Chunk único (resume manual):

```bash
python -m scripts.maintenance.backfill_market_choice_quotes \
  --commit \
  --confirm-ingestion-paused \
  --resume-from \
  --batch-size 200 \
  --max-events 10 \
  --max-rows 5000
```

Campaña en loop (recomendado en local/server). Una sola invocación procesa
chunks hasta `events_selected=0`. El presupuesto por chunk sigue siendo
`--max-events` / `--max-rows` (hard caps del CLI).

```bash
# Nueva campaña
python -m scripts.maintenance.backfill_market_choice_quotes \
  --commit \
  --confirm-ingestion-paused \
  --confirm-purge \
  --purge-oddspapi-null-mainline-lines \
  --until-empty \
  --fresh-artifacts \
  --max-events 200 \
  --max-rows 80000 \
  --batch-size 200

# Continuar campaña existente (reusa checkpoint.json si existe)
python -m scripts.maintenance.backfill_market_choice_quotes \
  --commit \
  --confirm-ingestion-paused \
  --confirm-purge \
  --purge-oddspapi-null-mainline-lines \
  --until-empty \
  --max-events 200 \
  --max-rows 80000 \
  --batch-size 200
```

`--resume-from` sin path usa el `checkpoint.json` compartido.
`rejections.ndjson` **siempre hace append** entre invocaciones; usa
`--fresh-artifacts` solo para empezar una campaña nueva (trunca rejections
y no auto-resume del checkpoint). Cada run escribe un `run_header` /
`run_boundary` (línea en blanco + JSON) para separar bloques.

`--resume-from` / cada chunk de `--until-empty` reinicia el presupuesto de
filas/eventos de la invocación y conserva el cursor + scope incompleto.

Si el checkpoint tiene `algorithm_version` distinta (p.ej. `4b.2` → `4b.3`
tras el auto-seed de mercados canónicos Back/Lay, `4b.3` → `4b.4` tras la
eliminación de sources por `MarketWritePolicy`, `4b.4` → `4b.5` al abandonar
rematerialización Back/Lay→oddsportal, `4b.5` → `4b.6` al dejar de atribuir
choice_states snapless de bookies OddsPortal-era y purgarlos con
`--purge-ambiguous-choice-states`, o `4b.6` → `4b.7` al borrar
automáticamente markets fuera de bookies `{1,3,4,302}`), hay que empezar
campaña nueva con
`--fresh-artifacts` (no se puede reanudar entre versiones). Cada run (≥4b.7)
también borra automáticamente markets cuyo ``bookie_id`` no está en
``{1, 3, 4, 302}`` (SofaScore / bet365 / Betfair / Pinnacle), con cascade de
choices/snapshots/quotes — sin flag CLI.

## 5. Verificación post-run

1. Repetir el mismo `--commit` scope: exigir cero inserts/updates de quotes y
   cero links nuevos.
2. Repetir dry-run: cero trabajo pendiente clasificable en el scope.
3. Consultas útiles:

```sql
-- Snapshots históricos aún sin quote en el scope
SELECT COUNT(*) AS unlinked
FROM market_choice_snapshots s
JOIN market_choices c ON c.choice_id = s.choice_id
JOIN markets m ON m.market_id = c.market_id
WHERE s.quote_id IS NULL
  AND m.event_id BETWEEN :min_event AND :max_event;

-- Duplicados NULL-safe de identidad de quote (debe ser 0)
SELECT choice_id, source, COALESCE(exchange_side, ''), exchange_level, COUNT(*)
FROM market_choice_quotes
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1;
```

4. Archivar JSON/NDJSON/checkpoint fuera de git.
5. Solo entonces reanudar ingesta y considerar cutover de lectores (Fase 5).

## 6. Rollback

No hay rollback destructivo automático. Si falla la verificación: pausar,
conservar artefactos, restaurar desde backup o preparar corrección con el
manifiesto del run. Nunca borrar quotes por rango sin validar dependencias live
posteriores al run.
