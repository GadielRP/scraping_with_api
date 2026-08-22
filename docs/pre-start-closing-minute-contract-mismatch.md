# Contrato de momentos pre-start entre P4 y P5

## Estado

Resuelto: `0` es ahora un momento configurable independiente. Puede coexistir
con `PRE_START_CLOSING_ODDS_MINUTE=1` y con cualquier momento negativo.

## Problema

- La configuración define `PRE_START_CLOSING_ODDS_MINUTE`, cuyo valor actual es
  `1`. El momento `0` ya no se sustituye por el minuto de cierre.
- P4 obtiene el cierre desde esa configuración y, por tanto, espera el punto
  T-1.
- P5 mantiene `CURRENT_TARGET_MINUTE = 0` y busca el precio actual únicamente en
  T0.

Cuando `0` está incluido en `PRE_START_ODDS_MOMENTS`, el scheduler captura T0
como momento independiente. Si también está configurado T-1, ambos momentos
quedan disponibles para el pipeline.

La selección de endpoint de Oddspapi sigue la semántica del signo:

- momentos positivos: `/odds`, `is_live=false`;
- momento `0` y momentos negativos: `/historical-odds`, `is_live=true`.

## Archivos relacionados

- `infrastructure/settings/config.py`
- `modules/jobs/pre_start_check_job/moment_policy.py`
- `modules/pillars/pillar_4/drift_engine/drift_engine.py`
- `modules/pillars/pillar_5/exact_price_memory_engine/exact_price_memory_engine.py`
