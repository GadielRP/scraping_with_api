# Snapshots de cambios significativos de OddsPapi

`ENABLE_ODDSPAPI_SIGNIFICANT_CHANGE_SNAPSHOTS` habilita el detector en la adquisición histórica ordinaria. La persistencia de momentos históricos en pre-start se controla con `ENABLE_ODDSPAPI_HISTORICAL_AS_OF_PERSIST`. T−5 fuerza el detector en el modo híbrido cuando el histórico de pre-start está habilitado. La recuperación diaria fuerza la detección y la reconstrucción de momentos. Las rutas y los flags están en la [guía de ingesta](pre_start_odds_ingestion.md).

## Selección por serie

El lector ordena los ticks por `createdAt` y, cuando interviene el detector, sanitiza cada serie de bookmaker, mercado, outcome y jugador. Excluye precios no finitos, precios de hasta `1.01` y ticks posteriores al kickoff. La política de actividad decide si admite observaciones inactivas. La recuperación limita además la serie a los ticks con timestamp en o antes de T−5.

Los parámetros versionados se definen en [la política del proveedor](../../modules/jobs/pre_start_check_job/providers/oddspapi/settings.py).

| Parámetro | Valor | Aplicación |
|---|---|---|
| `initial_odds_min_span_minutes` | 60 min | Antigüedad mínima del primer tick elegible al kickoff para aceptar la apertura. |
| `significant_change_min_history_hours` | 20 h | Antigüedad mínima del primer tick válido al kickoff para evaluar cambios significativos. |
| `significant_change_min_magnitude_pct` | 10 % | Cambio relativo mínimo frente al precio del ancla. |
| `significant_change_flash_reversal_minutes` | 3 min | Ventana para descartar una reversión por debajo del umbral. |
| `significant_change_min_price` | 1.01 | Límite inferior excluido en la sanitización. |

El primer tick válido sirve como ancla. Cada cambio confirmado mueve el ancla. Un tick sigue vigente hasta el siguiente válido, así que la confirmación admite períodos sin nuevas observaciones. Dentro del tramo final de tres minutos del corte de observación, el detector evalúa solo el último tick y puede emitirlo sin esperar confirmación.

## Momentos fijos y persistencia

Los objetivos fijos salen de `PRE_START_ODDS_MOMENTS`. Cada uno toma el último tick elegible en o antes de su objetivo. Los checkpoints se conservan aunque la serie sea demasiado corta para el detector, el cambio sea pequeño o haya una reversión breve. El recovery solicita los objetivos **estrictamente anteriores a T−5** y obtiene la observación de T−5 de la reconciliación entre la captura original y el histórico. Con los perfiles actuales, esto deja T−360, T−120 y T−30 en local, y T−120 y T−30 en producción, siempre que exista un tick válido para el objetivo.

Un mismo tick puede representar varios objetivos. La persistencia histórica lo deduplica por identidad de cuota, timestamp completo del proveedor, precio, límite y tamaño del exchange. `collected_at` es la hora real de escritura y `source_collected_at` es la hora de la observación según el proveedor. Los campos `initial_odds` y `current_odds` se seleccionan por su propia regla temporal. Consulta [el modelo de persistencia](../architecture/odds_db_ingestion_context.md) para auditar una cuota.
