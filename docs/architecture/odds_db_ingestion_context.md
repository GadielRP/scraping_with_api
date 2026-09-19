# Contexto de base de datos después de una ingestión de cuotas

Este archivo describe qué queda persistido después de procesar cuotas para un evento. Está pensado para copiarse como contexto al investigar un evento concreto en PostgreSQL.

## Alcance

El modelo canónico de cuotas tiene esta jerarquía:

```text
events
  └── markets                 (evento + bookmaker + market_type_id + línea + is_live)
        └── market_choices    (resultado canónico: 1, x, 2, over, under, etc.)
              └── market_choice_quotes       (instrumento de precio actual)
                    └── market_choice_snapshots (histórico append-only)
```

Una misma ejecución puede escribir datos de varios proveedores. El campo `source` identifica el proveedor y los datos de Oddspapi normalmente usan `source = 'oddspapi'`.

## 1. `events`

Es la entidad canónica del partido o evento. La ingestión de cuotas no crea un evento nuevo si ya existe uno resuelto; utiliza su `events.id`.

Campos relevantes:

| Campo | Uso |
|---|---|
| `id` | Identificador canónico del evento. Es la clave que se usa en todas las consultas de cuotas. |
| `starts_at` | Inicio del evento. Es una columna `TIMESTAMP WITH TIME ZONE` (`timestamptz`) normalizada a UTC en base de datos y tipada como `datetime` timezone-aware en Python (cumpliendo el contrato temporal canónico; históricamente denominada `start_time_utc`). |
| `sport`, `competition_id` | Contexto deportivo y competición. |
| `home_participant_id`, `away_participant_id` | Participantes canónicos. |
| `created_at`, `updated_at` | Auditoría de la entidad (instantes `timestamptz` en UTC). |

## 2. `event_source_mappings`

Relaciona el evento canónico con los identificadores de cada proveedor.

Para Oddspapi contiene normalmente:

| Campo | Ejemplo / significado |
|---|---|
| `event_id` | `events.id`. |
| `source` | `'oddspapi'`. |
| `source_event_id` | Fixture externo, por ejemplo `id1000000872478584`. |
| `source_sport_id` | Sport ID entregado por OddsPapi. |
| `has_odds` | Estado de disponibilidad de `/odds`; un 404 confirmado puede dejarlo en `false`. |
| `match_method`, `confidence` | Cómo y con qué confianza se resolvió el fixture. |

Esta tabla no guarda precios ni snapshots.

## 3. `bookies`

Catálogo canónico de bookmakers. Normalmente las filas se crean una vez y se reutilizan entre eventos.

| Campo | Uso |
|---|---|
| `bookie_id` | Clave usada por `markets`. |
| `name` | Nombre visible, por ejemplo `Bet365`, `Pinnacle`, `Betfair`. |
| `slug` | Identificador normalizado, por ejemplo `bet365`, `pinnacle`, `betfair-ex`. |

La ingestión no crea una fila por observación: crea o reutiliza la fila del catálogo.

## 4. `bookie_source_mappings`

Mapea el bookmaker del proveedor al bookmaker canónico.

Ejemplo de Oddspapi:

| Campo | Uso |
|---|---|
| `bookie_id` | Bookmaker canónico. |
| `source` | `'oddspapi'`. |
| `source_bookie_name` | Nombre recibido del proveedor. |
| `source_bookie_slug` | Slug recibido, por ejemplo `bet365`. |
| `match_method`, `confidence` | Resolución del catálogo. |

Tampoco contiene precios.

## 5. `canonical_market_types`

Catálogo de mercados canónicos (véase [`canonical_markets_list.md`](file:///c:/Users/gadie/Documents/projects/sofascore/docs/architecture/canonical_markets_list.md)). Sus filas son configuración compartida, no datos exclusivos del evento.

Campos relevantes:

| Campo | Uso |
|---|---|
| `canonical_market_key` | Clave estable del mercado (ej. `1x2_full_time`, `over_under_full_time`). |
| `canonical_market_name` | Nombre canónico, por ejemplo `1X2 Full Time`. |
| `canonical_market_group` | Grupo, por ejemplo `1X2`, `Over/Under`, `Asian Handicap`. |
| `canonical_market_period` | `Full Time`, `1st Half`, etc. |
| `market_family`, `requires_line_value` | Reglas del mercado. |
| `enabled_for_ingestion` | Si el catálogo permite ingestión (`True` para los 39 tipos canónicos). |
| `enabled_for_trajectory` | Si el mercado participa en análisis de trayectoria temporal (Pilar 4). |

## 6. `market_source_mappings`

Traduce `(source, source_sport_id, source_market_id)` a un mercado canónico.

Para auditar un mercado como 1X2 Full Time con `source_market_id = 101`, esta es la tabla que confirma la interpretación del ID externo.

| Campo | Uso |
|---|---|
| `canonical_market_key` | Mercado canónico destino. |
| `source` | `'oddspapi'`, `'sofascore'`, etc. |
| `source_sport_id` | Sport ID del proveedor. |
| `source_market_id` | ID externo, por ejemplo `101`. |
| `source_market_name`, `source_market_group`, `source_period` | Metadatos recibidos. |
| `source_handicap` | Línea cuando aplica. |

## 7. `market_outcome_source_mappings`

Traduce el outcome externo de un mercado al nombre canónico de la elección.

Ejemplos:

```text
source_market_id=101, source_outcome_id=1   -> choice_name='1'
source_market_id=101, source_outcome_id=2   -> choice_name='x'
source_market_id=101, source_outcome_id=3   -> choice_name='2'
```

Los IDs reales dependen del catálogo del proveedor.

| Campo | Uso |
|---|---|
| `market_source_mapping_id` | FK a `market_source_mappings`. |
| `source_outcome_id` | Outcome externo. |
| `source_outcome_name` | Nombre externo. |
| `canonical_choice_name` | Elección persistida en `market_choices`. |
| `display_order` | Orden de presentación. |

## 8. `markets`

Representa un mercado canónico para un evento y un bookmaker. Es la primera tabla de cuotas específica del evento.

Identidad lógica:

```text
event_id + bookie_id + market_type_id + line_value + is_live
```

Campos relevantes:

| Campo | Uso |
|---|---|
| `market_id` | Clave padre de `market_choices`. |
| `event_id` | Evento canónico. |
| `bookie_id` | Bookmaker canónico. |
| `market_type_id` | FK al catálogo canónico; resuelve nombre, grupo y periodo. |
| `line_value` | Línea numérica, por ejemplo `2.5`; suele ser NULL en 1X2. |
| `is_live` | Clasificación temporal del mercado. El flujo significativo forzado en T−5 conserva `false`; usar el detector no convierte el mercado en live. |
| `collected_at` | Instante de la escritura de la observación canónica (en UTC). |

`is_live` describe el tipo de mercado/ingestión, no si una cuota individual tuvo un cambio significativo.

## 9. `market_choices`

Representa cada resultado canónico dentro de `markets`.

Ejemplos:

```text
1X2 Full Time -> '1', 'x', '2'
Over/Under 2.5 -> 'over', 'under'
Both Teams To Score -> 'yes', 'no'
```

| Campo | Uso |
|---|---|
| `choice_id` | Clave padre de quotes y snapshots. |
| `market_id` | Mercado al que pertenece. |
| `choice_name` | Nombre canónico de la elección. |

No se guarda aquí el histórico de precios. El precio está en `market_choice_quotes` y `market_choice_snapshots`.

## 10. `market_choice_quotes`

Es el instrumento de precio actual. Hay como máximo una fila por identidad de instrumento:

```text
choice_id + source + exchange_side + exchange_level
```

Para bookmakers regulares, `exchange_side` es NULL y `exchange_level` normalmente es `0`. Para Betfair pueden existir instrumentos separados, por ejemplo `back/0` y `lay/0`.

Campos relevantes:

| Campo | Uso |
|---|---|
| `quote_id` | Clave padre de `market_choice_snapshots`. |
| `choice_id` | Elección canónica. |
| `source` | Proveedor, normalmente `'oddspapi'`. |
| `exchange_side`, `exchange_level` | Identidad de back/lay y nivel. |
| `main_line` | Indica si la línea fue seleccionada como principal cuando aplica. |
| `source_market_id`, `source_outcome_id` | Identidad externa conservada para auditoría. |
| `bookmaker_outcome_id` | ID adicional del bookmaker si el payload lo entrega. |
| `source_limit` | Límite/tamaño de la cuota cuando aplica. |
| `initial_odds` | Apertura normalizada, redondeada a la precisión SQL. |
| `initial_captured_at` | Timestamp del proveedor para la apertura (`initialChangedAt`). |
| `current_odds` | Última cuota válida normalizada antes del cutoff de kickoff. |
| `current_updated_at` | Hora de escritura del estado actual, no necesariamente el timestamp del proveedor. |
| `movement` | `-1` bajó, `0` sin cambio, `+1` subió. |
| `created_at`, `updated_at` | Auditoría de la fila. |

La estrategia significativa no reemplaza `initial_odds` ni `current_odds`. Aunque una cuota no supere el umbral, puede seguir siendo la cuota actual y quedar en estos campos.

## 11. `market_choice_snapshots`

Es el histórico append-only de observaciones de un `quote_id`.

| Campo | Uso |
|---|---|
| `snapshot_id` | Clave del snapshot. |
| `quote_id` | Instrumento exacto al que pertenece. |
| `odds_value` | Precio observado, con precisión SQL `NUMERIC(8,3)`. |
| `collected_at` | Momento en que se registra la observación (instante UTC). |
| `source_collected_at` | Timestamp informado por el proveedor (`createdAt`, `initialChangedAt`, `changedAt`, etc.). |
| `source_limit` | Límite/tamaño de la observación. |
| `exchange_size` | Tamaño para back/lay; NULL para bookmakers regulares. |

### Tipos de snapshots que puede contener

1. **Apertura ordinaria**: se crea cuando se establece `initial_odds`. Usa el timestamp de apertura del proveedor en `source_collected_at` y la hora de escritura en `collected_at`.
2. **Cuota actual ordinaria**: representa `current_odds` de esa ingestión. No requiere haber superado el umbral significativo.
3. **Snapshot de exchange**: observación de cada lado/nivel back o lay.
4. **`momentQuote` dinámico**: tick seleccionado por el detector al superar la magnitud configurada y sobrevivir la regla de reversión. Su `collected_at` es la hora local real del tick, con microsegundos antes de la precisión SQL; su `source_collected_at` es el timestamp del proveedor.
5. **`momentQuote` de fallback**: observación reconstruida en T−120, T−30, T−5, T−1 o T−0. Su `collected_at` es el momento teórico configurado y `source_collected_at` conserva el timestamp del tick que estaba vigente.

El detector se evalúa por serie (`bookmaker + mercado + outcome + jugador`). Por eso un mismo mercado puede tener algunas elecciones con snapshots dinámicos y otras con snapshots de fallback.

### Deduplicación

La precisión SQL de `collected_at` es de segundos. La deduplicación de momentos usa:

```text
quote_id + collected_at truncado a segundo
```

La memoria de la escritura conserva todos los `source_collected_at` asociados a esa clave. Una repetición con el mismo timestamp del proveedor se omite; otro timestamp del proveedor en el mismo segundo puede insertar un snapshot adicional.

La tabla no tiene orden implícito. Toda auditoría debe usar `ORDER BY`, por ejemplo:

```sql
ORDER BY
    snapshot.collected_at,
    snapshot.source_collected_at,
    snapshot.snapshot_id;
```

## 12. `oddspapi_mainline_outcome_cache`

Cache auxiliar usado para enriquecer adquisiciones históricas/live con la selección `mainLine` obtenida desde `/odds`.

| Campo | Uso |
|---|---|
| `cache_id` | Clave de cache. |
| `event_id` | Evento canónico. |
| `fixture_id` | Fixture de OddsPapi. |
| `source_sport_id` | Sport ID externo. |
| `bookmaker_slug` | Bookmaker externo. |
| `source_market_id` | Mercado externo. |
| `source_outcome_id` | Outcome externo seleccionado. |
| `canonical_market_key` | Mercado canónico resuelto. |
| `is_exchange` | Si pertenece a exchange. |
| `captured_at` | Hora de actualización del cache. |

Esta tabla no es un histórico de precios. Solo permite que una posterior respuesta de `/historical-odds` sepa qué outcomes/líneas deben conservarse como principales.

En el flujo híbrido forzado, el orden es:

```text
/odds -> oddspapi_mainline_outcome_cache -> /historical-odds -> markets/choices/quotes/snapshots
```

## Qué esperar después de una ingestión Oddspapi

Para un evento correctamente procesado, lo habitual es encontrar:

- una fila existente en `events`;
- una fila de `event_source_mappings` para el fixture Oddspapi;
- una fila reutilizada en `bookies` por cada bookmaker;
- uno o más `markets` por bookmaker y mercado canónico;
- una o más `market_choices` por mercado;
- una fila de `market_choice_quotes` por elección, proveedor y lado/nivel;
- uno o más `market_choice_snapshots` por quote, dependiendo de si se recibió apertura, cuota actual, exchange o `momentQuotes`;
- filas en `oddspapi_mainline_outcome_cache` si `/odds` se ejecutó y produjo outcomes principales.

No debe asumirse que todos los mercados tienen el mismo número de choices, quotes o snapshots. La selección, la disponibilidad de ticks, la antigüedad mínima y los filtros de mapeo se aplican por serie.

## Consulta base para auditar un evento

Sustituir `:event_id` por el ID canónico de `events.id`:

```sql
SELECT
    e.id AS event_id,
    e.starts_at,
    m.market_id,
    cmt.canonical_market_name AS market_name,
    cmt.canonical_market_group AS market_group,
    cmt.canonical_market_period AS market_period,
    m.line_value,
    m.is_live,
    b.bookie_id,
    b.name AS bookmaker,
    b.slug AS bookmaker_slug,
    mc.choice_id,
    mc.choice_name,
    q.quote_id,
    q.source,
    q.exchange_side,
    q.exchange_level,
    q.main_line,
    q.source_market_id,
    q.source_outcome_id,
    q.initial_odds,
    q.initial_captured_at,
    q.current_odds,
    q.current_updated_at,
    s.snapshot_id,
    s.odds_value,
    s.collected_at,
    s.source_collected_at,
    s.source_limit,
    s.exchange_size
FROM events e
JOIN markets m ON m.event_id = e.id
JOIN canonical_market_types cmt ON cmt.market_type_id = m.market_type_id
JOIN bookies b ON b.bookie_id = m.bookie_id
JOIN market_choices mc ON mc.market_id = m.market_id
LEFT JOIN market_choice_quotes q ON q.choice_id = mc.choice_id
LEFT JOIN market_choice_snapshots s ON s.quote_id = q.quote_id
WHERE e.id = :event_id
ORDER BY
    b.slug,
    cmt.canonical_market_name,
    cmt.canonical_market_period,
    m.line_value NULLS FIRST,
    mc.choice_name,
    q.source,
    q.exchange_side NULLS FIRST,
    q.exchange_level,
    s.collected_at,
    s.source_collected_at,
    s.snapshot_id;
```

Para limitar la auditoría a Oddspapi y a un mercado externo concreto:

```sql
...
WHERE e.id = :event_id
  AND q.source = 'oddspapi'
  AND q.source_market_id = '101'
  AND b.slug IN ('bet365', 'pinnacle')
...
```

La consulta de snapshots debe ordenar explícitamente; el orden físico de PostgreSQL no representa necesariamente T−120, T−30, T−5, T−1 y T−0.
