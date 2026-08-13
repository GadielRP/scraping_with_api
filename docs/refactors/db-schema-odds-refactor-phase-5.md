# Fase 5 — Cutover de lectores a `market_choice_quotes`

> **Estado definitivo (2026-08-12):** cutover validado y cerrado. Las variantes
> `legacy`/`shadow`, sus flags, comparador y vistas privadas de rollback fueron
> retirados antes de Fase 8. Este documento conserva su diseño como historial;
> el único contrato ejecutable es quote-aware.

**Documento maestro:** [db-schema-odds-refactor.md](./db-schema-odds-refactor.md)

**Backfill previo:** [Fase 4b](./db-schema-odds-refactor-phase-4b.md) y
[runbook 4c](./db-schema-odds-refactor-phase-4c-runbook.md)

**Branch objetivo:** `refactor/db-schema-odds-refactor`

**Estado (2026-08-12):** implementación terminada y cutover validado en la
copia PostgreSQL local post-4c. Pendiente únicamente el ciclo de observación en
el entorno objetivo antes de declarar cierre operativo y habilitar Fase 6.

## 1. Resultado esperado

Al cerrar esta fase, toda lectura activa de estado, identidad y lineage de odds
sale de `market_choice_quotes`; `MarketChoice` conserva únicamente identidad de
outcome y `MarketChoiceSnapshot` aporta únicamente el tick histórico. El cambio
se despliega por consumidor, con shadow comparison, gates medibles y una ruta de
reversión operativa que no modifica datos.

La fase termina con estos cuatro resultados:

1. Las alertas externas leen quotes y distinguen correctamente bookies normales
   de exchanges.
2. Trajectory, Pilar 4 (drift) y Pilar 5 conservan una serie por quote sin
   colisiones de source, side o level.
3. Dual-process, `event_all_odds` y `mv_alert_events` leen la quote explícita de
   SofaScore.
4. CI impide introducir nuevas lecturas de columnas legacy.

No se eliminan columnas, índices, filas redundantes ni SQL de rollback. Esas
acciones pertenecen a Fases 6 y 7.

## 2. Estado real confirmado en código

| Superficie | Estado actual | Riesgo que corrige Fase 5 |
|---|---|---|
| `MarketRepository.get_external_markets_for_event` | Lee `MarketChoice.initial_odds/current_odds` y ejecuta una query de snapshot por market para inferir `source` | Datos congelados/`NULL`, N+1 e identidad inferida |
| `_format_external_markets_section` | Agrupa por source inferido y usa `choice_group` como Back/Lay si el nombre contiene Betfair | Mezcla identidad de línea con side real |
| `alert_pipeline.py` | Usa el mismo reader legacy como prueba de disponibilidad | Puede declarar sin datos un evento que sí tiene quotes |
| `PRE_START_ODDS_TRAJECTORY_VIEW_SQL` | Toma opening de `mc.*` e identidad de `mcs.*` | Opening congelado; source/side/level repetidos en snapshots |
| `OddsTrajectoryRepository` | Particiona por `(event, market, bookie, choice, minute)` | Dos sources/sides compiten por el mismo slot |
| `odds_trajectory_context.py` | Indexa bookies solo por `bookie_name` y choices solo por nombre | Sobrescritura silenciosa de series |
| `drift_engine.py` | Su result key no incluye quote/source/side/level | Resultados de series distintas se pisan |
| Pilar 5 | Recorre el mismo contexto de trajectory y selecciona SofaScore por `bookie_id=1` | Debe conservar compatibilidad al cambiar las claves internas |
| `build_dual_process_event_odds_view_sql` | Lee opening/current de `mc.*` y busca el último snapshot por `choice_id` | Estado congelado y posible tick de otra quote |
| `create_or_replace_views` | Recrea vistas públicas directamente y usa `DROP ... CASCADE` para trajectory | No permite shadow seguro ni rollback preciso |

La escritura live de los tres providers ya converge en
`save_canonical_bookmaker_batches`; no se modifica durante esta fase.

## 3. Límites y decisiones cerradas

### 3.1. Dentro de alcance

- Read models y query set-based para alertas externas.
- Política explícita de selección/fusión de campos.
- Comparador legacy-versus-quotes y logs estructurados.
- Vistas paralelas legacy/quotes para trajectory y dual-process.
- Propagación de `quote_id`, `source`, `exchange_side` y `exchange_level` hasta
  contextos y resultados.
- Flags de cutover validados al arranque.
- Auditoría read-only de readiness y guard estático en CI.
- Pruebas unitarias, integración SQLite donde aplique y validación SQL real en
  PostgreSQL/staging.

### 3.2. Fuera de alcance

- `DROP COLUMN` en snapshots o choices.
- Borrar la quote Oddspapi `exchange_side=NULL` redundante.
- Borrar markets históricos con `choice_group IN ('Back', 'Lay')`.
- Reescribir adapters/writers o resolver ambigüedades de Fase 4b.
- Fusionar providers en la identidad persistida de una quote.
- Exponer profundidad completa del exchange; los consumidores actuales son
  top-of-book.

### 3.3. Invariantes no negociables

1. Una serie histórica se identifica por `quote_id`; nunca por nombre de
   bookie.
2. En exchange no se fusionan sources ni levels.
3. En bookies normales sí se permite fusionar `initial` y `current` de sources
   distintos, pero cada campo conserva su provenance.
4. Nunca se mezcla el opening de una quote con snapshots de otra en trajectory.
5. `choice_group` solo representa línea de mercado; jamás Back/Lay.
6. Un empate o contradicción no se resuelve con `.first()` sin orden total: se
   diagnostica y se omite el bloque afectado.
7. El reader quote-aware no hace fallback automático a columnas legacy después
   del cutover; esas columnas ya están congeladas y podrían ocultar el fallo.
8. La fase no cambia datos persistidos.

## 4. Contrato común de lectura

### 4.1. Nuevos módulos

Crear bajo `infrastructure/persistence/repositories/market/`:

| Archivo | Responsabilidad única |
|---|---|
| `market_read_models.py` | DTOs inmutables y serialización; cero SQL |
| `market_quote_read_policy.py` | Resolver prioridad de sources por campo y scope |
| `market_read_queries.py` | Query set-based y proyección de filas a read models |

`market_repository.py` conserva el nombre público
`get_external_markets_for_event`, pero delega directamente al query quote-aware.

### 4.2. DTOs

El contrato mínimo es equivalente a lo siguiente; los nombres son parte del
contrato y no deben mutar entre 5A y 5B:

```python
@dataclass(frozen=True, slots=True)
class QuoteFieldOrigin:
    quote_id: int
    source: str
    captured_at: datetime | None

@dataclass(frozen=True, slots=True)
class ExternalChoiceQuote:
    choice_id: int
    choice_name: str
    exchange_level: int | None
    initial: Decimal | None
    current: Decimal | None
    movement: int | None           # -1, 0, 1; None = desconocido
    initial_origin: QuoteFieldOrigin | None
    current_origin: QuoteFieldOrigin | None

@dataclass(frozen=True, slots=True)
class ExternalMarketQuoteBlock:
    market_id: int
    bookie_id: int
    bookie_name: str
    market_name: str
    market_group: str | None
    market_period: str
    choice_group: str | None
    is_live: bool
    aggregation: Literal["field_priority", "exchange"]
    source: str | None             # None solo para field_priority
    exchange_side: str | None
    contributing_sources: tuple[str, ...]
    choices: tuple[ExternalChoiceQuote, ...]

@dataclass(frozen=True, slots=True)
class MarketQuoteReadDiagnostic:
    code: str
    blocking: bool
    market_id: int | None
    choice_id: int | None
    quote_ids: tuple[int, ...]

@dataclass(frozen=True, slots=True)
class ExternalMarketQuoteReadResult:
    event_id: int
    blocks: tuple[ExternalMarketQuoteBlock, ...]
    diagnostics: tuple[MarketQuoteReadDiagnostic, ...]
```

Reglas del contrato:

- Los precios permanecen como `Decimal` hasta el formatter.
- `movement` se recalcula a partir de los campos finalmente seleccionados. Esto
  es obligatorio para el bloque fusionado, donde initial y current pueden venir
  de quotes distintas. Con un solo campo presente, es `None`, no `0`.
- Un bloque `field_priority` representa un bookie normal y tiene `source=None`;
  provenance vive por campo.
- Un bloque `exchange` representa exactamente `(market_id, source, side)`; el
  nivel elegido es por choice y queda expuesto.
- `to_legacy_dict()` existe solo en la fachada temporal y traduce movement a
  glifo. No se usa dentro de persistencia ni en trajectory.

## 5. Política de selección de quotes

### 5.1. Normalización previa

La query base recupera en una sola consulta:

`Event → Market → Bookie → MarketChoice → MarketChoiceQuote`.

Filtros:

- `Market.event_id = :event_id`.
- `Market.bookie_id != 1` para alertas externas.
- Al menos uno de `mcq.initial_odds` o `mcq.current_odds` no es `NULL`.
- No se filtra por nombre de provider/bookie.

Orden SQL total:

`market_id, choice_id, source, exchange_side NULLS FIRST, exchange_level,
quote_id`.

La proyección pura aplica después estas reglas.

### 5.2. Supresión de la quote `NULL` redundante

Para cada `(choice_id, source)`:

1. Si existe al menos una quote con side `back` o `lay` y algún precio, se
   excluyen las quotes del mismo scope con `exchange_side=NULL` en alertas. En
   trajectory, el side explícito debe tener al menos un snapshot enlazado antes
   de suprimir una serie `NULL` con historia; así el reader no fabrica una
   pérdida por aplicar un filtro de estado actual al histórico.
2. La fila se conserva en DB; la supresión solo afecta presentación y
   trajectory.
3. Se emite diagnóstico no bloqueante `redundant_unsided_quote_suppressed` con
   los quote IDs.
4. Una quote `NULL` de otro source sin sides explícitos no se atribuye a back o
   lay. Permanece como serie sin side y genera
   `unsided_quote_in_exchange_market`; no se fusiona con las series explícitas.

### 5.3. Top-of-book

Para cada `(choice_id, source, exchange_side)` se elige una sola quote:

1. Candidatos con algún precio para alertas, o con algún snapshot enlazado para
   trajectory.
2. Menor `exchange_level`.
3. `quote_id` solo actúa como desempate determinista si los datos ya violan la
   unicidad esperada; en ese caso se emite `unexpected_duplicate` bloqueante y
   se omite el choice, aunque exista un orden técnico.
4. Initial y current nunca se toman de niveles distintos.

Para un market totalmente no-exchange, más de un level por
`(choice_id, source, side=NULL)` se considera `unexpected_level` bloqueante; no
se interpreta como profundidad implícita.

### 5.4. Fusión de bookies normales

Solo se aplica cuando el market no tiene quotes con side explícito. Por cada
choice:

```text
initial = primer initial no nulo según initial_priority
current = primer current no nulo según current_priority
movement = compare(current, initial), o None si falta uno
```

Prioridades default:

- initial: `oddsportal`, `oddspapi`, `sofascore`.
- current: `oddspapi`, `sofascore`, `oddsportal`.

Sources presentes pero no configurados se evalúan después de los configurados,
en orden alfabético, y generan `unconfigured_source_fallback`. Nunca ganan sobre
un source configurado con valor.

Cada campo conserva `quote_id`, `source` y timestamp de la quote elegida. No se
crea una quote sintética ni se persiste el resultado fusionado.

### 5.5. Exchanges

- Un bloque por `(market_id, source, exchange_side)`.
- Back y lay son bloques distintos.
- OddsPortal opening-only se presenta como `opening→N/A`.
- Oddspapi conserva su propio opening/current.
- No se cruza provenance entre providers aunque ambos reporten el mismo side y
  level numérico.

## 6. Configuración y validación

La única configuración vigente es `ODDS_READ_PRIORITY_CONFIG`, cuyo default es
`config/odds_read_priority.json`. El archivo define prioridades por campo y
scope; un path ausente o un contenido inválido abortan la inicialización.

Se retiraron `EXTERNAL_ODDS_READ_MODE`, `PRE_START_TRAJECTORY_READ_MODE`,
`DUAL_PROCESS_ODDS_READ_MODE`, `ODDS_READ_SHADOW_SAMPLE_RATE` y
`MARKET_CHOICE_LEGACY_STOP_WRITE_AT`. Ya no existe selección runtime: alerts,
trajectory y dual-process usan quotes siempre.

Versionar `config/odds_read_priority.json`:

```json
{
  "version": 1,
  "default": {
    "initial": ["oddsportal", "oddspapi", "sofascore"],
    "current": ["oddspapi", "sofascore", "oddsportal"]
  },
  "overrides": []
}
```

Cada override acepta `sport`, `bookie_id` o ambos. Precedencia:

1. `sport + bookie_id`.
2. `bookie_id`.
3. `sport`.
4. `default`.

Validar version, scopes duplicados, sources vacíos/duplicados y arrays vacíos.
Todos los source names se normalizan a lowercase.

## 7. Fase 5A — Read models, query, readiness y shadow

### 7.1. Implementación

1. Crear los cuatro módulos de §4.
2. Exportarlos desde
   `infrastructure/persistence/repositories/market/__init__.py` solo cuando no
   introduzca ciclos.
3. Implementar
   `MarketReadQueries.get_external_market_quotes_for_event(event_id, policy)`.
4. Mantener `MarketRepository.get_external_markets_for_event` como fachada:
   - `legacy`: comportamiento anterior.
   - `shadow`: devuelve legacy, ejecuta quotes según sample y compara.
   - `quotes`: devuelve el adaptador de los bloques quote-aware.
5. Extraer el reader legacy a un helper nombrado `_get_external_markets_legacy`
   y marcarlo `LEGACY_ODDS_READ`; no duplicar su SQL.
6. Eliminar el N+1 únicamente del path quotes. El path legacy queda congelado
   para comparación/rollback y se borra en Fase 8.
7. Añadir `has_external_market_quotes_for_event(event_id)` mediante
   `SELECT EXISTS`; `alert_pipeline.py` lo usa como availability check en modo
   quotes para no materializar dos veces el read model.

### 7.2. Comparador shadow

El comparador recibe ambos resultados ya materializados; nunca abre sesión. Los
normaliza por market identity, bookie y choice, redondea solo para comparación a
la precisión física `NUMERIC(8,3)` y produce estas clases:

| Clase | Bloquea cutover | Definición |
|---|---:|---|
| `equal` | No | Mismos campos comparables |
| `expected_source_split` | No | Legacy colapsó sources exchange que quotes separa |
| `expected_side_split` | No | Legacy usó Back/Lay en `choice_group` |
| `expected_frozen_legacy` | No | Quote posterior a `MARKET_CHOICE_LEGACY_STOP_WRITE_AT`; legacy vacío/stale |
| `redundant_unsided_suppressed` | No | Solo difiere la quote `NULL` redundante |
| `missing_quote` | Sí | Estado legacy/snapshot clasificable sin quote |
| `missing_choice` | Sí | Choice desaparece de un lado sin razón esperada |
| `price_mismatch` | Sí | Precio comparable distinto |
| `unexpected_duplicate` | Sí | Más de una fila para la misma identidad de lectura |
| `ambiguous_quote` | Sí | No existe selección determinista válida |
| `unclassified_difference` | Sí | Diferencia que no encaja en las clases anteriores |

Si `MARKET_CHOICE_LEGACY_STOP_WRITE_AT` está vacío, una diferencia posiblemente
congelada queda como `unclassified_difference`; nunca se autoacepta.

Un log por evento:

```text
event=odds_quote_read_shadow
consumer=external_alerts
event_id=...
legacy_blocks=...
quote_blocks=...
duration_legacy_ms=...
duration_quotes_ms=...
diff_equal=...
diff_expected_*=...
diff_blocking=...
blocking_codes=[...]
```

No añadir una dependencia de métricas nueva: usar logging estructurado existente
y un resumen agregado al final del ciclo pre-start.

### 7.3. Auditoría read-only de readiness

Crear `scripts/maintenance/audit_market_quote_readiness.py`. Es read-only, acepta
`--event-id`, rango o todos, y escribe JSON opcional. Gates bloqueantes:

- Cualquier snapshot con `quote_id IS NULL` dentro del scope de cutover. La
  auditoría no vuelve a adivinar si una fila es clasificable: toda excepción
  debe haberse resuelto o excluido explícitamente del scope en Fase 4c.
- `snapshot.quote_id` que apunta a una quote de otro `choice_id`.
- Choices con estado legacy no nulo y cero quotes.
- Identidades NULL-safe duplicadas.
- Side fuera de `{NULL, back, lay}` o level negativo.
- Quotes sin source o sin choice/market válido.

Exit codes: `0` listo, `2` blockers de coverage/datos, `3` schema/config inválido,
`4` fallo de query. El script no llama `check_and_migrate_schema()`.

### 7.4. Gate de 5A

- Query quotes: una consulta principal, cero lazy loads.
- Todos los casos unitarios de §5 verdes.
- Shadow activo en staging sin cambiar mensajes.
- Ningún blocker del comparador en eventos cubiertos por Fase 4.
- 5A puede mergearse antes de 4c, pero ningún consumidor cambia a `quotes`
  hasta pasar la auditoría del scope completo.

## 8. Fase 5B — Alertas externas

### 8.1. Formatter

Actualizar `_format_external_markets_section` para consumir el contrato de §4:

- Bloques `field_priority` bajo `🟡 CONSOLIDATED ODDS`.
- Bloques `exchange` bajo `🟡 {SOURCE} EXCHANGE ODDS`.
- Label del instrumento: `Back`, `Lay` o `Unspecified`; nunca se deriva del
  nombre del bookie.
- `choice_group` se muestra entre corchetes solo para líneas (`Asian Handicap`,
  `Over/Under`).
- `movement`: `-1→↓`, `0→=`, `1→↑`, `None→sin glifo`.
- Initial-only: `1.89→N/A`; current-only: `1.89`; ambos ausentes no entran al
  read model.
- El formatter no recibe `quote_id` en el texto, pero lo conserva en el objeto
  para logs/debug.

Orden estable:

1. `field_priority` antes de `exchange`.
2. Source (`""` para consolidado), market display/group, period, market name.
3. `choice_group NULLS FIRST`, bookie name.
4. side `NULL`, `back`, `lay`.
5. choice display order conocido y luego `choice_name`.

### 8.2. Call sites

- `odds_alert.py` sigue llamando la fachada, ya sin inferencias locales.
- `alert_pipeline.py` usa `has_external_market_quotes_for_event` cuando el modo
  es quotes y el reader legacy solo en legacy/shadow.
- Un fallo del reader externo no impide enviar la sección principal SofaScore;
  omite la sección externa y emite `external_odds_read_failed` con event/mode.
- En quotes no hay fallback automático a legacy.

### 8.3. Casos de aceptación

1. Evento exchange `158955`: un market canónico, bloques separados por
   `(oddsportal|oddspapi) × (back|lay)`, sin quote `NULL` duplicada visible.
2. Bookie normal multi-source, por ejemplo bet365/evento `169158`: un solo
   bloque por market/bookie, initial/current fusionados por campo, con origins
   correctos.
3. OddsPortal opening-only continúa mostrando `opening→N/A`.
4. Una línea `2.5` permanece `choice_group=2.5`; Back/Lay nunca aparece allí.

### 8.4. Cutover

1. Desplegar 5B con `EXTERNAL_ODDS_READ_MODE=shadow`.
2. Observar al menos un ciclo pre-start completo que incluya T-120, T-30, T-5,
   T0 y T-5 posterior según `PRE_START_ODDS_MOMENTS`.
3. Exigir cero diffs bloqueantes y auditoría readiness exit `0`.
4. Cambiar solo este flag a `quotes` y observar otro ciclo completo.

## 9. Fase 5C — Trajectory, contexto, drift y Pilar 5

### 9.1. Vistas paralelas

Refactorizar la constante actual en tres definiciones con el mismo schema
público:

- `v_pre_start_odds_trajectory_legacy`: definición actual, congelada, con
  columnas quote identity añadidas como `mcs.quote_id`/metadata legacy para
  permitir comparación.
- `v_pre_start_odds_trajectory_quotes`: definición nueva.
- `v_pre_start_odds_trajectory`: wrapper que selecciona una de las anteriores
  según `PRE_START_TRAJECTORY_READ_MODE` al inicializar.

La vista quotes parte de:

```sql
market_choice_snapshots mcs
JOIN market_choice_quotes mcq ON mcq.quote_id = mcs.quote_id
JOIN market_choices mc ON mc.choice_id = mcq.choice_id
```

Además exige `mcs.choice_id = mcq.choice_id`; si el backfill dejó lineage
inconsistente, la fila no se admite y readiness debe bloquear el cutover.

Origen de columnas:

| Campo | Tabla |
|---|---|
| `quote_id`, source, side, level, IDs, `main_line`, `initial_odds` | `mcq` |
| `odds_value`, collected/source-collected time, tick limit, exchange size | `mcs` |
| choice identity | `mc` vía `mcq.choice_id` |
| market/bookie/event identity | `m`, `b`, `e` |

Los joins a `event_source_mappings`, `market_source_mappings` y outcome mappings
usan `mcq.source`, `mcq.source_market_id` y `mcq.source_outcome_id`.

La vista quotes aplica supresión de unsided redundante y selecciona un único
level top-of-book por `(choice_id, source, side)` antes de unir snapshots. Un
level no cambia entre target minutes.

`create_or_replace_views` deja de dropear la vista pública con `CASCADE` durante
shadow. Crea/actualiza primero ambas vistas privadas y al final el wrapper
público. Todas conservan el orden y tipos de columnas; las columnas nuevas se
añaden al final.

### 9.2. Repository

Extender `OddsTrajectoryPoint` y `to_dict()` con:

- `quote_id`.
- `source`.
- `exchange_side`.
- `exchange_level`.

`source_market_id`, `source_outcome_id`, `bookmaker_outcome_id` y `main_line`
permanecen disponibles en la vista para lineage/debug, pero no se agregan a
`OddsTrajectoryPoint` en Fase 5 porque ningún consumidor actual los usa.

Cambiar ranking a:

```sql
PARTITION BY event_id, quote_id, target_minute
ORDER BY distance_from_target, collected_at DESC, snapshot_id DESC
```

El orden final incluye `source`, side order, level y quote ID para ser total.

En `shadow`, el repository ejecuta legacy y quotes para la muestra determinista,
devuelve legacy y compara por `(event_id, quote_id o legacy identity,
target_minute)`. En `quotes`, consulta el wrapper quote-aware.

Implementar un helper privado con `view_name` y `ranking_mode`: el modo legacy
conserva la partición anterior `(event, market, bookie, choice, minute)` y el
modo quotes exige `(event, quote_id, minute)`. No reutilizar la partición quotes
sobre filas legacy con `quote_id=NULL`, porque colapsaría todo el histórico sin
lineage.

### 9.3. Contexto

Extender dataclasses:

- `OddsPointMeta`: agrega `quote_id`.
- `ChoiceOddsTrajectory`: agrega `quote_id`.
- `BookieOddsTrajectory`: agrega `source`, `exchange_side`,
  `exchange_level`.

Clave interna serializable:

```text
{bookie_id|unknown}:{source|unknown}:{side|single}:{level}
```

No usar una tupla como key porque el contexto se serializa a debug JSON. El
display name sigue en `bookie_name`.

`get_choice_odds_values` deja de indexar directamente por nombre. Acepta filtros
opcionales `bookie_id`, `source`, `exchange_side`, `exchange_level`; devuelve
vacío y diagnóstico/exception de dominio ante más de una coincidencia. El
default SofaScore resuelve explícitamente `bookie_id=1`, `source=sofascore`,
`side=None`, `level=0` para conservar compatibilidad.

### 9.4. Drift y Pilar 5

`drift_engine._build_choice_result_key` incluye, en este orden:

`bookie identity | source | side | level | quote_id | choice_group |
choice_name | market_name`.

Los resultados active/missing y su bloque raw incluyen esos mismos campos. El
opening leído por `_get_required_inputs` continúa en
`ChoiceOddsTrajectory.initial_odds`, pero ahora proviene de la misma quote que
los ticks.

Aunque Pilar 5 no use exchanges, debe recibir regresiones específicas: su filtro
por `bookie_id=1` y selección SofaScore producen exactamente un price set antes
y después, sin depender de que la key del diccionario sea `"SofaScore"`.

### 9.5. Gate de 5C

- Caso sintético 2 sources × 2 sides en el mismo target minute: cuatro series,
  cero colisiones.
- Ninguna fila quotes con `quote_id=NULL`.
- Ninguna serie cambia de level entre minutos.
- Drift emite cuatro keys distintas.
- Pilar 5 SofaScore mantiene salida equivalente.
- Shadow de staging sin pérdidas y p95 dentro de presupuesto.

## 10. Fase 5D — Dual-process, dependencias y cierre

### 10.1. Vistas dual-process

Crear definiciones paralelas con schema idéntico:

- Durante el rollout existieron `v_dual_process_event_odds_legacy` y
  `v_dual_process_event_odds_quotes` detrás de un wrapper configurable.
- Tras la validación funcional y Fase 6, ambas privadas y el selector fueron
  retirados. `v_dual_process_event_odds` es ahora la implementación quote-aware
  canónica.

La definición quotes une para cada choice exactamente:

```sql
mcq.source = 'sofascore'
AND mcq.exchange_side IS NULL
AND mcq.exchange_level = 0
```

Opening: `mcq.initial_odds`.

Current:

```sql
COALESCE(latest_snapshot.odds_value, mcq.current_odds)
```

El lateral de último snapshot filtra por `mcs.quote_id = mcq.quote_id`, ordena
por `collected_at DESC, snapshot_id DESC` y nunca por `choice_id` solo.

`last_sync_at`:

```sql
COALESCE(
  latest_snapshot.collected_at,
  mcq.current_updated_at,
  mcq.initial_captured_at,
  m.collected_at
)
```

El pivot y schema público (`one_open`, `one_final`, etc.) no cambian, por lo que
`DualProcessOddsRepository` y engines no requieren un contrato nuevo.

### 10.2. Orden de despliegue de dependencias

En una transacción de DDL controlada:

1. Crear/replace vistas privadas legacy y quotes.
2. Comparar ambas directamente antes del cutover.
3. Repoint del wrapper público a quotes.
4. Re-crear `event_all_odds`.
5. Dropear/re-crear `mv_alert_events` con su definición e índices actuales.
6. Refrescar MV y validar resultados.

No usar `CASCADE` sin inventariar el objeto que se recreará en la misma
transacción. `basketball_results` y `season_events_with_results` no dependen de
dual-process y no se tocan.

Comparaciones obligatorias antes/después:

- Cardinalidad total y por sport.
- Event IDs añadidos/perdidos.
- Nulos por cada columna 1/X/2.
- Distribución y checksum ordenado de opens/finals/variaciones.
- Resultados de `DualProcessOddsRepository.get_event_odds*` en una muestra.
- Queries de `candidate_search.py` y `historical_samples.py` contra
  `mv_alert_events`.

### 10.3. Guard estático

Crear `scripts/maintenance/check_no_legacy_odds_reads.py` y ejecutarlo en CI.
Debe detectar:

- Accesos ORM a `MarketChoice.initial_odds/current_odds/change`.
- SQL embebido con `mc.initial_odds/current_odds/change`.
- Lectura de identity estable desde `mcs.source`, source IDs, `main_line`, side o
  level.

La allowlist es por path + símbolo + motivo + fase de expiración. Al cerrar 5D
solo puede contener:

- Definiciones legacy de rollback.
- Writers/migraciones/backfill.
- Declaraciones del modelo pendientes de Fases 6–7.
- Tests explícitos de compatibilidad legacy.

Antes de activar el guard, migrar los prints activos de
`scripts/development/simulate_pre_start_check.py` para inspeccionar
`MarketChoiceQuote`/read models en lugar de `choice.initial_odds/current_odds/change`.
Los scripts bajo `legacy/` que todavía **escriben** el mirror se mantienen
allowlisted hasta Fase 8; no se confunden con lectores activos.

El análisis AST debe resolver el modelo/alias y el análisis SQL debe buscar
aliases conocidos. No se prohíbe el texto genérico `choice.initial_odds`, porque
`ChoiceOddsTrajectory.initial_odds` es parte válida del nuevo contrato.

El test del guard crea un fixture temporal violatorio y exige exit no cero; no
se limita a ejecutar el guard sobre un repo que casualmente ya está limpio.

## 11. Matriz de pruebas

### 11.1. Archivos nuevos o ampliados

| Test | Cobertura mínima |
|---|---|
| `tests/test_market_read_queries.py` | Query set-based, merge normal, exchange split, supresión NULL, top-of-book, orden, lineage |
| `tests/test_market_read_comparator.py` | Retirado junto con el comparador tras validar el cutover |
| `tests/test_market_quote_readiness.py` | Auditoría y exit codes |
| `tests/test_external_odds_alert_quote_read.py` | Snapshots textuales consolidado/exchange/opening-only |
| `tests/test_odds_endpoint_404_handling.py` | Actualizar la regresión existente de OddsPortal initial-only al nuevo label/contrato |
| `tests/test_odds_trajectory_repository.py` | Ranking por quote y serialización |
| `tests/test_odds_trajectory_quote_view.py` | Origen correcto de columnas y exclusión de lineage inconsistente |
| `tests/pillars/test_odds_trajectory_context.py` | Keys multi-source/side, helpers no ambiguos, filtros |
| `tests/pillars/test_drift_engine_quote_identity.py` | Keys/resultados sin colisión |
| `tests/pillars/test_exact_price_memory_quote_context.py` | Compatibilidad SofaScore del Pilar 5 |
| `tests/test_dual_process_quote_view.py` | Quote SofaScore exacta, latest por quote, pivot estable |
| `tests/test_no_legacy_odds_reads.py` | Guard positivo y negativo |

Actualizar `.gitignore` con excepciones para cada `test_*.py`, este documento y
`config/odds_read_priority.json`; el repo ignora esos patrones por defecto.

### 11.2. Escenarios obligatorios

| Escenario | Resultado |
|---|---|
| Normal, un source | Paridad exacta |
| Normal, OddsPortal initial + Oddspapi current | Un bloque fusionado con dos origins |
| Exchange, dos sources × back/lay | Cuatro series independientes |
| Exchange con NULL redundante + sides | NULL no visible |
| Niveles 0/1/2 | Solo nivel 0 elegible; no mezcla temporal |
| Nivel 0 sin snapshots, nivel 1 con snapshots | Trajectory elige nivel 1 (primer level elegible con historia) y lo conserva en todos los minutos |
| Empate/duplicado inválido | Diagnóstico bloqueante y bloque omitido |
| Initial-only | `movement=None`, `opening→N/A` |
| Current-only | Precio actual sin movimiento fabricado |
| Snapshot apunta a quote de otro choice | Readiness bloquea; vista quotes excluye |
| SofaScore dual-process | 1/X/2 y variaciones equivalentes |

### 11.3. Capas de ejecución

1. Unitarias puras: policies, projector, comparator, formatter y context.
2. Integración SQLite: ORM query/read models y readiness donde el dialecto sea
   compatible.
3. PostgreSQL real: vistas, `LATERAL`, window functions, `NULLS FIRST`, DDL y
   materialized view. No aceptar string assertions como sustituto de esta capa.
4. Staging: eventos de referencia, shadow, latencia y refresh completo.

Suite focal propuesta:

```bash
python -m pytest -q \
  tests/test_market_read_queries.py \
  tests/test_market_quote_readiness.py \
  tests/test_external_odds_alert_quote_read.py \
  tests/test_odds_endpoint_404_handling.py \
  tests/test_odds_trajectory_repository.py \
  tests/test_odds_trajectory_quote_view.py \
  tests/pillars/test_odds_trajectory_context.py \
  tests/pillars/test_drift_engine_quote_identity.py \
  tests/pillars/test_exact_price_memory_quote_context.py \
  tests/test_dual_process_quote_view.py \
  tests/test_no_legacy_odds_reads.py
```

Después, suite completa existente.

## 12. Presupuesto de performance

Capturar baseline legacy en el mismo dataset y conexión antes de cambiar cada
flag. Gates:

- Alert query quote-aware: exactamente una consulta principal; p95 no mayor que
  `max(legacy_p95 × 1.20, legacy_p95 + 25 ms)`.
- Trajectory batch: p95 no mayor que el mismo presupuesto relativo y sin
  crecimiento de filas fuera de la cardinalidad esperada por source/side.
- Refresh de `mv_alert_events`: duración no mayor que baseline × 1.20.
- Memoria del ciclo pre-start: no aumentar peak RSS más de 10% por shadow; en
  producción el sample rate se reduce si el límite se excede.

Usar al menos 100 eventos estratificados, o todos si el scope contiene menos.
Registrar tamaño de muestra, p50/p95, statement count, filas y plan de
`EXPLAIN (ANALYZE, BUFFERS)` en staging. Añadir índice solo si el plan real lo
justifica; los índices actuales de quote identity y
`snapshot(quote_id, collected_at, snapshot_id)` deberían cubrir el acceso
principal.

## 13. Rollout, rollback y gates

### 13.1. Precondiciones de cutover

- Fase 4c completada para el scope.
- Auditoría readiness exit `0`.
- Backfill repetido idempotente: cero inserts/updates/links.
- Shadow sin blockers en muestra estratificada por sport, antigüedad, source,
  bookie, market y side.
- Suites focal/completa verdes.
- Presupuesto de performance aprobado.

### 13.2. Orden

1. 5A en shadow.
2. 5B alerts a quotes; observar ciclo completo.
3. 5C trajectory/context/drift/Pilar 5 a quotes; observar ciclo completo.
4. 5D dual-process; recrear/validar dependencias.
5. Activar guard CI y archivar reportes de cutover.
6. Mantener SQL legacy y flags durante al menos 24 horas, un ciclo pre-start
   completo y un refresh completo de `mv_alert_events`; autorizar Fase 6 solo
   si los tres gates transcurren sin blocker.

### 13.3. Rollback honesto

Los flags y wrappers permiten volver técnicamente a legacy, pero el mirror de
`MarketChoice` ya no se actualiza para eventos nuevos. Por tanto:

- `legacy` es un rollback degradado y temporal, útil sobre histórico anterior al
  stop-write, no una garantía de datos completos.
- Si falla alerts quotes para eventos nuevos, se omite la sección externa antes
  que publicar odds stale.
- Si falla trajectory quotes, se desactiva/aisla el consumidor afectado o se
  corrige forward; no se mezcla automáticamente con trajectory legacy.
- Si falla dual-process, repoint del wrapper a legacy solo después de medir qué
  eventos recientes quedarían incompletos; `mv_alert_events` se recrea de nuevo.
- Dual-write/backfill permanecen intactos en todos los casos.

## 14. División exacta de PRs

| PR | Cambios | Gate de merge |
|---|---|---|
| **5A.1** | DTOs, policy config, archivo JSON, readiness CLI | Unit tests + readiness sobre fixtures |
| **5A.2** | Query set-based, comparator, fachada/flags shadow | Una query, todas las diferencias clasificadas |
| **5B** | Formatter, availability call, snapshots textuales | `158955` y `169158` correctos; shadow sin blockers |
| **5C.1** | Vistas trajectory privadas/pública + repository | PostgreSQL integration; ranking por quote |
| **5C.2** | Context, drift y regresión Pilar 5 | 2×2 sin colisión; SofaScore compatible |
| **5D.1** | Vistas dual privadas/pública + rebuild de dependencias | Counts/checksums/MV y consumers verdes |
| **5D.2** | Guard estático, allowlist y cierre operativo | CI verde; referencias activas legacy = 0 |

No mezclar el cutover de dos consumidores en un mismo cambio de configuración.

## 15. Criterio final de aceptación

Fase 5 está completa únicamente cuando:

- Alerts externos, trajectory y dual-process usan exclusivamente el contrato
  quote-aware, sin flags de selección.
- La vista pública dual-process apunta a quotes y sus dependencias fueron
  recreadas/verificadas.
- Cero snapshots clasificables sin `quote_id` en scope.
- Cero lecturas activas de odds state desde `MarketChoice`.
- Cero lecturas activas de identity desde `MarketChoiceSnapshot`.
- Alertas normales fusionan por campo con provenance; exchanges permanecen
  separados por quote/source/side/level.
- Trajectory y drift no colapsan series; Pilar 5 conserva compatibilidad.
- Guard CI, suite completa, staging y presupuestos de latencia/cardinalidad
  están verdes.
- Reportes de readiness, shadow y comparación de MVs quedan archivados.

Solo entonces se habilita Fase 6.

## 16. Registro de ejecución local

Implementado:

- 5A: DTOs inmutables, política JSON versionada, query set-based, comparator,
  flags, muestreo estable y auditoría read-only.
- 5B: formatter quote-aware y availability barato; legacy queda aislado detrás
  de la fachada de rollback.
- 5C: vistas privadas legacy/quotes, ranking por quote, identidad completa en
  contexto/drift y filtro exacto SofaScore para Pilar 5.
- 5D: vistas dual privadas/pública, rebuild de dependencias sin `CASCADE` para
  la MV, guard AST+SQL con allowlist por símbolo y workflow CI.
- Simuladores activos migrados a quotes. Se corrigió además el timestamp
  provisional de una ladder exchange opening-only para permitir que el current
  provider posterior avance.

Evidencia:

- readiness total `ready=true`;
- trajectory `158955`: 84 legacy / 84 quotes; `169158`: 16/16;
- alert reader: paridad `equal`, cero blockers, quote-aware más rápido en ambos
  eventos de referencia;
- dual final: 126,071 eventos comunes, cero pérdidas/mismatches y 177 altas
  quote-only; las 177 tienen mirror legacy incompleto y cero choices presentan
  quotes SofaScore elegibles duplicadas;
- MV final: 123,051 filas y checksum ordenado por `event_id`
  `ff4802dea244f4fb6651264d47143c2f`, idéntico en legacy/quotes;
- refresh alternado final (3+3): medianas 3.658 s legacy / 4.329 s quotes,
  ratio `1.183×` (`≤1.20×`). El plan usa los índices específicos de quote y
  snapshot;
- guard repo-wide: cero violaciones no allowlisted;
- regresión mantenida más tests nuevos: 287 verdes. La suite global antigua
  conserva fallos preexistentes documentados en el maestro §12.4.

Configuración local final: no existen flags de modo. Sólo permanece la política
versionada `ODDS_READ_PRIORITY_CONFIG`.

## 17. Cierre y transición

La implementación de esta fase quedó cerrada en el commit `67b1d3f`
(`feat(odds): cut readers over to quote identities`). Fase 6 inició después de
ese commit sobre la copia PostgreSQL local. Su plan, gates destructivos y
evidencia se mantienen separados en
[`db-schema-odds-refactor-phase-6.md`](db-schema-odds-refactor-phase-6.md).

El rollback por flags descrito en el plan original fue retirado después de la
validación funcional. Después del DROP slim de Fase 6, volver al schema anterior
requiere restaurar una copia/backup.

## 18. Limpieza definitiva posterior a la validación

Ejecutada antes de iniciar cualquier trabajo propio de Fase 8:

- `MarketRepository` usa exclusivamente `MarketReadQueries` y ya no contiene
  reader legacy, muestreo ni branches por modo;
- el formatter sólo acepta `ExternalMarketQuoteBlock`; el contrato dict legacy
  falla explícitamente;
- se eliminaron `market_read_comparator.py`, sus DTOs shadow y sus tests;
- se eliminaron el alias muerto `get_oddsportal_markets_for_event` y el script
  de prueba obsoleto que dependía de contratos antiguos;
- el CLI de Fase 6 ya no valida modos configurables inexistentes;
- dual-process y trajectory conservan un único nombre/vista canónica; también
  se retiró del script de Fase 6 la compatibilidad temporal para variantes,
  después de eliminarlas de la base local y confirmar que no existen en server.
