# Refactor de schema de odds — separación de responsabilidades

**Branch:** `refactor/db-schema-odds-refactor`
**Estado:** Fases 1-3 completadas (tabla/writer de quotes + convergencia de los 3 providers en un solo writer + fix de back/lay en OddsPortal). Adicionalmente: `exchange_side` migrado de sentinel `'single'` a `NULL` + índice funcional `COALESCE` (congruencia con `Market.choice_group`), y `market_choices` dejó de recibir escrituras de `initial_odds/current_odds/change` desde el path canónico — ver [§3.2](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado). Pendiente: Fase 4 (backfill) en adelante, con Fase 5 (migrar lectores) ahora más urgente.
**Alcance:** `infrastructure/persistence/models.py`, `infrastructure/persistence/repositories/market_repository.py`, `infrastructure/persistence/market_write_policy.py`, `modules/odds_ingestion/adapters/*`, `modules/oddspapi/exchange_quotes.py`, lectores de trajectory/alerts/pillars.

## Índice

- [1. Problema](#1-problema)
- [2. Causa raíz](#2-causa-raíz)
- [3. Inventario actual: código vivo vs legacy](#3-inventario-actual-código-vivo-vs-legacy)
  - [3.1. Estado actual (post Fase 3)](#31-estado-actual-post-fase-3)
  - [3.2. Decisión: `market_choices` deja de escribirse antes de Fase 5 (riesgo aceptado)](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado)
- [4. Principio de diseño](#4-principio-de-diseño)
- [5. Schema propuesto](#5-schema-propuesto)
- [6. Estructura de módulos propuesta](#6-estructura-de-módulos-propuesta)
- [7. Tabla de renombres/reemplazos](#7-tabla-de-renombresreemplazos)
- [8. Fases de implementación](#8-fases-de-implementación)
  - [Fase 0 — Preparación y freeze de contrato](#fase-0--preparación-y-freeze-de-contrato)
  - [Fase 1 — Tabla y repositorio nuevos: `market_choice_quotes`](#fase-1--tabla-y-repositorio-nuevos-market_choice_quotes)
  - [Fase 2 — Unificar escritura: writers compartidos](#fase-2--unificar-escritura-writers-compartidos)
  - [Fase 3 — OddsPortal: split del adapter y fix Back/Lay](#fase-3--oddsportal-split-del-adapter-y-fix-backlay)
  - [Fase 4 — Backfill de datos históricos](#fase-4--backfill-de-datos-históricos)
  - [Fase 5 — Migrar lectores a quotes](#fase-5--migrar-lectores-a-quotes)
  - [Fase 6 — Adelgazar `market_choice_snapshots`](#fase-6--adelgazar-market_choice_snapshots)
  - [Fase 7 — Deprecar columnas legacy en `market_choices`](#fase-7--deprecar-columnas-legacy-en-market_choices)
  - [Fase 8 — Limpieza de código legacy](#fase-8--limpieza-de-código-legacy)
- [9. Riesgos y mitigaciones](#9-riesgos-y-mitigaciones)
- [10. Mapa rápido de archivos](#10-mapa-rápido-de-archivos)

---

## 1. Problema

Dos providers (`oddsportal`, `oddspapi`) escriben odds de Betfair Exchange para el mismo evento/mercado, y terminan en **filas distintas** que la capa de lectura (alertas, pillars, trajectory) no puede reconciliar.

Ejemplo real (evento `158955`, WNBA, mercado Home/Away):

| market_id | Origen | choice_group | initial | current | Motivo |
|---|---|---|---|---|---|
| 757936 | Oddspapi `/odds` | `NULL` | 3.30 | 3.05 | fila "normal" |
| 757937 | OddsPortal | `Back` | 1.01 | `NULL` | opening-only |
| 757938 | OddsPortal | `Lay` | 3.50 / 1.45 | `NULL` | opening-only |

La alerta muestra `1.01→N/A` para el mercado OddsPortal, mientras el current real (3.05) vive en otra fila, en otra sección de la alerta.

Además, `mainLine` y `source` no tienen columna propia en `markets` ni `market_choices` — viven únicamente en `market_choice_snapshots`, mezclando "estado actual" con "historial append-only". Cada snapshot (insertado en cada tick de precio) repite columnas de identidad que no cambian tick a tick (`source`, `source_market_id`, `source_outcome_id`, `bookmaker_outcome_id`) — impacto directo en tamaño de filas/índices y velocidad de escritura.

## 2. Causa raíz

`MarketChoice` asume **1 outcome = 1 precio**. Correcto para bookies normales, falso para exchanges (Betfair tiene *back* y *lay*). Cada provider improvisó dónde meter la dimensión que no cabía:

- **OddsPortal**: metió el lado (`Back`/`Lay`) en `Market.choice_group` → rompe la identidad de `Market` (que ya usa `choice_group` para líneas) → markets duplicados.
- **Oddspapi**: metió el lado y los metadatos (`source`, `mainLine`) en `market_choice_snapshots` → rompe la pureza del historial.

```484:494:infrastructure/persistence/models.py
    choice_group = Column(Text)  # For Over/Under: "2.5", "3.5", etc. NULL for non-line markets
    ...
    __table_args__ = (
        UniqueConstraint('event_id', 'bookie_id', 'market_name', 'market_period', 'choice_group', 'is_live', name='unique_market_per_event_bookie'),
```

```26:59:infrastructure/persistence/market_write_policy.py
ODDSPORTAL_OPENING_ONLY_POLICY = MarketWritePolicy(
    name="oddsportal_opening_only",
    overwrite_initial_odds=True,
    persist_current_odds=False,
    persist_opening_snapshots=False,
    persist_current_snapshots=False,
    require_initial_odds=True,
)
```

## 3. Inventario actual: código vivo vs legacy

> ⚠️ **Esta sección es el snapshot de Fase 0, tomado ANTES de implementar nada** (sirvió para decidir qué tocar primero y qué marcar como muerto). **Ya no describe el estado actual del código.** Para el estado real post-Fase 3, ver [§3.1](#31-estado-actual-post-fase-3) inmediatamente abajo. Se deja el snapshot histórico sin editar como registro de la causa raíz.

Antes de tocar nada, esto es lo que confirmé leyendo `market_odds_ingestion_service.py` de punta a punta — **hay dos escritores paralelos que ya divergieron en funcionalidad**, y parte del código de `MarketRepository` ya está muerto hoy, antes de cualquier refactor.

| Método | ¿Quién lo llama hoy? | Estado |
|---|---|---|
| `MarketRepository.save_canonical_bookmaker_batches` | `MarketOddsIngestionService.save_from_oddsportal_data` (línea 195) — **único path vivo de OddsPortal** | **VIVO**, pero sin soporte de exchange quotes (back/lay) en absoluto |
| `MarketRepository.save_markets_from_response_with_stats` | `MarketOddsIngestionService.save_from_oddspapi_response` (línea 607) y `_save_normalized` (línea 785, SofaScore) — **path vivo de Oddspapi y SofaScore** | **VIVO**, contiene la única lógica de exchange back/lay que existe hoy (líneas 389-496) |
| `MarketRepository.save_markets_from_oddsportal` | Nadie (0 call sites fuera de su propio log) | **MUERTO** — reemplazado por `OddsPortalMarketAdapter` + `save_canonical_bookmaker_batches` |
| `MarketRepository._save_oddsportal_market` | Solo desde `save_markets_from_oddsportal` (muerto) | **MUERTO** |
| `MarketRepository._build_choice_payload` | Solo desde `_save_oddsportal_market` (muerto) | **MUERTO** |
| `MarketRepository.save_markets_from_response` (sin `_with_stats`) | `scripts/legacy/*.py`, `scripts/sport_seasons_processing.py` | **VIVO pero solo para scripts de mantenimiento**, no en el pipeline de ingesta en tiempo real |
| `MarketRepository.get_oddsportal_markets_for_event` | Alias declarado, sin call sites propios encontrados | **MUERTO/alias redundante** |

**Implicación clave para el diseño:** el hecho de que `save_canonical_bookmaker_batches` (la ruta "nueva"/canónica, usada por OddsPortal) **nunca recibió** la lógica de exchange quotes que sí tiene `save_markets_from_response_with_stats` (la ruta "vieja", usada por Oddspapi) es en sí mismo un segundo bug de diseño, independiente del de `choice_group`: son dos implementaciones casi paralelas de "upsert market+choice+snapshot" con funcionalidades distintas. Este refactor las converge en una sola.

### 3.1. Estado actual (post Fase 3)

**La ingesta en tiempo real (los 3 providers) ya corre 100% sobre la implementación nueva.** Verificado por búsqueda global de call sites (`MarketOddsIngestionService.*` y `MarketRepository.save_*`):

| Provider | Entry point(s) en `market_odds_ingestion_service.py` | Llamados desde (pipeline en vivo) | Termina en |
|---|---|---|---|
| OddsPortal | `save_from_oddsportal_data` | `modules/jobs/pre_start_check_job/oddsportal_worker.py` | `save_canonical_bookmaker_batches` (con quotes back/lay correctos desde Fase 3) |
| Oddspapi | `save_from_oddspapi_response` | `modules/jobs/pre_start_check_job/providers/oddspapi/*` | `save_canonical_bookmaker_batches` (una sola llamada agregada, no un loop por bookmaker) |
| SofaScore | `save_from_sofascore_response`, `save_from_dropping_odds_map_entry`, `save_from_daily_odds_entry` (los 3 vía `_save_normalized`) | `modules/jobs/parallelism/discovery_optimization.py`, `modules/jobs/pre_start_check_job/rescheduled_events.py`, `modules/jobs/pre_start_check_job/providers/sofascore/odds_phase.py`, `modules/jobs/daily_discovery/persistence.py`, `modules/jobs/results_collection_job/run_results_collection_job.py` | `save_canonical_bookmaker_batches` |

Ningún archivo de `modules/jobs/*` (pipeline en vivo) llama ya a `save_markets_from_response_with_stats` ni a `save_markets_from_response`. Los **únicos** call sites restantes de esas dos rutas legacy son scripts fuera del pipeline en vivo:

- `scripts/sport_seasons_processing.py`
- `scripts/legacy/extract_historical_results_legacy_event_odds.py`
- `scripts/legacy/process_null_seasons_legacy_event_odds.py`
- `legacy/parse_telegram_odds.py`
- `verify_snapshots.py` (script de desarrollo, raíz del repo)

Esas rutas **no escriben `MarketChoiceQuote`** (nunca se extendieron, quedan documentadas como `LEGACY_MAINTENANCE_ONLY`, ver [§7](#7-tabla-de-renombresreemplazos)) — si algún día vuelven a correr sobre datos con exchange back/lay, ese dato no llegaría a quotes. No es un problema hoy porque no ingieren datos en producción, pero es una razón más para resolverlo en Fase 8 en vez de dejarlo indefinidamente.

**Lo que falta para cerrar el refactor por completo** (Fases 4-8, ver [§8](#8-fases-de-implementación)):
1. **Fase 4 — Backfill**: todo el historial anterior a este refactor (`market_choice_snapshots` viejos, `MarketChoice` sin quotes) no tiene fila en `market_choice_quotes` todavía. Los eventos que ya estaban en curso antes de este deploy no tienen quotes retroactivas hasta correr el script de backfill.
2. **Fase 5 — Migrar lectores**: `odds_alert.py`, `odds_trajectory_context.py`/`v_pre_start_odds_trajectory`, `drift_engine.py` y el dual-process view **siguen leyendo de las columnas legacy** (`MarketChoice.initial_odds/current_odds/change`, metadata smuggled en `market_choice_snapshots`), no de `market_choice_quotes`. Desde la decisión documentada en [§3.2](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado), esas columnas ya **no se actualizan** para eventos nuevos vía `save_canonical_bookmaker_batches` — estos lectores verán `NULL`/datos incompletos hasta que se migren.
3. **Fase 6 — Adelgazar `market_choice_snapshots`**: quitar columnas de identidad redundantes una vez todo lector pase por `quote_id`.
4. **Fase 7 — Deprecar columnas legacy en `market_choices`**: el *dejar de escribir* ya se adelantó (ver [§3.2](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado)); a Fase 7 le queda únicamente el DDL — `ALTER TABLE market_choices DROP COLUMN initial_odds/current_odds/change` — una vez Fase 5 esté en producción y nada las siga leyendo.
5. **Fase 8 — Limpieza de código legacy**: borrar los métodos muertos ya marcados y decidir qué hacer con los 5 scripts que todavía llaman `save_markets_from_response(_with_stats)`.

En resumen: la ingesta ya es 100% la implementación nueva; lo que resta es backfill (Fase 4) + migrar lectores (Fase 5, ahora más urgente por [§3.2](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado)) + limpieza/adelgazamiento (Fases 6-8).

### 3.2. Decisión: `market_choices` deja de escribirse antes de Fase 5 (riesgo aceptado)

**Contexto.** Al revisar `save_canonical_bookmaker_batches` para decidir si `initial_odds`/`current_odds`/`change` debían seguir viviendo también en `MarketChoice` (además de `MarketChoiceQuote`), se encontró que el mirror de `MarketChoice` **no era un duplicado inerte**: era el único lugar donde se combinaban **dos fuentes** en un solo valor — OddsPortal aporta el `initial` (política `overwrite_initial_odds=True, persist_current_odds=False`) y Oddspapi aporta el `current` (política default), sin pisarse entre sí sin importar el orden de escritura (cubierto por `test_oddsportal_opening_and_oddspapi_current_are_order_independent`).

`market_choice_quotes`, en cambio, guarda una fila **separada por `source`** (por diseño — es justo lo que corrige el bug original). Eso significa que hoy **no existe ningún lector que sepa reconstruir "initial de OddsPortal + current de Oddspapi" combinando quotes**: esa combinación solo la hacían `odds_alert.py`, `odds_trajectory_context.py`, `drift_engine.py` y el dual-process view, leyendo el mirror de `choices`.

**Decisión (explícita, aceptando el riesgo):** se detiene la escritura de `MarketChoice.initial_odds/current_odds/change` en `save_canonical_bookmaker_batches` **ahora**, en vez de esperar a que Fase 5 migre los lectores primero. Para eventos nuevos ingeridos desde este cambio, esos 4 lectores no migrados verán datos incompletos/`NULL` en esas columnas hasta que se implemente la Fase 5. Esto es un adelanto deliberado de la parte de "dejar de escribir" de la Fase 7 — el DDL (`DROP COLUMN`) de esa fase se mantiene sin cambios, para no romper lecturas directas mientras el código legacy termine de retirarse.

**Qué cambió en `market_repository.py::save_canonical_bookmaker_batches`:**
- `MarketChoice` ahora se crea/actualiza como identidad pura (`market_id`, `choice_name`) — ya no recibe `initial_odds=`, `current_odds=` ni `change=`.
- La señal "¿se fijó el initial por primera vez (o se sobreescribió legítimamente)?" — que decide si se agrega un `MarketChoiceSnapshot` de apertura — ya no se lee de `choice.initial_odds` (congelado) sino de la `MarketChoiceQuote` primaria existente (`exchange_side IS NULL`, `exchange_level=0`, mismo `source`), precargada en una sola query para evitar N+1.
- `save_markets_from_response_with_stats` (ruta legacy, `LEGACY_MAINTENANCE_ONLY`) **no se tocó** — sigue escribiendo el mirror de `choices` de forma independiente, tal como antes. Solo el path canónico deja de hacerlo.

**Tests actualizados** para verificar la combinación cross-source contra `MarketChoiceQuote` en vez de `MarketChoice` (mismo comportamiento, distinta tabla de verificación): `tests/test_oddsportal_canonical_ingestion.py` (`test_service_persists_one_canonical_event_batch_with_one_session`, `test_oddsportal_opening_and_oddspapi_current_are_order_independent`, `test_oddsportal_toggle_selects_opening_owner_without_losing_oddspapi_current`). Estas pruebas también se cambiaron para invocar Oddspapi vía `save_canonical_bookmaker_batches` (el path real de producción) en vez de la ruta legacy directa, porque solo el path canónico escribe quotes.

## 4. Principio de diseño

| Tabla | Responsabilidad única | NUNCA debe contener |
|---|---|---|
| `markets` | Identidad del mercado (evento, bookie, mercado, línea) | side, source, precios |
| `market_choices` | Identidad del outcome dentro del mercado | side, source, precios |
| `market_choice_quotes` (**nueva**) | Estado **actual** de un precio: quién lo reporta, qué lado, qué nivel | historial |
| `market_choice_snapshots` | Historial puro append-only | metadatos de identidad repetidos por tick |

`mainLine` y `source` van a `market_choice_quotes`, no a `markets` ni `market_choices`.

## 5. Schema propuesto

### Tabla nueva: `market_choice_quotes`

```sql
CREATE TABLE market_choice_quotes (
    quote_id             BIGSERIAL PRIMARY KEY,
    choice_id            INTEGER NOT NULL REFERENCES market_choices(choice_id) ON DELETE CASCADE,

    source               TEXT NOT NULL,                       -- 'oddspapi', 'oddsportal'
    exchange_side        TEXT,                                 -- NULL | 'back' | 'lay'
    exchange_level       SMALLINT NOT NULL DEFAULT 0,

    main_line            BOOLEAN,
    source_market_id     TEXT,
    source_outcome_id    TEXT,
    bookmaker_outcome_id TEXT,
    source_limit         NUMERIC(12,3),

    initial_odds         NUMERIC(8,3),
    initial_captured_at  TIMESTAMP,
    current_odds         NUMERIC(8,3),
    current_updated_at   TIMESTAMP,
    movement             SMALLINT DEFAULT 0,

    created_at           TIMESTAMP NOT NULL DEFAULT now(),
    updated_at           TIMESTAMP NOT NULL DEFAULT now(),

    -- Constraint plana declarada en el ORM por paridad de introspección;
    -- NO protege por sí sola contra dos filas exchange_side=NULL (ver nota).
    UNIQUE (choice_id, source, exchange_side, exchange_level)
);

CREATE INDEX idx_market_choice_quotes_choice ON market_choice_quotes (choice_id);
CREATE INDEX idx_market_choice_quotes_source ON market_choice_quotes (source);

-- Enforcement real, NULL-safe (mismo patrón que
-- unique_market_per_event_bookie_period_line en `markets`):
CREATE UNIQUE INDEX unique_market_choice_quote_side_null_safe
    ON market_choice_quotes (choice_id, source, COALESCE(exchange_side, ''), exchange_level);
```

> **Decisión (revisada tras Fase 3):** `exchange_side` es `NULL` para bookies sin split back/lay — misma convención "NULL = no aplica" que `Market.choice_group`, en vez de un sentinel string `'single'` (la primera versión implementada). Como `NULL != NULL` en un `UNIQUE` (Postgres y SQLite por igual), la protección real no es la constraint plana de arriba sino el índice funcional `unique_market_choice_quote_side_null_safe` con `COALESCE(exchange_side, '')`, creado en `database.py::_migrate_market_choice_quotes` con un nombre distinto al de la constraint plana (para que el `CREATE ... IF NOT EXISTS` no se salte silenciosamente por colisión de nombre). En tests que solo llaman `manager.create_tables()` (sin `check_and_migrate_schema()`), el índice funcional no se crea — ver `tests/test_market_choice_quote_model.py`.

### `market_choice_snapshots` — versión adelgazada (Fase 6)

La transición necesita un paso intermedio: en Fase 4 se agrega `quote_id` como
FK **nullable** y los writers pasan a escribir simultáneamente `choice_id` y
`quote_id`. El backfill completa `quote_id` para el historial clasificable. La
Fase 5 ya lee identidad y metadata a través de `quote_id`; la Fase 6 únicamente
endurece el contrato (`quote_id NOT NULL`) y elimina `choice_id` y las columnas
de identidad redundantes.

```sql
CREATE TABLE market_choice_snapshots (
    snapshot_id          BIGSERIAL PRIMARY KEY,
    quote_id             BIGINT NOT NULL REFERENCES market_choice_quotes(quote_id) ON DELETE CASCADE,
    odds_value           NUMERIC(8,3) NOT NULL,
    collected_at         TIMESTAMP NOT NULL DEFAULT now(),
    source_collected_at  TIMESTAMP,
    exchange_size        NUMERIC(18,3)
);

CREATE INDEX idx_market_choice_snapshots_quote_collected ON market_choice_snapshots (quote_id, collected_at);
```

### Contrato de payload compartido entre providers

Hoy Oddspapi emite `choice["exchangeQuotes"]` (lista de `{side, level, price, size}`, ver `best_exchange_quotes` en `modules/oddspapi/exchange_quotes.py`), y OddsPortal no emite nada equivalente (produce Back/Lay como markets separados). Para que **un solo writer** entienda a ambos providers, se define un contrato único:

```python
# infrastructure/persistence/repositories/market/exchange_quote_payload.py (NUEVO)
@dataclass(frozen=True, slots=True)
class ExchangeQuotePayload:
    side: Optional[str] = None    # None | 'back' | 'lay'
    level: int = 0
    price: float | None = None
    size: float | None = None
    main_line: bool | None = None

    def as_dict(self) -> dict: ...
```

Ambos adapters (Oddspapi y OddsPortal) deben producir `choice["quotes"]: list[dict]` con esta forma. Se renombra la clave de wire `exchangeQuotes` → `quotes` (ver [§7](#7-tabla-de-renombresreemplazos)).

## 6. Estructura de módulos propuesta

### 6.1. Unificación de adapters — decisión revisada al implementar

Al empezar Fase 2 se encontró que la unificación "literal" (los 3 adapters devuelven `CanonicalOddsResponse` tipado) tenía un blast radius mucho mayor de lo estimado en el diseño original: `OddspapiMarketAdapter.from_odds_response` y `SofaScoreMarketAdapter.from_daily_odds_entry`/`from_dropping_odds_map_entry` no solo los usa `market_odds_ingestion_service.py`, también:

- `tests/oddspapi/test_market_adapter.py` (~1044 líneas, decenas de asserts sobre `adapted["bookmakers"][0]["markets"][0]["choices"]`)
- `tests/test_oddspapi_market_adapter_mainline.py`
- `modules/jobs/daily_discovery/odds_parser.py` (usa `normalized.get("markets")`)
- `scripts/maintenance/verify_market_ingestion_adapters.py`, `scripts/development/pre_start_odds_simulation.py`, `scripts/maintenance/test_oddspapi_local_ingestion.py`

Reescribir el `return` de esos dos adapters a dataclass rompía todo eso en la misma fase. Se decidió con el usuario (ver transcript) una alternativa de **mismo objetivo, menor blast radius**:

**Decisión implementada:** los 3 adapters **mantienen su forma de retorno actual** (`dict` para Oddspapi/SofaScore, dataclasses para OddsPortal). La unificación ocurre en `market_odds_ingestion_service.py`: cada método de guardado arma un `bookmaker_batches: list[{"bookie_id": int, "markets": list[dict]}]` a partir de la salida de su adapter, y **los 3 llaman al mismo `MarketRepository.save_canonical_bookmaker_batches(event_id, bookmaker_batches, source=...)`**. Esto sí se completó:

- `save_from_oddsportal_data` — ya llamaba a `save_canonical_bookmaker_batches` desde antes de este refactor (sin cambios).
- `save_from_oddspapi_response` — antes hacía un loop por bookmaker llamando a `save_markets_from_response_with_stats` una vez por bookmaker resuelto; ahora arma `bookmaker_batches` (uno por bookmaker resuelto) y hace **una sola llamada agregada** a `save_canonical_bookmaker_batches`.
- `_save_normalized` (usado por `save_from_sofascore_response`, `save_from_dropping_odds_map_entry`, `save_from_daily_odds_entry`) — antes llamaba a `save_markets_from_response_with_stats` con `bookie_id=1` fijo; ahora envuelve `normalized_response["markets"]` en `[{"bookie_id": 1, "markets": markets}]` y llama a `save_canonical_bookmaker_batches`.

**No implementado en esta fase** (queda pendiente, no bloquea el resto del refactor): el módulo `canonical_odds_payload.py` con `CanonicalChoicePayload`/`CanonicalMarketPayload`/`CanonicalBookmakerPayload`/`CanonicalOddsResponse` extraídas de `oddsportal_market_adapter.py`, y la reescritura de `OddspapiMarketAdapter`/`SofaScoreMarketAdapter` para devolver esas dataclasses. Si se retoma, debe ir en un PR propio que incluya la migración de los ~6 archivos listados arriba (adapters + tests + scripts), no mezclado con cambios de persistencia.

`MarketRepository.save_canonical_bookmaker_batches` sí se extendió (ver [§6.2](#62-subpaquete-de-persistencia)) para soportar todo lo que antes solo soportaba `save_markets_from_response_with_stats`: `choice["exchangeQuotes"]` (back/lay), `sourceMarketId`/`sourceOutcomeId`/`bookmakerOutcomeId`/`mainLine`/`limit`, y conversión de timezone para timestamps UTC de Oddspapi (`_uses_utc_source_timestamps`). Por eso el shim ya no hace falta: **`save_markets_from_response_with_stats` ya no tiene ningún call site en el pipeline de ingesta en vivo** (`market_odds_ingestion_service.py`); solo lo siguen llamando `save_markets_from_response` (sin `_with_stats`) y los scripts de mantenimiento que dependen de él (ver [§3](#3-inventario-actual-código-vivo-vs-legacy)).

### 6.2. Subpaquete de persistencia

`infrastructure/persistence/repositories/market_repository.py` hoy tiene ~1300 líneas y mezcla: resolución de identidad de mercado, upsert de choices, upsert de exchange quotes, snapshots, y queries de lectura. El plan original separaba **4** colaboradores nuevos (`MarketIdentityResolver`, `MarketChoiceWriter`, `MarketChoiceQuoteWriter`, `MarketChoiceSnapshotWriter`). Al implementar Fase 2 solo se extrajo el que era estrictamente necesario para el fix de back/lay; los otros 3 quedan pendientes (ver abajo):

```
infrastructure/persistence/repositories/market/
    __init__.py
    exchange_quote_payload.py        # ExchangeQuotePayload      (Fase 1, hecho)
    odds_movement.py                 # compute_movement()        (Fase 1, hecho — extraído de _choice_change)
    market_choice_quote_writer.py    # MarketChoiceQuoteWriter   (Fase 2, hecho)
    market_identity_resolver.py      # MarketIdentityResolver           — PENDIENTE, sigue inline en save_canonical_bookmaker_batches
    market_choice_writer.py          # MarketChoiceWriter               — PENDIENTE, sigue inline en save_canonical_bookmaker_batches
    market_choice_snapshot_writer.py # MarketChoiceSnapshotWriter       — PENDIENTE, sigue inline en save_canonical_bookmaker_batches
    market_read_queries.py           # MarketReadQueries         (Fase 5A)
    legacy_writer.py                 # funciones muertas, marcadas, para borrar en Fase 8

infrastructure/persistence/repositories/market_repository.py
    # Sigue siendo la clase real (no una fachada todavía). save_canonical_bookmaker_batches
    # ahora delega en MarketChoiceQuoteWriter vía el método privado
    # _upsert_choice_quotes(session, choice, choice_data, source, write_policy,
    # initial_odds, initial_captured_at, current_odds, current_captured_at),
    # que escribe una quote side-agnostic (exchange_side=NULL) con los mismos valores efectivos que
    # MarketChoice.initial_odds/current_odds, y una quote adicional por cada
    # entrada de choice_data["exchangeQuotes"] (back/lay). El resto de la
    # identidad de mercado y el upsert de MarketChoice/MarketChoiceSnapshot
    # sigue siendo código inline de save_canonical_bookmaker_batches, tal
    # como estaba antes de esta fase.
```

`market_repository.py` **no desaparece** en este refactor — la conversión completa a fachada (extrayendo `MarketIdentityResolver`/`MarketChoiceWriter`/`MarketChoiceSnapshotWriter`) queda como trabajo pendiente, no bloqueante para las fases siguientes. No romperá los ~19 archivos que hacen `from infrastructure.persistence.repositories.market_repository import MarketRepository` cuando se haga, porque los métodos públicos no cambian de firma.

## 7. Tabla de renombres/reemplazos

| Actual | Ubicación | Acción | Nuevo nombre / destino |
|---|---|---|---|
| `MarketRepository._choice_change` | `market_repository.py:101` | **Extraer** (función pura, sin cambio de comportamiento) | `market/odds_movement.py::compute_movement(explicit_change, initial_odds, current_odds)` |
| Closure `market_identity(...)` dentro de `save_canonical_bookmaker_batches` | `market_repository.py:574` | **Promover** a método de clase, reusar en ambos writers | `market/market_identity_resolver.py::MarketIdentityResolver.identity_key(...)` |
| Bloque de resolución/creación de `Market` (existing_market query + create) | `market_repository.py:245-273` y `:564-693` (duplicado en ambos métodos) | **Unificar** | `MarketIdentityResolver.resolve_or_create(session, event_id, bookie_id, market_name, market_period, choice_group, is_live)` |
| Bloque de upsert de `MarketChoice` (initial/current/change) | `market_repository.py:280-371` y `:699-796` (duplicado) | **Unificar** | `market/market_choice_writer.py::MarketChoiceWriter.upsert(session, market, choice_data, write_policy)` |
| Bloque inline de `exchangeQuotes`/`exchange_side` en snapshots | `market_repository.py:389-496` | **Reemplazar por completo** (no legacy, se borra) | `market/market_choice_quote_writer.py::MarketChoiceQuoteWriter.upsert(session, choice, quotes, source, write_policy)` |
| Inserts de `MarketChoiceSnapshot` (2 bloques casi idénticos) | `market_repository.py:396-496` y `:800-835` | **Unificar** | `market/market_choice_snapshot_writer.py::MarketChoiceSnapshotWriter.append(session, quote, odds_value, source_collected_at)` |
| `MarketRepository.save_markets_from_response_with_stats` | `market_repository.py:165` | **Hecho (Fase 2):** se retiran sus 3 call sites en `market_odds_ingestion_service.py`; queda documentada como `LEGACY_MAINTENANCE_ONLY` (no shim, sigue con su propio código porque scripts de mantenimiento la llaman directo vía `save_markets_from_response`) | sin cambio de nombre; eliminar en Fase 8 solo si se migran también los scripts de mantenimiento |
| `MarketRepository.save_canonical_bookmaker_batches` | `market_repository.py:531` | **Extendido (Fase 2):** único writer real para las 3 fuentes; agrega `_upsert_choice_quotes` (usa `MarketChoiceQuoteWriter`). Los otros 3 colaboradores (`MarketIdentityResolver`, `MarketChoiceWriter`, `MarketChoiceSnapshotWriter`) **no** se extrajeron todavía — sigue código inline | sin cambio de nombre público |
| `MarketRepository.get_external_markets_for_event` | `market_repository.py:894` | **Reemplazar** lógica de `source` "adivinado" desde snapshot (líneas 940-951) | `market/market_read_queries.py::MarketReadQueries.get_external_market_quotes_for_event(event_id)` — quote-aware, agrupa por `(bookie, choice, source, exchange_side)` |
| `MarketRepository.get_oddsportal_markets_for_event` (alias) | `market_repository.py:980` | **Legacy, borrar** | eliminar en Fase 8 |
| `MarketRepository.get_markets_for_event`, `get_market_count`, `delete_markets_for_event` | `market_repository.py:882,983,1223` | **Mover** (sin cambio de nombre/firma) | `market/market_read_queries.py` / `market/market_lifecycle.py` |
| `MarketRepository.save_markets_from_oddsportal` | `market_repository.py:1047` | **Legacy muerto — borrar directo**, ya no tiene call sites | eliminar en Fase 8 |
| `MarketRepository._save_oddsportal_market` | `market_repository.py:1007` | **Legacy muerto — borrar directo** | eliminar en Fase 8 |
| `MarketRepository._build_choice_payload` | `market_repository.py:996` | **Legacy muerto — borrar directo** | eliminar en Fase 8 |
| `OddsPortalMarketAdapter.from_match_odds_data` | `oddsportal_market_adapter.py` | **Hecho (Fase 3):** split en orquestador + 2 métodos | `from_match_odds_data` ahora solo resuelve el canonical_type por extracción y delega |
| — (extraído de la sección "regular bookies", sin cambio de comportamiento) | `oddsportal_market_adapter.py::_build_regular_bookmaker_markets` | **Hecho (Fase 3)** | `OddsPortalMarketAdapter._build_regular_bookmaker_markets(extraction, *, canonical_key, canonical_type, requires_choice_group, reference_time, markets_by_bookie, diagnostics)` |
| — (extraído y corregido, aquí vivía el bug) | `oddsportal_market_adapter.py::_build_betfair_exchange_markets` | **Hecho (Fase 3):** corrige `choice_group=side_group` (`"Back"`/`"Lay"`/`"Back 2.5"`) → `choice_group=choice_group_line` (solo línea, igual que cualquier otro bookie) + `exchange_side='back'/'lay'` por choice. Back y lay del mismo outcome ahora comparten **un solo** `CanonicalMarketPayload`/`Market` | `OddsPortalMarketAdapter._build_betfair_exchange_markets(extraction, *, canonical_key, canonical_type, requires_choice_group, reference_time, markets_by_bookie, diagnostics)` |
| `CanonicalChoicePayload` | `oddsportal_market_adapter.py:16` | **Hecho (Fase 3):** campo nuevo agregado | `exchange_side: str | None = None`; `as_repository_dict()` emite `"exchangeSide"` cuando no es `None`. (El wrapper `"quotes": [...]` de `ExchangeQuotePayload` **no** se implementó — quedó atado a la unificación de adapters diferida, ver [§6.1](#61-unificación-de-adapters--decisión-revisada-al-implementar)) |
| `MarketRepository.save_canonical_bookmaker_batches` (dedupe de choices) | `market_repository.py::save_canonical_bookmaker_batches` | **Hecho (Fase 3):** `seen_choice_names` (dedupe solo por `name`) → `seen_choice_sides` (dedupe por `(name, exchangeSide)`) — si no, la fila *lay* de OddsPortal se descartaba como "duplicado" de *back* dentro del mismo market | sin cambio de nombre público, solo de la clave de dedupe interna |
| `MarketRepository._upsert_choice_quotes` | `market_repository.py::_upsert_choice_quotes` | **Extendido (Fase 3):** nueva rama para `choice_data["exchangeSide"]` (singular, OddsPortal: un choice_data = un lado) además de la rama existente para `choice_data["exchangeQuotes"]` (lista, Oddspapi: ambos lados en un choice_data) | sin cambio de nombre público |
| `choice["exchangeQuotes"]` (clave de wire) | `oddspapi_market_adapter.py:266`, `market_repository.py` (`save_markets_from_response_with_stats` y `_upsert_choice_quotes`) | **Pendiente** — el rename a `choice["quotes"]` estaba atado a la reescritura de adapters que se difirió (ver [§6.1](#61-unificación-de-adapters--decisión-revisada-al-implementar)); `save_canonical_bookmaker_batches` hoy lee `exchangeQuotes` tal cual, sin renombrar. OddsPortal usa una clave distinta y ya definitiva, `exchangeSide` (singular, no lista), porque un `choice_data` de OddsPortal siempre es un solo lado — no tiene el mismo problema de forma que Oddspapi | `choice["quotes"]` (ver `ExchangeQuotePayload`) — queda para cuando se retome la unificación de adapters; `exchangeSide` de OddsPortal no se toca |
| `modules/alerts/alerts_formatter/odds_alert.py::_format_external_markets_section` | líneas 220-278 | **Ajustar agrupación**: hoy agrupa por `source` adivinado y trata `choice_group=Back/Lay` como caso especial (líneas 274-278) | agrupar por `(bookie_name, exchange_side)` real proveniente de quotes; eliminar el caso especial de `choice_group` para Betfair |
| `modules/pillars/odds_trajectory_context.py::_get_bookie_container` | línea ~343 | **Extender clave de agrupación** — hoy `bookie_key = bookie_name` colisionaría back/lay bajo el mismo bookie | `bookie_key = (bookie_name, exchange_side)` cuando `exchange_side IS NOT NULL` |
| `PRE_START_ODDS_TRAJECTORY_VIEW_SQL` (CTE `snapshot_context`) | `models.py:1066-1102` | **Reescribir JOIN**: hoy lee `mcs.source`, `mcs.exchange_side`, `mcs.main_line` directo de snapshot | `JOIN market_choice_quotes mcq ON mcq.quote_id = mcs.quote_id`, seleccionar esos campos desde `mcq.*` |

## 8. Fases de implementación

Cada fase es un PR independiente. Cuando una fase tiene subfases explícitas
(5A–5D), cada subfase es un PR desplegable y reversible; no mezclar subfases.

### Fase 0 — Preparación y freeze de contrato

- [ ] Congelar este documento como fuente de verdad; cualquier cambio de diseño lo actualiza primero.
- [ ] Confirmar con búsqueda global (ya hecho, ver [§3](#3-inventario-actual-código-vivo-vs-legacy)) que `save_markets_from_oddsportal`, `_save_oddsportal_market`, `_build_choice_payload`, `get_oddsportal_markets_for_event` no tienen call sites nuevos antes de tocarlos.
- [ ] Marcar esas 4 funciones con comentario `# LEGACY_DEAD_CODE: sin call sites activos, ver docs/refactors/db-schema-odds-refactor.md §8 Fase 8` (cambio de 1 línea, cero riesgo, deja rastro para cualquier dev que llegue a la rama).
- [ ] Snapshot de evento(s) de referencia (ej. `158955`) para regresión manual en cada fase.

**Criterio de aceptación:** documento aprobado + comentarios de marcado mergeados.

---

### Fase 1 — Tabla y repositorio nuevos: `market_choice_quotes`

**Objetivo:** crear la tabla y sus colaboradores sin tocar ningún writer/reader existente.

**Archivos nuevos:**
- `infrastructure/persistence/models.py` — modelo `MarketChoiceQuote` (agregar al archivo existente, junto a `MarketChoiceSnapshot`).
- `infrastructure/persistence/repositories/market/exchange_quote_payload.py` — `ExchangeQuotePayload`.
- `infrastructure/persistence/repositories/market/market_choice_quote_writer.py` — `MarketChoiceQuoteWriter` (sin uso todavía, solo implementado + testeado).
- `infrastructure/persistence/repositories/market/odds_movement.py` — `compute_movement()` extraída de `MarketRepository._choice_change` (puede moverse ya, es 1:1, bajo riesgo).

**Archivos modificados:**
- `infrastructure/persistence/database.py` — creación de tabla/índices (seguir patrón de `OddspapiMainlineOutcomeCache`).

**Criterio de aceptación:**
- Tabla creada en dev/staging.
- Tests de modelo: índice funcional NULL-safe (`COALESCE(exchange_side, '')`), FK cascade.
- `compute_movement` cubierta con tests unitarios que replican los casos actuales de `_choice_change`.

---

### Fase 2 — Unificar writer único (COMPLETADA con alcance revisado)

**Objetivo logrado:** `MarketOddsIngestionService` llama siempre a `MarketRepository.save_canonical_bookmaker_batches` para las 3 fuentes, y esa función ahora escribe también `MarketChoiceQuote` (single + back/lay). Ver [§6.1](#61-unificación-de-adapters--decisión-revisada-al-implementar) para el porqué del cambio de alcance frente al diseño original (los adapters Oddspapi/SofaScore **no** se reescribieron a `CanonicalOddsResponse`).

**Archivos nuevos:**
- `infrastructure/persistence/repositories/market/market_choice_quote_writer.py` — `MarketChoiceQuoteWriter.upsert(...)`: upsert idempotente por `(choice_id, source, exchange_side, exchange_level)`, tolera llegada parcial de `initial_*`/`current_*` en llamadas independientes (el escenario original del bug: initial a T-120, current a T-5).
- `tests/test_market_choice_quote_writer.py` — tests unitarios del writer (creación, merge parcial, no-overwrite de initial por defecto, overwrite cuando la política lo permite, back/lay como filas independientes).
- `tests/test_save_canonical_bookmaker_batches_quotes.py` — test de integración que reproduce el bug original: `exchangeQuotes` de Oddspapi ahora queda como estado "current" consultable en `MarketChoiceQuote`, no solo en `market_choice_snapshots`.

**Archivos modificados:**
- `infrastructure/persistence/repositories/market_repository.py::save_canonical_bookmaker_batches` — parsea timestamps con `_uses_utc_source_timestamps(source)` (paridad con Oddspapi, antes solo lo hacía `save_markets_from_response_with_stats`); después de cada upsert de `MarketChoice`, llama al nuevo método privado `_upsert_choice_quotes(...)` (ver [§6.2](#62-subpaquete-de-persistencia)).
- `infrastructure/persistence/repositories/market_repository.py::save_markets_from_response`/`save_markets_from_response_with_stats` — sin cambios de comportamiento; se documentan como `LEGACY_MAINTENANCE_ONLY` (no `LEGACY_DEAD_CODE`: siguen vivos para `scripts/legacy/process_null_seasons_legacy_event_odds.py`, `scripts/legacy/extract_historical_results_legacy_event_odds.py` y `scripts/sport_seasons_processing.py`).
- `modules/odds_ingestion/market_odds_ingestion_service.py::save_from_oddspapi_response` — el loop por bookmaker ya no llama a `save_markets_from_response_with_stats` una vez por bookmaker; arma `bookmaker_batches` y hace una única llamada a `save_canonical_bookmaker_batches`.
- `modules/odds_ingestion/market_odds_ingestion_service.py::_save_normalized` — ídem, envuelve `{"bookie_id": 1, "markets": markets}` y llama a `save_canonical_bookmaker_batches` en vez de `save_markets_from_response_with_stats`.
- `tests/oddspapi/test_ingestion_service.py` — 2 tests (`test_commit_uses_source_resolution_and_skips_unresolved_bookmaker`, `test_commit_passes_canonical_market_payload_to_repository`) actualizados para mockear `save_canonical_bookmaker_batches` en vez del método retirado.
- `tests/pillars/test_market_odds_ingestion_service.py` — ídem para `test_save_from_dropping_odds_map_entry_normalizes_source_and_logs_it` (este test ya fallaba antes por un assert de log obsoleto, no relacionado; se deja documentado, no se corrige en este PR).

**No implementado en esta fase** (ver [§6.1](#61-unificación-de-adapters--decisión-revisada-al-implementar) y [§6.2](#62-subpaquete-de-persistencia)):
- `canonical_odds_payload.py`, reescritura de `OddspapiMarketAdapter`/`SofaScoreMarketAdapter` a `CanonicalOddsResponse`.
- Extracción de `MarketIdentityResolver`, `MarketChoiceWriter`, `MarketChoiceSnapshotWriter` (siguen inline en `save_canonical_bookmaker_batches`).

**Criterio de aceptación (cumplido):**
- `market_odds_ingestion_service.py` ya no llama a `save_markets_from_response_with_stats` en ningún punto.
- `MarketChoiceQuoteWriter` cubierto con tests unitarios + test de integración de extremo a extremo sobre `save_canonical_bookmaker_batches`.
- Suite completa de tests de mercado/odds corrida antes/después vía `git stash`: los únicos tests que cambiaron de resultado fueron los 3 con mocks del método retirado (ya corregidos); las fallas restantes son pre-existentes en la rama (`Full-time` vs `Full Time`, `DROP TABLE ... CASCADE` en SQLite, candidate matcher, etc.), no causadas por este cambio.

---

### Fase 3 — OddsPortal: split del adapter y fix Back/Lay (COMPLETADA)

**Objetivo logrado:** `OddsPortalMarketAdapter` ya no usa `choice_group` para el lado del exchange. Back y lay del mismo outcome ahora comparten **un solo** `Market`/`MarketChoice` (igual identidad que cualquier otro bookie para esa línea), y se disambiguan únicamente vía `MarketChoiceQuote.exchange_side`.

**Archivos modificados:**
- `modules/odds_ingestion/adapters/oddsportal_market_adapter.py`:
  - `CanonicalChoicePayload` — nuevo campo `exchange_side: str | None = None`; `as_repository_dict()` emite `"exchangeSide"` solo cuando no es `None` (los bookies normales nunca lo llevan).
  - `from_match_odds_data` se dividió en orquestador (resuelve `canonical_type` por extracción, delega) + `_build_regular_bookmaker_markets(...)` (idéntico al código anterior, sin cambios de comportamiento) + `_build_betfair_exchange_markets(...)` (aquí vivía el bug: generaba `choice_group=side_group` con valores `"Back"`, `"Lay"`, `"Back 2.5"`, `"Lay 2.5"`, forzando dos `Market` sin relación entre sí y sin relación con la línea real. Ahora genera `choice_group=choice_group_line` — solo la línea, `None` para 1X2/Home-Away — y cada `CanonicalChoicePayload` lleva su propio `exchange_side='back'`/`'lay'`. Los choices de ambos lados se concatenan en **una** tupla dentro del mismo `CanonicalMarketPayload`).
- `infrastructure/persistence/repositories/market_repository.py::save_canonical_bookmaker_batches`:
  - El dedupe de choices por market (`seen_choice_names`, solo por `name`) pasó a `seen_choice_sides`, dedupe por `(name, exchangeSide)`. Sin este cambio, la fila *lay* de OddsPortal se descartaba silenciosamente como "duplicado" de *back* dentro del mismo market (ambas se llaman `"1"`/`"x"`/`"2"`).
  - `_upsert_choice_quotes` ganó una rama nueva: si `choice_data["exchangeSide"]` es `"back"`/`"lay"` (forma singular de OddsPortal: un `choice_data` = un lado), escribe directo a esa quote y no también a la fila side-agnostic (`exchange_side=NULL`) (no existe un precio side-agnostic que espejar ahí). La rama existente para `choice_data["exchangeQuotes"]` (lista, forma de Oddspapi: ambos lados en un mismo `choice_data`) no cambió.
- `tests/test_oddsportal_canonical_ingestion.py::test_adapter_shares_one_market_between_betfair_back_and_lay` (renombrado desde `test_adapter_transports_exchange_tooltip_current_for_back_and_lay`) — reescrito para afirmar el comportamiento correcto: 1 market, `choice_group=None`, 6 choices (`1/x/2` × `back/lay`), cada uno con su `exchange_side`.
- `tests/test_oddsportal_betfair_back_lay_quotes.py` (nuevo) — integración end-to-end real (adapter → `MarketOddsIngestionService.save_from_oddsportal_data` → `MarketRepository`): un evento con Betfair Exchange termina en **un** `Market`, **un** `MarketChoice` por outcome, y dos `MarketChoiceQuote` (`back`, `lay`) con sus propios `initial_odds` independientes.

**Nota operativa — `.gitignore`:** el repo ignora `test_*.py` y `tests/*` por defecto, con excepciones explícitas (`!tests/test_x.py`). Los tests nuevos de Fase 1/2/3 (`test_market_choice_quote_model.py`, `test_odds_movement.py`, `test_market_choice_quote_writer.py`, `test_save_canonical_bookmaker_batches_quotes.py`, `test_oddsportal_betfair_back_lay_quotes.py`) no aparecían en `git status` hasta que se agregaron sus excepciones. **Cualquier archivo `tests/test_*.py` nuevo en las fases siguientes necesita su propia línea `!tests/test_nombre.py` en `.gitignore` o quedará fuera de cualquier commit sin aviso.**

**Criterio de aceptación (cumplido):**
- Ingesta de un evento nuevo con Betfair en OddsPortal no crea `Market` con `choice_group IN ('Back','Lay')` — hay un solo `Market` por línea, igual que para cualquier otro bookie.
- Un mismo `MarketChoice` recibe quotes `(oddsportal, back)` y `(oddsportal, lay)` sin colisión (probado en `test_oddsportal_betfair_back_lay_quotes.py`); la convergencia con `(oddspapi, back/lay)` sobre el mismo choice ya estaba probada desde Fase 2 (`test_save_canonical_bookmaker_batches_quotes.py`).
- `ODDSPORTAL_OPENING_ONLY_POLICY` se sigue respetando a nivel de quote (nunca `current_odds`/snapshots desde oddsportal) — verificado explícitamente en el test nuevo.
- Suite completa corrida antes/después vía `git stash`: mismo set de 77 fallas pre-existentes (sin relación con este cambio) en ambos casos; +15 tests nuevos en verde.

---

### Fase 4 — Backfill de datos históricos

**Objetivo:** poblar quotes para el histórico, enlazar cada snapshot
clasificable con su quote y preparar el cutover de lectores sin borrar ni
reescribir todavía las columnas legacy.

#### Invariante de transición

Fase 5 necesita hacer `JOIN market_choice_snapshots.quote_id →
market_choice_quotes.quote_id`, pero el schema slim original no agregaba esa FK
hasta Fase 6. Por eso Fase 4 empieza con una migración *expand-only*:

```sql
ALTER TABLE market_choice_snapshots
    ADD COLUMN quote_id BIGINT NULL
    REFERENCES market_choice_quotes(quote_id) ON DELETE CASCADE;

CREATE INDEX idx_market_choice_snapshots_quote_collected
    ON market_choice_snapshots (quote_id, collected_at, snapshot_id);
```

Durante Fases 4 y 5 conviven ambas FKs. Un snapshot nuevo debe cumplir
`mcs.choice_id = mcq.choice_id`; un snapshot histórico OddsPortal puede apuntar
desde su `choice_id` legacy a un quote cuyo `choice_id` ya es el canónico. La
vista de Fase 5 toma mercado/choice desde `mcq.choice_id`, no desde
`mcs.choice_id`. Fase 6 elimina finalmente la FK legacy.

**Archivo nuevo:**
- `scripts/maintenance/backfill_market_choice_quotes.py` (seguir convención de `scripts/maintenance/backfill_sofascore_choice_names_and_groups.py`).

**Archivos modificados:**
- `infrastructure/persistence/models.py` — `MarketChoiceSnapshot.quote_id`
  nullable + relación `quote`; mantener `choice_id` y el resto de columnas.
- `infrastructure/persistence/database.py` — migración aditiva e índice.
- `infrastructure/persistence/repositories/market/market_choice_snapshot_writer.py`
  — `append(...)` recibe el `MarketChoiceQuote` y hace dual-write de ambas FKs.
- `infrastructure/persistence/repositories/market/market_choice_quote_writer.py`
  — expone el upsert idempotente que reutiliza el backfill.

#### Orden de despliegue

1. **Expandir schema**: agregar FK/índice nullable, sin cambiar lectores.
2. **Activar dual-write**: todo snapshot nuevo sale con `quote_id`; rechazar en
   tests cualquier append del writer nuevo que no reciba quote.
3. **Preflight**: ejecutar el script en `--dry-run`, producir reporte de
   clasificación/conflictos y resolver ambiguos no permitidos.
4. **Backfill por lotes**: upsert de quotes y update de `mcs.quote_id`, por rango
   de `event_id`, con transacción por lote.
5. **Verificación**: repetir dry-run; el segundo pase debe proyectar cero
   mutaciones y los contadores deben cuadrar antes de habilitar Fase 5.

#### Reglas deterministas de clasificación

La identidad final siempre es `(choice_id canónico, source, exchange_side,
exchange_level)`. No se infieren datos cuando existe más de una interpretación
posible.

| Campo | Regla de backfill, en orden de precedencia |
|---|---|
| `source` | `LOWER(TRIM(mcs.source))`; si no existe y `bookie_id = 1`, `sofascore`; si el market legacy codifica Back/Lay, `oddsportal`; cualquier otro externo sin evidencia queda `ambiguous_source` |
| `exchange_side` | valor normalizado de `mcs.exchange_side`; si falta, side extraído de `Market.choice_group`; en otro caso `NULL` |
| `exchange_level` | `COALESCE(mcs.exchange_level, 0)`; nunca negativo |
| choice destino | para markets normales, el choice actual; para OddsPortal legacy, el choice con el mismo nombre dentro del market canónico destino |
| market destino OddsPortal | misma identidad `(event_id, bookie_id, market_name, market_period, línea, is_live)`; la línea sale de `(?i)^(Back|Lay)(?:\s+(.+))?$` y queda `NULL` cuando no hay sufijo |

Si el market/choice canónico destino no existe, el script puede crearlo solo
cuando la identidad y el nombre del choice son unívocos. Diferencias de case,
dos targets posibles, source desconocido, side inválido o metadata de lineage
contradictoria se reportan y no se mutan. En Fase 4 **no se borran** markets ni
choices Back/Lay legacy; quedan disponibles para rollback y se eliminan en la
limpieza de datos posterior, explícita y separada (no es requisito de Fase 8),
después de una ventana de observación.

El script inspecciona también `MarketChoice` sin snapshots. Esos valores solo
se migran cuando las reglas anteriores prueban ownership (SofaScore o market
OddsPortal Back/Lay). Un choice externo normal sin lineage queda
`ambiguous_choice_state`; para resolverlo se admite un `--resolution-file`
versionado con `market_id/choice_id → source/side/level`, nunca una heurística
oculta en código.

#### Reglas de consolidación de estado

- Un quote ya escrito por el pipeline nuevo gana frente al backfill. El script
  solo completa campos `NULL`, excepto `current_odds`, que puede avanzar si el
  candidato tiene un `current_updated_at` estrictamente posterior.
- `initial_odds` sale de `MarketChoice.initial_odds` únicamente cuando el
  ownership source/side es unívoco. No se usa “primer snapshot” como opening
  ficticio.
- `current_odds` sale del último snapshot del bucket por
  `COALESCE(source_collected_at, collected_at), collected_at, snapshot_id`; si
  no hay snapshots, puede usar `MarketChoice.current_odds` solo con ownership
  unívoco.
- `initial_captured_at`/`current_updated_at` usan tiempo de fuente cuando
  existe y `collected_at` como fallback.
- `main_line`, IDs de lineage y `source_limit` se toman del snapshot más
  reciente con valor no nulo; `exchange_size` permanece en cada snapshot. Dos
  valores de identity metadata
  distintos para el mismo quote incrementan `metadata_conflicts` y requieren
  revisión; no se elige silenciosamente.
- Cada snapshot clasificable recibe exactamente un `quote_id`. El script no
  cambia su `odds_value`, timestamps ni `choice_id` legacy.

#### Contrato operativo del script

- `--dry-run` por defecto y `--commit` explícito.
- Filtros `--event-id`, `--event-id-min`, `--event-id-max`, `--source` y
  `--batch-size`, más `--resolution-file` para decisiones manuales auditables;
  reanudar equivale a repetir el último rango porque el proceso es idempotente.
- Advisory lock de PostgreSQL compartido con otras migraciones de markets;
  pausar jobs de ingesta durante el primer commit de producción.
- Salida humana + `--output-json` con, como mínimo:
  `snapshots_scanned`, `snapshots_linked`, `legacy_choice_states_scanned`,
  `quotes_inserted`, `quotes_updated`, `legacy_markets_mapped`,
  `canonical_markets_created`, `ambiguous_source`, `ambiguous_choice_state`,
  `ambiguous_target`, `metadata_conflicts`, `invalid_side_or_level`,
  `unlinked_snapshots` y `unmigrated_choice_states`.
- Código de salida no cero si existen ambiguos fuera de un allowlist explícito.
- Backup lógico previo; el rollback de aplicación no exige deshacer el
  backfill porque todos los cambios son aditivos y los lectores legacy siguen
  intactos.

#### Tests

- Unitarios de parser `Back`/`Lay`, source/side/level y precedencia temporal.
- Integración en SQLite/Postgres para: market destino existente, creación
  segura de destino, back+lay sobre el mismo choice, conflicto ya existente,
  metadata contradictoria y snapshot no clasificable.
- Idempotencia real: ejecutar `--commit` dos veces y verificar que el segundo
  pase no inserta ni actualiza filas.
- Concurrencia: una quote más nueva creada por el writer entre lotes no puede
  ser degradada por el backfill.

**Criterio de aceptación:**
- 100% de snapshots creados después del dual-write tienen `quote_id` no nulo y
  `mcs.choice_id = mcq.choice_id`.
- 100% de snapshots históricos clasificables tienen un único `quote_id`; los
  no clasificables aparecen nominados en el reporte, no ocultos en un contador.
- Todo estado legacy que consumen alertas/trajectory/dual process tiene quote o
  resolución explícita; `unmigrated_choice_states = 0` en el scope del cutover.
- No existen quotes duplicados por la constraint de identidad y no se perdió ni
  cambió ningún snapshot (`COUNT`, `MIN/MAX(snapshot_id)` y checksum por lote).
- Segundo pase idempotente con cero mutaciones.
- Staging compara eventos de Fase 0, incluido `158955`, y producción cuenta con
  backup, ventana de baja actividad y reporte JSON archivado.

---

### Fase 5 — Migrar lectores a quotes

**Objetivo:** hacer de `market_choice_quotes` la única fuente de estado e
identidad para lecturas de odds, con cutover observable y reversible. No se
eliminan todavía columnas legacy.

**Precondición bloqueante:** Fase 4 desplegada, dual-write activo, cero
snapshots clasificables sin `quote_id` y cero estados legacy leídos en
producción sin quote. Si no se cumple, los lectores quote-aware perderían
historia/estado y Fase 5 no comienza.

#### Política de lectura común

- Cada serie se identifica por `(market_id, choice_id, source, exchange_side,
  exchange_level)`; `bookie_name` es presentación, no identidad.
- Los consumidores actuales son top-of-book. Cuando hay varios niveles, se
  elige el menor `exchange_level` existente por
  `(choice_id, source, exchange_side)`. No se mezclan niveles en una trayectoria.
- Nunca combinar opening de una fuente con current de otra. Un
  `(oddsportal, back)` y un `(oddspapi, back)` son series distintas aunque
  compartan market, choice y bookie.
- `initial_odds`, `current_odds`, `movement`, `source`, side y lineage salen de
  `MarketChoiceQuote`; `MarketChoice` aporta solo identidad del outcome.

#### Fase 5A — Query de alertas y contrato de salida

- Crear
  `market/market_read_queries.py::MarketReadQueries.get_external_market_quotes_for_event`.
  Devuelve un bloque por `(market_id, source, exchange_side)` y choices con
  `choice_id`, `quote_id`, `initial`, `current`, `movement`; el nivel elegido
  también queda explícito para diagnóstico.
- Mantener temporalmente
  `MarketRepository.get_external_markets_for_event` como fachada para no romper
  `odds_alert.py` y `alert_pipeline.py` en el primer deploy.
- Agregar un comparador legacy-vs-quotes por evento. Paridad exacta es exigible
  para mercados `single` de una sola fuente; diferencias Betfair se clasifican
  como esperadas solo si consisten en separar source/side, nunca en perder
  choices o precios.

#### Fase 5B — Alertas

- `modules/alerts/alerts_formatter/odds_alert.py` agrupa primero por `source` y
  después por market/bookie/side. El label Back/Lay usa `exchange_side`, no
  `choice_group`; `choice_group` queda reservado para líneas como `2.5`.
- Actualizar ambos call sites (`odds_alert.py` y
  `modules/jobs/pre_start_check_job/alert_pipeline.py`) y sus tests de snapshot
  textual. OddsPortal opening-only conserva `opening→N/A`; Oddspapi muestra su
  propio opening/current bajo otra sección, sin fabricar una trayectoria mixta.
- El orden estable es market, período, línea, bookie, `NULL/back/lay`.

#### Fase 5C — Trajectory y pillars

- Crear primero `v_pre_start_odds_trajectory_quotes` en paralelo a la vista
  actual. Su CTE parte de `mcs JOIN mcq ON mcq.quote_id = mcs.quote_id`, y luego
  une `market_choices` mediante `mcq.choice_id`. Lee `source`, side, level,
  lineage, `main_line`, límites e `initial_odds` desde `mcq`.
- El join a `event_source_mappings` y `market_source_mappings` usa
  `mcq.source`/`mcq.source_market_id`, no columnas del snapshot.
- `OddsTrajectoryPoint` agrega `quote_id`, `source`, `exchange_side` y
  `exchange_level`. El `ROW_NUMBER` se particiona por
  `(event_id, quote_id, target_minute)`, no solo por bookie/choice, para que
  back, lay y providers no compitan por el mismo slot.
- `BookieOddsTrajectory` expone source/side/level y
  `ChoiceOddsTrajectory` expone `quote_id`. La clave interna de bookie/serie es
  `(bookie_id o bookie_name, source, exchange_side, exchange_level)`, no solo
  `bookie_name` ni `(bookie_name, exchange_side)`.
- `drift_engine.py` puede seguir leyendo `choice.initial_odds`, pero ese valor
  ya proviene de `mcq.initial_odds`; agregar regresión que impida mezclar el
  opening OddsPortal con snapshots Oddspapi.
- Comparar ambas vistas en staging y después sustituir
  `v_pre_start_odds_trajectory` por la definición quote-aware, manteniendo el
  nombre público para no cambiar el pipeline de pillars.

#### Fase 5D — Lectores restantes y cierre

- Migrar `build_dual_process_event_odds_view_sql`: para `bookie_id = 1` usar la
  quote explícita `(source='sofascore', exchange_side IS NULL, nivel
  preferido)` y buscar el último snapshot por `quote_id`, no por `choice_id`.
  Verificar también la materialized view que depende de ella.
- Actualizar scripts activos de desarrollo/mantenimiento que filtran metadata
  directamente en `MarketChoiceSnapshot`; las herramientas legacy pueden
  quedar excluidas solo con comentario y owner explícitos.
- Añadir guard estático
  `scripts/maintenance/check_no_legacy_odds_reads.py`. Debe fallar ante nuevas
  lecturas de `MarketChoice.initial_odds/current_odds/change` o de metadata de
  `MarketChoiceSnapshot`, con allowlist temporal y fechada para writers y
  migraciones.

#### Estrategia de cutover y rollback

1. Desplegar query/vista quote-aware sin cambiar consumidores.
2. Ejecutar comparación sobre los eventos de Fase 0 y una muestra por deporte,
   provider, market y side.
3. Cortar alertas y trajectory por subfase; observar al menos un ciclo completo
   del pre-start job y métricas de filas/latencia/errores.
4. Ante regresión, volver a la fachada/vista legacy; dual-write y backfill
   permanecen porque son compatibles hacia atrás.
5. Solo después de la ventana acordada se autoriza Fase 6. Fase 5 no elimina
   columnas, índices ni código de rollback.

**Criterio de aceptación:**
- Paridad exacta para eventos no exchange/single-source y diferencias esperadas
  documentadas para Betfair multi-source.
- Evento `158955` produce un market/choice canónico con series separadas para
  `(oddsportal|oddspapi) × (back|lay)`, sin colisiones ni side en `choice_group`.
- Ningún ranking de trajectory colapsa quotes distintas; tests cubren dos
  sources y dos sides en el mismo target minute.
- Alertas, pillars, drift y dual process pasan suites de regresión y un ciclo de
  staging; conteos y latencia no degradan fuera del presupuesto acordado.
- El guard de lecturas legacy queda verde. Las únicas referencias permitidas
  están en writers de compatibilidad, backfill y código expresamente agendado
  para Fases 6–7.

---

### Fase 6 — Adelgazar `market_choice_snapshots`

**Objetivo:** migrar a la versión slim (ver [§5](#5-schema-propuesto)) una vez nada dependa de sus columnas redundantes.

**Criterio de aceptación:**
- `market_choice_snapshots.quote_id` puede cambiarse a `NOT NULL` sin encontrar
  snapshots clasificables pendientes; los casos exceptuados están documentados
  y resueltos antes de ejecutar el DDL.
- Todas las queries que filtran/seleccionan `source`/`exchange_side`/`main_line`/etc. en `market_choice_snapshots` pasan a hacerlo vía `JOIN market_choice_quotes`.
- Medir tamaño de tabla/índices antes/después.

---

### Fase 7 — Deprecar columnas legacy en `market_choices`

**Objetivo:** eliminar `MarketChoice.initial_odds`, `current_odds`, `change` una vez todos los lectores (Fase 5) usen quotes.

**Criterio de aceptación:**
- Ningún archivo referencia esas columnas (búsqueda global).
- Migración de schema que las elimina, con ventana de aviso/backup.

---

### Fase 8 — Limpieza de código legacy

**Objetivo:** eliminar en bloque todo lo marcado como legacy a lo largo de las fases anteriores. Esta fase es exclusivamente borrado + verificación, sin nueva funcionalidad.

**A eliminar:**
- `MarketRepository.save_markets_from_oddsportal`, `_save_oddsportal_market`, `_build_choice_payload` (marcados en Fase 0, confirmados sin uso).
- `MarketRepository.save_markets_from_response_with_stats` como shim (Fase 2) — una vez `market_odds_ingestion_service.py` llama directo a `save_canonical_bookmaker_batches`.
- `MarketRepository.get_oddsportal_markets_for_event` (alias).
- Cualquier bloque de código dejado con comentario `# LEGACY_*` durante Fases 0-7 que ya no tenga call sites.
- `infrastructure/persistence/repositories/market_repository.py` — evaluar si queda algo propio o si se convierte 100% en imports de `market/*` (decisión de equipo, no bloqueante).

**Criterio de aceptación:**
- Búsqueda global de `LEGACY_` sin resultados pendientes de esta lista.
- Suite de tests completa en verde.
- `save_markets_from_response` (sin stats) se mantiene documentado como "solo para scripts de mantenimiento", explícitamente fuera de esta limpieza.

---

## 9. Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| Fase 2 desincroniza el comportamiento de Oddspapi al converger sobre `save_canonical_bookmaker_batches` | Test de paridad automatizado antes de retirar el shim |
| Backfill (Fase 4) corre sobre datos en producción con jobs activos | Ventana de baja actividad o pausa del pre-start job durante el backfill |
| Fase 5 intenta unir por `quote_id` antes de que exista/esté poblado | Fase 4 agrega la FK nullable, activa dual-write y exige coverage antes del cutover |
| Source de un estado legacy externo no puede probarse | Clasificar como ambiguo; resolver con archivo auditable, nunca adivinar por nombre de bookie |
| Migrar lectores (Fase 5) rompe alertas en producción | Cada PR compara output contra eventos de referencia de Fase 0 |
| Ranking de trajectory vuelve a colisionar providers o Back/Lay | Particionar por `quote_id` y testear 2 sources × 2 sides en el mismo minuto |
| Duplicado de fila `exchange_side IS NULL` por migración incompleta | Índice funcional `unique_market_choice_quote_side_null_safe` (`COALESCE`) + test explícito que llama `check_and_migrate_schema()` |
| `market_choices` deja de escribirse antes de Fase 5 ([§3.2](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado)): `odds_alert.py`/`odds_trajectory_context.py`/`drift_engine.py`/dual-process view ven datos incompletos para eventos nuevos | Riesgo aceptado explícitamente; Fase 5 pasa a ser prioritaria, no opcional — no debe quedar pendiente indefinidamente |
| Borrar código en Fase 8 antes de tiempo | Fase 8 solo se ejecuta después de que Fases 2-7 estén en producción sin incidentes |

## 10. Mapa rápido de archivos

**Modelos / schema:**
- `infrastructure/persistence/models.py` — `Market`, `MarketChoice`, `MarketChoiceSnapshot`, `MarketChoiceQuote` (nuevo), vista `v_pre_start_odds_trajectory`.
- `infrastructure/persistence/market_write_policy.py`.
- `infrastructure/persistence/database.py`.

**Writers (nuevo subpaquete):**
- `infrastructure/persistence/repositories/market/market_identity_resolver.py`
- `infrastructure/persistence/repositories/market/market_choice_writer.py`
- `infrastructure/persistence/repositories/market/market_choice_quote_writer.py`
- `infrastructure/persistence/repositories/market/market_choice_snapshot_writer.py`
- `infrastructure/persistence/repositories/market/exchange_quote_payload.py`
- `infrastructure/persistence/repositories/market/odds_movement.py`
- `infrastructure/persistence/repositories/market/market_read_queries.py`
- `infrastructure/persistence/repositories/market_repository.py` (fachada)

**Adapters:**
- `modules/odds_ingestion/adapters/canonical_odds_payload.py` (PENDIENTE — no creado, ver [§6.1](#61-unificación-de-adapters--decisión-revisada-al-implementar))
- `modules/odds_ingestion/adapters/sofascore_market_adapter.py`
- `modules/odds_ingestion/adapters/oddspapi_market_adapter.py`
- `modules/odds_ingestion/adapters/oddsportal_market_adapter.py`
- `modules/oddspapi/exchange_quotes.py`
- `modules/odds_ingestion/market_odds_ingestion_service.py`

**Lectores:**
- `modules/alerts/alerts_formatter/odds_alert.py`
- `infrastructure/persistence/repositories/odds_trajectory_repository.py`
- `modules/pillars/odds_trajectory_context.py`
- `modules/pillars/pillar_4/drift_engine/drift_engine.py`

**Tests relevantes existentes:**
- `tests/test_oddsportal_canonical_ingestion.py` (ejercita `OddsPortalMarketAdapter` + `save_canonical_bookmaker_batches` + `save_markets_from_response_with_stats` — se actualizará en cada fase; `test_adapter_shares_one_market_between_betfair_back_and_lay` prueba el fix de Fase 3 a nivel adapter)
- `tests/test_oddsportal_hover_parser.py`
- `tests/test_market_choice_quote_model.py` (Fase 1)
- `tests/test_odds_movement.py` (Fase 1)
- `tests/test_market_choice_quote_writer.py` (Fase 2)
- `tests/test_save_canonical_bookmaker_batches_quotes.py` (Fase 2, integración end-to-end del fix de back/lay vía Oddspapi)
- `tests/test_oddsportal_betfair_back_lay_quotes.py` (Fase 3, integración end-to-end del fix de back/lay vía OddsPortal — adapter real + servicio + repositorio)

**Nota:** todos los `tests/test_*.py` nuevos deben agregarse también como excepción en `.gitignore` (ver nota operativa en Fase 3) o quedan fuera de git silenciosamente.
