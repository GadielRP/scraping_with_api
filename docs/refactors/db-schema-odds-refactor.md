# Refactor de schema de odds — separación de responsabilidades

**Branch:** `refactor/db-schema-odds-refactor`
**Estado:** Fases 1–5 implementadas; Fase 5 quedó cerrada en el commit
`67b1d3f`. Fase 4b/4c terminó en producción el 2026-08-12
(`algorithm_version=4b.7`, `events_selected=0`) y los tres lectores públicos de
la copia PostgreSQL local están en `quotes`. Fase 6 inició el 2026-08-12 sobre
esa copia y quedó completada localmente con migración slim fail-closed,
compactación y postflight integral ejecutados únicamente por el CLI.
La ventana operativa de Fase 5 continúa siendo requisito para repetir el DDL en
el servidor. Evidencia de Fase 5 en [§12](#12-implementación-de-fase-5-mapa-y-deuda-de-cleanup)
y ejecución de Fase 6 en [§13](#13-implementación-de-fase-6-snapshots-slim).
**Alcance:** `infrastructure/persistence/models.py`, `infrastructure/persistence/repositories/market_repository.py`, `infrastructure/persistence/market_write_policy.py`, `modules/odds_ingestion/adapters/*`, `modules/oddspapi/exchange_quotes.py`, lectores de trajectory/alerts/pillars.

## Índice

- [1. Problema](#1-problema)
- [2. Causa raíz](#2-causa-raíz)
- [3. Inventario actual: código vivo vs legacy](#3-inventario-actual-código-vivo-vs-legacy)
  - [3.1. Estado actual (post Fase 3)](#31-estado-actual-post-fase-3)
  - [3.2. Decisión: `market_choices` deja de escribirse antes de Fase 5 (riesgo aceptado)](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado)
  - [3.3. Decisión revisada: se conserva `source` en la identidad de `market_choice_quotes`](#33-decisión-revisada-se-conserva-source-en-la-identidad-de-market_choice_quotes)
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
    - [Plan ejecutable de Fase 4b](./db-schema-odds-refactor-phase-4b.md)
  - [Fase 5 — Migrar lectores a quotes](#fase-5--migrar-lectores-a-quotes)
    - [Plan ejecutable detallado de Fase 5](./db-schema-odds-refactor-phase-5.md)
  - [Fase 6 — Adelgazar `market_choice_snapshots`](#fase-6--adelgazar-market_choice_snapshots)
  - [Fase 7 — Deprecar columnas legacy en `market_choices`](#fase-7--deprecar-columnas-legacy-en-market_choices)
  - [Fase 8 — Limpieza de código legacy](#fase-8--limpieza-de-código-legacy)
- [9. Riesgos y mitigaciones](#9-riesgos-y-mitigaciones)
- [10. Mapa rápido de archivos](#10-mapa-rápido-de-archivos)
- [11. Handoff: continuar desde Fase 4b](#11-handoff-continuar-desde-fase-4b)
  - [11.1. Qué ya está hecho (no rehacer)](#111-qué-ya-está-hecho-no-rehacer)
  - [11.2. APIs y puntos de entrada a reutilizar](#112-apis-y-puntos-de-entrada-a-reutilizar)
  - [11.3. Inventario de lectores aún en columnas legacy](#113-inventario-de-lectores-aún-en-columnas-legacy)
  - [11.4. Inventario de metadata de identidad aún en snapshots](#114-inventario-de-metadata-de-identidad-aún-en-snapshots)
  - [11.5. Patrones de scripts de backfill a copiar](#115-patrones-de-scripts-de-backfill-a-copiar)
  - [11.6. Orden sugerido de PRs y smoke checks](#116-orden-sugerido-de-prs-y-smoke-checks)
  - [11.7. Gotchas operativos](#117-gotchas-operativos)
- [12. Implementación de Fase 5: mapa y deuda de cleanup](#12-implementación-de-fase-5-mapa-y-deuda-de-cleanup)

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

Esas rutas están marcadas `LEGACY_MAINTENANCE_ONLY`: ya delegan quotes en `MarketChoiceQuoteWriter` y snapshots en `MarketChoiceSnapshotWriter`, por lo que no pueden romper el lineage mientras sigan invocables. Siguen siendo legacy porque duplican resolución de markets/choices y escriben el mirror congelado de `MarketChoice`; sus scripts deben migrarse y ambos métodos deben eliminarse en Fase 8.

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
- `save_markets_from_response_with_stats` sigue escribiendo el mirror de `choices` y por eso permanece `LEGACY_MAINTENANCE_ONLY`, pero sus quotes y snapshots pasan por los mismos writers SRP que el path canónico. No se conserva una implementación legacy paralela para esas dos tablas.

**Tests actualizados** para verificar la combinación cross-source contra `MarketChoiceQuote` en vez de `MarketChoice` (mismo comportamiento, distinta tabla de verificación): `tests/test_oddsportal_canonical_ingestion.py` (`test_service_persists_one_canonical_event_batch_with_one_session`, `test_oddsportal_opening_and_oddspapi_current_are_order_independent`, `test_oddsportal_toggle_selects_opening_owner_without_losing_oddspapi_current`). Estas pruebas también se cambiaron para invocar Oddspapi vía `save_canonical_bookmaker_batches` (el path real de producción) en vez de la ruta legacy directa, porque solo el path canónico escribe quotes.

### 3.3. Decisión revisada: se conserva `source` en la identidad de `market_choice_quotes`

**Contexto.** Al operar la Fase 4b apareció `ambiguous_source` en volumen para
bet365 (`bookie_id=3`, mapeado a `oddspapi` **y** `oddsportal` en
`bookie_source_mappings`): el histórico nunca grabó `source` a nivel de fila
para ese bookie (todo `market_choice_snapshots.source IS NULL` antes de
`2026-07-30`), y con dos providers posibles no había forma de saber a cuál
pertenecía cada fila. Esto, sumado a evidencia real de un evento con OddsPortal
+ Oddspapi para el mismo bet365/mercado terminando en 4 quotes en vez de 2
([ver ejemplo completo](#1-problema)-style, evento `169158`), abrió la
pregunta: ¿fue un error separar `market_choice_quotes` por `source`? ¿Debería
colapsarse una fila por `(choice_id, exchange_side, exchange_level)` sin
importar el provider, como hacía el mirror de `MarketChoice`?

**Corrección a la causa raíz descrita en §3.2.** La motivación original de
separar por `source` **no** fue evitar que dos providers asíncronos se
pisaran — ese problema ya estaba resuelto a nivel de campo con
`MarketWritePolicy` sobre una fila *compartida* (`MarketChoice`), probado por
`test_oddsportal_opening_and_oddspapi_current_are_order_independent` **antes**
de que existieran las quotes. La causa raíz real es la de [§2](#2-causa-raíz):
`MarketChoice` asume 1 outcome = 1 precio, falso para exchanges (`back`/`lay`
con múltiples `exchange_level`), y cada provider metía esa dimensión donde no
cabía. Al resolverlo con una tabla nueva, `source` terminó en la clave porque
cada provider trae sus propios IDs de correlación
(`source_market_id`/`source_outcome_id`/`bookmaker_outcome_id`) y su propia
numeración de `exchange_level`, no porque separar por provider fuera un
objetivo en sí mismo.

**Decisión (mantenida tras revisión):** no se quita `source` de la identidad.
Motivo concreto, no el de "evitar carreras": `source_market_id`,
`source_outcome_id`, `bookmaker_outcome_id`, `source_limit`,
`initial_captured_at` y `current_updated_at` son columnas de **un solo valor
por fila**. Si dos providers compartieran una fila, el segundo `upsert`
pisaría silenciosamente los IDs de correlación y el timestamp del primero —
exactamente el patrón de "meter cosas donde no caben" que [§2](#2-causa-raíz)
señala como el defecto original, solo que ahora en `market_choice_quotes` en
vez de en `snapshots`/`choice_group`. Además, para exchanges, el
`exchange_level` de un provider no siempre corresponde 1:1 con el de otro
(uno puede no reportar niveles en absoluto), así que fusionar en la
*identidad de escritura* sería ambiguo justo donde más se necesita precisión.

**Consecuencia para Fase 5 (lectores).** Lo que sí se acepta como error es
que, tras dejar de escribir el mirror de `MarketChoice` ([§3.2](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado)),
no quedó ningún reemplazo — ni en escritura ni en lectura — para el "un
número por bookie" que ese mirror daba gratis combinando OddsPortal (`initial`)
+ Oddspapi (`current`). La Fase 5B (ver más abajo) queda ajustada: para
quotes **sin side/level múltiple** (bookies normales, no exchange), el lector
debe fusionar por prioridad de campo entre sources — el equivalente en
lectura de `MarketWritePolicy` — en vez de mostrarlas como series separadas
sin relación. Para exchanges (`exchange_side`/`exchange_level` múltiples)
se mantiene la separación por source, porque ahí no hay correspondencia
segura entre niveles de distintos providers.

**Backfill (Fase 4b).** Para el histórico donde `source` nunca se grabó (bet365
y cualquier otro bookie mapeado a más de un provider), la ambigüedad se
resuelve — cuando es posible — con evidencia dura de `MarketWritePolicy` en
vez de con una fila fusionada: ver
[Fase 4b §6.1](./db-schema-odds-refactor-phase-4b.md#61-source).

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
    quote_id             SERIAL PRIMARY KEY,
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
    quote_id             INTEGER NOT NULL REFERENCES market_choice_quotes(quote_id) ON DELETE CASCADE,
    odds_value           NUMERIC(8,3) NOT NULL,
    collected_at         TIMESTAMP NOT NULL DEFAULT now(),
    source_collected_at  TIMESTAMP,
    source_limit         NUMERIC(12,3),
    exchange_size        NUMERIC(18,3)
);

CREATE INDEX idx_market_choice_snapshots_quote_collected
    ON market_choice_snapshots (quote_id, collected_at DESC, snapshot_id DESC);
```

> `quote_id` es `INTEGER`, no `BIGINT`: el modelo ya desplegado de
> `MarketChoiceQuote.quote_id` usa `Integer`. La FK nueva debe conservar el
> mismo tipo físico. `source_limit` y `exchange_size` se mantienen porque son
> valores del tick; la identidad estable (`source`, side, level, lineage y
> `main_line`) sí se obtiene de la quote.

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

`infrastructure/persistence/repositories/market_repository.py` sigue mezclando resolución de identidad, upsert de choices, orquestación y queries de lectura, pero ya no construye quotes ni snapshots directamente. `MarketChoiceQuoteWriter` y `MarketChoiceSnapshotWriter` son los dos puntos de escritura SRP; `MarketIdentityResolver` y `MarketChoiceWriter` siguen pendientes:

```
infrastructure/persistence/repositories/market/
    __init__.py
    exchange_quote_payload.py        # ExchangeQuotePayload      (Fase 1, hecho)
    odds_movement.py                 # compute_movement()        (Fase 1, hecho — extraído de _choice_change)
    market_choice_quote_writer.py    # MarketChoiceQuoteWriter   (Fase 2, hecho)
    market_identity_resolver.py      # MarketIdentityResolver           — PENDIENTE, sigue inline en save_canonical_bookmaker_batches
    market_choice_writer.py          # MarketChoiceWriter               — PENDIENTE, sigue inline en save_canonical_bookmaker_batches
    market_choice_snapshot_writer.py # MarketChoiceSnapshotWriter       — HECHO (Fase 4a), único append de snapshots
    market_read_queries.py           # MarketReadQueries         (Fase 5A)
    legacy_writer.py                 # funciones muertas, marcadas, para borrar en Fase 8

infrastructure/persistence/repositories/market_repository.py
    # Sigue siendo la clase real (no una fachada todavía). save_canonical_bookmaker_batches
    # ahora delega en MarketChoiceQuoteWriter vía el método privado
    # _upsert_choice_quotes(session, choice, choice_data, source, write_policy,
    # initial_odds, initial_captured_at, current_odds, current_captured_at),
    # que escribe una quote side-agnostic (exchange_side=NULL) con los mismos
    # valores efectivos de precio (ya NO escribe el mirror de MarketChoice —
    # ver §3.2), y una quote adicional por cada entrada de
    # choice_data["exchangeQuotes"] (back/lay) o por choice_data["exchangeSide"]
    # (forma singular de OddsPortal). El resto de la identidad de mercado y el
    # _upsert_choice_quotes retorna un mapa por (side, level). La orquestación
    # selecciona la quote exacta y delega todo append a
    # MarketChoiceSnapshotWriter; no hay MarketChoiceSnapshot(...) inline.
```

`market_repository.py` **no desaparece** todavía: conserva la transacción y la orquestación. La conversión completa a fachada solo necesita extraer `MarketIdentityResolver`/`MarketChoiceWriter` y mover lecturas; la escritura de snapshots ya salió por completo del archivo.

## 7. Tabla de renombres/reemplazos

| Actual | Ubicación | Acción | Nuevo nombre / destino |
|---|---|---|---|
| `MarketRepository._choice_change` | `market_repository.py:101` | **Extraer** (función pura, sin cambio de comportamiento) | `market/odds_movement.py::compute_movement(explicit_change, initial_odds, current_odds)` |
| Closure `market_identity(...)` dentro de `save_canonical_bookmaker_batches` | `market_repository.py:574` | **Promover** a método de clase, reusar en ambos writers | `market/market_identity_resolver.py::MarketIdentityResolver.identity_key(...)` |
| Bloque de resolución/creación de `Market` (existing_market query + create) | `market_repository.py:245-273` y `:564-693` (duplicado en ambos métodos) | **Unificar** | `MarketIdentityResolver.resolve_or_create(session, event_id, bookie_id, market_name, market_period, choice_group, is_live)` |
| Bloque de upsert de `MarketChoice` (initial/current/change) | `market_repository.py:280-371` y `:699-796` (duplicado) | **Unificar** | `market/market_choice_writer.py::MarketChoiceWriter.upsert(session, market, choice_data, write_policy)` |
| Bloque inline de `exchangeQuotes`/`exchange_side` en snapshots | `market_repository.py:389-496` | **Reemplazar por completo** (no legacy, se borra) | `market/market_choice_quote_writer.py::MarketChoiceQuoteWriter.upsert(session, choice, quotes, source, write_policy)` |
| Inserts de `MarketChoiceSnapshot` en paths canónico y legacy | `market_repository.py` → `repositories/market/market_choice_snapshot_writer.py` | **Hecho (Fase 4a):** cero constructores inline; ambos enlazan el tick con la quote exacta y el path canónico hace un solo flush final por batch | `MarketChoiceSnapshotWriter.append(session, quote, odds_value, collected_at, ...)` |
| `MarketRepository.save_markets_from_response_with_stats` | `market_repository.py:165` | **Hecho (Fase 2):** se retiran sus 3 call sites en `market_odds_ingestion_service.py`; queda documentada como `LEGACY_MAINTENANCE_ONLY` (no shim, sigue con su propio código porque scripts de mantenimiento la llaman directo vía `save_markets_from_response`) | sin cambio de nombre; eliminar en Fase 8 solo si se migran también los scripts de mantenimiento |
| `MarketRepository.save_canonical_bookmaker_batches` | `market_repository.py` | **Extendido (Fases 2 y 4a):** orquestador para las 3 fuentes; delega quotes y snapshots a sus writers SRP. `MarketIdentityResolver`/`MarketChoiceWriter` siguen pendientes | sin cambio de nombre público |
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
- `infrastructure/persistence/repositories/market_repository.py::save_markets_from_response`/`save_markets_from_response_with_stats` — se documentan como `LEGACY_MAINTENANCE_ONLY` (no `LEGACY_DEAD_CODE`: siguen vivos para scripts); en Fase 4a su escritura de quotes/snapshots se delegó a los writers compartidos sin conservar otro writer legacy.
- `modules/odds_ingestion/market_odds_ingestion_service.py::save_from_oddspapi_response` — el loop por bookmaker ya no llama a `save_markets_from_response_with_stats` una vez por bookmaker; arma `bookmaker_batches` y hace una única llamada a `save_canonical_bookmaker_batches`.
- `modules/odds_ingestion/market_odds_ingestion_service.py::_save_normalized` — ídem, envuelve `{"bookie_id": 1, "markets": markets}` y llama a `save_canonical_bookmaker_batches` en vez de `save_markets_from_response_with_stats`.
- `tests/oddspapi/test_ingestion_service.py` — 2 tests (`test_commit_uses_source_resolution_and_skips_unresolved_bookmaker`, `test_commit_passes_canonical_market_payload_to_repository`) actualizados para mockear `save_canonical_bookmaker_batches` en vez del método retirado.
- `tests/pillars/test_market_odds_ingestion_service.py` — ídem para `test_save_from_dropping_odds_map_entry_normalizes_source_and_logs_it`; el log del batch conserva los conteos semánticos de markets, choices y snapshots.

**No implementado en esta fase** (ver [§6.1](#61-unificación-de-adapters--decisión-revisada-al-implementar) y [§6.2](#62-subpaquete-de-persistencia)):
- `canonical_odds_payload.py`, reescritura de `OddspapiMarketAdapter`/`SofaScoreMarketAdapter` a `CanonicalOddsResponse`.
- Extracción de `MarketIdentityResolver` y `MarketChoiceWriter` (siguen inline en `save_canonical_bookmaker_batches`). `MarketChoiceSnapshotWriter` se extrajo en Fase 4a.

**Criterio de aceptación (cumplido):**
- `market_odds_ingestion_service.py` ya no llama a `save_markets_from_response_with_stats` en ningún punto.
- `MarketChoiceQuoteWriter` cubierto con tests unitarios + test de integración de extremo a extremo sobre `save_canonical_bookmaker_batches`.
- Suite de tests de mercado/odds corrida tras el cambio; los mocks del método retirado y el contrato de observabilidad del batch quedaron actualizados.

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

El plan ejecutable de Fase 4b vive en
[db-schema-odds-refactor-phase-4b.md](./db-schema-odds-refactor-phase-4b.md).
Esta sección conserva las decisiones arquitectónicas y criterios globales del
refactor; el documento dedicado define archivos, contratos, orden de commits,
tests y runbook de implementación.

**Estado actual:** Fase 4 **completada** (4a + 4b herramienta + 4c ejecución
en producción, 2026-08-12). `MarketChoiceSnapshot.quote_id`, su FK real y el
índice temporal existen; snapshots nuevos y el historial clasificable quedan
ligados a quotes. Oddspapi con `exchangeQuotes` conserva un tick por
`(side, level)`. La persistencia canónica procesa bookmakers en un solo batch
con un único flush final. El backfill `4b.7` aplicó fill-only, purges
(bookies permitidos, null-mainline, Back/Lay, choice_states ambiguos) y
cerró con scope vacío. Lo que **seguía** tras 4a (ya hecho en 4b/4c) era:
1. Política temporal/fill-only que impida degradar estado nuevo al procesar
   candidatos históricos.
2. Script de backfill histórico + reporte de cobertura antes del cutover.
3. Ejecución y verificación de coverage/idempotencia en staging y producción.

**Objetivo:** poblar quotes para el histórico, enlazar cada snapshot
clasificable con su quote y preparar el cutover de lectores sin borrar ni
reescribir todavía las columnas legacy.

#### Decisiones no negociables

1. Un snapshot representa un tick de **una quote exacta**. Su identidad es
   `(choice_id canónico, source, exchange_side, exchange_level)`; no basta con
   enlazarlo a “alguna quote del choice”.
2. `MarketChoiceQuoteWriter.upsert` se ejecuta antes del append sobre el
   `quote_index` precargado por el orquestador. El mapa de quotes exactas
   retornado por `_upsert_choice_quotes` selecciona el instrumento y
   `MarketChoiceSnapshotWriter.append` realiza toda la escritura histórica.
3. No hay excepción para la ruta `LEGACY_MAINTENANCE_ONLY`: también hace
   upsert → snapshot ligado dentro de la misma unidad de trabajo. Dejarla crear filas con
   `quote_id=NULL` haría imposible el `NOT NULL` de Fase 6.
4. El backfill nunca degrada una quote más nueva. El writer actual reemplaza
   `current_odds` sin comparar timestamps; antes de usarlo para histórico se
   agrega una política de merge temporal explícita y testeada.
5. En exchange, una serie explícita Back/Lay domina a la quote redundante
   `exchange_side=NULL` que Oddspapi conserva por compatibilidad. No se crean
   snapshots side-agnostic cuando el payload contiene `exchangeQuotes`. **Dejar
   de escribir** esa quote `NULL` en la ingesta Oddspapi Betfair y/o borrarla
   del histórico es trabajo **diferido** (post Fase 5 / limpieza dedicada) —
   ver [§11.7 gotcha 10](#117-gotchas-operativos). El backfill (4b) debe
   *clasificar* filas `NULL` históricas, no eliminarlas ni decidir el corte
   del writer live.

#### Checklist completado (PR 4a — expand schema + dual-write)

Completado **antes** del script de backfill, como cambio aditivo y reversible:

1. Agregar `MarketChoiceSnapshot.quote_id = Column(Integer, ForeignKey(...),
   nullable=True)`, relación `snapshot.quote` y relación inversa
   `quote.snapshots` en `models.py`. Mantener temporalmente `choice_id` y todas
   las columnas legacy.
2. Extender el migrador existente
   `database.py::_migrate_market_choice_snapshot_lineage()` —sin crear otro
   método— y ejecutarlo **después** de `_migrate_market_choice_quotes()`:
   - agregar la columna nullable con tipo `INTEGER`;
   - crear/verificar la FK real en bases existentes (el migrador genérico solo
     agrega columnas y no garantiza constraints);
   - crear `idx_market_choice_snapshots_quote_collected` sobre
     `(quote_id, collected_at DESC, snapshot_id DESC)`;
   - fallar la inicialización si la migración no queda aplicada; no ocultar el
     error y continuar con un schema parcial.
3. Extraer `MarketChoiceSnapshotWriter.append` a
   `repositories/market/market_choice_snapshot_writer.py`. Recibe una
   `MarketChoiceQuote` asociada a la misma sesión —puede estar pending—, deriva
   de ella `choice_id`, source, side, level y metadata estable, y solo acepta
   como argumentos los valores del tick.
4. Hacer que `_upsert_choice_quotes` devuelva el mapa
   `(exchange_side, exchange_level) → MarketChoiceQuote` en lugar de descartar
   lo retornado por `MarketChoiceQuoteWriter.upsert`. El orquestador precarga
   todas las quotes de los choices existentes en un `quote_index`; cada upsert
   reutiliza ese índice sin ejecutar SELECT. Después enlaza los snapshots a las
   quotes aunque estén pending y hace un único `flush()` del grafo completo.
5. **Pendiente para 4b, antes de reutilizar el writer en el backfill:** extender
   `MarketChoiceQuoteWriter` con dos políticas de merge explícitas:
   - **live**: comportamiento del pipeline, pero un current con timestamp más
     viejo no puede pisar otro más nuevo;
   - **backfill-fill-only**: completar NULLs, conservar initial existente y
     avanzar current solo con timestamp estrictamente posterior. Metadata
     estable se completa si falta; una contradicción se devuelve al caller.
6. Retirar todos los constructores `MarketChoiceSnapshot(...)` de
   `market_repository.py` (path canónico y legacy). El único constructor de
   producción queda dentro de `MarketChoiceSnapshotWriter` y siempre escribe
   `choice_id` + `quote_id` a partir de la misma quote.
7. Eliminar el N+1 de quotes y los flushes por choice en
   `save_canonical_bookmaker_batches`: un batch existente tiene un presupuesto
   constante de tres SELECT (`Market`, `MarketChoice` mediante `selectinload` y
   `MarketChoiceQuote`) sin importar cuántos choices contenga. Hay un test de
   integración con 25 choices que fija este contrato.
8. Resolver todos los bookmakers de una respuesta Oddspapi dentro de una sola
   sesión de referencia. La persistencia de odds conserva su propia sesión y
   transacción atómica; por tanto este tramo usa dos unidades de trabajo con
   SRP, no una sesión por bookmaker más otra por persistencia. La resolución
   previa del evento/mapeo y la comprobación posterior del dual-process siguen
   siendo lecturas independientes porque tienen contratos distintos.

**Semántica exacta de snapshots nuevos:**

| Forma del payload | Quote del snapshot de apertura | Quote(s) de current |
|---|---|---|
| Bookie normal | `(source, NULL, 0)` | `(source, NULL, 0)` |
| Oddspapi con `exchangeQuotes` | `(source, back, 0)`, siguiendo la convención legacy existente | una por cada `(source, side, level)` válido; sin duplicado `NULL` |
| OddsPortal con `exchangeSide` | el side explícito | el side explícito; hoy la policy opening-only no persiste snapshots |

Tests de 4a cubren las tres formas, la migración de una tabla SQLite legacy,
ausencia de writes nuevos sin `quote_id` y la invariante
`mcs.choice_id = mcq.choice_id`. El orden temporal fuera de secuencia se prueba
en 4b junto con la política fill-only. Un guard estático en
`test_market_choice_snapshot_writer.py` falla si reaparece un constructor de
snapshot inline en `market_repository.py`.

Solo después de eso: script de backfill (PR 4b).

#### Invariante de transición

Fase 5 necesita hacer `JOIN market_choice_snapshots.quote_id →
market_choice_quotes.quote_id`, pero el schema slim original no agregaba esa FK
hasta Fase 6. Por eso Fase 4 empieza con una migración *expand-only*:

```sql
ALTER TABLE market_choice_snapshots
    ADD COLUMN quote_id INTEGER NULL
    REFERENCES market_choice_quotes(quote_id) ON DELETE CASCADE;

CREATE INDEX idx_market_choice_snapshots_quote_collected
    ON market_choice_snapshots (quote_id, collected_at DESC, snapshot_id DESC);
```

Durante Fases 4 y 5 conviven ambas FKs. Un snapshot nuevo debe cumplir
`mcs.choice_id = mcq.choice_id`; un snapshot histórico OddsPortal puede apuntar
desde su `choice_id` legacy a un quote cuyo `choice_id` ya es el canónico. La
vista de Fase 5 toma mercado/choice desde `mcq.choice_id`, no desde
`mcs.choice_id`. Fase 6 elimina finalmente la FK legacy.

**Archivos nuevos de 4b:**
- `scripts/maintenance/backfill_market_choice_quotes.py` — copiar CLI/contratos
  de `scripts/maintenance/backfill_sofascore_choice_names_and_groups.py`
  (`--dry-run` default, `--commit` explícito, filtros por evento y reporte
  JSON). El script **no llama** `check_and_migrate_schema()` en dry-run: esa
  función puede mutar schema. 4a ya debe estar desplegada; el script hace un
  preflight read-only que verifica columna, FK, índice y versión esperada.
- `modules/odds_ingestion/backfill/market_choice_quote_backfill.py` —
  clasificación pura, contratos y orquestación de las dos pasadas.
- `infrastructure/persistence/repositories/market/market_choice_quote_backfill_repository.py`
  — lecturas set-based y bulk link de snapshots, sin reglas de negocio.
- `infrastructure/persistence/repositories/market/market_choice_quote_merge_policy.py`
  — decisión temporal pura compartida por dry-run y writer.

**Archivos modificados:**
- `infrastructure/persistence/models.py` — `MarketChoiceSnapshot.quote_id`
  nullable + relación `quote`; mantener `choice_id` y el resto de columnas.
- `infrastructure/persistence/database.py` — migración aditiva e índice.
- `infrastructure/persistence/repositories/market_repository.py` —
  ordena upsert de quotes y append de snapshots en ambos paths todavía vivos.
- `infrastructure/persistence/repositories/market/market_choice_snapshot_writer.py`
  — único punto de append después de 4a.
- `infrastructure/persistence/repositories/market/market_choice_quote_writer.py`
  — agregar merge temporal/fill-only y reutilizarlo desde el backfill; no
  inventar otro upsert SQL con reglas divergentes.

#### Orden de despliegue

1. **Expandir schema**: agregar FK/índice nullable, sin cambiar lectores.
2. **Activar lineage obligatorio**: todo snapshot nuevo de cualquier path sale
   con `quote_id`; además se recupera el historial Back/Lay del path canónico.
3. **Preflight**: ejecutar el script en `--dry-run`, producir reporte de
   clasificación/conflictos y resolver ambiguos no permitidos.
4. **Backfill por lotes**: upsert de quotes y update de `mcs.quote_id`, por rango
   de `event_id` con keyset pagination y transacción por lote.
5. **Verificación**: repetir dry-run; el segundo pase debe proyectar cero
   mutaciones y los contadores deben cuadrar antes de habilitar Fase 5.

#### Reglas deterministas de clasificación

La identidad final siempre es `(choice_id canónico, source, exchange_side,
exchange_level)`. No se infieren datos cuando existe más de una interpretación
posible.

| Campo | Regla de backfill, en orden de precedencia |
|---|---|
| `source` | `LOWER(TRIM(mcs.source))`; si falta, source único probado por `source_market_id` + mappings; si el market legacy codifica Back/Lay, `oddsportal`; si `bookie_id = 1`, `sofascore`; si existe un único source distinto en `bookie_source_mappings` para ese bookie, usarlo; en otro caso `ambiguous_source` |
| `exchange_side` | valor normalizado de `mcs.exchange_side`; si falta, side extraído de `Market.choice_group`; para snapshots canónicos Oddspapi creados entre Fase 2 y 4a, usar `back` solo si el choice/bookie está probado como exchange por quotes side-specific o mapping y se cumple el contrato documentado `decimalValue = top back`; en otro caso `NULL` |
| `exchange_level` | `COALESCE(mcs.exchange_level, 0)`; nunca negativo |
| choice destino | para markets normales, el choice actual; para OddsPortal legacy, el choice con el mismo nombre dentro del market canónico destino |
| market destino OddsPortal | misma identidad `(event_id, bookie_id, market_name, market_period, línea, is_live)`; la línea sale de `(?i)^(Back|Lay)(?:\s+(.+))?$` y queda `NULL` cuando no hay sufijo |

Toda evidencia disponible se valida aunque una regla anterior ya haya resuelto
el valor: `mcs.source='oddspapi'` frente a un lineage que solo puede ser
`oddsportal` es `contradictory_evidence`, no “precedencia aplicada”. Si el
market/choice canónico destino no existe, solo puede crearse mediante un único
resolver testeado que use la misma normalización/constraint de identidad del
writer canónico; diferencias de case, dos targets posibles, source desconocido
o side inválido se reportan y no mutan. En Fase 4 **no se borran** markets ni
choices Back/Lay legacy. Tampoco se eliminan mientras `mcs.choice_id` siga
apuntándolos, porque la FK legacy tiene `ON DELETE CASCADE`.

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
- `initial_captured_at` solo se rellena cuando existe evidencia de apertura
  (quote existente o snapshot creado por el bloque de opening legacy). Si el
  initial solo proviene de `MarketChoice`, queda `NULL`; no se inventa una hora.
  `current_updated_at` usa tiempo de fuente y `collected_at` como fallback.
- `main_line` e IDs de lineage son metadata estable: contradicciones para una
  misma quote incrementan `metadata_conflicts`. `source_limit` y
  `exchange_size` son valores variables del tick; la quote conserva el más
  reciente y el snapshot conserva su valor histórico, sin tratarlos como
  conflicto de identidad.
- Cada snapshot clasificable recibe exactamente un `quote_id`. El script no
  cambia su `odds_value`, timestamps ni `choice_id` legacy.

#### Diseño de ejecución y rendimiento

- Congelar primero un scope acotado de event IDs y paginar snapshots por
  keyset de `snapshot_id`; la segunda pasada usa `choice_id`. Nunca usar
  `OFFSET` ni ordenar todo el histórico por event.
- Agregar candidatos por identidad de quote y hacer **un upsert por bucket**,
  no uno por snapshot. Precargar markets/choices/quotes del lote en mapas para
  evitar N+1.
- Enlazar snapshots con `executemany`/bulk update en chunks acotados; no cargar
  relaciones ORM completas. Registrar filas/segundo y duración por lote.
- `--batch-size` limita snapshots, no solo eventos; un evento patológico no
  puede consumir memoria sin límite.
- `--max-events` y `--max-rows` limitan el trabajo total de una ejecución. En
  el servidor de 1 GB, la CLI rechaza scopes no acotados y el histórico se
  completa mediante runs pequeños reanudados por checkpoint.
- El advisory lock evita dos backfills simultáneos. Como el writer live hoy no
  toma ese lock, **pausar ingesta es obligatorio** para el primer `--commit`.
  Un backfill online solo se autoriza después de implementar un upsert
  condicional/locking compartido que gane carreras en base de datos.

#### Contrato operativo del script

- `--dry-run` por defecto y `--commit` explícito.
- Filtros `--event-id`, `--event-id-min`, `--event-id-max`, `--source`,
  `--batch-size`, `--max-events`, `--max-rows` y `--after-snapshot-id`, más
  `--resolution-file` versionado para decisiones manuales auditables;
  reanudar equivale a repetir el último rango porque el proceso es idempotente.
- `--event-id` o al menos uno de `--max-events`/`--max-rows` es obligatorio;
  si ambos límites aparecen, gana el primero que se alcance.
- El resolution file incluye versión, motivo y evidencia por decisión; se
  valida contra IDs existentes y se incluye su checksum en el reporte.
- Salida humana + `--output-json` con, como mínimo:
  `snapshots_scanned`, `snapshots_linked`, `legacy_choice_states_scanned`,
  `quotes_inserted`, `quotes_updated`, `legacy_markets_mapped`,
  `canonical_markets_created`, `ambiguous_source`, `ambiguous_choice_state`,
  `ambiguous_target`, `contradictory_evidence`, `metadata_conflicts`,
  `invalid_side_or_level`, `oddspapi_null_ticks_mapped_to_back`,
  `stale_candidates_ignored`, `unlinked_snapshots`, `unmigrated_choice_states`,
  duración y throughput por lote. Cada rechazo
  incluye `event_id/market_id/choice_id/snapshot_id`, razón y evidencia.
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
- Regresión del path canónico: Oddspapi con back+lay crea dos snapshots ligados
  a dos quote IDs y no un snapshot `exchange_side=NULL` duplicado.
- Merge temporal: una quote más nueva nunca es degradada por un candidato viejo;
  timestamps iguales son no-op determinista.
- Dry-run: cero cambios de datos **y de schema**.

**Criterio de aceptación:**
- 100% de snapshots creados por cualquier writer después de 4a tienen
  `quote_id` no nulo y `mcs.choice_id = mcq.choice_id`.
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

> Plan ejecutable, contratos, gates y orden de PRs:
> [db-schema-odds-refactor-phase-5.md](./db-schema-odds-refactor-phase-5.md).

**Estado al arrancar / urgencia:** por [§3.2](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado)
los lectores de abajo ya ven `NULL`/datos incompletos en
`MarketChoice.initial_odds/current_odds/change` para **eventos nuevos**.
Fase 5 no es opcional ni puede quedar indefinida. Si Fase 4 aún no cerró
coverage histórica, se puede empezar **5A** (query quote-aware + fachada)
sobre eventos que ya tienen quotes del pipeline nuevo, pero solo en modo
`shadow`; el cutover se acota explícitamente a eventos cubiertos o espera el
backfill completo.

**Objetivo:** hacer de `market_choice_quotes` la única fuente de estado e
identidad para lecturas de odds, con cutover observable y reversible. No se
eliminan todavía columnas legacy.

**Precondición bloqueante (cutover completo):** Fase 4 desplegada, dual-write
activo, cero snapshots clasificables sin `quote_id` y cero estados legacy
leídos en producción sin quote. Si no se cumple, los lectores quote-aware
perderían historia/estado — acotar el cutover a eventos post-deploy o
completar backfill primero.

#### Inventario concreto de call sites a migrar

| Prioridad | Archivo | Superficie | Qué lee hoy |
|---|---|---|---|
| P0 | `infrastructure/persistence/repositories/market_repository.py` | `get_external_markets_for_event` | `choice.initial_odds` / `choice.current_odds` + `source` del último snapshot |
| P0 | `modules/alerts/alerts_formatter/odds_alert.py` | `_format_external_markets_section` | consume `initial`/`current` del método anterior |
| P0 | `modules/jobs/pre_start_check_job/alert_pipeline.py` | availability / alert path | llama `get_external_markets_for_event` |
| P0 | `infrastructure/persistence/models.py` | `PRE_START_ODDS_TRAJECTORY_VIEW_SQL` | `mc.initial_odds` + metadata de `mcs.*` |
| P0 | `infrastructure/persistence/repositories/odds_trajectory_repository.py` | `OddsTrajectoryPoint` / query a la vista | propaga `initial_odds`; ranking sin `quote_id` |
| P0 | `modules/pillars/odds_trajectory_context.py` | `build_odds_trajectory_context`, `_get_bookie_container` | `initial_odds`; key = `bookie_name` (colisiona back/lay) |
| P1 | `modules/pillars/pillar_4/drift_engine/drift_engine.py` | `_get_required_inputs` | `choice.initial_odds` vía trajectory context |
| P1 | `infrastructure/persistence/models.py` | `build_dual_process_event_odds_view_sql` | `mc.initial_odds` + `COALESCE(latest.odds_value, mc.current_odds)` |
| P1 | `infrastructure/persistence/repositories/dual_process_odds_repository.py` | `get_event_odds*` | `v_dual_process_event_odds` |
| P1 | `modules/pillars/pillar_4/drift_engine/drift_engine.py` | `_build_choice_result_key` y resultados | la clave no incluye source/side/level y sobrescribiría series |
| P2 | `scripts/development/simulate_pre_start_check.py` | prints diagnósticos | `choice.initial_odds/current_odds/change` |

**No migrar por confusión:** `modules/jobs/pre_start_check_job/odds_extraction.py`
arma `initial_odds`/`current_odds` desde el **payload live de SofaScore API**,
no desde columnas de DB.

#### Política de lectura común

- Cada serie se identifica por `(market_id, choice_id, source, exchange_side,
  exchange_level)`; `bookie_name` es presentación, no identidad.
- Los consumidores actuales son top-of-book. Entre quotes con precio se elige
  el menor `exchange_level` por `(choice_id, source, exchange_side)`. El nivel
  elegido queda visible en el contrato; no se mezclan niveles en una serie.
- Oddspapi conserva hoy una quote `exchange_side=NULL` además de Back/Lay para
  exchange. Si existen sides explícitos para `(choice_id, source)`, los readers
  de presentación/trajectory suprimen esa fila `NULL`; no se muestran tres
  instrumentos para un mercado que realmente tiene dos. La fila se conserva
  durante el rollout para rollback y se audita antes de borrarla.
- Nunca combinar opening de una fuente con current de otra. Un
  `(oddsportal, back)` y un `(oddspapi, back)` son series distintas aunque
  compartan market, choice y bookie.
- `initial_odds`, `current_odds`, `movement`, `source`, side y lineage salen de
  `MarketChoiceQuote`; `MarketChoice` aporta solo identidad del outcome.
- La metadata estable (`source`, side, level, IDs, `main_line`) sale de la
  quote. Los valores históricos del tick (`odds_value`, tiempos,
  `source_limit`, `exchange_size`) salen del snapshot.
- Un reader no puede elegir una quote arbitraria ante empate o datos
  contradictorios: devuelve diagnóstico y omite el bloque afectado.

#### Fase 5A — Query de alertas y contrato de salida

- Crear `market/market_read_models.py` con DTOs/`TypedDict` del contrato y
  `market/market_read_queries.py::get_external_market_quotes_for_event`.
  La query es set-based (`Market → Choice → Quote → Bookie`), sin N+1, y aplica
  top-of-book + supresión del `NULL` redundante en SQL o en una sola capa pura.
- Devuelve un bloque por `(market_id, source, exchange_side)` con:
  `market_id`, identidad de market/bookie, `source`, `exchange_side` y una lista
  de choices con `choice_id`, `quote_id`, `exchange_level`, `initial`,
  `current`, `movement` (`-1/0/1`). Solo incluye quotes con algún precio; el
  read model no mezcla datos con glifos de presentación.
- Mantener `MarketRepository.get_external_markets_for_event` como fachada. El
  modo de lectura es configurable y acotado a este consumidor:
  `legacy | shadow | quotes`. `shadow` devuelve legacy pero ejecuta ambos,
  compara y emite métricas; `quotes` es el cutover reversible.
- El comparador clasifica diferencias como `equal`, `expected_source_split`,
  `expected_side_split`, `expected_frozen_legacy`, `missing_quote`,
  `missing_choice`, `price_mismatch` o `unexpected_duplicate`. Las cuatro
  últimas bloquean el cutover. Paridad exacta solo se exige al histórico
  single-source anterior al stop-write; para filas nuevas el mirror legacy
  congelado no es un oracle válido.
- Tests de query verifican orden estable, una sola consulta principal, choices
  incompletos, empate de level y que Betfair Oddspapi expone Back/Lay sin la
  quote `NULL` duplicada.

#### Fase 5B — Alertas

- **Ajustado tras [§3.3](#33-decisión-revisada-se-conserva-source-en-la-identidad-de-market_choice_quotes):**
  para quotes sin side/level múltiple (`exchange_side IS NULL`, bookies
  normales), `get_external_market_quotes_for_event` (Fase 5A) debe exponer un
  valor **fusionado por prioridad de campo entre sources**, no una lista de
  bloques sin relación — es el equivalente en lectura de `MarketWritePolicy`:
  `initial = COALESCE(oddsportal.initial, oddspapi.initial, sofascore.initial)`,
  `current = COALESCE(oddspapi.current, sofascore.current, oddsportal.current)`.
  La prioridad es configurable por campo, no hardcodeada a "OddsPortal siempre
  gana"; debe poder ajustarse por deporte/bookie si la fiabilidad de un
  provider cambia. Cada campo fusionado conserva en el read model de qué
  `quote_id`/`source` vino, para no perder trazabilidad aunque se presente un
  solo número.
- Para exchanges (`exchange_side`/`exchange_level` múltiples) **no se fusiona
  entre sources** — ahí sigue aplicando el criterio anterior: OddsPortal
  opening-only conserva `opening→N/A`; Oddspapi muestra su propio
  opening/current bajo otra sección. La razón es la misma de
  [§3.3](#33-decisión-revisada-se-conserva-source-en-la-identidad-de-market_choice_quotes):
  el nivel de un provider no corresponde 1:1 con el del otro, fusionar ahí
  sería inventar una correspondencia que no existe.
- `modules/alerts/alerts_formatter/odds_alert.py` agrupa primero por `source`
  solo para el caso exchange; para bookies normales consume el valor ya
  fusionado del read model. El label Back/Lay usa `exchange_side`, no
  `choice_group`; `choice_group` queda reservado para líneas como `2.5`.
- Actualizar ambos call sites (`odds_alert.py` y
  `modules/jobs/pre_start_check_job/alert_pipeline.py`) y sus tests de snapshot
  textual, cubriendo tanto el caso fusionado (bookie normal, 2 sources) como
  el caso separado (exchange, 2 sides).
- El orden estable es source, market, período, línea, bookie y
  `NULL/back/lay`; el formatter no vuelve a inferir source o side por nombre.
- El formatter traduce `movement` a `↓/=/↑`; la fachada legacy puede conservar
  temporalmente el glifo para su contrato anterior, pero persistencia/query no.
- Desplegar primero en `shadow`, observar al menos un ciclo pre-start y cambiar
  a `quotes` solo si `missing_*`, `price_mismatch` y duplicados inesperados son
  cero. Rollback = volver el flag a `legacy`, sin tocar datos.

#### Fase 5C — Trajectory y pillars

- Crear primero `v_pre_start_odds_trajectory_quotes` en paralelo a la vista
  actual. Su CTE parte de `mcs JOIN mcq ON mcq.quote_id = mcs.quote_id`, y luego
  une `market_choices` mediante `mcq.choice_id`. Lee `source`, side, level,
  lineage, `main_line` e `initial_odds` desde `mcq`; mantiene odds, tiempos,
  `source_limit` y `exchange_size` desde `mcs`.
- El join a `event_source_mappings` y `market_source_mappings` usa
  `mcq.source`/`mcq.source_market_id`, no columnas del snapshot.
- `OddsTrajectoryPoint` agrega `quote_id`, `source`, `exchange_side` y
  `exchange_level`. El `ROW_NUMBER` se particiona por
  `(event_id, quote_id, target_minute)`, no solo por bookie/choice, para que
  back, lay y providers no compitan por el mismo slot.
- `BookieOddsTrajectory` expone source/side/level y
  `ChoiceOddsTrajectory` expone `quote_id`. La clave interna es un string
  estable derivado de `(bookie_id o bookie_name, source, exchange_side,
  exchange_level)`, no una tupla difícil de serializar. Helpers como
  `get_choice_odds_values` resuelven por los campos del objeto y fallan ante
  ambigüedad, en vez de indexar directamente por `bookie_name`.
- `drift_engine.py` puede seguir leyendo `choice.initial_odds`, pero ese valor
  ya proviene de `mcq.initial_odds`. Su `_build_choice_result_key` y output
  incluyen `quote_id/source/side/level`; de lo contrario dos providers del
  mismo bookie se pisan silenciosamente. Agregar regresión que impida mezclar
  opening OddsPortal con snapshots Oddspapi.
- Comparar ambas vistas en staging y después sustituir
  `v_pre_start_odds_trajectory` por la definición quote-aware, manteniendo el
  nombre público para no cambiar el pipeline de pillars. Conservar ambas
  constantes SQL y el selector de rollout hasta Fase 6; `create_or_replace_views`
  crea la vista shadow sin dropear la pública antes de la comparación.
- Tests cubren: 2 sources × 2 sides en el mismo `target_minute`, helper de
  SofaScore compatible, claves serializables, no colisión en drift y ausencia
  total de filas `quote_id=NULL` en la vista nueva.

#### Fase 5D — Lectores restantes y cierre

- Migrar `build_dual_process_event_odds_view_sql`: para `bookie_id = 1` usar la
  quote explícita `(source='sofascore', exchange_side IS NULL, nivel
  preferido) y calcular current como
  `COALESCE(latest_snapshot.odds_value, mcq.current_odds)`, buscando el último
  snapshot por `quote_id`, no por `choice_id`.
- Re-crear/refrescar en el mismo despliegue `event_all_odds` y
  `mv_alert_events`, que dependen de `v_dual_process_event_odds`; comparar
  cardinalidad, nulos y resultados 1/X/2 antes/después.
- Actualizar scripts activos de desarrollo/mantenimiento que filtran metadata
  directamente en `MarketChoiceSnapshot`; las herramientas legacy pueden
  quedar excluidas solo con comentario y owner explícitos.
- Añadir guard estático
  `scripts/maintenance/check_no_legacy_odds_reads.py`. Debe fallar ante nuevas
  lecturas de `MarketChoice.initial_odds/current_odds/change` o de metadata de
  `MarketChoiceSnapshot`, con allowlist temporal y fechada para writers y
  migraciones.
- El guard incluye SQL embebido (`mc.initial_odds`, `mcs.source`, etc.), no solo
  accesos ORM, y se ejecuta en CI. Tests de la ruta legacy que validan escritura
  se permiten hasta Fase 7; código de producción lector no.

#### Estrategia de cutover y rollback

1. Desplegar query/vistas quote-aware en shadow, sin cambiar consumidores.
2. Comparar eventos de Fase 0 y una muestra estratificada por deporte,
   antigüedad, provider, market, side y presencia/ausencia de snapshots.
3. Definir presupuesto antes del corte: cero pérdidas/mezclas/duplicados,
   cardinalidad esperada y p95 de query no peor que el límite acordado.
4. Cortar alertas, trajectory y dual-process por subfase; observar un ciclo
   pre-start completo y al menos un refresh de materialized views.
5. Ante regresión, volver el flag/fachada o recrear la vista legacy; dual-write
   y backfill permanecen porque son aditivos.
6. Solo tras la ventana acordada se autoriza Fase 6. Fase 5 no elimina columnas,
   índices, quotes redundantes ni código de rollback.

**Criterio de aceptación:**
- Paridad exacta para histórico no exchange/single-source con mirror vigente;
  eventos post stop-write se validan contra quotes/payload, no contra columnas
  legacy congeladas. Toda diferencia queda clasificada.
- Evento `158955` (exchange) produce un market/choice canónico con series
  separadas para `(oddsportal|oddspapi) × (back|lay)`, sin quote `NULL`
  visible duplicada, colisiones ni side en `choice_group`.
- Un bookie normal con dos providers mapeados (p. ej. bet365, evento `169158`)
  produce **un solo** valor fusionado de `initial`/`current` por choice en el
  read model de alertas — no 2 quotes visibles sin relación — con
  trazabilidad de qué `source` aportó cada campo ([§3.3](#33-decisión-revisada-se-conserva-source-en-la-identidad-de-market_choice_quotes)).
- Ningún ranking de trajectory colapsa quotes distintas de exchange; tests
  cubren dos sources y dos sides en el mismo target minute.
- Alertas, pillars, drift, dual process y materialized views pasan suites de
  regresión y un ciclo de staging; conteos y latencia quedan dentro del
  presupuesto definido antes del cutover.
- El guard de lecturas legacy queda verde. Las únicas referencias permitidas
  están en writers de compatibilidad, backfill y código expresamente agendado
  para Fases 6–7.

---

### Fase 6 — Adelgazar `market_choice_snapshots`

**Estado al arrancar:** solo después de que Fase 5 esté en producción y el
guard `check_no_legacy_odds_reads.py` esté verde (salvo allowlist fechada de
writers/backfill).

**Objetivo:** migrar a la versión slim (ver [§5](#5-schema-propuesto)) una vez
nada dependa de sus columnas redundantes.

**Pasos concretos:**
1. Confirmar con búsqueda global que ningún lector/query/vista selecciona
   `mcs.source`, `mcs.main_line`, `mcs.exchange_side`, `mcs.source_market_id`,
   `mcs.source_outcome_id`, `mcs.bookmaker_outcome_id` (deben venir de `mcq`).
2. DDL: `quote_id SET NOT NULL` tras verificar `COUNT(*) WHERE quote_id IS NULL`
   = 0. Un allowlist no es compatible con el constraint: toda excepción debe
   resolverse, archivarse fuera de la tabla o bloquear esta fase.
3. DDL: `DROP COLUMN` de las columnas de identity redundantes; mantener
   `odds_value`, `collected_at`, `source_collected_at`, `source_limit`,
   `exchange_size`. Eliminar también `choice_id`: desde este punto el choice se
   obtiene exclusivamente por `quote_id → market_choice_quotes.choice_id`.
   Solo después es seguro limpiar choices/markets Back/Lay legacy.
4. Actualizar ORM `MarketChoiceSnapshot` y cualquier writer legacy que aún
   rellene esas columnas.
5. Medir tamaño de tabla/índices antes/después y archivarlo en el PR.

**Criterio de aceptación:**
- `market_choice_snapshots.quote_id` puede cambiarse a `NOT NULL` sin encontrar
  snapshots pendientes; toda excepción fue resuelta antes de ejecutar el DDL.
- Todas las queries que filtran/seleccionan `source`/`exchange_side`/`main_line`/etc. en `market_choice_snapshots` pasan a hacerlo vía `JOIN market_choice_quotes`.
- Medir tamaño de tabla/índices antes/después.

---

### Fase 7 — Deprecar columnas legacy en `market_choices`

**Estado al arrancar:** la *escritura* desde el path canónico **ya se detuvo**
([§3.2](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado)).
A esta fase solo le queda el DDL + retirar lecturas residuales / ruta legacy.

**Objetivo:** eliminar `MarketChoice.initial_odds`, `current_odds`, `change`
una vez todos los lectores (Fase 5) usen quotes y
`save_markets_from_response_with_stats` ya no las escriba (migrada o retirada
en Fase 8).

**Pasos concretos:**
1. Búsqueda global de `MarketChoice.initial_odds`, `.current_odds`, `.change`
   y de columnas SQL `mc.initial_odds` / `c.initial_odds` — cero hits fuera de
   migraciones/backfill documentados.
2. Confirmar que la ruta `LEGACY_MAINTENANCE_ONLY` ya no escribe esas columnas
   (o que los scripts que la usan se migraron).
3. Migración: `ALTER TABLE market_choices DROP COLUMN initial_odds, …`
   (Postgres) + rebuild SQLite en tests si aplica.
4. Quitar columnas del ORM `MarketChoice` y actualizar comentarios FROZEN.

**Criterio de aceptación:**
- Ningún archivo referencia esas columnas (búsqueda global).
- Migración de schema que las elimina, con ventana de aviso/backup.

---

### Fase 8 — Limpieza de código legacy

**Objetivo:** eliminar en bloque todo lo marcado como legacy a lo largo de las
fases anteriores. Esta fase es exclusivamente borrado + verificación, sin
nueva funcionalidad.

**A eliminar (lista verificada al momento del handoff):**

| Símbolo | Ubicación aprox. | Call sites restantes |
|---|---|---|
| `save_markets_from_oddsportal` | `market_repository.py` | ninguno (muerto) |
| `_save_oddsportal_market` | idem | ninguno |
| `_build_choice_payload` | idem | ninguno |
| `get_oddsportal_markets_for_event` | idem | alias sin call sites |
| `get_external_markets_for_event` | idem, `LEGACY_ODDS_READ` | alertas externas; migrar a `MarketReadQueries` en Fase 5 y borrar en Fase 8 |
| `save_markets_from_response_with_stats` | idem | solo vía `save_markets_from_response` + scripts abajo |
| `save_markets_from_response` | idem | scripts de mantenimiento; migrar y eliminar, no conservar como compatibilidad |

**Scripts que aún llaman la ruta legacy** (resolver uno a uno antes de borrar
`save_markets_from_response*`):
- `scripts/sport_seasons_processing.py`
- `scripts/legacy/extract_historical_results_legacy_event_odds.py`
- `scripts/legacy/process_null_seasons_legacy_event_odds.py`
- `legacy/parse_telegram_odds.py`
- `verify_snapshots.py` (raíz del repo)

**También limpiar:** cualquier `# LEGACY_DEAD_CODE`,
`# LEGACY_MAINTENANCE_ONLY` o `# LEGACY_ODDS_READ`. Convertir
`market_repository.py` en fachada/orquestador puro sobre `market/*`; no se
conservan aliases ni rutas de compatibilidad una vez migrados sus call sites.

**Criterio de aceptación:**
- Búsqueda global de `LEGACY_` sin resultados pendientes de esta lista.
- Suite de tests completa en verde.
- Los 5 scripts de arriba ya migraron a `save_canonical_bookmaker_batches` y
  `save_markets_from_response*` fue eliminado, no conservado.

---

## 9. Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| Fase 2 desincroniza el comportamiento de Oddspapi al converger sobre `save_canonical_bookmaker_batches` | Test de paridad automatizado antes de retirar el shim |
| Backfill (Fase 4) corre sobre datos en producción con jobs activos | Ventana de baja actividad o pausa del pre-start job durante el backfill |
| El backfill pisa una quote live más nueva porque el writer actual no compara timestamps | Política `backfill-fill-only`, test de orden fuera de secuencia y pausa obligatoria de ingesta hasta tener locking condicional compartido |
| Fase 5 intenta unir por `quote_id` antes de que exista/esté poblado | Fase 4 agrega la FK nullable, activa dual-write y exige coverage antes del cutover |
| La ruta legacy crea snapshots nuevos sin `quote_id` después de 4a | Mitigado: el path marcado hace upsert/flush y delega al mismo `MarketChoiceSnapshotWriter`; no existe writer legacy paralelo |
| Oddspapi actualiza quotes Back/Lay pero solo guarda snapshot side-agnostic | 4a genera ticks por quote explícita y testea que no exista el duplicado `NULL` |
| Source de un estado legacy externo no puede probarse | Clasificar como ambiguo; resolver con archivo auditable, nunca adivinar por nombre de bookie |
| Migrar lectores (Fase 5) rompe alertas en producción | Cada PR compara output contra eventos de referencia de Fase 0 |
| Ranking de trajectory vuelve a colisionar providers o Back/Lay | Particionar por `quote_id` y testear 2 sources × 2 sides en el mismo minuto |
| Alerts/drift muestran o sobrescriben la quote `NULL` redundante junto a Back/Lay | Regla común “side explícito domina NULL” + claves que incluyen source/side/level/quote_id |
| Duplicado de fila `exchange_side IS NULL` por migración incompleta | Índice funcional `unique_market_choice_quote_side_null_safe` (`COALESCE`) + test explícito que llama `check_and_migrate_schema()` |
| `market_choices` deja de escribirse antes de Fase 5 ([§3.2](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado)): `odds_alert.py`/`odds_trajectory_context.py`/`drift_engine.py`/dual-process view ven datos incompletos para eventos nuevos | Riesgo aceptado explícitamente; Fase 5 pasa a ser prioritaria, no opcional — no debe quedar pendiente indefinidamente |
| Borrar código en Fase 8 antes de tiempo | Fase 8 solo se ejecuta después de que Fases 2-7 estén en producción sin incidentes |

## 10. Mapa rápido de archivos

**Modelos / schema:**
- `infrastructure/persistence/models.py` — `Market`, `MarketChoice` (odds columns FROZEN), `MarketChoiceSnapshot` (`quote_id` nullable + relaciones), `MarketChoiceQuote`, vistas `PRE_START_ODDS_TRAJECTORY_VIEW_SQL` / `build_dual_process_event_odds_view_sql`.
- `infrastructure/persistence/market_write_policy.py`.
- `infrastructure/persistence/database.py` — incluye `_migrate_market_choice_quotes` (NULL-safe + ALTER de nullability).

**Writers (subpaquete `repositories/market/`):**
- `exchange_quote_payload.py` — hecho
- `odds_movement.py` — hecho
- `market_choice_quote_writer.py` — hecho
- `market_identity_resolver.py` — **PENDIENTE** (sigue inline)
- `market_choice_writer.py` — **PENDIENTE** (sigue inline; ya no escribe odds)
- `market_choice_snapshot_writer.py` — **HECHO**; único constructor/append de snapshots, deriva identidad estable desde `MarketChoiceQuote`
- `market_read_queries.py` — **PENDIENTE** (Fase 5A)
- `infrastructure/persistence/repositories/market_repository.py` — sigue siendo la clase real, no fachada pura

**Adapters / ingesta:**
- `modules/odds_ingestion/adapters/canonical_odds_payload.py` — PENDIENTE (diferido, §6.1)
- `modules/odds_ingestion/adapters/sofascore_market_adapter.py`
- `modules/odds_ingestion/adapters/oddspapi_market_adapter.py`
- `modules/odds_ingestion/adapters/oddsportal_market_adapter.py`
- `modules/oddspapi/exchange_quotes.py`
- `modules/odds_ingestion/market_odds_ingestion_service.py` — 100% canónico

**Lectores quote-aware (Fase 5):**
- `modules/alerts/alerts_formatter/odds_alert.py`
- `modules/jobs/pre_start_check_job/alert_pipeline.py`
- `infrastructure/persistence/repositories/odds_trajectory_repository.py`
- `modules/pillars/odds_trajectory_context.py`
- `modules/pillars/pillar_4/drift_engine/drift_engine.py`
- `infrastructure/persistence/repositories/dual_process_odds_repository.py`

**Backfill completado:**
- `scripts/maintenance/backfill_market_choice_quotes.py` (Fase 4b/4c)
- Referencias: `backfill_sofascore_choice_names_and_groups.py`, `backfill_sofascore_canonical_markets.py`

**Tests relevantes existentes:**
- `tests/test_oddsportal_canonical_ingestion.py`
- `tests/test_oddsportal_hover_parser.py`
- `tests/test_market_choice_quote_model.py` (Fase 1 + NULL sentinel rewrite)
- `tests/test_odds_movement.py` (Fase 1)
- `tests/test_market_choice_quote_writer.py` (Fase 2)
- `tests/test_market_choice_snapshot_writer.py` (Fase 4a, contrato SRP)
- `tests/test_save_canonical_bookmaker_batches_quotes.py` (Fase 2)
- `tests/test_oddsportal_betfair_back_lay_quotes.py` (Fase 3)

**Nota:** si el repo vuelve a ignorar `tests/test_*.py` por defecto, cualquier
archivo nuevo necesita excepción explícita en `.gitignore` o queda fuera de git
sin aviso. Verificar el estado actual de `.gitignore` al crear tests nuevos.

---

## 11. Handoff: continuar desde Fase 4b

> Registro histórico de la transición 4b/4c. Para el estado ejecutable actual
> post-Fase 5, usar [§12](#12-implementación-de-fase-5-mapa-y-deuda-de-cleanup).

Esta sección es el punto de entrada para un dev que llega a
`refactor/db-schema-odds-refactor` **después** de completar el expand schema y
dual-write de Fase 4a.
Lee primero [§3.1](#31-estado-actual-post-fase-3), [§3.2](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado)
y después esta sección; las fases detalladas siguen en [§8](#8-fases-de-implementación).

### 11.1. Qué ya está hecho (no rehacer)

| Pieza | Dónde | Estado |
|---|---|---|
| Tabla/modelo `MarketChoiceQuote` | `models.py`, `database.py::_migrate_market_choice_quotes` | Hecho |
| `exchange_side` nullable + índice `unique_market_choice_quote_side_null_safe` (`COALESCE`) | ORM + migración Postgres (`DROP NOT NULL`/`DROP DEFAULT` + rewrite `'single'→NULL`) | Hecho |
| `ExchangeQuotePayload` (`side: Optional[str] = None`) | `repositories/market/exchange_quote_payload.py` | Hecho |
| `compute_movement` | `repositories/market/odds_movement.py` | Hecho |
| `MarketChoiceQuoteWriter.upsert` | `repositories/market/market_choice_quote_writer.py` | Hecho — **reutilizar en backfill** |
| Convergencia 3 providers → `save_canonical_bookmaker_batches` | `market_odds_ingestion_service.py` | Hecho |
| OddsPortal back/lay sin `choice_group=Back/Lay` | `oddsportal_market_adapter.py` | Hecho |
| Stop-write de odds a `MarketChoice` en path canónico | `market_repository.py::save_canonical_bookmaker_batches` | Hecho (adelanto de Fase 7 write-stop) |
| `MarketChoiceSnapshot.quote_id` nullable + FK/índice | `models.py`, `database.py::_migrate_market_choice_snapshot_lineage` | Hecho (Fase 4a) |
| Writer único de snapshots | `repositories/market/market_choice_snapshot_writer.py::MarketChoiceSnapshotWriter.append` | Hecho; `market_repository.py` no construye snapshots |
| Lineage exacto en paths canónico y legacy | `_upsert_choice_quotes` → `MarketChoiceSnapshotWriter` | Hecho; ambos delegan al mismo writer |
| Snapshots Oddspapi Back/Lay por side/level | `save_canonical_bookmaker_batches` | Hecho; sin current `exchange_side=NULL` redundante |
| Persistencia batch sin N+1 de quotes | `save_canonical_bookmaker_batches` + `MarketChoiceQuoteWriter` | Hecho; quote preload único, índice de identidad compartido y tres SELECT constantes para un batch existente |
| Resolución Oddspapi con sesión compartida | `market_odds_ingestion_service.py::save_from_oddspapi_response` | Hecho; una sesión de referencias para todos los bookmakers y otra transacción atómica para odds |
| Tests de quotes / OddsPortal / writer | `tests/test_market_choice_quote_*.py`, `test_save_canonical_bookmaker_batches_quotes.py`, `test_oddsportal_*` | Hecho |

**Pendiente (empezar aquí):**

| Pieza | Estado |
|---|---|
| Merge temporal live + fill-only | Pendiente — implementar antes de usar el writer desde 4b |
| Script `backfill_market_choice_quotes.py` | Pendiente |
| Lectores quote-aware (alertas/trajectory/drift/dual-process) | Pendiente (Fase 5, urgente) |
| Slim de snapshots / DROP columns de choices / limpieza legacy | Fases 6–8 |
| Extracción de `MarketIdentityResolver` / `MarketChoiceWriter` | Limpieza posterior; snapshot writer ya extraído |
| Unificación de adapters a `CanonicalOddsResponse` | Diferida ([§6.1](#61-unificación-de-adapters--decisión-revisada-al-implementar)) |

### 11.2. APIs y puntos de entrada a reutilizar

**Escritura de quotes (idempotente, partial-arrival safe):**

```python
MarketChoiceQuoteWriter.upsert(
    session,
    quote_index=preloaded_quotes_by_identity,
    choice_id=...,
    source="oddspapi" | "oddsportal" | "sofascore",
    exchange_side=None | "back" | "lay",  # NULL = non-exchange
    exchange_level=0,
    initial_price=...,
    initial_captured_at=...,
    current_price=...,
    current_captured_at=...,
    main_line=...,
    source_market_id=...,
    source_outcome_id=...,
    bookmaker_outcome_id=...,
    source_limit=...,
    overwrite_initial=False,  # True solo para OddsPortal opening-only
)
```

**Append de snapshots (único punto de escritura):**

```python
MarketChoiceSnapshotWriter.append(
    session,
    quote=persisted_or_pending_quote,  # asociada a la misma Session
    odds_value=...,
    collected_at=...,
    source_collected_at=...,
    source_limit=...,
    exchange_size=...,
)
```

El writer deriva `choice_id`, `source`, metadata, `exchange_side` y
`exchange_level` desde la quote; ningún caller puede suministrar una identidad
paralela contradictoria. No exige `quote_id` antes del append: la relación ORM
permite que SQLAlchemy ordene los INSERT y propague la PK/FK en el flush final.

**Wiring en vivo:** `MarketRepository._upsert_choice_quotes` (privado) es
llamado desde `save_canonical_bookmaker_batches` después del flush de choices.
Soporta dos formas de wire:
- `choice_data["exchangeQuotes"]` — lista (Oddspapi).
- `choice_data["exchangeSide"]` — singular `back`/`lay` (OddsPortal).

**Políticas:** `infrastructure/persistence/market_write_policy.py`
(`ODDSPORTAL_OPENING_ONLY_POLICY`, `DEFAULT_MARKET_WRITE_POLICY`).

**Migraciones de aplicación:** el deploy pasa por
`db_manager.check_and_migrate_schema()` (`app/initialize.py`). La migración de
quotes ya incluye el ALTER de nullability en Postgres. El backfill no dispara
ese migrador: su `--dry-run` valida el schema de forma read-only y falla si 4a
no fue desplegada.

**NO inventar** un segundo upsert de quotes en el backfill. Si el writer no
cubre un caso, extenderlo con la política `backfill-fill-only` y tests. Estado
actual importante: `upsert()` retorna la quote y `_upsert_choice_quotes` ya
propaga un mapa por `(side, level)`; lo pendiente antes del histórico es impedir
que `current_odds` retroceda por timestamps antiguos.

### 11.3. Inventario de lectores aún en columnas legacy

Ver tabla detallada en [Fase 5](#fase-5--migrar-lectores-a-quotes). Resumen de
prioridad:

1. **Alertas externas:** `get_external_markets_for_event` → `odds_alert.py` →
   `alert_pipeline.py`. Hoy leen el mirror congelado de `MarketChoice` +
   `source` del último snapshot. Primer cutover visible para el usuario.
2. **Trajectory:** `PRE_START_ODDS_TRAJECTORY_VIEW_SQL` →
   `odds_trajectory_repository.py` → `odds_trajectory_context.py` →
   `drift_engine.py`. Hoy particiona sin `quote_id`/`exchange_side`.
3. **Dual-process:** `build_dual_process_event_odds_view_sql` →
   `dual_process_odds_repository.py` (+ engines process_1/process_2,
   `streak_analysis_resolver.py`). Scope `bookie_id = 1` / SofaScore.

Hasta que esos tres grupos lean quotes, **eventos nuevos** muestran
`NULL`/incompletos en opening/current donde antes el mirror cruzaba fuentes
([§3.2](#32-decisión-market_choices-deja-de-escribirse-antes-de-fase-5-riesgo-aceptado)).

### 11.4. Inventario de metadata de identidad aún en snapshots

Columnas de identity que **deberían** vivir solo en `market_choice_quotes`
(y que Fase 6 dropea de snapshots una vez Fase 5 lea vía `quote_id`):

`source`, `source_market_id`, `source_outcome_id`, `bookmaker_outcome_id`,
`main_line`, `exchange_side`, `exchange_level`.

| Quién escribe hoy | Qué pone en el snapshot |
|---|---|
| `MarketChoiceSnapshotWriter` desde path canónico | Copia temporalmente identity estable desde la quote + valores propios del tick |
| `MarketChoiceSnapshotWriter` desde path legacy marcado | Exactamente el mismo contrato; no existe writer legacy paralelo |

Lectores que aún dependen de `mcs.*` para identity: sobre todo
`PRE_START_ODDS_TRAJECTORY_VIEW_SQL` y `get_external_markets_for_event`
(último `source`).

Tick-only (se quedan en snapshots): `odds_value`, `collected_at`,
`source_collected_at`, `source_limit`, `exchange_size`.

### 11.5. Patrones de scripts de backfill a copiar

| Script de referencia | Qué copiar |
|---|---|
| `scripts/maintenance/backfill_sofascore_choice_names_and_groups.py` | CLI: `--dry-run` default / `--commit` explícito (`args.dry_run = not args.commit`), filtros por `event-id` range, `--output-json`, reportes CSV bajo `debug/` |
| `scripts/maintenance/backfill_sofascore_canonical_markets.py` | Copiar bootstrap/imports y `progress_step`; **no** copiar la llamada automática a `check_and_migrate_schema()` porque el dry-run de Fase 4 debe ser read-only |
| `scripts/maintenance/backfill_event_entities_from_sofascore.py` | Segundo ejemplo de lote + idempotencia |

Contrato mínimo del nuevo
`scripts/maintenance/backfill_market_choice_quotes.py`: ver
[Fase 4 § Contrato operativo](#contrato-operativo-del-script).

### 11.6. Orden sugerido de PRs y smoke checks

| PR | Contenido | Smoke / aceptación mínima |
|---|---|---|
| **4a — HECHO** | `quote_id` nullable + FK/índice + `MarketChoiceSnapshotWriter` único + ticks Back/Lay | Cero `MarketChoiceSnapshot(...)` en `market_repository.py`; todo snapshot tiene quote exacta, incluido path legacy marcado |
| **4b** | Merge temporal/fill-only + backfill optimizado + dry-run read-only + tests de clasificación | `--dry-run` sobre `158955`; cero mutaciones de datos/schema; segundo `--commit` es no-op |
| **4c** | Commit de backfill en staging/prod (ventana de baja actividad) | Coverage: `unlinked_snapshots` / `unmigrated_choice_states` = 0 en scope del cutover; JSON archivado |
| **5A** | Read models + query set-based + fachada `legacy/shadow/quotes` + comparador | Cero N+1; diferencias clasificadas; side explícito suprime quote `NULL` redundante |
| **5B** | `odds_alert.py` + `alert_pipeline.py` + cutover por flag | Alertas muestran back/lay y sources separados; métricas bloqueantes en cero |
| **5C** | Vista trajectory shadow + ranking por `quote_id` + context/drift | 2 sources × 2 sides no colisionan en ranking, contexto ni output de drift |
| **5D** | Dual-process + refresh de dependencias + guard de lecturas legacy | Guard CI verde; dual-process usa quote SofaScore y materialized views conservan cardinalidad |
| **6** | Slim snapshots | `quote_id NOT NULL` + DROP identity cols; tamaño medido |
| **7** | `DROP COLUMN` en `market_choices` | Búsqueda global limpia |
| **8** | Borrar métodos muertos + decidir scripts legacy | `LEGACY_` resuelto; suite verde |

**Tests de humo locales recomendados al tocar persistencia:**

```bash
python -m pytest tests/test_market_choice_quote_model.py tests/test_market_choice_quote_writer.py tests/test_save_canonical_bookmaker_batches_quotes.py tests/test_oddsportal_canonical_ingestion.py tests/test_oddsportal_betfair_back_lay_quotes.py -q
```

**Simulación end-to-end:** `scripts/development/simulate_pre_start_check.py`
(llama `initialize_system` → `check_and_migrate_schema`). Tras cambios de
schema, reiniciar para que corra la migración (p.ej. el `DROP NOT NULL` de
`exchange_side`).

Consulta útil para inspeccionar el estado post-ingesta:

```sql
SELECT m.market_id, m.market_name, m.choice_group, b.name AS bookie,
       mc.choice_name,
       mcq.source, mcq.exchange_side, mcq.exchange_level,
       mcq.initial_odds, mcq.current_odds, mcq.main_line,
       mcq.source_market_id, mcq.source_outcome_id
FROM markets m
JOIN bookies b ON b.bookie_id = m.bookie_id
JOIN market_choices mc ON mc.market_id = m.market_id
LEFT JOIN market_choice_quotes mcq ON mcq.choice_id = mc.choice_id
WHERE m.event_id = :event_id
ORDER BY b.name, m.market_name, m.choice_group NULLS FIRST,
         mc.choice_name, mcq.source, mcq.exchange_side NULLS FIRST;
```

### 11.7. Gotchas operativos

1. **`NULL != NULL` en UNIQUE:** nunca confiar solo en
   `UniqueConstraint(choice_id, source, exchange_side, exchange_level)`. La
   protección real es el índice funcional
   `unique_market_choice_quote_side_null_safe`. Tras crear tablas en tests,
   llamar `check_and_migrate_schema()` (o al menos
   `_migrate_market_choice_quotes()`).
2. **Tablas viejas con `NOT NULL DEFAULT 'single'`:**
   `_migrate_market_choice_quotes` ya hace `DROP NOT NULL`/`DROP DEFAULT` en
   Postgres y reescribe `'single'→NULL`. Si un entorno no pasó por
   `initialize_system` después de ese cambio, Oddspapi falla con
   `NotNullViolation` al insertar quotes no-exchange.
3. **No mezclar sources en una sola serie:** OddsPortal opening + Oddspapi
   current son **dos quotes**. El mirror de `MarketChoice` que las combinaba
   ya no se escribe; Fase 5 debe presentarlas como series distintas (o una
   política de presentación explícita), nunca reintroducir un merge silencioso
   en el reader.
4. **OddsPortal legacy `choice_group IN ('Back','Lay')`:** el pipeline nuevo ya
   no las crea, pero el histórico sí las tiene. El backfill (Fase 4) las mapea
   al market canónico; **no las borres** en Fase 4. Limpieza de datos = paso
   separado post-observación.
5. **Las rutas de scripts siguen siendo legacy aunque compartan writers.**
   `save_markets_from_response(_with_stats)` está marcado
   `LEGACY_MAINTENANCE_ONLY` porque duplica orquestación y mirror de choices;
   no agregar call sites y eliminarlo en Fase 8.
6. **No hay writer legacy de snapshots.** Ambos paths delegan en
   `MarketChoiceSnapshotWriter`; una regresión se detecta buscando cualquier
   `MarketChoiceSnapshot(...)` fuera de ese archivo y fixtures/tests.
7. **Oddspapi exchange conserva ticks por side/level.** `_upsert_choice_quotes`
   crea Back/Lay y el writer agrega una serie por instrumento, sin current
   side-agnostic redundante.
8. **Gate de opening snapshot es side-aware (vivo).**
   `save_canonical_bookmaker_batches` decide `initial_was_set` mirando en el
   `quote_index` precargado la quote de
   `_opening_gate_side_and_level(choice_data)` — `exchangeSide` (OddsPortal),
   `back/0` si hay `exchangeQuotes` (Oddspapi), o `NULL/0` en bookies normales.
   **No** hace SELECT extra. Antes solo miraba `exchange_side IS NULL`, lo que
   hacía que OddsPortal Betfair (solo back/lay) pareciera “primer opening” en
   cada re-ingest; era inocuo mientras `persist_opening_snapshots=False`.
9. **Oddspapi Betfair aún escribe una quote `exchange_side=NULL` además de
   back/lay** (eco del `decimalValue` plano del payload). Es compatibilidad,
   no el modelo ideal. **No** copiar ese patrón en OddsPortal. Plan de retiro:
   - Fase 5: readers suprimen `NULL` cuando existen sides explícitos (ya
     documentado en política de lectura).
   - Post cutover (PR propio, no 4b): dejar de upsertar la fila `NULL` cuando
     `exchangeQuotes` viene poblado; auditar/borrar filas `NULL` huérfanas
     solo cuando nada las lea.
   - 4b: clasificar evidencia histórica `NULL` vs `back`/`lay` sin borrar;
     nunca fusionar silenciosamente `NULL` con `back`.
10. **El writer actual no es seguro para backfill temporal.** Siempre reemplaza
   current; agregar `backfill-fill-only` antes de reutilizarlo (Fase 4b).
11. **Fase 5 es prioritaria** respecto a extracciones cosméticas
   (`MarketIdentityResolver`, unificación de adapters). No bloquees alertas por
   refactors de estructura de archivos.

---

## 12. Implementación de Fase 5: mapa y deuda de cleanup

**Fecha de implementación local:** 2026-08-12.  Esta sección es el estado
vigente; los inventarios de §§3 y 11 se conservan como historial de decisiones.

### 12.1. Mapa de responsabilidades post-cutover

| Capa | Módulo | Responsabilidad única |
|---|---|---|
| Configuración | `infrastructure/settings/config.py`, `config/odds_read_priority.json` | Validar flags, sample rate, boundary UTC y política versionada de prioridad por campo |
| Contrato | `repositories/market/market_read_models.py` | DTOs inmutables con identidad y provenance; cero SQL/formato |
| Política | `repositories/market/market_quote_read_policy.py` | Resolver prioridad default y overrides por sport/bookie |
| Query de alertas | `repositories/market/market_read_queries.py` | Una consulta set-based y proyección determinista normal/exchange |
| Shadow | `repositories/market/market_read_comparator.py` | Comparación pura y clasificación de diferencias; cero acceso a DB |
| Readiness | `repositories/market/market_quote_readiness.py`, `scripts/maintenance/audit_market_quote_readiness.py` | Gates read-only de schema, coverage, lineage e identidad |
| Fachada temporal | `repositories/market_repository.py` | Selección `legacy/shadow/quotes`; delega el algoritmo nuevo |
| Presentación | `modules/alerts/alerts_formatter/odds_alert.py` | Traduce DTOs a texto; no infiere source/side ni consulta DB |
| Trajectory | vista canónica en `models.py`, `odds_trajectory_repository.py` | Selección histórica por `quote_id` y target minute; sin modos runtime |
| Contexto/pillars | `odds_trajectory_context.py`, drift y Pilar 5 | Identidad serializable y consumo de series ya seleccionadas |
| Dual-process | vista canónica en `models.py` | Quote SofaScore exacta, último tick por quote y contrato público estable; sin modos runtime |
| Protección | `check_no_legacy_odds_reads.py`, workflow `legacy-odds-read-guard.yml` | Impedir nuevas lecturas legacy ORM/SQL fuera de allowlist fechada |
| Inicialización | `app/initialize.py` | Validar config, migrar, crear wrappers en orden y reconstruir dependencias |

Flujo activo:

```text
market_choice_quotes + snapshots(quote_id)
    ├─ MarketReadQueries → DTOs → formatter de alertas
    ├─ v_pre_start_odds_trajectory → repository → contexto → drift/Pilar 5
    └─ v_dual_process_event_odds → event_all_odds / mv_alert_events
```

### 12.2. Evidencia local de aceptación

- Auditoría total post-4c: `ready=true`; 2,758,365 snapshots auditados en la
  base, cero snapshots sin `quote_id`, cero identidad NULL-safe duplicada y
  cero mismatch quote/choice.
- Eventos de referencia: `158955` produjo 84/84 filas trajectory
  legacy/quotes y `169158` 16/16; en ambos casos `quote_id IS NULL = 0`.
- Alertas: ambos eventos dieron igual número de bloques, comparación `equal`,
  cero diagnostics/blockers. Medianas quote-aware 5.91 ms y 5.07 ms versus
  11.82 ms y 7.80 ms legacy.
- Dual-process, verificación final sobre el mismo estado: 126,071 eventos
  comunes, cero pérdidas y cero value mismatch; quotes añadió 177 eventos.
  Los 177 tienen estado legacy incompleto, ninguno tiene el mirror legacy
  completo y no existen choices con más de una quote SofaScore elegible. Son
  recuperación de estado normalizado, no duplicación del reader.
- `mv_alert_events` (verificación final): 123,051 filas y checksum ordenado por
  `event_id` sobre `row_to_json`, `ff4802dea244f4fb6651264d47143c2f`, idéntico
  en ambos modos. El dataset cambió durante la sesión; cada comparación se
  repitió en legacy/quotes sobre el mismo estado y el wrapper final quedó en
  quotes.
- Refresh alternado final (3+3): legacy `[3.723, 3.526, 3.658]` s y quotes
  `[4.329, 4.374, 4.294]` s; medianas 3.658/4.329 s, ratio `1.183×`, dentro del
  presupuesto `≤1.20×`. `EXPLAIN (ANALYZE, BUFFERS)` confirmó
  `idx_market_choice_quotes_dual_sofascore` y
  `idx_market_choice_snapshots_quote_collected` (2,431.807 ms en el conteo
  quote-aware medido).
- Casos sintéticos cubren dos sources × back/lay, top-of-book, supresión NULL,
  field merge con provenance, opening-only, colisiones de context/drift y
  selección SofaScore estricta en Pilar 5.

### 12.3. Código legacy y cleanup futuro

| Pieza | Marca/estado | Por qué no se elimina ahora | Cleanup |
|---|---|---|---|
| `_get_external_markets_legacy` y branch dict del formatter | `LEGACY_ODDS_READ` | Shadow/rollback durante observación | Borrar en Fase 8 junto con modo `legacy` |
| `retire_odds_read_variants_postgresql` y `RETIRED_ODDS_READ_VIEWS` | `PHASE8_CLEANUP` | Local/staging sí pueden tener variantes dual/trajectory de la primera Fase 5; el servidor puede no tenerlas | Borrar después de que todos los entornos crucen Fase 6; sus `DROP VIEW IF EXISTS` son no-op donde nunca existieron |
| `save_markets_from_response(_with_stats)` y scripts callers | `LEGACY_MAINTENANCE_ONLY` | Aún usados por scripts históricos | Migrar scripts y borrar en Fase 8 |
| `_save_oddsportal_market`, `_build_choice_payload`, `save_markets_from_oddsportal`, alias `get_oddsportal_markets_for_event` | `LEGACY_DEAD_CODE` | Se preservaron fuera del cutover lector | Eliminación conjunta en Fase 8 |
| Quotes exchange `exchange_side=NULL` redundantes de Oddspapi | compatibilidad de datos | Readers ya las suprimen; borrarlas durante rollout rompería rollback | Dejar de escribir y purgar en PR post-observación |
| `market_repository.py` monolítico | deuda SRP | La fachada aún contiene orquestación y writers legacy | Extraer `MarketIdentityResolver`/`MarketChoiceWriter`; reducir a fachada |
| DDL de reporting dentro de `models.py` | deuda SRP/modularidad | Cambio de ubicación junto al cutover aumentaría riesgo DDL | Mover a `infrastructure/persistence/views/` en Fase 8 |
| Dual view creado tanto en `create_or_replace_views` como en `create_or_replace_materialized_views` | redundancia | Garantiza dependencia hoy, pero duplica responsabilidad | Un único `rebuild_dual_process_dependencies()` transaccional |
| `DROP VIEW ... CASCADE` para vistas basketball/season durante init | cleanup de seguridad | Preexistente y fuera de odds; puede ocultar dependencias nuevas | Inventariar y reemplazar por DDL fail-safe sin `CASCADE` |
| `DatabaseManager.check_and_migrate_schema` monolítico | deuda SRP | Orquesta migraciones históricas de muchas áreas | Mover DDL versionado a Alembic/servicios de migración pequeños |
| `initialize_system` continuaba después de un schema migration failure | `LEGACY_INCORRECT` corregido en Fase 6 | Permitía arrancar writers con contrato incompatible | Mantener el startup fail-closed y agregar healthcheck de schema por versión |
| Allowlist del guard | temporal, por path+símbolo+motivo+fase | Backfill/readiness/modelos todavía necesitan columnas legacy | Expira en Fase 7/8; no admitir consumidores nuevos |
| `.gitignore` ignora globalmente `tests/`, `*.json`, `*.md` y exige excepciones | deuda de tooling | Preexistente | Simplificar reglas para que tests/config/docs no queden fuera de git |

### 12.4. Hallazgos fuera de alcance, marcados para cleanup

- La suite indiscriminada `pytest tests` contiene pruebas antiguas que ni
  recolectan: referencias a `module_8`, `_aggregate_multilayer_side_engine_v2`,
  `_is_active_signal`, módulo raíz `odds_alert`, API retirada
  `get_event_information`, y dependencia no declarada `loguru` en un test
  ignorado. No mezclar esa reparación con el schema refactor.
- La suite versionada conserva 12 fallos ajenos al cutover (24 pruebas del
  mismo grupo sí pasan): 11 contratos obsoletos de mapping/adapter en
  `tests/oddspapi/test_market_adapter.py` y un test manual de extracción cuyo
  binario Chromium de Playwright no está instalado en
  `test_oddsportal_betfair_extraction.py`; requieren cleanup propio.
- `modules/oddsportal/scraper_lookup.py` emite `SyntaxWarning` por una secuencia
  `\/` dentro de JavaScript embebido; convertir ese literal a raw/escape válido.
- `shared/timezone_utils.py` usa `datetime.utcnow()` deprecado y
  `models.py` todavía importa `declarative_base` desde la ruta SQLAlchemy 1.x.
- Se corrigió durante Fase 5 una deuda funcional encontrada por regresión:
  ladders exchange opening-only usaban hora de extracción como
  `current_updated_at`, por lo que un current provider posterior podía verse
  stale. Ahora `_resolve_exchange_observation_time` usa el timestamp opening
  para esa observación provisional; suites writer/merge/exchange: 33 verdes.
- `requirements.txt` declaraba `psycopg2` mientras `compose.yaml` usa
  `postgresql+psycopg`; quedó alineado a psycopg v3 y se declaró
  `pytest-asyncio`, requerido por tests versionados.
- La regresión mantenida (suite versionada, dos archivos preexistentes
  excluidos, más tests nuevos de Fase 5) terminó con 287 pruebas verdes. Los
  dos archivos excluidos son exactamente los nominados arriba; no se ocultó
  ningún fallo nuevo del refactor.

### 12.5. Pendiente operativo antes del DDL de Fase 6 en servidor

El código y el entorno local están cortados a quotes. Falta observar en el
entorno objetivo al menos un ciclo pre-start completo (T-120/T-30/T-5/T0 y el
momento posterior configurado), recopilar p95 con una muestra estratificada y
confirmar cero blockers. `MARKET_CHOICE_LEGACY_STOP_WRITE_AT` permanece vacío:
no se inventó una hora; sólo hace falta para clasificar shadow.

La preparación y validación de Fase 6 puede hacerse sobre la copia local. El
DDL destructivo **no se replica al servidor** hasta cerrar esa ventana. Una
vez aplicado el snapshot slim, el rollback de Fase 5 por flag deja de ser una
garantía estructural completa: restaurar columnas exige recuperar el backup o
la copia pre-6.

---

## 13. Implementación de Fase 6: snapshots slim

**Ejecución local completada:** 2026-08-12. Plan, runbook y evidencia en
[`db-schema-odds-refactor-phase-6.md`](db-schema-odds-refactor-phase-6.md).

- La migración y compactación se ejecutaron exclusivamente mediante
  `python -m scripts.maintenance.migrate_market_choice_snapshots_slim`; queda
  prohibido aplicar su SQL manualmente en servidor.
- Schema final: siete columnas, `quote_id NOT NULL`, cero nulos/huérfanos y
  una sola identidad vía quote.
- Filas/checksum preservados: 2,762,285 y
  `8fec7e3fb72e38a910a84d657b7f1784`.
- Tamaño total: 651,206,656 → 366,993,408 bytes (-43.6%).
- Postflight de `158955`/`169158`, vistas, wrappers y MV: verde; ejecución
  repetida del CLI: idempotente.
- Servidor sigue bloqueado hasta completar la observación operativa descrita
  en §12.5; luego debe usar el mismo runbook, con jobs detenidos y backup.

Deuda marcada: el backfill 4b/4c y los dos scripts históricos de
canonicalización todavía dependen del schema snapshot expandido. Sus
preflights los bloquean bajo Fase 6; eliminar o portar a quote lineage en Fase
8. El startup ya no migra snapshots y falla de forma explícita si se intenta
reanudar la aplicación Fase 6 antes de ejecutar el script.

### 13.1. Baseline confirmado

- Tabla exacta: `public.market_choice_snapshots`.
- Filas al preflight inicial: 2,762,285.
- `quote_id IS NULL = 0`.
- Tamaño total 651,206,656 bytes: heap 218,112,000 e índices 433,004,544.
- Dependencias SQL iniciales: las antiguas vistas privadas dual y trajectory.
  La migración instala ambas vistas canónicas quote-aware y retira las cuatro
  privadas cuando existan.
- Columnas slim conservadas: `snapshot_id`, `quote_id`, `odds_value`,
  `collected_at`, `source_collected_at`, `source_limit`, `exchange_size`.
- Columnas a retirar: `choice_id`, `source`, `source_market_id`,
  `source_outcome_id`, `bookmaker_outcome_id`, `main_line`, `exchange_side`,
  `exchange_level`.

### 13.2. Decisiones de frontera

- Toda identidad se obtiene mediante
  `snapshot.quote_id → market_choice_quotes → market_choices`.
- Los datos por tick (`source_collected_at`, `source_limit`, `exchange_size`)
  permanecen en snapshots; no son identidad duplicada.
- El writer deja de copiar identidad desde la quote al snapshot.
- Las vistas llamadas `*_legacy` sólo pueden conservar rollback del estado de
  precio de `market_choices`; incluso ellas deben resolver lineage histórica
  por `quote_id`. No se conserva un reader de identidad snapshot legacy.
- El backfill 4b/4c de snapshots queda cerrado tras Fase 6: un esquema con
  `quote_id NOT NULL` no puede volver a clasificar snapshots sin lineage. Las
  operaciones de limpieza que sigan vivas deben navegar mediante quotes.
- La migración no usa `CASCADE`. Cualquier dependencia no reconocida bloquea
  el DROP.

### 13.3. Estado

Fase 6 está **completada en local y lista para commit**. El commit de cierre de
Fase 5 es `67b1d3f`; la aplicación, el script, el schema local, readers, MV,
guards y regresión quedaron verdes. La réplica en servidor conserva el gate
operativo de §12.5 y debe usar exclusivamente el runbook versionado. Las
modificaciones preexistentes de los documentos 4b/4c permanecen fuera del
scope del commit de Fase 6.
