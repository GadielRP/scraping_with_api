# P4 — Trayectoria Temporal del Mercado
## Especificación y Contrato Técnico v4 (`p4-signal-profile-v1`)

> **Estado:** IMPLEMENTADO — CONTRATO V4 EN PRODUCCIÓN (`p4-signal-profile-v1`)  
> **Repositorio:** `GadielRP/scraping_with_api`  
> **Punto principal de orquestación:** `modules/jobs/pre_start_check_job/pillar_pipeline.py`  
> **Paquete del pilar:** `modules/pillars/pillar_4/`  
> **Entry point:** `modules/pillars/pillar_4/run_pillar_4.py` (`calculate_pillar_4`)  
> **Minería:** `modules/pillars/mining/adapters/pillar_4.py` (`P4MiningAdapter`, scope `temporal_market_drift`)  
> **Arquitectura de referencia:** Modelo desacoplado análogo a Pilar 2 y Pilar 3  
> **Endpoint operativo:** `Tn`, donde `n = P4_TARGET_MINUTE` recibido dinámicamente desde el pipeline (default evaluado: `T5`)  
> **Contrato físico de inputs:** `modules/pillars/context.py` (`EventContext`), `modules/pillars/odds_trajectory_context.py` (`OddsTrajectoryContext`) y referencia complementaria en [inputs.md](../../inputs.md)  
> **Objetivo de este documento:** Contrato normativo y especificación canónica del motor de trayectoria temporal de P4.

---

# 0. Lenguaje normativo

Los siguientes términos son normativos:

- **DEBE**: comportamiento obligatorio.
- **NO DEBE**: comportamiento prohibido.
- **DEBERÍA**: comportamiento preferido; cualquier desviación debe justificarse técnicamente.
- **PUEDE**: comportamiento opcional.

Este blueprint sustituye los borradores de revisión de P4 que trataban `T1` como parte de la trayectoria operativa.

La lógica y las decisiones canónicas de P4 están cerradas y su implementación vive en `modules/pillars/pillar_4/`. La sección 79 registra el resultado de la auditoría técnica y las limitaciones conocidas; ya no constituye una lista de decisiones abiertas.

Regla de lectura de este documento: `Tn` designa siempre el target operativo dinámico recibido por P4. Las apariciones de `T5` en ejemplos numéricos representan la configuración actual (`n = 5`) y no autorizan hardcodear el valor cinco.

---

# 1. Motivación y arquitectura del rediseño v4

P4 fue implementado originalmente bajo un modelo semántico más antiguo.

Esa implementación previa contenía conceptos aprovechables, pero sus responsabilidades y su semántica temporal no coincidían con la arquitectura de System I.

El diseño v4 consolidó a P4 con la arquitectura utilizada por P2/P3:

```text
pre_start_check_job
        │
        ▼
modules/jobs/pre_start_check_job/pillar_pipeline.py
        │
        ├── construye EventContext
        ├── construye OddsTrajectoryContext
        │
        ▼
   P1 / P2 / P3 / P4 / P5
        │
        ▼
 integración / persistencia / minería
```

P2 y P3 ya utilizan la arquitectura general deseada:

```text
orquestador delgado del pilar
        ↓
política de extracción / selección de snapshot
        ↓
modelos internos tipados/normalizados
        ↓
calculadores matemáticos puros
        ↓
ensamblaje de signal/profile
        ↓
raw audit / trazabilidad
```

P4 DEBERÍA seguir la misma separación de responsabilidades.

P4 NO DEBE permanecer como un `drift_engine` monolítico si eso mezcla:

- extracción,
- resolución de identidad de mercado,
- cálculos temporales,
- cálculos semánticos,
- clasificación de patrones,
- serialización,
- auditoría post-signal.

---

# 2. Responsabilidad canónica de P4

P4 responde:

> **¿Cómo llegó temporalmente el mercado al estado que System I observó en su target operativo Tn?**

P4 no responde:

- qué lado es deportivamente mejor;
- quién probablemente ganará;
- si Over o Under es más probable;
- si existe value;
- si debe hacerse una apuesta;
- si el movimiento es sharp/smart/professional;
- si el movimiento es estadísticamente significativo.

Límites de responsabilidad:

```text
P1
estructura deportiva/equipo

P2
snapshot estructural SIDE en el punto operativo

P3
snapshot estructural Over/Under en el punto operativo

P4
trayectoria temporal del mercado que termina en Tn

P5
precedente histórico exacto

        ↓

INTEGRACIÓN / DECISIÓN DE SYSTEM I
```

P4 es por tanto un **productor de features estructurales temporales**.

Puede describir estructura matemática observable.

NO DEBE convertir esa estructura en significado predictivo salvo que una futura calibración de Minería lo autorice mediante un contrato explícito y versionado.

---

# 3. Contrato temporal fundamental

## 3.1 Significado de Tn

Toda notación `Tn` se refiere a minutos relativos al horario de inicio del evento contenido en `EventContext`.

Ejemplos:

```text
T360 = 360 minutos antes del inicio
T120 = 120 minutos antes del inicio
T30  = 30 minutos antes del inicio
T5   = 5 minutos antes del inicio
T1   = 1 minuto antes del inicio
T0   = inicio nominal del evento
T-5  = 5 minutos después del inicio nominal
```

El número es una **etiqueta de target/checkpoint**.

No demuestra que la cuota haya sido capturada exactamente en ese segundo.

El tiempo real de captura proviene del metadata del punto.

---

## 3.2 Endpoint operativo canónico

Para P4:

```text
P4_OPERATIVE_ENDPOINT = T{P4_TARGET_MINUTE}
```

Toda representación operativa de P4 DEBE terminar en la lectura canónica exacta disponible en el target solicitado. En la ejecución actual del pipeline `P4_TARGET_MINUTE = 5`, pero el pilar acepta cualquier entero soportado por el contexto.

Sin embargo, a partir de la arquitectura nueva de ingesta histórica, es necesario distinguir dos conceptos:

```text
TRAYECTORIA ADAPTATIVA / EVENT-DRIVEN
=
número variable de cambios persistidos por cada choice

TRAYECTORIA DE CHECKPOINTS
=
proyección/fallback sobre momentos canónicos Tn
```

Ejemplo con la configuración actualmente disponible para P4:

```text
T120 → T30 → T5
```

El mismo ejemplo con T360 presente es:

```text
T360 → T120 → T30 → T5
```

Esto NO significa que el P4 definitivo deba reducir toda su información temporal exclusivamente a cuatro puntos.

`ChoiceOddsTrajectory.snapshots` ya expone la trayectoria persistida completa de cada choice. Por tanto, una serie puede contener un número indeterminado de observaciones anteriores al target operativo, incluso múltiples observaciones dentro del mismo minuto.

T360 debe entenderse principalmente como:

- nuevo early anchor canónico para el fallback/proyección por checkpoints;
- referencia reproducible para investigación;
- mecanismo de resiliencia cuando la trayectoria adaptativa no esté disponible.

Si la trayectoria adaptativa ya contiene observaciones válidas anteriores a T360, P4 NO DEBE descartarlas solo para forzar el modelo de cuatro checkpoints.

---

## 3.3 Por qué existe T360

T360 no se agrega simplemente para aumentar el número de observaciones.

Su función principal dentro de la trayectoria canónica de checkpoints/fallback es distinguir:

- movimiento que ya estaba formado antes de las últimas dos horas;
- movimiento desarrollado principalmente durante las últimas dos horas.

En modo de trayectoria adaptativa, esa distinción podrá construirse con una secuencia más rica de observaciones. T360 seguirá siendo útil como boundary/reference temporal, pero no necesariamente como el único punto que representa toda la historia previa.

Ejemplo:

```text
Ejemplo A
T360   T120   T30   T5
0.10   0.40   0.42  0.43

La mayor parte del movimiento ocurrió antes de T120.
```

```text
Ejemplo B
T360   T120   T30   T5
0.10   0.12   0.30  0.43

La mayor parte del movimiento ocurrió después de T120.
```

El estado final T5 puede ser casi igual mientras la trayectoria estructural es distinta.

P4 DEBE conservar esa diferencia.

---

# 4. P4 y System I deben compartir la misma verdad temporal

La arquitectura objetivo es:

```text
                   P2 SIDE @ punto operativo
                             │
                             │
T360 → T120 → T30 → T5 ─────┼────→ SYSTEM I
                             │
                             │
                   P3 O/U @ punto operativo
```

Interpretación operativa:

```text
P2 = cómo se ve SIDE en el punto de decisión de System I
P3 = cómo se ve TOTALS en el punto de decisión de System I
P4 = cómo llegaron esos mercados hasta ese punto
```

El pipeline transmite su `evaluation_minute` a P4 como `target_minute`. P2 y P3 conservan sus propias políticas de selección; P4 exige el target exacto y no retrocede silenciosamente a otro checkpoint.

---

# 5. Los datos post-signal forman una capa separada

Todo target posterior a `Tn` queda fuera del perfil operativo P4. Con la configuración actual `Tn = T5`, esto incluye:

```text
T1
T0
T-5
```

Se clasifican como:

```text
POST-SIGNAL / AUDIT / MINING DATA
```

Posteriormente pueden utilizarse para estudiar:

- qué hizo el mercado después de la señal Tn;
- si el movimiento continuó;
- si corrigió;
- si revirtió;
- si aceleró;
- qué ocurrió en el closing;
- qué ocurrió inmediatamente después del inicio nominal.

NO DEBEN modificar el perfil operativo P4 producido para Tn.

Separación canónica:

```text
              PRE-SIGNAL / OPERATIVO

T360 → T120 → T30 → T5
                      │
                      ▼
             P4_SIGNAL_PROFILE
                      │
                      ▼
                 SYSTEM I

──────────────────────┼──────────────────────

                 DESPUÉS DE LA SEÑAL

                 T1 → T0 → T-5
                      │
                      ▼
          POST_SIGNAL_MARKET_AUDIT
                      │
                      ▼
               AUDIT / MINING
```

Distinción importante:

Cuando `Tn = T5`, `T1` sigue siendo un minuto antes del inicio del evento, pero es **post-signal** porque la lectura de System I ya ocurrió en T5. Para otro `n`, la frontera se desplaza con el target solicitado.

Por tanto la frontera correcta es **el momento de la señal**, no simplemente kickoff.

---

# 6. Prohibición absoluta de leakage temporal

Invariante canónico:

> Ninguna información que se vuelva disponible después de la lectura operativa Tn puede modificar el output P4 que System I consumió en Tn.

Por tanto:

```text
T1 / T0 / T-5
≠ inputs operativos P4
```

y:

```text
cuota post-signal
≠ sustituto de cuota pre-signal
```

Cualquier lógica legacy equivalente a:

```python
effective_kickoff_odds = post_kickoff_odds
```

cuando la observación posterior difiere del valor pre-match/closing NO DEBE formar parte del P4 operativo.

Una observación posterior puede serializarse como auditoría, por ejemplo:

```text
POST_KICKOFF_CHANGED = TRUE
```

pero no puede sobrescribir el valor utilizado por System I.

---

# 7. Contrato directo de función P4

El entry point actual es:

```python
calculate_pillar_4(
    event_context: EventContext,
    odds_trajectory_context: OddsTrajectoryContext | None = None,
    *,
    target_minute: int,
    debug_mode: bool = False,
)
```

`target_minute` es explícito para impedir que P4 adivine o hardcodee la frontera operativa. El pipeline le pasa su `evaluation_minute`.

Inputs directos:

1. `EventContext`
2. `OddsTrajectoryContext`
3. `target_minute`, decidido por el pipeline

P4 NO DEBE requerir parámetros externos individuales para:

- odds,
- lines,
- bookmakers,
- la lista de checkpoints históricos,
- timestamps,
- exchange sizes,
- market names,
- choice names,
- etc.

Esos valores se derivan de los dos objetos de contexto que ya construye `pillar_pipeline.py`.

`streak_analysis` no forma parte del contrato de P4.

P4 NO DEBE consultar cuotas directamente a proveedores.

P4 NO DEBE consultar repositorios/base de datos para reconstruir observaciones faltantes si el `OddsTrajectoryContext` canónico no las contiene.

El contexto es la fuente de verdad entregada por el pipeline.

---

# 8. Tres niveles arquitectónicos de entrada

La implementación y documentación DEBEN distinguir tres niveles.

## Nivel 1 — Objetos directos

```text
EventContext
OddsTrajectoryContext
```

Son los inputs reales de función.

---

## Nivel 2 — Variables mínimas derivadas

Se extraen del Nivel 1.

Ejemplos:

```text
event_id
sport
starts_at

market_group
market_period
market_name
choice_group_key

bookie_id
bookie_name
source

exchange_side
exchange_level

choice_name
choice_id
main_line

odds_price
target_minute

snapshot_id
quote_id
collected_at
minutes_before_start
distance_from_target
changed_at
exchange_size
```

Estas NO son parámetros independientes de función.

---

## Nivel 3 — Métricas/features P4

Ejemplos:

```text
DELTA_RAW
ABS_DELTA_RAW
DIRECTION_BY_SIGN

NET_MOVE_RAW
PATH_LENGTH_RAW
PATH_EFFICIENCY_RAW

SIGN_CHANGE_COUNT_RAW
PATH_PATTERN_RAW
TURNING_POINTS_RAW

CORRECTION_RATIO_RAW
MOVE_RETENTION_RAW

VELOCITY_RAW
VELOCITY_CHANGE_RAW
ABS_VELOCITY_CHANGE_RAW

MOVE_SHARE_BY_LEG
DOMINANT_SEGMENT_RAW

EXCHANGE_SIZE_DELTA_RAW
```

Los valores de Nivel 3 DEBEN ser reproducibles desde los valores de Nivel 2.

---

# 9. Contrato físico actual de inputs de P4

`pillar_pipeline.py` ya dispone de los dos objetos directos requeridos por P4:

```text
EventContext
OddsTrajectoryContext
```

El segundo puede recibirse como argumento explícito de `calculate_pillar_4()` o, como fallback de integración, mediante `event_context.odds_trajectory_context`. Si ambos existen, el argumento explícito es la fuente seleccionada por el entrypoint.

La matemática de P4 DEBE consumir una abstracción propia (`TemporalSeries` / `TemporalPoint`) producida por una capa de extracción. Esa capa debe leer el contrato físico que sigue; no debe inventar nombres futuros ni consultar repositorios para completar datos.

## 9.1 `EventContext`

Contrato actual:

```python
EventContext(
    event_id: int,
    custom_id: str | None,
    sport: str,
    season_id: int | None,
    season_name: str | None,
    season_year: int | None,
    starts_at: datetime,
    minutes_until_start: int | None,
    discovery_source: str | None,
    home: ParticipantContext,
    away: ParticipantContext,
    competition: CompetitionContext,
    participants_label: str,
    context_status: str,
    slug: str | None,
    gender: str | None,
    country: str | None,
    round: str | None,
    observations: list[dict],
    odds_response: dict | None,
    odds_trajectory: list[dict],
    odds_trajectory_context: OddsTrajectoryContext | None,
    ft_1x2_odds_trajectory_context: OddsTrajectoryContext | None,
    streak_analysis: Any | None,
    should_send_streak_alert: bool,
    dual_report: Any | None,
    competition_metadata_resolved: bool,
    success: bool,
    alert_sent: bool,
    created_at: datetime | None,
    updated_at: datetime | None,
)
```

`starts_at` conserva ese nombre por compatibilidad; actualmente su valor está configurado con hora de México. P4 NO DEBE inferir por el nombre que el objeto es timezone-aware UTC: debe usar el valor y la convención temporal entregados por el pipeline de forma consistente con el formatter.

`home` y `away` usan:

```python
ParticipantContext(
    participant_id: int | None,
    source: str | None,
    source_participant_id: int | None,
    name: str,
    slug: str | None,
    short_name: str | None,
    source_status: str,
    code_name: str | None,
    snapshot_ranking: int | None,
    created_at: datetime | None,
    updated_at: datetime | None,
)
```

`competition` usa:

```python
CompetitionContext(
    competition_id: int | None,
    source: str | None,
    source_tournament_id: int | None,
    source_unique_tournament_id: int | None,
    canonical_name: str | None,
    display_name: str,
    slug: str | None,
    unique_slug: str | None,
    category_id: int | None,
    category_name: str | None,
    number_of_teams: int | None,
    number_of_teams_source: str | None,
    total_regular_season_games: int | None,
    standings_grouping: str | None,
    league_config_source: str | None,
    has_standings_source_endpoint: bool | None,
    source_status: str,
    standings_response: list | None,
    source_tournament_name: str | None,
    source_unique_tournament_name: str | None,
    created_at: datetime | None,
    updated_at: datetime | None,
)
```

Para P4, sus campos relevantes de identidad/contexto son `event_id`, `sport`, `participants_label`, `starts_at`, `minutes_until_start`, participantes y competencia. Los demás campos se conservan para integración y auditoría, pero no son parámetros analíticos independientes.

`EventContext` contiene dos representaciones temporales distintas:

```text
odds_trajectory
    raw serializado del repositorio

odds_trajectory_context
    jerarquía tipada y transformada para consumo de pilares
```

La extracción analítica de P4 DEBE usar `OddsTrajectoryContext`. `EventContext.odds_trajectory` PUEDE conservarse en `raw` para auditoría, pero no debe convertirse en una segunda autoridad analítica.

## 9.2 Trayectoria raw: `OddsTrajectoryPoint`

Cada elemento de `EventContext.odds_trajectory` es la forma serializada de:

```python
OddsTrajectoryPoint(
    event_id: int,
    market_id: int | None,
    canonical_market_key: str | None,
    market_family: str | None,
    market_display_order: int | None,
    market_name: str | None,
    market_group: str | None,
    market_period: str | None,
    choice_group: str | None,
    bookie_id: int | None,
    bookie_name: str | None,
    choice_id: int | None,
    choice_name: str | None,
    choice_display_order: int | None,
    quote_id: int | None,
    source: str | None,
    exchange_side: str | None,
    exchange_level: int | None,
    initial_odds: Decimal | None,
    odds_value: Decimal | None,
    snapshot_id: int | None,
    source_collected_at: datetime | None,
    collected_at: datetime | None,
    observed_minutes_before_start: int | None,
    trajectory_minutes_before_start: Decimal | None,
    main_line: bool | None,
    source_limit: Decimal | None,
    exchange_size: Decimal | None,
)
```

Los dos campos de minuto NO son aliases:

- `observed_minutes_before_start` es un entero redondeado desde `collected_at` y sirve para la proyección compatible con targets configurados;
- `trajectory_minutes_before_start` es un `Decimal` de seis decimales calculado desde `source_collected_at`, con fallback a `collected_at`, y conserva el momento preciso del proveedor.

## 9.3 `OddsTrajectoryContext` y jerarquía de mercados

Contrato actual:

```python
OddsTrajectoryContext(
    available: bool,
    event_id: int | None,
    target_minutes_expected: list[int],
    target_minutes_present: list[int],
    missing_target_minutes: list[int],
    markets: dict,
)
```

`available=True` significa que existe al menos un snapshot válido; no implica que todos los targets configurados estén presentes.

`target_minutes_present` y `missing_target_minutes` describen exclusivamente la proyección de targets configurados. Los momentos arbitrarios de la trayectoria completa nunca se agregan a esas listas.

La jerarquía de `markets` es:

```text
markets
└── market_group
    └── market_period
        └── market_name
            └── choice_group_key
                └── MarketLineOddsTrajectory
                    └── bookies
                        └── bookie_key
                            └── BookieOddsTrajectory
                                └── choices
                                    └── choice_name
                                        └── ChoiceOddsTrajectory
```

Los nodos tipados son:

```python
MarketLineOddsTrajectory(
    market_id: int | None,
    market_name: str,
    market_group: str,
    market_period: str,
    choice_group: str | None,
    bookies: dict[str, BookieOddsTrajectory],
)

BookieOddsTrajectory(
    bookie_id: int | None,
    bookie_name: str,
    source: str | None,
    exchange_side: str | None,
    exchange_level: int,
    choices: dict[str, ChoiceOddsTrajectory],
)

ChoiceOddsTrajectory(
    choice_name: str,
    choice_id: int | None,
    initial_odds: Decimal | None,
    quote_id: int | None,
    main_line: bool | None,
    odds_values: dict[int, Decimal],
    meta_by_minute: dict[int, OddsPointMeta],
    snapshots: list[OddsSnapshotPoint],
)
```

Para bookmakers regulares, `exchange_side` normalmente es `None`. Para exchange, normalmente es `back` o `lay`; `exchange_level=0` identifica el mejor nivel disponible.

## 9.4 Dos canales temporales dentro de cada choice

`ChoiceOddsTrajectory` ya contiene ambos canales que P4 necesita:

```text
odds_values + meta_by_minute
    proyección por checkpoints configurados

snapshots
    trayectoria persistida completa del proveedor
```

`odds_values` y `meta_by_minute` usan como keys únicamente targets configurados. Sus mapas se ordenan en minuto descendente; una configuración típica es `120, 30, 5, 1, 0, -5`.

`snapshots` puede contener cualquier número de puntos, momentos no enteros y múltiples puntos dentro del mismo minuto. El formatter conserva todos los snapshots retornados por el repositorio sin deduplicarlos por minuto, precio ni timestamp. Cualquier deduplicación checkpoint-aware entre un momento histórico idéntico y una observación actual ocurre upstream, durante ingesta.

P4 DEBE construir:

```text
ADAPTIVE_VIEW   ← ChoiceOddsTrajectory.snapshots, filtrados causalmente hasta T5
CHECKPOINT_VIEW ← odds_values[target] + meta_by_minute[target]
```

No debe reconstruir `ADAPTIVE_VIEW` desde los mapas de targets ni reducir `snapshots` a targets configurados.

## 9.5 Metadata de checkpoint y punto preciso

```python
OddsPointMeta(
    snapshot_id: int | None,
    collected_at: datetime | None,
    minutes_before_start: int | None,
    target_minute: int,
    distance_from_target: int | None,
    quote_id: int | None,
    changed_at: datetime | None,
    exchange_size: Decimal | None,
)

OddsSnapshotPoint(
    snapshot_id: int | None,
    quote_id: int | None,
    odds_value: Decimal,
    collected_at: datetime | None,
    source_collected_at: datetime | None,
    minutes_before_start: Decimal | None,
    source_limit: Decimal | None,
    exchange_size: Decimal | None,
)
```

Mapeos y autoridades temporales:

```text
OddsPointMeta.changed_at = source_collected_at
OddsPointMeta.minutes_before_start = minuto entero observado desde collected_at
OddsSnapshotPoint.minutes_before_start = minuto preciso desde source_collected_at or collected_at
```

Para velocidad, orden causal y `as-of`, los timestamps raw son autoritativos. El `Decimal minutes_before_start` es un eje start-relative conveniente, no un sustituto del timestamp.

Los snapshots se ordenan por:

1. `source_collected_at`, con fallback a `collected_at`;
2. `collected_at`;
3. `snapshot_id`.

## 9.6 Selección de checkpoints configurados

Para cada target, el formatter elige el candidato con menor distancia respecto a `observed_minutes_before_start`, dentro de tolerancia. En empate, el ranking es:

1. menor distancia al target;
2. `source_collected_at DESC` mediante `OddsPointMeta.changed_at`;
3. `collected_at DESC`;
4. `snapshot_id DESC`.

Así, `collected_at` determina cercanía al checkpoint observado y `source_collected_at` escoge el estado de proveedor más fresco entre candidatos igualmente cercanos.

Cuando se suministra `evaluation_minute`, solo se proyectan targets con `target_minute >= evaluation_minute`. La selección compartida elige después el candidato causal más cercano al kickoff sin cruzar el límite de evaluación.

## 9.7 Frontera de consumo P2/P3 y P4

P2 y P3 no recorren `snapshots`; consumen exclusivamente:

```python
choice.odds_values[target_minute]
choice.meta_by_minute[target_minute]
```

El pipeline elige un único `TargetMinuteSelection`, y ambos pilares reciben esa misma selección.

P4 tiene una responsabilidad distinta: consume la trayectoria completa para `ADAPTIVE_VIEW` y la proyección de targets para `CHECKPOINT_VIEW`, conservando la misma lectura operativa T5. Añadir un punto arbitrario como T83.75 no debe cambiar P2/P3 salvo que ese row gane la proyección de un target configurado.

## 9.8 Adapter P4 y endpoint T5

La arquitectura requerida es:

```text
OddsTrajectoryContext
        │
        ▼
extract_p4_trajectory_inputs(...)
        │
        ├── snapshots → ADAPTIVE TemporalSeries[]
        ├── odds_values/meta_by_minute → CHECKPOINT TemporalSeries[]
        ├── normalización de identidad
        ├── filtro causal as-of T5
        └── diagnostics
        │
        ▼
pure temporal calculators
```

El adapter DEBE preservar series independientes por mercado, contrato, bookmaker, exchange side/level y choice. Dos choices pueden tener cantidades y timestamps diferentes.

La lectura T5 de `odds_values[5]` + `meta_by_minute[5]` es el endpoint operativo canónico. Si el último snapshot adaptativo anterior tiene el mismo valor pero otro timestamp/identidad, el adapter DEBE añadir T5 como `OPERATIVE_ENDPOINT`; ese punto no debe etiquetarse como `ADAPTIVE_CHANGE`. Solo se evita duplicación física cuando snapshot y endpoint comparten la misma identidad de observación (`snapshot_id`/timestamp autoritativo), sin perder que T5 es el cierre contractual.

Los calculadores NO DEBEN asumir exactamente `T360/T120/T30/T5`: deben aceptar series de longitud variable y gaps explícitos.

## 9.9 Reducción upstream y límites analíticos

El histórico persistido puede haber sido reducido upstream por serie mediante `modules/oddspapi/historical_odds_change_detector.py`. Esa política pertenece a ingesta/persistencia, no a la matemática RAW de P4.

Por tanto:

```text
upstream significant-change threshold
≠ P4 MIN_MOVE_THRESHOLD
```

P4 NO DEBE volver a aplicar un threshold para transformar deltas observados en cero:

```text
delta > 0 → +1
delta < 0 → -1
delta = 0 → 0
```

P4 solo puede describir los snapshots persistidos que recibió. La trazabilidad debe conservar `TRAJECTORY_SOURCE_MODE` y, cuando exista en el contrato real, versión/policy del detector. La ausencia actual de metadata de policy se diagnostica; no autoriza a P4 a inventarla.

# 10. Convención canónica de nombres de variables

## 10.1 Regla general

P4 DEBE usar el vocabulario ya establecido por P2/P3 para las mismas observaciones de mercado cuando exista una variable equivalente.

Ejemplos ya utilizados:

```text
PIN_HOME_1X2_FULL_TIME_ODDS_PRICE
PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE

B365_HOME_1X2_FULL_TIME_ODDS_PRICE
B365_AWAY_1X2_FULL_TIME_ODDS_PRICE

PIN_AH_FULL_TIME_LINE
PIN_AH_HOME_FULL_TIME_ODDS_PRICE
PIN_AH_AWAY_FULL_TIME_ODDS_PRICE

BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE
BF_HOME_LAY_1X2_FULL_TIME_ODDS_PRICE
BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE
BF_DRAW_LAY_1X2_FULL_TIME_ODDS_PRICE
BF_AWAY_BACK_1X2_FULL_TIME_ODDS_PRICE
BF_AWAY_LAY_1X2_FULL_TIME_ODDS_PRICE

BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE
BF_HOME_LAY_1X2_FULL_TIME_EXCHANGE_SIZE

PIN_FT_OU_LINE
PIN_FT_OVER_ODDS
PIN_FT_UNDER_ODDS

B365_FT_OU_LINE
B365_FT_OVER_ODDS
B365_FT_UNDER_ODDS

BF_OU_FULL_TIME_LINE
BF_OU_BACK_OVER_FULL_TIME_ODDS_PRICE
BF_OU_BACK_UNDER_FULL_TIME_ODDS_PRICE
BF_OU_LAY_OVER_FULL_TIME_ODDS_PRICE
BF_OU_LAY_UNDER_FULL_TIME_ODDS_PRICE
```

Los componentes conceptuales son:

```text
BOOKIE
MARKET / MARKET FAMILY
EXCHANGE SIDE cuando aplique
CHOICE
MARKET PERIOD
VALUE TYPE
```

El orden exacto de tokens serializados DEBERÍA seguir el vocabulario ya usado por P2/P3 para esa familia de mercado, en lugar de renombrar inputs existentes solamente para imponer una nueva uniformidad.

---

## 10.2 `DRAW` y `X`

En 1X2:

```text
HOME = 1
DRAW = X
AWAY = 2
```

`DRAW` es una choice real del mercado.

En código y fórmulas del blueprint, los placeholders algebraicos genéricos NO DEBERÍAN utilizar la letra `X`, porque puede confundirse con el empate en fútbol.

Las fórmulas genéricas de este documento utilizarán:

```text
SERIES_VALUE
```

o un nombre explícito.

Ejemplos:

```text
PIN_DRAW_1X2_FULL_TIME_ODDS_PRICE
B365_DRAW_1X2_FULL_TIME_ODDS_PRICE
BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE
```

cuando estén disponibles en el contexto.

---

## 10.3 Representación temporal

La arquitectura interna preferida no debe crear una clase completamente distinta de variable para cada target.

En su lugar:

```text
input_name = PIN_HOME_1X2_FULL_TIME_ODDS_PRICE
target_minute = 120
value = 2.10
```

Conceptualmente:

```text
PIN_HOME_1X2_FULL_TIME_ODDS_PRICE[T120]
```

Una serialización plana PUEDE usar un sufijo `_T120`, pero la implementación DEBERÍA conservar iteración genérica sobre targets.

El diseño NO DEBE hardcodear cientos de variables temporales si un modelo estructurado de series resuelve el problema.

---

# 11. Identidad de mercado y `choice_group_key`

En mercados con línea/handicap, `choice_group_key` forma parte de la identidad contractual.

P4 DEBE conservarlo durante:

- extracción,
- trazabilidad,
- comparación temporal.

P4 NO DEBE fusionar silenciosamente observaciones pertenecientes a distintos `choice_group_key`.

Cada serie debería poder identificarse al menos mediante:

```text
market_group
market_period
market_name
choice_group_key
bookie
choice
exchange_side si aplica
exchange_level si aplica
```

Esa identidad define exactamente qué se está siguiendo.

---

# 12. Contrato de main line

Upstream se espera que `OddsTrajectoryContext` contenga los mercados principales seleccionados y que las choices expongan:

```text
main_line = True
```

P4 NO DEBE inventar otro algoritmo de ranking de main line.

Si el contexto relevante produce más de un candidato válido para el mismo contrato canónico, el resultado es ambiguo.

La implementación DEBE seguir la misma filosofía ya usada por P2/P3:

```text
más de un candidato válido
→ AMBIGUOUS
→ no fusionar
→ no devolver estado exitoso ACTIVE para ese cálculo afectado
```

No se puede elegir un candidato solo porque aparece primero.

No se puede promediar líneas ambiguas.

No se puede sustituir un bookmaker por otro.

---

# 13. La arquitectura de extracción de P4 debe reflejar P2/P3

Flujo recomendado:

```text
run_pillar_4.py
        │
        ▼
P4 temporal extraction policy
        │
        ├── extrae identidades canónicas de mercado
        ├── valida candidato único
        ├── conserva missing / invalid / ambiguous diagnostics
        └── produce inputs temporales tipados
        │
        ▼
calculadores semánticos puros
        │
        ▼
calculadores temporales puros
        │
        ▼
trajectory/profile engine
        │
        ▼
P4_SIGNAL_PROFILE + raw audit
```

P4 DEBERÍA reutilizar infraestructura compartida cuando la semántica sea idéntica:

```text
market_snapshot_extractor
market_candidate_selection
market_coverage
market_audit
extraction_logging
```

P4 NO DEBE duplicar reglas de selección de candidatos ya centralizadas salvo que exista una necesidad temporal específica.

---

# 14. Estructura recomendada del módulo P4

Los nombres exactos de archivo no son obligatorios, pero las responsabilidades DEBERÍAN separarse aproximadamente así:

```text
modules/pillars/pillar_4/
│
├── __init__.py
│
├── run_pillar_4.py
│   └── orquestador público delgado
│
├── periods.py / scopes.py
│   └── identidades declarativas y vocabulario de inputs
│
├── models.py
│   └── DTOs de puntos / series / extracción temporal
│
├── snapshot_policy.py / trajectory_policy.py
│   └── extracción desde OddsTrajectoryContext
│
├── metrics.py
│   └── matemáticas temporales puras
│
├── semantic_metrics.py
│   └── wrappers/reutilización de cálculos P2/P3 cuando sea necesario
│
├── relations.py
│   └── relaciones/edges estructurales reutilizables
│
├── trajectory_engine.py
│   └── legs, path, patterns, momentum, correction
│
├── signal_models.py
│   └── DTOs serializables del perfil P4
│
├── signal_engine.py
│   └── ensamble del perfil estructural temporal
│
├── post_signal_audit.py
│   └── lógica opcional separada T5→T1→T0→T-5
│
└── mining adapter
    └── P4MiningAdapter para persistencia mediante PillarMiningRun
```

Funciones públicas/canónicas:

```python
calculate_pillar_4(
    event_context,
    odds_trajectory_context=None,
    debug_mode=False,
) -> dict

extract_p4_trajectory_inputs(
    event_context,
    odds_trajectory_context,
) -> P4ExtractionResult

build_p4_signal_profile(
    extraction_or_snapshot,
    debug_mode=False,
) -> P4SignalProfile

build_raw_audit(...) -> dict
```

`calculate_pillar_4()` ensambla el envelope. `extract_p4_trajectory_inputs()` devuelve series normalizadas y diagnostics. `build_p4_signal_profile()` crea un único DTO con `to_dict()`, fuente canónica de `P4_SIGNAL_PROFILE`. `build_raw_audit()` conserva inputs y trazabilidad, pero no produce un segundo resultado analítico.

La obligación real no es el filename.

La obligación es mantener separadas:

- extracción,
- matemáticas puras,
- ensamble del perfil,
- auditoría post-signal.

---

# 15. Reutilización de semántica matemática de P2/P3

P4 NO DEBE crear definiciones ligeramente distintas de edges ya definidos por P2/P3.

## SIDE edge

```text
home_raw = 1 / home_price
away_raw = 1 / away_price

SIDE_EDGE =
(home_raw - away_raw)
/
(home_raw + away_raw)
```

Esto genera un eje estructural HOME/AWAY con signo.

Conceptualmente:

```text
SIDE_EDGE > 0 → HOME
SIDE_EDGE < 0 → AWAY
SIDE_EDGE = 0 → neutral
```

Es una métrica estructural, no una predicción.

---

## O/U edge

```text
over_raw = 1 / over_price
under_raw = 1 / under_price

OU_EDGE =
(over_raw - under_raw)
/
(over_raw + under_raw)
```

Conceptualmente:

```text
OU_EDGE > 0 → OVER
OU_EDGE < 0 → UNDER
OU_EDGE = 0 → neutral
```

De nuevo: estructura, no predicción.

---

## Primitivas compartidas

Cuando aplique, P4 DEBERÍA reutilizar la implementación canónica de:

```text
side_edge
ou_edge
absolute_gap
pair_mean
relative_spread
```

en lugar de copiar fórmulas en una implementación independiente.

Si para reutilizarlas conviene extraerlas a un módulo más compartido, el Dev debe proponer ese refactor.

P2 y P3 DEBEN seguir siendo engines estructurales de snapshot.

NO DEBERÍAN ser llamados repetidamente como engines temporales.

Arquitectura preferida:

```text
                  OddsTrajectoryContext
                           │
                           ▼
               shared pure calculators
                           │
             ┌─────────────┼─────────────┐
             │             │             │
             ▼             ▼             ▼
            P2            P3            P4
       SIDE snapshot   O/U snapshot   trajectory
```

---

# 16. Distinción importante: dirección RAW de precio vs dirección semántica

El signo del cambio de una cuota decimal individual no equivale automáticamente a HOME/AWAY u OVER/UNDER.

Ejemplo:

```text
HOME odds
2.10 → 2.20

raw odds delta = +0.10
```

Ese delta positivo no significa “movimiento hacia HOME”.

Por tanto P4 DEBE distinguir:

1. trayectoria RAW de precio;
2. trayectoria semántica del mercado.

Etiquetas como:

```text
HOME_TO_AWAY_REVERSAL
AWAY_TO_HOME_REVERSAL
OVER_TO_UNDER_REVERSAL
UNDER_TO_OVER_REVERSAL
```

solo pueden generarse a partir de una métrica con eje semántico de signo estable, por ejemplo los edges canónicos P2/P3.

NO DEBEN inferirse del signo de la variación de una sola cuota decimal.

---

# 17. Cobertura temporal de mercados

P4 debe seguir las mismas familias centrales que System I lee estructuralmente a través de P2/P3, sujeto a disponibilidad histórica real.

## 17.1 SIDE — scope inicial P4 v4

Ramas deseadas cuando existan datos:

```text
Pinnacle FT 1X2
bet365 FT 1X2

Pinnacle FT AH
bet365 FT AH

Betfair FT 1X2 BACK
Betfair FT 1X2 LAY

Betfair FT AH BACK/LAY si existe

ramas 1H equivalentes cuando realmente existan históricamente
```

El Dev DEBE confirmar cobertura real.

P4 NO DEBE asumir que una fuente/mercado existe solo porque el schema puede representarla.

---

## 17.2 TOTALS — scope inicial P4 v4

Ramas deseadas cuando existan datos:

```text
Pinnacle FT O/U
bet365 FT O/U

Betfair FT O/U BACK/LAY si existe

Pinnacle 1H O/U si existe
bet365 1H O/U si existe
Betfair 1H O/U si existe
```

P3 permanece como engine estructural O/U del snapshot operativo.

P4 estudia la evolución temporal de la misma semántica.

---

## 17.3 Standard Handicap

Decisión canónica:

```text
Standard Handicap
→ FUERA DEL SCOPE INICIAL DE P4 v4
```

Debe permanecer separado de `Asian Handicap`.

No se permite:

```text
Handicap
→ relabel como AH
```

Si la auditoría técnica del Dev demuestra que Standard Handicap ya es una rama contractual plenamente utilizada por el flujo real de System I y que excluirla produciría una incompatibilidad concreta, el Dev debe reportar esa evidencia.

En ese caso se evaluará su incorporación como extensión explícita y separada.

Hasta que exista esa incompatibilidad demostrada, el scope inicial de P4 se concentra en:

```text
1X2
Asian Handicap
Over/Under
Betfair exchange asociado cuando exista
```

# 18. Orden temporal: trayectorias adaptativas y checkpoints

P4 debe soportar dos formas de ordenar observaciones.

## 18.1 Trayectoria adaptativa

Los puntos se ordenan cronológicamente por timestamp real.

Conceptualmente:

```text
P1 → P2 → P3 → ... → Pn → T5
```

donde cada `Pi` representa una observación persistida de esa serie.

El orden debe basarse en:

```text
collected_at
```

o en el timestamp canónico equivalente que finalmente exponga `OddsTrajectoryContext`.

No existe obligación de que:

- dos series tengan el mismo número de puntos;
- dos series compartan timestamps;
- los puntos coincidan con T360/T120/T30.

## 18.2 Trayectoria de checkpoints/fallback

Los targets expresan minutos antes del evento.

Su orden cronológico correcto es:

```text
360 → 120 → 30 → 5
```

P4 NO DEBE ordenar numéricamente ascendente y producir:

```text
5 → 30 → 120 → 360
```

Implementación genérica para el modo checkpoint:

```python
ordered_targets = [360, 120, 30, 5]

for start_target, end_target in adjacent_pairs(ordered_targets):
    calculate_leg(start_target, end_target)
```

## 18.3 Regla común

Independientemente del source mode:

```text
earlier observation → later observation → ... → T5
```

debe ser la dirección temporal de cálculo.

Los calculadores puros deberían recibir una secuencia ya ordenada y NO encargarse de adivinar el modo upstream.

# 19. Ventanas operativas nombradas

Las ventanas temporales canónicas son:

```text
EARLY_WINDOW
T360 → T120

DEVELOPMENT_WINDOW
T120 → T30

LATE_WINDOW
T30 → T5
```

En la vista de checkpoints corresponden directamente a:

```text
EARLY_LEG
DEVELOPMENT_LEG
LATE_LEG
```

En una trayectoria adaptativa pueden existir múltiples observaciones dentro de una misma ventana.

Ejemplo:

```text
EARLY_WINDOW
P1 → P2 → P3

DEVELOPMENT_WINDOW
P4 → P5

LATE_WINDOW
P6 → P7 → T5
```

Los nombres describen tiempo.

No implican:

- valor predictivo;
- fuerza;
- calidad;
- importancia estadística.

## 19.1 Resumen canónico de una ventana adaptativa

Una ventana rica NO debe colapsarse únicamente a su movimiento neto.

Por cada ventana, cuando los datos permitan calcularlo, P4 debe conservar como mínimo:

```text
WINDOW_START_VALUE
WINDOW_END_VALUE

WINDOW_NET_MOVE
WINDOW_PATH_LENGTH

WINDOW_POINT_COUNT
WINDOW_LEG_COUNT
WINDOW_SIGN_CHANGE_COUNT

WINDOW_START_TIMESTAMP
WINDOW_END_TIMESTAMP
WINDOW_ELAPSED_MINUTES

WINDOW_NET_DIRECTION_RAW
WINDOW_PATTERN_RAW
```

Si dentro de una ventana ocurre:

```text
+0.10
-0.08
+0.18
```

entonces:

```text
WINDOW_NET_MOVE = +0.20
WINDOW_PATH_LENGTH = 0.36
```

Ambos valores son necesarios porque responden preguntas distintas.

## 19.2 `MOVE_SHARE_BY_WINDOW`

La distribución temporal del recorrido debe usar path, no net:

```text
MOVE_SHARE_BY_WINDOW =
WINDOW_PATH_LENGTH / TOTAL_PATH_LENGTH
```

si:

```text
TOTAL_PATH_LENGTH > 0
```

La razón es que el net puede cancelar gran parte del movimiento real ocurrido dentro de la ventana.

Si:

```text
TOTAL_PATH_LENGTH = 0
```

los shares deben ser `NULL`.

# 20. Política de transición de T360 y fallback

T360 es especialmente importante para el modo:

```text
FIXED_CHECKPOINT_FALLBACK
```

Durante rollout e históricos anteriores:

```text
T360 = NULL
```

NO DEBE producir automáticamente:

```text
P4 = totalmente insuficiente
```

si existen puntos posteriores válidos.

Ejemplo fallback:

```text
T360 = NULL
T120 = valid
T30  = valid
T5   = valid
```

debe conservar:

```text
EARLY_LEG = NULL
DEVELOPMENT_LEG = valid
LATE_LEG = valid
trajectory_partial = TRUE
```

En modo adaptativo, la ausencia de un punto llamado exactamente T360 no significa que falte la historia temprana.

Puede existir:

```text
P1 @ T410
P2 @ T275
P3 @ T160
...
```

Por tanto:

```text
T360 missing
```

solo tiene significado directo dentro de la proyección/checkpoint policy.

P4 NO DEBE marcar una trayectoria adaptativa como incompleta únicamente porque no existe una observación exactamente etiquetada T360.

# 21. Modelo genérico de serie temporal

P4 DEBERÍA tratar cada valor rastreable como una serie temporal de longitud variable.

Estructura conceptual:

```text
TemporalSeries
├── series_id / input_name
├── market identity
├── source_mode
├── semantic metric identity
├── observations[]     # 0..N
├── operative_endpoint_t5
├── legs[]             # derivados de observaciones ordenadas
├── window summaries
├── net metrics
├── path metrics
├── pattern metrics
├── momentum metrics
├── correction metrics
└── traceability
```

Ejemplos:

```text
PIN_HOME_1X2_FULL_TIME_ODDS_PRICE
```

o:

```text
PIN_1X2_FULL_TIME_SIDE_EDGE
```

o:

```text
BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE
```

Una serie puede venir de:

```text
ADAPTIVE_CHANGES
```

con número indeterminado de observaciones, o de:

```text
FIXED_CHECKPOINT_FALLBACK
```

con una secuencia limitada de checkpoints.

El temporal engine DEBERÍA operar genéricamente sobre `TemporalSeries`.

Esto es preferible a una función distinta por bookmaker/choice/target.

La estructura debe permitir que:

```text
len(HOME.points)
!= len(DRAW.points)
!= len(AWAY.points)
```

sin considerar eso por sí mismo un error.

# 22. Punto temporal RAW canónico

Cada punto usado por P4 debería conservar información suficiente para reconstruir su origen.

Modelo conceptual recomendado:

```text
TemporalPoint
├── input_name
├── value
├── observation_kind / source_mode
├── target_minute          # nullable/no aplicable en punto adaptativo
│
├── event_id
│
├── market_group
├── market_period
├── market_name
├── choice_group_key
│
├── bookie_id
├── bookie_name
├── source
│
├── exchange_side
├── exchange_level
│
├── choice_name
├── choice_id
├── main_line
│
├── snapshot_id
├── quote_id
├── collected_at
├── minutes_before_start
├── distance_from_target
├── changed_at
└── exchange_size
```

No todos los campos aplican a todas las series.

Campos no aplicables pueden ser `NULL`.

En especial, un punto adaptativo puede tener timestamp/minutos-before-start reales sin corresponder a un `target_minute` configurado.

La ausencia de `target_minute` en ese caso NO convierte la observación en inválida.

---

# 23. Deltas

La regla fundamental se aplica a observaciones consecutivas y comparables de la misma serie:

```text
DELTA_RAW_i =
SERIES_VALUE_i+1 - SERIES_VALUE_i
```

Para una trayectoria adaptativa:

```text
P1 → P2 → P3 → ... → Pn → T5
```

pueden existir:

```text
D1_RAW
D2_RAW
...
Dn_RAW
```

o, preferiblemente, una colección genérica de legs con:

```text
start_observation
end_observation
DELTA_RAW
ABS_DELTA_RAW
```

La numeración `D1/D2/D3` NO debe ser el contrato estructural fundamental del engine.

En el modo checkpoint/fallback, pueden conservarse aliases conceptuales:

```text
D1_RAW =
SERIES_VALUE_T120 - SERIES_VALUE_T360

D2_RAW =
SERIES_VALUE_T30 - SERIES_VALUE_T120

D3_RAW =
SERIES_VALUE_T5 - SERIES_VALUE_T30
```

Si falta un endpoint:

```text
dependent delta = NULL
```

Ejemplo fallback:

```text
T360 = NULL

D1_RAW = NULL
D2_RAW = valid si existen T120 y T30
D3_RAW = valid si existen T30 y T5
```

Persistir por leg:

```text
DELTA_RAW
ABS_DELTA_RAW = abs(DELTA_RAW)
```

No se permite threshold mínimo de movimiento dentro de P4.

El hecho de que la trayectoria adaptativa upstream ya haya sido reducida por una policy de cambios significativos NO autoriza a P4 a aplicar un segundo threshold.

# 24. Dirección por signo matemático

El threshold legacy interno de P4:

```text
MIN_MOVE_THRESHOLD = 0.02
```

NO DEBE decidir la dirección RAW.

Esto no debe confundirse con la policy upstream que decide qué cambios históricos se persisten.

Son capas diferentes:

```text
UPSTREAM CHANGE DETECTOR
→ decide qué observaciones sobreviven a la reducción histórica

P4
→ calcula fielmente sobre las observaciones recibidas
```

P4 no reevalúa si un punto persistido era "significativo".

Nueva regla:

```text
DELTA_RAW > 0 → DIRECTION_BY_SIGN = +1
DELTA_RAW < 0 → DIRECTION_BY_SIGN = -1
DELTA_RAW = 0 → DIRECTION_BY_SIGN = 0
```

Ejemplo:

```text
DELTA_RAW = +0.019
ABS_DELTA_RAW = 0.019
DIRECTION_BY_SIGN = +1
```

P4 DEBE conservar el movimiento.

Minería podrá determinar después si una magnitud como `0.019` se comporta como ruido.

P4 no debe tomar esa decisión empírica.

---

# 25. Secuencia de dirección de legs

Persistir la secuencia completa de signos de legs disponibles.

Ejemplos:

```text
[+1, +1, +1]
[-1, -1, -1]
[+1, -1, -1]
[-1, +1, -1]
[+1, 0, -1]
```

Los ceros exactos son estados reales y NO DEBEN desaparecer del RAW serializado.

---

# 26. Cambios de signo y zero bridging

P4 conserva siempre la secuencia RAW completa de direcciones, incluyendo ceros.

Ejemplo:

```text
LEG_DIRECTIONS_RAW = [+1, 0, -1]
```

Decisión canónica:

```text
+ 0 -
→ sí cuenta como reversal
```

El cero representa un plateau, no una ruptura semántica que borra el cambio de dirección posterior.

Para el conteo de cambios de signo:

1. conservar `LEG_DIRECTIONS_RAW` exactamente;
2. derivar conceptualmente la secuencia de estados direccionales no cero para detectar transición;
3. si dos estados no cero consecutivos en esa secuencia tienen signo diferente, existe un sign change;
4. registrar que el cambio atravesó uno o más legs cero cuando aplique.

Ejemplo:

```text
RAW_SEQUENCE
[+1, 0, -1]

NON_ZERO_DIRECTION_SEQUENCE
[+1, -1]

ZERO_BRIDGED_SIGN_CHANGE
TRUE

SIGN_CHANGE_COUNT_RAW
1

PATTERN_RAW
REVERSAL
```

También:

```text
+ 0 0 -
→ 1 sign change
```

y:

```text
+ 0 +
→ 0 sign changes
```

Los ceros siguen siendo parte de la información RAW y no deben eliminarse del output.

Esto permite que Minería distinga:

```text
reversal directo
```

de:

```text
reversal atravesando plateau
```

sin cambiar la definición estructural de reversal.

# 27. Patrón del path

Estados RAW permitidos:

```text
NO_MOVEMENT
UNIDIRECTIONAL
REVERSAL
MULTI_REVERSAL
```

Definición base cuando la semántica de sign changes sea inequívoca:

```text
todos los legs observados = 0
→ NO_MOVEMENT

SIGN_CHANGE_COUNT_RAW = 0
y existe al menos un leg no cero
→ UNIDIRECTIONAL

SIGN_CHANGE_COUNT_RAW = 1
→ REVERSAL

SIGN_CHANGE_COUNT_RAW >= 2
→ MULTI_REVERSAL
```

`CHOPPY` NO DEBERÍA ser el label matemático canónico.

Si se conserva por compatibilidad visual, debe ser alias de presentación y no el estado RAW central.

---

# 28. Movimiento neto y gaps no contiguos

P4 debe separar estrictamente dos verdades:

```text
NET
```

y:

```text
PATH
```

## 28.1 NET puede cruzar un gap

Si existen dos endpoints válidos y contractualmente comparables:

```text
NET_MOVE_RAW =
SERIES_VALUE_end - SERIES_VALUE_start
```

puede calcularse aunque falten observaciones intermedias.

Ejemplo:

```text
T360 = 0.10
T120 = NULL
T30  = 0.30
T5   = 0.40
```

Es válido afirmar:

```text
NET_MOVE_T360_T5 = +0.30
```

porque conocemos ambos extremos.

Persistir:

```text
NET_START_TARGET / NET_START_OBSERVATION
NET_END_TARGET / NET_END_OBSERVATION
NET_MOVE_RAW
NET_DIRECTION_RAW
```

Dirección neta:

```text
NET_MOVE_RAW > 0 → POSITIVE
NET_MOVE_RAW < 0 → NEGATIVE
NET_MOVE_RAW = 0 → ZERO
```

## 28.2 PATH no puede inventar lo ocurrido dentro del gap

En el ejemplo anterior no conocemos el recorrido entre T360 y T30.

Podría haber sido:

```text
0.10 → 0.15 → 0.30
```

o:

```text
0.10 → 0.50 → 0.05 → 0.30
```

Por tanto P4 NO puede inferir:

```text
PATH_LENGTH_T360_T30 = 0.20
```

solo a partir de endpoints separados por una observación faltante.

Regla:

```text
NET_MOVE
→ puede cruzar gaps si los endpoints existen

PATH_LENGTH
SIGN_CHANGE_COUNT
PATH_PATTERN
TURNING_POINTS
CORRECTION
VELOCITY_SEQUENCE
→ solo usan legs realmente observados/continuos
```

Persistir cuando aplique:

```text
GAP_PRESENT_RAW = TRUE
```

y suficiente metadata para identificar qué tramo no fue observado.

## 28.3 Consecuencia para `PATH_EFFICIENCY_RAW`

`PATH_EFFICIENCY_RAW` solo puede calcularse cuando `NET_MOVE_RAW` y `PATH_LENGTH_RAW` describen el mismo path continuo conocido.

No debe combinarse un `NET_MOVE` que cruza un gap con un `PATH_LENGTH` calculado únicamente sobre otra porción observada.

Cuando no exista un path continuo coherente entre los endpoints usados:

```text
PATH_EFFICIENCY_RAW = NULL
```

aunque el `NET_MOVE_RAW` global siga siendo válido.

# 29. Longitud del path

Para legs temporales válidos:

```text
PATH_LENGTH_RAW =
Σ abs(DELTA_RAW_valid_leg)
```

Ejemplo:

```text
D1 = +0.20
D2 = -0.10
D3 = +0.15

PATH_LENGTH_RAW =
0.20 + 0.10 + 0.15
= 0.45
```

Los legs faltantes no se convierten a cero.

La interacción con gaps no contiguos está gobernada por la regla canónica definida en la sección 28: el path solo usa legs observados/continuos.

---

# 30. Eficiencia del path

Cuando exista una definición coherente de path y:

```text
PATH_LENGTH_RAW > 0
```

calcular:

```text
PATH_EFFICIENCY_RAW =
abs(NET_MOVE_RAW) / PATH_LENGTH_RAW
```

Si:

```text
PATH_LENGTH_RAW = 0
```

usar:

```text
PATH_EFFICIENCY_RAW = NULL
NO_MOVEMENT_RAW = TRUE
```

No usar:

```text
PATH_EFFICIENCY_RAW = 1
```

cuando no hubo movimiento.

Una implementación final válida debería respetar:

```text
0 <= PATH_EFFICIENCY_RAW <= 1
```

cuando esté definida.

Cualquier política de gaps que pueda romper este invariante debe reconsiderarse.

---

# 31. Patrón vs dirección neta

P4 DEBE mantener separados:

- estructura del path;
- resultado neto de endpoints.

Ejemplo:

```text
T360→T120 = +0.30
T120→T30  = -0.10
T30→T5    = -0.05
```

Posible resultado:

```text
PATH_PATTERN_RAW = REVERSAL
NET_DIRECTION_RAW = POSITIVE
```

Son hechos distintos.

---

# 32. Turning points y turning zones

Para un cambio directo entre legs no cero:

```text
+ → -
= PEAK

- → +
= TROUGH
```

puede existir un turning point concreto en el checkpoint/observación compartida.

Ejemplo:

```json
{
  "type": "PEAK",
  "structure": "POINT",
  "observation": "P3"
}
```

## 32.1 Cambio atravesando plateau

Para:

```text
+ → 0 → -
```

o:

```text
+ → 0 → 0 → -
```

NO se debe inventar un instante exacto de giro.

Decisión canónica:

```text
TURNING_STRUCTURE = PLATEAU
```

representado como una zona/intervalo de giro.

Nombre conceptual:

```text
TURNING_ZONE
```

Ejemplo:

```json
{
  "type": "PEAK",
  "structure": "PLATEAU",
  "start_observation": "P2",
  "end_observation": "P4"
}
```

La forma final del DTO puede variar, pero debe conservar:

```text
tipo de giro
inicio de plateau
fin de plateau
estructura = PLATEAU
```

No elegir arbitrariamente el primer o último punto plano como “el pico”.

La misma regla aplica a un `TROUGH`.

# 33. Corrección basada en directional runs

Correction se define sobre **directional runs**, no sobre legs aislados.

## 33.1 Construcción de runs

A partir de la secuencia de legs observados y continuos:

1. agrupar legs consecutivos del mismo signo no cero;
2. los legs cero/plateau se conservan en RAW y se asocian a la estructura de transición sin crear un nuevo run direccional por sí mismos;
3. cada run almacena:
   - signo;
   - legs incluidos;
   - movimiento neto del run;
   - movimiento absoluto;
   - timestamps/observaciones de inicio y fin.

Ejemplo:

```text
+0.10
+0.08
-0.05
-0.07
+0.03
```

produce:

```text
RUN 1 = +0.18
RUN 2 = -0.12
RUN 3 = +0.03
```

## 33.2 `INITIAL_RUN`

El primer directional run no cero es:

```text
INITIAL_RUN
```

## 33.3 `CORRECTION_RUN`

El primer directional run posterior con signo opuesto a `INITIAL_RUN` es:

```text
CORRECTION_RUN
```

Los runs posteriores se conservan como nuevas fases de trayectoria.

No se deben perder para forzar la trayectoria a solo “initial + correction”.

Persistir como mínimo:

```text
DIRECTIONAL_RUNS_RAW

INITIAL_RUN_RAW
INITIAL_RUN_ABS

CORRECTION_RUN_RAW
CORRECTION_RUN_ABS

INITIAL_MOVE_SEGMENTS
CORRECTION_SEGMENTS
```

## 33.4 Ratio

Si:

```text
INITIAL_RUN_ABS > 0
```

entonces:

```text
CORRECTION_RATIO_RAW =
CORRECTION_RUN_ABS / INITIAL_RUN_ABS
```

Ejemplo:

```text
INITIAL_RUN = +0.18
CORRECTION_RUN = -0.12

CORRECTION_RATIO_RAW =
0.12 / 0.18
= 0.667
```

No aplicar threshold de significancia dentro de P4.

# 34. Move retention y overshoot

Con una correction válida:

```text
MOVE_RETENTION_RAW =
max(
    0,
    1 - CORRECTION_RATIO_RAW
)
```

Es una feature descriptiva.

NO DEBE multiplicar ni reducir automáticamente otra señal.

## 34.1 `CORRECTION_RATIO_RAW` no se capea

P4 debe conservar el ratio original incluso cuando:

```text
CORRECTION_RATIO_RAW > 1
```

Ejemplo:

```text
INITIAL_RUN_ABS = 0.18
CORRECTION_RUN_ABS = 0.25

CORRECTION_RATIO_RAW = 1.389
```

No convertirlo en `1.0`.

## 34.2 Overshoot

Cuando:

```text
CORRECTION_RUN_ABS > INITIAL_RUN_ABS
```

persistir:

```text
OVERSHOOT_RAW = TRUE
```

y:

```text
OVERSHOOT_MAGNITUDE_RAW =
CORRECTION_RUN_ABS - INITIAL_RUN_ABS
```

Ejemplo:

```text
OVERSHOOT_MAGNITUDE_RAW =
0.25 - 0.18
= 0.07
```

Cuando no existe overshoot:

```text
OVERSHOOT_RAW = FALSE
OVERSHOOT_MAGNITUDE_RAW = 0
```

si ambos runs son válidos.

Si no existe una correction calculable:

```text
OVERSHOOT_RAW = NULL
OVERSHOOT_MAGNITUDE_RAW = NULL
```

# 35. Retirar `RAW_MARKET_TRANSFER_PP` y unificar implied probability

El concepto compuesto legacy:

```text
RAW_MARKET_TRANSFER_PP =
IMPLIED_PROB_MOVE
× MOVE_RETENTION
× 100
```

NO DEBE permanecer como output canónico P4.

El problema no es `×100`.

El problema es asumir sin Minería que:

```text
IMPLIED_PROB_MOVE × MOVE_RETENTION
```

es la transformación predictiva correcta.

P4 debe conservar componentes independientes.

## 35.1 Semántica canónica de implied probability

P4 NO crea una definición propia.

Debe reutilizar exactamente la semántica matemática ya usada por P2/P3 al convertir una cuota decimal en su componente recíproco:

```text
RAW_IMPLIED_PROBABILITY =
1 / decimal_odds
```

si:

```text
decimal_odds > 0
```

Esta primitive debería vivir en un módulo matemático compartido si actualmente no existe como función pública reutilizable.

P4 NO debe conservar una función privada alternativa con comportamiento distinto.

Cuando una probabilidad normalizada/de-vig requiera el mercado completo, debe utilizarse únicamente una primitive compartida/canónica que aplique exactamente la misma semántica que P2/P3.

No mezclar:

```text
raw 1/odds
```

con:

```text
normalized market probability
```

bajo el mismo field name.

Fields recomendados cuando apliquen:

```text
RAW_IMPLIED_PROB_START
RAW_IMPLIED_PROB_END
RAW_IMPLIED_PROB_MOVE

NORMALIZED_IMPLIED_PROB_START
NORMALIZED_IMPLIED_PROB_END
NORMALIZED_IMPLIED_PROB_MOVE
```

Los campos normalizados solo existen si el contrato completo necesario está disponible.

`RAW_MARKET_TRANSFER_PP` queda eliminado.

# 36. Confidence no es un campo RAW de P4

Retirar categorías como:

```text
DRIFT_CONFIDENCE = HIGH
DRIFT_CONFIDENCE = MEDIUM
DRIFT_CONFIDENCE = LOW
```

Son valoraciones empíricas.

P4 debe serializar hechos estructurales observables.

La documentación puede indicar:

```text
predictive_interpretation = MINING_PENDING
```

pero P4 NO DEBERÍA conservar un pseudo-campo `confidence` cuyo único valor sea `MINING_PENDING`.

---

# 37. Strength no es un campo RAW de P4

P4 NO DEBE emitir:

```text
P4_STRENGTH
STRONG
VERY_STRONG
WEAK
STRONG_REVERSAL
WEAK_REVERSAL
CONFIRMED_REVERSAL
FALSE_REVERSAL
```

salvo que una futura calibración de Minería lo defina explícitamente y de forma versionada.

---

# 38. Distribución temporal del movimiento y dominancia

No clasificar timing mediante un threshold arbitrario como:

```text
>= 50% → LATE
```

Debe conservarse la distribución RAW.

## 38.1 Vista checkpoint

Por cada leg válido:

```text
ABS_MOVE_LEG =
abs(DELTA_RAW)
```

Con un path checkpoint continuo válido:

```text
MOVE_SHARE_LEG =
ABS_MOVE_LEG / PATH_LENGTH_CHECKPOINT
```

Ejemplo:

```json
{
  "EARLY_LEG": 0.12,
  "DEVELOPMENT_LEG": 0.20,
  "LATE_LEG": 0.68
}
```

## 38.2 Vista adaptativa por ventanas

Cuando exista Adaptive View:

```text
MOVE_SHARE_BY_WINDOW =
WINDOW_PATH_LENGTH / TOTAL_PATH_LENGTH_ADAPTIVE
```

según la regla definida en la sección 19.

## 38.3 Dominant segment/window y empates

La dominancia significa únicamente:

```text
máximo movimiento absoluto observado
```

sin threshold adicional.

Si existe un único máximo, puede persistirse una colección de un elemento.

Si existe empate, NO elegir arbitrariamente un segmento.

Persistir:

```text
DOMINANT_SEGMENTS_RAW = [...]
```

o para ventanas:

```text
DOMINANT_WINDOWS_RAW = [...]
```

Ejemplo:

```json
{
  "DOMINANT_SEGMENTS_RAW": [
    "EARLY_LEG",
    "LATE_LEG"
  ]
}
```

si ambos tienen exactamente la misma magnitud máxima.

El empate es información válida.

# 39. Tiempo real y velocidad

Velocity debe basarse en timestamps reales.

Esto es obligatorio conceptualmente tanto para:

```text
ADAPTIVE_CHANGES
```

como para:

```text
FIXED_CHECKPOINT_FALLBACK
```

Para cada leg comparable:

```text
ELAPSED_MINUTES_ACTUAL =
abs(
    collected_at_end - collected_at_start
)
```

Después:

```text
VELOCITY_RAW =
DELTA_RAW / ELAPSED_MINUTES_ACTUAL
```

si:

```text
ELAPSED_MINUTES_ACTUAL > 0
```

Si faltan timestamps:

```text
VELOCITY_RAW = NULL
```

En una trayectoria adaptativa, los intervals pueden ser completamente irregulares:

```text
P1→P2 = 47 min
P2→P3 = 11 min
P3→P4 = 83 min
P4→T5 = 6 min
```

Eso es comportamiento esperado, no un error.

En modo checkpoint, P4 NO DEBE inventar que el elapsed fue exactamente:

```text
240 min
90 min
25 min
```

solo porque los labels sean:

```text
T360
T120
T30
T5
```

cuando existe `collected_at`.

`target_minute` describe el checkpoint nominal.

`collected_at` gobierna la velocidad real.

# 40. Cambio de velocidad / aceleración / desaceleración

Con velocidades consecutivas válidas:

```text
VELOCITY_CHANGE_RAW =
V_next - V_previous
```

También:

```text
ABS_VELOCITY_CHANGE_RAW =
abs(V_next) - abs(V_previous)
```

Flags estructurales permitidos:

```text
misma dirección
y abs(V_next) > abs(V_previous)
→ ACCELERATION_RAW = TRUE

misma dirección
y abs(V_next) < abs(V_previous)
→ DECELERATION_RAW = TRUE

cambio de signo
→ REVERSAL_RAW = TRUE
```

No introducir:

```text
STRONG_ACCELERATION
SIGNIFICANT_DECELERATION
```

sin Minería.

La igualdad exacta debe permanecer como caso neutral.

---

# 41. Capa temporal SIDE

P4 debería conservar:

1. precios RAW por choice;
2. métricas semánticas SIDE estables derivadas con calculadores compatibles con P2.

Ejemplos RAW:

```text
PIN_HOME_1X2_FULL_TIME_ODDS_PRICE
PIN_DRAW_1X2_FULL_TIME_ODDS_PRICE
PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE

B365_HOME_1X2_FULL_TIME_ODDS_PRICE
B365_DRAW_1X2_FULL_TIME_ODDS_PRICE
B365_AWAY_1X2_FULL_TIME_ODDS_PRICE
```

cuando existan.

Ejemplos semánticos:

```text
PIN_1X2_FULL_TIME_SIDE_EDGE
B365_1X2_FULL_TIME_SIDE_EDGE
BOOK_1X2_FULL_TIME_REP_EDGE
```

solo cuando sus inputs sean válidos y comparables según P2.

Draw puede tener su propia trayectoria RAW aunque un edge SIDE específico use HOME/AWAY.

P4 DEBE mantener separados esos conceptos.

---

# 42. Asian Handicap y mercados SIDE con línea

Para AH separar siempre:

```text
LINE MOVEMENT
```

de:

```text
PRICE / EDGE MOVEMENT
```

La línea es una serie temporal propia.

## 42.0 Fuente físicamente implementable sin migración

El schema actual no persiste una serie de `main_line_at_capture` independiente. Por ello P4 v4 aplica una política conservadora:

```text
PRICE
→ serie independiente por choice_group/contrato
→ nunca cruza contratos

LINE en CHECKPOINT_VIEW
→ se infiere del choice_group únicamente cuando existe un contrato único
   para bookmaker/mercado/choice/checkpoint

más de un choice_group elegible en el mismo checkpoint
→ AMBIGUOUS_LINE_SELECTION
→ no se emite punto de línea para ese checkpoint
```

La inferencia de línea se limita a `CHECKPOINT_VIEW`; `ADAPTIVE_VIEW` no inventa una historia de línea que la persistencia no identifica inequívocamente. Una futura migración con `main_line_at_capture`, `observation_kind`, `ingested_at` y batch id permitiría enriquecer esta rama sin cambiar el DTO de series.

Ejemplos:

```text
PIN_AH_FULL_TIME_LINE
B365_AH_FULL_TIME_LINE
```

y cuando exista exchange:

```text
BF_AH_FULL_TIME_LINE
```

## 42.1 Cambio de contrato de línea

Decisión canónica:

```text
si cambia choice_group_key / line contract
→ cortar la trayectoria de PRICE/EDGE
```

La línea puede seguir comparándose temporalmente como serie de línea.

Pero los precios de contratos distintos NO se restan entre sí como un `PRICE_DELTA_RAW` simple.

Ejemplo:

```text
T120
Over 2.5 @ 1.90

T30
Over 3.0 @ 2.05
```

Resultado:

```text
LINE_MOVE_RAW = +0.5

PRICE_DELTA_RAW = NULL
PRICE_COMPARABLE = FALSE
PRICE_UNAVAILABLE_REASON = CONTRACT_CHANGED
```

Después comienza un nuevo price segment para la nueva línea/contrato.

La misma regla aplica a Asian Handicap.

## 42.2 Segmentación contractual

Una `TemporalSeries` de precio/edge debe mantener identidad contractual suficiente para que:

```text
same choice label
```

no implique automáticamente:

```text
same contract
```

`choice_group_key`/line contract forma parte de la comparabilidad.

P4 NO debe coser segmentos de precio entre líneas distintas.

# 43. Capa temporal O/U

Valores RAW:

```text
LINE
OVER_ODDS
UNDER_ODDS
```

por bookmaker/period.

Ejemplo:

```text
T120
Over 2.5 @ 1.95

T30
Over 3.0 @ 1.95
```

Esto no es:

```text
price drift = 0
```

como lectura completa del mercado.

El contrato cambió.

P4 DEBE persistir:

```text
LINE_START
LINE_END
LINE_MOVE_RAW
```

y aplicar la misma regla contractual definida para AH:

```text
line contract cambia
→ PRICE/EDGE segment termina
→ cross-contract PRICE_DELTA_RAW = NULL
→ reason = CONTRACT_CHANGED
→ inicia nuevo price segment
```

La trayectoria de la línea continúa independientemente.

Esto permite estudiar:

```text
movimiento de línea
```

sin fingir que dos precios pertenecientes a apuestas distintas son directamente comparables.

# 44. Contrato Betfair

Para Betfair:

```text
exchange_side ∈ {"back", "lay"}
```

`exchange_level` representa el nivel ordenado del exchange:

```text
0 = best available price/size level
```

El `OddsTrajectoryContext` entregado a los pilares ya debe contener el nivel seleccionado relevante, y normalmente:

```text
exchange_level = 0
```

P4 no es responsable de limpiar ladder ni elegir level.

---

# 45. `exchange_size`

`exchange_size` significa el **available size existente en ese momento para esa choice/side/level de Betfair**.

No es:

- total matched volume;
- total money traded;
- liquidez agregada de todas las choices;
- BACK + LAY combinado.

P4 DEBE conservarlo por identidad exacta:

```text
target_minute
choice
exchange_side
exchange_level
market contract
```

Ejemplo:

```text
BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE[T30]
```

es distinto de:

```text
BF_HOME_LAY_1X2_FULL_TIME_EXCHANGE_SIZE[T30]
```

y de:

```text
BF_AWAY_BACK_1X2_FULL_TIME_EXCHANGE_SIZE[T30]
```

---

# 46. Agregaciones prohibidas de `exchange_size`

P4 NO DEBE calcular automáticamente:

```text
BACK_SIZE + LAY_SIZE
```

como total.

P4 NO DEBE calcular automáticamente:

```text
HOME_SIZE + DRAW_SIZE + AWAY_SIZE
```

como supuesto volumen de mercado.

P4 NO DEBE etiquetar cambios de size como:

```text
MONEY_FLOW
SMART_MONEY
SHARP_MONEY
LIQUIDITY_PRESSURE
```

sin Minería.

---

# 47. Métricas temporales de `exchange_size`

Cuando existan dos puntos comparables:

```text
EXCHANGE_SIZE_DELTA_RAW =
size_end - size_start
```

y:

```text
EXCHANGE_SIZE_ABS_DELTA_RAW =
abs(EXCHANGE_SIZE_DELTA_RAW)
```

Métricas de velocidad sobre size podrán considerarse después si existe una necesidad de investigación concreta.

No agregarlas solo por simetría.

---

# 48. Estructura temporal BACK/LAY

P4 puede calcular por snapshot los mismos conceptos estructurales de P2 cuando sean aplicables:

```text
BACK_EDGE_t
LAY_EDGE_t
EXCHANGE_REP_EDGE_t
EXCHANGE_INTERNAL_GAP_t
BACK_LAY_RELATION_t
```

Ejemplo:

```text
BACK_EDGE_t =
side_edge(BACK_HOME_PRICE_t, BACK_AWAY_PRICE_t)

LAY_EDGE_t =
side_edge(LAY_HOME_PRICE_t, LAY_AWAY_PRICE_t)
```

Después:

```text
EXCHANGE_INTERNAL_GAP_t =
abs(BACK_EDGE_t - LAY_EDGE_t)
```

Y temporalmente:

```text
BACK_EDGE_DRIFT_RAW
LAY_EDGE_DRIFT_RAW
EXCHANGE_REP_EDGE_DRIFT_RAW
EXCHANGE_INTERNAL_GAP_DRIFT_RAW
```

P4 DEBE reutilizar la semántica P2.

---

# 49. Estructura temporal book vs exchange

Cuando las lecturas sean contractualmente comparables, P4 puede conservar su relación en cada target y a través del tiempo.

Ejemplos:

```text
BOOK_REP_EDGE_t
EXCHANGE_REP_EDGE_t

BOOK_EXCHANGE_GAP_t
BOOK_EXCHANGE_RELATION_t
```

Cambios temporales:

```text
BOOK_EXCHANGE_GAP_DELTA_RAW
BOOK_EXCHANGE_RELATION_STATE_CHANGE_RAW
```

P4 puede decir estructuralmente:

```text
books y exchange estaban alineados en T120
y opuestos en T5
```

si esos estados provienen de calculadores canónicos P2/P3.

Minería decide si esa transición es predictiva.

---

# 50. Contrato de missing data

Regla general:

```text
endpoint faltante
→ métrica dependiente = NULL
```

Nunca:

```text
NULL = 0
```

Nunca:

```text
T30 missing
→ copiar T120 en T30
```

Nunca:

```text
Pinnacle missing
→ usar bet365 como sustitución
```

Nunca:

```text
BACK missing
→ usar LAY
```

Nunca fusionar contratos independientes para aparentar completeness.

---

# 51. Trayectoria parcial

P4 DEBE soportar trayectorias válidas parciales.

Ejemplo:

```text
T360 = NULL
T120 = valid
T30  = valid
T5   = valid
```

Resultado esperado:

```text
EARLY_LEG = NULL
DEVELOPMENT_LEG = valid
LATE_LEG = valid
trajectory_partial = TRUE
```

La información disponible no debe descartarse por ausencia de un checkpoint temprano.

---

# 52. Disponibilidad del target operativo exacto Tn

El target `T{P4_TARGET_MINUTE}` es obligatorio para el profile operativo de esa ejecución.

Decisión canónica:

```text
Tn missing
→ P4_STATUS = INSUFFICIENT_DATA
→ P4_SIGNAL_PROFILE = NULL
```

P4 puede conservar:

```text
raw
history parcial
diagnostics
traceability
```

para auditoría/Miniería, pero no existe un profile operativo válido para System I.

No se permite sustituir Tn con:

```text
T1
T0
T-5
OPEN
otro bookmaker
último adaptive change
```

La última observación adaptativa anterior NO equivale automáticamente al endpoint operativo.

Tn debe existir como lectura operativa canónica. El requisito se satisface si al menos una serie elegible contiene ese endpoint; las series sin endpoint se diagnostican y el profile resultante es `PARTIAL`. Si ninguna serie elegible lo contiene, el profile completo es `NULL`.

# 53. Completeness

P4 NO DEBERÍA reducir completeness a un solo porcentaje genérico si distintas ramas requieren distintos datos.

Posibles diagnósticos por rama:

```text
Q_TEMPORAL_COMPLETE_BOOK_SIDE
Q_TEMPORAL_COMPLETE_EXCHANGE
Q_TEMPORAL_COMPLETE_TOTALS
Q_TEMPORAL_COMPLETE_AH
```

Completeness es disponibilidad/calidad de datos.

NO DEBE convertirse en:

```text
confidence
strength
predictive quality
```

sin Minería.

---

# 54. Distinguir missing de not configured

La implementación DEBERÍA distinguir conceptualmente:

```text
TARGET_NOT_CONFIGURED
TARGET_EXPECTED_BUT_MISSING
SOURCE_NOT_AVAILABLE
MARKET_NOT_AVAILABLE
AMBIGUOUS_INPUT
INVALID_INPUT
```

Esto es especialmente importante en rollout de T360 y en la coexistencia entre trayectoria adaptativa y fallback.

Eventos históricos capturados antes de que T360 existiera no deben parecer fallas de adquisición de T360.

Del mismo modo:

```text
sin checkpoint T120 explícito
```

no equivale necesariamente a:

```text
sin observaciones en esa región temporal
```

si la serie proviene de snapshots persistidos con resolución adaptativa.

Si el contexto actual no permite distinguir todas estas causas, el Dev debe señalar qué metadata adicional se requeriría.

P4 no debe inventarla.

---

# 55. `initial_odds` / OPEN

El contexto actual expone:

```text
ChoiceOddsTrajectory.initial_odds
```

P4 NO DEBE tratarlo como target temporal automático.

En especial:

```text
OPEN != T360
```

La apertura puede variar por fuente y existir mucho antes.

Ejemplo del core temporal cuando `P4_TARGET_MINUTE = 5`:

```text
T360 → T120 → T30 → T5
```

`initial_odds` puede conservarse como:

```text
OPENING_REFERENCE
```

para investigación o contexto suplementario.

NO DEBE rellenar T360 cuando falte.

---

# 56. Target minute vs captura real

Conservar por punto:

```text
target_minute
minutes_before_start
distance_from_target
collected_at
```

Ejemplo:

dos observaciones etiquetadas T30 pueden haberse capturado a:

```text
31 minutos antes
28 minutos antes
```

La etiqueta sigue siendo T30.

El metadata conserva la posición real.

Importa para:

- velocity;
- reproducibilidad;
- auditoría;
- análisis de calidad.

---

# 57. Invariante de reproducibilidad y `as-of`

Pregunta central:

> **WHAT DID SYSTEM I KNOW AT Tn?**

Toda métrica operativa P4 debe usar exclusivamente información disponible hasta el instante nominal del target operativo:

```python
P4_OPERATIVE_AS_OF = EventContext.starts_at - timedelta(minutes=P4_TARGET_MINUTE)
```

Decisión canónica:

```text
point.availability_at > P4_OPERATIVE_AS_OF
→ punto prohibido en P4_SIGNAL_PROFILE
```

Ninguna observación posterior al as-of Tn puede entrar al profile operativo, incluso si por proximidad pudiera parecer un buen candidato para un checkpoint histórico. La implementación usa `collected_at` como disponibilidad autoritativa y solo recurre a `source_collected_at` si el primero falta.

Por tanto:

```text
T1
T0
T-5
post-signal adaptive changes
```

quedan fuera de la vista operativa.

El corte se aplica dentro de `extract_p4_trajectory_inputs()` antes de construir tanto la vista adaptativa como la proyección de checkpoints. Esto protege ejecuciones normales y simulaciones/reprocesamientos posteriores que carguen snapshots más recientes que Tn.

Limitación conocida sin migración: `market_choice_snapshots.collected_at` mezcla en datos legacy el momento de ingesta de snapshots corrientes con timestamps históricos nominales de `momentQuotes`. No existen todavía `ingested_at`, `observation_kind`, batch id ni `main_line_at_capture`. P4 conserva esta procedencia como `LEGACY_MIXED_COLLECTED_AT`; el corte evita leakage observable con el contrato disponible, pero no promete reconstruir una distinción de procedencia que la base no persistió.

La infraestructura involucrada es:

- target selection;
- query/persistence cutoff;
- pipeline execution timestamp;
- `available_through_utc`;
- o mecanismo equivalente.

La semántica no queda abierta al Dev: la regla P4 ya está definida.

La limitación de procedencia anterior no cambia la frontera causal ni autoriza incluir un punto posterior.

# 58. Contrato de output — envelope exacto P2/P3-compatible

P4 DEBE usar el mismo patrón arquitectónico de respuesta que P2/P3:

```text
extractor/policy
        ↓
inputs normalizados + diagnostics
        ↓
trajectory/signal engine
        ↓
P4SignalProfile DTO
        ↓
profile.to_dict()
        ↓
P4_SIGNAL_PROFILE
        ↓
P4MiningAdapter
```

La uniformidad aplica al envelope, separación de responsabilidades y persistencia. No obliga a copiar la forma interna de P2/P3: P4 conserva dos vistas temporales y múltiples series.

Schema top-level canónico:

```json
{
  "pillar_id": "pillar_4_temporal_market_drift",
  "pillar_name": "Temporal Market Drift Signal Profile",
  "engine_version": "p4-signal-profile-v1",
  "event_id": 123,
  "participants": "Home vs Away",
  "P4_TARGET_MINUTE": 5,
  "PERIODS": {},
  "MISSING_INPUTS": [],
  "INVALID_INPUTS": [],
  "AMBIGUOUS_INPUTS": [],
  "P4_STATUS": "PARTIAL",
  "status": "PARTIAL",
  "P4_SIGNAL_PROFILE": {},
  "modules": [],
  "raw": {}
}
```

`P4_TARGET_MINUTE` es la única clave serializada para el endpoint operativo. Su valor es el entero recibido desde el pipeline (`5` en el ejemplo). El término conceptual `P4_OPERATIVE_ENDPOINT` puede usarse en explicación, pero NO debe serializarse como un segundo campo con el mismo significado.

`PERIODS` se reserva exclusivamente para cobertura y diagnósticos de extracción por dominio/período. Las series y métricas finales viven dentro de `P4_SIGNAL_PROFILE`; NO se duplican en `PERIODS`.

`P4_SIGNAL_PROFILE` es siempre Tn-safe. Los datos posteriores a su target no viven dentro de él.

## 58.1 Dos vistas temporales canónicas y nulabilidad

Las vistas tienen cardinalidad exacta `0..1` dentro del profile y no son módulos:

```text
ADAPTIVE_VIEW disponible   → objeto con SERIES[]
ADAPTIVE_VIEW no disponible → null

CHECKPOINT_VIEW disponible   → objeto con SERIES[]
CHECKPOINT_VIEW no disponible → null
```

No se usa un bloque alternativo `{"available": false}`. La convención canónica de ausencia es `null`.

`ADAPTIVE_VIEW` responde qué hizo la trayectoria con la máxima resolución persistida disponible. `CHECKPOINT_VIEW` responde cómo se ve bajo una rejilla común y reproducible. Ninguna sustituye, reduce ni rellena silenciosamente a la otra.

Cada vista disponible usa esta forma:

```json
{
  "SOURCE_MODE": "PERSISTED_SNAPSHOTS",
  "SERIES": []
}
```

o:

```json
{
  "SOURCE_MODE": "FIXED_CHECKPOINTS",
  "SERIES": []
}
```

Las métricas de trayectoria, ventanas, timing, momentum y corrección pertenecen a una serie concreta. No existen como bloques globales paralelos a `SERIES`.

# 59. `P4_SIGNAL_PROFILE` canónico

`P4SignalProfile.to_dict()` DEBE serializar exactamente la estructura estable siguiente:

```json
{
  "META": {
    "TARGET_MINUTE": 5,
    "OPERATIVE_AS_OF": "...",
    "SOURCE_SERIES_SEEN": 4,
    "ENDPOINT_SERIES_PRESENT": 3,
    "TRAJECTORY_PARTIAL": true
  },
  "ADAPTIVE_VIEW": null,
  "CHECKPOINT_VIEW": {
    "SOURCE_MODE": "FIXED_CHECKPOINTS",
    "SERIES": []
  },
  "STRUCTURAL_DOMAIN_SUMMARY": {
    "SIDE": {
      "SERIES_IDS": [],
      "VIEWS": {"ADAPTIVE_VIEW": [], "CHECKPOINT_VIEW": []},
      "STRUCTURAL_SIGNALS": {}
    },
    "TOTALS": {
      "SERIES_IDS": [],
      "VIEWS": {"ADAPTIVE_VIEW": [], "CHECKPOINT_VIEW": []},
      "STRUCTURAL_SIGNALS": {}
    }
  },
  "SUMMARY": {},
  "TRACEABILITY": {}
}
```

Reglas de autoridad:

- `event_id` vive canónicamente en el envelope, no se repite en `META`;
- `META` contiene solo metadata temporal/operativa del profile;
- `ADAPTIVE_VIEW.SERIES[]` y `CHECKPOINT_VIEW.SERIES[]` son las únicas fuentes canónicas de puntos, legs, métricas y señales por serie;
- `STRUCTURAL_DOMAIN_SUMMARY` referencia `SERIES_IDS` y sintetiza hechos del dominio, pero no copia series completas;
- `SIDE` y `TOTALS` no se combinan en una señal, score o dirección común;
- `SUMMARY` solo puede contener conteos, disponibilidad y referencias no analíticas; no puede duplicar métricas por serie;
- `TRACEABILITY` global documenta versión/procedencia compartida y referencias; la traza de un punto concreto permanece en su serie.

El profile contiene dos capas complementarias:

```text
RAW_TEMPORAL_FEATURES
    series, points, legs, deltas, path, timing, windows

STRUCTURAL_SIGNALS
    lectura determinista del recorrido observado hasta Tn
```

Ambas se derivan de los mismos puntos serializados y respetan la vista que las produjo.

# 60. Output canónico a nivel de serie

Cada elemento de `SERIES[]` usa nombres serializados upper-case, aunque los DTOs Python puedan usar atributos lower-case internamente:

```json
{
  "SERIES_ID": "PIN_HOME_1X2_FULL_TIME_ODDS_PRICE",
  "MARKET": {
    "MARKET_ID": 10,
    "MARKET_GROUP": "1X2",
    "MARKET_PERIOD": "Full Time",
    "MARKET_NAME": "1X2 Full Time",
    "CHOICE_GROUP_KEY": "__default__",
    "BOOKIE_ID": 302,
    "BOOKIE_NAME": "Pinnacle",
    "SOURCE": "oddspapi",
    "CHOICE_ID": 1,
    "CHOICE_NAME": "home",
    "MAIN_LINE": true,
    "EXCHANGE_SIDE": null,
    "EXCHANGE_LEVEL": null
  },
  "POINTS": [
    {
      "OBSERVATION_KIND": "ADAPTIVE_CHANGE",
      "TARGET_MINUTE": null,
      "VALUE": "2.10",
      "SNAPSHOT_ID": 123,
      "QUOTE_ID": 456,
      "COLLECTED_AT": "...",
      "SOURCE_COLLECTED_AT": "...",
      "MINUTES_BEFORE_START": "173.250000",
      "DISTANCE_FROM_TARGET": null,
      "SOURCE_LIMIT": null,
      "EXCHANGE_SIZE": null
    },
    {
      "OBSERVATION_KIND": "OPERATIVE_ENDPOINT",
      "TARGET_MINUTE": 5,
      "VALUE": "2.04",
      "COLLECTED_AT": "..."
    }
  ],
  "LEGS": [],
  "RAW_TEMPORAL_FEATURES": {
    "OBSERVATION_COUNT": 8,
    "LEG_COUNT": 7,
    "NET_MOVE_RAW": null,
    "PATH_LENGTH_RAW": null,
    "PATH_EFFICIENCY_RAW": null,
    "DIRECTIONAL_RUNS": [],
    "TURNING_POINTS_RAW": [],
    "WINDOWS": {},
    "TIMING": {}
  },
  "STRUCTURAL_SIGNALS": {
    "PATH_PATTERN_RAW": null,
    "NET_DIRECTION_RAW": null,
    "FINAL_RUN_DIRECTION_RAW": null,
    "TURNING_STRUCTURE_RAW": null,
    "CORRECTION_STATE_RAW": null,
    "OVERSHOOT_RAW": null,
    "ACCELERATION_RAW": null,
    "DECELERATION_RAW": null,
    "BOOK_EXCHANGE_RELATION_CHANGE_RAW": null
  },
  "TRACEABILITY": {}
}
```

`MARKET` debe conservar suficiente identidad para distinguir dominio, período, contrato/line, bookmaker, choice y exchange side/level. Si una serie describe línea, price, implied probability, edge o size, su `SERIES_ID` y metadata deben declarar esa variable sin ambigüedad.

En `ADAPTIVE_VIEW`, `POINTS` procede de `ChoiceOddsTrajectory.snapshots` filtrados causalmente y termina con el endpoint operativo proyectado. En `CHECKPOINT_VIEW`, los mismos snapshots causalmente seguros se reproyectan a los targets esperados expuestos por el contexto; T360 se soporta si está presente sin modificar por ello la configuración de adquisición.

No introducir `PERSISTENCE_RAW`: la persistencia ya se representa con `DIRECTIONAL_RUNS`, `FINAL_RUN_DIRECTION_RAW` y el path canónico. `LATE_REINFORCEMENT_RAW` permanece fuera del contrato mientras `PD-014` no defina la agregación de ventanas adaptativas que le daría semántica única.

`STRUCTURAL_SIGNALS` nunca contiene `score`, `weight`, `strength`, `confidence`, `predictive_value` ni `bet/no-bet`.

Usar las convenciones compartidas del proyecto para serializar `Decimal`, `datetime` y DTOs.

# 61. Módulo, raw audit, trazabilidad y Minería

## 61.1 Un único módulo de engine

Cuando existe profile, `modules` contiene exactamente un módulo de engine, no uno por vista ni por serie:

```json
{
  "pillar_id": "pillar_4_temporal_market_drift",
  "module_id": "p4_signal_engine",
  "module_name": "Temporal Market Drift Engine",
  "engine_version": "p4-signal-profile-v1",
  "P4_STATUS": "PARTIAL",
  "status": "PARTIAL",
  "P4_TARGET_MINUTE": 5,
  "PERIODS": {},
  "P4_SIGNAL_PROFILE": {},
  "raw": {}
}
```

El `P4_SIGNAL_PROFILE` top-level y el del módulo representan el mismo payload canónico serializado. El módulo no recalcula ni mantiene una copia divergente.

Si `P4_SIGNAL_PROFILE = null`, `modules = []`, siguiendo el patrón de P2/P3 para ausencia de resultado del engine.

## 61.2 Raw audit y diagnósticos

El envelope conserva:

```text
PERIODS
MISSING_INPUTS
INVALID_INPUTS
AMBIGUOUS_INPUTS
raw
```

`raw` conserva inputs, metadata de origen y trazabilidad de extracción. No es un segundo payload analítico y no puede contradecir el profile.

Por cada serie/punto utilizado debe poder reconstruirse, cuando esté disponible:

```text
TRAJECTORY_SOURCE_MODE
change_detector_version/policy metadata
event_id (como referencia al canónico top-level)
market_group / market_period / market_name / choice_group_key
bookie_id / bookie_name / source
exchange_side / exchange_level
choice_name / choice_id / main_line
target_minute / value / price / line / size
snapshot_id / quote_id
collected_at / source_collected_at / changed_at
minutes_before_start / distance_from_target
source_limit / exchange_size
```

## 61.3 `P4MiningAdapter`

P4 DEBE añadir un adapter paralelo a `P2MiningAdapter` y `P3MiningAdapter`:

```text
pillar_id    = pillar_4_temporal_market_drift
result_scope = temporal_market_drift
payload      = {"P4_SIGNAL_PROFILE": ...}
```

El `PillarMiningRun` conserva como mínimo:

```text
event_id
competition/context
evaluation_minute
P4_TARGET_MINUTE
producer_status + canonical_status
inputs + diagnostics
engine_version
output_payload
P4_SIGNAL_PROFILE
```

El adapter crea una unidad `summary` y, cuando existe el módulo, una unidad `module`. No inventa score escalar, direction predictiva ni una unidad por serie.

Las dimensiones/payload deben permitir distinguir al menos:

```text
market_group: SIDE / TOTALS
view: ADAPTIVE / CHECKPOINT
market_period
bookie/source
exchange_side
SERIES_ID
```

Las series y métricas se persisten dentro del profile o en unidades inequívocamente relacionadas con su `SERIES_ID`; no como columnas sueltas sin identidad.

---

# 62. Qué puede interpretar estructuralmente P4

P4 DEBE producir una lectura estructural determinista, no limitarse a entregar features para interpretación posterior. Cada lectura vive en `SERIES[].STRUCTURAL_SIGNALS` y se deriva exclusivamente de `SERIES[].POINTS` y `RAW_TEMPORAL_FEATURES` de la misma vista.

El contrato contempla como mínimo:

```text
PATH_PATTERN_RAW
NET_DIRECTION_RAW
FINAL_RUN_DIRECTION_RAW
TURNING_STRUCTURE_RAW
CORRECTION_STATE_RAW
OVERSHOOT_RAW
ACCELERATION_RAW
DECELERATION_RAW
BOOK_EXCHANGE_RELATION_CHANGE_RAW
```

Estos campos pueden materializar labels deterministas derivados de matemáticas observables:

```text
NO_MOVEMENT
UNIDIRECTIONAL
REVERSAL
MULTI_REVERSAL

PEAK
TROUGH

ACCELERATION_RAW
DECELERATION_RAW

DOMINANT_SEGMENT_RAW
```

Cuando se basen en un eje semántico estable, también:

```text
HOME_TO_AWAY_REVERSAL
AWAY_TO_HOME_REVERSAL

OVER_TO_UNDER_REVERSAL
UNDER_TO_OVER_REVERSAL
```

Describen el path.

No predicen el resultado deportivo.

`STRUCTURAL_DOMAIN_SUMMARY.SIDE` y `.TOTALS` sintetizan por separado estas lecturas y enumeran los `SERIES_IDS` que las sustentan. No fusionan ambos dominios ni crean una métrica común.

---

# 63. Qué NO puede concluir P4

P4 NO DEBE emitir ni implicar:

```text
HOME likely to win
AWAY likely to win
OVER expected
UNDER expected

BET
NO BET

VALUE
TRAP

SMART MONEY
SHARP MONEY
PROFESSIONAL MONEY

CONFIRMED REVERSAL
FALSE REVERSAL

STRONG
WEAK
HIGH CONFIDENCE
LOW CONFIDENCE
```

salvo una futura calibración explícita de Minería.

---

# 64. P4 vs Minería y comparabilidad entre vistas

La frontera sigue siendo:

```text
P4:
"esto ocurrió"

Minería:
"esto históricamente importó"
```

## 64.1 Adaptive y checkpoint responden preguntas distintas

Adaptive View:

> ¿Qué hizo la trayectoria con la máxima resolución persistida disponible?

Checkpoint View:

> ¿Cómo se ve bajo una rejilla temporal común y reproducible?

P4 debe conservar ambas cuando existan.

## 64.2 Métricas sensibles a resolución

Las siguientes métricas dependen fuertemente de cuántos puntos observamos:

```text
SIGN_CHANGE_COUNT
PATH_LENGTH
POINT_COUNT
LEG_COUNT
TURNING_POINT_COUNT
CORRECTION_COMPLEXITY
PATTERN COMPLEXITY
VELOCITY_SEQUENCE
```

Por tanto deben llevar explícitamente la vista/source mode:

```text
PATH_LENGTH_ADAPTIVE
PATH_LENGTH_CHECKPOINT

SIGN_CHANGE_COUNT_ADAPTIVE
SIGN_CHANGE_COUNT_CHECKPOINT
```

y Minería debe conocer al menos:

```text
TRAJECTORY_SOURCE_MODE
OBSERVATION_COUNT
LEG_COUNT
```

No comparar ciegamente:

```text
adaptive con 15 puntos
```

contra:

```text
fallback con 3 puntos
```

como si tuvieran la misma capacidad de observar zigzags.

## 64.3 Métricas endpoint-based

Métricas como:

```text
NET_MOVE
```

son comparables en mayor grado cuando utilizan los mismos endpoints contractuales.

Aun así debe conservarse la vista/origen para auditoría.

## 64.4 Ejemplos de frontera P4/Mining

```text
P4:
SIGN_CHANGE_COUNT_ADAPTIVE = 4

Minería:
determina si 4 cambios observados bajo esa resolución tienen valor
```

```text
P4:
MOVE_SHARE_LATE_WINDOW = 0.72

Minería:
determina si esa concentración temporal es predictiva
```

```text
P4:
abs(V_next) > abs(V_previous)

Minería:
determina si la diferencia es suficientemente relevante
```

P4 debe preservar suficiente RAW para que Minería pueda segmentar por:

- source mode;
- resolution;
- observation count;
- liga;
- deporte;
- bookmaker;
- mercado;
- rango de odds;
- rango de línea;
- timing;
- exchange behavior.

# 65. `POST_SIGNAL_MARKET_AUDIT`

Un componente separado puede analizar:

```text
T5 → T1
T1 → T0
T0 → T-5
```

Nombre conceptual sugerido:

```text
POST_SIGNAL_MARKET_AUDIT
```

No forma parte de:

```text
P4_SIGNAL_PROFILE
```

Puede estudiar:

```text
POST_SIGNAL_DELTA
POST_SIGNAL_CONTINUATION
POST_SIGNAL_REVERSAL
POST_SIGNAL_CORRECTION
POST_KICKOFF_CHANGED
```

siempre que queden claramente marcados como audit/research y nunca retroalimenten el profile T5.

Los mismos calculadores temporales genéricos pueden reutilizarse, pero la capa y serializer deben mantener la etiqueta post-signal.

---

# 66. P4 no debe hardcodear tres legs

Aunque el window inicial sea:

```text
T360
T120
T30
T5
```

evitar arquitectura fundamental basada en:

```python
_SEGMENTS = ("D1", "D2", "D3")
```

Usar cálculo genérico entre targets adyacentes.

Esto permite que Minería investigue en el futuro:

```text
T720
T240
T60
T15
```

sin rediseñar toda la matemática temporal.

Cualquier nuevo checkpoint operativo seguirá requiriendo decisión explícita de arquitectura/configuración.

---

# 67. No agregar T60 todavía

Scope actual:

```text
NO agregar T60
```

Motivo:

```text
T360 agrega una dimensión temporal genuinamente nueva.
```

T60 principalmente subdividiría:

```text
T120 → T30
```

Minería debe demostrar primero si esa resolución extra aporta valor.

---

# 68. Política de integración T360

T360 debe evaluarse principalmente como ampliación del mecanismo de fallback/checkpoints.

Si adquisición agrega `360`, DEBE hacerlo mediante el mismo mecanismo general usado por:

```text
PRE_START_ODDS_MOMENTS
OddsTrajectoryContext.target_minutes_expected
ChoiceOddsTrajectory.odds_values[target_minute]
ChoiceOddsTrajectory.meta_by_minute[target_minute]
```

No crear un pipeline de datos T360 exclusivo de P4 si la arquitectura compartida puede soportarlo.

Importante:

la nueva trayectoria adaptativa histórica puede contener observaciones mucho anteriores a T360 sin necesidad de crear un checkpoint para cada una.

Por tanto:

```text
T360
```

no sustituye ni limita la historia adaptativa.

Su papel es:

```text
fallback temprano reproducible
+
boundary/research checkpoint
+
resiliencia cuando no exista trayectoria adaptativa
```

La decisión v4 es no modificar scheduler/adquisición. Antes de habilitar T360 en producción deberá validarse fuera de P4:

```text
scheduler behavior
provider eligibility
API cost
provider request windows
competition availability
historical availability
persistence behavior
context reconstruction
interaction with adaptive-history ingestion
```

# 69. Cobertura T360 de rollout

La cobertura real de T360 debe medirse antes de habilitar su captura para:

```text
Pinnacle
bet365
Betfair
```

y por mercado:

```text
FT 1X2
FT AH
1H 1X2
1H AH

FT O/U
1H O/U

Betfair BACK
Betfair LAY
Betfair exchange_size
```

No asumir cobertura.

---

# 70. Cobertura histórica Betfair

La disponibilidad histórica debe medirse por checkpoint para:

```text
BACK price
LAY price
exchange_size
```

en:

```text
T120
T30
T5
```

y potencialmente:

```text
T360
```

Debe verificarse por familia de mercado.

Este blueprint define cómo usar esos campos si existen.

No afirma que todos estén poblados históricamente.

---

# 71. Retiro del P4 legacy

La implementación legacy fue retirada completamente, junto con sus tests específicos. No constituye una dependencia ni una fuente semántica de v4.

El rediseño DEBE retirar o aislar comportamiento legacy equivalente a:

```text
MIN_MOVE_THRESHOLD hardcodeado

DRIFT_CONFIDENCE arbitraria

timing thresholds arbitrarios

RAW_MARKET_TRANSFER_PP compuesto

post-signal/post-kickoff sustituyendo valores operativos

OPEN como checkpoint temporal obligatorio

T1 como endpoint operativo
```

La corrección semántica tiene prioridad sobre compatibilidad interna con el P4 viejo.

---

# 72. Modelo de status

P4 debe preservar el contrato top-level común de P2/P3. Los únicos status serializados en `P4_STATUS` y `status` son:

```text
ACTIVE
PARTIAL
INSUFFICIENT_DATA
ERROR
```

`AMBIGUOUS` e `INVALID` son estados de extracción/diagnóstico dentro de `PERIODS`, `AMBIGUOUS_INPUTS` e `INVALID_INPUTS`; no son status top-level.

## 72.1 Precedencia canónica

Para el profile operativo:

```text
1. ERROR
2. INSUFFICIENT_DATA
3. ACTIVE / PARTIAL
```

Interpretación:

### `ERROR`

Excepción real o fallo inesperado del engine.

No usar para missing data normal.

### Diagnósticos `AMBIGUOUS` / `INVALID`

Una ambigüedad significa que existe más de un candidato válido para un contrato requerido y no puede seleccionarse uno determinísticamente. Un input inválido está presente, pero viola el contrato: scalar/odds/identidad inválida u otra condición estructural explícita.

No fusionar, elegir arbitrariamente ni degradar esos diagnósticos. Si afectan el core requerido y dejan el profile sin base operativa, el status top-level es `INSUFFICIENT_DATA`; si solo afectan una rama opcional y queda core utilizable, es `PARTIAL`.

### `INSUFFICIENT_DATA`

No existe un profile operativo utilizable por falta de información requerida.

Regla explícita:

```text
target Tn exacto ausente en todas las series elegibles
→ INSUFFICIENT_DATA
→ P4_SIGNAL_PROFILE = NULL
```

### `ACTIVE`

Tn válido y el core observado está completo.

### `PARTIAL`

Tn válido y existe core operativo utilizable, pero una o más ramas observadas/temporales están incompletas.

Ejemplos que por sí solos pueden producir `PARTIAL` y no fallo total:

```text
T360 ausente
1H ausente
Betfair branch opcional ausente
Adaptive View no disponible pero Checkpoint View válida
```

La implementación puede reutilizar `market_coverage` para diagnósticos de período/rama, pero P4 debe mapear esos diagnósticos al status top-level anterior.

No usar completeness como confidence predictiva.

# 73. Manejo de errores

El pipeline actual ya tiene fallback de error P4.

El rediseño debe distinguir:

```text
cálculo no disponible porque faltan inputs
```

de:

```text
excepción inesperada de implementación
```

Missing data normal no debe convertirse en excepción.

Una excepción representa fallo real del engine.

Los faltantes esperables deben devolver status estructurado con diagnósticos.

---

# 74. Debug logging

P2/P3 ya registran:

- inputs extraídos;
- nombres de fórmulas;
- sustituciones;
- resultados;
- secciones del profile.

P4 implementa transparencia equivalente cuando `debug_mode=True`, organizada
en tres familias estables de mensajes:

```text
P4 DEBUG
P4 FORMULA
P4 SIGNAL
```

`P4 DEBUG` registra el corte causal, conteos y diagnósticos de extracción, cada
serie normalizada, asignaciones por punto, procedencia, timestamps y
trazabilidad. `P4 FORMULA` registra fórmula, sustitución y resultado para legs,
métricas del recorrido y series semánticas derivadas. `P4 SIGNAL` registra las
dos vistas, las señales estructurales por serie, la síntesis por dominio y el
summary final.

Para un leg:

```text
series
start target
end target
start value
end value
delta
actual timestamps
elapsed time
velocity
direction
```

Para path:

```text
leg sequence
net move
path length
path efficiency
sign changes
pattern
```

El debug logging NO DEBE cambiar comportamiento de cálculo. Cuando
`debug_mode=False`, no se emite ningún mensaje `P4 DEBUG`, `P4 FORMULA` ni
`P4 SIGNAL`; permanece únicamente el resumen operativo habitual del pilar.

---

# 75. Precisión

Valores de mercado y métricas DEBERÍAN conservar las convenciones basadas en `Decimal` existentes.

P4 NO DEBERÍA introducir `float` en cálculos core de odds por comodidad.

La serialización puede convertir conforme a las convenciones existentes del proyecto.

El rounding de display debe mantenerse separado del valor usado para calcular.

---

# 76. Fases recomendadas de implementación

## Fase A — arquitectura/extracción y adapter temporal

Implementar/refactorizar:

```text
P4 scopes/input vocabulary
temporal point models
variable-length TemporalSeries
series identity
trajectory_source_mode
P4 trajectory input adapter
context extraction
missing/invalid/ambiguous diagnostics
T5-safe temporal filtering
traceability
```

La capa adapter debe leer ambos canales actuales de `OddsTrajectoryContext`: `snapshots` para la vista adaptativa y `odds_values/meta_by_minute` para la vista checkpoint. Debe mantenerlos separados desde la extracción.

No es necesario reabrir decisiones matemáticas en esta fase; deben implementarse únicamente las reglas ya definidas por este blueprint.

---

## Fase B — matemática RAW ya definida

Implementar solo cálculos suficientemente cerrados:

```text
adjacent deltas
absolute deltas
direction by sign
elapsed actual time
velocity
exchange-size deltas
line deltas
```

Path/net/pattern solo hasta donde lo permitan las reglas de gaps ya selladas.

---

## Fase C — métricas temporales semánticas

Reutilizar/extraer pure calculators P2/P3 para:

```text
SIDE_EDGE
OU_EDGE
BACK/LAY edge
book/exchange comparable structure
```

No crear fórmulas alternativas.

---

## Fase D — implementación de semántica de trayectoria ya cerrada

Implementar conforme a las decisiones canónicas de la sección 78:

```text
zero-aware sign change
turning plateaus
directional correction runs
overshoot
non-contiguous path policy
dominant ties
cross-line price segmentation
adaptive/checkpoint dual view
window summaries
```

No introducir variantes alternativas.

---

## Fase E — consumir historia actual + ampliar T360 fallback

Sobre el contrato upstream ya disponible:

1. consumir `ChoiceOddsTrajectory.snapshots` como trayectorias únicas por serie;
2. mantener la lectura canónica T5 desde la proyección configurada;
3. conservar `odds_values/meta_by_minute` como vista/fallback de checkpoints;
4. agregar T360 al mecanismo compartido de targets si es viable.

Para T360 confirmar:

```text
provider feasibility
scheduler behavior
API cost
historical coverage
```

P4 debe seguir funcionando con:

```text
historia adaptativa disponible
```

y con:

```text
solo fallback T120/T30/T5
```

y posteriormente:

```text
fallback T360/T120/T30/T5
```

---

## Fase F — post-signal audit

Implementar solo como componente claramente separado.

Nunca reutilizarlo como input operativo T5.

---

# 77. Requisitos mínimos de pruebas

La reimplementación P4 no está completa sin tests.

## Tests de temporal leakage

Cambios en:

```text
T1
T0
T-5
```

NO deben modificar:

```text
P4_SIGNAL_PROFILE @ T5
```

si T360/T120/T30/T5 son idénticos.

---

## Tests de transición T360

```text
T360 missing
T120/T30/T5 valid
```

debe conservar los legs posteriores válidos.

---

## Tests de candidate ambiguity

Más de un candidato válido para el mismo contrato:

```text
→ ambiguous
→ no merge
→ no ACTIVE exitoso para esa rama
```

---

## Tests de main line

P4 no implementa un segundo selector independiente de main line.

Main lines ambiguas no se promedian ni se eligen arbitrariamente.

---

## Tests de null propagation

```text
endpoint faltante
→ dependent metric NULL
```

nunca cero/sustitución.

---

## Tests de eliminación de threshold

```text
delta = 0.019
```

debe permanecer no cero y conservar su signo matemático.

---

## Tests de no movement

Todos los legs válidos iguales a cero:

```text
PATH_LENGTH_RAW = 0
PATH_EFFICIENCY_RAW = NULL
NO_MOVEMENT_RAW = TRUE
```

---

## Tests de timestamp/velocity

`collected_at` real debe gobernar velocity cuando exista.

La distancia nominal del target no debe reemplazar el tiempo real.

---

## Tests de exchange side

Verificar:

- BACK y LAY separados;
- `exchange_size` por choice/side;
- ninguna agregación automática de size.

---

## Tests de identidad contractual

Distintos `choice_group_key` no se fusionan.

---

## Tests línea vs precio

El line movement permanece observable de manera independiente.

Cuando cambia el contrato de línea:

```text
PRICE_DELTA_RAW = NULL
PRICE_COMPARABLE = FALSE
reason = CONTRACT_CHANGED
```

y debe comenzar un nuevo price segment.

---

## Tests OPEN/T360

`initial_odds` no rellena T360.

---

## Tests de separación post-signal

`POST_SIGNAL_MARKET_AUDIT` permanece separado de `P4_SIGNAL_PROFILE`.

---


## Tests de longitud variable por choice

Verificar que distintas series puedan contener diferente número de puntos:

```text
HOME = 8 points
DRAW = 3 points
AWAY = 6 points
```

sin que eso sea considerado automáticamente ambiguous/invalid.

---

## Tests de timestamps no sincronizados

Verificar que HOME/DRAW/AWAY o BACK/LAY no requieran timestamps idénticos para conservar sus trayectorias RAW individuales.

Las comparaciones cross-series que sí requieran sincronización/comparabilidad deben aplicar su contrato específico y devolver `NULL` cuando no se cumpla.

---

## Tests de source mode

Verificar que el mismo engine pueda consumir:

```text
ADAPTIVE_CHANGES
FIXED_CHECKPOINT_FALLBACK
```

sin cambiar fórmulas RAW por el simple hecho de cambiar la procedencia.

---

## Tests de no doble threshold

Un delta entre dos puntos ya entregados por el contexto debe conservarse por su signo/magnitud exacta.

P4 no debe aplicar nuevamente la magnitud mínima utilizada por el detector histórico upstream.

---

## Tests de T5 con trayectoria adaptativa

Verificar que una serie adaptativa pueda contener cero o más cambios anteriores y aun así identificar de manera separada la observación operativa T5.

No se debe asumir que el último significant-change persistido equivale automáticamente a T5.

---

# 78. P4 — Decisiones canónicas de diseño PD-001 a PD-016

Las decisiones que anteriormente aparecían como pendientes quedan cerradas por este blueprint.

El Dev NO debe rediseñarlas por conveniencia de implementación.

Su responsabilidad es auditar compatibilidad técnica y reportar incompatibilidades reales.

| PD | Decisión canónica |
|---|---|
| **P4-PD-001 — Zero bridging** | `+ 0 -` sí cuenta como `REVERSAL`. El cero permanece explícitamente como plateau. La secuencia RAW se conserva y `ZERO_BRIDGED_SIGN_CHANGE = TRUE`. |
| **P4-PD-002 — Turning point con plateau** | Un cambio atravesando uno o más legs cero se representa como `TURNING_ZONE` con `structure = PLATEAU`, no como un punto falso. |
| **P4-PD-003 — Gaps no contiguos** | `NET_MOVE` puede calcularse entre endpoints válidos aunque exista gap. `PATH_LENGTH`, sign changes, pattern, turning, correction y métricas path-dependent solo utilizan tramos observados/continuos. `GAP_PRESENT_RAW = TRUE`. |
| **P4-PD-004 — Falta Tn** | Si ninguna serie elegible contiene el target exacto solicitado: `P4_STATUS = INSUFFICIENT_DATA`, `P4_SIGNAL_PROFILE = NULL`. Raw/history parcial puede conservarse. No existe fallback a otro target. |
| **P4-PD-005 — Correction run** | Se usan `DIRECTIONAL_RUNS`. El primer run no cero es `INITIAL_RUN`; el primer run posterior con signo contrario es `CORRECTION_RUN`; fases posteriores se conservan. |
| **P4-PD-006 — Overshoot** | `CORRECTION_RATIO_RAW` no se capea. Si `>1`, `OVERSHOOT_RAW = TRUE` y `OVERSHOOT_MAGNITUDE_RAW = correction_abs - initial_abs`. |
| **P4-PD-007 — Empate dominante** | Persistir `DOMINANT_SEGMENTS_RAW = [...]` o `DOMINANT_WINDOWS_RAW = [...]`. Nunca elegir arbitrariamente uno. |
| **P4-PD-008 — Cambio de línea** | La línea continúa como serie; la trajectory de price/edge se corta por contrato. Cambio de `choice_group_key`/line contract → `PRICE_DELTA_RAW = NULL`, `reason = CONTRACT_CHANGED`, y comienza nuevo price segment. |
| **P4-PD-009 — Implied probability** | P4 reutiliza exactamente la semántica P2/P3. El componente RAW es `1 / decimal_odds`; cualquier normalized/de-vig probability debe provenir de primitive compartida, nunca de una definición privada de P4. |
| **P4-PD-010 — Standard Handicap** | Fuera del scope inicial de P4 v4. Mantener separado de AH. Solo reconsiderar si la auditoría técnica demuestra incompatibilidad real con el flujo contractual actual. |
| **P4-PD-011 — Status global** | Top-level: `ACTIVE`, `PARTIAL`, `INSUFFICIENT_DATA`, `ERROR`. `AMBIGUOUS` e `INVALID` quedan en diagnostics; si inutilizan el core requerido, el top-level es `INSUFFICIENT_DATA`. |
| **P4-PD-012 — Historical as-of** | Ningún punto con `availability_at > starts_at - target_minute` puede entrar al profile operativo. El extractor aplica el corte antes de construir ambas vistas. |
| **P4-PD-013 — Adaptive vs checkpoints** | Conservar ambas vistas. `ADAPTIVE_VIEW` es la vista primaria rica cuando existe. `CHECKPOINT_VIEW` es la vista estandarizada/comparable y fallback. Ninguna sustituye a la otra. |
| **P4-PD-014 — Resumen por ventanas** | Por ventana conservar start/end, net, path, point/leg count, sign changes, timestamps, elapsed, direction y pattern. `MOVE_SHARE_BY_WINDOW = WINDOW_PATH_LENGTH / TOTAL_PATH_LENGTH`. |
| **P4-PD-015 — Comparabilidad adaptive/fallback** | Métricas sensibles a resolución deben identificar vista/source mode y no compararse ciegamente. Persistir `TRAJECTORY_SOURCE_MODE`, `OBSERVATION_COUNT`, `LEG_COUNT`. Endpoint metrics son más comparables cuando comparten endpoints. |
| **P4-PD-016 — Tn con adaptive history** | Adaptive history termina en su último snapshot persistido causalmente seguro. Tn se agrega o reetiqueta como `observation_kind = OPERATIVE_ENDPOINT`, aunque el valor sea igual al último snapshot. No se falsifica un `SIGNIFICANT_CHANGE @ Tn`. |

---

## 78.1 Ejemplo canónico PD-001/002

```text
LEG_DIRECTIONS_RAW
[+1, 0, -1]

ZERO_BRIDGED_SIGN_CHANGE
TRUE

SIGN_CHANGE_COUNT_RAW
1

PATTERN_RAW
REVERSAL

TURNING_STRUCTURE
PLATEAU
```

Interpretación:

el mercado avanzó en una dirección, se mantuvo plano y posteriormente salió en dirección opuesta.

El plateau no borra el reversal.

Solo cambia la forma de localizar el giro.

---

## 78.2 Ejemplo canónico PD-003

```text
T360 = 0.10
T120 = NULL
T30  = 0.30
T5   = 0.40
```

Podemos afirmar:

```text
NET_MOVE_T360_T5 = +0.30
```

pero no conocemos el recorrido T360→T30.

Por tanto:

```text
NET
→ puede cruzar gap

PATH
→ no inventa el tramo perdido
```

---

## 78.3 Ejemplo canónico PD-005/006

```text
+0.10
+0.08
-0.05
-0.07
+0.03
```

Directional runs:

```text
RUN 1 = +0.18
RUN 2 = -0.12
RUN 3 = +0.03
```

Resultado:

```text
INITIAL_RUN = +0.18
CORRECTION_RUN = -0.12

CORRECTION_RATIO_RAW = 0.12 / 0.18 = 0.667
MOVE_RETENTION_RAW = 1 - 0.667 = 0.333
```

Si:

```text
RUN 2 = -0.25
```

entonces:

```text
CORRECTION_RATIO_RAW = 1.389
OVERSHOOT_RAW = TRUE
OVERSHOOT_MAGNITUDE_RAW = 0.07
```

---

## 78.4 Ejemplo canónico PD-008

```text
T120
Over 2.5 @ 1.90

T30
Over 3.0 @ 2.05
```

Resultado:

```text
LINE_MOVE_RAW = +0.5

PRICE_DELTA_RAW = NULL
PRICE_COMPARABLE = FALSE
reason = CONTRACT_CHANGED
```

La nueva línea inicia un nuevo price segment.

---

## 78.5 Arquitectura canónica PD-013

```text
P4
│
├── ADAPTIVE_VIEW
│   └── trayectoria de mayor resolución disponible
│
└── CHECKPOINT_VIEW
    └── vista estandarizada/comparable/fallback
```

Adaptive puede observar:

```text
T287 → T241 → T173 → T88 → T42 → T18 → T5
```

y calcular, por ejemplo:

```text
PATH_LENGTH_ADAPTIVE
SIGN_CHANGE_COUNT_ADAPTIVE
PATTERN_ADAPTIVE
CORRECTION_ADAPTIVE
VELOCITY_SEQUENCE_ADAPTIVE
```

En paralelo, Checkpoint View puede observar:

```text
T360 → T120 → T30 → T5
```

y calcular:

```text
NET_MOVE_CHECKPOINT
PATH_LENGTH_CHECKPOINT
PATTERN_CHECKPOINT
```

cuando esos checkpoints existen.

---

## 78.6 Ejemplo canónico PD-014

Dentro de una ventana:

```text
+0.10
-0.08
+0.18
```

persistir:

```text
WINDOW_NET_MOVE = +0.20
WINDOW_PATH_LENGTH = 0.36
```

y:

```text
MOVE_SHARE_BY_WINDOW =
WINDOW_PATH_LENGTH / TOTAL_PATH_LENGTH
```

No usar net para medir cuánto movimiento total ocurrió.

---

## 78.7 Regla canónica PD-015

Si:

```text
Evento A
ADAPTIVE = 15 points

Evento B
FALLBACK = 3 points
```

no interpretar directamente:

```text
SIGN_CHANGE_COUNT_A = 4
SIGN_CHANGE_COUNT_B = 1
```

como prueba de que A fue objetivamente más inestable.

A tuvo mayor resolución observacional.

Por tanto Minería debe conocer:

```text
TRAJECTORY_SOURCE_MODE
OBSERVATION_COUNT
LEG_COUNT
```

y las métricas sensibles a resolución deben identificar su vista.

---

## 78.8 Ejemplo canónico PD-016

```text
último ADAPTIVE_CHANGE:
T18
value = 1.88

OPERATIVE_ENDPOINT:
T5
value = 1.88
```

Persistir dos observaciones conceptualmente distintas:

```text
T18
observation_kind = ADAPTIVE_CHANGE

T5
observation_kind = OPERATIVE_ENDPOINT
```

El leg:

```text
T18 → T5
DELTA_RAW = 0
```

es válido.

No significa que ocurrió un nuevo significant change en T5.

Significa que el valor permaneció igual desde el último cambio persistido hasta la lectura que System I utilizó.

# 79. Resultado de auditoría e implementación técnica

Las decisiones `P4-PD-001…P4-PD-016` se materializaron en una implementación nueva; el `drift_engine` legacy y sus tests dejaron de ser autoridad.

## 79.1 Pipeline y target dinámico

`pillar_pipeline.py` construye `EventContext` y `OddsTrajectoryContext`, e invoca P4 con `target_minute=evaluation_minute`. P4 exige ese target exacto. La configuración productiva actual sigue usando `5`, pero el engine no contiene un endpoint T5 hardcodeado.

## 79.2 Corte causal

`extract_p4_trajectory_inputs()` calcula:

```text
operative_as_of = starts_at - timedelta(minutes=target_minute)
```

y excluye antes de cualquier proyección todo snapshot con `availability_at > operative_as_of`. Las pruebas cubren T1/T0/T-5 y el caso en que un snapshot futuro declara un `source_collected_at` antiguo.

## 79.3 Persistencia y limitación de procedencia

`ChoiceOddsTrajectory.snapshots` es la fuente de `ADAPTIVE_VIEW`; los checkpoints se reproyectan desde esos mismos puntos seguros. La tabla legacy no conserva `observation_kind`, `ingested_at`, batch id ni `main_line_at_capture`, y `collected_at` tiene procedencia mixta. El profile y `raw` lo declaran como `LEGACY_MIXED_COLLECTED_AT`. Se adoptó expresamente una implementación conservadora sin migración.

## 79.4 Vistas, endpoint y T360

La vista adaptativa preserva 0..N snapshots y agrega o reetiqueta la observación del target exacto como `OPERATIVE_ENDPOINT`. La vista checkpoint usa los targets esperados del contexto. T360 funciona si ya está presente; P4 no altera scheduler, costos ni adquisición.

## 79.5 Cobertura e identidad

El scope implementado admite Pinnacle (`302`), bet365 (`3`) y Betfair (`4`); conserva mercado, período, contrato, choice, bookmaker, source y exchange side/level. `BACK` y `LAY` no se agregan. `exchange_size` y `source_limit` son series derivadas cuando alcanzan el endpoint. Standard Handicap queda excluido; Asian Handicap permanece incluido.

## 79.6 Cálculos y gaps

La matemática vive en módulos puros. Probabilidad implícita RAW usa `1 / decimal_odds`. Un net entre endpoints puede sobrevivir un gap, pero cada leg no contiguo se marca `CONTIGUOUS_RAW = false`; path, efficiency, velocity, turning, correction y pattern no inventan ese tramo.

## 79.7 Status y output

El envelope usa `ACTIVE`, `PARTIAL`, `INSUFFICIENT_DATA` y `ERROR`; ambigüedad e invalidez permanecen en diagnostics. `P4SignalProfile.to_dict()` es la fuente del payload top-level y del módulo único `p4_signal_engine`. `STRUCTURAL_DOMAIN_SUMMARY` indexa `SERIES_ID` por estado para conservar desacuerdos.

## 79.8 Minería

`P4MiningAdapter` está registrado junto a P2/P3 y produce `PillarMiningRun` con una unidad `summary` y una unidad `module`, sin score escalar. Persiste el profile canónico y las dimensiones de target/contexto necesarias.

## 79.9 Verificación automatizada

Las pruebas específicas cubren target dinámico, ausencia sin fallback, non-leakage, endpoint único, gaps, zero bridging, cambio y ambigüedad de línea, serialización y adapter de Minería. La suite integrada de pilares valida además que el registro nuevo no modifica el comportamiento de P2/P3.

# 80. Definition of Done verificada

La implementación v4 satisface y prueba los siguientes invariantes:

```text
[x] Target Tn dinámico, exacto y sin fallback a otro minuto.
[x] Corte causal nominal; ningún availability_at posterior entra al profile.
[x] Profile NULL/INSUFFICIENT_DATA cuando ninguna serie alcanza Tn.
[x] ADAPTIVE_VIEW y CHECKPOINT_VIEW separadas, con cardinalidad 0..1.
[x] Series de longitud variable, timestamps no sincronizados y T360 opcional.
[x] Identidad por mercado, período, contrato, bookmaker, choice y exchange side/level.
[x] Ambigüedad de línea diagnosticada; no merge, promedio ni selección silenciosa.
[x] Precio segmentado por contrato; línea inferida solo bajo unicidad checkpoint.
[x] BACK/LAY y exchange_size separados, sin agregaciones inventadas.
[x] NULL propagation, ausencia de threshold P4 y OPEN no usado como T360.
[x] Signos, zero bridging, plateau, turning zone y MULTI_REVERSAL deterministas.
[x] NET puede cruzar gaps; PATH y métricas dependientes no inventan legs.
[x] Directional runs, correction ratio, retention y overshoot sin cap arbitrario.
[x] Velocidad por timestamp real; ventanas y empates dominantes conservados.
[x] Implied probability RAW = 1/odds y semantic edges reutilizan P2/P3.
[x] Envelope/profile/módulo/status compatibles con el patrón P2/P3.
[x] STRUCTURAL_DOMAIN_SUMMARY indexa estados sin mezclar SIDE y TOTALS.
[x] P4MiningAdapter produce summary + module sin score.
[x] Tests de non-leakage, vistas, gaps, línea, semántica y Minería.
```

La limitación `LEGACY_MIXED_COLLECTED_AT` permanece visible y aceptada bajo la decisión de no migrar persistencia en esta versión.

# 81. Arquitectura temporal implementada

```text
EventContext + OddsTrajectoryContext + target_minute
                         │
                         ▼
             extract_p4_trajectory_inputs
                         │
              cutoff availability_at <= as_of_Tn
                         │
             ┌───────────┴────────────┐
             ▼                        ▼
 ChoiceOddsTrajectory.snapshots   reproyección de los mismos
 0..N + OPERATIVE_ENDPOINT Tn     snapshots seguros a targets
             │                        │
             ▼                        ▼
       ADAPTIVE_VIEW             CHECKPOINT_VIEW
                                      │
                           semantic metrics P2/P3
             └───────────┬────────────┘
                         ▼
                P4SignalProfile DTO
                         │
          envelope + módulo p4_signal_engine
                         │
                         ▼
                  P4MiningAdapter
```

`odds_values/meta_by_minute` siguen siendo la proyección de compatibilidad consumida por P2/P3. P4 reproyecta snapshots ya filtrados para que un mapa de checkpoints construido en una simulación posterior no pueda reintroducir leakage.

T360 se consume automáticamente si aparece en `target_minutes_expected` y tiene candidato; P4 no cambia la captura ni el scheduler.

# 82. Principio rector

P4 representa:

> **Todo lo que el mercado mostró hasta la lectura operativa Tn, usando únicamente observaciones disponibles en ese instante.**

Minería determina cuáles de esas características RAW demostraron significado predictivo. P4 no emite score, confidence, strength ni recomendación de apuesta.

Los datos posteriores a Tn quedan fuera de `P4_SIGNAL_PROFILE`. `POST_SIGNAL_MARKET_AUDIT` no se implementa en v1; si se añade después, será un componente separado.

# 83. Estado final del documento

Este archivo es el **P4 FINAL IMPLEMENTATION BLUEPRINT v4** correspondiente a `p4-signal-profile-v1`.

La implementación, el contrato serializado y el adapter de Minería ya están materializados. Los cambios futuros de adquisición o una migración de procedencia temporal deben versionarse sin reinterpretar perfiles históricos.
