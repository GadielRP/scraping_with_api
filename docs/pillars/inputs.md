# Pipeline In-Memory Objects & Context Contracts

This document specifies the in-memory contracts used while evaluating an event
through the pillar pipeline. The main orchestration lives in
[`pillar_pipeline.py`](../../modules/jobs/pre_start_check_job/pillar_pipeline.py);
the alert flow reuses the same event context through
[`alert_pipeline.py`](../../modules/jobs/pre_start_check_job/alert_pipeline.py).

The pipeline has three layers of input data:

1. `EventContext`: canonical event, participants, competition, and lifecycle
   metadata.
2. `OddsTrajectoryContext`: one shared, structured read model of the raw odds
   trajectory.
3. Consumer-specific projections: a strict target-minute snapshot for P2/P3,
   a filtered 1X2 trajectory for P5, and typed causal series for P4.

The context is assembled once per event. Pillars must consume these objects and
must not query the database to reconstruct a second source of truth.

---

## 1. Runtime assembly and ownership

The pre-start pillar flow performs these steps:

1. Build `EventContext` from the normalized event and its related metadata.
2. Load provider observations into `EventContext.odds_trajectory` as serialized
   `OddsTrajectoryPoint` dictionaries.
3. Build `OddsTrajectoryContext` with the event's current
   `minutes_until_start` as `evaluation_minute`.
4. Select one strict structural target with
   `select_target_minute(...)`. P2 and P3 receive the resulting
   `TargetMinuteSelection`; they do not independently choose a minute.
5. Run P2, P3, and P4 against the shared context. P5 receives a derived
   context filtered to the `1X2`/`Home/Away` groups, `Full Time`, and bookie
   ID `1`.

The selected target is causal: among allowed minutes that are present and not
later than the current evaluation boundary, the latest available causal target
is selected. A missing context, unavailable trajectory, event-id mismatch, or
absence of an eligible target produces an explicit selection reason instead of
silently falling back to another minute.

---

## 2. `EventContext`

Defined in [`modules/pillars/context.py`](../../modules/pillars/context.py).
The runtime annotations for the two trajectory fields are intentionally
permissive for compatibility, but their populated values are
`OddsTrajectoryContext` instances.

```python
@dataclass
class EventContext:
    event_id: int
    custom_id: str | None
    sport: str
    season_id: int | None
    season_name: str | None
    season_year: int | None
    starts_at: datetime
    minutes_until_start: int | None
    discovery_source: str | None

    home: ParticipantContext
    away: ParticipantContext
    competition: CompetitionContext

    participants_label: str
    context_status: str

    slug: str | None = None
    gender: str | None = None
    country: str | None = None
    round: str | None = None

    observations: list[dict] = field(default_factory=list)
    odds_response: dict | None = None
    odds_trajectory: list[dict] = field(default_factory=list)

    odds_trajectory_context: OddsTrajectoryContext | None = None
    ft_1x2_odds_trajectory_context: OddsTrajectoryContext | None = None
    streak_analysis: Any | None = None

    should_send_streak_alert: bool = False
    dual_report: Any | None = None

    competition_metadata_resolved: bool = False
    success: bool = True
    alert_sent: bool = False

    created_at: datetime | None = None
    updated_at: datetime | None = None
```

### Temporal and status invariants

* `starts_at` is a timezone-aware UTC `datetime`, normalized through
  `shared.temporal.as_utc`. Naive datetimes and legacy local-hour values are not
  valid at this boundary.
* `event_id` is the canonical internal event identifier and must match the
  trajectory context when both are present.
* `context_status` is `normalized`, `mixed`, or `legacy_compat` and records
  whether normalized relations or the temporary legacy fallback populated the
  event metadata.
* `odds_trajectory` is the raw/audit channel. Analytical consumers use
  `odds_trajectory_context` instead.

### `ParticipantContext`

Used by both `home` and `away`:

```python
@dataclass
class ParticipantContext:
    participant_id: int | None
    source: str | None
    source_participant_id: int | None
    name: str
    slug: str | None
    short_name: str | None
    source_status: str
    code_name: str | None = None
    snapshot_ranking: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
```

### `CompetitionContext`

Carries canonical tournament metadata and its provenance:

```python
@dataclass
class CompetitionContext:
    competition_id: int | None
    source: str | None
    source_tournament_id: int | None
    source_unique_tournament_id: int | None
    canonical_name: str | None
    display_name: str
    slug: str | None
    unique_slug: str | None
    category_id: int | None
    category_name: str | None
    number_of_teams: int | None
    number_of_teams_source: str | None
    total_regular_season_games: int | None
    standings_grouping: str | None
    league_config_source: str | None
    has_standings_source_endpoint: bool | None
    source_status: str
    standings_response: list | None = field(default=None, repr=False)
    source_tournament_name: str | None = None
    source_unique_tournament_name: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
```

---

## 3. Raw trajectory points (`OddsTrajectoryPoint`)

Defined in
[`odds_trajectory_repository.py`](../../infrastructure/persistence/repositories/odds_trajectory_repository.py).
The repository returns `OddsTrajectoryPoint` objects; `EventContext` stores
their serialized dictionaries in `odds_trajectory`.

```python
@dataclass
class OddsTrajectoryPoint:
    event_id: int
    market_id: int | None
    canonical_market_key: str | None
    market_family: str | None
    market_display_order: int | None
    market_name: str | None
    market_group: str | None
    market_period: str | None
    line_value: Decimal | None
    bookie_id: int | None
    bookie_name: str | None
    choice_id: int | None
    choice_name: str | None
    choice_display_order: int | None
    quote_id: int | None
    source: str | None
    exchange_side: str | None
    exchange_level: int | None
    initial_odds: Decimal | None
    odds_value: Decimal | None
    snapshot_id: int | None
    source_collected_at: datetime | None
    collected_at: datetime | None
    observed_minutes_before_start: int | None
    trajectory_minutes_before_start: Decimal | None
    main_line: bool | None = None
    source_limit: Decimal | None = None
    exchange_size: Decimal | None = None
```

### Timing fields

* `observed_minutes_before_start` is the rounded integer used by the
  configured-target projection and legacy consumers.
* `trajectory_minutes_before_start` is the precise `Decimal` computed from
  `source_collected_at`, falling back to `collected_at`. It preserves the
  continuous provider timeline for trajectory-aware consumers.
* `source_collected_at` is the preferred effective timestamp. The fallback to
  `collected_at` must remain visible in diagnostics when provenance is mixed.

---

## 4. Shared trajectory read model (`OddsTrajectoryContext`)

Defined in
[`odds_trajectory_context.py`](../../modules/pillars/odds_trajectory_context.py).
It is built from raw points and exposes a deterministic, exchange-aware market
tree.

```python
@dataclass(frozen=True)
class OddsTrajectoryContext:
    available: bool
    event_id: int | None
    target_minutes_expected: list[int]
    target_minutes_present: list[int]
    missing_target_minutes: list[int]
    markets: dict[
        str,  # market_group
        dict[str, dict[str, dict[str, MarketLineOddsTrajectory]]],
    ] = field(default_factory=dict)
```

`available=True` means that at least one valid market snapshot was loaded. It
does not mean every configured target minute is present.

### Market hierarchy

`markets` is a read-model projection. Market identity is represented by the
canonical `market_group`, `market_period`, `market_name`, and nullable
`line_value`; the dictionary key for a null line is `__default__`.

```text
markets
└── market_group
    └── market_period
        └── market_name
            └── line_value_key
                └── MarketLineOddsTrajectory
                    └── bookies
                        └── bookie_key
                            └── BookieOddsTrajectory
                                └── choices
                                    └── choice_name
                                        └── ChoiceOddsTrajectory
```

### `MarketLineOddsTrajectory`

```python
@dataclass(frozen=True)
class MarketLineOddsTrajectory:
    market_id: int | None
    market_name: str
    market_group: str
    market_period: str
    line_value: str | None
    bookies: dict[str, BookieOddsTrajectory] = field(default_factory=dict)
```

### `BookieOddsTrajectory`

```python
@dataclass(frozen=True)
class BookieOddsTrajectory:
    bookie_id: int | None
    bookie_name: str
    source: str | None = "sofascore"
    exchange_side: str | None = None  # None for bookmakers; back/lay for exchanges
    exchange_level: int = 0
    choices: dict[str, ChoiceOddsTrajectory] = field(default_factory=dict)
```

### `ChoiceOddsTrajectory`

```python
@dataclass(frozen=True)
class ChoiceOddsTrajectory:
    choice_name: str
    choice_id: int | None
    initial_odds: Decimal | None
    quote_id: int | None = None
    main_line: bool | None = None
    odds_values: dict[int, Decimal] = field(default_factory=dict)
    meta_by_minute: dict[int, OddsPointMeta] = field(default_factory=dict)
    snapshots: list[OddsSnapshotPoint] = field(default_factory=list)
```

Each choice exposes two deliberately different views:

1. `odds_values` and `meta_by_minute` are the configured target-minute
   projection. Their keys are only minutes that were projected or observed for
   the configured schedule (for example `120, 30, 5, 1, 0, -5`).
2. `snapshots` is the complete chronological provider observation list. P4
   uses this list to preserve the continuous trajectory.

### `OddsPointMeta`

```python
@dataclass(frozen=True)
class OddsPointMeta:
    snapshot_id: int | None
    collected_at: datetime | None
    minutes_before_start: int | None
    target_minute: int
    distance_from_target: int | None
    quote_id: int | None = None
    changed_at: datetime | None = None
    exchange_size: Decimal | None = None
```

### `OddsSnapshotPoint`

```python
@dataclass(frozen=True)
class OddsSnapshotPoint:
    snapshot_id: int | None
    quote_id: int | None
    odds_value: Decimal
    collected_at: datetime | None
    source_collected_at: datetime | None
    minutes_before_start: Decimal | None
    source_limit: Decimal | None = None
    exchange_size: Decimal | None = None
```

For a snapshot with a known event start:

```text
minutes_before_start =
    (starts_at - (source_collected_at or collected_at)) / 60 seconds
```

Snapshots are sorted by effective timestamp (`source_collected_at`, then
`collected_at`, then `snapshot_id`). Per-choice minute maps are kept in
descending target-minute order for deterministic serialization and debugging.

The context also provides non-mutating filters used to derive consumer views:

* `filter_by_market_groups(...)`
* `filter_by_market_period(...)`
* `filter_by_bookie_ids(...)`

---

## 5. Consumer boundaries

### P2 and P3: structural target-minute snapshots

P2 (side market) and P3 (totals market) receive the same
`TargetMinuteSelection`. Their market policies declare a
`MarketSnapshotRequest` and use [`market_snapshot_extractor.py`](../../modules/pillars/market_snapshot_extractor.py)
to read:

* `choice.odds_values[target_minute]` for the price;
* `choice.meta_by_minute[target_minute]` for quote lineage;
* `market_line.line_value` for line inputs;
* `exchange_size` from the same minute metadata when required.

They do not scan arbitrary `snapshots`. Missing, invalid, or ambiguous values
are reported in the extraction contract rather than silently substituted.

### P4: causal temporal trajectory

P4 receives the shared context and an exact integer `target_minute` through
[`run_pillar_4.py`](../../modules/pillars/pillar_4/run_pillar_4.py). Its adapter
[`trajectory_policy.py`](../../modules/pillars/pillar_4/trajectory_policy.py):

* computes `operative_as_of = starts_at - target_minute`;
* excludes snapshots newer than that causal boundary;
* keeps only supported/main-line series;
* normalizes snapshots into `P4Point` values;
* projects checkpoints within the configured tolerance; and
* returns separate adaptive and checkpoint series.

P4 must not query the repository to fill gaps. If the operative endpoint is
missing, the extraction remains auditable and the resulting profile can be
`INSUFFICIENT_DATA`.

### P5: filtered full-time 1X2 trajectory

P5 receives `ft_1x2_odds_trajectory_context`, derived from the shared context
by filtering market groups to `{"1X2", "Home/Away"}`, period to `{"Full Time"}`,
and bookie IDs to `{1}`. If the filtered context is not passed explicitly,
`calculate_pillar_5` derives it from `event_context.odds_trajectory_context`.
P5 evaluates quote distribution and exact-price stability across the resulting
choices; it does not rebuild the full trajectory.

---

## 6. Typed P4 extraction output

Defined in [`modules/pillars/pillar_4/models.py`](../../modules/pillars/pillar_4/models.py).
These are P4's analytical inputs after the shared context has been normalized;
they are not a replacement for `OddsTrajectoryContext`.

### `P4Point`

```python
@dataclass(frozen=True, slots=True)
class P4Point:
    point_id: str
    value: Decimal
    effective_at: datetime
    availability_at: datetime
    minutes_before_start: Decimal
    snapshot_id: int | None = None
    quote_id: int | None = None
    collected_at: datetime | None = None
    source_collected_at: datetime | None = None
    source_limit: Decimal | None = None
    exchange_size: Decimal | None = None
    observation_kind: str = "PERSISTED_SNAPSHOT"
    target_minute: int | None = None
    distance_from_target_minutes: Decimal | None = None
```

`effective_at` describes the provider timeline; `availability_at` describes
when the observation was available to the system. Keeping both prevents a
late-arriving snapshot from being treated as historically causal.

### `P4SeriesInput`

`P4SeriesInput` carries the series identity, market/bookmaker provenance, the
ordered `points`, expected and missing target minutes, diagnostics, and whether
the operative endpoint is present. Its important fields are:

```python
@dataclass(frozen=True, slots=True)
class P4SeriesInput:
    series_id: str
    base_series_id: str
    domain: str
    view: str                 # ADAPTIVE_VIEW or CHECKPOINT_VIEW
    value_type: str
    market_id: int | None
    market_group: str
    market_period: str
    market_name: str
    line_value: str | None
    line_value_key: str
    choice_name: str
    choice_id: int | None
    main_line: bool | None
    bookie_id: int | None
    bookie_name: str
    source: str | None
    exchange_side: str | None
    exchange_level: int
    quote_id: int | None
    points: tuple[P4Point, ...]
    expected_target_minutes: tuple[int, ...] = ()
    missing_target_minutes: tuple[int, ...] = ()
    diagnostics: tuple[str, ...] = ()
    constituent_series_ids: tuple[str, ...] = ()
    operative_endpoint_present: bool = True
```

### `P4ExtractionResult`

```python
@dataclass(frozen=True, slots=True)
class P4ExtractionResult:
    event_id: int
    target_minute: int
    operative_as_of: datetime
    adaptive_series: tuple[P4SeriesInput, ...] = ()
    checkpoint_series: tuple[P4SeriesInput, ...] = ()
    periods: dict[str, Any] = field(default_factory=dict)
    missing_inputs: tuple[str, ...] = ()
    invalid_inputs: tuple[str, ...] = ()
    ambiguous_inputs: tuple[str, ...] = ()
    excluded_future_points: int = 0
    source_series_seen: int = 0
    endpoint_series_present: int = 0
    reason: str | None = None
```

`usable` is true only when at least one operative endpoint is present and at
least one adaptive or checkpoint series exists. The serialized P4 result keeps
the normalized inputs in `raw` for auditability and exposes the derived signal
profile separately.

---

## 7. Failure and lineage semantics

Across the shared extractor and pillar policies, input problems are classified
as:

* **missing**: the requested market, choice, line, target minute, or exchange
  size is absent;
* **invalid**: a present value cannot satisfy the scalar contract (for example,
  a non-positive price or negative exchange size); and
* **ambiguous**: more than one candidate matches a supposedly unique request.

These classifications are part of the result payload and debug lineage. A
consumer may apply its own completeness policy, but it must preserve the
original classification and the selected target minute.

The canonical flow is therefore:

```text
raw repository points
        |
        v
EventContext.odds_trajectory
        |
        v
OddsTrajectoryContext  -- shared market tree, target projections, snapshots
        |
        +--> TargetMinuteSelection --> P2 / P3 snapshot extraction
        |
        +--> exact target_minute ------> P4 causal trajectory extraction
        |
        +--> filtered 1X2/full-time --> P5 price-memory extraction
```

Any audit payload may retain raw observations and lineage, but no pillar should
create a second analytical trajectory from the database or from a competing
serialized payload.
