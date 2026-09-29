# Pipeline In-Memory Objects & Context Contracts

This document specifies the in-memory contracts used while evaluating an event
through the pillar pipeline. The main orchestration lives in
[`pillar_pipeline.py`](../../modules/jobs/pre_start_check_job/pillar_pipeline.py);
the alert flow reuses the same event context through
[`alert_pipeline.py`](../../modules/jobs/pre_start_check_job/alert_pipeline.py).

The pipeline uses these input contracts:

1. `EventContext`: the full, mutable event context accepted by the processor
   and used directly by P1.
2. `OddsTrajectoryPoint`: typed rows loaded once from the trajectory repository
   and passed separately from `EventContext`.
3. `EventIdentity`: a small, immutable event DTO projected from `EventContext`
   for P2, P3, P4, P5, and mining persistence.
4. `OddsTrajectoryContext`: one shared, structured read model built locally from
   the loaded rows and stamped with the UTC `evaluation_as_of`. All four market
   pillars receive the same `TargetMinuteSelection` and this same context.

The trajectory context is assembled once per event. The pillars share that read
model rather than each loading and rebuilding the event's odds history.

---

## 1. Runtime assembly and ownership

The pre-start pillar flow performs these steps:

1. Build and enrich `EventContext` from the normalized event. Its
   `odds_trajectory` field is initialized empty in this production path.
2. After validating the pillar events, bulk-load one
   `dict[event_id, list[OddsTrajectoryPoint]]` from PostgreSQL, then capture one
   UTC `evaluation_as_of` and pass both values to the batch runner. The cutoff
   therefore includes the snapshots returned by that read.
3. Each event worker takes its list out of the batch map and builds one local
   `OddsTrajectoryContext`, using `EventContext.minutes_until_start` as
   `evaluation_minute`, plus the event kickoff and the shared evaluation time.
   The context keeps the full trajectory for P4 and projects only snapshots
   eligible for the configured target windows. If no separate list was passed,
   the processor can use `EventContext.odds_trajectory` as a compatibility
   fallback.
4. Create `EventIdentity` with `EventContext.to_identity()` and select one
   `TargetMinuteSelection` from the shared trajectory context. P2, P3, P4, and
   P5 receive that same selection and the same `OddsTrajectoryContext`.
5. Run P2–P5 with `EventIdentity` and the local trajectory context. Release
   references to the raw rows and that context before P1, which continues to
   receive the full `EventContext` and its `streak_analysis`.

The shared selection is causal: among allowed minutes present and not later than
the current evaluation minute, it chooses the latest eligible target. A missing
context, unavailable trajectory, event-id mismatch, or absence of an eligible
target produces an explicit selection reason. The shared timestamp policy in
`trajectory_selection.py` defines one target window and one ranking rule;
`OddsTrajectoryContext` and P4 apply those rules to their respective views.

---

## 2. `EventContext`

Defined in [`modules/pillars/context.py`](../../modules/pillars/context.py).
The processor requires this type at its public per-event boundary. Its optional
trajectory fields remain for compatibility, but the current production path
passes raw points separately and keeps `OddsTrajectoryContext` local to the
worker; it does not populate those fields on `EventContext`.

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

    odds_trajectory_context: Any | None = None
    ft_1x2_odds_trajectory_context: Any | None = None
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
* `odds_trajectory` is a compatibility fallback. In the production batch,
  loaded `OddsTrajectoryPoint` objects travel in a separate event-keyed map.
* The processor uses a local `OddsTrajectoryContext` for P2–P5 and clears raw
  odds references before P1; it does not install that context on `EventContext`.

### `EventIdentity`: DTO passed to P2–P5

`EventContext.to_identity()` copies only event identity and timing fields into
this immutable, slotted DTO. It does not carry participants as full objects,
competition metadata as a nested object, raw odds, or streak analysis:

```python
@dataclass(frozen=True, slots=True)
class EventIdentity:
    event_id: int
    participants_label: str
    starts_at: datetime
    minutes_until_start: int | None
    sport: str
    round: str | None = None
    competition_id: int | None = None
    competition_name: str | None = None
    season_id: int | None = None
    country: str | None = None
    context_status: str = "VALID"
```

P2–P5 accept either `EventIdentity` or `EventContext` at their standalone
entrypoints, but `EventPillarProcessor` passes `EventIdentity` in the current
production path. P1 still needs the full `EventContext` for team and competition
features. This division avoids retaining or passing the large odds payload in
every pillar call.

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
The repository returns `OddsTrajectoryPoint` DTOs grouped by event ID. The
key-moment job passes that map to the batch processor without first serializing
the objects into `EventContext.odds_trajectory`. The shared context builder
reads their attributes directly; it also accepts dictionaries for older callers.

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
    evaluation_as_of: datetime | None = None
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
2. `snapshots` is the chronological list of observations loaded for this
   choice. P4 uses it to preserve the available trajectory.

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
effective_at = (
    min(source_collected_at, collected_at)
    if source_collected_at is not None
    else collected_at
)
minutes_before_start = (starts_at - effective_at) / 60 seconds
```

When `source_collected_at` is missing, `collected_at` supplies the time. If a
provider timestamp is later than the stored collection timestamp, the context
uses `collected_at` for the effective trajectory position.

Snapshots are sorted by effective timestamp (`source_collected_at`, then
`collected_at`, then `snapshot_id`). Per-choice minute maps are kept in
descending target-minute order for deterministic serialization and debugging.

The context also provides non-mutating filters used to derive consumer views:

* `filter_by_market_groups(...)`
* `filter_by_market_period(...)`
* `filter_by_bookie_ids(...)`

---

## 5. Consumer boundaries

### Arguments passed to each pillar

| Pillar | Event argument from the processor | Other inputs |
|---|---|---|
| P1 | Full `EventContext` | `debug_mode`; reads `event_context.streak_analysis`. Raw odds references have already been released. |
| P2, P3, P4, and P5 | `EventIdentity` | Shared `OddsTrajectoryContext`, the same `TargetMinuteSelection`, `debug_mode`. |

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

P4 receives the same `TargetMinuteSelection` as P2, P3, and P5 through
[`run_pillar_4.py`](../../modules/pillars/pillar_4/run_pillar_4.py). The shared
policy in [`trajectory_selection.py`](../../modules/pillars/trajectory_selection.py)
computes each target window from kickoff, tolerance, and `evaluation_as_of`.
`OddsTrajectoryContext` uses those windows for its target projections, and
P4 uses the same windows to bound its trajectory:

* takes the operative target from the shared selection;
* takes the UTC evaluation boundary from `OddsTrajectoryContext`;
* retains historical points through the shared cutoff and uses the configured
  tolerance window when selecting each checkpoint;
* keeps only supported/main-line series;
* normalizes snapshots into `P4Point` values;
* selects the eligible snapshot nearest each nominal checkpoint and records its
  actual distance from that checkpoint; and
* returns separate adaptive and checkpoint series, ending each adaptive series
  at its selected operative endpoint.

The target minute is a scheduling label, not the wall-clock instant when
acquisition, persistence, and evaluation finish. For example, if kickoff is
03:10:00 UTC, nominal T−5 is 03:05:00 UTC. A Betfair quote collected at
03:05:15 UTC may serve as the T−5 endpoint when evaluation occurs at 03:05:16
UTC, but a quote collected at 03:05:45 cannot be used in that evaluation. The
result records `nominal_target_as_of`, `evaluation_as_of`, and
`operative_as_of` separately. Legacy callers without an evaluation time use
the nominal target as the cutoff.

`collected_at` is the timestamp the persistence contract assigns to the
snapshot and is the shared axis for checkpoint selection and the evaluation
cutoff. It can be approximate for reconstructed historical moments, so the
configured tolerance is applied to it consistently by every pillar.
`source_collected_at` records the provider's tick time and is used to position
P4's trajectory; if it is later than `collected_at`, P4 clamps its effective
time to `collected_at` without discarding the price. A snapshot without
`collected_at` cannot be placed on the shared timeline and is excluded.

P4 must not query the repository to fill gaps. If no usable required-source
price series exists, the global status is `INSUFFICIENT_DATA`; if at least one
required series exists but required sources or observations are incomplete or
invalid, the profile is `PARTIAL`. Optional-source gaps remain visible on their
own series and in source diagnostics without changing the global profile status.

P4 currently treats Pinnacle Sports (bookie ID 302) and bet365 (ID 3) as
required sources. Betfair Exchange (ID 4) is optional: its raw back/lay series
and derived comparisons are retained, and an incomplete Betfair price series
keeps its own `PARTIAL` status. Betfair does not gate the global profile or
view status. The serialized `SUMMARY.SOURCE_STATUS` reports `ACTIVE`,
`PARTIAL`, `MISSING`, or `NOT_PRESENT` per supported bookmaker; `MARKET.SOURCE_ROLE`
identifies each series as `REQUIRED`, `OPTIONAL`, or `DERIVED`. Per-period
`PERIODS.*.status` also measures required-source completeness;
`PERIODS.*.all_sources_status` reports completeness across required and optional
series, with separate partial-series counts for each role.

Required-source status is explicit: no required price series means
`INSUFFICIENT_DATA`; with required price series present, a missing required
source, a required-source issue, or an incomplete required price series means
`PARTIAL`; otherwise the profile is `ACTIVE`. Regular-bookmaker trajectories
remain independent. The representative bookmaker edge requires both Pinnacle
and bet365; Betfair representative and book/exchange comparisons require the
relevant Betfair back/lay and bookmaker series to be present at matching
checkpoints.

### P5: exact-price memory input

P5 receives the same full `OddsTrajectoryContext` and shared
`TargetMinuteSelection` as P2–P4. Its own `extract_p5_market_snapshot` policy chooses
the relevant Full Time 1X2/Home-Away prices and supported bookmakers. The
processor does not create or pass a separate filtered context for P5.

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

`effective_at` describes the provider timeline for a raw observation, clamped
to `collected_at` when the provider time is later; projected checkpoints use
the selected snapshot's `collected_at`. `availability_at` is the same shared
timestamp used for cutoff checks. For current snapshots this prevents a late
quote from entering an earlier evaluation; reconstructed historical moments
retain the timestamp assigned by their persistence contract.

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
    target_minute: int | None
    operative_as_of: datetime | None
    nominal_target_as_of: datetime | None = None
    evaluation_as_of: datetime | None = None
    adaptive_series: tuple[P4SeriesInput, ...] = ()
    checkpoint_series: tuple[P4SeriesInput, ...] = ()
    periods: dict[str, Any] = field(default_factory=dict)
    missing_inputs: tuple[str, ...] = ()
    missing_endpoint_details: tuple[dict[str, Any], ...] = ()
    invalid_inputs: tuple[str, ...] = ()
    ambiguous_inputs: tuple[str, ...] = ()
    excluded_future_points: int = 0
    source_series_seen: int = 0
    endpoint_series_present: int = 0
    observed_bookie_ids: tuple[int, ...] = ()
    issue_bookie_ids: tuple[int, ...] = ()
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
EventContext -----------------------------> P1 (after odds references are released)
      |
      +--> to_identity() --> EventIdentity --+--> P2 / P3 / P4 / P5

repository --> dict[event_id, list[OddsTrajectoryPoint]]
      |
      +--> one event's rows --> OddsTrajectoryContext (local to its worker)
                                    |
                                    +--> TargetMinuteSelection --> P2 / P3 / P4 / P5
```

Any audit payload may retain raw observations and lineage. Pillars do not reload
this event's odds to create a competing trajectory. P5 may query its separate
cross-event exact-price memory; that query does not rebuild the current event's
`OddsTrajectoryContext`.
