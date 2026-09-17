# Pipeline In-Memory Objects & Context Contracts

This document specifies the typed in-memory objects orchestrated during match evaluation across [`modules/jobs/pre_start_check_job/pillar_pipeline.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/jobs/pre_start_check_job/pillar_pipeline.py) and [`modules/jobs/pre_start_check_job/alert_pipeline.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/jobs/pre_start_check_job/alert_pipeline.py).

The core runtime objects are:
1. `EventContext`
2. `OddsTrajectoryContext`

---

## 1. `EventContext`

Defined in [`modules/pillars/context.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/pillars/context.py).

```python
@dataclass
class EventContext:
    event_id: int  # Canonical event identifier
    custom_id: str | None
    sport: str
    season_id: int | None
    season_name: str | None
    season_year: int | None
    starts_at: datetime  # Timezone-aware UTC instant (via shared.temporal.as_utc)
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

> **Temporal contract invariant:** `starts_at` is strictly timezone-aware UTC (`datetime`). Naive datetimes and legacy local hour representations are banned at the boundary.

### `ParticipantContext`

Used for both `home` and `away` participants:

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

Used for tournament and competition metadata:

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

## 2. Raw Trajectory Points (`OddsTrajectoryPoint`)

Defined in [`infrastructure/persistence/repositories/odds_trajectory_repository.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/repositories/odds_trajectory_repository.py).

The raw trajectory loaded into `EventContext.odds_trajectory` consists of serialized dictionaries from `OddsTrajectoryPoint`:

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
    choice_group: str | None
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

### Timing Field Semantics

* `observed_minutes_before_start`: Rounded integer from `collected_at` for legacy compatibility and configured-target projection.
* `trajectory_minutes_before_start`: High-precision `Decimal` computed from `source_collected_at` (falling back to `collected_at`). Preserves the exact continuous timeline.

---

## 3. Formatted Trajectory (`OddsTrajectoryContext`)

Defined in [`modules/pillars/odds_trajectory_context.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/pillars/odds_trajectory_context.py).

Transformed and structured from raw points into a navigable hierarchy:

```python
@dataclass(frozen=True)
class OddsTrajectoryContext:
    available: bool
    event_id: int | None
    target_minutes_expected: list[int]
    target_minutes_present: list[int]
    missing_target_minutes: list[int]
    markets: dict[str, dict[str, dict[str, dict[str, MarketLineOddsTrajectory]]]] = field(default_factory=dict)
```

`available=True` indicates at least one valid snapshot exists in the context; it does not require all configured target moments to be present.

### Market Hierarchy

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

### `MarketLineOddsTrajectory`

```python
@dataclass(frozen=True)
class MarketLineOddsTrajectory:
    market_id: int | None
    market_name: str
    market_group: str
    market_period: str
    choice_group: str | None
    bookies: dict[str, BookieOddsTrajectory] = field(default_factory=dict)
```

### `BookieOddsTrajectory`

```python
@dataclass(frozen=True)
class BookieOddsTrajectory:
    bookie_id: int | None
    bookie_name: str
    source: str | None = "sofascore"
    exchange_side: str | None = None  # None for bookmakers; "back" or "lay" for exchanges
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

Dual Responsibilities:
1. `odds_values` and `meta_by_minute`: Formatted target projection used by P2/P3. Keys are exclusively configured target minutes (e.g. `120, 30, 5, 1, 0, -5`).
2. `snapshots`: Complete chronological list of all provider observations for trajectory-aware consumers (P4).

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

Here `minutes_before_start` is the precise decimal minute:

$$\text{minutes\_before\_start} = \frac{\text{starts\_at} - (\text{source\_collected\_at} \lor \text{collected\_at})}{60\text{ s}}$$

Snapshots are sorted deterministically:
1. `source_collected_at` (fallback: `collected_at`);
2. `collected_at`;
3. `snapshot_id`.

---

## 4. Consumer Boundaries (P2/P3 vs P4)

* **P2 (Side Market) & P3 (Totals Market)**: Read exclusively through `market_snapshot_extractor.py`, querying `choice.odds_values[target_minute]` and `choice.meta_by_minute[target_minute]`. They do not iterate arbitrary snapshot points.
* **P4 (Temporal Market Drift)**: Consumes the full `snapshots` trajectory to compute price velocity, line movement, and opening-to-closing drift.
* **P5 (Price Memory)**: Evaluates exact quote distribution and stability across bookmakers.
