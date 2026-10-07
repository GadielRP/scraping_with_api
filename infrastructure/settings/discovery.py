"""Discovery policy and schedules. Edit this module and restart the application.

No environment overrides. Credentials/transport remain in Config; shared memory
limits remain in JobExecutionSettings. Evidence: docs/analysis/discovery-filter-plan.md.
Dataclass defaults preserve the original Config/provider defaults. Runtime
instances below pin discovery values from the supplied 2026-10-06 server snapshot.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class DiscoveryFilters:
    future_only: bool = True
    minimum_lead_minutes: int = 10
    tracked_competitions_only: bool = False
    exclude_sports: bool = False
    excluded_sports: frozenset[str] = frozenset()  # Canonical IDs, e.g. "handball".
    exclude_categories: bool = True
    # Exact (canonical sport ID, SofaScore category name) pairs; never venue country.
    excluded_categories: frozenset[tuple[str, str]] = frozenset({
        ("football", "Romania Amateur"),
        ("football", "Portugal Amateur"),
        ("football", "Slovenia Amateur"),
        ("football", "Norway Amateur"),
        ("football", "Slovakia Amateur"),
        ("football", "Ukraine Amateur"),
        ("football", "Kenya Amateur"),
        ("football", "Ecuador Amateur"),
        ("football", "Colombia Amateur"),
        ("handball", "Romania Amateur"),
    })
    exclude_competitions: bool = True
    # SofaScore uniqueTournament IDs, including when evaluating canonical events
    # for OddsPapi. These are NOT competitions.competition_id or OddsPapi IDs.
    excluded_sofascore_unique_tournament_ids: frozenset[int] = frozenset({
        28541,  # Football NCAA II Men
        27118,  # Football Germany Friendly Games
        27113,  # Football Bulgaria Friendly Games
        28932,  # Football Türkiye Friendly Games
        35960,  # Football England Friendly Games
        27115,  # Football Netherlands Friendly Games
        20468,  # Baseball Pioneer League
        19464,  # Baseball American Association
        19466,  # Baseball Atlantic League
        19465,  # Baseball Frontier League
        9409,   # Handball Club Friendly Games
    })

    def __post_init__(self):
        if self.minimum_lead_minutes < 0:
            raise ValueError("minimum_lead_minutes must be non-negative")


@dataclass(frozen=True, slots=True)
class SofascoreDiscoverySettings:
    filters: DiscoveryFilters = field(default_factory=DiscoveryFilters)
    tennis_ranking_filter_enabled: bool = False
    tennis_ranking_cutoff: int = 120  # Reject when any known participant rank >= cutoff.
    dropping_interval_hours: int = 6
    secondary_interval_hours: int = 6
    daily_check_interval_minutes: int = 30
    daily_advance_time: str = "17:02"
    daily_refresh_time: str = "08:02"
    daily_progress_retention_days: int = 1
    team_event_workers: int = 10
    odds_workers: int = 5
    team_streaks_require_odds: bool = True  # Existing feed policy: fetch odds before admitting events.

    def __post_init__(self):
        for name in ("dropping_interval_hours", "secondary_interval_hours", "daily_check_interval_minutes",
                     "team_event_workers", "odds_workers", "daily_progress_retention_days",
                     "tennis_ranking_cutoff"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
        _validate_times((self.daily_advance_time, self.daily_refresh_time))

    @property
    def dropping_times(self) -> tuple[str, ...]:
        return tuple(f"{hour:02d}:12" for hour in range(0, 24, self.dropping_interval_hours))

    @property
    def secondary_times(self) -> tuple[str, ...]:
        return tuple(f"{hour:02d}:02" for hour in range(0, 24, self.secondary_interval_hours))


@dataclass(frozen=True, slots=True)
class OddspapiDiscoverySettings:
    filters: DiscoveryFilters = field(default_factory=DiscoveryFilters)
    scheduled_times: tuple[str, ...] = ("17:47",)
    catchup_lookback_hours: int = 36
    max_catchup_runs: int = 2
    status_id: int = 0
    request_has_odds: bool | None = False  # Provider request parameter, not admission policy.
    language: str = "en"
    lookahead_days: int = 1
    persist_queue: bool = False
    max_request_window_hours: int = 48
    persistence_chunk_size: int = 50

    def __post_init__(self):
        _validate_times(self.scheduled_times)
        if self.catchup_lookback_hours < 0 or self.max_catchup_runs < 0:
            raise ValueError("catchup limits must be non-negative")
        for name in ("lookahead_days", "max_request_window_hours", "persistence_chunk_size"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")


def _validate_times(times):
    from datetime import datetime

    for time in times:
        datetime.strptime(time, "%H:%M")


# Runtime configuration from copies/server/06-10-2026/.env.prod. Values absent
# from that snapshot use the original Config/provider job defaults above.
# Each provider owns its switches and can override the shared audited lists.
SOFASCORE = SofascoreDiscoverySettings(
    # Optional tennis policy enabled explicitly; the constructor defaults to disabled.
    tennis_ranking_filter_enabled=True,
    tennis_ranking_cutoff=120,
    dropping_interval_hours=3,
    secondary_interval_hours=6,
    daily_check_interval_minutes=30,
    daily_advance_time="17:02",
    daily_refresh_time="08:02",
    daily_progress_retention_days=1,
    filters=DiscoveryFilters(
        future_only=True,
        minimum_lead_minutes=10,
        tracked_competitions_only=False,
        exclude_sports=False,
        exclude_categories=True,
        exclude_competitions=True,
    ),
)
ODDSPAPI = OddspapiDiscoverySettings(
    scheduled_times=("17:47",),
    catchup_lookback_hours=36,
    max_catchup_runs=2,
    filters=DiscoveryFilters(
        future_only=True,
        minimum_lead_minutes=10,
        tracked_competitions_only=False,
        exclude_sports=False,
        exclude_categories=True,
        exclude_competitions=True,
    ),
)
