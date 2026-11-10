"""Canonical discipline vocabulary and application boundaries (no ORM or HTTP)."""
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol

from shared.temporal import as_utc


class IncidentType(str, Enum):
    YELLOW_CARD = "yellow_card"
    RED_CARD = "red_card"
    RED_CARD_SECOND_YELLOW = "red_card_second_yellow"
    EJECTION = "ejection"
    FOUL_OUT = "foul_out"


# A second caution counts in both inclusive card totals.
SECOND_YELLOW_TOTAL_TYPES = (IncidentType.YELLOW_CARD, IncidentType.RED_CARD)


class Origin(str, Enum):
    REPORTED = "reported"
    DERIVED = "derived"


class DisciplineEndpoint(str, Enum):
    COMMENTS = "comments"
    STATISTICS = "statistics"


class Side(str, Enum):
    HOME = "home"
    AWAY = "away"


class Outcome(str, Enum):
    READY = "ready"
    UPDATED = "updated"
    UNCHANGED = "unchanged"
    UNAVAILABLE = "unavailable"
    INVALID = "invalid"
    DEFERRED = "deferred"
    MAPPING_CONFLICT = "mapping_conflict"
    FAILED = "failed"


class FactKind(str, Enum):
    YELLOW_CARD = "yellow_card"
    RED_CARD = "red_card"
    RED_CARD_SECOND_YELLOW = "red_card_second_yellow"
    EJECTION = "ejection"
    PERSONAL_FOUL = "personal_foul"
    TECHNICAL_FOUL = "technical_foul"
    UNKNOWN_FOUL = "unknown_foul"
    UNSUPPORTED_EXCEPTION = "unsupported_exception"


@dataclass(frozen=True, slots=True)
class IncidentDraft:
    type: IncidentType
    origin: Origin
    source_player_id: str | None
    player_name: str | None
    side: Side


@dataclass(frozen=True, slots=True)
class DisciplineEventContext:
    event_id: int
    mapping_id: int | None
    source_event_id: str | None
    source: str
    sport: str
    competition_id: int | None
    season_id: int | None
    source_home_participant_id: int | None
    source_away_participant_id: int | None
    mapping_count: int = 1


@dataclass(frozen=True, slots=True)
class DisciplineFact:
    kind: FactKind
    source_player_id: str | None
    player_name: str | None
    side: Side | None
    identity: tuple[str, str] | None = None


@dataclass(frozen=True, slots=True)
class DisciplineTotalDraft:
    type: IncidentType
    side: Side
    count: int
    endpoint: DisciplineEndpoint

    def __post_init__(self):
        object.__setattr__(self, "type", IncidentType(self.type))
        object.__setattr__(self, "side", Side(self.side))
        object.__setattr__(self, "endpoint", DisciplineEndpoint(self.endpoint))
        if isinstance(self.count, bool) or not isinstance(self.count, int) or self.count < 0:
            raise ValueError("Discipline totals require a nonnegative integer")


@dataclass(frozen=True, slots=True)
class Acquisition:
    outcome: Outcome
    incidents: tuple[IncidentDraft, ...] = ()
    managed_types: frozenset[IncidentType] = frozenset()
    reason: str | None = None
    comments_count: int = 0
    totals: tuple[DisciplineTotalDraft, ...] = ()
    statistics_attempted: bool = False
    statistics_reason: str | None = None

    @property
    def managed_total_scopes(self):
        return frozenset((total.type, total.side) for total in self.totals)


@dataclass(frozen=True, slots=True)
class EventReport:
    outcome: Outcome
    acquisition: Acquisition | None = None


@dataclass(frozen=True, slots=True)
class Selection:
    event_id: int | None = None
    start: datetime | None = None
    end: datetime | None = None
    competition_id: int | None = None
    season_id: int | None = None
    after_event_id: int = 0
    event_ids: tuple[int, ...] = ()

    def __post_init__(self):
        modes = (self.event_id is not None, self.start is not None or self.end is not None,
                 self.competition_id is not None or self.season_id is not None, bool(self.event_ids))
        if sum(modes) != 1:
            raise ValueError("Choose event ID, date range, or competition ID with season ID")
        if modes[1] and (self.start is None or self.end is None or self.start >= self.end):
            raise ValueError("A nonempty start/end range is required")
        if modes[1]:
            object.__setattr__(self, "start", as_utc(self.start))
            object.__setattr__(self, "end", as_utc(self.end))
        if modes[2] and (self.competition_id is None or self.season_id is None):
            raise ValueError("Competition and season IDs are required together")
        for value in (self.event_id, self.competition_id, self.season_id):
            if value is not None and value <= 0:
                raise ValueError("IDs must be positive")
        if any(value <= 0 for value in self.event_ids):
            raise ValueError("Event IDs must be positive")
        if self.after_event_id < 0:
            raise ValueError("after_event_id cannot be negative")


@dataclass(frozen=True, slots=True)
class TeamSeasonTotals:
    yellow_cards: int
    red_cards: int
    ejections: int
    foul_outs: int

    @property
    def total_cards(self):
        return self.yellow_cards + self.red_cards


class IncidentProvider(Protocol):
    def acquire(self, context: DisciplineEventContext) -> Acquisition: ...


class IncidentStore(Protocol):
    def iter_candidates(self, selection: Selection, page_size: int): ...
    def replace_acquisition(self, context: DisciplineEventContext, acquisition: Acquisition,
                            *, dry_run: bool = False) -> Outcome: ...


class EventExclusion(Protocol):
    def __call__(self, event_id: int) -> AbstractContextManager[bool]: ...
