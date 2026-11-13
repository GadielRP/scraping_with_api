"""Universal domain models for competition berths and qualification."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.competition_ids import name_of
from modules.competition.final_outcome import ParticipantIdentity, CupFinalEvidence
from modules.competition.rule_terms import DestinationPhase, DestinationRound, Finalist, QualificationPath

if TYPE_CHECKING:
    from modules.competition.season_aggregate import SeasonStanding


class BerthResolution(StrEnum):
    """How far a sporting berth is resolved."""
    CONFIRMED = "CONFIRMED"      # The holder is decided and will not change
    PROJECTED = "PROJECTED"      # The holder according to the current table and evidence; may change
    UNRESOLVED = "UNRESOLVED"    # No holder can be named yet: missing evidence or a tied position
    UNAWARDED = "UNAWARDED"      # The berth was not awarded to anyone


@dataclass(frozen=True, slots=True)
class QualificationDestination:
    """The competition phase reached and the route used to reach it."""
    destination_competition_identity: CompetitionIdentity | None = None
    destination_competition_name: str | None = None
    destination_phase: DestinationPhase | None = None
    destination_round: DestinationRound | None = None
    qualification_path: QualificationPath | None = None

    def __post_init__(self) -> None:
        identity = self.destination_competition_identity
        if isinstance(identity, dict):
            identity = CompetitionIdentity(**identity)
            object.__setattr__(self, "destination_competition_identity", identity)
        if identity is not None and not isinstance(identity, CompetitionIdentity):
            raise ValueError("destination_competition_identity requires a provider identity")
        if self.destination_competition_name is None and identity is not None:
            object.__setattr__(self, "destination_competition_name", name_of(identity))
        if self.destination_competition_name is not None:
            if not isinstance(self.destination_competition_name, str) or not self.destination_competition_name.strip():
                raise ValueError("destination_competition_name must be a non-empty string")
        if self.destination_phase is not None:
            object.__setattr__(self, "destination_phase", DestinationPhase(self.destination_phase))
        if self.destination_round is not None:
            object.__setattr__(self, "destination_round", DestinationRound(self.destination_round))
            if self.destination_phase is None:
                raise ValueError("destination_round requires destination_phase")
        if self.qualification_path is not None:
            object.__setattr__(self, "qualification_path", QualificationPath(self.qualification_path))


@dataclass(frozen=True, slots=True)
class TournamentBerthRule:
    """A berth awarded through a knockout tournament or playoff final.

    ``before_table_slot`` is the 1-based position in the table-berth sequence
    that this berth precedes: table berths from that position onward are
    allocated after it. ``None`` places it after every table berth.
    ``reallocate_to_table`` hands the berth to the best remaining table club
    when no eligible club wins it. ``open_to_outsiders`` lets a winner outside
    the table (for example from a lower division) keep the berth.
    """
    destination: QualificationDestination
    competition: CompetitionIdentity
    finalists: tuple[Finalist, ...] = (Finalist.WINNER,)
    replacement_rounds: tuple[tuple[str, int], ...] = ()
    reallocate_to_table: bool = False
    before_table_slot: int | None = 1
    open_to_outsiders: bool = False
    sources: tuple[str, ...] = ()
    competition_name: str | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        object.__setattr__(self, "competition_name", name_of(self.competition))


@dataclass(frozen=True, slots=True)
class TableBerthRule:
    """Berths awarded sequentially to the highest-ranking eligible clubs in standings."""
    destination: QualificationDestination
    places: int


@dataclass(frozen=True, slots=True)
class QualificationRules:
    """Declarative specification of qualification allocations for a competition."""
    destination_season_offset: int = 1
    tournament_berths: tuple[TournamentBerthRule, ...] = ()
    table_berths: tuple[TableBerthRule, ...] = ()

    @property
    def ordered_berths(self) -> tuple[tuple[QualificationDestination, TournamentBerthRule | None], ...]:
        """Every berth as (destination, tournament rule or None) in allocation order."""
        table = [rule.destination for rule in self.table_berths for _ in range(rule.places)]
        ordered = []
        for position in range(1, len(table) + 2):
            ordered.extend(
                (rule.destination, rule) for rule in self.tournament_berths
                if (rule.before_table_slot is not None and min(rule.before_table_slot, len(table) + 1) == position)
            )
            if position <= len(table):
                ordered.append((table[position - 1], None))
        ordered.extend(
            (rule.destination, rule) for rule in self.tournament_berths
            if rule.before_table_slot is None
        )
        return tuple(ordered)


@dataclass(frozen=True, slots=True)
class AllocatedBerth:
    """A resolved berth: the plaza (``destination``) and the club that holds it.

    ``source`` is the tournament the plaza belongs to (None for table plazas),
    even when the plaza was reassigned to the table. ``issue`` is None for a
    plain assignment and names the complication otherwise (unresolved evidence,
    unawarded, reassigned to the table, or waiting on an earlier berth).
    ``table_rank`` is the holder's position in the table, None if pending or
    outside the table. ``final`` is the evidence that decided a tournament berth.
    """
    destination: QualificationDestination
    participant: ParticipantIdentity | None
    source: CompetitionIdentity | None
    resolution: BerthResolution
    issue: str | None = None
    table_rank: int | None = None
    final: CupFinalEvidence | None = None
    source_name: str | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_name", name_of(self.source))


@dataclass(frozen=True, slots=True)
class BerthAllocation:
    """The resolved qualification berths, in priority order, with the evidence behind them.

    ``table`` is the ranking table shared by several tournaments when the clubs
    are ranked by one (Argentina's annual table); None otherwise.
    """
    destination_season_year: int
    berths: tuple[AllocatedBerth, ...]
    table: tuple[SeasonStanding, ...] | None = None

    @property
    def berths_by_source(self) -> dict[CompetitionIdentity, AllocatedBerth]:
        """Berths that belong to a tournament, keyed by that tournament."""
        return {berth.source: berth for berth in self.berths if berth.source is not None}

