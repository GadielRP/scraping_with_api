"""Explicit rules for redirecting cup qualification berths."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CupReallocationCase:
    """Select the recipient of a cup berth for a league-rank scenario.

    ``recipient_rank`` is a position in the league table. When omitted, the
    cup winner keeps the berth. Cases are evaluated in profile order and must
    not overlap.
    """

    champion_rank_start: int
    champion_rank_end: int
    recipient_rank: int | None = None

    def __post_init__(self) -> None:
        if not (1 <= self.champion_rank_start <= self.champion_rank_end):
            raise ValueError("invalid cup-winner rank range")
        if self.recipient_rank is not None and self.recipient_rank < 1:
            raise ValueError("recipient_rank must be positive")


@dataclass(frozen=True, slots=True)
class CupReallocationPolicy:
    """How a cup berth is reassigned when the cup winner already qualifies.

    Rank-specific cases model competition rules that redirect the cup berth
    even when there is no league objective with the cup's exact destination.
    Without a matching case, the cup winner is the intended recipient.
    """

    cases: tuple[CupReallocationCase, ...] = ()
    reallocate_if_champion_already_qualified: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "cases", tuple(
            case if isinstance(case, CupReallocationCase) else CupReallocationCase(**case)
            for case in self.cases
        ))
        ordered = sorted(self.cases, key=lambda case: case.champion_rank_start)
        for previous, current in zip(ordered, ordered[1:]):
            if current.champion_rank_start <= previous.champion_rank_end:
                raise ValueError("cup reallocation rank cases must not overlap")
