"""Business policy for competitions processed by pre-start workflows.

The allowlist uses canonical ``competitions.competition_id`` values. It is
intentionally independent from provider-specific routing such as OddsPortal.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, Mapping


@dataclass(frozen=True, slots=True)
class TrackedCompetition:
    """Human-readable metadata for one tracked canonical competition."""

    competition_id: int
    name: str
    sport: str


_TRACKED_COMPETITIONS: Final[tuple[TrackedCompetition, ...]] = (
    TrackedCompetition(176, "NBA", "Basketball"),
    TrackedCompetition(318, "Brasileirao Serie A", "Football"),
    TrackedCompetition(129, "MLB", "Baseball"),
    TrackedCompetition(167, "LaLiga", "Football"),
    TrackedCompetition(88, "Serie A", "Football"),
    TrackedCompetition(168, "Premier League", "Football"),
    TrackedCompetition(50, "Saudi Pro League", "Football"),
    TrackedCompetition(171, "Bundesliga", "Football"),
)

TRACKED_COMPETITIONS_BY_ID: Final[Mapping[int, TrackedCompetition]] = (
    MappingProxyType(
        {
            competition.competition_id: competition
            for competition in _TRACKED_COMPETITIONS
        }
    )
)
TRACKED_COMPETITION_IDS: Final[frozenset[int]] = frozenset(
    TRACKED_COMPETITIONS_BY_ID
)


def is_tracked_competition(competition_id: object) -> bool:
    """Return whether a value identifies a tracked canonical competition."""
    try:
        normalized_id = int(competition_id)
    except (TypeError, ValueError):
        return False
    return normalized_id in TRACKED_COMPETITION_IDS


def tracked_competition_ids() -> tuple[int, ...]:
    """Return stable, deterministic IDs suitable for database filters."""
    return tuple(sorted(TRACKED_COMPETITION_IDS))


__all__ = [
    "TRACKED_COMPETITIONS_BY_ID",
    "TRACKED_COMPETITION_IDS",
    "TrackedCompetition",
    "is_tracked_competition",
    "tracked_competition_ids",
]
