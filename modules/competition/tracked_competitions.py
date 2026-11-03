"""Business policy for competitions processed by pre-start workflows.

The allowlist uses canonical ``competitions.competition_id`` values. It is
intentionally independent from provider-specific routing such as OddsPortal.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, Mapping

from . import competition_ids


@dataclass(frozen=True, slots=True)
class TrackedCompetition:
    """Human-readable metadata for one tracked canonical competition."""

    competition_id: int
    name: str
    sport: str


_TRACKED_COMPETITIONS: Final[tuple[TrackedCompetition, ...]] = (
    TrackedCompetition(competition_ids.NBA, "NBA", "Basketball"),
    TrackedCompetition(competition_ids.CBA, "CBA", "Basketball"), #china's league
    TrackedCompetition(competition_ids.EUROLEAGUE, "Euroleague", "Basketball"),

    TrackedCompetition(competition_ids.MLB, "MLB", "Baseball"),

    TrackedCompetition(competition_ids.BRASILEIRAO, "Brasileirao Serie A", "Football"),
    TrackedCompetition(competition_ids.LALIGA, "LaLiga", "Football"),
    TrackedCompetition(competition_ids.SERIE_A, "Serie A", "Football"),
    TrackedCompetition(competition_ids.PREMIER_LEAGUE, "Premier League", "Football"),
    TrackedCompetition(competition_ids.SAUDI_PRO_LEAGUE, "Saudi Pro League", "Football"),
    TrackedCompetition(competition_ids.BUNDESLIGA, "Bundesliga", "Football"),
    TrackedCompetition(competition_ids.LIGUE_1, "Ligue 1", "Football"),
    TrackedCompetition(competition_ids.LIGA_MX_CLAUSURA, "Liga MX Clausura", "Football"),
    TrackedCompetition(competition_ids.LIGA_MX_APERTURA, "Liga MX Apertura", "Football"),
    TrackedCompetition(competition_ids.LIGA_PORTUGAL, "Liga Portugal", "Football"),
    TrackedCompetition(competition_ids.MLS, "MLS", "Football"),
    TrackedCompetition(competition_ids.ARGENTINA_CLAUSURA, "Liga profesional de futbol argentino Clausura", "Football"),
    TrackedCompetition(competition_ids.ARGENTINA_APERTURA, "Liga profesional de futbol argentino Apertura", "Football"),
    TrackedCompetition(competition_ids.EREDIVISIE, "Eredivisie", "Football"),

    # TURNED OFF DUE TO LACK OF ODDS MARKETS DATA: CURRENTLY ODDSPAPI ONLY RETURNS FULL TIME DATA, NO 1ST HALF DATA
    # TrackedCompetition(competition_ids.LIIGA, "Liiga", "Ice hockey"),
    # TrackedCompetition(competition_ids.SHL, "SHL", "Ice hockey"),
    # TrackedCompetition(competition_ids.NHL, "NHL", "Ice hockey"),
    # TrackedCompetition(competition_ids.AHL, "AHL", "Ice hockey"),


    TrackedCompetition(competition_ids.NFL, "NFL", "American football"),


    # KNOCKOUT COMPETITIONS, WE DONT SUPPORT THEM YET BUT MIGHT DO IN THE FUTURE
    # TrackedCompetition(348, "UEFA Champions League", "Football"),
    # TrackedCompetition(146, "NBA Playoffs", "Basketball"),
    # TrackedCompetition(360, "Conmebol Libertadores", "Football"),
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
