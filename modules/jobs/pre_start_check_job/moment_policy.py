"""Business semantics for configured pre-start key moments."""

from __future__ import annotations

from infrastructure.settings import Config


def is_closing_odds_moment(minutes: int | float | None) -> bool:
    return minutes == Config.PRE_START_CLOSING_ODDS_MINUTE


def is_live_odds_moment(minutes: int | float | None) -> bool:
    """Return whether a moment is at or after kickoff."""
    return minutes is not None and float(minutes) <= 0


def dual_process_moments() -> frozenset[int]:
    return frozenset((30, Config.PRE_START_CLOSING_ODDS_MINUTE))


def regular_pre_start_moments() -> tuple[int, ...]:
    """Moments owned by the normal scheduler rather than the critical lane."""
    closing = Config.PRE_START_CLOSING_ODDS_MINUTE
    return tuple(moment for moment in Config.PRE_START_ODDS_MOMENTS if moment != closing)


__all__ = [
    "dual_process_moments",
    "is_closing_odds_moment",
    "is_live_odds_moment",
    "regular_pre_start_moments",
]
