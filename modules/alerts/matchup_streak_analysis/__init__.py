"""Matchup Streak Analysis public API."""

from .historical_form_service import (
    HistoricalFormService,
    historical_form_service,
)
from .run_matchup_streak_analysis import (
    MatchupStreakContext,
    build_matchup_streak_context,
    should_send_streak_alert,
)

__all__ = [
    "HistoricalFormService",
    "MatchupStreakContext",
    "build_matchup_streak_context",
    "historical_form_service",
    "should_send_streak_alert",
]
