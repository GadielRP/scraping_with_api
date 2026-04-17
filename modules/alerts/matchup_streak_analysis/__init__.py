"""Matchup Streak Analysis public API."""

from .historical_form_service import (
    HistoricalFormProcessor,
    HistoricalFormService,
    historical_form_processor,
    historical_form_service,
)
from .run_matchup_streak_analysis import (
    MatchupStreakContext,
    build_matchup_streak_context,
    should_send_streak_alert,
)
from .standings_engine import (
    HistoricalStandingsCalculator,
    StandingsSimulator,
    standings_calculator,
    standings_simulator,
)

__all__ = [
    "HistoricalFormProcessor",
    "HistoricalFormService",
    "HistoricalStandingsCalculator",
    "MatchupStreakContext",
    "StandingsSimulator",
    "build_matchup_streak_context",
    "historical_form_processor",
    "historical_form_service",
    "should_send_streak_alert",
    "standings_calculator",
    "standings_simulator",
]
