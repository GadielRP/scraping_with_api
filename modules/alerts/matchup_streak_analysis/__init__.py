"""
Matchup Streak Analysis module.

Public API:
    MatchupStreakContext      - Dataclass holding full analysis payload
    build_matchup_streak_context - Build analysis for an upcoming event
    should_send_streak_alert - Gate function to decide if alert should be sent
"""

from .run_matchup_streak_analysis import (
    MatchupStreakContext,
    build_matchup_streak_context,
    should_send_streak_alert,
)

__all__ = [
    "MatchupStreakContext",
    "build_matchup_streak_context",
    "should_send_streak_alert",
]
