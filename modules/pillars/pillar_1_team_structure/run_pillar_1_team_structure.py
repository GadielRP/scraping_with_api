"""Pillar 1 - Team Structure orchestrator."""

from __future__ import annotations

import logging
from typing import Any, Dict

from modules.pillars.context import EventContext
from modules.pillars.pillar_1_team_structure.side import calculate_p1_side
from modules.pillars.pillar_1_team_structure.totals import calculate_p1_totals

logger = logging.getLogger(__name__)


def calculate_pillar_1_team_structure(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> Dict[str, Any]:
    """Calculate Pillar 1 - Team Structure (Side and Totals) for an event.

    Args:
        streak_analysis: A ``MatchupStreakContext`` produced by the streak
            analysis pipeline.
        event_context: An ``EventContext``.
        debug_mode: True to log extra details.

    Returns:
        A dictionary containing both the side and totals results.
    """
    # 1. Calculate side component (Modules M1-M7 aggregated)
    side_result = calculate_p1_side(
        streak_analysis,
        event_context=event_context,
        debug_mode=debug_mode,
    )

    # 2. Calculate totals component
    try:
        totals_result = calculate_p1_totals(
            streak_analysis,
            event_context=event_context,
            debug_mode=debug_mode,
        )
    except Exception as exc:
        logger.error(
            "Error calculating P1_TOTALS for event %s: %s",
            getattr(streak_analysis, "event_id", getattr(event_context, "event_id", "?")),
            exc,
        )
        totals_result = None

    return {
        "side": side_result,
        "totals": totals_result,
    }
