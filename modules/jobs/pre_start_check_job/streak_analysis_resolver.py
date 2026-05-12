"""Streak analysis resolver — shared helper for building MatchupStreakContext.

Extracted from ``EventAlertProcessor._ensure_matchup_streak_analysis`` so
that both the alert pipeline and the pillar pipeline can reuse the same
construction logic without duplication.
"""

from __future__ import annotations

import logging
from typing import Optional, Tuple

from infrastructure.persistence.repositories import DualProcessOddsRepository
from modules.alerts.matchup_streak_analysis import (
    MatchupStreakContext,
    build_matchup_streak_context,
    should_send_streak_alert,
)
from modules.sofascore import api_client

logger = logging.getLogger(__name__)


def resolve_matchup_streak_analysis(
    event_payload: dict,
    event_obj,
    season_id: Optional[int],
    minutes_until_start: int,
    debug_mode: bool = False,
) -> Tuple[Optional[MatchupStreakContext], bool]:
    """Build or retrieve a ``MatchupStreakContext`` for *event_obj*.

    This mirrors the logic that was previously inside
    ``EventAlertProcessor._ensure_matchup_streak_analysis``.

    Args:
        event_payload: The event payload dict (may already contain a
            ``streak_analysis`` key).
        event_obj: The ORM event object with attributes like ``id``,
            ``custom_id``, ``home_team``, ``away_team``, etc.
        season_id: The season ID for this event.
        minutes_until_start: Minutes until the event starts.
        debug_mode: Whether to enable debug logging/output.

    Returns:
        A tuple of ``(streak_analysis, should_send_streak_alert_flag)``.
    """
    # Return cached analysis if already present
    streak_analysis = event_payload.get("streak_analysis")
    should_send = event_payload.get("should_send_streak_alert", False)

    if streak_analysis is not None:
        return streak_analysis, should_send

    # Only build at the 30-minute key moment and when we have a custom_id
    if minutes_until_start != 30 or not getattr(event_obj, "custom_id", None):
        return None, False

    try:
        meta = event_payload.get("metadata_snapshot") or {}
        dual_process_odds = DualProcessOddsRepository.get_event_odds(event_obj.id)
        matchup_response = api_client.get_h2h_events_for_event(event_obj.custom_id)
        matchup_events = matchup_response.get("events", []) if matchup_response else []

        streak_analysis = build_matchup_streak_context(
            event_id=event_obj.id,
            event_custom_id=event_obj.custom_id,
            event_start_time=event_obj.start_time_utc,
            sport=event_obj.sport,
            discovery_source=event_obj.discovery_source,
            tournament_id=meta.get("tournament_id"),
            competition_name=meta.get("tournament_name") or getattr(event_obj, "competition", None),
            competition_slug=meta.get("competition_slug"),
            season_id=int(meta.get("season_id")) if meta.get("season_id") else season_id,
            season_name=meta.get("season_name"),
            season_year=meta.get("season_year"),
            participants=f"{event_obj.home_team} vs {event_obj.away_team}",
            home_team_name=event_obj.home_team,
            away_team_name=event_obj.away_team,
            matchup_events=matchup_events,
            minutes_until_start=minutes_until_start,
            observations=event_payload.get("observations"),
            home_team_id=meta.get("home_team_id"),
            away_team_id=meta.get("away_team_id"),
            event_odds=dual_process_odds,
            debug_mode=debug_mode,
        )
        should_send = bool(streak_analysis and should_send_streak_alert(streak_analysis))
    except Exception as exc:
        logger.error(
            "Error generating matchup streak analysis for event %s: %s",
            event_obj.id,
            exc,
        )

    return streak_analysis, should_send
