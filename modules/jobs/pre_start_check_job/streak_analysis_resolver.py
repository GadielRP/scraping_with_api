"""Streak analysis resolver — shared helper for building MatchupStreakContext.

Extracted from ``EventAlertProcessor._ensure_matchup_streak_analysis`` so
that both the alert pipeline and the pillar pipeline can reuse the same
construction logic without duplication.
"""

from __future__ import annotations

import logging
from typing import Optional, Tuple

from infrastructure.persistence.repositories import DualProcessOddsRepository
from modules.pillars.context import EventContext
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
    event_context: Optional[EventContext] = None,
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
        event_context: Optional normalized pillar event context.
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

        resolved_home_name = (
            event_context.home.name
            if event_context is not None
            else getattr(event_obj, "home_team", None) or "Unknown"
        )
        resolved_away_name = (
            event_context.away.name
            if event_context is not None
            else getattr(event_obj, "away_team", None) or "Unknown"
        )
        resolved_participants = (
            event_context.participants_label
            if event_context is not None
            else f"{resolved_home_name} vs {resolved_away_name}"
        )

        resolved_competition_name = None
        competition_name_source = "legacy_fallback"
        if event_context is not None:
            resolved_competition_name = (
                event_context.competition.display_name
                or event_context.competition.canonical_name
            )
            if resolved_competition_name and event_context.competition.source_status == "normalized":
                competition_name_source = "normalized"
        if not resolved_competition_name:
            resolved_competition_name = getattr(event_obj, "competition", None) or "Unknown"

        resolved_competition_slug = None
        competition_slug_source = "legacy_fallback"
        if event_context is not None:
            resolved_competition_slug = (
                event_context.competition.slug
                or event_context.competition.unique_slug
            )
            if resolved_competition_slug and event_context.competition.source_status == "normalized":
                competition_slug_source = "normalized"
        if not resolved_competition_slug:
            resolved_competition_slug = meta.get("competition_slug")

        resolved_tournament_id = None
        tournament_id_source = "legacy_fallback"
        if event_context is not None:
            resolved_tournament_id = event_context.competition.source_tournament_id
            if (
                resolved_tournament_id is not None
                and event_context.competition.source_status == "normalized"
            ):
                tournament_id_source = "normalized"
        if resolved_tournament_id is None:
            resolved_tournament_id = meta.get("tournament_id")

        resolved_home_team_id = None
        home_team_id_source = "legacy_fallback"
        if event_context is not None:
            resolved_home_team_id = event_context.home.source_participant_id
            if (
                resolved_home_team_id is not None
                and event_context.home.source_status == "normalized"
            ):
                home_team_id_source = "normalized"
        if resolved_home_team_id is None:
            resolved_home_team_id = meta.get("home_team_id")

        resolved_away_team_id = None
        away_team_id_source = "legacy_fallback"
        if event_context is not None:
            resolved_away_team_id = event_context.away.source_participant_id
            if (
                resolved_away_team_id is not None
                and event_context.away.source_status == "normalized"
            ):
                away_team_id_source = "normalized"
        if resolved_away_team_id is None:
            resolved_away_team_id = meta.get("away_team_id")

        resolved_season_id = (
            event_context.season_id
            if event_context is not None and event_context.season_id is not None
            else season_id
        )

        resolution_audit = {
            "event_context_present": event_context is not None,
            "context_status": event_context.context_status if event_context else "legacy_compat",
            "participants_source": event_context.context_status if event_context is not None else "legacy_fallback",
            "competition_name_source": competition_name_source,
            "competition_slug_source": competition_slug_source,
            "tournament_id_source": tournament_id_source,
            "home_team_id_source": home_team_id_source,
            "away_team_id_source": away_team_id_source,
            "legacy_compat_used": event_context is None,
        }
        logger.debug("Matchup streak resolution audit for event %s: %s", event_obj.id, resolution_audit)

        streak_analysis = build_matchup_streak_context(
            event_id=event_obj.id,
            event_custom_id=event_obj.custom_id,
            event_start_time=event_obj.start_time_utc,
            sport=event_obj.sport,
            discovery_source=event_obj.discovery_source,
            tournament_id=resolved_tournament_id,
            competition_name=resolved_competition_name,
            competition_slug=resolved_competition_slug or "",
            season_id=int(meta.get("season_id")) if meta.get("season_id") else resolved_season_id,
            participants=resolved_participants,
            home_team_name=resolved_home_name,
            away_team_name=resolved_away_name,
            matchup_events=matchup_events,
            minutes_until_start=minutes_until_start,
            observations=event_payload.get("observations"),
            home_team_id=resolved_home_team_id,
            away_team_id=resolved_away_team_id,
            event_odds=dual_process_odds,
            debug_mode=debug_mode,
            season_name=(
                event_context.season_name
                if event_context is not None and event_context.season_name is not None
                else meta.get("season_name")
            ),
            season_year=(
                event_context.season_year
                if event_context is not None and event_context.season_year is not None
                else meta.get("season_year")
            ),
        )
        should_send = bool(streak_analysis and should_send_streak_alert(streak_analysis))
    except Exception as exc:
        logger.error(
            "Error generating matchup streak analysis for event %s: %s",
            event_obj.id,
            exc,
        )

    return streak_analysis, should_send
