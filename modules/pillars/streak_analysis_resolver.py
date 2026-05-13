"""Streak analysis resolver shared by alert and pillar pipelines."""

from __future__ import annotations

import logging
from typing import Optional, Tuple

from infrastructure.persistence.repositories import DualProcessOddsRepository
from modules.alerts.matchup_streak_analysis import (
    MatchupStreakContext,
    build_matchup_streak_context,
    should_send_streak_alert,
)
from modules.pillars.context import EventContext
from modules.sofascore import api_client

logger = logging.getLogger(__name__)


def _normalized_context_is_complete(event_context: EventContext) -> Tuple[bool, list[str]]:
    missing: list[str] = []

    if not event_context.home.name:
        missing.append("home.name")
    if not event_context.away.name:
        missing.append("away.name")
    if not event_context.participants_label:
        missing.append("participants_label")
    if not event_context.competition.display_name and not event_context.competition.canonical_name:
        missing.append("competition.display_name")
    if not (event_context.competition.slug or event_context.competition.unique_slug):
        missing.append("competition.slug")
    if (
        event_context.competition.source_unique_tournament_id is None
        and event_context.competition.source_tournament_id is None
    ):
        missing.append("competition.source_unique_tournament_id")
    if event_context.home.source_participant_id is None:
        missing.append("home.source_participant_id")
    if event_context.away.source_participant_id is None:
        missing.append("away.source_participant_id")
    if event_context.season_id is None:
        missing.append("season_id")
    if event_context.season_name is None:
        missing.append("season_name")
    if event_context.season_year is None:
        missing.append("season_year")

    return len(missing) == 0, missing


def _resolve_preloaded_standings_response(event_payload: dict, event_context: EventContext):
    preloaded = event_payload.get("competition_standings_response")
    if preloaded is not None:
        return preloaded
    competition = getattr(event_context, "competition", None)
    if competition is not None:
        return getattr(competition, "standings_response", None)
    return None


def resolve_matchup_streak_analysis(
    event_payload: dict,
    event_obj,
    season_id: Optional[int],
    minutes_until_start: int,
    event_context: EventContext,
    debug_mode: bool = False,
) -> Tuple[Optional[MatchupStreakContext], bool]:
    """Build or retrieve a strict ``MatchupStreakContext`` for *event_obj*."""
    streak_analysis = event_payload.get("streak_analysis")
    should_send = event_payload.get("should_send_streak_alert", False)

    if streak_analysis is not None:
        return streak_analysis, should_send

    if minutes_until_start != 30 or not getattr(event_obj, "custom_id", None):
        logger.info(f"🚫 Skipping streak analysis for event {event_obj.id} with {minutes_until_start} minutes until start")
        return None, False

    if event_context is None:
        logger.warning("missing_normalized_context_field: event_context is None for event_id=%s", getattr(event_obj, "id", "?"))
        return None, False

    is_complete, missing = _normalized_context_is_complete(event_context)
    if not is_complete:
        logger.info(
            "Skipping streak analysis for event %s due to incomplete normalized context",
            getattr(event_obj, "id", "?"),
        )
        logger.warning(
            "missing_normalized_context_field: event_id=%s missing=%s",
            getattr(event_obj, "id", "?"),
            ",".join(missing),
        )
        return None, False

    try:
        matchup_response = api_client.get_h2h_events_for_event(event_obj.custom_id)
        matchup_events = matchup_response.get("events", []) if matchup_response else []
        dual_process_odds = DualProcessOddsRepository.get_event_odds(event_obj.id)

        logger.debug(
            "normalized_context_audit: event_id=%s participants=%s competition_id=%s unique_tournament_id=%s home_team_id=%s away_team_id=%s season_id=%s",
            event_obj.id,
            event_context.participants_label,
            event_context.competition.competition_id,
            event_context.competition.source_unique_tournament_id,
            event_context.home.source_participant_id,
            event_context.away.source_participant_id,
            event_context.season_id,
        )

        streak_analysis = build_matchup_streak_context(
            event_id=event_obj.id,
            event_custom_id=event_obj.custom_id,
            event_start_time=event_context.start_time_utc,
            sport=event_context.sport,
            discovery_source=event_context.discovery_source,
            source_unique_tournament_id=event_context.competition.source_unique_tournament_id,
            source_tournament_id=event_context.competition.source_tournament_id,
            competition_name=(
                event_context.competition.display_name
                or event_context.competition.canonical_name
            ),
            competition_slug=(
                event_context.competition.slug
                or event_context.competition.unique_slug
            ),
            season_id=event_context.season_id if event_context.season_id is not None else season_id,
            season_name=event_context.season_name,
            season_year=event_context.season_year,
            participants=event_context.participants_label,
            home_team_name=event_context.home.name,
            away_team_name=event_context.away.name,
            matchup_events=matchup_events,
            minutes_until_start=minutes_until_start,
            observations=event_payload.get("observations"),
            home_team_id=event_context.home.source_participant_id,
            away_team_id=event_context.away.source_participant_id,
            standings_response=_resolve_preloaded_standings_response(event_payload, event_context),
            event_odds=dual_process_odds,
            debug_mode=debug_mode,
        )
        should_send = bool(streak_analysis and should_send_streak_alert(streak_analysis))
    except Exception as exc:
        logger.error("Error generating matchup streak analysis for event %s: %s", event_obj.id, exc)
        return None, False

    return streak_analysis, should_send
