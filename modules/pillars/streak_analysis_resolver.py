"""Streak analysis resolver shared by alert and pillar pipelines."""

from __future__ import annotations

import logging
from typing import Any, Optional, Tuple

from infrastructure.persistence.repositories import DualProcessOddsRepository
from infrastructure.settings import Config
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


def _resolve_preloaded_standings_response(event_context: Optional[EventContext]):
    if event_context is None:
        return None
    competition = getattr(event_context, "competition", None)
    if competition is not None:
        return getattr(competition, "standings_response", None)
    return None


def resolve_matchup_streak_analysis(
    event_context: EventContext,
    debug_mode: bool = False,
) -> Tuple[Optional[MatchupStreakContext], bool]:
    """Build or retrieve a strict ``MatchupStreakContext`` for *event_context*."""
    if event_context is None:
        logger.warning("missing_normalized_context_field: event_context is None")
        return None, False

    event_id = getattr(event_context, "event_id", None)
    custom_id = getattr(event_context, "custom_id", None)
    minutes = getattr(event_context, "minutes_until_start", None)
    effective_season_id = getattr(event_context, "season_id", None)

    streak_analysis = getattr(event_context, "streak_analysis", None)
    should_send = getattr(event_context, "should_send_streak_alert", False)

    if streak_analysis is not None:
        logger.info(
            "Reusing precomputed streak analysis for event %s",
            event_id,
        )
        return streak_analysis, should_send

    if minutes != 5 or not custom_id:
        logger.info(f"🚫 Skipping streak analysis for event {event_id} with {minutes} minutes until start")
        return None, False

    is_complete, missing = _normalized_context_is_complete(event_context)
    if not is_complete:
        logger.info(
            "Skipping streak analysis for event %s due to incomplete normalized context",
            event_id,
        )
        logger.warning(
            "missing_normalized_context_field: event_id=%s missing=%s",
            event_id,
            ",".join(missing),
        )
        return None, False

    try:
        matchup_response = api_client.get_h2h_events_for_event(custom_id)
        matchup_events = matchup_response.get("events", []) if matchup_response else []
        raw_matchup_count = len(matchup_events)
        max_h2h_events = Config.MATCHUP_H2H_MAX_EVENTS
        if raw_matchup_count > max_h2h_events:
            # Keep the newest raw events in-place, then release the rest of the
            # response before loading team histories.
            matchup_events.sort(
                key=lambda item: item.get("startTimestamp", 0),
                reverse=True,
            )
            del matchup_events[max_h2h_events:]
            logger.info(
                "Bounded H2H payload for event %s from %s to %s newest events",
                event_id,
                raw_matchup_count,
                len(matchup_events),
            )
        matchup_response = None
        dual_process_odds = DualProcessOddsRepository.get_event_odds(event_id) if event_id is not None else None

        logger.debug(
            "normalized_context_audit: event_id=%s participants=%s competition_id=%s unique_tournament_id=%s home_team_id=%s away_team_id=%s season_id=%s",
            event_id,
            event_context.participants_label,
            event_context.competition.competition_id,
            event_context.competition.source_unique_tournament_id,
            event_context.home.source_participant_id,
            event_context.away.source_participant_id,
            event_context.season_id,
        )

        observations = getattr(event_context, "observations", None)

        streak_analysis = build_matchup_streak_context(
            event_id=event_id,
            event_custom_id=custom_id,
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
            season_id=effective_season_id,
            season_name=event_context.season_name,
            season_year=event_context.season_year,
            participants=event_context.participants_label,
            home_team_name=event_context.home.name,
            away_team_name=event_context.away.name,
            matchup_events=matchup_events,
            minutes_until_start=minutes,
            observations=observations,
            home_team_id=event_context.home.source_participant_id,
            away_team_id=event_context.away.source_participant_id,
            standings_response=_resolve_preloaded_standings_response(event_context),
            competition_context=event_context.competition,
            event_odds=dual_process_odds,
            debug_mode=debug_mode,
        )
        should_send = bool(
            streak_analysis and should_send_streak_alert(streak_analysis, event_context)
        )
        event_context.streak_analysis = streak_analysis
        event_context.should_send_streak_alert = should_send
    except Exception as exc:
        logger.error("Error generating matchup streak analysis for event %s: %s", event_id, exc)
        return None, False

    return streak_analysis, should_send
