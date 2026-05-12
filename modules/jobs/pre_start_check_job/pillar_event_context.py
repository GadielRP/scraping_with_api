from __future__ import annotations

import logging
from typing import Dict, Optional, Set

from modules.pillars.context import (
    CompetitionContext,
    EventContext,
    ParticipantContext,
)

logger = logging.getLogger(__name__)


def _build_participant_context(participant_obj, legacy_name: str) -> ParticipantContext:
    if participant_obj is not None:
        return ParticipantContext(
            participant_id=getattr(participant_obj, "participant_id", None),
            source=getattr(participant_obj, "source", None),
            source_participant_id=getattr(participant_obj, "source_participant_id", None),
            name=getattr(participant_obj, "name", legacy_name) or legacy_name,
            slug=getattr(participant_obj, "slug", None),
            short_name=getattr(participant_obj, "short_name", None),
            source_status="normalized",
        )

    return ParticipantContext(
        participant_id=None,
        source=None,
        source_participant_id=None,
        name=legacy_name,
        slug=None,
        short_name=None,
        source_status="legacy_fallback",
    )


def _build_competition_context(competition_obj, legacy_name: str) -> CompetitionContext:
    if competition_obj is not None:
        number_of_teams = getattr(competition_obj, "number_of_teams", None)
        return CompetitionContext(
            competition_id=getattr(competition_obj, "competition_id", None),
            source=getattr(competition_obj, "source", None),
            source_tournament_id=getattr(competition_obj, "source_tournament_id", None),
            source_unique_tournament_id=getattr(competition_obj, "source_unique_tournament_id", None),
            canonical_name=getattr(competition_obj, "canonical_name", None),
            display_name=getattr(competition_obj, "display_name", None) or legacy_name,
            slug=getattr(competition_obj, "slug", None),
            unique_slug=getattr(competition_obj, "unique_slug", None),
            category_id=getattr(competition_obj, "category_id", None),
            category_name=getattr(competition_obj, "category_name", None),
            number_of_teams=number_of_teams,
            number_of_teams_source="db" if number_of_teams is not None else "missing",
            source_status="normalized",
        )

    return CompetitionContext(
        competition_id=None,
        source=None,
        source_tournament_id=None,
        source_unique_tournament_id=None,
        canonical_name=legacy_name,
        display_name=legacy_name,
        slug=None,
        unique_slug=None,
        category_id=None,
        category_name=None,
        number_of_teams=None,
        number_of_teams_source="missing",
        source_status="legacy_fallback",
    )


def _determine_context_status(
    home_status: str,
    away_status: str,
    competition_status: str,
) -> str:
    statuses: Set[str] = {home_status, away_status, competition_status}
    if statuses == {"normalized"}:
        return "normalized"
    if statuses == {"legacy_fallback"}:
        return "legacy_compat"
    return "mixed"


def build_event_context(
    event_obj,
    minutes_until_start: Optional[int] = None,
    metadata_snapshot: Optional[Dict] = None,
) -> EventContext:
    metadata_snapshot = metadata_snapshot or {}

    home_name = getattr(event_obj, "home_team", None) or "Unknown"
    away_name = getattr(event_obj, "away_team", None) or "Unknown"

    home = _build_participant_context(getattr(event_obj, "home_participant", None), home_name)
    away = _build_participant_context(getattr(event_obj, "away_participant", None), away_name)
    competition = _build_competition_context(
        getattr(event_obj, "competition_ref", None),
        getattr(event_obj, "competition", None) or "Unknown",
    )

    return EventContext(
        event_id=getattr(event_obj, "id", 0),
        custom_id=getattr(event_obj, "custom_id", None),
        sport=getattr(event_obj, "sport", None) or "Unknown",
        season_id=getattr(event_obj, "season_id", None),
        season_name=metadata_snapshot.get("season_name"),
        season_year=metadata_snapshot.get("season_year"),
        start_time_utc=getattr(event_obj, "start_time_utc", None),
        minutes_until_start=minutes_until_start,
        discovery_source=getattr(event_obj, "discovery_source", None),
        home=home,
        away=away,
        competition=competition,
        participants_label=f"{home.name} vs {away.name}",
        context_status=_determine_context_status(
            home.source_status,
            away.source_status,
            competition.source_status,
        ),
    )


def infer_number_of_teams_from_streak_analysis(streak_analysis) -> Optional[int]:
    sport = getattr(streak_analysis, "sport", None)
    if sport in {"Tennis", "Tennis Doubles"}:
        return None

    inferred_count = count_unique_teams_from_streak_analysis(streak_analysis)
    if inferred_count <= 1:
        return None
    return inferred_count


def count_unique_teams_from_streak_analysis(streak_analysis) -> int:
    unique_team_names: Set[str] = set()

    for result in (getattr(streak_analysis, "home_team_results", None) or []):
        for key in ("team_name", "opponent_name"):
            value = result.get(key)
            if value is None:
                continue
            normalized = str(value).strip()
            if not normalized or normalized == "Unknown":
                continue
            unique_team_names.add(normalized)

    for result in (getattr(streak_analysis, "away_team_results", None) or []):
        for key in ("team_name", "opponent_name"):
            value = result.get(key)
            if value is None:
                continue
            normalized = str(value).strip()
            if not normalized or normalized == "Unknown":
                continue
            unique_team_names.add(normalized)

    return len(unique_team_names)
