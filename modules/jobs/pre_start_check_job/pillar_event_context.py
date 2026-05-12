from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from modules.pillars.context import (
    CompetitionContext,
    EventContext,
    ParticipantContext,
)

logger = logging.getLogger(__name__)


def _missing_context_message(event_obj, missing: list[str]) -> str:
    event_id = getattr(event_obj, "id", "?")
    return f"event_id={event_id} missing_normalized_context_fields={','.join(missing)}"


def build_event_context(
    event_obj,
    minutes_until_start: Optional[int] = None,
    metadata_snapshot: Optional[dict] = None,
) -> Optional[EventContext]:
    """Build a strict, normalized EventContext or return None.

    This intentionally refuses to fall back to legacy event fields. The new
    runtime should only proceed when participants and competition relations
    are already normalized.
    """
    del metadata_snapshot

    missing: list[str] = []
    home_participant = event_obj.__dict__.get("home_participant")
    away_participant = event_obj.__dict__.get("away_participant")
    competition_ref = event_obj.__dict__.get("competition_ref")

    if home_participant is None:
        missing.append("home_participant")
    if away_participant is None:
        missing.append("away_participant")
    if competition_ref is None:
        missing.append("competition_ref")

    if missing:
        logger.warning("Normalized EventContext unavailable: %s", _missing_context_message(event_obj, missing))
        return None

    home_name = getattr(home_participant, "name", None)
    away_name = getattr(away_participant, "name", None)
    competition_display_name = getattr(competition_ref, "display_name", None)

    if not home_name:
        missing.append("home_participant.name")
    if not away_name:
        missing.append("away_participant.name")
    if not competition_display_name:
        missing.append("competition_ref.display_name")

    if missing:
        logger.warning("Normalized EventContext unavailable: %s", _missing_context_message(event_obj, missing))
        return None

    start_time_utc = getattr(event_obj, "start_time_utc", None)
    if start_time_utc is None:
        missing.append("start_time_utc")
    if missing:
        logger.warning("Normalized EventContext unavailable: %s", _missing_context_message(event_obj, missing))
        return None

    home = ParticipantContext(
        participant_id=getattr(home_participant, "participant_id", None),
        source=getattr(home_participant, "source", None),
        source_participant_id=getattr(home_participant, "source_participant_id", None),
        name=home_name,
        slug=getattr(home_participant, "slug", None),
        short_name=getattr(home_participant, "short_name", None),
        source_status="normalized",
    )
    away = ParticipantContext(
        participant_id=getattr(away_participant, "participant_id", None),
        source=getattr(away_participant, "source", None),
        source_participant_id=getattr(away_participant, "source_participant_id", None),
        name=away_name,
        slug=getattr(away_participant, "slug", None),
        short_name=getattr(away_participant, "short_name", None),
        source_status="normalized",
    )
    competition = CompetitionContext(
        competition_id=getattr(competition_ref, "competition_id", None),
        source=getattr(competition_ref, "source", None),
        source_tournament_id=getattr(competition_ref, "source_tournament_id", None),
        source_unique_tournament_id=getattr(competition_ref, "source_unique_tournament_id", None),
        canonical_name=getattr(competition_ref, "canonical_name", None),
        display_name=competition_display_name,
        slug=getattr(competition_ref, "slug", None),
        unique_slug=getattr(competition_ref, "unique_slug", None),
        category_id=getattr(competition_ref, "category_id", None),
        category_name=getattr(competition_ref, "category_name", None),
        number_of_teams=getattr(competition_ref, "number_of_teams", None),
        number_of_teams_source="db" if getattr(competition_ref, "number_of_teams", None) is not None else "missing",
        source_status="normalized",
    )

    return EventContext(
        event_id=getattr(event_obj, "id", 0),
        custom_id=getattr(event_obj, "custom_id", None),
        sport=getattr(event_obj, "sport", None) or "Unknown",
        season_id=getattr(event_obj, "season_id", None),
        season_name=event_obj.__dict__.get("season").name if event_obj.__dict__.get("season") else None,
        season_year=event_obj.__dict__.get("season").year if event_obj.__dict__.get("season") else None,
        start_time_utc=start_time_utc,
        minutes_until_start=minutes_until_start,
        discovery_source=getattr(event_obj, "discovery_source", None),
        home=home,
        away=away,
        competition=competition,
        participants_label=f"{home_name} vs {away_name}",
        context_status="normalized",
    )


@dataclass(frozen=True)
class NumberOfTeamsSummary:
    unique_team_count: int
    inferred_number_of_teams: Optional[int]


def summarize_number_of_teams_from_streak_analysis(streak_analysis) -> NumberOfTeamsSummary:
    sport = getattr(streak_analysis, "sport", None)
    unique_team_names = set()

    for results in (
        getattr(streak_analysis, "home_team_results", None) or [],
        getattr(streak_analysis, "away_team_results", None) or [],
    ):
        for result in results:
            for key in ("team_name", "opponent_name"):
                value = result.get(key)
                if value is None:
                    continue
                normalized = str(value).strip()
                if not normalized or normalized == "Unknown":
                    continue
                unique_team_names.add(normalized)

    unique_team_count = len(unique_team_names)
    if sport in {"Tennis", "Tennis Doubles"} or unique_team_count <= 1:
        inferred = None
    else:
        inferred = unique_team_count

    return NumberOfTeamsSummary(
        unique_team_count=unique_team_count,
        inferred_number_of_teams=inferred,
    )
