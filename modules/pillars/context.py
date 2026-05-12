from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ParticipantContext:
    participant_id: Optional[int]
    source: Optional[str]
    source_participant_id: Optional[int]
    name: str
    slug: Optional[str]
    short_name: Optional[str]
    source_status: str


@dataclass
class CompetitionContext:
    competition_id: Optional[int]
    source: Optional[str]
    source_tournament_id: Optional[int]
    source_unique_tournament_id: Optional[int]
    canonical_name: Optional[str]
    display_name: str
    slug: Optional[str]
    unique_slug: Optional[str]
    category_id: Optional[int]
    category_name: Optional[str]
    number_of_teams: Optional[int]
    number_of_teams_source: Optional[str]
    source_status: str


@dataclass
class EventContext:
    event_id: int
    custom_id: Optional[str]
    sport: str
    season_id: Optional[int]
    season_name: Optional[str]
    season_year: Optional[int]
    start_time_utc: datetime
    minutes_until_start: Optional[int]
    discovery_source: Optional[str]
    home: ParticipantContext
    away: ParticipantContext
    competition: CompetitionContext
    participants_label: str
    context_status: str


def _missing_context_message(event_obj, missing: list[str]) -> str:
    event_id = getattr(event_obj, "id", "?")
    return f"event_id={event_id} missing_normalized_context_fields={','.join(missing)}"


def _clean_text(value) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def build_event_context(
    event_obj,
    minutes_until_start: Optional[int] = None,
    metadata_snapshot: Optional[dict] = None,
) -> Optional[EventContext]:
    """Build EventContext, preferring normalized relations with temporary legacy fallback."""
    del metadata_snapshot

    missing: list[str] = []
    legacy_fallback_used = False
    home_participant = event_obj.__dict__.get("home_participant")
    away_participant = event_obj.__dict__.get("away_participant")
    competition_ref = event_obj.__dict__.get("competition_ref")
    legacy_home_name = _clean_text(getattr(event_obj, "home_team", None))
    legacy_away_name = _clean_text(getattr(event_obj, "away_team", None))
    legacy_competition_name = _clean_text(getattr(event_obj, "competition", None))

    if home_participant is None and legacy_home_name is None:
        missing.append("home_participant")
    if away_participant is None and legacy_away_name is None:
        missing.append("away_participant")
    if competition_ref is None and legacy_competition_name is None:
        missing.append("competition_ref")

    if missing:
        logger.warning("Normalized EventContext unavailable: %s", _missing_context_message(event_obj, missing))
        return None

    home_name = _clean_text(getattr(home_participant, "name", None))
    if home_name is None and legacy_home_name is not None:
        home_name = legacy_home_name
        legacy_fallback_used = True

    away_name = _clean_text(getattr(away_participant, "name", None))
    if away_name is None and legacy_away_name is not None:
        away_name = legacy_away_name
        legacy_fallback_used = True

    competition_display_name = _clean_text(getattr(competition_ref, "display_name", None))
    if competition_display_name is None and legacy_competition_name is not None:
        competition_display_name = legacy_competition_name
        legacy_fallback_used = True

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

    home_participant_id = getattr(home_participant, "participant_id", None)
    if home_participant_id is None:
        home_participant_id = getattr(event_obj, "home_participant_id", None)
        if legacy_home_name is not None:
            legacy_fallback_used = True

    away_participant_id = getattr(away_participant, "participant_id", None)
    if away_participant_id is None:
        away_participant_id = getattr(event_obj, "away_participant_id", None)
        if legacy_away_name is not None:
            legacy_fallback_used = True

    competition_id = getattr(competition_ref, "competition_id", None)
    if competition_id is None:
        competition_id = getattr(event_obj, "competition_id", None)
        if legacy_competition_name is not None:
            legacy_fallback_used = True

    home = ParticipantContext(
        participant_id=home_participant_id,
        source=getattr(home_participant, "source", None),
        source_participant_id=getattr(home_participant, "source_participant_id", None),
        name=home_name,
        slug=getattr(home_participant, "slug", None),
        short_name=getattr(home_participant, "short_name", None),
        source_status="normalized" if home_participant is not None and home_name == _clean_text(getattr(home_participant, "name", None)) else "legacy_fallback",
    )
    away = ParticipantContext(
        participant_id=away_participant_id,
        source=getattr(away_participant, "source", None),
        source_participant_id=getattr(away_participant, "source_participant_id", None),
        name=away_name,
        slug=getattr(away_participant, "slug", None),
        short_name=getattr(away_participant, "short_name", None),
        source_status="normalized" if away_participant is not None and away_name == _clean_text(getattr(away_participant, "name", None)) else "legacy_fallback",
    )
    competition = CompetitionContext(
        competition_id=competition_id,
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
        source_status="normalized"
        if competition_ref is not None and competition_display_name == _clean_text(getattr(competition_ref, "display_name", None))
        else "legacy_fallback",
    )

    season = event_obj.__dict__.get("season")
    context_status = "normalized"
    if legacy_fallback_used or any(
        value == "legacy_fallback" for value in (home.source_status, away.source_status, competition.source_status)
    ):
        normalized_count = sum(
            1
            for value in (home.source_status, away.source_status, competition.source_status)
            if value == "normalized"
        )
        context_status = "mixed" if normalized_count > 0 else "legacy_compat"

    if context_status != "normalized":
        logger.info(
            "Legacy EventContext fallback used: event_id=%s context_status=%s participants=%s",
            getattr(event_obj, "id", "?"),
            context_status,
            f"{home_name} vs {away_name}",
        )

    return EventContext(
        event_id=getattr(event_obj, "id", 0),
        custom_id=getattr(event_obj, "custom_id", None),
        sport=getattr(event_obj, "sport", None) or "Unknown",
        season_id=getattr(event_obj, "season_id", None),
        season_name=season.name if season else None,
        season_year=season.year if season else None,
        start_time_utc=start_time_utc,
        minutes_until_start=minutes_until_start,
        discovery_source=getattr(event_obj, "discovery_source", None),
        home=home,
        away=away,
        competition=competition,
        participants_label=f"{home_name} vs {away_name}",
        context_status=context_status,
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
