from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

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
    code_name: Optional[str] = None
    snapshot_ranking: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


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
    total_regular_season_games: Optional[int]
    standings_grouping: Optional[str]
    league_config_source: Optional[str]
    has_standings_source_endpoint: Optional[bool]
    source_status: str
    standings_response: Optional[list] = field(default=None, repr=False)
    source_tournament_name: Optional[str] = None
    source_unique_tournament_name: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


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
    slug: Optional[str] = None
    gender: Optional[str] = None
    country: Optional[str] = None
    round: Optional[str] = None
    observations: list[dict] = field(default_factory=list)
    odds_response: Optional[dict] = None
    odds_trajectory: list[dict] = field(default_factory=list)
    odds_trajectory_context: Optional[Any] = None
    ft_1x2_odds_trajectory_context: Optional[Any] = None
    streak_analysis: Optional[Any] = None
    should_send_streak_alert: bool = False
    dual_report: Optional[Any] = None
    competition_metadata_resolved: bool = False
    success: bool = True
    alert_sent: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


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
    observations: Optional[list[dict]] = None,
    odds_response: Optional[dict] = None,
    odds_trajectory: Optional[list[dict]] = None,
    success: bool = True,
) -> Optional[EventContext]:
    """Build EventContext, preferring normalized relations with temporary legacy fallback."""
    raw_metadata = metadata_snapshot or {}

    missing: list[str] = []
    legacy_fallback_used = False
    home_participant = event_obj.__dict__.get("home_participant")
    away_participant = event_obj.__dict__.get("away_participant")
    competition_ref = event_obj.__dict__.get("competition_ref")
    season = event_obj.__dict__.get("season")
    legacy_home_name = _clean_text(getattr(event_obj, "home_team", None))
    legacy_away_name = _clean_text(getattr(event_obj, "away_team", None))
    legacy_competition_name = _clean_text(getattr(event_obj, "competition", None))
    snapshot_home_team_id = raw_metadata.get("home_team_id")
    snapshot_away_team_id = raw_metadata.get("away_team_id")
    snapshot_competition_slug = _clean_text(raw_metadata.get("competition_slug"))
    snapshot_unique_tournament_id = raw_metadata.get("unique_tournament_id")

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

    home_source_participant_id = getattr(home_participant, "source_participant_id", None)
    if home_source_participant_id is None:
        home_source_participant_id = snapshot_home_team_id
        if home_source_participant_id is not None:
            legacy_fallback_used = True

    away_source_participant_id = getattr(away_participant, "source_participant_id", None)
    if away_source_participant_id is None:
        away_source_participant_id = snapshot_away_team_id
        if away_source_participant_id is not None:
            legacy_fallback_used = True

    competition_source_unique_tournament_id = getattr(competition_ref, "source_unique_tournament_id", None)
    if competition_source_unique_tournament_id is None:
        competition_source_unique_tournament_id = snapshot_unique_tournament_id
        if competition_source_unique_tournament_id is not None:
            legacy_fallback_used = True

    competition_slug = _clean_text(getattr(competition_ref, "slug", None))
    unique_competition_slug = _clean_text(getattr(competition_ref, "unique_slug", None))
    if not competition_slug and snapshot_competition_slug:
        competition_slug = snapshot_competition_slug
        legacy_fallback_used = True
    if not unique_competition_slug and snapshot_competition_slug:
        unique_competition_slug = snapshot_competition_slug
        legacy_fallback_used = True

    home = ParticipantContext(
        participant_id=home_participant_id,
        source=getattr(home_participant, "source", None),
        source_participant_id=home_source_participant_id,
        name=home_name,
        slug=getattr(home_participant, "slug", None),
        short_name=getattr(home_participant, "short_name", None),
        source_status="normalized" if home_participant is not None and home_name == _clean_text(getattr(home_participant, "name", None)) else "legacy_fallback",
        code_name=getattr(home_participant, "code_name", None),
        snapshot_ranking=raw_metadata.get("home_team_ranking"),
        created_at=getattr(home_participant, "created_at", None),
        updated_at=getattr(home_participant, "updated_at", None),
    )
    away = ParticipantContext(
        participant_id=away_participant_id,
        source=getattr(away_participant, "source", None),
        source_participant_id=away_source_participant_id,
        name=away_name,
        slug=getattr(away_participant, "slug", None),
        short_name=getattr(away_participant, "short_name", None),
        source_status="normalized" if away_participant is not None and away_name == _clean_text(getattr(away_participant, "name", None)) else "legacy_fallback",
        code_name=getattr(away_participant, "code_name", None),
        snapshot_ranking=raw_metadata.get("away_team_ranking"),
        created_at=getattr(away_participant, "created_at", None),
        updated_at=getattr(away_participant, "updated_at", None),
    )
    competition = CompetitionContext(
        competition_id=competition_id,
        source=getattr(competition_ref, "source", None),
        source_tournament_id=getattr(competition_ref, "source_tournament_id", None),
        source_unique_tournament_id=competition_source_unique_tournament_id,
        canonical_name=getattr(competition_ref, "canonical_name", None),
        display_name=competition_display_name,
        slug=competition_slug,
        unique_slug=unique_competition_slug,
        category_id=getattr(competition_ref, "category_id", None),
        category_name=getattr(competition_ref, "category_name", None),
        number_of_teams=getattr(competition_ref, "number_of_teams", None),
        number_of_teams_source=getattr(competition_ref, "league_config_source", None)
        or ("db_cache" if getattr(competition_ref, "number_of_teams", None) is not None else "missing"),
        total_regular_season_games=getattr(competition_ref, "total_regular_season_games", None),
        standings_grouping=getattr(competition_ref, "standings_grouping", None),
        league_config_source=getattr(competition_ref, "league_config_source", None) or "missing",
        has_standings_source_endpoint=getattr(competition_ref, "has_standings_source_endpoint", None),
        standings_response=getattr(competition_ref, "standings_response", None),
        source_status="normalized"
        if competition_ref is not None and competition_display_name == _clean_text(getattr(competition_ref, "display_name", None))
        else "legacy_fallback",
        source_tournament_name=raw_metadata.get("tournament_name"),
        source_unique_tournament_name=raw_metadata.get("unique_tournament_name"),
        created_at=getattr(competition_ref, "created_at", None),
        updated_at=getattr(competition_ref, "updated_at", None),
    )

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
        slug=getattr(event_obj, "slug", None),
        sport=getattr(event_obj, "sport", None) or "Unknown",
        gender=getattr(event_obj, "gender", None),
        country=getattr(event_obj, "country", None),
        round=getattr(event_obj, "round", None),
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
        observations=observations or [],
        odds_response=odds_response,
        odds_trajectory=odds_trajectory or [],
        success=success,
        alert_sent=bool(getattr(event_obj, "alert_sent", False)),
        created_at=getattr(event_obj, "created_at", None),
        updated_at=getattr(event_obj, "updated_at", None),
    )


@dataclass(frozen=True)
class NumberOfTeamsSummary:
    unique_team_count: int
    inferred_number_of_teams: Optional[int]


def summarize_number_of_teams_from_streak_analysis(
    streak_analysis,
    event_context: Optional[EventContext] = None,
) -> NumberOfTeamsSummary:
    sport = getattr(event_context, "sport", None)
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
