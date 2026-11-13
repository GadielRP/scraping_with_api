from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from modules.competition.berth_allocation_engine.models import BerthAllocation
from modules.competition.competitive_rules import CompetitionRules

from shared.temporal import as_utc

logger = logging.getLogger(__name__)


@dataclass
class ParticipantContext:
    participant_id: Optional[int]
    source: Optional[str]
    source_participant_id: Optional[int]
    name: str
    slug: Optional[str]
    short_name: Optional[str]
    code_name: Optional[str] = None
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
    competition_config_source: Optional[str]
    has_standings_source_endpoint: Optional[bool]
    standings_response: Optional[list] = field(default=None, repr=False)
    source_tournament_name: Optional[str] = None
    source_unique_tournament_name: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    competitive_rules: CompetitionRules | None = None
    berth_allocation: BerthAllocation | None = None


@dataclass(frozen=True, slots=True)
class EventIdentity:
    """Lightweight, immutable projection of an event for calculation pillars and mining adapters."""
    event_id: int
    participants_label: str
    starts_at: datetime
    minutes_until_start: Optional[int]
    sport: str
    round: Optional[str] = None
    competition_id: Optional[int] = None
    competition_name: Optional[str] = None
    season_id: Optional[int] = None
    season_name: Optional[str] = None
    season_year: Optional[int] = None
    home_team_name: Optional[str] = None
    away_team_name: Optional[str] = None
    country: Optional[str] = None


@dataclass
class EventContext:
    event_id: int
    custom_id: Optional[str]
    sport: str
    season_id: Optional[int]
    season_name: Optional[str]
    season_year: Optional[int]
    starts_at: datetime
    minutes_until_start: Optional[int]
    discovery_source: Optional[str]
    home: ParticipantContext
    away: ParticipantContext
    competition: CompetitionContext
    participants_label: str
    slug: Optional[str] = None
    gender: Optional[str] = None
    country: Optional[str] = None
    round: Optional[str] = None
    observations: list[dict] = field(default_factory=list)
    odds_trajectory: list[dict] = field(default_factory=list)
    odds_trajectory_context: Optional[Any] = None
    streak_analysis: Optional[Any] = None
    should_send_streak_alert: bool = False
    dual_report: Optional[Any] = None
    success: bool = True
    alert_sent: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_identity(self) -> EventIdentity:
        return EventIdentity(
            event_id=self.event_id,
            participants_label=self.participants_label,
            starts_at=self.starts_at,
            minutes_until_start=self.minutes_until_start,
            sport=self.sport,
            round=self.round,
            competition_id=self.competition.competition_id,
            competition_name=self.competition.display_name or self.competition.canonical_name,
            season_id=self.season_id,
            season_name=self.season_name,
            season_year=self.season_year,
            home_team_name=self.home.name,
            away_team_name=self.away.name,
            country=self.country,
        )


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
    odds_trajectory: Optional[list[dict]] = None,
    success: bool = True,
) -> Optional[EventContext]:
    """Build the EventContext of an event whose normalized relations are loaded.

    Participants and competition come only from the loaded relations. When one is
    missing the context cannot be built and None is returned.
    """
    raw_metadata = metadata_snapshot or {}
    home_participant = event_obj.__dict__.get("home_participant")
    away_participant = event_obj.__dict__.get("away_participant")
    competition_ref = event_obj.__dict__.get("competition_ref")
    season = event_obj.__dict__.get("season")
    starts_at = getattr(event_obj, "starts_at", None)

    home_name = _clean_text(getattr(home_participant, "name", None))
    away_name = _clean_text(getattr(away_participant, "name", None))
    competition_display_name = _clean_text(getattr(competition_ref, "display_name", None))
    missing = [
        label for label, value in (
            ("home_participant", home_participant),
            ("home_participant.name", home_name),
            ("away_participant", away_participant),
            ("away_participant.name", away_name),
            ("competition_ref", competition_ref),
            ("competition_ref.display_name", competition_display_name),
            ("starts_at", starts_at),
        ) if not value
    ]
    if missing:
        logger.warning(
            "Normalized EventContext unavailable: event_id=%s missing_normalized_context_fields=%s",
            getattr(event_obj, "id", "?"), ",".join(missing),
        )
        return None

    home = ParticipantContext(
        participant_id=home_participant.participant_id,
        source=home_participant.source,
        source_participant_id=home_participant.source_participant_id,
        name=home_name,
        slug=home_participant.slug,
        short_name=home_participant.short_name,
        code_name=home_participant.code_name,
        created_at=home_participant.created_at,
        updated_at=home_participant.updated_at,
    )
    away = ParticipantContext(
        participant_id=away_participant.participant_id,
        source=away_participant.source,
        source_participant_id=away_participant.source_participant_id,
        name=away_name,
        slug=away_participant.slug,
        short_name=away_participant.short_name,
        code_name=away_participant.code_name,
        created_at=away_participant.created_at,
        updated_at=away_participant.updated_at,
    )
    competition = CompetitionContext(
        competition_id=competition_ref.competition_id,
        source=competition_ref.source,
        source_tournament_id=competition_ref.source_tournament_id,
        source_unique_tournament_id=competition_ref.source_unique_tournament_id,
        canonical_name=competition_ref.canonical_name,
        display_name=competition_display_name,
        slug=_clean_text(competition_ref.slug),
        unique_slug=_clean_text(competition_ref.unique_slug),
        category_id=competition_ref.category_id,
        category_name=competition_ref.category_name,
        number_of_teams=competition_ref.number_of_teams,
        number_of_teams_source=competition_ref.competition_config_source
        or ("db_cache" if competition_ref.number_of_teams is not None else "missing"),
        total_regular_season_games=competition_ref.total_regular_season_games,
        standings_grouping=competition_ref.standings_grouping,
        competition_config_source=competition_ref.competition_config_source or "missing",
        has_standings_source_endpoint=competition_ref.has_standings_source_endpoint,
        source_tournament_name=raw_metadata.get("tournament_name"),
        source_unique_tournament_name=raw_metadata.get("unique_tournament_name"),
        created_at=competition_ref.created_at,
        updated_at=competition_ref.updated_at,
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
        starts_at=as_utc(starts_at, field_name="event_context.starts_at"),
        minutes_until_start=minutes_until_start,
        discovery_source=getattr(event_obj, "discovery_source", None),
        home=home,
        away=away,
        competition=competition,
        participants_label=f"{home_name} vs {away_name}",
        observations=observations or [],
        odds_trajectory=odds_trajectory or [],
        success=success,
        alert_sent=bool(getattr(event_obj, "alert_sent", False)),
        created_at=getattr(event_obj, "created_at", None),
        updated_at=getattr(event_obj, "updated_at", None),
    )
