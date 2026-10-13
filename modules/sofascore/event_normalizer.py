"""Normalize SofaScore event payloads into internal event records."""

from __future__ import annotations

import logging
from typing import Dict

from infrastructure.persistence.repositories import SeasonRepository
from .event_metadata import (
    UNRESOLVED_ROUND,
    build_competition_ref,
    build_competition_text,
    classify_event_round,
    normalize_season_name,
)
from .sport_classifier import sport_classifier

logger = logging.getLogger(__name__)


def get_gender(home_team: Dict, away_team: Dict) -> str:
    gender_home_team = home_team.get("gender", "unknown")
    gender_away_team = away_team.get("gender", "unknown")

    if gender_home_team == gender_away_team and gender_home_team != "unknown":
        return gender_home_team

    if gender_home_team == "unknown" and gender_away_team != "unknown":
        return gender_away_team
    if gender_away_team == "unknown" and gender_home_team != "unknown":
        return gender_home_team

    if gender_home_team == "unknown" and gender_away_team == "unknown":
        return "unknown"

    logger.info("Gender mismatch detected: %s != %s", gender_home_team, gender_away_team)
    return "mixed"


def normalize_event_payload(
    event: Dict, discovery_source: str = "dropping_odds", *, resolved_round=UNRESOLVED_ROUND,
) -> Dict:
    """Build a full event, reusing the round resolved by discovery admission."""
    try:
        event = event or {}
        event = event.get("event", event)
        tournament = event.get("tournament") or {}
        unique_tournament = tournament.get("uniqueTournament") or {}
        category = tournament.get("category") or {}
        home_team = event.get("homeTeam") or {}
        away_team = event.get("awayTeam") or {}

        original_sport = (category.get("sport") or {}).get("name")
        home_name = home_team.get("name")
        away_name = away_team.get("name")
        gender = get_gender(home_team, away_team)

        classified_sport = sport_classifier.classify_sport(
            sport=original_sport,
            home_team=home_name,
            away_team=away_name,
        )

        season_data = event.get("season") or {}
        season_id = season_data.get("id")
        unique_tournament_name = unique_tournament.get("name")

        season_name = season_data.get("name")
        if season_name or unique_tournament_name:
            season_name = normalize_season_name(season_name, unique_tournament_name)

        season_year_raw = season_data.get("year")
        season_year = SeasonRepository._parse_year(season_year_raw) if season_year_raw is not None else None

        competition = build_competition_text(event)
        competition_ref = build_competition_ref(event)
        round_name = (
            classify_event_round(event) if resolved_round is UNRESOLVED_ROUND else resolved_round
        )
        country_name = (
            ((event.get("venue") or {}).get("country") or {}).get("name")
            or (category.get("country") or {}).get("name")
            or category.get("name")
        )

        if not event.get("id"):
            logger.warning("SofaScore event payload is missing id")
        if event.get("startTimestamp") is None:
            logger.warning("SofaScore event %s is missing startTimestamp", event.get("id"))
        if home_team.get("id") is None:
            logger.warning("SofaScore event %s is missing home participant id", event.get("id"))
        if away_team.get("id") is None:
            logger.warning("SofaScore event %s is missing away participant id", event.get("id"))
        if tournament.get("id") is None:
            logger.warning("SofaScore event %s is missing tournament id", event.get("id"))

        event_data = {
            "event": {
                "id": event.get("id"),
                "customId": event.get("customId"),
                "slug": event.get("slug"),
                "startTimestamp": event.get("startTimestamp"),
                "sport": classified_sport,
                "competition": competition,
                "country": country_name,
                "homeTeam": home_name,
                "awayTeam": away_name,
                "gender": gender,
                "discovery_source": discovery_source,
                "season_id": season_id,
                "season_name": season_name,
                "season_year": season_year,
                "round": round_name,
            },
            "home_participant": {
                "source": "sofascore",
                "source_participant_id": home_team.get("id"),
                "name": home_team.get("name"),
                "slug": home_team.get("slug"),
                "short_name": home_team.get("shortName"),
                "code_name": home_team.get("nameCode"),
            },
            "away_participant": {
                "source": "sofascore",
                "source_participant_id": away_team.get("id"),
                "name": away_team.get("name"),
                "slug": away_team.get("slug"),
                "short_name": away_team.get("shortName"),
                "code_name": away_team.get("nameCode"),
            },
            "competition_ref": competition_ref,
        }

        if classified_sport != original_sport:
            logger.info(
                "Sport classified from %s to %s for %s vs %s",
                original_sport,
                classified_sport,
                home_name,
                away_name,
            )

        return event_data
    except Exception as exc:
        logger.error("Error normalizing event payload: %s", exc)
        return {}
