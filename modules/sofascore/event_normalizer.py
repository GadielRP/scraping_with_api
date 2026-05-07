"""Normalize SofaScore event payloads into internal event records."""

from __future__ import annotations

import logging
import re
from typing import Dict

from infrastructure.persistence.repositories import SeasonRepository
from .sport_classifier import sport_classifier

logger = logging.getLogger(__name__)


def clean_competition(competition: str) -> str:
    """Clean up competition string by removing duplicate parts and reordering."""
    if not competition:
        return competition

    # Split by comma
    parts = [part.strip() for part in competition.split(",")]

    # Build sets to identify unique parts
    seen = set()
    unique_parts = []

    for part in parts:
        key = part.lower()
        if key and key not in seen:
            seen.add(key)
            unique_parts.append(part)

    return ", ".join(unique_parts)


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


def _contains_any(text: str, terms: list[str]) -> bool:
    return any(term in text for term in terms)


def _is_knockout_round(text: str) -> bool:
    return _contains_any(
        text,
        [
            "quarterfinal",
            "semifinal",
            "semifinals",
            "final",
            "finals",
            "playoff",
            "playoffs",
            "knockout",
            "knockouts",
            "round-of-16",
            "round-of-32",
            "round-of-64",
            "round-of-128",
            "round-1",
            "round-2",
            "round-3",
            "round-4",
            "round-5",
            "round-6",
            "first-round",
            "second-round",
            "third-round",
            "western-conference",
            "eastern-conference",
            "play-in",
            "winnerloser",
            "match-for-3rd-place",
            "in-season-tournament",
        ],
    )


def _derive_round(competition: str, season_name: str | None, round_info: Dict) -> str:
    competition_lower = (competition or "").lower()

    if _contains_any(competition_lower, ["releg", "relegation", "relegations", "descenso"]):
        return "relegation"
    if _contains_any(competition_lower, ["qualification", "qualifier", "qualif.", "qual."]):
        return "qualification"
    if _contains_any(competition_lower, ["friendly", "amistoso"]):
        return "friendly"
    if _contains_any(
        competition_lower,
        ["pre season", "preseason", "pre-temporada", "pré-temporada", "pretemporada"],
    ):
        return "preseason"

    slug = (round_info or {}).get("slug")
    if slug:
        slug_lower = slug.lower()
        if _is_knockout_round(slug_lower):
            return "knockouts/playoffs"

        season_text = (season_name or "").lower()
        if _contains_any(season_text, ["cup", "copa", "taça", "coupe"]) or _contains_any(
            competition_lower, ["cup", "copa", "taça", "coupe"]
        ):
            return "regular_season"
        return slug

    if _is_knockout_round(competition_lower) or re.search(r"\bfinal(?: round)?\b", competition_lower):
        return "knockouts/playoffs"

    return "regular_season"


def _build_competition_text(event: Dict) -> str:
    tournament = event.get("tournament") or {}
    return clean_competition(
        f"{(tournament.get('category') or {}).get('name')}, {tournament.get('name')}, {(tournament.get('uniqueTournament') or {}).get('name')}"
    )


def get_event_information(event: Dict, discovery_source: str = "dropping_odds") -> Dict:
    try:
        event = event or {}
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
            season_name = SeasonRepository._parse_season_name(season_name, unique_tournament_name)

        season_year_raw = season_data.get("year")
        season_year = SeasonRepository._parse_year(season_year_raw) if season_year_raw is not None else None

        competition = _build_competition_text(event)
        round_info = event.get("roundInfo") or {}
        round_name = _derive_round(competition, season_name, round_info)
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
            },
            "away_participant": {
                "source": "sofascore",
                "source_participant_id": away_team.get("id"),
                "name": away_team.get("name"),
                "slug": away_team.get("slug"),
                "short_name": away_team.get("shortName"),
            },
            "competition_ref": {
                "source": "sofascore",
                "source_tournament_id": tournament.get("id"),
                "source_unique_tournament_id": unique_tournament.get("id"),
                "canonical_name": unique_tournament.get("name") or tournament.get("name"),
                "display_name": tournament.get("name") or unique_tournament.get("name"),
                "slug": tournament.get("slug"),
                "unique_slug": unique_tournament.get("slug"),
                "category_id": category.get("id"),
                "category_name": category.get("name"),
            },
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
        logger.error("Error extracting event information: %s", exc)
        return {}
