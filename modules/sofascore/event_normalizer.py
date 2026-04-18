"""Normalize SofaScore event payloads into internal event records."""

from __future__ import annotations

import logging
import re
from typing import Dict

from infrastructure.persistence.repositories import SeasonRepository
from .sport_classifier import sport_classifier

logger = logging.getLogger(__name__)


def clean_competition(competition: str) -> str:
    if not competition:
        return competition

    parts = [part.strip() for part in competition.split(",")]
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
    tournament = event.get("tournament", {})
    return clean_competition(
        f"{tournament.get('category', {}).get('name')}, {tournament.get('name')}, {tournament.get('uniqueTournament', {}).get('name')}"
    )


def get_event_information(event: Dict, discovery_source: str = "dropping_odds") -> Dict:
    try:
        original_sport = event.get("tournament", {}).get("category", {}).get("sport", {}).get("name")
        home_team = event.get("homeTeam", {}).get("name")
        away_team = event.get("awayTeam", {}).get("name")
        gender = get_gender(event.get("homeTeam", {}), event.get("awayTeam", {}))

        classified_sport = sport_classifier.classify_sport(
            sport=original_sport,
            home_team=home_team,
            away_team=away_team,
        )

        season_data = event.get("season", {})
        season_id = season_data.get("id")
        unique_tournament_name = event.get("tournament", {}).get("uniqueTournament", {}).get("name")

        season_name = season_data.get("name")
        if season_name or unique_tournament_name:
            season_name = SeasonRepository._parse_season_name(season_name, unique_tournament_name)

        season_year_raw = season_data.get("year")
        season_year = SeasonRepository._parse_year(season_year_raw) if season_year_raw is not None else None

        competition = _build_competition_text(event)
        round_info = event.get("roundInfo", {})
        round_name = _derive_round(competition, season_name, round_info)

        event_data = {
            "id": event.get("id"),
            "customId": event.get("customId"),
            "slug": event.get("slug"),
            "startTimestamp": event.get("startTimestamp"),
            "sport": classified_sport,
            "competition": competition,
            "country": event.get("venue", {}).get("country", {}).get("name")
            or event.get("tournament", {}).get("category", {}).get("country", {}).get("name"),
            "homeTeam": home_team,
            "awayTeam": away_team,
            "gender": gender,
            "discovery_source": discovery_source,
            "season_id": season_id,
            "season_name": season_name,
            "season_year": season_year,
            "round": round_name,
        }

        if classified_sport != original_sport:
            logger.info(
                "Sport classified from %s to %s for %s vs %s",
                original_sport,
                classified_sport,
                home_team,
                away_team,
            )

        return event_data
    except Exception as exc:
        logger.error("Error extracting event information: %s", exc)
        return {}
