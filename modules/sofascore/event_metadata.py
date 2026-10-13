"""Pure SofaScore competition metadata and round classification; no persistence."""

from __future__ import annotations

from collections.abc import Collection
from typing import Dict

from modules.events.round_policy import resolve_event_round

# None is a resolved unknown round; this sentinel means classification is needed.
UNRESOLVED_ROUND = object()


def clean_competition(competition: str) -> str:
    """Remove duplicate comma-separated names while preserving their order."""
    if not competition:
        return competition

    seen = set()
    unique_parts = []

    for part in competition.split(","):
        part = part.strip()
        key = part.lower()
        if key and key not in seen:
            seen.add(key)
            unique_parts.append(part)

    return ", ".join(unique_parts)


def normalize_season_name(season_name: str | None, tournament_name: str | None) -> str | None:
    """Prefix numeric season names with their provider tournament name."""
    if season_name and not any(character.isalpha() for character in season_name):
        return f"{tournament_name} {season_name}"
    return season_name


def _contains_any(text: str, terms: Collection[str]) -> bool:
    return any(term in text for term in terms)


def _is_knockout_round(text: str) -> bool:
    return _contains_any(
        text,
        (
            "super-bowl",
            "final",
            "playoff",
            "knockout",
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
            "match-for-",
            "placement",
            "round-of-256",
            "in-season-tournament",
            "wild-card",
            "conference-championships",
            "divisional-round",
        ),
    )


def _is_relegation_or_promotion_round(text: str) -> bool:
    return _contains_any(
        text,
        (
            "releg",
            "descenso",
            "promotion",
        ),
    )


def _is_qualification_round(text: str) -> bool:
    return _contains_any(
        text,
        (
            "qualification",
            "qualifier",
            "qualif.",
            "qual.",
            "preliminary",
        ),
    )


def _is_preseason_round(text: str) -> bool:
    return _contains_any(
        text,
        (
            "preseason",
        ),
    )


def _derive_round(
    season_name: str | None,
    round_info: Dict,
    competition_ref: Dict,
) -> str:
    competition_lower = " ".join(
        str(value)
        for value in (
            competition_ref.get("canonical_name"),
            competition_ref.get("display_name"),
            competition_ref.get("slug"),
            competition_ref.get("unique_slug"),
        )
        if value
    ).lower()

    if _is_relegation_or_promotion_round(competition_lower):
        return "relegation/promotion"
    if _is_qualification_round(competition_lower):
        return "qualification"
    if _contains_any(competition_lower, ["friendly", "amistoso"]):
        return "friendly"
    if _contains_any(
        competition_lower,
        ["pre season", "preseason", "pre-temporada", "pré-temporada", "pretemporada"],
    ):
        return "pre_season"

    slug = (round_info or {}).get("slug")
    if slug:
        slug_lower = slug.lower()
        if _is_knockout_round(slug_lower):
            return "knockouts/playoffs"
        if _is_relegation_or_promotion_round(slug_lower):
            return "relegation/promotion"
        if _is_qualification_round(slug_lower):
            return "qualification"
        if _is_preseason_round(slug_lower):
            return "pre_season"

        season_text = (season_name or "").lower()
        if _contains_any(season_text, ["cup", "copa", "taça", "coupe"]) or _contains_any(
            competition_lower, ["cup", "copa", "taça", "coupe"]
        ):
            return "regular_season"
        return slug

    if _is_knockout_round(competition_lower):
        return "knockouts/playoffs"

    return "regular_season"


def build_competition_text(event: Dict) -> str:
    tournament = event.get("tournament") or {}
    return clean_competition(
        f"{(tournament.get('category') or {}).get('name')}, {tournament.get('name')}, {(tournament.get('uniqueTournament') or {}).get('name')}"
    )


def build_competition_ref(event: Dict) -> Dict:
    tournament = event.get("tournament") or {}
    unique_tournament = tournament.get("uniqueTournament") or {}
    category = tournament.get("category") or {}
    return {
        "source": "sofascore",
        "source_tournament_id": tournament.get("id"),
        "source_unique_tournament_id": unique_tournament.get("id"),
        "canonical_name": unique_tournament.get("name"),
        "display_name": tournament.get("name"),
        "slug": tournament.get("slug"),
        "unique_slug": unique_tournament.get("slug"),
        "category_id": category.get("id"),
        "category_name": category.get("name"),
    }


def classify_event_round(event: Dict) -> str:
    """Classify a raw provider round from tournament, season and roundInfo metadata."""
    payload = event.get("event", event)
    tournament = payload.get("tournament") or {}
    unique_tournament = tournament.get("uniqueTournament") or {}
    season = payload.get("season") or {}
    season_name = normalize_season_name(
        season.get("name"), unique_tournament.get("name"),
    )
    competition = build_competition_text(payload)
    competition_ref = build_competition_ref(payload)
    round_name = _derive_round(season_name, payload.get("roundInfo") or {}, competition_ref)
    return resolve_event_round(season.get("id"), competition, round_name, None)
