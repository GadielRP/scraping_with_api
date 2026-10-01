"""Canonical sport identities with explicit provider request mappings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Collection


@dataclass(frozen=True, slots=True)
class Sport:
    id: str
    display_name: str
    aliases: tuple[str, ...]
    sofascore_slug: str | None
    oddspapi_slug: str | None
    oddspapi_id: int | None


_SPORTS = (
    Sport("football", "Football", ("soccer",), "football", "soccer", 10),
    Sport(
        "american_football",
        "American football",
        ("american football",),
        "american-football",
        "american-football",
        14,
    ),
    Sport("basketball", "Basketball", (), "basketball", "basketball", 11),
    Sport("volleyball", "Volleyball", (), "volleyball", "volleyball", 23),
    Sport("tennis", "Tennis", ("tennis singles",), "tennis", "tennis", 12),
    Sport("tennis_doubles", "Tennis doubles", ("tennis doubles",), "tennis", "tennis", 12),
    Sport("ice_hockey", "Ice hockey", ("hockey",), "ice-hockey", "ice-hockey", 15),
    Sport("handball", "Handball", (), "handball", "handball", 22),
    Sport("baseball", "Baseball", (), "baseball", "baseball", 13),
)
_SPORTS_BY_ID = {sport.id: sport for sport in _SPORTS}


def _key(value: object) -> str:
    if isinstance(value, dict):
        value = value.get("name") or value.get("slug")
    return " ".join(str(value or "").replace("-", " ").replace("_", " ").split()).casefold()


_ALIASES = {
    _key(alias): sport.id
    for sport in _SPORTS
    for alias in (sport.id, sport.display_name, *sport.aliases)
}


def canonical_sport_id(value: object) -> str | None:
    """Translate a display label or provider alias into a stable sport ID."""
    return _ALIASES.get(_key(value))


def configured_sport_ids(supported_sports: Collection[str] | None = None) -> frozenset[str]:
    """Resolve configured IDs, display names, and provider aliases to canonical IDs."""
    if supported_sports is None:
        from infrastructure.settings import Config

        supported_sports = Config.SUPPORTED_SPORTS or ()
    return _sport_ids(supported_sports)


def _sport_ids(values: Collection[str]) -> frozenset[str]:
    return frozenset(
        sport_id
        for value in values
        if (sport_id := canonical_sport_id(value)) is not None
    )


def _requested_sport_ids(requested_sports: Collection[str] | None) -> frozenset[str]:
    configured = configured_sport_ids()
    return configured if requested_sports is None else configured & _sport_ids(requested_sports)


def is_supported_sport_name(
    value: object,
    *,
    supported_sports: Collection[str] | None = None,
) -> bool:
    sport_id = canonical_sport_id(value)
    return sport_id is not None and sport_id in configured_sport_ids(supported_sports)


def is_supported_sofascore_event(
    event: dict | None,
    *,
    supported_sports: Collection[str] | None = None,
) -> bool:
    sport_id = sofascore_sport_id_for_event(event)
    return sport_id is not None and sport_id in configured_sport_ids(supported_sports)


def _participants_are_doubles(home: object, away: object) -> bool:
    def participant_name(value: object) -> str:
        return str(value.get("name") or "") if isinstance(value, dict) else str(value or "")

    return "/" in participant_name(home) and "/" in participant_name(away)


def sofascore_sport_id_for_event(event: dict | None) -> str | None:
    """Identify SofaScore's event sport, including its name-based tennis format."""
    if not isinstance(event, dict):
        return None
    payload = event.get("event", event)
    if not isinstance(payload, dict):
        return None
    sport = payload.get("sport")
    if sport is None:
        tournament = payload.get("tournament") or {}
        category = tournament.get("category") or {} if isinstance(tournament, dict) else {}
        sport = category.get("sport") if isinstance(category, dict) else None
    sport_id = canonical_sport_id(sport)
    if sport_id == "tennis" and _participants_are_doubles(
        payload.get("homeTeam"), payload.get("awayTeam")
    ):
        return "tennis_doubles"
    return sport_id


def oddspapi_sport_id_for_name(value: object) -> str | None:
    """Identify a sportName supplied by Oddspapi."""
    return canonical_sport_id(value)


def oddspapi_sport_id_for_fixture(fixture: dict | None) -> str | None:
    """Identify an Oddspapi fixture sport, inferring tennis doubles by participants."""
    if not isinstance(fixture, dict):
        return None
    sport_id = oddspapi_sport_id_for_name(fixture.get("sportName"))
    if sport_id is None:
        try:
            sport_id = _ODDSPAPI_IDS_TO_SPORT_IDS.get(int(fixture.get("sportId")))
        except (TypeError, ValueError):
            pass
    if sport_id == "tennis" and _participants_are_doubles(
        fixture.get("participant1Name"), fixture.get("participant2Name")
    ):
        return "tennis_doubles"
    return sport_id


def sofascore_sport_slugs(
    requested_sports: Collection[str] | None = None,
) -> list[str]:
    """Return unique routes in the intersection of requested/configured sports."""
    return list(dict.fromkeys(slug for _, slug in sofascore_sport_routes(requested_sports)))


def sofascore_sport_routes(
    requested_sports: Collection[str] | None = None,
) -> list[tuple[str, str]]:
    """Return canonical sport IDs paired with their configured SofaScore route."""
    enabled = _requested_sport_ids(requested_sports)
    return [
        (sport.id, sport.sofascore_slug)
        for sport in _SPORTS
        if sport.id in enabled and sport.sofascore_slug
    ]


def sofascore_sport_slug(value: object) -> str | None:
    sport_id = canonical_sport_id(value)
    return _SPORTS_BY_ID[sport_id].sofascore_slug if sport_id else None


def oddspapi_sport_ids(
    requested_sports: Collection[str] | None = None,
) -> dict[str, int]:
    """Return Oddspapi routes in the requested/configured sport intersection."""
    enabled = _requested_sport_ids(requested_sports)
    return {
        sport.oddspapi_slug: sport.oddspapi_id
        for sport in _SPORTS
        if sport.id in enabled
        and sport.oddspapi_slug
        and sport.oddspapi_id is not None
    }


def sport_display_name(value: object) -> str | None:
    sport_id = canonical_sport_id(value)
    return _SPORTS_BY_ID[sport_id].display_name if sport_id else None


_ODDSPAPI_IDS_TO_SPORT_IDS: dict[int, str] = {}
for _sport in _SPORTS:
    if _sport.oddspapi_id is not None:
        _ODDSPAPI_IDS_TO_SPORT_IDS.setdefault(_sport.oddspapi_id, _sport.id)
