"""Normalize Oddspapi fixture payloads into internal matching data."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
from typing import Any

from shared.timezone_utils import convert_utc_to_local

logger = logging.getLogger(__name__)


ODDSPAPI_SPORT_NAME_TO_INTERNAL = {
    "soccer": "Football",
    "football": "Football",
    "basketball": "Basketball",
    "baseball": "Baseball",
    "ice hockey": "Ice hockey",
    "hockey": "Ice hockey",
    "tennis": "Tennis",
    "volleyball": "Volleyball",
}


def _normalize_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _parse_start_time(value: Any) -> tuple[datetime | None, datetime | None]:
    text = _normalize_optional_text(value)
    if text is None:
        return None, None

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None, None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    utc_time = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    local_time = convert_utc_to_local(utc_time, keep_tzinfo=False)
    return utc_time, local_time


def _normalize_sport_name(value: Any) -> str | None:
    sport_name = _normalize_optional_text(value)
    if sport_name is None:
        return None
    return ODDSPAPI_SPORT_NAME_TO_INTERNAL.get(sport_name.casefold(), sport_name)


@dataclass
class OddspapiFixtureIdentity:
    fixture_id: str
    sport_id: str | None = None
    sport_name: str | None = None
    normalized_sport: str | None = None
    tournament_id: str | None = None
    tournament_name: str | None = None
    tournament_slug: str | None = None
    category_name: str | None = None
    category_slug: str | None = None
    season_id: str | None = None
    start_time_utc: datetime | None = None
    start_time_local: datetime | None = None
    participant1_id: str | None = None
    participant1_name: str | None = None
    participant1_short_name: str | None = None
    participant1_abbr: str | None = None
    participant2_id: str | None = None
    participant2_name: str | None = None
    participant2_short_name: str | None = None
    participant2_abbr: str | None = None
    external_providers: dict[str, Any] = field(default_factory=dict)
    raw_payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "OddspapiFixtureIdentity":
        data = payload if isinstance(payload, dict) else {}

        fixture_id = _normalize_optional_text(data.get("fixtureId"))
        if fixture_id is None:
            raise ValueError("missing_oddspapi_fixture_id")

        external_providers = data.get("externalProviders")
        if not isinstance(external_providers, dict):
            external_providers = {}

        start_time_utc, start_time_local = _parse_start_time(data.get("startTime"))
        external_provider_keys = sorted(
            key for key in external_providers.keys() if str(key or "").strip()
        )

        logger.info(
            "Normalized OddsPapi fixture %s sport=%s start_utc=%s start_local=%s providers=%s participants=%s vs %s tournament=%s / %s",
            fixture_id,
            _normalize_sport_name(data.get("sportName")),
            start_time_utc,
            start_time_local,
            external_provider_keys,
            _normalize_optional_text(data.get("participant1Name")),
            _normalize_optional_text(data.get("participant2Name")),
            _normalize_optional_text(data.get("tournamentName")),
            _normalize_optional_text(data.get("categoryName")),
        )

        return cls(
            fixture_id=fixture_id,
            sport_id=_normalize_optional_text(data.get("sportId")),
            sport_name=_normalize_optional_text(data.get("sportName")),
            normalized_sport=_normalize_sport_name(data.get("sportName")),
            tournament_id=_normalize_optional_text(data.get("tournamentId")),
            tournament_name=_normalize_optional_text(data.get("tournamentName")),
            tournament_slug=_normalize_optional_text(data.get("tournamentSlug")),
            category_name=_normalize_optional_text(data.get("categoryName")),
            category_slug=_normalize_optional_text(data.get("categorySlug")),
            season_id=_normalize_optional_text(data.get("seasonId")),
            start_time_utc=start_time_utc,
            start_time_local=start_time_local,
            participant1_id=_normalize_optional_text(data.get("participant1Id")),
            participant1_name=_normalize_optional_text(data.get("participant1Name")),
            participant1_short_name=_normalize_optional_text(data.get("participant1ShortName")),
            participant1_abbr=_normalize_optional_text(data.get("participant1Abbr")),
            participant2_id=_normalize_optional_text(data.get("participant2Id")),
            participant2_name=_normalize_optional_text(data.get("participant2Name")),
            participant2_short_name=_normalize_optional_text(data.get("participant2ShortName")),
            participant2_abbr=_normalize_optional_text(data.get("participant2Abbr")),
            external_providers=external_providers,
            raw_payload=data,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "fixture_id": self.fixture_id,
            "sport_id": self.sport_id,
            "sport_name": self.sport_name,
            "normalized_sport": self.normalized_sport,
            "tournament_id": self.tournament_id,
            "tournament_name": self.tournament_name,
            "tournament_slug": self.tournament_slug,
            "category_name": self.category_name,
            "category_slug": self.category_slug,
            "season_id": self.season_id,
            "start_time_utc": self.start_time_utc.isoformat() if self.start_time_utc else None,
            "start_time_local": self.start_time_local.isoformat() if self.start_time_local else None,
            "participant1_id": self.participant1_id,
            "participant1_name": self.participant1_name,
            "participant1_short_name": self.participant1_short_name,
            "participant1_abbr": self.participant1_abbr,
            "participant2_id": self.participant2_id,
            "participant2_name": self.participant2_name,
            "participant2_short_name": self.participant2_short_name,
            "participant2_abbr": self.participant2_abbr,
            "external_providers": dict(self.external_providers),
        }
