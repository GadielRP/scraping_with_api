"""Persist resolved Oddspapi fixtures without mixing storage into matching logic."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import re
import unicodedata

from sqlalchemy.orm import Session

from infrastructure.persistence.repositories.event_source_mapping_repository import (
    EventSourceMappingRepository,
)
from infrastructure.persistence.repositories.participant_repository import (
    ParticipantKey,
    ParticipantRepository,
)

from .fixture_normalizer import OddspapiFixtureIdentity
from .format_utils import normalize_source_id

logger = logging.getLogger(__name__)

ODDSPAPI_SOURCE = "oddspapi"
SECONDARY_PROVIDER_SOURCES = {
    "pinnacleId": "pinnacle",
    "betradarId": "betradar",
    "flashscoreId": "flashscore",
    "opticoddsId": "opticodds",
    "lsportsId": "lsports",
    "mollybetId": "mollybet",
    "txoddsId": "txodds",
    "betgeniusId": "betgenius",
    "oddinId": "oddin",
}


@dataclass(frozen=True)
class ResolvedFixtureWrite:
    """Persistence data produced after the resolver chooses a canonical event."""

    fixture: OddspapiFixtureIdentity
    canonical_event_id: int
    match_method: str | None
    confidence: float | None
    include_secondary_mappings: bool = False


def participant_slug(name: str | None) -> str | None:
    normalized_name = str(name or "").strip()
    if not normalized_name:
        return None
    ascii_name = (
        unicodedata.normalize("NFKD", normalized_name)
        .encode("ascii", "ignore")
        .decode("ascii")
        .casefold()
    )
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name).strip("-")
    return slug or None


def _participant_data(
    *,
    fixture_id: str,
    source_participant_id: str | None,
    name: str | None,
    short_name: str | None,
    abbreviation: str | None,
) -> tuple[ParticipantKey, dict] | None:
    if source_participant_id is None:
        return None
    try:
        normalized_participant_id = int(source_participant_id)
    except (TypeError, ValueError):
        logger.warning(
            "Skipping non-numeric Oddspapi participant id=%s for fixture %s",
            source_participant_id,
            fixture_id,
        )
        return None

    key = (ODDSPAPI_SOURCE, normalized_participant_id)
    return key, {
        "source": ODDSPAPI_SOURCE,
        "source_participant_id": normalized_participant_id,
        "name": name,
        "slug": participant_slug(name),
        "short_name": short_name,
        "code_name": abbreviation,
    }


def _fixture_participants(
    fixture: OddspapiFixtureIdentity,
) -> tuple[tuple[ParticipantKey, dict] | None, tuple[ParticipantKey, dict] | None]:
    return (
        _participant_data(
            fixture_id=fixture.fixture_id,
            source_participant_id=fixture.participant1_id,
            name=fixture.participant1_name,
            short_name=fixture.participant1_short_name,
            abbreviation=fixture.participant1_abbr,
        ),
        _participant_data(
            fixture_id=fixture.fixture_id,
            source_participant_id=fixture.participant2_id,
            name=fixture.participant2_name,
            short_name=fixture.participant2_short_name,
            abbreviation=fixture.participant2_abbr,
        ),
    )


def persist_resolved_fixtures(
    session: Session,
    writes: list[ResolvedFixtureWrite],
) -> dict[str, list[str]]:
    """Persist participants first, then all source mappings, in one transaction."""
    if not writes:
        return {}

    participant_rows: list[dict] = []
    participant_keys_by_fixture: dict[
        str,
        tuple[ParticipantKey | None, ParticipantKey | None],
    ] = {}
    for write in writes:
        home, away = _fixture_participants(write.fixture)
        participant_keys_by_fixture[write.fixture.fixture_id] = (
            home[0] if home is not None else None,
            away[0] if away is not None else None,
        )
        if home is not None:
            participant_rows.append(home[1])
        if away is not None:
            participant_rows.append(away[1])

    participants_by_key = ParticipantRepository.upsert_participants(
        session,
        participant_rows,
    )

    mapping_rows: list[dict] = []
    persisted_sources: dict[str, list[str]] = {}
    for write in writes:
        fixture = write.fixture
        home_key, away_key = participant_keys_by_fixture[fixture.fixture_id]
        participant_home = participants_by_key.get(home_key) if home_key is not None else None
        participant_away = participants_by_key.get(away_key) if away_key is not None else None
        external_providers = (
            fixture.external_providers
            if isinstance(fixture.external_providers, dict)
            else {}
        )

        mapping_rows.append(
            {
                "event_id": write.canonical_event_id,
                "source": ODDSPAPI_SOURCE,
                "source_event_id": fixture.fixture_id,
                "source_sport_id": fixture.sport_id,
                "source_tournament_id": fixture.tournament_id,
                "source_season_id": fixture.season_id,
                "participant_home_id": (
                    participant_home.participant_id if participant_home is not None else None
                ),
                "participant_away_id": (
                    participant_away.participant_id if participant_away is not None else None
                ),
                "match_method": write.match_method,
                "confidence": write.confidence,
                "raw_external_providers": external_providers,
            }
        )
        sources = [ODDSPAPI_SOURCE]

        if write.include_secondary_mappings:
            for provider_key, source in SECONDARY_PROVIDER_SOURCES.items():
                provider_id = normalize_source_id(external_providers.get(provider_key))
                if provider_id is None:
                    continue
                mapping_rows.append(
                    {
                        "event_id": write.canonical_event_id,
                        "source": source,
                        "source_event_id": provider_id,
                        "match_method": "external_provider_oddspapi_cross_reference",
                        "confidence": 1.0,
                        "raw_external_providers": external_providers,
                    }
                )
                sources.append(source)
        persisted_sources[fixture.fixture_id] = sources

    EventSourceMappingRepository.upsert_mappings(
        session=session,
        mappings_data=mapping_rows,
    )
    return persisted_sources
