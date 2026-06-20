"""Resolve OddsPapi fixture IDs to canonical events without creating events."""

from __future__ import annotations

from dataclasses import dataclass, field

from infrastructure.persistence.repositories.event_source_mapping_repository import (
    EventSourceMappingRepository,
)


@dataclass
class OddspapiEventResolution:
    oddspapi_fixture_id: str
    canonical_event_id: int | None
    resolved: bool
    match_method: str | None = None
    confidence: float | None = None
    skipped_reason: str | None = None
    created_mappings: list[str] = field(default_factory=list)


class OddspapiEventResolver:
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

    @staticmethod
    def _external_id(value) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @classmethod
    def resolve_from_odds_response(
        cls,
        odds_response: dict,
        *,
        create_mappings: bool = True,
    ) -> OddspapiEventResolution:
        payload = odds_response if isinstance(odds_response, dict) else {}
        fixture_id = cls._external_id(payload.get("fixtureId"))
        if fixture_id is None:
            return OddspapiEventResolution(
                oddspapi_fixture_id="",
                canonical_event_id=None,
                resolved=False,
                skipped_reason="missing_oddspapi_fixture_id",
            )

        canonical_event_id = EventSourceMappingRepository.get_event_id_by_source(
            "oddspapi",
            fixture_id,
        )
        if canonical_event_id is not None:
            return OddspapiEventResolution(
                oddspapi_fixture_id=fixture_id,
                canonical_event_id=canonical_event_id,
                resolved=True,
                match_method="existing_oddspapi_mapping",
                confidence=1.0,
            )

        external_providers = payload.get("externalProviders")
        if not isinstance(external_providers, dict):
            external_providers = {}

        sofascore_id = cls._external_id(external_providers.get("sofascoreId"))
        if sofascore_id is None:
            return OddspapiEventResolution(
                oddspapi_fixture_id=fixture_id,
                canonical_event_id=None,
                resolved=False,
                skipped_reason="missing_sofascore_external_provider_id",
            )

        canonical_event_id = EventSourceMappingRepository.get_event_id_by_source(
            "sofascore",
            sofascore_id,
        )
        if canonical_event_id is None:
            return OddspapiEventResolution(
                oddspapi_fixture_id=fixture_id,
                canonical_event_id=None,
                resolved=False,
                skipped_reason="sofascore_mapping_not_found",
            )

        created_mappings: list[str] = []
        if create_mappings:
            EventSourceMappingRepository.upsert_mapping(
                event_id=canonical_event_id,
                source="oddspapi",
                source_event_id=fixture_id,
                source_sport_id=payload.get("sportId"),
                source_tournament_id=payload.get("tournamentId"),
                source_season_id=payload.get("seasonId"),
                match_method="external_provider_sofascore_id",
                confidence=1.0,
                raw_external_providers=external_providers,
            )
            created_mappings.append("oddspapi")

            for provider_key, source in cls.SECONDARY_PROVIDER_SOURCES.items():
                provider_id = cls._external_id(external_providers.get(provider_key))
                if provider_id is None:
                    continue
                EventSourceMappingRepository.upsert_mapping(
                    event_id=canonical_event_id,
                    source=source,
                    source_event_id=provider_id,
                    match_method="external_provider_oddspapi_cross_reference",
                    confidence=1.0,
                    raw_external_providers=external_providers,
                )
                created_mappings.append(source)

        return OddspapiEventResolution(
            oddspapi_fixture_id=fixture_id,
            canonical_event_id=canonical_event_id,
            resolved=True,
            match_method="external_provider_sofascore_id",
            confidence=1.0,
            created_mappings=created_mappings,
        )

    @classmethod
    def resolve_from_fixture_response(
        cls,
        fixture_response: dict,
        *,
        create_mappings: bool = True,
    ) -> OddspapiEventResolution:
        return cls.resolve_from_odds_response(
            fixture_response,
            create_mappings=create_mappings,
        )
