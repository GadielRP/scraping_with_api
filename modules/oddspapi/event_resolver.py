"""Resolve OddsPapi fixture IDs to canonical events without creating events."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.event_source_mapping_repository import (
    EventSourceMappingRepository,
)
from infrastructure.persistence.repositories.event_source_resolution_queue_repository import (
    EventSourceResolutionQueueRepository,
)

from .event_candidate_matcher import MatchDecision, OddspapiEventCandidateMatcher
from .fixture_normalizer import OddspapiFixtureIdentity

logger = logging.getLogger(__name__)


@dataclass
class OddspapiEventResolution:
    oddspapi_fixture_id: str
    canonical_event_id: int | None
    resolved: bool
    match_method: str | None = None
    confidence: float | None = None
    skipped_reason: str | None = None
    created_mappings: list[str] = field(default_factory=list)
    needs_review: bool = False
    best_candidate_event_id: int | None = None
    second_candidate_event_id: int | None = None
    best_candidate_orientation: str | None = None
    score_gap: float | None = None
    candidate_scores: list[dict] = field(default_factory=list)
    layer1_resolved: bool = False
    layer2_resolved: bool = False
    layer3_found_candidates: bool = False


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

    _candidate_matcher = OddspapiEventCandidateMatcher()

    @staticmethod
    def _external_id(value) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @classmethod
    def _build_resolution(
        cls,
        *,
        fixture_id: str,
        decision: MatchDecision | None = None,
        resolved: bool,
        canonical_event_id: int | None,
        match_method: str | None,
        confidence: float | None,
        skipped_reason: str | None = None,
        created_mappings: list[str] | None = None,
        layer1_resolved: bool = False,
        layer2_resolved: bool = False,
    ) -> OddspapiEventResolution:
        return OddspapiEventResolution(
            oddspapi_fixture_id=fixture_id,
            canonical_event_id=canonical_event_id,
            resolved=resolved,
            match_method=match_method,
            confidence=confidence,
            skipped_reason=skipped_reason,
            created_mappings=created_mappings or [],
            needs_review=bool(decision.needs_review) if decision is not None else False,
            best_candidate_event_id=decision.best_candidate_event_id if decision else None,
            second_candidate_event_id=decision.second_candidate_event_id if decision else None,
            best_candidate_orientation=decision.best_candidate_orientation if decision else None,
            score_gap=decision.score_gap if decision else None,
            candidate_scores=[score.to_dict() for score in decision.candidate_scores] if decision else [],
            layer1_resolved=layer1_resolved,
            layer2_resolved=layer2_resolved,
            layer3_found_candidates=bool(decision.candidate_scores) if decision is not None else False,
        )

    @classmethod
    def _persist_layer2_mappings(
        cls,
        *,
        canonical_event_id: int,
        fixture: OddspapiFixtureIdentity,
        session,
    ) -> list[str]:
        created_mappings: list[str] = []
        EventSourceMappingRepository.upsert_mapping(
            event_id=canonical_event_id,
            source="oddspapi",
            source_event_id=fixture.fixture_id,
            source_sport_id=fixture.sport_id,
            source_tournament_id=fixture.tournament_id,
            source_season_id=fixture.season_id,
            match_method="external_provider_sofascore_id",
            confidence=1.0,
            raw_external_providers=fixture.external_providers,
            session=session,
        )
        created_mappings.append("oddspapi")

        for provider_key, source in cls.SECONDARY_PROVIDER_SOURCES.items():
            provider_id = cls._external_id(fixture.external_providers.get(provider_key))
            if provider_id is None:
                continue
            EventSourceMappingRepository.upsert_mapping(
                event_id=canonical_event_id,
                source=source,
                source_event_id=provider_id,
                match_method="external_provider_oddspapi_cross_reference",
                confidence=1.0,
                raw_external_providers=fixture.external_providers,
                session=session,
            )
            created_mappings.append(source)

        return created_mappings

    @classmethod
    def _resolve_via_existing_mappings(
        cls,
        fixture: OddspapiFixtureIdentity,
        session,
        create_mappings: bool,
        persist_queue: bool,
    ) -> OddspapiEventResolution | None:
        """Attempt direct Layer 1 (existing mapping) and Layer 2 (sofascoreId mapping) resolution."""
        canonical_event_id = EventSourceMappingRepository.get_event_id_by_source(
            "oddspapi",
            fixture.fixture_id,
            session=session,
        )
        if canonical_event_id is not None:
            logger.info(
                "Found existing Oddspapi mapping for fixture %s -> event %s",
                fixture.fixture_id,
                canonical_event_id,
            )
            if create_mappings and persist_queue:
                EventSourceResolutionQueueRepository.clear_resolved(
                    "oddspapi",
                    fixture.fixture_id,
                    session=session,
                )
            return cls._build_resolution(
                fixture_id=fixture.fixture_id,
                resolved=True,
                canonical_event_id=canonical_event_id,
                match_method="existing_oddspapi_mapping",
                confidence=1.0,
                created_mappings=[],
                layer1_resolved=True,
            )

        external_providers = fixture.external_providers if isinstance(fixture.external_providers, dict) else {}
        sofascore_id = cls._external_id(external_providers.get("sofascoreId"))

        if sofascore_id is not None:
            logger.info(
                "Checking external provider sofascoreId=%s for fixture %s",
                sofascore_id,
                fixture.fixture_id,
            )
            canonical_event_id = EventSourceMappingRepository.get_event_id_by_source(
                "sofascore",
                sofascore_id,
                session=session,
            )
            if canonical_event_id is not None:
                logger.info(
                    "Resolved OddsPapi fixture %s via SofaScore event %s",
                    fixture.fixture_id,
                    canonical_event_id,
                )
                created_mappings: list[str] = []
                if create_mappings:
                    logger.info(
                        "Persisting cross-source mappings for fixture %s -> event %s",
                        fixture.fixture_id,
                        canonical_event_id,
                    )
                    created_mappings = cls._persist_layer2_mappings(
                        canonical_event_id=canonical_event_id,
                        fixture=fixture,
                        session=session,
                    )
                if create_mappings and persist_queue:
                    EventSourceResolutionQueueRepository.clear_resolved(
                        "oddspapi",
                        fixture.fixture_id,
                        session=session,
                    )
                return cls._build_resolution(
                    fixture_id=fixture.fixture_id,
                    resolved=True,
                    canonical_event_id=canonical_event_id,
                    match_method="external_provider_sofascore_id",
                    confidence=1.0,
                    created_mappings=created_mappings,
                    layer2_resolved=True,
                )
        return None

    @classmethod
    def resolve_from_odds_response(
        cls,
        odds_response: dict,
        *,
        create_mappings: bool = True,
        persist_queue: bool = True,
    ) -> OddspapiEventResolution:
        payload = odds_response if isinstance(odds_response, dict) else {}
        logger.info("Starting OddsPapi event resolution from odds response")
        try:
            fixture = OddspapiFixtureIdentity.from_payload(payload)
        except ValueError:
            logger.warning("Skipping OddsPapi payload without fixtureId")
            return OddspapiEventResolution(
                oddspapi_fixture_id="",
                canonical_event_id=None,
                resolved=False,
                skipped_reason="missing_oddspapi_fixture_id",
            )

        with db_manager.get_session() as session:
            logger.info("Resolving OddsPapi fixture %s", fixture.fixture_id)
            resolution = cls._resolve_via_existing_mappings(
                fixture=fixture,
                session=session,
                create_mappings=create_mappings,
                persist_queue=persist_queue,
            )
            if resolution is not None:
                return resolution

            logger.info("OddsPapi fixture %s unresolved: no direct mapping found", fixture.fixture_id)
            return cls._build_resolution(
                fixture_id=fixture.fixture_id,
                resolved=False,
                canonical_event_id=None,
                match_method="unresolved_no_direct_mapping",
                confidence=None,
                skipped_reason="unresolved_no_direct_mapping",
            )

    @classmethod
    def resolve_from_fixture_response(
        cls,
        fixture_response: dict,
        *,
        create_mappings: bool = True,
        persist_queue: bool = True,
    ) -> OddspapiEventResolution:
        payload = fixture_response if isinstance(fixture_response, dict) else {}
        logger.info("Starting OddsPapi event resolution from fixture response")
        try:
            fixture = OddspapiFixtureIdentity.from_payload(payload)
        except ValueError:
            logger.warning("Skipping OddsPapi payload without fixtureId")
            return OddspapiEventResolution(
                oddspapi_fixture_id="",
                canonical_event_id=None,
                resolved=False,
                skipped_reason="missing_oddspapi_fixture_id",
            )

        with db_manager.get_session() as session:
            logger.info("Resolving OddsPapi fixture %s", fixture.fixture_id)
            resolution = cls._resolve_via_existing_mappings(
                fixture=fixture,
                session=session,
                create_mappings=create_mappings,
                persist_queue=persist_queue,
            )
            if resolution is not None:
                return resolution

            # Proceed to Layer 3: Candidate matcher (Fuzzy Matcher)
            decision = cls._candidate_matcher.find_best_match(fixture, session=session)
            if decision.resolved and decision.canonical_event_id is not None:
                logger.info(
                    "Candidate matcher resolved fixture %s -> event %s (orientation=%s, confidence=%s)",
                    fixture.fixture_id,
                    decision.canonical_event_id,
                    decision.best_candidate_orientation,
                    decision.confidence,
                )
                created_mappings = []
                if create_mappings:
                    logger.info(
                        "Persisting Oddspapi mapping for fixture %s -> event %s",
                        fixture.fixture_id,
                        decision.canonical_event_id,
                    )
                    EventSourceMappingRepository.upsert_mapping(
                        event_id=decision.canonical_event_id,
                        source="oddspapi",
                        source_event_id=fixture.fixture_id,
                        source_sport_id=fixture.sport_id,
                        source_tournament_id=fixture.tournament_id,
                        source_season_id=fixture.season_id,
                        match_method="deterministic_candidate_match",
                        confidence=decision.confidence,
                        raw_external_providers=fixture.external_providers,
                        session=session,
                    )
                    created_mappings.append("oddspapi")
                    if persist_queue:
                        EventSourceResolutionQueueRepository.clear_resolved(
                            "oddspapi",
                            fixture.fixture_id,
                            session=session,
                        )
                return cls._build_resolution(
                    fixture_id=fixture.fixture_id,
                    decision=decision,
                    resolved=True,
                    canonical_event_id=decision.canonical_event_id,
                    match_method="deterministic_candidate_match",
                    confidence=decision.confidence,
                    created_mappings=created_mappings,
                )

            if create_mappings and persist_queue:
                logger.info(
                    "Persisting unresolved OddsPapi fixture %s with status=%s",
                    fixture.fixture_id,
                    decision.status,
                )
                EventSourceResolutionQueueRepository.upsert_unresolved_attempt(
                    fixture=fixture,
                    resolution_status=decision.status,
                    candidate_scores=decision.candidate_scores,
                    session=session,
                )

            return cls._build_resolution(
                fixture_id=fixture.fixture_id,
                decision=decision,
                resolved=False,
                canonical_event_id=None,
                match_method=decision.status,
                confidence=decision.confidence,
                skipped_reason=decision.status,
            )
