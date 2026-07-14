"""Efficient, single-session processing for an Oddspapi fixture response."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
from typing import Callable

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from infrastructure.persistence.models import Event
from infrastructure.persistence.repositories.event_source_mapping_repository import (
    EventSourceMappingRepository,
)
from infrastructure.persistence.repositories.event_source_resolution_queue_repository import (
    EventSourceResolutionQueueRepository,
)
from modules.oddspapi.event_candidate_matcher import MatchDecision, OddspapiEventCandidateMatcher
from modules.oddspapi.event_resolver import OddspapiEventResolution, OddspapiEventResolver
from modules.oddspapi.fixture_normalizer import OddspapiFixtureIdentity

logger = logging.getLogger(__name__)


def _sport_key(value: object) -> str:
    text = str(value or "").strip().casefold()
    return {"soccer": "football", "hockey": "ice hockey"}.get(text, text)


def _fixture_time(fixture: OddspapiFixtureIdentity) -> datetime | None:
    # The existing candidate matcher compares Event.start_time_utc against
    # fixture.start_time_local as naive values because canonical event rows
    # currently use the local-naive storage convention.  The batch preload
    # must use the exact same basis or it can exclude the correct event before
    # the matcher gets a chance to score it.
    return fixture.start_time_local or fixture.start_time_utc


def should_persist_queue(decision: MatchDecision) -> bool:
    """Keep broad scans from producing noise for fixtures with no candidates."""
    if decision.status == "unresolved_no_candidates":
        return False
    return bool(
        decision.needs_review
        or decision.best_candidate_event_id is not None
        or decision.status
        in {
            "needs_review_low_confidence",
            "needs_review_ambiguous_candidates",
            "sofascore_mapping_not_found",
        }
    )


class OddspapiCandidatePool:
    """A preloaded, lightly bucketed set of canonical event candidates."""

    # Keep the batch preload aligned with OddspapiEventCandidateMatcher's
    # single-fixture candidate query window.
    TOLERANCE = timedelta(hours=1)

    def __init__(self, events: list[Event] | None = None) -> None:
        self.events_by_sport: dict[str, list[Event]] = defaultdict(list)
        self.events_by_sport_and_date_bucket: dict[tuple[str, date], list[Event]] = defaultdict(list)
        for event in events or []:
            sport = _sport_key(getattr(event, "sport", None))
            self.events_by_sport[sport].append(event)
            start = getattr(event, "start_time_utc", None)
            if start is not None:
                self.events_by_sport_and_date_bucket[(sport, start.date())].append(event)

    @classmethod
    def load(cls, fixtures: list[OddspapiFixtureIdentity], session: Session) -> "OddspapiCandidatePool":
        if not fixtures:
            return cls([])

        times = [_fixture_time(fixture) for fixture in fixtures if _fixture_time(fixture) is not None]
        sports = sorted({_sport_key(fixture.normalized_sport) for fixture in fixtures if fixture.normalized_sport})
        query = session.query(Event).options(
            joinedload(Event.home_participant),
            joinedload(Event.away_participant),
            joinedload(Event.competition_ref),
        )
        if sports:
            query = query.filter(func.lower(Event.sport).in_(sports))
        if times:
            query = query.filter(
                Event.start_time_utc >= min(times) - cls.TOLERANCE,
                Event.start_time_utc <= max(times) + cls.TOLERANCE,
            )
        events = query.all()
        logger.info("Loaded Oddspapi candidate pool events=%s fixtures=%s", len(events), len(fixtures))
        return cls(events)

    def get_candidates_for(self, fixture: OddspapiFixtureIdentity) -> list[Event]:
        sport = _sport_key(fixture.normalized_sport)
        fixture_time = _fixture_time(fixture)
        if fixture_time is None:
            return list(self.events_by_sport.get(sport, []))

        candidates: list[Event] = []
        for event in self.events_by_sport.get(sport, []):
            event_time = getattr(event, "start_time_utc", None)
            if event_time is None or abs(event_time - fixture_time) <= self.TOLERANCE:
                candidates.append(event)
        return candidates


@dataclass
class OddspapiFixtureBatchResult:
    fixtures_valid: int = 0
    fixtures_deduplicated: int = 0
    invalid_payloads: int = 0
    resolved_existing_oddspapi: int = 0
    resolved_external_sofascore: int = 0
    resolved_candidate_match: int = 0
    mappings_created: int = 0
    unresolved_no_candidates: int = 0
    needs_review: int = 0
    queue_rows_written: int = 0
    resolutions: list[OddspapiEventResolution] | None = None


class OddspapiFixtureBatchProcessor:
    """Resolve a response using bulk lookups and one caller-owned session."""

    def __init__(
        self,
        resolver: type[OddspapiEventResolver] = OddspapiEventResolver,
        matcher: OddspapiEventCandidateMatcher | None = None,
        candidate_pool_loader: Callable | None = None,
    ) -> None:
        self.resolver = resolver
        self.candidate_pool_loader = candidate_pool_loader or OddspapiCandidatePool.load
        if matcher is not None:
            self.resolver._candidate_matcher = matcher

    def process_batch(
        self,
        fixture_payloads: list[dict],
        create_mappings: bool,
        persist_queue: bool,
        session: Session,
    ) -> OddspapiFixtureBatchResult:
        result = OddspapiFixtureBatchResult(resolutions=[])
        identities: list[OddspapiFixtureIdentity] = []
        seen_ids: set[str] = set()

        for payload in fixture_payloads or []:
            try:
                identity = OddspapiFixtureIdentity.from_payload(payload)
            except (TypeError, ValueError):
                result.invalid_payloads += 1
                continue
            result.fixtures_valid += 1
            if identity.fixture_id in seen_ids:
                result.fixtures_deduplicated += 1
                continue
            seen_ids.add(identity.fixture_id)
            identities.append(identity)

        fixture_ids = [fixture.fixture_id for fixture in identities]
        existing_oddspapi = EventSourceMappingRepository.get_event_ids_by_source_event_ids(
            source="oddspapi",
            source_event_ids=fixture_ids,
            session=session,
        )
        sofascore_ids = [
            value
            for fixture in identities
            if (value := OddspapiEventResolver._external_id(fixture.external_providers.get("sofascoreId")))
        ]
        existing_sofascore = EventSourceMappingRepository.get_event_ids_by_source_event_ids(
            source="sofascore",
            source_event_ids=sofascore_ids,
            session=session,
        )

        unresolved = [fixture for fixture in identities if fixture.fixture_id not in existing_oddspapi]
        unresolved = [
            fixture
            for fixture in unresolved
            if not (
                (sofascore_id := OddspapiEventResolver._external_id(
                    fixture.external_providers.get("sofascoreId"),
                ))
                and sofascore_id in existing_sofascore
            )
        ]
        unresolved_ids = {fixture.fixture_id for fixture in unresolved}
        candidate_pool = self.candidate_pool_loader(unresolved, session)

        for fixture in identities:
            decision_candidates = (
                candidate_pool.get_candidates_for(fixture)
                if fixture.fixture_id in unresolved_ids
                else []
            )
            resolution = self.resolver.resolve_fixture_identity_in_session(
                fixture=fixture,
                session=session,
                create_mappings=create_mappings,
                persist_queue=False,
                existing_oddspapi=existing_oddspapi,
                existing_sofascore=existing_sofascore,
                candidate_events=decision_candidates,
                queue_pure_no_candidates=False,
            )
            result.resolutions.append(resolution)

            if resolution.layer1_resolved:
                result.resolved_existing_oddspapi += 1
            elif resolution.layer2_resolved:
                result.resolved_external_sofascore += 1
            elif resolution.match_method == "deterministic_candidate_match":
                result.resolved_candidate_match += 1

            if "oddspapi" in resolution.created_mappings:
                result.mappings_created += 1

            if resolution.resolved:
                if create_mappings and persist_queue:
                    EventSourceResolutionQueueRepository.clear_resolved(
                        "oddspapi",
                        fixture.fixture_id,
                        session=session,
                    )
                continue

            if resolution.skipped_reason == "unresolved_no_candidates":
                result.unresolved_no_candidates += 1
            elif resolution.needs_review:
                result.needs_review += 1

            if create_mappings and persist_queue and resolution.needs_review:
                decision = MatchDecision(
                    resolved=False,
                    needs_review=resolution.needs_review,
                    status=resolution.skipped_reason or "unresolved",
                    match_method=resolution.match_method or "unresolved",
                    confidence=resolution.confidence,
                    canonical_event_id=None,
                    best_candidate_event_id=resolution.best_candidate_event_id,
                    second_candidate_event_id=resolution.second_candidate_event_id,
                    best_candidate_orientation=resolution.best_candidate_orientation,
                    score_gap=resolution.score_gap,
                    candidate_scores=resolution.candidate_scores,
                )
                if should_persist_queue(decision):
                    EventSourceResolutionQueueRepository.upsert_unresolved_attempt(
                        fixture=fixture,
                        resolution_status=decision.status,
                        candidate_scores=resolution.candidate_scores,
                        session=session,
                    )
                    result.queue_rows_written += 1

        session.flush()
        return result
