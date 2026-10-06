"""Efficient, chunked processing for an Oddspapi fixture response."""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Callable

from sqlalchemy.orm import Session

from infrastructure.settings import discovery as settings
from infrastructure.persistence.repositories.discovery_repository import (
    DiscoveryRepository, OddspapiCandidatePool,
)
from modules.jobs.discovery.filters import oddspapi_fixture_filter_reason
from .summary import OddspapiFixtureBatchResult
from infrastructure.persistence.repositories.event_source_mapping_repository import (
    EventSourceMappingRepository,
)
from infrastructure.persistence.repositories.event_source_resolution_queue_repository import (
    EventSourceResolutionQueueRepository,
)
from modules.competition.tracked_competitions import (
    tracked_competition_ids as get_tracked_competition_ids,
)
from modules.oddspapi.event_candidate_matcher import MatchDecision, OddspapiEventCandidateMatcher
from modules.oddspapi.event_resolver import OddspapiEventResolution, OddspapiEventResolver
from modules.oddspapi.fixture_normalizer import OddspapiFixtureIdentity
from modules.oddspapi.fixture_response_debug_writer import (
    OddspapiFixtureResponseDebugWriter,
)
from modules.oddspapi.format_utils import normalize_source_id
from modules.oddspapi.fixture_persistence import (
    ResolvedFixtureWrite,
    persist_resolved_fixtures,
)

from .candidate_shortlist import shortlist_candidates

logger = logging.getLogger(__name__)

# Rare dual-perfect ties need human adjudication even when broad queue
# persistence is disabled for low-confidence / no-candidate noise.
ALWAYS_PERSIST_RESOLUTION_STATUSES = frozenset({
    "needs_review_ambiguous_candidates",
})


def should_persist_queue(decision: MatchDecision, *, persist_queue: bool) -> bool:
    """Decide whether an unresolved fixture belongs in the review queue.

    Ambiguous candidates are always queued when mappings are being written.
    All other unresolved statuses remain gated by ``persist_queue`` so routine
    discovery scans do not flood the table.
    """
    status = decision.status or ""
    if status == "unresolved_no_candidates":
        return False
    if status in ALWAYS_PERSIST_RESOLUTION_STATUSES:
        return True
    if not persist_queue:
        return False
    return bool(
        decision.needs_review
        or decision.best_candidate_event_id is not None
        or status
        in {
            "needs_review_low_confidence",
            "sofascore_mapping_not_found",
        }
    )


class OddspapiFixtureBatchProcessor:
    """Resolve fixtures using bulk lookups and chunked commits."""

    def __init__(
        self,
        resolver: type[OddspapiEventResolver] = OddspapiEventResolver,
        matcher: OddspapiEventCandidateMatcher | None = None,
        candidate_pool_loader: Callable | None = None,
        persistence_writer: Callable | None = None,
        chunk_size: int = settings.ODDSPAPI.persistence_chunk_size,
        keep_resolutions: bool = False,
    ) -> None:
        self.resolver = resolver
        self.candidate_pool_loader = candidate_pool_loader or OddspapiCandidatePool.load
        self.persistence_writer = persistence_writer or persist_resolved_fixtures
        self.chunk_size = int(chunk_size)
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self.keep_resolutions = keep_resolutions
        self.tracked_competition_ids = (
            get_tracked_competition_ids()
            if settings.ODDSPAPI.filters.tracked_competitions_only
            else None
        )
        self.matcher = matcher if matcher is not None else self.resolver._candidate_matcher

    def process_batch(
        self,
        fixture_payloads: list[dict],
        create_mappings: bool,
        persist_queue: bool,
        session: Session,
    ) -> OddspapiFixtureBatchResult:
        result = OddspapiFixtureBatchResult(resolutions=[] if self.keep_resolutions else None)
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
            reason = oddspapi_fixture_filter_reason(payload)
            if reason:
                result.fixtures_skipped_policy += 1
                result.rejected_by_reason[reason] = result.rejected_by_reason.get(reason, 0) + 1
                continue
            # Capture the exact fixture object before persistence can omit its
            # participant rows because required source data is absent or invalid.
            OddspapiFixtureResponseDebugWriter.save_if_incomplete(payload)
            identities.append(identity)

        if not identities:
            return result

        for offset in range(0, len(identities), self.chunk_size):
            chunk = identities[offset: offset + self.chunk_size]
            chunk_result = self._process_identity_chunk(
                identities=chunk,
                create_mappings=create_mappings,
                persist_queue=persist_queue,
                session=session,
            )
            result.merge_metrics_from(chunk_result)
            if self.keep_resolutions and chunk_result.resolutions:
                assert result.resolutions is not None
                result.resolutions.extend(chunk_result.resolutions)

            if create_mappings:
                session.commit()
                session.expire_all()

        return result

    def _process_identity_chunk(
        self,
        *,
        identities: list[OddspapiFixtureIdentity],
        create_mappings: bool,
        persist_queue: bool,
        session: Session,
    ) -> OddspapiFixtureBatchResult:
        result = OddspapiFixtureBatchResult(resolutions=[] if self.keep_resolutions else None)
        fixture_ids = [fixture.fixture_id for fixture in identities]
        oddspapi_mapping_details = (
            EventSourceMappingRepository.get_event_mapping_details_by_source_event_ids(
                source="oddspapi",
                source_event_ids=fixture_ids,
                session=session,
            )
        )
        sofascore_ids = [
            value
            for fixture in identities
            if (value := normalize_source_id(fixture.external_providers.get("sofascoreId")))
        ]
        sofascore_mapping_details = (
            EventSourceMappingRepository.get_event_mapping_details_by_source_event_ids(
                source="sofascore",
                source_event_ids=sofascore_ids,
                session=session,
            )
        )
        mapped_event_ids = {
            detail[0] for mappings in (oddspapi_mapping_details, sofascore_mapping_details)
            for detail in mappings.values()
        }
        admitted_ids = DiscoveryRepository.admitted_event_ids(
            session, mapped_event_ids, policy=settings.ODDSPAPI.filters,
        )
        eligible_identities = []
        for fixture in identities:
            detail = oddspapi_mapping_details.get(fixture.fixture_id)
            if detail is None:
                detail = sofascore_mapping_details.get(
                    normalize_source_id(fixture.external_providers.get("sofascoreId"))
                )
            if detail is not None and detail[0] not in admitted_ids:
                result.fixtures_skipped_policy += 1
                reason = "ineligible_canonical_event"
                result.rejected_by_reason[reason] = result.rejected_by_reason.get(reason, 0) + 1
                continue
            eligible_identities.append(fixture)
        identities = eligible_identities
        if not identities:
            return result
        tracked_competition_id_set = (
            set(self.tracked_competition_ids)
            if self.tracked_competition_ids is not None
            else None
        )
        existing_oddspapi: dict[str, int] = {}
        untracked_oddspapi_ids: set[str] = set()
        for source_event_id, (event_id, competition_id) in oddspapi_mapping_details.items():
            if (
                tracked_competition_id_set is not None
                and competition_id not in tracked_competition_id_set
            ):
                untracked_oddspapi_ids.add(source_event_id)
                continue
            existing_oddspapi[source_event_id] = event_id

        existing_sofascore: dict[str, int] = {}
        untracked_sofascore_ids: set[str] = set()
        for source_event_id, (event_id, competition_id) in sofascore_mapping_details.items():
            if (
                tracked_competition_id_set is not None
                and competition_id not in tracked_competition_id_set
            ):
                untracked_sofascore_ids.add(source_event_id)
                continue
            existing_sofascore[source_event_id] = event_id

        unresolved = [
            fixture
            for fixture in identities
            if fixture.fixture_id not in existing_oddspapi
            and fixture.fixture_id not in untracked_oddspapi_ids
        ]
        unresolved = [
            fixture
            for fixture in unresolved
            if not (
                (sofascore_id := normalize_source_id(fixture.external_providers.get("sofascoreId")))
                and (
                    sofascore_id in existing_sofascore
                    or sofascore_id in untracked_sofascore_ids
                )
            )
        ]
        unresolved_ids = {fixture.fixture_id for fixture in unresolved}
        candidate_pool = self.candidate_pool_loader(
            unresolved,
            session,
            competition_ids=self.tracked_competition_ids,
        )
        pending_writes: list[ResolvedFixtureWrite] = []
        persisted_resolutions: list[tuple[OddspapiFixtureIdentity, OddspapiEventResolution]] = []
        untracked_fixture_ids = {
            fixture.fixture_id
            for fixture in identities
            if (
                fixture.fixture_id in untracked_oddspapi_ids
                or (
                    fixture.fixture_id not in existing_oddspapi
                    and normalize_source_id(
                        fixture.external_providers.get("sofascoreId")
                    )
                    in untracked_sofascore_ids
                )
            )
        }

        for fixture in identities:
            if fixture.fixture_id in untracked_fixture_ids:
                result.fixtures_skipped_untracked_competition += 1
                logger.info(
                    "Skipping OddsPapi fixture %s: SofaScore reference resolves to an "
                    "untracked competition",
                    fixture.fixture_id,
                )
                continue
            pool_candidates = (
                candidate_pool.get_candidates_for(fixture)
                if fixture.fixture_id in unresolved_ids
                else []
            )
            shortlist = None
            if pool_candidates:
                shortlist = shortlist_candidates(
                    fixture,
                    pool_candidates,
                    fixture_time=fixture.starts_at,
                )
                decision_candidates = shortlist.events
            else:
                decision_candidates = pool_candidates

            started = perf_counter()
            resolution = self.resolver.resolve_fixture_identity_in_session(
                fixture=fixture,
                session=session,
                # Matching remains read-only for the whole chunk. Successful
                # resolutions are persisted together after every decision exists.
                create_mappings=False,
                persist_queue=False,
                existing_oddspapi=existing_oddspapi,
                existing_sofascore=existing_sofascore,
                candidate_events=decision_candidates,
                queue_pure_no_candidates=False,
                matcher=self.matcher,
            )
            elapsed_ms = round((perf_counter() - started) * 1000.0, 3)
            if self.keep_resolutions and result.resolutions is not None:
                result.resolutions.append(resolution)

            if resolution.layer1_resolved:
                result.resolved_existing_oddspapi += 1
            elif resolution.layer2_resolved:
                result.resolved_external_sofascore += 1
            elif resolution.match_method == "deterministic_candidate_match":
                result.resolved_candidate_match += 1

            if fixture.fixture_id in unresolved_ids and not resolution.layer1_resolved and not resolution.layer2_resolved:
                result.layer3_scored += 1
                if shortlist is not None:
                    result.pool_candidate_counts.append(shortlist.pool_size)
                    result.fuzzy_candidate_counts.append(shortlist.shortlist_size)
                    if shortlist.used_temporal_fallback:
                        result.shortlist_fallback_count += 1
                    if shortlist.widened_time_window:
                        result.shortlist_widened_count += 1
                else:
                    result.pool_candidate_counts.append(0)
                    result.fuzzy_candidate_counts.append(0)
                if resolution.score_duration_ms is not None:
                    result.score_duration_ms_values.append(float(resolution.score_duration_ms))
                else:
                    result.score_duration_ms_values.append(elapsed_ms)

            if resolution.resolved:
                if create_mappings and resolution.canonical_event_id is not None:
                    pending_writes.append(
                        ResolvedFixtureWrite(
                            fixture=fixture,
                            canonical_event_id=resolution.canonical_event_id,
                            match_method=None if resolution.layer1_resolved else resolution.match_method,
                            confidence=None if resolution.layer1_resolved else resolution.confidence,
                            include_secondary_mappings=resolution.layer2_resolved,
                        )
                    )
                    persisted_resolutions.append((fixture, resolution))
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

            if create_mappings and resolution.needs_review:
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
                if should_persist_queue(decision, persist_queue=persist_queue):
                    EventSourceResolutionQueueRepository.upsert_unresolved_attempt(
                        fixture=fixture,
                        resolution_status=decision.status,
                        candidate_scores=resolution.candidate_scores,
                        session=session,
                    )
                    result.queue_rows_written += 1
                    if decision.status in ALWAYS_PERSIST_RESOLUTION_STATUSES:
                        result.ambiguous_queued += 1
                        logger.info(
                            "Queued ambiguous OddsPapi fixture %s for manual review "
                            "best_event=%s second_event=%s gap=%s",
                            fixture.fixture_id,
                            resolution.best_candidate_event_id,
                            resolution.second_candidate_event_id,
                            resolution.score_gap,
                        )

        if pending_writes:
            persisted_sources = self.persistence_writer(session, pending_writes)
            for fixture, resolution in persisted_resolutions:
                # Existing Oddspapi mappings are hydrated, not counted as newly
                # created mappings. Layer 2/3 keep the historical metric meaning.
                if resolution.layer1_resolved:
                    continue
                resolution.created_mappings = persisted_sources.get(fixture.fixture_id, [])
                if "oddspapi" in resolution.created_mappings:
                    result.mappings_created += 1

        return result
