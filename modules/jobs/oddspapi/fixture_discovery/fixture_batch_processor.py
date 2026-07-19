"""Efficient, chunked processing for an Oddspapi fixture response."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
from time import perf_counter
from typing import Callable

from sqlalchemy import func, or_
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

from .candidate_shortlist import shortlist_candidates

logger = logging.getLogger(__name__)

COMMIT_EVERY = 50
KEEP_RESOLUTIONS_DEFAULT = False

# Maps matcher sport keys to canonical Event.sport values stored in DB.
SPORT_KEY_TO_DB_NAME = {
    "football": ("Football",),
    "basketball": ("Basketball",),
    "tennis": ("Tennis",),
    "baseball": ("Baseball",),
    "ice hockey": ("Ice hockey",),
    "american football": ("American Football", "American football"),
    "american-football": ("American Football", "American football"),
    "volleyball": ("Volleyball",),
}


def _sport_key(value: object) -> str:
    text = str(value or "").strip().casefold()
    return {"soccer": "football", "hockey": "ice hockey"}.get(text, text)


def _db_sport_names(sport_keys: set[str]) -> list[str]:
    names: list[str] = []
    for key in sorted(sport_keys):
        mapped = SPORT_KEY_TO_DB_NAME.get(key)
        if mapped:
            names.extend(mapped)
        elif key:
            names.append(key.title())
    # Preserve order while deduplicating.
    return list(dict.fromkeys(names))


def _fixture_time(fixture: OddspapiFixtureIdentity) -> datetime | None:
    # The existing candidate matcher compares Event.start_time_utc against
    # fixture.start_time_local as naive values because canonical event rows
    # currently use the local-naive storage convention.  The batch preload
    # must use the exact same basis or it can exclude the correct event before
    # the matcher gets a chance to score it.
    return fixture.start_time_local or fixture.start_time_utc


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


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * pct
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return float(ordered[low] * (1.0 - weight) + ordered[high] * weight)


class OddspapiCandidatePool:
    """A preloaded candidate set with O(log N) time-window lookup."""

    # Keep the batch preload aligned with OddspapiEventCandidateMatcher's
    # single-fixture candidate query window.
    TOLERANCE = timedelta(hours=1)

    def __init__(self, events: list[Event] | None = None) -> None:
        self.events_by_sport: dict[str, list[Event]] = defaultdict(list)
        self._sorted_events_by_sport: dict[str, list[Event]] = {}
        self._sorted_times_by_sport: dict[str, list[datetime]] = {}
        for event in events or []:
            sport = _sport_key(getattr(event, "sport", None))
            self.events_by_sport[sport].append(event)

        for sport, sport_events in self.events_by_sport.items():
            timed = [
                event
                for event in sport_events
                if isinstance(getattr(event, "start_time_utc", None), datetime)
            ]
            timed.sort(key=lambda event: event.start_time_utc)
            self._sorted_events_by_sport[sport] = timed
            self._sorted_times_by_sport[sport] = [event.start_time_utc for event in timed]

    @classmethod
    def load(cls, fixtures: list[OddspapiFixtureIdentity], session: Session) -> "OddspapiCandidatePool":
        if not fixtures:
            return cls([])

        times = [_fixture_time(fixture) for fixture in fixtures if _fixture_time(fixture) is not None]
        sport_keys = {
            _sport_key(fixture.normalized_sport)
            for fixture in fixtures
            if fixture.normalized_sport
        }
        query = session.query(Event).options(
            joinedload(Event.home_participant),
            joinedload(Event.away_participant),
            joinedload(Event.competition_ref),
        )
        db_sports = _db_sport_names(sport_keys)
        if db_sports or sport_keys:
            # Prefer exact canonical sport labels for index-friendly equality,
            # and keep a lower() fallback for legacy spelling variants.
            clauses = []
            if db_sports:
                clauses.append(Event.sport.in_(db_sports))
            if sport_keys:
                clauses.append(func.lower(Event.sport).in_(sorted(sport_keys)))
            query = query.filter(or_(*clauses))
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

        times = self._sorted_times_by_sport.get(sport) or []
        events = self._sorted_events_by_sport.get(sport) or []
        if not times:
            return []

        window_start = fixture_time - self.TOLERANCE
        window_end = fixture_time + self.TOLERANCE
        left = bisect_left(times, window_start)
        right = bisect_right(times, window_end)
        return events[left:right]


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
    ambiguous_queued: int = 0
    layer3_scored: int = 0
    shortlist_fallback_count: int = 0
    shortlist_widened_count: int = 0
    pool_candidate_counts: list[int] = field(default_factory=list)
    fuzzy_candidate_counts: list[int] = field(default_factory=list)
    score_duration_ms_values: list[float] = field(default_factory=list)
    resolutions: list[OddspapiEventResolution] | None = None

    def merge_metrics_from(self, other: "OddspapiFixtureBatchResult") -> None:
        self.fixtures_valid += int(getattr(other, "fixtures_valid", 0) or 0)
        self.fixtures_deduplicated += int(getattr(other, "fixtures_deduplicated", 0) or 0)
        self.invalid_payloads += int(getattr(other, "invalid_payloads", 0) or 0)
        self.resolved_existing_oddspapi += int(getattr(other, "resolved_existing_oddspapi", 0) or 0)
        self.resolved_external_sofascore += int(getattr(other, "resolved_external_sofascore", 0) or 0)
        self.resolved_candidate_match += int(getattr(other, "resolved_candidate_match", 0) or 0)
        self.mappings_created += int(getattr(other, "mappings_created", 0) or 0)
        self.unresolved_no_candidates += int(getattr(other, "unresolved_no_candidates", 0) or 0)
        self.needs_review += int(getattr(other, "needs_review", 0) or 0)
        self.queue_rows_written += int(getattr(other, "queue_rows_written", 0) or 0)
        self.ambiguous_queued += int(getattr(other, "ambiguous_queued", 0) or 0)
        self.layer3_scored += int(getattr(other, "layer3_scored", 0) or 0)
        self.shortlist_fallback_count += int(getattr(other, "shortlist_fallback_count", 0) or 0)
        self.shortlist_widened_count += int(getattr(other, "shortlist_widened_count", 0) or 0)
        self.pool_candidate_counts.extend(list(getattr(other, "pool_candidate_counts", []) or []))
        self.fuzzy_candidate_counts.extend(list(getattr(other, "fuzzy_candidate_counts", []) or []))
        self.score_duration_ms_values.extend(list(getattr(other, "score_duration_ms_values", []) or []))


class OddspapiFixtureBatchProcessor:
    """Resolve fixtures using bulk lookups and chunked commits."""

    def __init__(
        self,
        resolver: type[OddspapiEventResolver] = OddspapiEventResolver,
        matcher: OddspapiEventCandidateMatcher | None = None,
        candidate_pool_loader: Callable | None = None,
        commit_every: int = COMMIT_EVERY,
        keep_resolutions: bool = KEEP_RESOLUTIONS_DEFAULT,
    ) -> None:
        self.resolver = resolver
        self.candidate_pool_loader = candidate_pool_loader or OddspapiCandidatePool.load
        self.commit_every = max(int(commit_every), 1)
        self.keep_resolutions = keep_resolutions
        if matcher is not None:
            self.resolver._candidate_matcher = matcher

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
            identities.append(identity)

        if not identities:
            return result

        for offset in range(0, len(identities), self.commit_every):
            chunk = identities[offset: offset + self.commit_every]
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

            session.flush()
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
                    fixture_time=_fixture_time(fixture),
                )
                decision_candidates = shortlist.events
            else:
                decision_candidates = pool_candidates

            started = perf_counter()
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

        return result


def format_batch_metrics(result: OddspapiFixtureBatchResult) -> str:
    """Compact metric line for sport/batch completion logs."""
    pool = result.pool_candidate_counts
    fuzzy = result.fuzzy_candidate_counts
    score_ms = result.score_duration_ms_values
    return (
        f"l3_scored={result.layer3_scored} "
        f"ambiguous_queued={result.ambiguous_queued} "
        f"shortlist_fallback={result.shortlist_fallback_count} "
        f"shortlist_widened={result.shortlist_widened_count} "
        f"pool_p50={_percentile(pool, 0.50)} pool_p95={_percentile(pool, 0.95)} pool_max={max(pool) if pool else None} "
        f"fuzzy_p50={_percentile(fuzzy, 0.50)} fuzzy_p95={_percentile(fuzzy, 0.95)} fuzzy_max={max(fuzzy) if fuzzy else None} "
        f"score_ms_p50={_percentile(score_ms, 0.50)} score_ms_p95={_percentile(score_ms, 0.95)} "
        f"score_ms_max={max(score_ms) if score_ms else None}"
    )
