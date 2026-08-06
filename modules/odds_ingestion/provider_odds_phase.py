"""Shared orchestration contract for pre-start provider odds phases.

Every odds provider (SofaScore, Oddspapi, and any future provider) plugs into
the pre-start job through the same narrow orchestration shape: given the
shared candidate plan and this source's own availability state, decide which
events are eligible, fetch odds for them, persist the result, and report a
uniform summary.

Acquisition (how a payload is actually fetched/shaped for one provider) is
intentionally *not* unified here and stays in each provider's own package -
SofaScore's single request and Oddspapi's historical/exchange/worker fan-out
are genuinely different. Only the surrounding orchestration - eligibility
filtering, summary counters, and the fetch/ingest/mark-unavailable sequence -
is shared, so adding a new provider means writing its fetch/ingest callables
and registering the phase, not re-deriving this control flow.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Callable, Protocol

from infrastructure.persistence.repositories import EventSourceMappingRepository

from .fetch_result import OddsFetchResult

logger = logging.getLogger(__name__)


@dataclass
class ProviderOddsSummary:
    """Counters every provider odds phase reports, regardless of source."""

    candidates_seen: int = 0
    requests_attempted: int = 0
    events_ingested: int = 0
    events_skipped: int = 0
    events_failed: int = 0
    missing_endpoints: int = 0
    markets_saved: int = 0


class IngestionOutcome(Protocol):
    """Minimal shape `run_provider_odds_phase` needs from an ingest result."""

    markets_saved: int
    dual_process_market_available: bool


def should_extract_odds(candidate: dict) -> bool:
    """Timing-only eligibility, already decided once by the shared candidate plan."""
    return candidate.get("should_extract_odds") is True


def is_eligible_for_source(candidate: dict, source_states: dict, source: str) -> bool:
    """Timing eligibility plus this source's own recorded availability."""
    if not should_extract_odds(candidate):
        return False
    source_state = source_states.get(candidate.get("event_id"), {}).get(source)
    if source_state is not None and not source_state.has_odds:
        logger.info(
            "🚫 Skipping %s odds fetch for event_id=%s because recorded has_odds=False",
            source,
            candidate.get("event_id"),
        )
        return False
    return True


def select_candidates_for_source(
    candidates: list[dict],
    source_states: dict,
    source: str,
) -> list[dict]:
    """Filter the shared candidate plan down to one source's requestable events."""
    return [
        candidate
        for candidate in candidates
        if is_eligible_for_source(candidate, source_states, source)
    ]


def mark_missing_endpoints_unavailable(missing_event_ids: set[int], source: str) -> None:
    """Persist confirmed 404s for one source in a single bulk update."""
    if not missing_event_ids:
        return
    EventSourceMappingRepository.mark_odds_unavailable(missing_event_ids, source)


def run_provider_odds_phase(
    candidates: list[dict],
    source_states: dict,
    *,
    source: str,
    fetch: Callable[[dict], OddsFetchResult],
    ingest: Callable[[dict, dict], IngestionOutcome],
    can_fetch: Callable[[dict], bool] | None = None,
    on_ingested: Callable[[dict], None] | None = None,
    summary_factory: Callable[[], ProviderOddsSummary] = ProviderOddsSummary,
) -> ProviderOddsSummary:
    """Run one provider's fetch/ingest loop over its eligible candidates.

    Stores the fetched payload and ingestion result back onto each candidate
    dict (``odds_response`` / ``ingestion_result``) so downstream pre-start
    phases (alerts, pillars) can read them, matching the existing shared
    candidate-plan contract.

    ``can_fetch`` lets a provider skip a candidate before counting it as a
    request (e.g. no resolved external id yet) without affecting the shared
    "endpoint missing" bookkeeping, which is reserved for confirmed 404s.
    """
    summary = summary_factory()
    summary.candidates_seen = len(candidates)

    eligible = select_candidates_for_source(candidates, source_states, source)
    summary.events_skipped = len(candidates) - len(eligible)

    missing_endpoint_ids: set[int] = set()
    for candidate in eligible:
        event_id = candidate["event_id"]
        try:
            if can_fetch is not None and not can_fetch(candidate):
                summary.events_skipped += 1
                continue

            summary.requests_attempted += 1
            fetch_result = fetch(candidate)

            if fetch_result.endpoint_missing:
                missing_endpoint_ids.add(event_id)
                summary.missing_endpoints += 1
                summary.events_skipped += 1
                continue

            payload = fetch_result.payload
            if not payload:
                summary.events_skipped += 1
                continue

            candidate["odds_response"] = payload
            ingestion_result = ingest(candidate, payload)
            candidate["ingestion_result"] = ingestion_result

            summary.markets_saved += getattr(ingestion_result, "markets_saved", 0) or 0
            if getattr(ingestion_result, "markets_saved", 0) > 0 or getattr(
                ingestion_result, "dual_process_market_available", False
            ):
                summary.events_ingested += 1
                if on_ingested is not None:
                    on_ingested(candidate)
            else:
                summary.events_skipped += 1
                logger.warning(
                    "No market odds saved for event %s (source=%s): %s",
                    event_id,
                    source,
                    getattr(ingestion_result, "reason", None),
                )
        except Exception as exc:
            summary.events_failed += 1
            logger.error(
                "Error processing %s odds for event %s: %s", source, event_id, exc
            )

    mark_missing_endpoints_unavailable(missing_endpoint_ids, source)
    return summary
