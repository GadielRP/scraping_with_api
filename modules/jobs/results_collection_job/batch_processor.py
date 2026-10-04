"""Resumable result collection: bounded reads, provider work, then committed writes."""

from __future__ import annotations

import logging
from time import monotonic
from collections import Counter
from .contracts import ResultBatchDeferred, ResultOutcome

from infrastructure.persistence.repositories import EventRepository, ResultRepository
from infrastructure.settings import Config
from modules.events.discards.contracts import DeletionBatch
from modules.sofascore import api_client
from modules.sofascore.event_details import fetch_authoritative_event_response, result_from_response
from modules.sofascore.event_normalizer import normalize_event_payload
from shared.execution_context import WorkDeferred, check_execution_budget

logger = logging.getLogger(__name__)


def collect_batch(events, job_name):
    stats = dict(updated=0, deferred=0, failed=0, deleted=0)
    deletions = DeletionBatch(origin=job_name)
    results = {}
    metadata = []
    expected_ids = {}
    outcomes = Counter()
    deferred_reason = None
    fetch_started = monotonic()
    for index, event in enumerate(events):
        try:
            check_execution_budget()
            if event.source_event_id is None:
                stats["failed"] += 1
                outcomes[
                    (
                        ResultOutcome.AMBIGUOUS_MAPPING.value
                        if event.mapping_count > 1
                        else ResultOutcome.MISSING_MAPPING.value
                    )
                ] += 1
                logger.warning(
                    "Result mapping unresolved event_id=%s kind=%s",
                    event.id,
                    "ambiguous_mapping" if event.mapping_count > 1 else "missing_mapping",
                )
                continue
            source_id = int(event.source_event_id)
            response = fetch_authoritative_event_response(
                api_client,
                source_id,
                canonical_event_id=event.id,
                deferred_deletion_event_ids=deletions,
            )
            if not response:
                stats["failed"] += int(event.id not in deletions)
                outcomes[ResultOutcome.PROVIDER_ERROR.value] += int(event.id not in deletions)
                continue
            if str(response.get("event", {}).get("id")) != str(source_id):
                raise ValueError("Provider response identity does not match requested event")
            result = result_from_response(
                response,
                source_id,
                canonical_event_id=event.id,
                deferred_deletion_event_ids=deletions,
                on_not_started="delete",
                log_result_diagnostics=Config.global_debug_mode,
            )
            # Do not update metadata after observing a deletion: that would make
            # our own evidence stale and defeat the deletion concurrency guard.
            if event.id in deletions:
                continue
            normalized = normalize_event_payload(response["event"], discovery_source="results_sync")
            payload = normalized.get("event", normalized)
            payload.pop("discovery_source", None)
            metadata.append(normalized)
            expected_ids[str(source_id)] = event.id
            if result:
                results[str(source_id)] = result
            else:
                stats["deferred"] += 1
                outcomes[ResultOutcome.DEFERRED.value] += 1
        except WorkDeferred as exc:
            deferred_reason = exc
            remaining = len(events) - index
            stats["deferred"] += remaining
            outcomes[ResultOutcome.BUDGET_DEFERRED.value] += remaining
            break
        except Exception:
            logger.exception("%s event failed event_id=%s", job_name, event.id)
            stats["failed"] += 1
            outcomes[ResultOutcome.PROVIDER_ERROR.value] += 1
    fetch_seconds = monotonic() - fetch_started
    write_started = monotonic()
    saved = EventRepository.batch_upsert_events(metadata, expected_event_ids=expected_ids)
    persistable = [
        (event.id, results[sid]) for sid, event in saved.events.items() if sid in results
    ]
    stats["failed"] += sum(sid not in saved.events for sid in results)
    outcomes[ResultOutcome.CONFLICT.value] += sum(sid not in saved.events for sid in results)
    confirmed = ResultRepository.batch_upsert_results(persistable)
    stats["updated"] = len(confirmed)
    stats["failed"] += len(persistable) - stats["updated"]
    outcomes[ResultOutcome.PERSISTED.value] += stats["updated"]
    outcomes[ResultOutcome.CONFLICT.value] += len(persistable) - stats["updated"]
    if deletions:
        stats["deleted"] = EventRepository.batch_delete_events(deletions)
        stats["failed"] += len(deletions) - stats["deleted"]
        outcomes[ResultOutcome.DELETED.value] += stats["deleted"]
        outcomes[ResultOutcome.CONFLICT.value] += len(deletions) - stats["deleted"]
    logger.info(
        "%s batch completed candidates=%s last_event_id=%s fetch_parse_s=%.3f "
        "persist_s=%.3f stats=%s",
        job_name,
        len(events),
        events[-1].id,
        fetch_seconds,
        monotonic() - write_started,
        stats,
    )
    logger.info("%s batch outcomes=%s", job_name, dict(outcomes))
    if deferred_reason:
        raise ResultBatchDeferred(deferred_reason, stats) from deferred_reason
    return stats
