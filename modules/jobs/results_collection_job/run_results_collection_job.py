"""Resumable result collection: bounded reads, provider work, then committed writes."""
from __future__ import annotations

import logging
from datetime import date, timedelta
from time import monotonic

from infrastructure.persistence.repositories import EventRepository, ResultRepository
from infrastructure.settings import Config
from modules.events.discards.contracts import DeletionBatch
from modules.observations import sport_observation_service
from modules.sofascore import api_client
from modules.sofascore.event_details import fetch_authoritative_event_response, result_from_response
from modules.sofascore.event_normalizer import normalize_event_payload
from shared.shutdown import is_shutdown_requested
from shared.temporal import now_in_timezone

logger = logging.getLogger(__name__)


def _collect_batch(events, job_name):
    stats = dict(updated=0, skipped=0, failed=0, deleted=0)
    deletions = DeletionBatch(origin=job_name)
    results = {}
    metadata = []
    expected_ids = {}
    fetch_started = monotonic()
    for event in events:
        if is_shutdown_requested():
            raise KeyboardInterrupt()
        try:
            if event.source_event_id is None:
                raise ValueError(f"Missing or ambiguous SofaScore mapping for event_id={event.id}")
            source_id = int(event.source_event_id)
            response = fetch_authoritative_event_response(
                api_client, source_id, canonical_event_id=event.id,
                deferred_deletion_event_ids=deletions,
            )
            if not response:
                stats['failed'] += int(event.id not in deletions)
                continue
            if str(response.get('event', {}).get('id')) != str(source_id):
                raise ValueError('Provider response identity does not match requested event')
            result = result_from_response(
                response, source_id, canonical_event_id=event.id,
                deferred_deletion_event_ids=deletions, on_not_started='delete',
                log_result_diagnostics=Config.global_debug_mode,
            )
            # Do not update metadata after observing a deletion: that would make
            # our own evidence stale and defeat the deletion concurrency guard.
            if event.id in deletions:
                continue
            normalized = normalize_event_payload(response['event'], discovery_source='results_sync')
            payload = normalized.get('event', normalized)
            payload.pop('discovery_source', None)
            metadata.append(normalized)
            expected_ids[str(source_id)] = event.id
            if result:
                results[str(source_id)] = result
            else:
                stats['failed'] += 1
        except Exception:
            logger.exception('%s event failed event_id=%s', job_name, event.id)
            stats['failed'] += 1
    fetch_seconds = monotonic() - fetch_started
    write_started = monotonic()
    saved = EventRepository.batch_upsert_events(metadata, expected_event_ids=expected_ids)
    persistable = [(event.id, results[sid]) for sid, event in saved.events.items() if sid in results]
    stats['failed'] += sum(sid not in saved.events for sid in results)
    stats['updated'] = ResultRepository.batch_upsert_results(persistable)
    stats['failed'] += len(persistable) - stats['updated']
    for sid, event in saved.events.items():
        if sid in results:
            sport_observation_service.process_result_observations(event, results[sid])
    if deletions:
        stats['deleted'] = EventRepository.batch_delete_events(deletions)
        stats['failed'] += len(deletions) - stats['deleted']
    logger.info('%s batch completed candidates=%s last_event_id=%s fetch_parse_s=%.3f '
                'persist_s=%.3f stats=%s', job_name, len(events), events[-1].id,
                fetch_seconds, monotonic() - write_started, stats)
    return stats


def _run_collection(target_date, job_name):
    stats = dict(updated=0, skipped=0, failed=0, deleted=0)
    started = monotonic()
    logger.info('Starting %s target_date=%s batch_size=%s', job_name, target_date, Config.EVENT_WRITE_BATCH_SIZE)
    try:
        for batch in ResultRepository.pending_batches(target_date):
            if is_shutdown_requested():
                raise KeyboardInterrupt()
            batch_stats = _collect_batch(batch, job_name)
            for key, count in batch_stats.items():
                stats[key] += count
    except KeyboardInterrupt:
        logger.info('%s interrupted; committed batches retained stats=%s', job_name, stats)
        raise
    except Exception:
        logger.exception('%s failed; committed batches retained stats=%s', job_name, stats)
        raise
    logger.info('%s completed duration_s=%.3f stats=%s', job_name, monotonic() - started, stats)
    return stats


def run_results_collection_for_date(target_date=None, job_name=None):
    if target_date is None:
        target_date = now_in_timezone(Config.TIMEZONE).date() - timedelta(days=1)
    elif isinstance(target_date, str):
        target_date = date.fromisoformat(target_date)
    return _run_collection(target_date, job_name or f'Results Collection ({target_date})')


def run_results_collection_previous_day():
    return run_results_collection_for_date(job_name='Results Collection (previous day)')


def run_results_collection_all_finished():
    return _run_collection(None, 'Results Collection (all finished)')
