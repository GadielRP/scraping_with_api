"""Timestamp correction helpers for the pre-start job."""

from __future__ import annotations

import logging
from modules.events.discards.contracts import DeletionBatch
from datetime import datetime
from shared.concurrency import bounded_results
from shared.execution_context import WorkDeferred, check_execution_budget
from typing import Dict, List, Optional, Set

from infrastructure.persistence.repositories import EventRepository
from infrastructure.persistence.repositories import ResultRepository
from infrastructure.settings import Config
from shared.temporal import from_unix_timestamp, utc_now
from modules.jobs.pre_start_check_job.timing import minutes_since_start
from modules.alerts import pre_start_notifier
from modules.alerts.alerts_formatter.time_correction_alert import send_time_correction_message
from modules.sofascore.event_identity import resolve_sofascore_event_id

logger = logging.getLogger(__name__)


def convert_timestamp_to_datetime(timestamp: int) -> datetime:
    """Convert a Unix timestamp to an aware UTC instant."""
    return from_unix_timestamp(timestamp)


def is_event_starting_soon(start_timestamp: int, window_minutes: int = 30) -> bool:
    """Check if an event is starting within the specified window."""
    now = utc_now()
    event_time = convert_timestamp_to_datetime(start_timestamp)

    delta_min = (event_time - now).total_seconds() / 60
    return 0 <= delta_min <= window_minutes


def check_and_update_starting_time(
    event_id: int,
    startTimeStamp: int,
    send_alert: bool = False,
    current_starting_time: Optional[datetime] = None,
) -> bool | None:
    """
    Compare the stored starting time with the API timestamp and update the DB if needed.
    Return True if unchanged, False after a committed change, None on failure.
    """
    try:
        if current_starting_time is None:
            event = EventRepository.get_event_by_id(event_id)
            if not event:
                logger.warning(f"Event {event_id} not found in database for timing check")
                return None
            current_starting_time = event.starts_at

        new_starting_time = convert_timestamp_to_datetime(startTimeStamp)

        if current_starting_time == new_starting_time:
            logger.debug(f"Starting time remains consistent for event {event_id}: {current_starting_time}")
            return True

        logger.info(f"⁉️ Starting time mismatch for event {event_id}: {current_starting_time} -> {new_starting_time}")

        if EventRepository.batch_update_starting_times([(event_id, new_starting_time)]) > 0:
            logger.info(f"✅ Successfully updated starting time for event {event_id}")
            if send_alert:
                try:
                    send_time_correction_message(pre_start_notifier, event_id, current_starting_time, new_starting_time)
                except Exception:
                    logger.exception("Starting time updated but correction alert failed event_id=%s", event_id)
            return False

        logger.error(f"Failed to update starting time for event {event_id}")
        return None
    except Exception as exc:
        logger.error(f"Error in check_and_update_starting_time for event {event_id}: {exc}")
        return None


def check_recently_started_events_for_timestamp_corrections(events_started_recently: List[Dict]) -> Set[int]:
    """Check recently started events for timestamp corrections.

    Also parses event status from the same API response to detect early
    finishes (result upserted in batch) and cancellations (deleted in batch).
    Both DB operations are deferred and executed once after all workers finish.
    """
    corrected_event_ids: Set[int] = set()
    try:
        if not events_started_recently:
            return corrected_event_ids

        checked_count = 0
        failed_count = 0
        # Accumulated in-memory; flushed to DB in batch after workers finish.
        results_to_upsert: list[tuple[int, dict]] = []
        event_ids_to_delete = DeletionBatch(origin="timestamp_corrections")

        def _process_single_recently_started(event_data: Dict) -> dict:
            result = {
                "checked": False,
                "failed": False,
                "corrected_event_id": None,
                "upsert": None,       # (canonical_event_id, result_data) | None
                "delete_id": None,    # canonical_event_id | None
            }
            try:
                from modules.sofascore import api_client

                event_id = event_data["id"]
                sport = event_data["sport"]
                stored_start_time = event_data["starts_at"]
                minutes_ago = abs(minutes_since_start(stored_start_time))

                try:
                    sofascore_event_id = resolve_sofascore_event_id(event_id)
                except ValueError as exc:
                    logger.warning("Unable to resolve sofascore_event_id for event %s: %s", event_id, exc)
                    return result

                if sport in ["Tennis", "Tennis Doubles"]:
                    check_intervals = [15, 30, 45, 60]
                else:
                    check_intervals = [15]
                    if minutes_ago > 15:
                        return result

                if minutes_ago not in check_intervals:
                    return result

                logger.info(
                    "Checking recently started event %s (%s) for timestamp correction "
                    "(started %s minutes ago)",
                    event_id, sport, minutes_ago,
                )
                timing_result, parsed = api_client.get_event_results(
                    sofascore_event_id,
                    canonical_event_id=event_id,
                    update_time=True,
                    update_event_info=False,
                    current_start_time=stored_start_time,
                    minutes_until_start=minutes_since_start(stored_start_time),
                    also_parse_result=True,
                )

                result["checked"] = timing_result is not None
                result["failed"] = timing_result is None
                if timing_result is False:
                    result["corrected_event_id"] = event_id

                # --- Status evaluation from the same response ---
                if parsed is not None:
                    if parsed.is_finished and parsed.result is not None:
                        logger.info(
                            "Early finish detected for event %s (%s) "
                            "— queuing result for batch upsert",
                            event_id, sport,
                        )
                        result["upsert"] = (event_id, parsed.result)
                    elif parsed.is_canceled:
                        logger.info(
                            "Cancellation detected for event %s (%s) "
                            "— queuing for batch delete",
                            event_id, sport,
                        )
                        result["delete_id"] = event_id
                        result["parsed"] = parsed
                        result["source_event_id"] = sofascore_event_id

            except WorkDeferred:
                raise
            except Exception as exc:
                result["failed"] = True
                logger.error(
                    "Error checking recently started event %s: %s",
                    event_data.get("id", "unknown"),
                    exc,
                )
            return result

        max_workers = Config.PRE_START_WORKERS
        for res in bounded_results(_process_single_recently_started, events_started_recently, max_workers):
            if res["checked"]:
                checked_count += 1
            if res["failed"]:
                failed_count += 1
            if res["corrected_event_id"] is not None:
                corrected_event_ids.add(res["corrected_event_id"])
            if res["upsert"] is not None:
                results_to_upsert.append(res["upsert"])
            if res["delete_id"] is not None:
                event_ids_to_delete.record(res["delete_id"], res["source_event_id"], res["parsed"])

        if checked_count or failed_count:
            logger.info(
                "Timestamp correction summary: events_checked=%s timestamps_corrected=%s timing_failed=%s",
                checked_count,
                len(corrected_event_ids),
                failed_count,
            )

        # --- Batch DB flush ---
        upserted_count = 0
        if results_to_upsert:
            try:
                upserted_count = len(ResultRepository.batch_upsert_results(results_to_upsert))
                if upserted_count:
                    logger.info(
                        "⚡ Timestamp correction: %s early result(s) upserted in batch",
                        upserted_count,
                    )
            except WorkDeferred:
                raise
            except Exception as exc:
                logger.error(
                    "Failed to batch upsert early results: %s",
                    exc,
                )

        deleted_count = 0
        if event_ids_to_delete:
            requested = len(event_ids_to_delete)
            deleted_count = int(
                EventRepository.batch_delete_events(event_ids_to_delete)
                or 0
            )
            failed_deletes = max(0, requested - deleted_count)
            logger.info(
                "🗑️ Timestamp correction batch deletion: requested=%s deleted=%s failed=%s",
                requested,
                deleted_count,
                failed_deletes,
            )

        return corrected_event_ids
    except WorkDeferred:
        raise
    except Exception as exc:
        logger.error("Error in timestamp correction checks: %s", exc)
        return corrected_event_ids
