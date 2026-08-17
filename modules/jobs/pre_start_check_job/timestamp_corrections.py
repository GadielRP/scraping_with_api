"""Timestamp correction helpers for the pre-start job."""

from __future__ import annotations

import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set

from infrastructure.persistence.repositories import EventRepository
from infrastructure.persistence.repositories import ResultRepository
from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.timing import minutes_since_start
from modules.alerts import pre_start_notifier
from modules.alerts.alerts_formatter.time_correction_alert import send_time_correction_message
from modules.sofascore.event_identity import resolve_sofascore_event_id

logger = logging.getLogger(__name__)


def convert_timestamp_to_datetime(timestamp: int) -> datetime:
    """Convert Unix timestamp to datetime object."""
    return datetime.fromtimestamp(timestamp)


def is_event_starting_soon(start_timestamp: int, window_minutes: int = 30) -> bool:
    """Check if an event is starting within the specified window."""
    now = datetime.now()
    event_time = convert_timestamp_to_datetime(start_timestamp)

    delta_min = (event_time - now).total_seconds() / 60
    return 0 <= delta_min <= window_minutes


def check_and_update_starting_time(
    event_id: int,
    startTimeStamp: int,
    send_alert: bool = False,
    current_starting_time: Optional[datetime] = None,
) -> bool:
    """
    Compare the stored starting time with the API timestamp and update the DB if needed.
    Returns True when the current and new timestamps match, False otherwise.
    """
    try:
        if current_starting_time is None:
            event = EventRepository.get_event_by_id(event_id)
            if not event:
                logger.warning(f"Event {event_id} not found in database for timing check")
                return False
            current_starting_time = event.start_time_utc

        new_starting_time = convert_timestamp_to_datetime(startTimeStamp)

        if current_starting_time == new_starting_time:
            logger.debug(f"Starting time remains consistent for event {event_id}: {current_starting_time}")
            return True

        logger.info(f"⁉️ Starting time mismatch for event {event_id}: {current_starting_time} -> {new_starting_time}")

        if EventRepository.batch_update_starting_times([(event_id, new_starting_time)]) > 0:
            logger.info(f"✅ Successfully updated starting time for event {event_id}")
            if send_alert:
                send_time_correction_message(pre_start_notifier, event_id, current_starting_time, new_starting_time)
            return False

        logger.error(f"Failed to update starting time for event {event_id}")
        return False
    except Exception as exc:
        logger.error(f"Error in check_and_update_starting_time for event {event_id}: {exc}")
        return False


def check_recently_started_events_for_timestamp_corrections(events_started_recently: List[Dict]) -> Set[int]:
    """Check recently started events for timestamp corrections.

    Also parses event status from the same API response to detect early
    finishes (result upserted in batch) and cancellations (deleted in batch).
    Both DB operations are deferred and executed once after all workers finish.
    """
    modified_event_ids: Set[int] = set()
    try:
        if not events_started_recently:
            return modified_event_ids

        checked_count = 0
        corrected_count = 0
        # Accumulated in-memory; flushed to DB in batch after workers finish.
        results_to_upsert: list[tuple[int, dict]] = []
        event_ids_to_delete: set[int] = set()

        def _process_single_recently_started(event_data: Dict) -> dict:
            result = {
                "checked": False,
                "corrected": False,
                "modified_event_id": None,
                "upsert": None,       # (canonical_event_id, result_data) | None
                "delete_id": None,    # canonical_event_id | None
            }
            try:
                from modules.sofascore import api_client

                event_id = event_data["id"]
                sport = event_data["sport"]
                stored_start_time = event_data["start_time_utc"]
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

                result["checked"] = True
                if timing_result is None:
                    return result
                if not timing_result:
                    result["corrected"] = True
                result["modified_event_id"] = event_id

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

            except Exception as exc:
                logger.error(
                    "Error checking recently started event %s: %s",
                    event_data.get("id", "unknown"),
                    exc,
                )
            return result

        max_workers = getattr(Config, "PRE_START_WORKERS", 5)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_process_single_recently_started, event) for event in events_started_recently]
            for future in as_completed(futures):
                res = future.result()
                if res["checked"]:
                    checked_count += 1
                if res["corrected"]:
                    corrected_count += 1
                if res["modified_event_id"]:
                    modified_event_ids.add(res["modified_event_id"])
                if res["upsert"] is not None:
                    results_to_upsert.append(res["upsert"])
                if res["delete_id"] is not None:
                    event_ids_to_delete.add(res["delete_id"])

        if modified_event_ids:
            logger.info("🔄 Timestamp correction detected for %s event(s)", len(modified_event_ids))
        if checked_count > 0:
            logger.info(
                "📊 Timestamp correction check completed: %s events checked, %s timestamps corrected",
                checked_count,
                corrected_count,
            )

        # --- Batch DB flush ---
        upserted_count = 0
        if results_to_upsert:
            try:
                upserted_count = ResultRepository.batch_upsert_results(results_to_upsert)
                if upserted_count:
                    logger.info(
                        "⚡ Timestamp correction: %s early result(s) upserted in batch",
                        upserted_count,
                    )
            except Exception as exc:
                logger.error(
                    "Failed to batch upsert early results: %s",
                    exc,
                )

        deleted_count = 0
        if event_ids_to_delete:
            requested = len(event_ids_to_delete)
            deleted_count = int(
                EventRepository.batch_delete_events(sorted(event_ids_to_delete))
                or 0
            )
            failed_deletes = max(0, requested - deleted_count)
            logger.info(
                "🗑️ Timestamp correction batch deletion: requested=%s deleted=%s failed=%s",
                requested,
                deleted_count,
                failed_deletes,
            )

        return modified_event_ids
    except Exception as exc:
        logger.error("Error in timestamp correction checks: %s", exc)
        return modified_event_ids
