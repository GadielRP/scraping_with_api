"""Timestamp correction helpers for the pre-start job."""

from __future__ import annotations

import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set

from infrastructure.persistence.repositories import EventRepository
from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.timing import minutes_since_start
from modules.alerts import pre_start_notifier
from modules.alerts.alerts_formatter.time_correction_alert import send_time_correction_message

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

        logger.info(f"Starting time mismatch for event {event_id}: {current_starting_time} -> {new_starting_time}")

        if EventRepository.update_event_starting_time(event_id, new_starting_time):
            logger.info(f"✅ Successfully updated starting time for event {event_id}")
            if send_alert:
                logger.info(f"🕐 Sending correction alert for event {event_id}")
                send_time_correction_message(pre_start_notifier, event_id, current_starting_time, new_starting_time)
            return False

        logger.error(f"Failed to update starting time for event {event_id}")
        return False
    except Exception as exc:
        logger.error(f"Error in check_and_update_starting_time for event {event_id}: {exc}")
        return False


def check_recently_started_events_for_timestamp_corrections(events_started_recently: List[Dict]) -> Set[int]:
    """Check recently started events for timestamp corrections."""
    modified_event_ids: Set[int] = set()
    try:
        if not events_started_recently:
            return modified_event_ids

        checked_count = 0
        corrected_count = 0

        def _process_single_recently_started(event_data: Dict) -> dict:
            result = {"checked": False, "corrected": False, "modified_event_id": None}
            try:
                from modules.sofascore import api_client

                event_id = event_data["id"]
                sport = event_data["sport"]
                stored_start_time = event_data["start_time_utc"]
                minutes_ago = abs(minutes_since_start(stored_start_time))

                if sport in ["Tennis", "Tennis Doubles"]:
                    check_intervals = list(range(5, 65, 5))
                else:
                    check_intervals = [15]
                    if minutes_ago > 15:
                        return result

                if minutes_ago not in check_intervals:
                    return result
                
                logger.info(f"Checking recently started event {event_id} ({sport}) for timestamp correction (started {minutes_ago:.1f} minutes ago)")
                correct_starting_time = api_client.get_event_results(
                    event_id,
                    update_time=True,
                    minutes_until_start=minutes_since_start(stored_start_time),
                )

                result["checked"] = True
                if correct_starting_time is None:
                    return result
                if not correct_starting_time:
                    result["corrected"] = True

                result["modified_event_id"] = event_id
            except Exception as exc:
                logger.error(f"Error checking recently started event {event_data.get('id', 'unknown')}: {exc}")
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

        if modified_event_ids:
            logger.info(f"🔄 Timestamp correction detected for {len(modified_event_ids)} event(s)")
        if checked_count > 0:
            logger.info(
                f"📊 Timestamp correction check completed: {checked_count} events checked, {corrected_count} timestamps corrected"
            )
        return modified_event_ids
    except Exception as exc:
        logger.error(f"Error in timestamp correction checks: {exc}")
        return modified_event_ids
