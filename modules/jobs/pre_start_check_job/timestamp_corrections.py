"""Timestamp correction helpers for the pre-start job."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set

from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.timing import minutes_since_start
from sofascore_api import api_client

logger = logging.getLogger(__name__)


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
