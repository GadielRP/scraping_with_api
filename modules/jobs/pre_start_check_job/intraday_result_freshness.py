"""Intraday result freshness checks for pre-start processing."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set

from infrastructure.persistence.repositories import EventRepository, ResultRepository
from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.timing import minutes_since_start
from modules.sofascore import api_client
from modules.sofascore.event_details import update_event_information_from_response
from modules.sofascore.results_parser import extract_results_from_response

logger = logging.getLogger(__name__)

RESULT_FRESHNESS_WINDOWS_BY_SPORT = {
    "Football": [(110, 125), (145, 160)],
    "Futsal": [(110, 125), (145, 160)],
    "Basketball": [(125, 145), (170, 200)],
    "Baseball": [(180, 200), (240, 260), (300, 330), (360, 390)],
    "American Football": [(180, 220), (240, 300)],
    "Ice Hockey": [(125, 145), (170, 210)],
    "Hockey": [(125, 145), (170, 210)],
}

DEFAULT_RESULT_FRESHNESS_WINDOWS = [(150, 170), (210, 240)]


def _get_minutes_ago(event_data: Dict) -> int:
    return abs(minutes_since_start(event_data["start_time_utc"]))


def _should_check_result_now(event_data: Dict) -> bool:
    sport = event_data.get("sport")
    minutes_ago = _get_minutes_ago(event_data)
    windows = RESULT_FRESHNESS_WINDOWS_BY_SPORT.get(
        sport,
        DEFAULT_RESULT_FRESHNESS_WINDOWS,
    )

    return any(start <= minutes_ago <= end for start, end in windows)


def _describe_status(raw_event: Dict) -> str:
    status = raw_event.get("status") or {}
    return (
        f"code={status.get('code')}, "
        f"type={status.get('type')}, "
        f"description={status.get('description')}"
    )


def _is_real_canceled_status(raw_event: Dict) -> bool:
    status = raw_event.get("status") or {}
    status_type = str(status.get("type") or "").lower().strip()
    status_description = str(status.get("description") or "").lower().strip()

    return (
        status_type in {"canceled", "cancelled"}
        or status_description in {"canceled", "cancelled"}
    )


def process_intraday_result_freshness(events: List[Dict]) -> Dict[str, int]:
    stats = {
        "candidates_received": len(events),
        "candidates_in_check_window": 0,
        "api_checked": 0,
        "results_upserted": 0,
        "not_finished": 0,
        "postponed_or_non_deleted_canceled_group": 0,
        "queued_for_deletion": 0,
        "deleted_events": 0,
        "failed": 0,
    }

    if not events:
        logger.info("Intraday result freshness: no events received")
        return stats

    candidates = []
    for event_data in events:
        try:
            if _should_check_result_now(event_data):
                candidates.append(event_data)
        except Exception as exc:
            logger.warning(
                "Intraday result freshness: could not evaluate check window for event %s: %s",
                event_data.get("id"),
                exc,
            )

    stats["candidates_in_check_window"] = len(candidates)
    logger.info("Intraday result freshness: candidates_received=%s", stats["candidates_received"])
    logger.info(
        "Intraday result freshness: candidates_in_check_window=%s",
        stats["candidates_in_check_window"],
    )

    if not candidates:
        return stats

    delete_event_ids: Set[int] = set()

    def _process_single_event(event_data: Dict) -> Dict:
        event_id = event_data["id"]
        sport = event_data.get("sport")
        try:
            minutes_ago = _get_minutes_ago(event_data)
        except Exception:
            minutes_ago = None

        try:
            response = api_client._request_json(f"/event/{event_id}", no_retry_on_404=True)
            if not response or "event" not in response:
                logger.info(
                    "Intraday result freshness: event %s has no response payload yet",
                    event_id,
                )
                return {"api_checked": 1, "failed": 1}

            update_event_information_from_response(response)
            raw_event = response["event"]
            logger.info(
                "Intraday result freshness checked event %s sport=%s minutes_ago=%s status=%s",
                event_id,
                sport,
                minutes_ago,
                _describe_status(raw_event),
            )

            result_data = extract_results_from_response(response)

            if isinstance(result_data, dict) and result_data.get("_canceled"):
                if _is_real_canceled_status(raw_event):
                    logger.info(
                        "Intraday result freshness: event %s queued for deletion as real canceled",
                        event_id,
                    )
                    return {
                        "api_checked": 1,
                        "queued_for_deletion": 1,
                        "delete_event_id": event_id,
                    }

                logger.info(
                    "Intraday result freshness: event %s not deleted because status is not real canceled. status=%s",
                    event_id,
                    _describe_status(raw_event),
                )
                return {
                    "api_checked": 1,
                    "postponed_or_non_deleted_canceled_group": 1,
                }

            if result_data is None:
                logger.info(
                    "Intraday result freshness: event %s has no finished result yet. status=%s",
                    event_id,
                    _describe_status(raw_event),
                )
                return {"api_checked": 1, "not_finished": 1}

            upserted = ResultRepository.upsert_result(event_id, result_data)
            if upserted:
                logger.info(
                    "Intraday result freshness: upserted result for event %s",
                    event_id,
                )
                return {"api_checked": 1, "results_upserted": 1}

            return {"api_checked": 1, "failed": 1}
        except Exception as exc:
            logger.exception(
                "Intraday result freshness: failed processing event %s: %s",
                event_id,
                exc,
            )
            return {"api_checked": 1, "failed": 1}

    max_workers = min(Config.INTRADAY_RESULT_FRESHNESS_WORKERS, len(candidates))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_event = {
            executor.submit(_process_single_event, event_data): event_data for event_data in candidates
        }
        for future in as_completed(future_to_event):
            try:
                outcome = future.result()
            except Exception as exc:
                logger.exception("Intraday result freshness worker failed unexpectedly: %s", exc)
                stats["api_checked"] += 1
                stats["failed"] += 1
                continue

            stats["api_checked"] += outcome.get("api_checked", 0)
            stats["results_upserted"] += outcome.get("results_upserted", 0)
            stats["not_finished"] += outcome.get("not_finished", 0)
            stats["postponed_or_non_deleted_canceled_group"] += outcome.get(
                "postponed_or_non_deleted_canceled_group",
                0,
            )
            stats["queued_for_deletion"] += outcome.get("queued_for_deletion", 0)
            stats["failed"] += outcome.get("failed", 0)

            delete_event_id = outcome.get("delete_event_id")
            if delete_event_id is not None:
                delete_event_ids.add(delete_event_id)

    if delete_event_ids:
        deleted_count = EventRepository.batch_delete_events(sorted(delete_event_ids))
        stats["deleted_events"] = deleted_count
        logger.info(
            "Intraday result freshness: batch deleted %s canceled event(s)",
            deleted_count,
        )

    return stats
