"""OddsPortal worker helpers for the pre-start job."""

from __future__ import annotations

import logging
import threading
import time
import traceback
from typing import Any, Dict, List, Optional

from infrastructure.persistence.repositories import MarketRepository
from infrastructure.settings import Config
from modules.oddsportal import scrape_multiple_matches_parallel_sync
from modules.oddsportal.oddsportal_config import SEASON_ODDSPORTAL_MAP, get_current_date

logger = logging.getLogger(__name__)


def build_oddsportal_scrape_candidates(
    upcoming_events: List[Dict],
    pre_calculated_timings: Dict[int, int],
) -> List[Dict]:
    """Collect the events that should be scraped by OddsPortal."""
    if not Config.ODDSPORTAL_SCRAPING_ENABLED:
        logger.info("OddsPortal scraping is disabled by config; skipping candidate selection.")
        return []

    candidates: List[Dict] = []

    for event_dict in upcoming_events or []:
        season_id = event_dict.get("season_id")
        minutes_until_start = pre_calculated_timings.get(event_dict["id"])

        if season_id and season_id in SEASON_ODDSPORTAL_MAP and minutes_until_start == -5:
            candidates.append(
                {
                    "event_id": event_dict["id"],
                    "event_data": event_dict,
                    "minutes_until_start": minutes_until_start,
                    "should_extract_odds": True,
                }
            )

    if candidates:
        logger.info(f"OddsPortal candidate selection produced {len(candidates)} tracked events.")
    else:
        logger.info("No OddsPortal candidates matched the current pre-start window.")

    return candidates


def create_oddsportal_scrape_state(op_candidates: List[Dict]) -> Dict[int, Dict[str, threading.Event]]:
    """Create the per-event state used to coordinate scraping and alert sending."""
    return {
        candidate["event_id"]: {
            "started_event": threading.Event(),
            "done_event": threading.Event(),
            "started_at_monotonic": None,
            "done_at_monotonic": None,
        }
        for candidate in op_candidates
    }


def start_oddsportal_scrape_thread(
    scheduler,
    op_candidates: List[Dict],
    op_event_states: Dict[int, Dict[str, threading.Event]],
    op_data_cache: Dict[int, Any],
):
    """Start OddsPortal scraping in the background if there is work to do."""
    if not Config.ODDSPORTAL_SCRAPING_ENABLED or not op_candidates:
        scheduler._active_op_thread = None
        return None

    def _orchestrate(previous_thread, candidates, event_states, data_cache):
        if previous_thread and previous_thread.is_alive():
            timeout = Config.ODDSPORTAL_PREVIOUS_CYCLE_TIMEOUT
            logger.warning(f"⏳ Previous OP worker still running - waiting up to {timeout}s for it to finish...")
            previous_thread.join(timeout=timeout)
            if previous_thread.is_alive():
                logger.error(
                    f"🛑 Previous OP worker STILL didn't finish after {timeout}s! "
                    "Aborting new OP cycle to prevent double-activation and memory exhaustion."
                )
                for state in event_states.values():
                    state["done_event"].set()
                return
            logger.info("✅ Previous OP worker finished - proceeding with new cycle")

        logger.info(f"🚀 Launching OddsPortal scraper for {len(candidates)} tracked-league events...")
        run_oddsportal_scrape_cycle(candidates, event_states, data_cache)

    previous_thread = getattr(scheduler, "_active_op_thread", None)
    oddsportal_thread = threading.Thread(
        target=_orchestrate,
        args=(previous_thread, op_candidates, op_event_states, op_data_cache),
        name="oddsportal_worker_launcher",
        daemon=False,
    )
    oddsportal_thread.start()
    scheduler._active_op_thread = oddsportal_thread
    return oddsportal_thread


def run_oddsportal_scrape_cycle(
    op_candidates: List[Dict],
    op_event_states: Optional[Dict[int, Dict[str, threading.Event]]] = None,
    op_data_cache: Optional[Dict[int, Any]] = None,
):
    """Run the OddsPortal scrape worker and guarantee event-state cleanup."""
    logger.info(f"🔥 OP Worker started: scraping {len(op_candidates)} tracked-league events.")
    try:
        scrape_oddsportal_batch(op_candidates, op_event_states, op_data_cache)
    except Exception as exc:
        logger.error(f"❌ OddsPortal Worker CRASHED: {exc}\n{traceback.format_exc()}")
    finally:
        if op_event_states:
            for event_id, state in op_event_states.items():
                if not state["done_event"].is_set():
                    state["done_event"].set()
                    state["done_at_monotonic"] = time.monotonic()
                    if not state["started_event"].is_set():
                        logger.warning(
                            f"⚠️ OP Worker: force-signaled event {event_id} (was force-unblocked without ever starting)"
                        )
                    else:
                        logger.warning(
                            f"⚠️ OP Worker: force-signaled event {event_id} "
                            "(was force-unblocked after starting but before clean completion)"
                        )
        logger.info("✅ OP Worker finished scraping, main thread unblocked.")


def scrape_oddsportal_batch(
    events_to_process: List[Dict],
    op_event_states: Optional[Dict[int, Dict[str, threading.Event]]] = None,
    op_data_cache: Optional[Dict[int, Any]] = None,
) -> Dict[int, Optional[int]]:
    """
    Scrape all OddsPortal-eligible matches and persist them.

    Returns a mapping of event_id -> number of markets saved, or None on failure.
    """
    op_current_date = get_current_date()
    op_tasks = []

    for event_info in events_to_process:
        event_data = event_info["event_data"]
        season_id = event_data.get("season_id")
        op_info = SEASON_ODDSPORTAL_MAP.get(season_id)

        if op_info and event_info.get("should_extract_odds"):
            league_url = f"https://www.{Config.ODDSPORTAL_DOMAIN}/{op_info['sport']}/{op_info['country']}/{op_info['league']}/"
            op_tasks.append(
                {
                    "event_id": event_data["id"],
                    "league_url": league_url,
                    "home_team": event_data["home_team"],
                    "away_team": event_data["away_team"],
                    "season_id": season_id,
                    "sport": op_info["sport"],
                    "_oddsportal_resume_state": None,
                    "_oddsportal_partial_match_data": None,
                }
            )

    if not op_tasks:
        logger.info("ℹ️ OddsPortal: No eligible events to scrape")
        return {}

    logger.info(f"🔍 OddsPortal worker: {len(op_tasks)} events eligible for scraping")
    saved_counts: Dict[int, Optional[int]] = {}

    def _on_event_started(event_id, task=None):
        if op_event_states and event_id in op_event_states:
            state = op_event_states[event_id]
            if not state["started_event"].is_set():
                state["started_at_monotonic"] = time.monotonic()
                state["started_event"].set()
                logger.info(
                    f"[OP] Event {event_id} scraping STARTED on browser worker "
                    f"at monotonic={state['started_at_monotonic']:.2f}"
                )

    def _on_event_scraped(event_id, op_data):
        if op_data:
            try:
                if op_data_cache is not None:
                    op_data_cache[event_id] = op_data

                saved = MarketRepository.save_markets_from_oddsportal(event_id, op_data)
                saved_counts[event_id] = saved
                logger.info(f"✅ OddsPortal: Saved {saved} markets/bookies for event {event_id}")
            except Exception as exc:
                logger.error(f"❌ OddsPortal: Error saving data for event {event_id}: {exc}")
                saved_counts[event_id] = None
        else:
            logger.warning(f"⚠️ OddsPortal: No data for event {event_id}")
            saved_counts[event_id] = None

        if op_event_states and event_id in op_event_states:
            state = op_event_states[event_id]
            state["done_at_monotonic"] = time.monotonic()
            state["done_event"].set()
            logger.info(f"🔔 OP: Signaled completion for event {event_id} - alert thread unblocked")

    num_browsers = Config.ODDSPORTAL_PARALLEL_BROWSERS
    logger.info(
        f"🌐 OddsPortal: Dispatching {len(op_tasks)} tasks with {num_browsers} browser(s) "
        f"(browser-per-worker, fresh-context-per-event)"
    )
    op_results = scrape_multiple_matches_parallel_sync(
        op_tasks,
        num_browsers=num_browsers,
        debug_dir="oddsportal_debug",
        on_task_started=_on_event_started,
        on_result=_on_event_scraped,
        current_date=op_current_date,
    )
    logger.info(f"🌐 OddsPortal: Tiered Orchestrator returned {len(op_results)} results")

    return saved_counts


# Backward-compatible aliases for the earlier refactor names.
build_oddsportal_candidates = build_oddsportal_scrape_candidates
create_oddsportal_tracking_state = create_oddsportal_scrape_state
launch_oddsportal_scraper_worker = start_oddsportal_scrape_thread
run_oddsportal_scrape_worker = run_oddsportal_scrape_cycle
run_oddsportal_scrape_batch = scrape_oddsportal_batch


__all__ = [
    "build_oddsportal_scrape_candidates",
    "create_oddsportal_scrape_state",
    "start_oddsportal_scrape_thread",
    "run_oddsportal_scrape_cycle",
    "scrape_oddsportal_batch",
    "build_oddsportal_candidates",
    "create_oddsportal_tracking_state",
    "launch_oddsportal_scraper_worker",
    "run_oddsportal_scrape_worker",
    "run_oddsportal_scrape_batch",
]
