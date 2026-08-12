"""OddsPortal worker helpers for the pre-start job."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import threading
import time
import traceback
from typing import Any, Dict, List, Optional

from infrastructure.settings import Config
from modules.odds_ingestion import MarketOddsIngestionService
from modules.oddsportal import scrape_multiple_matches_parallel_sync
from modules.oddsportal.oddsportal_config import (
    ODDSPORTAL_COMPETITION_ROUTES,
    get_current_date,
)
from modules.oddsportal.scraping_settings import ODDSPORTAL_SCRAPING_SETTINGS

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OddsPortalScrapeContext:
    """Shared state between the background scraper and evaluation pipelines."""

    event_states: Dict[int, Dict[str, Any]]
    event_ids: set[int]
    data_cache: Dict[int, Any]


def start_oddsportal_scrape_for_events(
    scheduler,
    upcoming_events: List[Dict],
    pre_calculated_timings: Dict[int, int],
    *,
    debug_mode: bool = False,
) -> OddsPortalScrapeContext:
    """Prepare and start one OddsPortal cycle for the upcoming event batch."""
    candidates = build_oddsportal_scrape_candidates(
        upcoming_events,
        pre_calculated_timings,
    )
    event_states = create_oddsportal_scrape_state(candidates) if candidates else {}
    context = OddsPortalScrapeContext(
        event_states=event_states,
        event_ids=set(event_states),
        data_cache={},
    )
    start_oddsportal_scrape_thread(
        scheduler,
        candidates,
        context.event_states,
        context.data_cache,
        debug_mode=debug_mode,
    )
    return context


def build_oddsportal_scrape_candidates(
    upcoming_events: List[Dict],
    pre_calculated_timings: Dict[int, int],
) -> List[Dict]:
    """Collect events for the temporary OddsPortal opening-odds capture."""
    if not Config.ODDSPORTAL_SCRAPING_ENABLED:
        logger.info("OddsPortal scraping is disabled by config; skipping candidate selection.")
        return []

    candidates: List[Dict] = []

    for event_dict in upcoming_events or []:
        competition_id = event_dict.get("competition_id")
        minutes_until_start = pre_calculated_timings.get(event_dict["id"])

        if (
            competition_id in ODDSPORTAL_COMPETITION_ROUTES
            # OddsPortal is intentionally scraped once at the configured early
            # moment. Persistence treats it as an opening-only provider; later
            # current timeframes remain exclusively owned by OddsPAPI.
            and minutes_until_start == Config.ODDSPORTAL_OPENING_CAPTURE_MINUTES
        ):
            candidates.append(
                {
                    "event_id": event_dict["id"],
                    "event_data": event_dict,
                    "minutes_until_start": minutes_until_start,
                    "should_extract_odds": True,
                }
            )

    if candidates:
        logger.info(
            "OddsPortal opening capture produced %s routable events at %sm.",
            len(candidates),
            Config.ODDSPORTAL_OPENING_CAPTURE_MINUTES,
        )
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
    *,
    debug_mode: bool = False,
):
    """Start OddsPortal scraping in the background if there is work to do."""
    if not Config.ODDSPORTAL_SCRAPING_ENABLED or not op_candidates:
        scheduler._active_op_thread = None
        return None

    def _orchestrate(
        previous_thread,
        candidates,
        event_states,
        data_cache,
        oddsportal_debug_mode,
    ):
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
        run_oddsportal_scrape_cycle(
            candidates,
            event_states,
            data_cache,
            debug_mode=oddsportal_debug_mode,
        )

    previous_thread = getattr(scheduler, "_active_op_thread", None)
    oddsportal_thread = threading.Thread(
        target=_orchestrate,
        args=(
            previous_thread,
            op_candidates,
            op_event_states,
            op_data_cache,
            debug_mode,
        ),
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
    *,
    debug_mode: bool = False,
):
    """Run the OddsPortal scrape worker and guarantee event-state cleanup."""
    logger.info(f"🔥 OP Worker started: scraping {len(op_candidates)} tracked-league events.")
    try:
        scrape_oddsportal_batch(
            op_candidates,
            op_event_states,
            op_data_cache,
            debug_mode=debug_mode,
        )
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
    *,
    debug_mode: bool = False,
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
        competition_id = event_data.get("competition_id")
        op_info = ODDSPORTAL_COMPETITION_ROUTES.get(competition_id)

        if op_info and event_info.get("should_extract_odds"):
            league_url = f"https://www.{ODDSPORTAL_SCRAPING_SETTINGS.domain}/{op_info['sport']}/{op_info['country']}/{op_info['league']}/"
            op_tasks.append(
                {
                    "event_id": event_data["id"],
                    "league_url": league_url,
                    "home_team": event_data["home_team"],
                    "away_team": event_data["away_team"],
                    "season_id": season_id,
                    "competition_id": competition_id,
                    "sport": op_info["sport"],
                    "start_time_utc": event_data.get("start_time_utc"),
                    "_oddsportal_resume_state": None,
                    "_oddsportal_partial_match_data": None,
                }
            )

    if not op_tasks:
        logger.info("ℹ️ OddsPortal: No eligible events to scrape")
        return {}

    logger.info(f"🔍 OddsPortal worker: {len(op_tasks)} events eligible for scraping")
    if debug_mode:
        logger.info(
            "OddsPortal tooltip debug capture enabled: "
            "./debug/oddsportal_{event_id}_tooltips/"
        )
    saved_counts: Dict[int, Optional[int]] = {}
    reference_data = None
    reference_data_lock = threading.Lock()

    def _get_ingestion_reference_data():
        nonlocal reference_data
        if reference_data is not None:
            return reference_data
        with reference_data_lock:
            if reference_data is None:
                source_bookies = [
                    (name, _source_bookie_slug(name))
                    for name in (
                        ODDSPORTAL_SCRAPING_SETTINGS.bookmakers.hover_names or ()
                    )
                ]
                if ODDSPORTAL_SCRAPING_SETTINGS.bookmakers.persist_betfair:
                    source_bookies.append(("Betfair Exchange", "betfair-ex"))
                reference_data = (
                    MarketOddsIngestionService.load_oddsportal_reference_data(
                        source_bookies
                    )
                )
                if reference_data.unresolved_bookie_slugs:
                    logger.warning(
                        "OddsPortal reference data has unresolved bookies: %s",
                        reference_data.unresolved_bookie_slugs,
                    )
        return reference_data

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
                ingestion_result = MarketOddsIngestionService.save_from_oddsportal_data(
                    event_id,
                    op_data,
                    reference_data=_get_ingestion_reference_data(),
                )
                saved = ingestion_result.markets_saved
                saved_counts[event_id] = saved
                if saved > 0 and op_data_cache is not None:
                    op_data_cache[event_id] = op_data
                logger.info(
                    "OddsPortal canonical ingestion event=%s markets=%s choices=%s "
                    "snapshots=%s skipped=%s reason=%s",
                    event_id,
                    ingestion_result.markets_saved,
                    ingestion_result.choices_saved,
                    ingestion_result.snapshots_saved,
                    ingestion_result.skipped,
                    ingestion_result.reason,
                )
                logger.info(f"💾 OddsPortal: Saved {saved} markets/bookies for event {event_id}")
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
        "OddsPortal bookmaker policy: regular hover+persistence=%s limit=%s; "
        "betfair persist=%s hover=%s",
        ODDSPORTAL_SCRAPING_SETTINGS.bookmakers.hover_names or "disabled",
        ODDSPORTAL_SCRAPING_SETTINGS.bookmakers.hover_limit,
        ODDSPORTAL_SCRAPING_SETTINGS.bookmakers.persist_betfair,
        ODDSPORTAL_SCRAPING_SETTINGS.bookmakers.hover_betfair,
    )
    logger.info(
        f"🌐 OddsPortal: Dispatching {len(op_tasks)} tasks with {num_browsers} browser(s) "
        f"(browser-per-worker, fresh-context-per-event)"
    )
    op_results = scrape_multiple_matches_parallel_sync(
        op_tasks,
        num_browsers=num_browsers,
        debug_dir="logs/debug/oddsportal" if debug_mode else None,
        debug_mode=debug_mode,
        on_task_started=_on_event_started,
        on_result=_on_event_scraped,
        current_date=op_current_date,
        collect_results=False,
    )
    logger.info(f"🌐 OddsPortal: Tiered Orchestrator returned {len(op_results)} results")

    return saved_counts


def _source_bookie_slug(name: str) -> str:
    """Build the stable source slug used by canonical bookie mappings."""
    import re

    normalized = str(name or "").strip().lower().replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")


# Backward-compatible aliases for the earlier refactor names.
build_oddsportal_candidates = build_oddsportal_scrape_candidates
create_oddsportal_tracking_state = create_oddsportal_scrape_state
launch_oddsportal_scraper_worker = start_oddsportal_scrape_thread
run_oddsportal_scrape_worker = run_oddsportal_scrape_cycle
run_oddsportal_scrape_batch = scrape_oddsportal_batch


__all__ = [
    "OddsPortalScrapeContext",
    "start_oddsportal_scrape_for_events",
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
