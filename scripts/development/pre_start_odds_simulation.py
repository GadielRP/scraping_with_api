"""Production odds phase used by the single-event development simulator."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Callable

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Market, MarketChoice, MarketChoiceSnapshot
from modules.jobs.oddspapi.pre_start_odds.pre_start_odds_job import (
    run_oddspapi_pre_start_odds_ingestion,
)
from modules.jobs.pre_start_check_job.odds_source_state import (
    ODDSPAPI_SOURCE,
    SOFASCORE_SOURCE,
    get_numeric_source_event_id,
    load_pre_start_odds_source_states,
)
from modules.jobs.pre_start_check_job.sofascore_odds_processor import (
    process_sofascore_pre_start_odds,
)
from modules.jobs.pre_start_check_job.timing import should_extract_odds_for_event
from modules.odds_ingestion.adapters.sofascore_market_adapter import SofaScoreMarketAdapter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SimulatedOddsOutcome:
    odds_response: dict | None
    metadata_snapshot: dict | None
    observations: dict | None


def run_production_odds_phase(
    event_obj,
    simulated_minutes: int,
    key_moments: list[int],
    *,
    debug_mode: bool,
    show_persistence_report: bool,
    log_persisted_market_odds: Callable[[int, set[int], dict], None],
) -> SimulatedOddsOutcome:
    """Run the same provider processors used by the production orchestrator."""
    event_id = int(event_obj.id)
    source_states = load_pre_start_odds_source_states([{"id": event_id}])
    sofascore_state = source_states.get(event_id, {}).get(SOFASCORE_SOURCE)
    oddspapi_state = source_states.get(event_id, {}).get(ODDSPAPI_SOURCE)
    sofascore_event_id = get_numeric_source_event_id(
        source_states,
        event_id,
        SOFASCORE_SOURCE,
    )
    logger.info("  SofaScore event ID: %s", sofascore_event_id)
    logger.info(
        "  Odds availability before run: sofascore=%s oddspapi=%s",
        sofascore_state.has_odds if sofascore_state else "<missing mapping>",
        oddspapi_state.has_odds if oddspapi_state else "<missing mapping>",
    )

    should_extract, metadata_snapshot, timing_changed, sofascore_event_id = (
        should_extract_odds_for_event(
            event_id,
            simulated_minutes,
            event_obj.start_time_utc,
            sofascore_event_id=sofascore_event_id,
        )
    )
    if timing_changed:
        logger.info(
            "  ℹ️ Timing changed for event %s during simulation; using updated metadata snapshot flow",
            event_id,
        )

    event_info = {
        "event_id": event_id,
        "event_data": {
            "id": event_id,
            "slug": event_obj.slug,
            "sport": event_obj.sport,
            "home_team": event_obj.home_team,
            "away_team": event_obj.away_team,
            "start_time_utc": event_obj.start_time_utc,
            "season_id": event_obj.season_id,
        },
        "minutes_until_start": simulated_minutes,
        "should_extract_odds": should_extract,
        "original_start_time": event_obj.start_time_utc,
        "metadata_snapshot": metadata_snapshot,
        "sofascore_event_id": sofascore_event_id,
    }

    previous_snapshot_ids: set[int] = set()
    if should_extract and show_persistence_report:
        with db_manager.get_session() as session:
            previous_snapshot_ids = {
                snapshot_id
                for (snapshot_id,) in (
                    session.query(MarketChoiceSnapshot.snapshot_id)
                    .join(MarketChoice, MarketChoiceSnapshot.choice_id == MarketChoice.choice_id)
                    .join(Market, MarketChoice.market_id == Market.market_id)
                    .filter(Market.event_id == event_id)
                    .all()
                )
            }

    logger.info("  Running production SofaScore odds processor...")
    process_sofascore_pre_start_odds(
        [event_info],
        source_states,
        debug_mode=show_persistence_report,
    )
    odds_response = event_info.get("odds_response")
    ingestion_result = event_info.get("ingestion_result")
    if odds_response:
        logger.info("  ✅ SofaScore odds fetched successfully")
        if ingestion_result is not None:
            logger.info(
                "  📊 Ingestion result: markets_saved=%s, dual_process_available=%s, reason=%s",
                ingestion_result.markets_saved,
                ingestion_result.dual_process_market_available,
                ingestion_result.reason,
            )
        if show_persistence_report:
            adapted_response = SofaScoreMarketAdapter.from_event_odds_response(
                odds_response,
                home_team=event_obj.home_team,
                away_team=event_obj.away_team,
            )
            log_persisted_market_odds(event_id, previous_snapshot_ids, adapted_response)
    elif should_extract:
        logger.warning("  ⚠️ No SofaScore odds response returned or endpoint is unavailable")
    else:
        logger.info(
            "  ⏭️ NOT a key moment — skipping odds extraction (minutes=%s not in %s)",
            simulated_minutes,
            key_moments,
        )

    logger.info("💰 Running production Oddspapi odds processor...")
    oddspapi_summary = run_oddspapi_pre_start_odds_ingestion(
        [event_info],
        debug_mode=debug_mode,
        source_states=source_states,
    )
    logger.info(
        "  Oddspapi result: requests=%s ingested=%s skipped=%s failed=%s",
        oddspapi_summary.requests_attempted,
        oddspapi_summary.events_ingested,
        oddspapi_summary.events_skipped,
        oddspapi_summary.events_failed,
    )

    refreshed_states = load_pre_start_odds_source_states([{"id": event_id}])
    refreshed_sofascore = refreshed_states.get(event_id, {}).get(SOFASCORE_SOURCE)
    refreshed_oddspapi = refreshed_states.get(event_id, {}).get(ODDSPAPI_SOURCE)
    logger.info(
        "  Odds availability after run: sofascore=%s oddspapi=%s",
        refreshed_sofascore.has_odds if refreshed_sofascore else "<missing mapping>",
        refreshed_oddspapi.has_odds if refreshed_oddspapi else "<missing mapping>",
    )
    return SimulatedOddsOutcome(
        odds_response=odds_response,
        metadata_snapshot=metadata_snapshot,
        observations=event_info.get("observations"),
    )
