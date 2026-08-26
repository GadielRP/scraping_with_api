"""Production odds phase used by the single-event development simulator."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from types import SimpleNamespace
from typing import Callable

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)
from infrastructure.persistence.repositories import EventRepository
from infrastructure.settings import Config
from modules.competition.tracked_competitions import tracked_competition_ids
from modules.jobs.pre_start_check_job.event_candidate_builder import (
    PreStartEventPlan,
    build_pre_start_event_candidates,
)
from modules.jobs.pre_start_check_job.odds_source_state import (
    ODDSPAPI_SOURCE,
    SOFASCORE_SOURCE,
    load_pre_start_odds_source_states,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.odds_phase import (
    run_oddspapi_pre_start_odds,
)
from modules.jobs.pre_start_check_job.providers.sofascore.odds_phase import (
    run_sofascore_pre_start_odds,
)
from modules.jobs.pre_start_check_job.providers.sofascore.tennis_observations import (
    attach_stored_observations,
    persist_snapshot_observations,
)
from modules.odds_ingestion.adapters.sofascore_market_adapter import (
    SofaScoreMarketAdapter,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SimulatedOddsOutcome:
    odds_response: dict | None
    metadata_snapshot: dict | None
    observations: dict | None
    event_plan: PreStartEventPlan


def run_production_odds_phase(
    event_obj,
    simulated_minutes: int,
    key_moments: list[int],
    *,
    debug_mode: bool,
    show_persistence_report: bool,
    log_persisted_market_odds: Callable[[int, set[int], dict], None],
    scheduler=None,
    enable_sofascore: bool = True,
    enable_oddspapi: bool = True,
) -> SimulatedOddsOutcome:
    """Build the production candidate plan and run both provider processors."""
    event_id = int(event_obj.id)
    if scheduler is None:
        scheduler = SimpleNamespace(
            event_repo=EventRepository(),
            recently_rescheduled=set(),
        )

    event_data = EventRepository._build_event_data_with_legacy_fallback(event_obj)
    source_states = load_pre_start_odds_source_states([event_data])
    sofascore_state = source_states.get(event_id, {}).get(SOFASCORE_SOURCE)
    oddspapi_state = source_states.get(event_id, {}).get(ODDSPAPI_SOURCE)
    logger.info(
        "  Odds availability before run: sofascore=%s oddspapi=%s",
        sofascore_state.has_odds if sofascore_state else "<missing mapping>",
        oddspapi_state.has_odds if oddspapi_state else "<missing mapping>",
    )

    global_ts_correction = Config.ENABLE_TIMESTAMP_CORRECTION
    restrict_timestamp_corrections = (
        global_ts_correction
        and Config.TIMESTAMP_CORRECTIONS_TRACKED_COMPETITIONS_ONLY
    )
    restrict_general_odds_extraction = (
        Config.ODDS_EXTRACTION_GENERAL_TRACKED_COMPETITIONS_ONLY
    )
    restrict_sofascore_odds_extraction = (
        Config.ODDS_EXTRACTION_SOFASCORE_TRACKED_COMPETITIONS_ONLY
    )
    restrict_oddspapi_odds_extraction = (
        Config.ODDS_EXTRACTION_ODDSPAPI_TRACKED_COMPETITIONS_ONLY
    )

    tracked_ids = (
        set(tracked_competition_ids())
        if (
            restrict_timestamp_corrections
            or restrict_general_odds_extraction
            or restrict_sofascore_odds_extraction
            or restrict_oddspapi_odds_extraction
        )
        else None
    )

    event_competition_id = event_data.get("competition_id")
    is_tracked = tracked_ids is not None and event_competition_id in tracked_ids

    ts_correction_enabled = global_ts_correction
    if restrict_timestamp_corrections and not is_tracked:
        ts_correction_enabled = False

    general_odds_extraction_competition_ids = (
        tracked_ids if restrict_general_odds_extraction else None
    )

    event_plan = build_pre_start_event_candidates(
        scheduler,
        [event_data],
        {event_id: simulated_minutes},
        source_states,
        key_moments=key_moments,
        timestamp_correction_enabled=ts_correction_enabled,
        general_odds_extraction_competition_ids=general_odds_extraction_competition_ids,
    )
    event_info = event_plan.by_event_id.get(event_id)
    if event_info is None:
        logger.warning(
            "  Production candidate builder excluded event %s; provider and "
            "alert evaluation will receive no candidate",
            event_id,
        )

    # Attach existing tennis observations and persist initial snapshot observations
    attach_stored_observations(event_plan.candidates)
    persist_snapshot_observations(event_plan.candidates)

    previous_snapshot_ids: set[int] = set()
    if (
        event_info is not None
        and event_info.get("should_extract_odds")
        and show_persistence_report
    ):
        with db_manager.get_session() as session:
            previous_snapshot_ids = {
                snapshot_id
                for (snapshot_id,) in (
                    session.query(MarketChoiceSnapshot.snapshot_id)
                    .join(
                        MarketChoiceQuote,
                        MarketChoiceSnapshot.quote_id == MarketChoiceQuote.quote_id,
                    )
                    .join(
                        MarketChoice,
                        MarketChoiceQuote.choice_id == MarketChoice.choice_id,
                    )
                    .join(Market, MarketChoice.market_id == Market.market_id)
                    .filter(Market.event_id == event_id)
                    .all()
                )
            }

    odds_response = None
    if enable_sofascore:
        logger.info("  Running production SofaScore odds processor...")
        run_sofascore_pre_start_odds(
            event_plan.candidates,
            source_states,
            debug_mode=show_persistence_report,
            tracked_competition_ids=(
                tracked_ids if restrict_sofascore_odds_extraction else None
            ),
        )
        odds_response = event_info.get("odds_response") if event_info else None
        ingestion_result = event_info.get("ingestion_result") if event_info else None
        if odds_response:
            logger.info("  SofaScore odds fetched successfully")
            if ingestion_result is not None:
                logger.info(
                    "  Ingestion result: markets_saved=%s, "
                    "dual_process_available=%s, reason=%s",
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
                log_persisted_market_odds(
                    event_id,
                    previous_snapshot_ids,
                    adapted_response,
                )
        elif event_info and event_info.get("should_extract_odds"):
            logger.warning(
                "  No SofaScore odds response returned or endpoint is unavailable"
            )
        elif event_info:
            logger.info(
                "  Not a key moment; skipping odds extraction "
                "(minutes=%s not in %s)",
                simulated_minutes,
                key_moments,
            )
    else:
        logger.info("  SofaScore odds processor skipped (disabled by toggle)")

    if enable_oddspapi:
        logger.info("  Running production Oddspapi odds processor...")
        oddspapi_summary = run_oddspapi_pre_start_odds(
            event_plan.candidates,
            source_states,
            debug_mode=debug_mode,
            tracked_competition_ids=(
                tracked_ids if restrict_oddspapi_odds_extraction else None
            ),
        )
        logger.info(
            "  Oddspapi result: requests=%s ingested=%s skipped=%s failed=%s",
            oddspapi_summary.requests_attempted,
            oddspapi_summary.events_ingested,
            oddspapi_summary.events_skipped,
            oddspapi_summary.events_failed,
        )
    else:
        logger.info("  Oddspapi odds processor skipped (disabled by toggle)")

    refreshed_states = load_pre_start_odds_source_states([event_data])
    refreshed_sofascore = refreshed_states.get(event_id, {}).get(SOFASCORE_SOURCE)
    refreshed_oddspapi = refreshed_states.get(event_id, {}).get(ODDSPAPI_SOURCE)
    logger.info(
        "  Odds availability after run: sofascore=%s oddspapi=%s",
        refreshed_sofascore.has_odds if refreshed_sofascore else "<missing mapping>",
        refreshed_oddspapi.has_odds if refreshed_oddspapi else "<missing mapping>",
    )
    return SimulatedOddsOutcome(
        odds_response=odds_response,
        metadata_snapshot=(
            event_info.get("metadata_snapshot") if event_info else None
        ),
        observations=event_info.get("observations") if event_info else None,
        event_plan=event_plan,
    )

