"""Production odds phase used by the development simulator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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
    events,
    simulated_minutes: int,
    key_moments: list[int],
    *,
    debug_mode: bool,
    show_persistence_report: bool,
    log_persisted_market_odds: Callable[[int, set[int], dict], None],
    scheduler=None,
    enable_sofascore: bool = True,
    enable_oddspapi: bool = True,
    oddspapi_available_through_utc: datetime | dict[int, datetime | None] | None = None,
) -> SimulatedOddsOutcome:
    """Build the production candidate plan and run both provider processors for one or more events."""
    if isinstance(events, (list, tuple)):
        event_objs = list(events)
    else:
        event_objs = [events]

    if not event_objs:
        raise ValueError("At least one event must be provided.")

    if scheduler is None:
        scheduler = SimpleNamespace(
            event_repo=EventRepository(),
            recently_rescheduled=set(),
        )

    events_data = [
        EventRepository._build_event_data_with_legacy_fallback(obj)
        for obj in event_objs
    ]
    source_states = load_pre_start_odds_source_states(events_data)
    for event_data in events_data:
        e_id = int(event_data["id"])
        sofascore_state = source_states.get(e_id, {}).get(SOFASCORE_SOURCE)
        oddspapi_state = source_states.get(e_id, {}).get(ODDSPAPI_SOURCE)
        logger.info(
            "  Event %s odds availability before run: sofascore=%s oddspapi=%s",
            e_id,
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
    include_all_tennis = getattr(
        Config, "TIMESTAMP_CORRECTIONS_INCLUDE_ALL_TENNIS", True
    )
    timings = {int(e["id"]): simulated_minutes for e in events_data}

    if restrict_timestamp_corrections:
        def _is_ts_candidate(e: dict) -> bool:
            if tracked_ids is not None and e.get("competition_id") in tracked_ids:
                return True
            if include_all_tennis:
                sport = e.get("sport") or (e.get("event_data") or {}).get("sport")
                if sport in ["Tennis", "Tennis Doubles"] and timings.get(int(e.get("id"))) == 5:
                    return True
            return False

        ts_events = [e for e in events_data if _is_ts_candidate(e)]
        no_ts_events = [e for e in events_data if not _is_ts_candidate(e)]
    else:
        ts_events = events_data
        no_ts_events = []

    general_odds_extraction_competition_ids = (
        tracked_ids if restrict_general_odds_extraction else None
    )

    def _safe_build_candidates(evts: list[dict], **kwargs) -> PreStartEventPlan:
        try:
            return build_pre_start_event_candidates(
                scheduler,
                evts,
                timings,
                source_states,
                **kwargs,
            )
        except TypeError:
            return build_pre_start_event_candidates(
                scheduler,
                evts,
                timings,
                source_states,
            )

    plan_ts = (
        _safe_build_candidates(
            ts_events,
            key_moments=key_moments,
            timestamp_correction_enabled=global_ts_correction,
            fetch_alert_metadata=True,
            general_odds_extraction_competition_ids=general_odds_extraction_competition_ids,
        )
        if ts_events
        else PreStartEventPlan(candidates=[], by_event_id={})
    )

    if no_ts_events:
        plan_no_ts = _safe_build_candidates(
            no_ts_events,
            key_moments=key_moments,
            timestamp_correction_enabled=False,
            fetch_alert_metadata=False,
            general_odds_extraction_competition_ids=general_odds_extraction_competition_ids,
        )
        event_plan = PreStartEventPlan(
            candidates=plan_ts.candidates + plan_no_ts.candidates,
            by_event_id={**plan_ts.by_event_id, **plan_no_ts.by_event_id},
        )
    else:
        event_plan = plan_ts

    for event_obj_item in event_objs:
        e_id = int(event_obj_item.id)
        if e_id not in event_plan.by_event_id:
            logger.warning(
                "  Production candidate builder excluded event %s; provider and "
                "alert evaluation will receive no candidate",
                e_id,
            )

    # Attach existing tennis observations and persist initial snapshot observations
    attach_stored_observations(event_plan.candidates)
    persist_snapshot_observations(event_plan.candidates)

    previous_snapshot_ids_by_event: dict[int, set[int]] = {}
    if show_persistence_report:
        with db_manager.get_session() as session:
            for event_info in event_plan.candidates:
                if event_info.get("should_extract_odds"):
                    e_id = event_info["event_id"]
                    previous_snapshot_ids_by_event[e_id] = {
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
                            .filter(Market.event_id == e_id)
                            .all()
                        )
                    }

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
        for event_obj_item in event_objs:
            e_id = int(event_obj_item.id)
            event_info = event_plan.by_event_id.get(e_id)
            if not event_info:
                continue
            odds_response = event_info.get("odds_response")
            ingestion_result = event_info.get("ingestion_result")
            if odds_response:
                logger.info("  Event %s: SofaScore odds fetched successfully", e_id)
                if ingestion_result is not None:
                    logger.info(
                        "  Event %s ingestion result: markets_saved=%s, "
                        "dual_process_available=%s, reason=%s",
                        e_id,
                        ingestion_result.markets_saved,
                        ingestion_result.dual_process_market_available,
                        ingestion_result.reason,
                    )
                if show_persistence_report:
                    adapted_response = SofaScoreMarketAdapter.from_event_odds_response(
                        odds_response,
                        home_team=event_obj_item.home_team,
                        away_team=event_obj_item.away_team,
                    )
                    log_persisted_market_odds(
                        e_id,
                        previous_snapshot_ids_by_event.get(e_id, set()),
                        adapted_response,
                    )
            elif event_info.get("should_extract_odds"):
                logger.warning(
                    "  Event %s: No SofaScore odds response returned or endpoint is unavailable",
                    e_id,
                )
            else:
                logger.info(
                    "  Event %s: Not a key moment; skipping odds extraction "
                    "(minutes=%s not in %s)",
                    e_id,
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
            available_through_utc=oddspapi_available_through_utc,
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

    refreshed_states = load_pre_start_odds_source_states(events_data)
    for event_data in events_data:
        e_id = int(event_data["id"])
        refreshed_sofascore = refreshed_states.get(e_id, {}).get(SOFASCORE_SOURCE)
        refreshed_oddspapi = refreshed_states.get(e_id, {}).get(ODDSPAPI_SOURCE)
        logger.info(
            "  Event %s odds availability after run: sofascore=%s oddspapi=%s",
            e_id,
            refreshed_sofascore.has_odds if refreshed_sofascore else "<missing mapping>",
            refreshed_oddspapi.has_odds if refreshed_oddspapi else "<missing mapping>",
        )

    first_event_info = event_plan.by_event_id.get(int(event_objs[0].id))
    return SimulatedOddsOutcome(
        odds_response=first_event_info.get("odds_response") if first_event_info else None,
        metadata_snapshot=(
            first_event_info.get("metadata_snapshot") if first_event_info else None
        ),
        observations=first_event_info.get("observations") if first_event_info else None,
        event_plan=event_plan,
    )
