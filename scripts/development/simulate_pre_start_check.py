"""Simulate the production pre-start check flow for one event.

The simulator forces one database event to the requested minutes-until-start,
then delegates candidate selection, provider ingestion, OddsPortal selection,
and key-moment evaluation to the same functions used by production.

Usage:
    python -m scripts.development.simulate_pre_start_check <event_id> <minutes>

Example:
    python -m scripts.development.simulate_pre_start_check 14083613 30
"""

from __future__ import annotations

import argparse
import logging
import sys

from sqlalchemy.orm import joinedload

from app.initialize import initialize_system
from app.logging_setup import setup_logging
from infrastructure.persistence.catalogs.canonical_market_types import (
    CANONICAL_MARKET_TYPE_SEEDS,
)
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Market, MarketChoice
from infrastructure.persistence.repositories import EventRepository
from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.key_moment_evaluation import (
    evaluate_pre_start_key_moments,
)
from modules.jobs.pre_start_check_job.oddsportal_worker import (
    start_oddsportal_scrape_for_events,
)
from modules.odds_ingestion.canonical_market_normalizer import (
    CanonicalMarketNormalizer,
)
from modules.competition.tracked_competitions import is_tracked_competition
from modules.sofascore import api_client
from scripts.development.pre_start_odds_simulation import (
    run_production_odds_phase,
)
from shared.runtime_observability import observe_operation

SHOW_MARKET_PERSISTENCE_REPORT = True

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logger = logging.getLogger(__name__)


class _SingleEventSimulationScheduler:
    """Small scheduler surface required by the production pre-start phases."""

    def __init__(self) -> None:
        self.event_repo = EventRepository()
        self.recently_rescheduled: set[int] = set()
        self._active_op_thread = None


def _ensure_logging_configured() -> None:
    if not any(
        isinstance(handler, logging.FileHandler)
        for handler in logging.getLogger().handlers
    ):
        setup_logging()


def _log_pipeline_eligibility(event_obj) -> bool:
    """Explain the production competition gates before costly simulation work."""
    competition_id = getattr(event_obj, "competition_id", None)
    tracked_competition = is_tracked_competition(competition_id)

    if (
        Config.PRE_START_TRACKED_COMPETITIONS_ONLY
        and not tracked_competition
    ):
        logger.warning(
            "PRODUCTION FLOW STOPS BEFORE INGESTION: competition_id=%s is "
            "not tracked and PRE_START_TRACKED_COMPETITIONS_ONLY=True.",
            competition_id,
        )
        return False

    if (
        Config.FILTER_PIPELINES_BY_TRACKED_COMPETITIONS
        and not tracked_competition
    ):
        logger.warning(
            "ALERT AND PILLAR PIPELINES WILL SKIP: competition_id=%s is "
            "not tracked and FILTER_PIPELINES_BY_TRACKED_COMPETITIONS=True. "
            "Provider odds can still be ingested because "
            "PRE_START_TRACKED_COMPETITIONS_ONLY=False.",
            competition_id,
        )
    else:
        logger.info(
            "Competition pipeline gate: eligible "
            "(competition_id=%s tracked=%s filter_enabled=%s)",
            competition_id,
            tracked_competition,
            Config.FILTER_PIPELINES_BY_TRACKED_COMPETITIONS,
        )
    return True


def _normalize_market_field(value) -> str:
    return str(value or "").strip().lower().replace("-", " ")


def _resolve_market_key_from_db_fields(market) -> str | None:
    db_name = _normalize_market_field(market.market_name)
    db_group = _normalize_market_field(market.market_group)
    db_period = _normalize_market_field(market.market_period)

    for key, seed in CANONICAL_MARKET_TYPE_SEEDS.items():
        if (
            db_name
            == _normalize_market_field(seed["canonical_market_name"])
            and db_group
            == _normalize_market_field(seed["canonical_market_group"])
            and db_period
            == _normalize_market_field(seed["canonical_market_period"])
        ):
            return key

    return CanonicalMarketNormalizer._resolve_sofascore_key(
        {
            "marketName": market.market_name,
            "marketGroup": market.market_group,
            "marketPeriod": market.market_period,
        }
    )


def _find_matching_raw_market(
    market,
    adapted_response: dict | None,
) -> dict | None:
    raw_markets = (adapted_response or {}).get("markets", [])
    for choice in market.choices:
        for snapshot in choice.snapshots:
            if not snapshot.source_outcome_id:
                continue
            for raw_market in raw_markets:
                if any(
                    raw_choice.get("sourceOutcomeId")
                    == snapshot.source_outcome_id
                    for raw_choice in raw_market.get("choices", [])
                ):
                    return raw_market
    return None


def _log_persisted_market_odds(
    event_id: int,
    previous_snapshot_ids: set[int],
    adapted_response: dict | None = None,
) -> None:
    """Report the exact market state persisted by the production processor."""
    with db_manager.get_session() as session:
        markets = (
            session.query(Market)
            .options(
                joinedload(Market.bookie),
                joinedload(Market.choices).joinedload(MarketChoice.snapshots),
            )
            .filter(Market.event_id == event_id)
            .order_by(Market.market_id)
            .all()
        )

        snapshots = [
            snapshot
            for market in markets
            for choice in market.choices
            for snapshot in choice.snapshots
        ]
        new_snapshot_ids = {
            snapshot.snapshot_id
            for snapshot in snapshots
            if snapshot.snapshot_id not in previous_snapshot_ids
        }
        logger.info("")
        logger.info("=" * 100)
        logger.info("DATABASE PERSISTENCE AFTER ODDS INGESTION")
        logger.info(
            "event_id=%s markets=%s choices=%s snapshots=%s "
            "new_snapshots_this_run=%s",
            event_id,
            len(markets),
            sum(len(market.choices) for market in markets),
            len(snapshots),
            len(new_snapshot_ids),
        )

        for market in markets:
            raw_market = _find_matching_raw_market(market, adapted_response)
            has_new_snapshot = any(
                snapshot.snapshot_id in new_snapshot_ids
                for choice in market.choices
                for snapshot in choice.snapshots
            )
            logger.info("-" * 100)
            logger.info(
                "%sMARKET id=%s key=%r bookie=%r live=%s "
                "name=%r group=%r period=%r choice_group=%r "
                "raw_name=%r",
                "NEW " if has_new_snapshot else "",
                market.market_id,
                _resolve_market_key_from_db_fields(market),
                market.bookie.name if market.bookie else None,
                market.is_live,
                market.market_name,
                market.market_group,
                market.market_period,
                market.choice_group,
                raw_market.get("marketName") if raw_market else None,
            )
            for choice in sorted(
                market.choices,
                key=lambda item: item.choice_id,
            ):
                logger.info(
                    "  CHOICE id=%s name=%r initial=%s current=%s change=%s",
                    choice.choice_id,
                    choice.choice_name,
                    choice.initial_odds,
                    choice.current_odds,
                    choice.change,
                )
                for snapshot in sorted(
                    choice.snapshots,
                    key=lambda item: item.snapshot_id,
                ):
                    logger.info(
                        "    %sSNAPSHOT id=%s odds=%s collected_at=%s "
                        "source=%r source_market_id=%r source_outcome_id=%r "
                        "main_line=%r limit=%r",
                        (
                            "NEW "
                            if snapshot.snapshot_id in new_snapshot_ids
                            else ""
                        ),
                        snapshot.snapshot_id,
                        snapshot.odds_value,
                        snapshot.collected_at,
                        snapshot.source,
                        snapshot.source_market_id,
                        snapshot.source_outcome_id,
                        snapshot.main_line,
                        snapshot.source_limit,
                    )
        logger.info("=" * 100)
        logger.info("END DATABASE PERSISTENCE REPORT")
        logger.info("=" * 100)


def simulate_pre_start_check(event_id: int, simulated_minutes: int) -> bool:
    """Run the single-event flow with production observability and debug mode."""
    _ensure_logging_configured()
    previous_evidence_mode = getattr(
        api_client,
        "challenge_evidence_enabled",
        None,
    )
    api_client.set_challenge_evidence_enabled(True)
    try:
        with observe_operation("pre_start_check"):
            return _run_pre_start_check_simulation(
                event_id,
                simulated_minutes,
            )
    finally:
        if previous_evidence_mode is not None:
            api_client.set_challenge_evidence_enabled(previous_evidence_mode)


def _run_pre_start_check_simulation(
    event_id: int,
    simulated_minutes: int,
) -> bool:
    key_moments = Config.PRE_START_ODDS_MOMENTS
    debug_mode = True
    scheduler = _SingleEventSimulationScheduler()

    logger.info("=" * 80)
    logger.info("PRE-START CHECK DEVELOPMENT SIMULATION")
    logger.info("=" * 80)
    logger.info("Event ID: %s", event_id)
    logger.info("Simulated minutes: %s", simulated_minutes)
    logger.info("Is key moment: %s", simulated_minutes in key_moments)
    logger.info("=" * 80)

    logger.info("Step 1: Loading event from database")
    event_obj = scheduler.event_repo.get_event_by_id(event_id)
    if event_obj is None:
        logger.error("Event %s not found in database; aborting.", event_id)
        return False

    logger.info(
        "Event loaded: %s vs %s | sport=%s season_id=%s start=%s",
        event_obj.home_team,
        event_obj.away_team,
        event_obj.sport,
        event_obj.season_id,
        event_obj.start_time_utc,
    )
    if not _log_pipeline_eligibility(event_obj):
        logger.info(
            "Simulation complete: production upcoming-event selection would "
            "exclude event %s.",
            event_id,
        )
        return True

    if event_obj.sport in Config.EXCLUDED_SPORTS:
        logger.warning(
            "Event sport %r is excluded from production alert/pillar "
            "evaluation; provider ingestion can still run.",
            event_obj.sport,
        )

    event_data = EventRepository._build_event_data_with_legacy_fallback(
        event_obj
    )
    logger.info("Step 2: Starting production OddsPortal candidate selection")
    oddsportal_context = start_oddsportal_scrape_for_events(
        scheduler,
        [event_data],
        {event_id: simulated_minutes},
    )

    logger.info(
        "Step 3: Running production candidate builder and provider ingestion"
    )
    odds_outcome = run_production_odds_phase(
        event_obj,
        simulated_minutes,
        key_moments,
        debug_mode=debug_mode,
        show_persistence_report=SHOW_MARKET_PERSISTENCE_REPORT,
        log_persisted_market_odds=_log_persisted_market_odds,
        scheduler=scheduler,
    )

    logger.info("Step 4: Running production key-moment evaluation")
    evaluate_pre_start_key_moments(
        scheduler,
        odds_outcome.event_plan,
        oddsportal_context,
        debug_mode=debug_mode,
    )

    logger.info("=" * 80)
    logger.info(
        "SIMULATION COMPLETE for event %s at %s minutes",
        event_id,
        simulated_minutes,
    )
    logger.info("=" * 80)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Simulate the production pre-start flow for one database event at "
            "a forced minutes-until-start value."
        ),
    )
    parser.add_argument(
        "event_id",
        type=int,
        help="Canonical event ID (must exist in the database).",
    )
    parser.add_argument(
        "minutes",
        type=int,
        help="Forced minutes until start (configured key moments are used).",
    )
    args = parser.parse_args()
    _ensure_logging_configured()

    if not initialize_system():
        logger.error(
            "Failed to initialize system; the pre-start simulation cannot run."
        )
        return 1

    return 0 if simulate_pre_start_check(args.event_id, args.minutes) else 1


if __name__ == "__main__":
    raise SystemExit(main())
