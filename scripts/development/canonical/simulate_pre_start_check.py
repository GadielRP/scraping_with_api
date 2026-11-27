"""Simulate the production pre-start check flow for one or more events.

The simulator supplies a forced minutes-until-start value for scheduling,
then delegates candidate selection, provider ingestion, OddsPortal selection,
and key-moment evaluation to the same functions used by production.

Usage:
    python -m scripts.development.simulate_pre_start_check <event_id> <minutes>
    python -m scripts.development.simulate_pre_start_check <event_id_1> <event_id_2> ... <minutes>
    python -m scripts.development.simulate_pre_start_check <event_ids> --minutes <minutes>

Examples:
    python -m scripts.development.simulate_pre_start_check 14083613 30
    python -m scripts.development.simulate_pre_start_check 14083613 14083614 30
    python -m scripts.development.simulate_pre_start_check 14083613,14083614 30
    python -m scripts.development.simulate_pre_start_check 14083613 14083614 --minutes 30
    python -m scripts.development.simulate_pre_start_check 14083613 -m 5 --no-odds-ingestion --pillar-5

All toggles accept --<flag> and --no-<flag>; see --help. This runs real
persistence and provider calls when enabled. Alerts can send real notifications.
Only scheduling minutes are simulated: evaluation_as_of remains the real UTC
input-capture time. Production moment and competition filters still apply.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta
import logging
import sys
from typing import Sequence

from sqlalchemy.orm import joinedload

from app.initialize import initialize_system
from app.logging_setup import setup_logging
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    Event,
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)
from infrastructure.persistence.repositories import EventRepository
from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.event_candidate_builder import (
    build_pre_start_event_candidates,
)
from modules.jobs.pre_start_check_job.key_moment_evaluation import (
    evaluate_pre_start_key_moments,
)
from modules.jobs.pre_start_check_job.odds_source_state import (
    load_pre_start_odds_source_states,
)
from modules.jobs.pre_start_check_job.oddsportal_worker import (
    OddsPortalScrapeContext,
    start_oddsportal_scrape_for_events,
)
from modules.jobs.pre_start_check_job.runtime import PreStartRuntime
from modules.sports.catalog import is_supported_sport_name
from modules.oddspapi.historical_odds_as_of import OddspapiHistoricalOddsAsOf
from modules.competition.tracked_competitions import is_tracked_competition
from modules.sofascore import api_client
from scripts.development.pre_start_odds_simulation import (
    run_production_odds_phase,
)
from shared.runtime_observability import observe_operation

# Simulation toggles - Pipeline flows
ENABLE_ODDS_INGESTION_SIMULATION = True
ENABLE_ALERT_PIPELINE = False
ENABLE_PILLAR_PIPELINE = True
SHOW_MARKET_PERSISTENCE_REPORT = False

# Simulation toggles - Providers (active when ENABLE_ODDS_INGESTION_SIMULATION = True)
ENABLE_SOFASCORE_ODDS_SIMULATION = False
ENABLE_ODDSPAPI_ODDS_SIMULATION = False
ENABLE_ODDSPORTAL_ODDS_SIMULATION = False

# Simulation toggles - Individual Pillars (active when ENABLE_PILLAR_PIPELINE = True)
ENABLE_PILLAR_1 = False  # Pillar 1 - Team Structure (Side & Totals: M1-M7)
ENABLE_PILLAR_2 = False  # Pillar 2 - Side Market Signal Profile
ENABLE_PILLAR_3 = False  # Pillar 3 - Over/Under Market Signal Profile
ENABLE_PILLAR_4 = False  # Pillar 4 - Temporal Market Drift
ENABLE_PILLAR_5 = False  # Pillar 5 - Exact Price Memory
ENABLE_CUSTOM_PILLAR_FILTER = (
    True  # Set True to override production pillar selection with the toggles above
)

# CLI overrides use the same switches as editing this file; omitted flags retain
# the values above. Keep one mapping for argument parsing and scoped restoration.
_TOGGLE_FLAGS = {
    "odds-ingestion": "ENABLE_ODDS_INGESTION_SIMULATION",
    "alerts": "ENABLE_ALERT_PIPELINE",
    "pillars": "ENABLE_PILLAR_PIPELINE",
    "market-report": "SHOW_MARKET_PERSISTENCE_REPORT",
    "sofascore": "ENABLE_SOFASCORE_ODDS_SIMULATION",
    "oddspapi": "ENABLE_ODDSPAPI_ODDS_SIMULATION",
    "oddsportal": "ENABLE_ODDSPORTAL_ODDS_SIMULATION",
    "custom-pillar-filter": "ENABLE_CUSTOM_PILLAR_FILTER",
    **{f"pillar-{i}": f"ENABLE_PILLAR_{i}" for i in range(1, 6)},
}

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logger = logging.getLogger(__name__)


@contextmanager
def _simulation_toggle_overrides(overrides: dict[str, bool] | None):
    """Keep CLI switches and provider gates local to this one-shot execution."""
    overrides = overrides or {}
    unknown = overrides.keys() - _TOGGLE_FLAGS.keys()
    if unknown:
        raise ValueError(f"Unknown simulation toggles: {sorted(unknown)}")
    previous_toggles = {
        _TOGGLE_FLAGS[flag]: globals()[_TOGGLE_FLAGS[flag]] for flag in overrides
    }
    previous_config = {
        name: getattr(Config, name)
        for name in ("ENABLE_ODDS_EXTRACTION", "ENABLE_ODDSPAPI_PRE_START_ODDS")
    }
    try:
        for flag, enabled in overrides.items():
            globals()[_TOGGLE_FLAGS[flag]] = enabled
        # These are additional production gates inside candidate/provider code.
        Config.ENABLE_ODDS_EXTRACTION = ENABLE_ODDS_INGESTION_SIMULATION
        Config.ENABLE_ODDSPAPI_PRE_START_ODDS = (
            ENABLE_ODDS_INGESTION_SIMULATION and ENABLE_ODDSPAPI_ODDS_SIMULATION
        )
        yield
    finally:
        globals().update(previous_toggles)
        for name, value in previous_config.items():
            setattr(Config, name, value)


def _ensure_logging_configured() -> None:
    if not any(
        isinstance(handler, logging.FileHandler)
        for handler in logging.getLogger().handlers
    ):
        setup_logging()


def _wait_for_oddsportal_worker(runtime) -> None:
    """Keep the one-shot simulator alive until its OddsPortal cycle completes."""
    oddsportal_worker = getattr(runtime, "oddsportal", None)
    worker_thread = getattr(oddsportal_worker, "active_thread", None)
    if worker_thread is None:
        return

    logger.info("Waiting for the OddsPortal scrape worker to finish...")
    worker_thread.join()
    logger.info("OddsPortal scrape worker finished.")


def _log_pipeline_eligibility(event_obj) -> bool:
    """Explain the production competition gates before costly simulation work."""
    competition_id = getattr(event_obj, "competition_id", None)
    tracked_competition = is_tracked_competition(competition_id)

    if Config.TRACKED_COMPETITIONS_ONLY and not tracked_competition:
        logger.info(
            "🚫 PRODUCTION FLOW STOPS BEFORE INGESTION: competition_id=%s is "
            "not tracked and TRACKED_COMPETITIONS_ONLY=True.",
            competition_id,
        )
        return False

    if (
        Config.ODDS_EXTRACTION_GENERAL_TRACKED_COMPETITIONS_ONLY
        and not tracked_competition
    ):
        logger.warning(
            "GENERAL PROVIDER ODDS EXTRACTION WILL SKIP: competition_id=%s is "
            "not tracked and ODDS_EXTRACTION_GENERAL_TRACKED_COMPETITIONS_ONLY=True.",
            competition_id,
        )

    if (
        Config.ODDS_EXTRACTION_SOFASCORE_TRACKED_COMPETITIONS_ONLY
        and not tracked_competition
    ):
        logger.warning(
            "SOFASCORE ODDS EXTRACTION WILL SKIP: competition_id=%s is "
            "not tracked and ODDS_EXTRACTION_SOFASCORE_TRACKED_COMPETITIONS_ONLY=True.",
            competition_id,
        )

    if (
        Config.ODDS_EXTRACTION_ODDSPAPI_TRACKED_COMPETITIONS_ONLY
        and not tracked_competition
    ):
        logger.warning(
            "ODDSPAPI ODDS EXTRACTION WILL SKIP: competition_id=%s is "
            "not tracked and ODDS_EXTRACTION_ODDSPAPI_TRACKED_COMPETITIONS_ONLY=True.",
            competition_id,
        )

    if Config.FILTER_PIPELINES_BY_TRACKED_COMPETITIONS and not tracked_competition:
        logger.warning(
            "ALERT AND PILLAR PIPELINES WILL SKIP: competition_id=%s is "
            "not tracked and FILTER_PIPELINES_BY_TRACKED_COMPETITIONS=True.",
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


def _load_snapshot_ids_by_event(event_ids: list[int]) -> dict[int, set[int]]:
    """Read just identities in one query, and only when the report is enabled."""
    with db_manager.get_session() as session:
        rows = (
            session.query(Market.event_id, MarketChoiceSnapshot.snapshot_id)
            .join(MarketChoice, MarketChoice.market_id == Market.market_id)
            .join(
                MarketChoiceQuote, MarketChoiceQuote.choice_id == MarketChoice.choice_id
            )
            .join(
                MarketChoiceSnapshot,
                MarketChoiceSnapshot.quote_id == MarketChoiceQuote.quote_id,
            )
            .filter(Market.event_id.in_(event_ids))
        )
        snapshots_by_event = {event_id: set() for event_id in event_ids}
        for event_id, snapshot_id in rows:
            snapshots_by_event[event_id].add(snapshot_id)
        return snapshots_by_event


def _log_persisted_market_odds(
    event_id: int,
    previous_snapshot_ids: set[int],
) -> None:
    """Report persisted markets from every source, including stored-only runs."""

    with db_manager.get_session() as session:
        markets = (
            session.query(Market)
            .options(
                joinedload(Market.bookie),
                joinedload(Market.canonical_market_type),
                joinedload(Market.choices)
                .joinedload(MarketChoice.quotes)
                .joinedload(MarketChoiceQuote.snapshots),
            )
            .filter(Market.event_id == event_id)
            .order_by(Market.market_id)
            .all()
        )

        snapshots = [
            snapshot
            for market in markets
            for choice in market.choices
            for quote in choice.quotes
            for snapshot in quote.snapshots
        ]
        new_snapshot_ids = {
            snapshot.snapshot_id
            for snapshot in snapshots
            if snapshot.snapshot_id not in previous_snapshot_ids
        }
        logger.info("")
        logger.info("=" * 100)
        logger.info("DATABASE MARKET PERSISTENCE REPORT")
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
            has_new_snapshot = any(
                snapshot.snapshot_id in new_snapshot_ids
                for choice in market.choices
                for quote in choice.quotes
                for snapshot in quote.snapshots
            )
            logger.info("-" * 100)
            logger.info(
                "%sMARKET id=%s key=%r bookie=%r live=%s "
                "name=%r group=%r period=%r line_value=%r",
                "NEW " if has_new_snapshot else "",
                market.market_id,
                (
                    market.canonical_market_type.canonical_market_key
                    if market.canonical_market_type
                    else None
                ),
                market.bookie.name if market.bookie else None,
                market.is_live,
                (
                    market.canonical_market_type.canonical_market_name
                    if market.canonical_market_type
                    else None
                ),
                (
                    market.canonical_market_type.canonical_market_group
                    if market.canonical_market_type
                    else None
                ),
                (
                    market.canonical_market_type.canonical_market_period
                    if market.canonical_market_type
                    else None
                ),
                market.line_value,
            )
            for choice in sorted(
                market.choices,
                key=lambda item: item.choice_id,
            ):
                logger.info(
                    "  CHOICE id=%s name=%r quotes=%s",
                    choice.choice_id,
                    choice.choice_name,
                    len(choice.quotes),
                )
                for quote in sorted(choice.quotes, key=lambda item: item.quote_id):
                    logger.info(
                        "    QUOTE id=%s source=%r side=%r level=%s "
                        "initial=%s current=%s movement=%s "
                        "source_market_id=%r source_outcome_id=%r main_line=%r",
                        quote.quote_id,
                        quote.source,
                        quote.exchange_side,
                        quote.exchange_level,
                        quote.initial_odds,
                        quote.current_odds,
                        quote.movement,
                        quote.source_market_id,
                        quote.source_outcome_id,
                        quote.main_line,
                    )
                    for snapshot in sorted(
                        quote.snapshots,
                        key=lambda item: item.snapshot_id,
                    ):
                        logger.info(
                            "      %sSNAPSHOT id=%s odds=%s collected_at=%s "
                            "source_collected_at=%s "
                            "quote_id=%s limit=%r",
                            (
                                "NEW "
                                if snapshot.snapshot_id in new_snapshot_ids
                                else ""
                            ),
                            snapshot.snapshot_id,
                            snapshot.odds_value,
                            snapshot.collected_at,
                            snapshot.source_collected_at,
                            snapshot.quote_id,
                            snapshot.source_limit,
                        )
        logger.info("=" * 100)
        logger.info("END DATABASE PERSISTENCE REPORT")
        logger.info("=" * 100)


def _parse_event_ids_and_minutes(
    positional_args: list[str],
    minutes_flag: int | None,
) -> tuple[list[int], int]:
    """Parse one or more event IDs and minutes from CLI arguments."""
    if minutes_flag is not None:
        simulated_minutes = minutes_flag
        raw_id_tokens = positional_args
    else:
        if len(positional_args) < 2:
            raise ValueError(
                "When --minutes is omitted, you must provide at least one event ID "
                "and the forced minutes as the final argument (e.g. <event_id> <minutes>)."
            )
        try:
            simulated_minutes = int(positional_args[-1])
        except ValueError:
            raise ValueError(
                f"Could not parse minutes from final argument '{positional_args[-1]}'. "
                "Use --minutes/-m or provide an integer as the final argument."
            )
        raw_id_tokens = positional_args[:-1]

    event_ids: list[int] = []
    for token in raw_id_tokens:
        for part in str(token).split(","):
            part = part.strip()
            if part:
                try:
                    event_ids.append(int(part))
                except ValueError:
                    raise ValueError(f"Invalid event ID: '{part}'")

    if not event_ids:
        raise ValueError("At least one event ID must be provided.")

    return event_ids, simulated_minutes


def simulate_pre_start_check(
    event_ids: int | Sequence[int],
    simulated_minutes: int,
    *,
    toggle_overrides: dict[str, bool] | None = None,
) -> bool:
    """Run the pre-start flow for one or more events with production observability and debug mode."""
    _ensure_logging_configured()
    previous_evidence_mode = getattr(
        api_client,
        "challenge_evidence_enabled",
        None,
    )
    api_client.set_challenge_evidence_enabled(True)
    try:
        with _simulation_toggle_overrides(toggle_overrides), observe_operation(
            "pre_start_check"
        ):
            return _run_pre_start_check_simulation(
                event_ids,
                simulated_minutes,
            )
    finally:
        if previous_evidence_mode is not None:
            api_client.set_challenge_evidence_enabled(previous_evidence_mode)


def _run_pre_start_check_simulation(
    event_ids: int | Sequence[int],
    simulated_minutes: int,
) -> bool:
    if isinstance(event_ids, int):
        event_id_list = [event_ids]
    else:
        event_id_list = list(event_ids)

    key_moments = Config.PRE_START_ODDS_MOMENTS
    debug_mode = True
    runtime = PreStartRuntime(EventRepository())

    logger.info("=" * 80)
    logger.info("PRE-START CHECK DEVELOPMENT SIMULATION")
    logger.info("=" * 80)
    logger.info("Event IDs: %s", event_id_list)
    logger.info("Simulated minutes: %s", simulated_minutes)
    logger.info(
        "The simulated minute selects the historical target; collected_at and "
        "evaluation_as_of retain real UTC acquisition/evaluation times. "
        "Source timestamps reconstruct checkpoints; this is a new evaluation."
    )
    logger.info(
        "Effective toggles: %s",
        {flag: globals()[name] for flag, name in _TOGGLE_FLAGS.items()},
    )
    logger.info(
        "Is key moment: %s (configured: %s)",
        simulated_minutes in key_moments,
        key_moments,
    )
    if simulated_minutes not in key_moments:
        logger.warning(
            "This minute is outside PRE_START_ODDS_MOMENTS; alert/pillar evaluation will skip it."
        )
    allowed_pillar_moments = Config.PILLAR_PIPELINE_EXECUTION_MOMENTS
    if (
        ENABLE_PILLAR_PIPELINE
        and allowed_pillar_moments
        and simulated_minutes not in allowed_pillar_moments
    ):
        logger.warning(
            "Pillars will skip this minute; PILLAR_PIPELINE_EXECUTION_MOMENTS=%s",
            allowed_pillar_moments,
        )
    if ENABLE_ODDS_INGESTION_SIMULATION and ENABLE_ODDSPAPI_ODDS_SIMULATION:
        logger.info(
            "Oddspapi allowed moments: %s", Config.ODDSPAPI_PRE_START_ALLOWED_MOMENTS
        )
    if ENABLE_ODDS_INGESTION_SIMULATION and ENABLE_ODDSPORTAL_ODDS_SIMULATION:
        logger.info(
            "OddsPortal opening capture minute: %s",
            Config.ODDSPORTAL_OPENING_CAPTURE_MINUTES,
        )
    logger.info("=" * 80)

    logger.info("Step 1: Loading %s event(s) from database", len(event_id_list))
    events_by_id: dict[int, Event] = {}
    missing_ids: list[int] = []
    for e_id in event_id_list:
        event_obj = runtime.event_repo.get_event_by_id(e_id)
        if event_obj is None:
            logger.error("Event %s not found in database.", e_id)
            missing_ids.append(e_id)
        else:
            events_by_id[e_id] = event_obj

    if not events_by_id:
        logger.error("No requested event(s) found in database; aborting.")
        return False

    oddspapi_boundaries_by_event: dict[int, datetime | None] = {}
    for e_id, event_obj in events_by_id.items():
        logger.info(
            "Event loaded: id=%s | %s vs %s | sport=%s season_id=%s start=%s",
            e_id,
            event_obj.home_team,
            event_obj.away_team,
            event_obj.sport,
            event_obj.season_id,
            event_obj.starts_at,
        )
        kickoff_utc = OddspapiHistoricalOddsAsOf.start_time_as_utc(event_obj.starts_at)
        boundary = (
            kickoff_utc - timedelta(minutes=simulated_minutes)
            if kickoff_utc is not None
            else None
        )
        oddspapi_boundaries_by_event[e_id] = boundary
        logger.info(
            "Event %s Oddspapi historical source cutoff: %s",
            e_id,
            boundary,
        )

    eligible_events: list = []
    for e_id, event_obj in events_by_id.items():
        if not _log_pipeline_eligibility(event_obj):
            logger.info(
                "Production upcoming-event selection would exclude event %s.",
                e_id,
            )
        else:
            eligible_events.append(event_obj)

    if not eligible_events:
        logger.info(
            "Simulation complete: production upcoming-event selection would "
            "exclude all requested event(s).",
        )
        return not missing_ids

    supported_events = []
    for event_obj in eligible_events:
        if not is_supported_sport_name(event_obj.sport):
            logger.warning(
                "Event %s sport %r is unsupported by production alert/pillar evaluation.",
                event_obj.id,
                event_obj.sport,
            )
            continue
        supported_events.append(event_obj)

    eligible_events = supported_events
    if not eligible_events:
        logger.info("Simulation complete: no supported sports were provided.")
        return not missing_ids

    events_data = [
        EventRepository._build_event_data_with_legacy_fallback(event_obj)
        for event_obj in eligible_events
    ]
    timings = {int(e["id"]): simulated_minutes for e in events_data}

    previous_snapshot_ids = (
        _load_snapshot_ids_by_event([event.id for event in eligible_events])
        if SHOW_MARKET_PERSISTENCE_REPORT
        else {}
    )
    try:
        if ENABLE_ODDS_INGESTION_SIMULATION:
            if ENABLE_ODDSPORTAL_ODDS_SIMULATION:
                logger.info(
                    "Step 2: Starting production OddsPortal candidate selection (%s events)",
                    len(events_data),
                )
                oddsportal_context = start_oddsportal_scrape_for_events(
                    runtime,
                    events_data,
                    timings,
                    debug_mode=debug_mode,
                    scraping_enabled=ENABLE_ODDSPORTAL_ODDS_SIMULATION,
                )
            else:
                logger.info(
                    "Step 2: Skipping OddsPortal scrape (ENABLE_ODDSPORTAL_ODDS_SIMULATION=False)"
                )
                oddsportal_context = OddsPortalScrapeContext(
                    event_states={},
                    event_ids=set(),
                )

            logger.info(
                "Step 3: Running production candidate builder and provider ingestion (%s events)",
                len(eligible_events),
            )
            target_for_phase = (
                eligible_events[0] if len(eligible_events) == 1 else eligible_events
            )
            odds_outcome = run_production_odds_phase(
                target_for_phase,
                simulated_minutes,
                key_moments,
                debug_mode=debug_mode,
                runtime=runtime,
                enable_sofascore=ENABLE_SOFASCORE_ODDS_SIMULATION,
                enable_oddspapi=ENABLE_ODDSPAPI_ODDS_SIMULATION,
                oddspapi_available_through_utc=(
                    oddspapi_boundaries_by_event.get(eligible_events[0].id)
                    if len(eligible_events) == 1
                    else oddspapi_boundaries_by_event
                ),
            )
            event_plan = odds_outcome.event_plan
        else:
            logger.info(
                "Step 2: Skipping OddsPortal scrape (ENABLE_ODDS_INGESTION_SIMULATION=False)"
            )
            oddsportal_context = OddsPortalScrapeContext(
                event_states={},
                event_ids=set(),
            )

            logger.info(
                "Step 3: Skipping provider odds ingestion (ENABLE_ODDS_INGESTION_SIMULATION=False); building candidate plan only"
            )
            source_states = load_pre_start_odds_source_states(events_data)
            event_plan = build_pre_start_event_candidates(
                runtime,
                events_data,
                timings,
                source_states,
                key_moments=key_moments,
            )

    finally:
        # Finish provider writes before reading trajectories, and also on failure.
        _wait_for_oddsportal_worker(runtime)

    if SHOW_MARKET_PERSISTENCE_REPORT:
        for event in eligible_events:
            _log_persisted_market_odds(event.id, previous_snapshot_ids[event.id])

    if ENABLE_ALERT_PIPELINE or ENABLE_PILLAR_PIPELINE:
        logger.info("Step 4: Running production key-moment evaluation")
        evaluate_pre_start_key_moments(
            runtime,
            event_plan,
            oddsportal_context,
            debug_mode=debug_mode,
            enable_alert_pipeline=ENABLE_ALERT_PIPELINE,
            enable_pillar_pipeline=ENABLE_PILLAR_PIPELINE,
            enabled_pillars=(
                {
                    "pillar_1": ENABLE_PILLAR_1,
                    "pillar_2": ENABLE_PILLAR_2,
                    "pillar_3": ENABLE_PILLAR_3,
                    "pillar_4": ENABLE_PILLAR_4,
                    "pillar_5": ENABLE_PILLAR_5,
                }
                if ENABLE_CUSTOM_PILLAR_FILTER
                else None
            ),
        )
    else:
        logger.info(
            "Step 4: Skipping pillar and alert evaluation "
            "(both ENABLE_ALERT_PIPELINE and ENABLE_PILLAR_PIPELINE are False)"
        )

    logger.info("=" * 80)
    logger.info(
        "SIMULATION COMPLETE for %s event(s) at %s minutes",
        len(eligible_events),
        simulated_minutes,
    )
    logger.info("=" * 80)
    return not missing_ids


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Simulate the production pre-start flow for one or more database events at "
            "a forced minutes-until-start value."
        ),
    )
    parser.add_argument(
        "event_ids",
        nargs="+",
        help=(
            "One or more canonical event IDs (e.g. 14083613 14083614 or 14083613,14083614). "
            "If --minutes is omitted, the final argument is treated as minutes."
        ),
    )
    parser.add_argument(
        "--minutes",
        "-m",
        type=int,
        default=None,
        help="Forced minutes until start. If omitted, the last positional argument is used as minutes.",
    )
    for flag, constant in _TOGGLE_FLAGS.items():
        parser.add_argument(
            f"--{flag}",
            action=argparse.BooleanOptionalAction,
            default=None,
            help=f"Override {constant} (file default: {globals()[constant]}).",
        )
    args = parser.parse_args()
    _ensure_logging_configured()

    try:
        event_ids, minutes = _parse_event_ids_and_minutes(args.event_ids, args.minutes)
    except ValueError as err:
        logger.error("Argument error: %s", err)
        return 1

    if not initialize_system():
        logger.error(
            "Failed to initialize system; the pre-start simulation cannot run."
        )
        return 1

    return (
        0
        if simulate_pre_start_check(
            event_ids,
            minutes,
            toggle_overrides={
                flag: getattr(args, flag.replace("-", "_"))
                for flag in _TOGGLE_FLAGS
                if getattr(args, flag.replace("-", "_")) is not None
            },
        )
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
