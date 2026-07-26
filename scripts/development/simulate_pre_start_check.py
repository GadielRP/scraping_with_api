"""Simulate the production pre-start check flow for one event.

Mirrors production's JobScheduler.job_pre_start_check path as closely as
possible for one event at a forced key-moment (minutes until start), including
the real observe_operation("pre_start_check") runtime stats logs.

Usage:
    python -m scripts.development.simulate_pre_start_check <event_id> <minutes_until_start>

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
from infrastructure.persistence.catalogs.canonical_market_types import CANONICAL_MARKET_TYPE_SEEDS
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    Market,
    MarketChoice,
    refresh_materialized_views,
)
from infrastructure.persistence.repositories import EventRepository, OddsTrajectoryRepository
from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.alert_pipeline import evaluate_and_dispatch_alerts_batch
from modules.jobs.pre_start_check_job.pillar_pipeline import evaluate_and_calculate_pillars_batch
from modules.jobs.pre_start_check_job.run_pre_start_check_job import (
    _enrich_event_context_competition_metadata,
    _flush_missing_standings_endpoints,
)
from modules.odds_ingestion.canonical_market_normalizer import CanonicalMarketNormalizer
from modules.pillars.context import build_event_context
from shared.runtime_observability import observe_operation
from scripts.development.pre_start_odds_simulation import run_production_odds_phase

# ---------------------------------------------------------------------------
# Global configuration for the development simulation
# ---------------------------------------------------------------------------
SHOW_MARKET_PERSISTENCE_REPORT = True  # Toggle market persistence logs & database report

# Reconfigure stdout to utf-8 to support emojis in Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Logging setup — verbose so the tester sees everything
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


def _ensure_logging_configured() -> None:
    if not any(
        isinstance(handler, logging.FileHandler)
        for handler in logging.getLogger().handlers
    ):
        setup_logging()


# ---------------------------------------------------------------------------
# Development reporting helpers; job logic stays in production modules
# ---------------------------------------------------------------------------

def _find_matching_raw_market(market, adapted_response: dict | None) -> dict | None:
    if not adapted_response or not adapted_response.get("markets"):
        return None

    def _norm(v):
        return str(v or "").strip().lower()

    # Try matching by checking if any choice's snapshot in this market matches a raw choice's sourceOutcomeId
    for choice in market.choices:
        for snapshot in choice.snapshots:
            if snapshot.source_outcome_id:
                for raw_m in adapted_response["markets"]:
                    for raw_c in raw_m.get("choices", []):
                        if raw_c.get("sourceOutcomeId") == snapshot.source_outcome_id:
                            return raw_m

    # Fallback: Match by resolving raw market's canonical key and comparing to market fields
    for raw_m in adapted_response["markets"]:
        raw_key = CanonicalMarketNormalizer._resolve_sofascore_key(raw_m)
        if not raw_key:
            continue
        canonical_seed = CANONICAL_MARKET_TYPE_SEEDS.get(raw_key)
        if canonical_seed:
            db_market_dict = {
                "marketName": market.market_name,
                "marketGroup": market.market_group,
                "marketPeriod": market.market_period,
            }
            db_canonical_key = CanonicalMarketNormalizer._resolve_sofascore_key(db_market_dict)
            if db_canonical_key and db_canonical_key == raw_key:
                if canonical_seed.get("requires_choice_group"):
                    if _norm(market.choice_group) == _norm(raw_m.get("choiceGroup")):
                        return raw_m
                else:
                    return raw_m
    return None


def _resolve_market_key_from_db_fields(market) -> str | None:
    def _norm(v):
        return str(v or "").strip().lower().replace("-", " ")

    db_name = _norm(market.market_name)
    db_group = _norm(market.market_group)
    db_period = _norm(market.market_period)

    for key, seed in CANONICAL_MARKET_TYPE_SEEDS.items():
        seed_name = _norm(seed["canonical_market_name"])
        seed_group = _norm(seed["canonical_market_group"])
        seed_period = _norm(seed["canonical_market_period"])
        if db_name == seed_name and db_group == seed_group and db_period == seed_period:
            return key

    db_market_dict = {
        "marketName": market.market_name,
        "marketGroup": market.market_group,
        "marketPeriod": market.market_period,
    }
    return CanonicalMarketNormalizer._resolve_sofascore_key(db_market_dict)


def _log_persisted_market_odds(
    event_id: int,
    previous_snapshot_ids: set[int],
    adapted_response: dict | None = None,
) -> None:
    """Log the exact market odds state stored for one event after ingestion."""
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

        choice_count = sum(len(market.choices) for market in markets)
        snapshots = [
            snapshot
            for market in markets
            for choice in market.choices
            for snapshot in choice.snapshots
        ]
        new_snapshot_count = sum(
            snapshot.snapshot_id not in previous_snapshot_ids
            for snapshot in snapshots
        )

        logger.info("")
        logger.info("=" * 100)
        logger.info("DATABASE PERSISTENCE AFTER ODDS INGESTION")
        logger.info("=" * 100)
        logger.info("event_id=%s", event_id)
        logger.info(
            "persisted totals: markets=%s choices=%s snapshots=%s new_snapshots_this_run=%s",
            len(markets),
            choice_count,
            len(snapshots),
            new_snapshot_count,
        )

        if not markets:
            logger.info("No market odds rows are persisted for this event.")

        for market in markets:
            raw_m = _find_matching_raw_market(market, adapted_response)
            raw_market_name = raw_m.get("marketName") if raw_m else None
            raw_group = raw_m.get("marketGroup") if raw_m else None
            raw_period = raw_m.get("marketPeriod") if raw_m else None
            market_key = _resolve_market_key_from_db_fields(market)

            has_new_snapshots = False
            for choice in market.choices:
                for snapshot in choice.snapshots:
                    if snapshot.snapshot_id not in previous_snapshot_ids:
                        has_new_snapshots = True
                        break
                if has_new_snapshots:
                    break

            prefix = "🟢 " if has_new_snapshots else ""

            logger.info("-" * 100)
            logger.info(
                "%sMARKET market_id=%s | bookie_id=%s | bookie=%r | live=%s",
                prefix,
                market.market_id,
                market.bookie_id,
                market.bookie.name if market.bookie else None,
                market.is_live,
            )
            logger.info("  market_key    = %r", market_key)
            logger.info("  market_name   = %r (raw_market_name=%r)", market.market_name, raw_market_name)
            logger.info("  market_group  = %r (raw_group=%r)", market.market_group, raw_group)
            logger.info("  market_period = %r (raw_period=%r)", market.market_period, raw_period)
            logger.info("  choice_group  = %r", market.choice_group)
            logger.info("  collected_at  = %s", market.collected_at)

            for choice in sorted(market.choices, key=lambda item: item.choice_id):
                logger.info(
                    "  CHOICE choice_id=%s | name=%r | initial_odds=%s | current_odds=%s | change=%s",
                    choice.choice_id,
                    choice.choice_name,
                    choice.initial_odds,
                    choice.current_odds,
                    choice.change,
                )
                for snapshot in sorted(choice.snapshots, key=lambda item: item.snapshot_id):
                    persistence_state = (
                        "NEW THIS RUN"
                        if snapshot.snapshot_id not in previous_snapshot_ids
                        else "PRE-EXISTING"
                    )
                    logger.info(
                        "    SNAPSHOT [%s] snapshot_id=%s | odds=%s | collected_at=%s | "
                        "source=%r | source_collected_at=%s | source_market_id=%r | "
                        "source_outcome_id=%r | bookmaker_outcome_id=%r | main_line=%r | "
                        "source_limit=%s | exchange_side=%r | exchange_level=%r | exchange_size=%s",
                        persistence_state,
                        snapshot.snapshot_id,
                        snapshot.odds_value,
                        snapshot.collected_at,
                        snapshot.source,
                        snapshot.source_collected_at,
                        snapshot.source_market_id,
                        snapshot.source_outcome_id,
                        snapshot.bookmaker_outcome_id,
                        snapshot.main_line,
                        snapshot.source_limit,
                        snapshot.exchange_side,
                        snapshot.exchange_level,
                        snapshot.exchange_size,
                    )

        logger.info("=" * 100)
        logger.info("END DATABASE PERSISTENCE REPORT")
        logger.info("=" * 100)


# ---------------------------------------------------------------------------
# Main simulation
# ---------------------------------------------------------------------------

def simulate_pre_start_check(event_id: int, simulated_minutes: int) -> bool:
    """Simulate the pre-start check flow for a single event at a given key moment.

    Wrapped with the same observe_operation("pre_start_check") context used by
    JobScheduler.job_pre_start_check so duration/RSS/cgroup stats are logged.
    """
    _ensure_logging_configured()
    with observe_operation("pre_start_check"):
        return _run_pre_start_check_simulation(event_id, simulated_minutes)


def _run_pre_start_check_simulation(
    event_id: int,
    simulated_minutes: int,
) -> bool:
    key_moments = Config.PRE_START_ODDS_MOMENTS
    debug_mode = True
    standings_endpoint_missing_competition_ids: set[int] = set()

    logger.info("=" * 80)
    logger.info("🧪 PRE-START CHECK DEVELOPMENT SIMULATION")
    logger.info("=" * 80)
    logger.info("  Event ID         : %s", event_id)
    logger.info("  Simulated minutes: %s", simulated_minutes)
    logger.info("  Is key moment    : %s", simulated_minutes in key_moments)
    logger.info("=" * 80)

    # ------------------------------------------------------------------
    # 1. Load the event from the database
    # ------------------------------------------------------------------
    logger.info("\n📦 Step 1: Loading event from database...")
    event_obj = EventRepository.get_event_by_id(event_id)
    if event_obj is None:
        logger.error("❌ Event %s not found in database. Aborting.", event_id)
        return False

    logger.info("  ✅ Event loaded: %s vs %s", event_obj.home_team, event_obj.away_team)
    logger.info(
        "  Sport: %s | Season ID: %s | Start: %s",
        event_obj.sport,
        event_obj.season_id,
        event_obj.start_time_utc,
    )

    if event_obj.sport in Config.EXCLUDED_SPORTS:
        logger.warning(
            "⚠️ Event sport '%s' is in EXCLUDED_SPORTS — would be skipped in production.",
            event_obj.sport,
        )

    # ------------------------------------------------------------------
    # 2. Simulate odds extraction (same as should_extract_odds logic)
    # ------------------------------------------------------------------
    logger.info("\n💰 Step 2: Simulating odds extraction (key moment = %s)...", simulated_minutes)

    odds_outcome = run_production_odds_phase(
        event_obj,
        simulated_minutes,
        key_moments,
        debug_mode=debug_mode,
        show_persistence_report=SHOW_MARKET_PERSISTENCE_REPORT,
        log_persisted_market_odds=_log_persisted_market_odds,
    )
    odds_response = odds_outcome.odds_response
    metadata_snapshot = odds_outcome.metadata_snapshot
    observations = odds_outcome.observations

    # ------------------------------------------------------------------
    # 3. Build EventContext
    # ------------------------------------------------------------------
    logger.info("\n🏗️ Step 3: Building EventContext...")

    event_context = build_event_context(
        event_obj=event_obj,
        minutes_until_start=simulated_minutes,
        metadata_snapshot=metadata_snapshot,
    )

    if event_context is None:
        logger.error("❌ Failed to build EventContext for event %s. Aborting.", event_id)
        return False

    logger.info("  ✅ EventContext built: %s", event_context.participants_label)
    logger.info("  Context status: %s", event_context.context_status)

    # ------------------------------------------------------------------
    # 4. Enrich competition metadata (real job helper)
    # ------------------------------------------------------------------
    logger.info("\n🏟️ Step 4: Enriching competition metadata...")
    _enrich_event_context_competition_metadata(
        event_context,
        event_obj,
        standings_endpoint_missing_competition_ids,
    )

    logger.info("  Competition: %s", event_context.competition.display_name)
    logger.info(
        "  Number of teams: %s (source: %s)",
        event_context.competition.number_of_teams,
        event_context.competition.number_of_teams_source,
    )
    logger.info("  Total regular season games: %s", event_context.competition.total_regular_season_games)
    logger.info("  Standings grouping: %s", event_context.competition.standings_grouping)
    logger.info("  League config source: %s", event_context.competition.league_config_source)

    # ------------------------------------------------------------------
    # 5. Build event_for_alerts payload (same structure as production)
    # ------------------------------------------------------------------
    logger.info("\n📋 Step 5: Building event payload for alert/pillar pipelines...")

    event_payload = {
        "event_obj": event_obj,
        "initial_minutes": simulated_minutes,
        "observations": observations,
        "odds_response": odds_response,
        "odds_trajectory": [],
        "metadata_snapshot": metadata_snapshot,
        "event_context": event_context,
        "season_id": getattr(event_obj, "season_id", None),
        "should_send_streak_alert": False,
        "streak_analysis": None,
        "dual_report": None,
        "minutes_until_start": simulated_minutes,
        "success": True,
    }

    # ------------------------------------------------------------------
    # 6. Refresh materialized views
    # ------------------------------------------------------------------
    logger.info("\n🔄 Step 6: Refreshing materialized views...")
    try:
        refresh_materialized_views(db_manager.engine)
        logger.info("  ✅ Materialized views refreshed")
    except Exception as exc:
        logger.warning("  ⚠️ Failed to refresh materialized views: %s", exc)

    if simulated_minutes in key_moments:
        logger.info("\n📈 Step 6b: Loading odds trajectory for the pillar pipeline...")
        try:
            trajectory_by_event_id = OddsTrajectoryRepository.get_pre_start_trajectory_map(
                event_ids=[event_id],
                target_minutes=key_moments,
                tolerance_minutes=Config.PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES,
            )
            trajectory_payload_by_event_id = {
                event_id: [point.to_dict() for point in points]
                for event_id, points in trajectory_by_event_id.items()
            }
            event_payload["odds_trajectory"] = trajectory_payload_by_event_id.get(event_id, [])
            logger.info(
                "  ✅ Odds trajectory loaded for event %s: rows=%s",
                event_id,
                len(event_payload["odds_trajectory"]),
            )
        except Exception as exc:
            logger.warning("  ⚠️ Failed to load odds trajectory for event %s: %s", event_id, exc)

    events_for_alerts = [event_payload]

    # ------------------------------------------------------------------
    # 7. Run legacy alert pipeline
    # ------------------------------------------------------------------
    if Config.ENABLE_LEGACY_ALERT_PIPELINE:
        logger.info("\n📢 Step 7a: Running LEGACY ALERT PIPELINE...")
        try:
            evaluate_and_dispatch_alerts_batch(
                events_for_alerts,
                key_moments,
                EventRepository,
                op_event_states={},
                op_event_ids=set(),
                op_data_cache={},
                debug_mode=debug_mode,
            )
            logger.info("  ✅ Legacy alert pipeline completed")
        except Exception as exc:
            logger.error("  ❌ Legacy alert pipeline failed: %s", exc)
    else:
        logger.info("\n📢 Step 7a: Legacy alert pipeline DISABLED (ENABLE_LEGACY_ALERT_PIPELINE=False)")

    # Same flush the real job does between alert and pillar pipelines.
    _flush_missing_standings_endpoints(standings_endpoint_missing_competition_ids)

    # ------------------------------------------------------------------
    # 8. Run pillar pipeline
    # ------------------------------------------------------------------
    if Config.ENABLE_PILLAR_PIPELINE:
        logger.info("\n🏛️ Step 7b: Running PILLAR PIPELINE...")
        try:
            evaluate_and_calculate_pillars_batch(
                events_for_pillars=events_for_alerts,
                key_moments=key_moments,
                event_repo=EventRepository,
                op_event_states={},
                op_event_ids=set(),
                op_data_cache={},
                debug_mode=debug_mode,
            )
            logger.info("  ✅ Pillar pipeline completed")
        except Exception as exc:
            logger.error("  ❌ Pillar pipeline failed: %s", exc)
    else:
        logger.info("\n🏛️ Step 7b: Pillar pipeline DISABLED (ENABLE_PILLAR_PIPELINE=False)")

    logger.info("\n" + "=" * 80)
    logger.info("✅ SIMULATION COMPLETE for event %s at %s minutes", event_id, simulated_minutes)
    logger.info("=" * 80)
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Simulate the pre-start check job flow for a single event at a specific key moment.",
    )
    parser.add_argument(
        "event_id",
        type=int,
        help="The event ID to simulate (must exist in the database).",
    )
    parser.add_argument(
        "minutes",
        type=int,
        help="Simulated minutes until start (key moments: 120, 30, 5, 0, -5).",
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
