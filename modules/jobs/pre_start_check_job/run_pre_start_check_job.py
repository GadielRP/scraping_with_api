"""Top-level orchestration for the pre-start check job."""

from __future__ import annotations

import logging
from datetime import datetime

from infrastructure.settings import Config
from modules.alerts.matchup_streak_analysis.standings_engine import (
    standings_calculator,
)
from modules.jobs.oddspapi.pre_start_odds.pre_start_odds_job import (
    run_oddspapi_pre_start_odds_ingestion,
)
from modules.jobs.pre_start_check_job.event_candidate_builder import (
    PreStartEventPlan,
    build_pre_start_event_candidates,
)
from modules.jobs.pre_start_check_job.in_game_checks import run_in_game_checks
from modules.jobs.pre_start_check_job.intraday_result_freshness import (
    process_intraday_result_freshness,
)
from modules.jobs.pre_start_check_job.key_moment_evaluation import (
    enrich_event_context_competition_metadata as _enrich_event_context_competition_metadata,
    evaluate_pre_start_key_moments,
    flush_missing_standings_endpoints as _flush_missing_standings_endpoints,
)
from modules.jobs.pre_start_check_job.odds_source_state import (
    PreStartOddsSourceStates,
    load_pre_start_odds_source_states,
)
from modules.jobs.pre_start_check_job.oddsportal_worker import (
    OddsPortalScrapeContext,
    start_oddsportal_scrape_for_events,
)
from modules.jobs.pre_start_check_job.sofascore_odds_processor import (
    process_sofascore_pre_start_odds,
)
from modules.jobs.pre_start_check_job.timestamp_corrections import (
    check_recently_started_events_for_timestamp_corrections,
)
from modules.jobs.pre_start_check_job.timing import (
    minutes_since_start,
    minutes_until_start,
)
from modules.oddsportal.oddsportal_config import SEASON_ODDSPORTAL_MAP
from modules.sofascore import api_client

logger = logging.getLogger(__name__)


def _tracked_season_ids() -> list[int] | None:
    if not Config.TRACKED_SEASONS_ONLY:
        return None

    season_ids = list(SEASON_ODDSPORTAL_MAP)
    logger.info(
        "Pre-start check restricted to %s tracked seasons",
        len(season_ids),
    )
    return season_ids


def _split_recently_started_events(
    events: list[dict],
) -> tuple[list[dict], list[dict]]:
    timestamp_candidates: list[dict] = []
    result_freshness_candidates: list[dict] = []

    for event_data in events:
        try:
            minutes_ago = abs(minutes_since_start(event_data["start_time_utc"]))
        except Exception:
            logger.warning(
                "Could not compute minutes_ago for started event %s",
                event_data.get("id"),
            )
            continue

        target = (
            timestamp_candidates
            if minutes_ago <= 60
            else result_freshness_candidates
        )
        target.append(event_data)

    logger.info(
        "Started event split: timestamp_candidates=%s result_freshness_candidates=%s",
        len(timestamp_candidates),
        len(result_freshness_candidates),
    )
    return timestamp_candidates, result_freshness_candidates


def _maintain_recently_started_events(
    scheduler,
    upcoming_events: list[dict],
    tracked_season_ids: list[int] | None,
) -> list[dict]:
    """Run timestamp/result maintenance and remove newly rescheduled events."""
    started_events = scheduler.event_repo.get_events_started_recently(
        window_minutes=Config.INTRADAY_RESULT_FRESHNESS_WINDOW_MINUTES,
        season_ids=tracked_season_ids,
    )
    logger.info(
        "Found %s started events without results within the last %s minutes",
        len(started_events),
        Config.INTRADAY_RESULT_FRESHNESS_WINDOW_MINUTES,
    )
    timestamp_candidates, result_freshness_candidates = (
        _split_recently_started_events(started_events)
    )

    logger.info(
        "⏱️ Starting recently-started timestamp corrections (%s candidates)",
        len(timestamp_candidates),
    )
    modified_event_ids = check_recently_started_events_for_timestamp_corrections(
        timestamp_candidates
    )
    if modified_event_ids:
        upcoming_events = [
            event
            for event in upcoming_events
            if event["id"] not in modified_event_ids
        ]
        logger.info(
            "Filtered %s upcoming events modified by timestamp correction",
            len(modified_event_ids),
        )

    scheduler._cleanup_recently_rescheduled()
    upcoming_events = [
        event
        for event in upcoming_events
        if event["id"] not in scheduler.recently_rescheduled
    ]

    logger.info(
        "🔄 Starting intraday result freshness (%s candidates)",
        len(result_freshness_candidates),
    )
    freshness_stats = process_intraday_result_freshness(
        result_freshness_candidates
    )
    logger.info("🔄✅ Intraday result freshness completed: %s", freshness_stats)
    if (
        freshness_stats.get("results_upserted", 0) > 0
        or freshness_stats.get("deleted_events", 0) > 0
    ):
        standings_calculator.clear_cache()
        logger.info(
            "Cleared standings calculator cache after intraday result changes"
        )

    return upcoming_events


def _ingest_provider_odds(
    event_plan: PreStartEventPlan,
    source_states: PreStartOddsSourceStates,
    *,
    debug_mode: bool,
) -> None:
    """Execute independent provider phases using the same candidate plan."""
    active_count = sum(
        1 for c in event_plan.candidates if c.get("should_extract_odds")
    )
    logger.info(
        "💰 Starting provider odds ingestion (%s candidates)",
        active_count,
    )
    process_sofascore_pre_start_odds(
        event_plan.candidates,
        source_states,
        debug_mode=debug_mode,
    )

    try:
        run_oddspapi_pre_start_odds_ingestion(
            event_plan.candidates,
            debug_mode=debug_mode,
            source_states=source_states,
        )
    except Exception:
        logger.exception("Oddspapi pre-start odds ingestion failed")


def _load_upcoming_events(scheduler, tracked_season_ids) -> list[dict]:
    logger.info(
        "📋 Starting upcoming-event load (window=%s minutes)",
        Config.PRE_START_WINDOW_MINUTES,
    )
    upcoming_events = scheduler.event_repo.get_events_starting_soon(
        Config.PRE_START_WINDOW_MINUTES,
        season_ids=tracked_season_ids,
    )
    logger.info(
        "Found %s events starting within %s minutes",
        len(upcoming_events),
        Config.PRE_START_WINDOW_MINUTES,
    )
    return upcoming_events


def _count_key_moment_events(
    upcoming_events: list[dict],
    timings: dict[int, int],
) -> int:
    key_moments = Config.PRE_START_ODDS_MOMENTS
    return sum(
        1
        for event in upcoming_events
        if timings.get(event["id"]) in key_moments
    )


def run_pre_start_check_job(scheduler, global_debug_mode: bool = False) -> None:
    """Run maintenance, odds ingestion, and key-moment evaluation in order."""
    logger.info(
        "🚀 PRE-START CHECK EXECUTED at %s",
        datetime.now().strftime("%H:%M:%S"),
    )
    previous_evidence_mode = getattr(
        api_client,
        "challenge_evidence_enabled",
        None,
    )
    api_client.set_challenge_evidence_enabled(global_debug_mode)
    logger.info(
        "SofaScore challenge evidence capture %s for pre-start check",
        "enabled" if global_debug_mode else "disabled",
    )

    try:
        tracked_season_ids = _tracked_season_ids()
        upcoming_events = _load_upcoming_events(
            scheduler,
            tracked_season_ids,
        )
        timings = {
            event["id"]: minutes_until_start(event["start_time_utc"])
            for event in upcoming_events
        }

        logger.info("🌐 Starting OddsPortal scrape selection")
        oddsportal_context: OddsPortalScrapeContext = (
            start_oddsportal_scrape_for_events(
                scheduler,
                upcoming_events,
                timings,
            )
        )

        logger.info(
            "🔧 Starting maintenance "
            "(recently-started timestamp corrections + intraday freshness)"
        )
        upcoming_events = _maintain_recently_started_events(
            scheduler,
            upcoming_events,
            tracked_season_ids,
        )

        logger.info("🏀 Starting in-game checks (NBA 4th quarter)")
        run_in_game_checks()

        if not upcoming_events:
            logger.warning("No upcoming events found after maintenance checks")
            return

        key_moment_count = _count_key_moment_events(upcoming_events, timings)
        logger.info(
            "🎯 Starting key-moment candidate build: %s/%s upcoming events "
            "at key minutes_until_start %s",
            key_moment_count,
            len(upcoming_events),
            Config.PRE_START_ODDS_MOMENTS,
        )

        # Load once, after filtering rescheduled events, and share the result
        # between both provider processors.
        source_states = load_pre_start_odds_source_states(upcoming_events)
        event_plan = build_pre_start_event_candidates(
            scheduler,
            upcoming_events,
            timings,
            source_states,
        )
        _ingest_provider_odds(
            event_plan,
            source_states,
            debug_mode=global_debug_mode,
        )
        active_count = sum(
            1 for c in event_plan.candidates if c.get("should_extract_odds")
        )
        logger.info(
            "✅ Provider odds ingestion completed for %s candidates",
            active_count,
        )

        logger.info("🔔 Starting key-moment alert/pillar evaluation")
        evaluate_pre_start_key_moments(
            scheduler,
            upcoming_events,
            event_plan,
            oddsportal_context,
            debug_mode=global_debug_mode,
        )
        logger.info("✅ Pre-start check phases completed")
    except Exception:
        logger.exception("Pre-start check job failed")
    finally:
        if previous_evidence_mode is not None:
            api_client.set_challenge_evidence_enabled(previous_evidence_mode)


__all__ = ["run_pre_start_check_job"]
