"""Top-level orchestration for the pre-start check job."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Callable

from infrastructure.settings import Config
from modules.alerts.matchup_streak_analysis.standings_engine import (
    standings_calculator,
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
from modules.jobs.pre_start_check_job.moment_policy import regular_pre_start_moments
from modules.jobs.pre_start_check_job.odds_source_state import (
    PreStartOddsSourceStates,
    load_pre_start_odds_source_states,
)
from modules.jobs.pre_start_check_job.oddsportal_worker import (
    OddsPortalScrapeContext,
    start_oddsportal_scrape_for_events,
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
from modules.jobs.pre_start_check_job.timestamp_corrections import (
    check_recently_started_events_for_timestamp_corrections,
)
from modules.jobs.pre_start_check_job.timing import (
    minutes_since_start,
    minutes_until_start,
)
from modules.competition.tracked_competitions import tracked_competition_ids
from modules.odds_ingestion import ProviderOddsSummary
from modules.sofascore import api_client

logger = logging.getLogger(__name__)


def _tracked_competition_ids() -> list[int] | None:
    if not Config.TRACKED_COMPETITIONS_ONLY:
        return None

    competition_ids = list(tracked_competition_ids())
    logger.info(
        "Pre-start check restricted to %s tracked competitions",
        len(competition_ids),
    )
    return competition_ids


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
    active_tracked_competition_ids: list[int] | None,
) -> list[dict]:
    """Run timestamp/result maintenance and remove newly rescheduled events."""
    started_events = scheduler.event_repo.get_events_started_recently(
        window_minutes=Config.INTRADAY_RESULT_FRESHNESS_WINDOW_MINUTES,
        competition_ids=active_tracked_competition_ids,
    )
    logger.info(
        "Found %s started events without results within the last %s minutes",
        len(started_events),
        Config.INTRADAY_RESULT_FRESHNESS_WINDOW_MINUTES,
    )
    timestamp_candidates, result_freshness_candidates = (
        _split_recently_started_events(started_events)
    )

    if Config.TIMESTAMP_CORRECTIONS_TRACKED_COMPETITIONS_ONLY:
        timestamp_tracked_ids = set(tracked_competition_ids())
        timestamp_candidates = [
            event
            for event in timestamp_candidates
            if event.get("competition_id") in timestamp_tracked_ids
        ]

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

    if Config.INTRADAY_RESULT_FRESHNESS_TRACKED_COMPETITIONS_ONLY:
        freshness_tracked_ids = set(tracked_competition_ids())
        result_freshness_candidates = [
            event
            for event in result_freshness_candidates
            if event.get("competition_id") in freshness_tracked_ids
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


# Every provider phase shares the same call shape - (candidates, source_states,
# *, debug_mode) -> ProviderOddsSummary - so adding a provider is one entry
# here plus its own fetch/ingest implementation, not a new orchestration path.
ProviderOddsPhase = Callable[..., ProviderOddsSummary]
_PROVIDER_ODDS_PHASES: tuple[ProviderOddsPhase, ...] = (
    run_sofascore_pre_start_odds,
    run_oddspapi_pre_start_odds,
)


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
    for phase in _PROVIDER_ODDS_PHASES:
        phase_name = getattr(phase, "__name__", repr(phase))
        try:
            phase(event_plan.candidates, source_states, debug_mode=debug_mode)
        except Exception:
            logger.exception("%s odds ingestion failed", phase_name)


def _load_upcoming_events(scheduler, tracked_competition_ids) -> list[dict]:
    logger.info(
        "📋 Starting upcoming-event load (window=%s minutes)",
        Config.PRE_START_WINDOW_MINUTES,
    )
    upcoming_events = scheduler.event_repo.get_events_starting_soon(
        Config.PRE_START_WINDOW_MINUTES,
        competition_ids=tracked_competition_ids,
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
    key_moments,
) -> int:
    return sum(
        1
        for event in upcoming_events
        if timings.get(event["id"]) in key_moments
    )


def run_pre_start_odds_moments(
    scheduler,
    upcoming_events: list[dict],
    timings: dict[int, int],
    *,
    key_moments,
    oddsportal_context: OddsPortalScrapeContext,
    debug_mode: bool = False,
    timestamp_correction_enabled: bool | None = None,
    evaluate_key_moments: bool = True,
) -> PreStartEventPlan:
    """Run candidate selection and provider ingestion, optionally evaluating downstream flows."""
    source_states = load_pre_start_odds_source_states(upcoming_events)

    global_ts_correction = (
        Config.ENABLE_TIMESTAMP_CORRECTION
        if timestamp_correction_enabled is None
        else timestamp_correction_enabled
    )

    restrict_timestamp_corrections = (
        global_ts_correction
        and Config.TIMESTAMP_CORRECTIONS_TRACKED_COMPETITIONS_ONLY
    )
    tracked_ids = (
        set(tracked_competition_ids())
        if (
            restrict_timestamp_corrections
            or Config.ODDS_EXTRACTION_TRACKED_COMPETITIONS_ONLY
        )
        else None
    )

    if restrict_timestamp_corrections:
        ts_events = [e for e in upcoming_events if e.get("competition_id") in tracked_ids]
        no_ts_events = [e for e in upcoming_events if e.get("competition_id") not in tracked_ids]
    else:
        ts_events = upcoming_events
        no_ts_events = []

    odds_extraction_competition_ids = (
        tracked_ids if Config.ODDS_EXTRACTION_TRACKED_COMPETITIONS_ONLY else None
    )

    plan_ts = build_pre_start_event_candidates(
        scheduler,
        ts_events,
        timings,
        source_states,
        key_moments=key_moments,
        timestamp_correction_enabled=global_ts_correction,
        odds_extraction_competition_ids=odds_extraction_competition_ids,
    )

    if no_ts_events:
        logger.info(
            "🚫 Skipping timestamp corrections for %s events from untracked competitions",
            len(no_ts_events),
        )
        plan_no_ts = build_pre_start_event_candidates(
            scheduler,
            no_ts_events,
            timings,
            source_states,
            key_moments=key_moments,
            timestamp_correction_enabled=False,
            odds_extraction_competition_ids=odds_extraction_competition_ids,
        )
        event_plan = PreStartEventPlan(
            candidates=plan_ts.candidates + plan_no_ts.candidates,
            by_event_id={**plan_ts.by_event_id, **plan_no_ts.by_event_id},
        )
    else:
        event_plan = plan_ts

    attach_stored_observations(event_plan.candidates)
    persist_snapshot_observations(event_plan.candidates)
    _ingest_provider_odds(
        event_plan,
        source_states,
        debug_mode=debug_mode,
    )
    logger.info(
        "Provider odds ingestion completed for %s candidates",
        sum(
            1
            for candidate in event_plan.candidates
            if candidate.get("should_extract_odds")
        ),
    )
    if evaluate_key_moments:
        evaluate_pre_start_key_moments(
            scheduler,
            event_plan,
            oddsportal_context,
            debug_mode=debug_mode,
        )
    return event_plan


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
        tracked_competition_ids = _tracked_competition_ids()
        upcoming_events = _load_upcoming_events(
            scheduler,
            tracked_competition_ids,
        )
        timings = {
            event["id"]: minutes_until_start(event["start_time_utc"])
            for event in upcoming_events
        }

        logger.info(
            "🌐 Starting OddsPortal scrape selection",
            extra={"oddsportal": True},
        )
        oddsportal_context: OddsPortalScrapeContext = (
            start_oddsportal_scrape_for_events(
                scheduler,
                upcoming_events,
                timings,
                debug_mode=global_debug_mode,
            )
        )

        logger.info(
            "🔧 Starting maintenance "
            "(recently-started timestamp corrections + intraday freshness)"
        )
        upcoming_events = _maintain_recently_started_events(
            scheduler,
            upcoming_events,
            tracked_competition_ids,
        )

        logger.info("🏀 Starting in-game checks (NBA 4th quarter)")
        run_in_game_checks()

        if not upcoming_events:
            logger.warning("No upcoming events found after maintenance checks")
            return

        key_moments = regular_pre_start_moments()
        key_moment_count = _count_key_moment_events(
            upcoming_events,
            timings,
            key_moments,
        )
        logger.info(
            "🎯 Starting key-moment candidate build: %s/%s upcoming events "
            "at key minutes_until_start %s",
            key_moment_count,
            len(upcoming_events),
            key_moments,
        )

        run_pre_start_odds_moments(
            scheduler,
            upcoming_events,
            timings,
            key_moments=key_moments,
            oddsportal_context=oddsportal_context,
            debug_mode=global_debug_mode,
        )
        logger.info("✅ Pre-start check phases completed")
    except Exception:
        logger.exception("Pre-start check job failed")
    finally:
        if previous_evidence_mode is not None:
            api_client.set_challenge_evidence_enabled(previous_evidence_mode)


__all__ = ["run_pre_start_check_job", "run_pre_start_odds_moments"]
