"""Prepare and execute alert and pillar evaluation at pre-start key moments."""

from __future__ import annotations

import logging
import pprint

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import refresh_materialized_views
from infrastructure.persistence.repositories import (
    CompetitionRepository,
    OddsTrajectoryRepository,
)
from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.alert_pipeline import (
    evaluate_and_dispatch_alerts_batch,
)
from modules.jobs.pre_start_check_job.event_candidate_builder import PreStartEventPlan
from modules.jobs.pre_start_check_job.oddsportal_worker import OddsPortalScrapeContext
from modules.jobs.pre_start_check_job.pillar_pipeline import (
    evaluate_and_calculate_pillars_batch,
)
from modules.jobs.pre_start_check_job.timing import minutes_until_start
from modules.pillars.competition_metadata_resolver import (
    apply_competition_metadata_resolution,
    mark_competition_metadata_refresh_attempted,
    resolve_competition_metadata,
)
from modules.pillars.context import build_event_context
from modules.sofascore import api_client

logger = logging.getLogger(__name__)


def enrich_event_context_competition_metadata(
    event_context,
    event_obj,
    missing_competition_ids: set[int],
) -> None:
    """Resolve competition metadata and batch known missing standings endpoints."""
    logger.info(
        "Pre-start metadata check for event %s: competition_id=%s "
        "source_unique_tournament_id=%s season_id=%s number_of_teams=%s "
        "total_regular_season_games=%s standings_grouping=%s league_config_source=%s",
        event_context.event_id,
        getattr(event_context.competition, "competition_id", None),
        getattr(event_context.competition, "source_unique_tournament_id", None),
        getattr(event_context, "season_id", None),
        getattr(event_context.competition, "number_of_teams", None),
        getattr(event_context.competition, "total_regular_season_games", None),
        getattr(event_context.competition, "standings_grouping", None),
        getattr(event_context.competition, "league_config_source", None),
    )
    resolution = resolve_competition_metadata(
        event_context,
        event_obj=event_obj,
        standings_endpoint_missing_competition_ids=missing_competition_ids,
    )
    apply_competition_metadata_resolution(event_context, resolution)
    event_context.competition.standings_response = resolution.raw.get(
        "standings_response_raw"
    )

    competition_id = event_context.competition.competition_id
    standings_missing = (
        event_context.competition.has_standings_source_endpoint is False
        or resolution.raw.get("skip_reason")
        == "known_missing_standings_source_endpoint"
    )
    if (
        competition_id is not None
        and standings_missing
        and int(competition_id) not in missing_competition_ids
    ):
        missing_competition_ids.add(int(competition_id))
        logger.info(
            "Pre-start metadata marked competition_id=%s as missing standings "
            "endpoint in memory",
            competition_id,
        )

    logger.info(
        "Pre-start metadata resolution result for event %s: source=%s "
        "standings_called=%s should_persist=%s number_of_teams=%s "
        "total_regular_season_games=%s standings_grouping=%s skip_reason=%s",
        event_context.event_id,
        resolution.league_config_source,
        resolution.standings_called,
        resolution.should_persist,
        resolution.number_of_teams,
        resolution.total_regular_season_games,
        resolution.standings_grouping,
        resolution.raw.get("skip_reason"),
    )

    if not resolution.should_persist or competition_id is None:
        return

    try:
        with db_manager.get_session() as session:
            updated = CompetitionRepository.update_competition_metadata_if_better(
                session=session,
                competition_id=competition_id,
                number_of_teams=resolution.number_of_teams,
                total_regular_season_games=resolution.total_regular_season_games,
                standings_grouping=resolution.standings_grouping,
                league_config_source=resolution.league_config_source,
            )
        mark_competition_metadata_refresh_attempted(competition_id)
        logger.info(
            "Competition metadata resolved for event_id=%s competition_id=%s "
            "source=%s standings_called=%s persisted=%s",
            event_context.event_id,
            competition_id,
            resolution.league_config_source,
            resolution.standings_called,
            updated,
        )
    except Exception as exc:
        mark_competition_metadata_refresh_attempted(competition_id)
        logger.warning(
            "Failed to persist competition metadata for event_id=%s "
            "competition_id=%s: %s",
            event_context.event_id,
            competition_id,
            exc,
        )


def flush_missing_standings_endpoints(missing_competition_ids: set[int]) -> None:
    """Persist all confirmed missing standings endpoints in one update."""
    if not missing_competition_ids:
        return

    competition_ids = sorted(missing_competition_ids)
    logger.info(
        "Flushing %d competition(s) with missing standings endpoint before "
        "pillar evaluation: %s",
        len(competition_ids),
        competition_ids if len(competition_ids) <= 20 else competition_ids[:20],
    )
    with db_manager.get_session() as session:
        updated_count = CompetitionRepository.update_has_standings_source_endpoints(
            session=session,
            competition_ids=competition_ids,
            has_standings_source_endpoint=False,
        )
    logger.info(
        "Completed standings endpoint flush for %d competition(s) (updated=%d)",
        len(competition_ids),
        updated_count,
    )


def _hydrate_missing_tennis_metadata(
    scheduler,
    candidates: list[dict],
    key_moments: list[int],
) -> None:
    """Fetch metadata needed by tennis context only when the candidate lacks it."""
    skipped_by_filters = 0

    for candidate in candidates:
        if candidate.get("metadata_snapshot") is not None:
            continue
        if candidate.get("minutes_until_start") not in key_moments:
            continue

        event_data = candidate.get("event_data", {})
        if event_data.get("sport") not in {"Tennis", "Tennis Doubles"}:
            continue

        event_id = candidate.get("event_id")
        event_obj = scheduler.event_repo.get_event_by_id(event_id)
        if not event_obj or not event_obj.round:
            continue

        # Mirror the _build_evaluation_payloads filters so we don't spend an
        # API call on events that evaluation will drop anyway.
        if (
            event_obj.round != "regular_season"
            or event_obj.sport in Config.EXCLUDED_SPORTS
            or event_obj.id in scheduler.recently_rescheduled
        ):
            skipped_by_filters += 1
            continue

        sofascore_event_id = candidate.get("sofascore_event_id")
        if sofascore_event_id is None:
            logger.warning(
                "No sofascore_event_id for event %s, skipping metadata snapshot",
                event_id,
            )
            continue

        try:
            logger.info(
                "Fetching metadata snapshot for tennis event %s during "
                "pre-start context enrichment",
                event_id,
            )
            _, metadata_snapshot = api_client.get_event_results(
                sofascore_event_id,
                update_time=False,
                return_snapshot=True,
                current_start_time=candidate.get("original_start_time"),
                minutes_until_start=candidate.get("minutes_until_start", 0),
            )
            if metadata_snapshot:
                candidate["metadata_snapshot"] = metadata_snapshot
        except Exception as exc:
            logger.warning(
                "Failed to fetch metadata snapshot for event %s during "
                "pre-start enrichment: %s",
                event_id,
                exc,
            )

    if skipped_by_filters:
        logger.info(
            "Skipped tennis metadata hydration for %s event(s) that "
            "evaluation would drop (round/sport/rescheduled filters)",
            skipped_by_filters,
        )


def _load_trajectory_payloads(
    event_ids: set[int],
    key_moments: list[int],
) -> dict[int, list[dict]]:
    refresh_materialized_views(db_manager.engine)
    trajectory_by_event_id = OddsTrajectoryRepository.get_pre_start_trajectory_map(
        event_ids=list(event_ids),
        target_minutes=key_moments,
        tolerance_minutes=Config.PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES,
    )
    logger.info(
        "Loaded odds trajectory for %s/%s key-moment events",
        len(trajectory_by_event_id),
        len(event_ids),
    )
    return {
        event_id: [point.to_dict() for point in points]
        for event_id, points in trajectory_by_event_id.items()
    }


def _build_evaluation_payloads(
    scheduler,
    upcoming_events: list[dict],
    event_plan: PreStartEventPlan,
    key_event_ids: set[int],
    trajectory_payloads: dict[int, list[dict]],
    missing_competition_ids: set[int],
) -> list[dict]:
    payloads: list[dict] = []

    for event_data in upcoming_events:
        event_id = event_data["id"]
        if event_id not in key_event_ids:
            continue

        event_obj = scheduler.event_repo.get_event_by_id(event_id)
        if not event_obj or event_obj.sport in Config.EXCLUDED_SPORTS:
            continue
        if event_obj.id in scheduler.recently_rescheduled:
            continue

        candidate = event_plan.by_event_id.get(event_id, {})
        initial_minutes = candidate.get("minutes_until_start")
        if initial_minutes is None:
            initial_minutes = minutes_until_start(event_obj.start_time_utc)
        event_context = build_event_context(
            event_obj=event_obj,
            minutes_until_start=initial_minutes,
            metadata_snapshot=candidate.get("metadata_snapshot"),
        )
        if event_context is None:
            logger.warning(
                "Skipping event %s because normalized EventContext could not be built",
                event_obj.id,
            )
            continue
        if event_obj.round != "regular_season":
            logger.info(
                "Skipping event %s because round=%s",
                event_obj.id,
                event_obj.round,
            )
            continue

        enrich_event_context_competition_metadata(
            event_context,
            event_obj,
            missing_competition_ids,
        )
        payloads.append(
            {
                "event_obj": event_obj,
                "initial_minutes": initial_minutes,
                "observations": candidate.get("observations"),
                "odds_response": candidate.get("odds_response"),
                "odds_trajectory": trajectory_payloads.get(event_id, []),
                "metadata_snapshot": candidate.get("metadata_snapshot"),
                "event_context": event_context,
                "season_id": getattr(event_obj, "season_id", None),
                "should_send_streak_alert": False,
                "streak_analysis": None,
                "dual_report": None,
                "minutes_until_start": initial_minutes,
                "success": True,
            }
        )
    return payloads


def _log_debug_payloads(payloads: list[dict]) -> None:
    logger.info("EVENTS FOR PILLARS (DEBUG MODE)")
    for index, payload in enumerate(payloads, 1):
        context = payload.get("event_context")
        label = (
            context.participants_label
            if context
            else f"Event {payload.get('event_obj').id}"
        )
        sport = context.sport if context else "Unknown"
        filtered_payload = {
            key: value
            for key, value in payload.items()
            if key not in {"odds_response", "odds_trajectory"}
        }
        logger.info(
            "[%s/%s] %s | sport=%s | minutes=%s\n%s",
            index,
            len(payloads),
            label,
            sport,
            payload.get("minutes_until_start"),
            pprint.pformat(filtered_payload, indent=2, width=120),
        )


def evaluate_pre_start_key_moments(
    scheduler,
    upcoming_events: list[dict],
    event_plan: PreStartEventPlan,
    oddsportal_context: OddsPortalScrapeContext,
    *,
    debug_mode: bool = False,
) -> None:
    """Build shared evaluation payloads, then run enabled alert pipelines."""
    if not (
        Config.ENABLE_LEGACY_ALERT_PIPELINE
        or Config.ENABLE_PILLAR_PIPELINE
    ):
        logger.debug("All pre-start evaluation pipelines are disabled")
        return

    key_moments = Config.PRE_START_ODDS_MOMENTS
    _hydrate_missing_tennis_metadata(
        scheduler,
        event_plan.candidates,
        key_moments,
    )
    key_event_ids = {
        candidate["event_id"]
        for candidate in event_plan.candidates
        if candidate["minutes_until_start"] in key_moments
    }
    if not key_event_ids:
        logger.debug("No events captured at key moments for alert evaluation")
        return

    logger.info(
        "Evaluating %s events at key moments for alerts",
        len(key_event_ids),
    )
    trajectory_payloads = _load_trajectory_payloads(key_event_ids, key_moments)
    missing_competition_ids: set[int] = set()
    payloads = _build_evaluation_payloads(
        scheduler,
        upcoming_events,
        event_plan,
        key_event_ids,
        trajectory_payloads,
        missing_competition_ids,
    )
    if not payloads:
        return

    if Config.ENABLE_LEGACY_ALERT_PIPELINE:
        evaluate_and_dispatch_alerts_batch(
            payloads,
            key_moments,
            scheduler.event_repo,
            op_event_states=oddsportal_context.event_states,
            op_event_ids=oddsportal_context.event_ids,
            op_data_cache=oddsportal_context.data_cache,
            debug_mode=debug_mode,
        )

    flush_missing_standings_endpoints(missing_competition_ids)

    if Config.ENABLE_PILLAR_PIPELINE:
        if debug_mode:
            _log_debug_payloads(payloads)
        evaluate_and_calculate_pillars_batch(
            events_for_pillars=payloads,
            key_moments=key_moments,
            event_repo=scheduler.event_repo,
            op_event_states=oddsportal_context.event_states,
            op_event_ids=oddsportal_context.event_ids,
            op_data_cache=oddsportal_context.data_cache,
            debug_mode=debug_mode,
        )


__all__ = [
    "enrich_event_context_competition_metadata",
    "evaluate_pre_start_key_moments",
    "flush_missing_standings_endpoints",
]
