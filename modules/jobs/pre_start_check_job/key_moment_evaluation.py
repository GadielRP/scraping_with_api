"""Prepare and execute alert and pillar evaluation at pre-start key moments."""

from __future__ import annotations

from shared.execution_context import WorkDeferred, check_execution_budget

import logging
import pprint

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories import (
    CompetitionRepository,
    OddsTrajectoryLoadError,
    OddsTrajectoryPoint,
    OddsTrajectoryRepository,
)
from infrastructure.settings import Config
from modules.competition.tracked_competitions import is_tracked_competition
from modules.jobs.pre_start_check_job.alert_pipeline import (
    evaluate_and_dispatch_alerts_batch,
)
from modules.jobs.pre_start_check_job.event_candidate_builder import PreStartEventPlan
from modules.jobs.pre_start_check_job.oddsportal_worker import OddsPortalScrapeContext
from modules.jobs.pre_start_check_job.pillar_pipeline import (
    evaluate_and_calculate_pillars_batch,
)
from modules.jobs.pre_start_check_job.timing import minutes_until_start
from modules.pillars.context import EventContext, build_event_context
from modules.sofascore import api_client
from modules.sports.catalog import is_supported_sport_name

logger = logging.getLogger(__name__)


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
    runtime,
    candidates: list[dict],
    key_moments: list[int],
) -> None:
    """Prefetch SofaScore /event snapshots for tennis candidates missing them.

    This is NOT EventContext construction. It only fills
    ``candidate["metadata_snapshot"]`` so ``build_event_context`` can later
    resolve tennis participant/competition IDs when DB relations are incomplete.
    """
    logger.info(
        "🎾 START tennis event-snapshot prefetch "
        "(candidates=%s key_moments=%s)",
        len(candidates),
        key_moments,
    )
    skipped_by_filters = 0
    already_had_snapshot = 0
    fetch_attempted = 0
    fetch_succeeded = 0
    fetch_failed = 0
    missing_sofascore_id = 0

    for candidate in candidates:
        check_execution_budget()
        if candidate.get("metadata_snapshot") is not None:
            already_had_snapshot += 1
            continue
        if candidate.get("minutes_until_start") not in key_moments:
            continue

        event_data = candidate.get("event_data", {})
        if event_data.get("sport") not in {"Tennis", "Tennis Doubles"}:
            continue

        event_id = candidate.get("event_id")
        event_obj = runtime.event_repo.get_event_by_id(event_id)
        if event_obj is not None:
            # Cache the loaded event so _build_evaluation_payloads can reuse
            # it instead of repeating the same joined query.
            candidate["event_obj"] = event_obj
        if not event_obj or not event_obj.round:
            continue

        # Mirror the _build_evaluation_payloads filters so we don't spend an
        # API call on events that evaluation will drop anyway.
        if (
            event_obj.round != "regular_season"
            or not is_supported_sport_name(event_obj.sport)
            or event_obj.id in runtime.recently_rescheduled
        ):
            skipped_by_filters += 1
            continue

        sofascore_event_id = candidate.get("sofascore_event_id")
        if sofascore_event_id is None:
            missing_sofascore_id += 1
            logger.warning(
                "No sofascore_event_id for event %s, skipping metadata snapshot",
                event_id,
            )
            continue

        try:
            logger.info(
                "Fetching SofaScore /event snapshot for tennis event %s "
                "(used later by EventContext build)",
                event_id,
            )
            fetch_attempted += 1
            _, metadata_snapshot = api_client.get_event_results(
                sofascore_event_id,
                update_time=False,
                return_snapshot=True,
                current_start_time=candidate.get("original_start_time"),
                minutes_until_start=candidate.get("minutes_until_start", 0),
            )
            if metadata_snapshot:
                candidate["metadata_snapshot"] = metadata_snapshot
                fetch_succeeded += 1
            else:
                fetch_failed += 1
        except WorkDeferred:
            raise
        except Exception as exc:
            fetch_failed += 1
            logger.warning(
                "Failed to fetch metadata snapshot for event %s during "
                "tennis event-snapshot prefetch: %s",
                event_id,
                exc,
            )

    logger.info(
        "🎾 END tennis event-snapshot prefetch "
        "(already_had_snapshot=%s skipped_by_filters=%s missing_sofascore_id=%s "
        "fetch_attempted=%s fetch_succeeded=%s fetch_failed=%s)",
        already_had_snapshot,
        skipped_by_filters,
        missing_sofascore_id,
        fetch_attempted,
        fetch_succeeded,
        fetch_failed,
    )


def _load_trajectory_payloads(
    event_ids: set[int],
) -> dict[int, list[OddsTrajectoryPoint]]:
    """Load complete pillar histories while keeping persistence failures explicit."""
    try:
        trajectory_by_event_id = OddsTrajectoryRepository.get_pre_start_trajectory_map(
            event_ids=list(event_ids),
        )
    except OddsTrajectoryLoadError:
        logger.warning(
            "Pillar evaluation will continue without odds trajectory for "
            "%s event(s) after a persistence failure",
            len(event_ids),
        )
        return {}

    logger.info(
        "Loaded odds trajectory for %s/%s key-moment events",
        len(trajectory_by_event_id),
        len(event_ids),
    )
    return trajectory_by_event_id



def _build_evaluation_payloads(
    runtime,
    event_plan: PreStartEventPlan,
    key_event_ids: set[int],
    missing_competition_ids: set[int],
) -> list[EventContext]:
    """Build event contexts; P1 resolves the shared sporting profile before history.
    """
    # --- Phase 1: EventContext construction ---
    logger.info(
        "🧩 START EventContext construction "
        "(key_moment_events=%s candidates=%s)",
        len(key_event_ids),
        len(event_plan.candidates),
    )
    prepared: list[dict] = []
    skipped_unsupported_or_rescheduled = 0
    skipped_context_build = 0
    skipped_non_regular = 0

    for candidate in event_plan.candidates:
        check_execution_budget()
        event_id = candidate["event_id"]
        if event_id not in key_event_ids:
            continue

        # Reuse the event loaded during tennis snapshot prefetch when
        # available; otherwise load it once here.
        event_obj = candidate.get("event_obj")
        if event_obj is None:
            event_obj = runtime.event_repo.get_event_by_id(event_id)
        if not event_obj or not is_supported_sport_name(event_obj.sport):
            skipped_unsupported_or_rescheduled += 1
            continue
        if event_obj.id in runtime.recently_rescheduled:
            skipped_unsupported_or_rescheduled += 1
            continue

        initial_minutes = candidate.get("minutes_until_start")
        if initial_minutes is None:
            initial_minutes = minutes_until_start(event_obj.starts_at)
        event_context = build_event_context(
            event_obj=event_obj,
            minutes_until_start=initial_minutes,
            metadata_snapshot=candidate.get("metadata_snapshot"),
            observations=candidate.get("observations"),
            odds_trajectory=[],
            success=True,
        )
        if event_context is None:
            skipped_context_build += 1
            logger.warning(
                "🚫 Skipping event %s because normalized EventContext could not be built",
                event_obj.id,
            )
            continue
        if event_obj.round != "regular_season":
            skipped_non_regular += 1
            logger.info(
                "🚫 Skipping event %s because round=%s",
                event_obj.id,
                event_obj.round,
            )
            continue

        prepared.append(
            {
                "candidate": candidate,
                "event_obj": event_obj,
                "event_context": event_context,
                "initial_minutes": initial_minutes,
            }
        )

    logger.info(
        "🧩 END EventContext construction "
        "(contexts_built=%s skipped_unsupported_or_rescheduled=%s "
        "skipped_context_build=%s skipped_non_regular=%s)",
        len(prepared),
        skipped_unsupported_or_rescheduled,
        skipped_context_build,
        skipped_non_regular,
    )

    return [item["event_context"] for item in prepared]


def _log_debug_payloads(contexts: list[EventContext]) -> None:
    logger.info("EVENTS FOR PILLARS (DEBUG MODE)")
    for index, context in enumerate(contexts, 1):
        check_execution_budget()
        label = (
            context.participants_label
            if context
            else f"Event {getattr(context, 'event_id', '?')}"
        )
        sport = context.sport if context else "Unknown"
        logger.info(
            "[%s/%s] %s | sport=%s | minutes=%s\n%s",
            index,
            len(contexts),
            label,
            sport,
            context.minutes_until_start,
            pprint.pformat(context, indent=2, width=120),
        )


def _select_pipeline_candidates(
    candidates: list[dict],
    key_moments: list[int],
) -> list[dict]:
    """Select key-moment candidates shared by alert and pillar pipelines."""
    key_candidates = [
        candidate
        for candidate in candidates
        if candidate["minutes_until_start"] in key_moments
    ]
    if not Config.FILTER_PIPELINES_BY_TRACKED_COMPETITIONS:
        return key_candidates

    tracked_candidates = [
        candidate
        for candidate in key_candidates
        if is_tracked_competition(
            candidate.get("event_data", {}).get("competition_id")
        )
    ]
    skipped_count = len(key_candidates) - len(tracked_candidates)
    if skipped_count:
        logger.info(
            "🚫 Pipeline competition ID filter skipped %s/%s key-moment events",
            skipped_count,
            len(key_candidates),
        )
    return tracked_candidates


def evaluate_pre_start_key_moments(
    runtime,
    event_plan: PreStartEventPlan,
    oddsportal_context: OddsPortalScrapeContext,
    *,
    debug_mode: bool = False,
    enable_alert_pipeline: bool | None = None,
    enable_pillar_pipeline: bool | None = None,
    enabled_pillars: dict[str, bool] | None = None,
) -> None:
    """Build shared evaluation payloads, then run enabled alert pipelines."""
    legacy_alerts_enabled = (
        Config.ENABLE_LEGACY_ALERT_PIPELINE
        if enable_alert_pipeline is None
        else enable_alert_pipeline
    )
    pillars_enabled = (
        Config.ENABLE_PILLAR_PIPELINE
        if enable_pillar_pipeline is None
        else enable_pillar_pipeline
    )

    if not (legacy_alerts_enabled or pillars_enabled):
        logger.debug("All pre-start evaluation pipelines are disabled")
        return

    key_moments = Config.PRE_START_ODDS_MOMENTS
    pipeline_candidates = _select_pipeline_candidates(
        event_plan.candidates,
        key_moments,
    )
    if not pipeline_candidates:
        logger.debug(
            "No events eligible for alert or pillar evaluation at key moments"
        )
        return

    _hydrate_missing_tennis_metadata(
        runtime,
        pipeline_candidates,
        key_moments,
    )
    key_event_ids = {
        candidate["event_id"]
        for candidate in pipeline_candidates
    }

    logger.info(
        "Evaluating %s events at key moments for alert and pillar pipelines",
        len(key_event_ids),
    )
    missing_competition_ids: set[int] = set()
    contexts = _build_evaluation_payloads(
        runtime,
        event_plan,
        key_event_ids,
        missing_competition_ids,
    )
    if not contexts:
        return

    if legacy_alerts_enabled:
        evaluate_and_dispatch_alerts_batch(
            contexts,
            key_moments,
            runtime.event_repo,
            op_event_states=oddsportal_context.event_states,
            op_event_ids=oddsportal_context.event_ids,
            debug_mode=debug_mode,
        )

    flush_missing_standings_endpoints(missing_competition_ids)

    if pillars_enabled:
        pillar_contexts = contexts
        allowed_pillar_moments = Config.PILLAR_PIPELINE_EXECUTION_MOMENTS
        if allowed_pillar_moments:
            pillar_contexts = [
                context
                for context in contexts
                if getattr(context, "minutes_until_start", None) in allowed_pillar_moments
            ]
            skipped_moment_count = len(contexts) - len(pillar_contexts)
            if skipped_moment_count:
                logger.info(
                    "🚫 Pillar pipeline key-moment filter skipped %s/%s events (allowed moments: %s)",
                    skipped_moment_count,
                    len(contexts),
                    allowed_pillar_moments,
                )

        if pillar_contexts:
            validated_event_ids = {
                context.event_id if isinstance(context, EventContext) else int(context["event_id"])
                for context in pillar_contexts
            }
            logger.info(
                "📈 Loading odds trajectory for %s validated pillar event(s)",
                len(validated_event_ids),
            )
            trajectory_payloads = _load_trajectory_payloads(
                validated_event_ids,
            )
            # The loaded rows are the exact snapshot set evaluated by the pillars.
            # Capture the boundary after loading so writes completed during the
            # query are not accidentally cut off at the nominal key moment.
            evaluate_and_calculate_pillars_batch(
                events_for_pillars=pillar_contexts,
                debug_mode=debug_mode,
                enabled_pillars=enabled_pillars,
                trajectories_by_event_id=trajectory_payloads,
            )


__all__ = [
    "enrich_event_context_competition_metadata",
    "evaluate_pre_start_key_moments",
    "flush_missing_standings_endpoints",
]
