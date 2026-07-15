"""Pre-start check job orchestrator."""

from __future__ import annotations

import logging
import pprint
from datetime import datetime

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import refresh_materialized_views
from infrastructure.persistence.repositories import CompetitionRepository, EventRepository, OddsTrajectoryRepository
from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.alert_pipeline import evaluate_and_dispatch_alerts_batch
from modules.pillars.context import build_event_context
from modules.pillars.competition_metadata_resolver import (
    apply_competition_metadata_resolution,
    mark_competition_metadata_refresh_attempted,
    resolve_competition_metadata,
)
from modules.jobs.pre_start_check_job.pillar_pipeline import evaluate_and_calculate_pillars_batch
from modules.jobs.pre_start_check_job.in_game_checks import run_in_game_checks
from modules.jobs.pre_start_check_job.oddsportal_worker import (
    build_oddsportal_scrape_candidates,
    create_oddsportal_scrape_state,
    start_oddsportal_scrape_thread,
)
from modules.jobs.pre_start_check_job.intraday_result_freshness import (
    process_intraday_result_freshness,
)
from modules.jobs.pre_start_check_job.rescheduled_events import handle_rescheduled_event
from modules.jobs.pre_start_check_job.timestamp_corrections import (
    check_recently_started_events_for_timestamp_corrections,
)
from modules.jobs.pre_start_check_job.timing import (
    minutes_since_start,
    minutes_until_start,
    should_extract_odds_for_event,
)
from modules.jobs.oddspapi.pre_start_odds.pre_start_odds_job import (
    run_oddspapi_pre_start_odds_ingestion,
)
from modules.oddsportal.oddsportal_config import SEASON_ODDSPORTAL_MAP
from modules.odds_ingestion import MarketOddsIngestionService
from modules.sofascore import api_client
from modules.observations import sport_observation_service
from modules.alerts.matchup_streak_analysis.standings_engine import (
    standings_calculator,
)

logger = logging.getLogger(__name__)


def _enrich_event_context_competition_metadata(
    event_context,
    event_obj,
    standings_endpoint_missing_competition_ids,
) -> None:
    logger.info("💧 started competition meta data hydration through _enrich_event_context_competition_metadata")
    logger.info(
        "Pre-start metadata check for event %s: competition_id=%s source_unique_tournament_id=%s season_id=%s number_of_teams=%s total_regular_season_games=%s standings_grouping=%s league_config_source=%s",
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
        standings_endpoint_missing_competition_ids=standings_endpoint_missing_competition_ids,
    )
    apply_competition_metadata_resolution(event_context, resolution)
    event_context.competition.standings_response = resolution.raw.get("standings_response_raw")
    competition_id = event_context.competition.competition_id
    if (
        competition_id is not None
        and (
            event_context.competition.has_standings_source_endpoint is False
            or resolution.raw.get("skip_reason") == "known_missing_standings_source_endpoint"
        )
        and int(competition_id) not in standings_endpoint_missing_competition_ids
    ):
        standings_endpoint_missing_competition_ids.add(int(competition_id))
        logger.info(
            "Pre-start metadata marked competition_id=%s as missing standings endpoint in memory",
            competition_id,
        )
    logger.info(
        "Pre-start metadata resolution result for event %s: source=%s standings_called=%s should_persist=%s number_of_teams=%s total_regular_season_games=%s standings_grouping=%s skip_reason=%s",
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
        logger.info(
            "Pre-start metadata resolution for event %s will not persist (competition_id=%s)",
            event_context.event_id,
            competition_id,
        )
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
            "Competition metadata resolved for event_id=%s competition_id=%s source=%s standings_called=%s persisted=%s",
            event_context.event_id,
            competition_id,
            resolution.league_config_source,
            resolution.standings_called,
            updated,
        )
    except Exception as exc:
        mark_competition_metadata_refresh_attempted(competition_id)
        logger.warning(
            "Failed to persist competition metadata for event_id=%s competition_id=%s: %s",
            event_context.event_id,
            competition_id,
            exc,
        )


def _flush_missing_standings_endpoints(standings_endpoint_missing_competition_ids) -> None:
    if not standings_endpoint_missing_competition_ids:
        return

    competition_ids = sorted(int(competition_id) for competition_id in standings_endpoint_missing_competition_ids)
    logger.info(
        "Flushing %d competition(s) with missing standings endpoint before pillar evaluation: %s",
        len(competition_ids),
        competition_ids if len(competition_ids) <= 20 else competition_ids[:20],
    )

    with db_manager.get_session() as session:
        updated_count = CompetitionRepository.update_has_standings_source_endpoints(
            session=session,
            competition_ids=standings_endpoint_missing_competition_ids,
            has_standings_source_endpoint=False,
        )

    logger.info(
        "Completed standings endpoint flush for %d competition(s) (updated=%d)",
        len(competition_ids),
        updated_count,
    )


def run_pre_start_check_job(scheduler, global_debug_mode=False) -> None:
    """Run the pre-start check flow."""
    logger.info("🚨 PRE-START CHECK EXECUTED at " + datetime.now().strftime("%H:%M:%S"))

    standings_endpoint_missing_competition_ids: set[int] = set()
    previous_sofascore_evidence_mode = getattr(api_client, "challenge_evidence_enabled", None)
    api_client.set_challenge_evidence_enabled(global_debug_mode)
    logger.info(
        "SofaScore challenge evidence capture %s for pre-start check (debug_mode=%s)",
        "enabled" if global_debug_mode else "disabled",
        global_debug_mode,
    )
    try:
        tracked_season_ids = None
        if Config.TRACKED_SEASONS_ONLY:
            tracked_season_ids = list(SEASON_ODDSPORTAL_MAP.keys())
            logger.info(
                f"Pre-start check restricted to {len(tracked_season_ids)} tracked seasons (TRACKED_SEASONS_ONLY=True)"
            )

        upcoming_events = scheduler.event_repo.get_events_starting_soon(
            Config.PRE_START_WINDOW_MINUTES,
            season_ids=tracked_season_ids,
        )
        logger.info(
            f"Found {len(upcoming_events)} events starting within {Config.PRE_START_WINDOW_MINUTES} minutes"
        )
        
        pre_calculated_timings = {event["id"]: minutes_until_start(event["start_time_utc"]) for event in upcoming_events}

        op_candidates = build_oddsportal_scrape_candidates(upcoming_events, pre_calculated_timings)
        op_event_states = create_oddsportal_scrape_state(op_candidates) if op_candidates else {}
        op_event_ids = set(op_event_states.keys())
        op_data_cache = {}
        start_oddsportal_scrape_thread(scheduler, op_candidates, op_event_states, op_data_cache)

        events_started_recently = scheduler.event_repo.get_events_started_recently(
            window_minutes=Config.INTRADAY_RESULT_FRESHNESS_WINDOW_MINUTES,
            season_ids=tracked_season_ids,
        )
        logger.info(
            "Found %s started events without results within the last %s minutes",
            len(events_started_recently),
            Config.INTRADAY_RESULT_FRESHNESS_WINDOW_MINUTES,
        )

        timestamp_correction_candidates = []
        intraday_result_candidates = []

        for event_data in events_started_recently:
            try:
                minutes_ago = abs(minutes_since_start(event_data["start_time_utc"]))
            except Exception:
                logger.warning(
                    "Could not compute minutes_ago for started event %s",
                    event_data.get("id"),
                )
                continue

            if minutes_ago <= 60:
                timestamp_correction_candidates.append(event_data)
            else:
                intraday_result_candidates.append(event_data)

        logger.info(
            "Started event split: timestamp_correction_candidates=%s, intraday_result_candidates=%s",
            len(timestamp_correction_candidates),
            len(intraday_result_candidates),
        )

        modified_event_ids = check_recently_started_events_for_timestamp_corrections(timestamp_correction_candidates)
        if modified_event_ids:
            upcoming_events = [event for event in upcoming_events if event["id"] not in modified_event_ids]
            logger.info(
                f"ℹ️ Filtered out {len(modified_event_ids)} upcoming events that were just rescheduled/modified"
            )

        scheduler._cleanup_recently_rescheduled()
        upcoming_events = [event for event in upcoming_events if event["id"] not in scheduler.recently_rescheduled]

        logger.info(f"🏆 Starting intraday result freshness for {len(intraday_result_candidates)} events...")
        intraday_result_stats = process_intraday_result_freshness(intraday_result_candidates)
        logger.info(
            "Intraday result freshness completed: %s",
            intraday_result_stats,
        )
        if (
            intraday_result_stats.get("results_upserted", 0) > 0
            or intraday_result_stats.get("deleted_events", 0) > 0
        ):
            standings_calculator.clear_cache()
            logger.info(
                "Cleared standings calculator cache after intraday result freshness changes"
            )

        run_in_game_checks()

        events_to_process = []
        event_meta_lookup = {}
        key_moments = Config.PRE_START_ODDS_MOMENTS

        if not upcoming_events:
            logger.warning("No upcoming events found")
            return

        logger.info(f"✅ Events confirmed available for processing")
        logger.info(f"🚨 Starting pre-start checking for {len(upcoming_events)} events starting soon...")
        for event_data in upcoming_events:
            try:
                event_id = event_data["id"]
                minutes = pre_calculated_timings.get(event_id, minutes_until_start(event_data["start_time_utc"]))
                should_extract_odds, metadata_snapshot, timing_changed, sofascore_event_id = should_extract_odds_for_event(
                    event_id,
                    minutes,
                    event_data.get("start_time_utc"),
                )

                if timing_changed:
                    scheduler.recently_rescheduled.add(event_id)
                    handle_rescheduled_event(event_id, scheduler.event_repo, minutes, metadata_snapshot=metadata_snapshot, sofascore_event_id=sofascore_event_id)

                refreshed_event = scheduler.event_repo.get_event_by_id(event_id)
                if refreshed_event:
                    event_data["season_id"] = refreshed_event.season_id
                    event_data["start_time_utc"] = refreshed_event.start_time_utc

                events_to_process.append(
                    {
                        "event_id": event_id,
                        "event_data": event_data,
                        "minutes_until_start": minutes,
                        "should_extract_odds": should_extract_odds,
                        "original_start_time": event_data["start_time_utc"],
                        "metadata_snapshot": metadata_snapshot,
                        "sofascore_event_id": sofascore_event_id,
                    }
                )
                event_meta_lookup[event_id] = events_to_process[-1]
            except Exception as exc:
                logger.error(f"Error processing upcoming event {event_data.get('id', 'unknown')}: {exc}")

        events_with_odds_extracted = []
        for event_info in events_to_process:
            try:
                event_data = event_info["event_data"]
                minutes = event_info["minutes_until_start"]

                if not event_info["should_extract_odds"]:
                    continue

                sofascore_event_id = event_info.get("sofascore_event_id")
                if sofascore_event_id is None:
                    logger.warning("No sofascore_event_id available for event %s, skipping odds extraction", event_data["id"])
                    continue
                final_odds_response = api_client.get_event_final_odds(sofascore_event_id, event_data["slug"])
                if not final_odds_response:
                    continue

                event_info["odds_response"] = final_odds_response
                ingestion_result = MarketOddsIngestionService.save_from_event_odds_response(
                    event_data["id"],
                    final_odds_response,
                    source="sofascore",
                    home_team=event_data.get("home_team"),
                    away_team=event_data.get("away_team"),
                    debug_mode=global_debug_mode,
                )
                if ingestion_result.markets_saved > 0 or ingestion_result.dual_process_market_available:
                    event_info["event_with_odds"] = {
                        "event_id": event_data["id"],
                        "start_time": event_data["start_time_utc"],
                        "initial_minutes": minutes,
                    }
                    events_with_odds_extracted.append(event_info["event_with_odds"])

                    if event_data["sport"] in ["Tennis", "Tennis Doubles"]:
                        if not sport_observation_service.event_has_observations(event_data["id"]):
                            snapshot = event_info.get("metadata_snapshot")
                            if snapshot and snapshot.get("observations"):
                                event_info["observations"] = snapshot["observations"]
                            else:
                                observations = api_client.get_event_results(
                                    sofascore_event_id,
                                    canonical_event_id=event_data["id"],
                                    update_court_type=True,
                                )
                                if observations:
                                    event_info["observations"] = observations
                else:
                    logger.warning("No market odds saved for event %s: %s", event_data["id"], ingestion_result.reason)
            except Exception as exc:
                logger.error(f"Error processing upcoming event odds {event_info.get('event_id', 'unknown')}: {exc}")

        try:
            oddspapi_pre_start_summary = run_oddspapi_pre_start_odds_ingestion(
                events_to_process,
                debug_mode=global_debug_mode,
            )
            logger.info(
                "Oddspapi pre-start odds ingestion completed candidates=%s mapped=%s requests=%s "
                "ingested=%s skipped=%s failed=%s snapshots_saved=%s",
                oddspapi_pre_start_summary.candidates_seen,
                oddspapi_pre_start_summary.candidates_with_mapping,
                oddspapi_pre_start_summary.requests_attempted,
                oddspapi_pre_start_summary.events_ingested,
                oddspapi_pre_start_summary.events_skipped,
                oddspapi_pre_start_summary.events_failed,
                oddspapi_pre_start_summary.snapshots_saved,
            )
        except Exception:
            logger.exception("Oddspapi pre-start odds ingestion failed")

        for event_info in events_to_process:
            if event_info.get("metadata_snapshot") is not None:
                continue
            if event_info.get("minutes_until_start") not in key_moments:
                continue

            event_data = event_info.get("event_data", {})
            sport = event_data.get("sport")

            if sport in ["Tennis", "Tennis Doubles"]:
                event_id = event_info.get("event_id")
                event_obj = scheduler.event_repo.get_event_by_id(event_id)
                if not event_obj or not event_obj.round:
                    continue
                try:
                    logger.info(
                        "Fetching metadata snapshot for event %s for pre-start context enrichment, useful for tennis events",
                        event_id,
                    )
                    sofascore_event_id = event_info.get("sofascore_event_id")
                    if sofascore_event_id is None:
                        logger.warning("No sofascore_event_id for event %s, skipping metadata snapshot", event_id)
                        continue
                    _, metadata_snapshot = api_client.get_event_results(
                        sofascore_event_id,
                        update_time=False,
                        return_snapshot=True,
                        current_start_time=event_info.get("original_start_time"),
                        minutes_until_start=event_info.get("minutes_until_start", 0),
                    )
                    if metadata_snapshot:
                        event_info["metadata_snapshot"] = metadata_snapshot
                except Exception as exc:
                    logger.warning(
                        "Failed to fetch metadata snapshot for event %s during pre-start enrichment: %s",
                        event_id,
                        exc,
                    )

        if events_to_process:
            logger.info(f"Pre-start check completed: {len(events_to_process)} games starting soon!")

        all_key_moment_event_ids = {
            info["event_id"] for info in events_to_process if info["minutes_until_start"] in key_moments
        }

        if all_key_moment_event_ids:
            logger.info(f"🔍 Evaluating {len(all_key_moment_event_ids)} events at key moments for alerts (main thread)...")
            refresh_materialized_views(db_manager.engine)

            trajectory_by_event_id = OddsTrajectoryRepository.get_pre_start_trajectory_map(
                event_ids=list(all_key_moment_event_ids),
                target_minutes=key_moments,
                tolerance_minutes=Config.PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES,
            )
            logger.info(
                "Loaded odds trajectory for %s/%s key-moment events",
                len(trajectory_by_event_id),
                len(all_key_moment_event_ids),
            )
            trajectory_payload_by_event_id = {
                event_id: [point.to_dict() for point in points]
                for event_id, points in trajectory_by_event_id.items()
            }

            events_for_alerts = []
            for event_data in upcoming_events:
                if event_data["id"] not in all_key_moment_event_ids:
                    continue
                event_data["odds_trajectory"] = trajectory_payload_by_event_id.get(event_data["id"], [])
                event_obj = scheduler.event_repo.get_event_by_id(event_data["id"])
                if not event_obj or event_obj.sport in Config.EXCLUDED_SPORTS:
                    continue
                if event_obj.id in scheduler.recently_rescheduled:
                    continue

                meta = event_meta_lookup.get(event_data["id"], {})
                initial_minutes = meta.get("minutes_until_start", minutes_until_start(event_obj.start_time_utc))
                event_context = build_event_context(
                    event_obj=event_obj,
                    minutes_until_start=initial_minutes,
                    metadata_snapshot=meta.get("metadata_snapshot"),
                )
                if event_context is None:
                    logger.warning(
                        "⚠️ Skipping event %s because normalized EventContext could not be built",
                        event_obj.id,
                    )
                    continue

                round = event_obj.round
                if round != "regular_season":
                    logger.info(f"🚫Skipping event {event_obj.id}, round: {round}")
                    continue

                _enrich_event_context_competition_metadata(
                    event_context,
                    event_obj,
                    standings_endpoint_missing_competition_ids,
                )

                events_for_alerts.append(
                    {
                        "event_obj": event_obj,
                        "initial_minutes": initial_minutes,
                        "observations": meta.get("observations"),
                        "odds_response": meta.get("odds_response"),
                        "odds_trajectory": event_data.get("odds_trajectory", []),
                        "metadata_snapshot": meta.get("metadata_snapshot"),
                        "event_context": event_context,
                        "season_id": getattr(event_obj, "season_id", None),
                        "should_send_streak_alert": False,
                        "streak_analysis": None,
                        "dual_report": None,
                        "minutes_until_start": initial_minutes,
                        "success": True,
                    }
                )

            if events_for_alerts:
                # Legacy alert pipeline
                if Config.ENABLE_LEGACY_ALERT_PIPELINE:
                    evaluate_and_dispatch_alerts_batch(
                        events_for_alerts,
                        key_moments,
                        scheduler.event_repo,
                        op_event_states=op_event_states,
                        op_event_ids=op_event_ids,
                        op_data_cache=op_data_cache,
                        debug_mode=global_debug_mode
                    )

                _flush_missing_standings_endpoints(standings_endpoint_missing_competition_ids)

                # New pillar pipeline
                if Config.ENABLE_PILLAR_PIPELINE:
                    if global_debug_mode:
                        # print events for pillars for debugging
                        logger.info("\n" + "═" * 80)
                        logger.info("🚨 EVENTS FOR PILLARS (DEBUG MODE)")
                        logger.info("═" * 80)
                        for i, event in enumerate(events_for_alerts, 1):
                            ctx = event.get('event_context')
                            label = ctx.participants_label if ctx else f"Event {event.get('event_obj').id}"
                            sport = ctx.sport if ctx else "Unknown"
                            minutes = event.get('minutes_until_start')

                            logger.info(f"\n📍 [{i}/{len(events_for_alerts)}] {label}")
                            logger.info(f"   Sport: {sport} | Minutes until start: {minutes}")
                            logger.info("-" * 40)
                            filtered_event = {k: v for k, v in event.items() if k not in ('odds_response', 'odds_trajectory')}
                            logger.info(pprint.pformat(filtered_event, indent=2, width=120))
                            logger.info("─" * 80)

                    evaluate_and_calculate_pillars_batch(
                        events_for_pillars=events_for_alerts,
                        key_moments=key_moments,
                        event_repo=scheduler.event_repo,
                        op_event_states=op_event_states,
                        op_event_ids=op_event_ids,
                        op_data_cache=op_data_cache,
                        debug_mode=global_debug_mode,
                    )
        else:
            logger.debug("No events captured at key moments for alert evaluation")
    except Exception as exc:
        logger.error(f"Error in Job C: {exc}")
    finally:
        if previous_sofascore_evidence_mode is not None:
            api_client.set_challenge_evidence_enabled(previous_sofascore_evidence_mode)


