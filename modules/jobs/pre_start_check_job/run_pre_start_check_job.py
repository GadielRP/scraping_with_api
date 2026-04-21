"""Pre-start check job orchestrator."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import refresh_materialized_views
from infrastructure.persistence.repositories import EventRepository, OddsRepository, ObservationRepository
from infrastructure.settings import Config
from modules.alerts.basketball_4q import basketball_4q_monitor
from modules.jobs.pre_start_check_job.alert_pipeline import build_event_payload, evaluate_and_send_alerts_batch
from modules.jobs.pre_start_check_job.in_game_checks import run_in_game_checks
from modules.jobs.pre_start_check_job.odds_extraction import extract_final_odds_from_response
from modules.jobs.pre_start_check_job.rescheduled_events import handle_rescheduled_event
from modules.jobs.pre_start_check_job.timestamp_corrections import check_recently_started_events_for_timestamp_corrections
from modules.jobs.pre_start_check_job.timing import minutes_until_start, should_extract_odds_for_event
from modules.oddsportal.oddsportal_config import SEASON_ODDSPORTAL_MAP
from modules.sofascore import api_client

from sport_observations import sport_observations_manager

logger = logging.getLogger(__name__)


def run_pre_start_check_job(scheduler) -> None:
    """Run the pre-start check flow."""
    logger.info("🚨 PRE-START CHECK EXECUTED at " + datetime.now().strftime("%H:%M:%S"))

    try:
        tracked_season_ids = None
        if Config.TRACKED_SEASONS_ONLY:
            tracked_season_ids = list(SEASON_ODDSPORTAL_MAP.keys())
            logger.info(
                f"Pre-start check restricted to {len(tracked_season_ids)} tracked seasons (TRACKED_SEASONS_ONLY=True)"
            )

        upcoming_events = scheduler.event_repo.get_events_starting_soon_with_odds(
            Config.PRE_START_WINDOW_MINUTES,
            season_ids=tracked_season_ids,
        )
        logger.info(
            f"Found {len(upcoming_events)} events starting within {Config.PRE_START_WINDOW_MINUTES} minutes"
        )

        pre_calculated_timings = {event["id"]: minutes_until_start(event["start_time_utc"]) for event in upcoming_events}

        # OddsPortal worker is currently disabled, but we keep the hook.
        scheduler._active_op_thread = None

        events_started_recently = scheduler.event_repo.get_events_started_recently(
            window_minutes=60,
            season_ids=tracked_season_ids,
        )
        logger.info(
            f"Found {len(events_started_recently)} events that started recently (checking for late timestamp corrections)"
        )

        modified_event_ids = check_recently_started_events_for_timestamp_corrections(events_started_recently)
        if modified_event_ids:
            upcoming_events = [event for event in upcoming_events if event["id"] not in modified_event_ids]
            logger.info(
                f"ℹ️ Filtered out {len(modified_event_ids)} upcoming events that were just rescheduled/modified"
            )

        scheduler._cleanup_recently_rescheduled()
        upcoming_events = [event for event in upcoming_events if event["id"] not in scheduler.recently_rescheduled]

        run_in_game_checks()

        events_to_process = []
        event_meta_lookup = {}
        key_moments = [120, 30, 5, 0, -5]

        for event_data in upcoming_events:
            try:
                event_id = event_data["id"]
                minutes = pre_calculated_timings.get(event_id, minutes_until_start(event_data["start_time_utc"]))
                should_extract_odds, metadata_snapshot, timing_changed = should_extract_odds_for_event(
                    event_id,
                    minutes,
                    event_data.get("start_time_utc"),
                )

                if timing_changed:
                    scheduler.recently_rescheduled.add(event_id)
                    handle_rescheduled_event(event_id, scheduler.event_repo, minutes, metadata_snapshot=metadata_snapshot)

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

                final_odds_response = api_client.get_event_final_odds(event_data["id"], event_data["slug"])
                if not final_odds_response:
                    continue

                event_info["odds_response"] = final_odds_response
                final_odds_data = extract_final_odds_from_response(final_odds_response, initial_odds_extraction=True)
                if not final_odds_data:
                    continue

                upserted_id = OddsRepository.upsert_event_odds(event_data["id"], final_odds_data)
                if not upserted_id:
                    continue

                if OddsRepository.create_odds_snapshot(event_data["id"], final_odds_data):
                    event_info["event_with_odds"] = {
                        "event_id": event_data["id"],
                        "start_time": event_data["start_time_utc"],
                        "initial_minutes": minutes,
                    }
                    events_with_odds_extracted.append(event_info["event_with_odds"])

                    try:
                        from infrastructure.persistence.repositories import MarketRepository

                        MarketRepository.save_markets_from_response(event_data["id"], final_odds_response)
                    except Exception as market_exc:
                        logger.warning(f"Error saving markets to DB for event {event_data['id']}: {market_exc}")

                    if event_data["sport"] in ["Tennis", "Tennis Doubles"]:
                        if not sport_observations_manager.has_observations_for_event(event_data["id"]):
                            snapshot = event_info.get("metadata_snapshot")
                            if snapshot and snapshot.get("observations"):
                                event_info["observations"] = snapshot["observations"]
                            else:
                                observations = api_client.get_event_results(event_id=event_data["id"], update_court_type=True)
                                if observations:
                                    event_info["observations"] = observations
            except Exception as exc:
                logger.error(f"Error processing upcoming event odds {event_info.get('event_id', 'unknown')}: {exc}")

        if events_to_process:
            logger.info(f"Pre-start check completed: {len(events_to_process)} games starting soon!")

        all_key_moment_event_ids = {
            info["event_id"] for info in events_to_process if info["minutes_until_start"] in key_moments
        }

        if all_key_moment_event_ids:
            logger.info(f"🔍 Evaluating {len(all_key_moment_event_ids)} events at key moments for alerts (main thread)...")
            refresh_materialized_views(db_manager.engine)

            events_for_alerts = []
            for event_data in upcoming_events:
                if event_data["id"] not in all_key_moment_event_ids:
                    continue
                event_obj = scheduler.event_repo.get_event_by_id(event_data["id"])
                if not event_obj or event_obj.sport in Config.EXCLUDED_SPORTS:
                    continue
                if event_obj.id in scheduler.recently_rescheduled:
                    continue

                meta = event_meta_lookup.get(event_data["id"], {})
                initial_minutes = meta.get("minutes_until_start", minutes_until_start(event_obj.start_time_utc))
                events_for_alerts.append(
                    {
                        "event_obj": event_obj,
                        "initial_minutes": initial_minutes,
                        "observations": meta.get("observations"),
                        "odds_response": meta.get("odds_response"),
                        "metadata_snapshot": meta.get("metadata_snapshot"),
                        "season_id": getattr(event_obj, "season_id", None),
                        "should_send_streak_alert": False,
                        "streak_analysis": None,
                        "dual_report": None,
                        "minutes_until_start": initial_minutes,
                        "success": True,
                    }
                )

            if events_for_alerts:
                evaluate_and_send_alerts_batch(
                    events_for_alerts,
                    key_moments,
                    scheduler.event_repo,
                    op_event_states={},
                    op_event_ids=set(),
                    op_data_cache={},
                )
        else:
            logger.debug("No events captured at key moments for alert evaluation")
    except Exception as exc:
        logger.error(f"Error in Job C: {exc}")
