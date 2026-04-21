"""Alert pipeline for the pre-start job."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import PredictionLog, refresh_materialized_views
from infrastructure.persistence.repositories import MarketRepository, ObservationRepository
from infrastructure.settings import Config
from modules.alerts import pre_start_notifier
from modules.alerts.alerts_formatter.matchup_streak_alert import send_matchup_streak_alerts
from modules.alerts.alerts_formatter.odds_alert import send_odds_alert
from modules.alerts.matchup_streak_analysis import build_matchup_streak_context, should_send_streak_alert
from modules.prediction import prediction_logger
from modules.oddsportal.oddsportal_config import SEASON_ODDSPORTAL_MAP
from modules.sofascore import api_client

logger = logging.getLogger(__name__)


def evaluate_and_send_alerts_batch(
    events_for_alerts: list,
    key_moments: list,
    event_repo,
    op_event_states=None,
    op_event_ids=None,
    op_data_cache=None,
):
    """Evaluate upcoming events and send alerts."""

    def _send_event_alerts(result):
        if not result.get("success"):
            return

        event_obj = result.get("event_obj")
        streak_analysis = result.get("streak_analysis")
        should_send_streak_alert_flag = result.get("should_send_streak_alert", False)
        dual_report = result.get("dual_report")
        odds_response = result.get("odds_response")
        minutes_until_start = result.get("minutes_until_start")
        op_data = None

        if odds_response and op_event_ids and event_obj.id in op_event_ids and op_event_states:
            state = op_event_states.get(event_obj.id)
            if state:
                if state["done_event"].is_set():
                    logger.info(f"[OP] Event {event_obj.id} already completed; alert thread unblocked")
                else:
                    logged_queue = False
                    while not state["started_event"].is_set() and not state["done_event"].is_set():
                        if not logged_queue:
                            logger.info(
                                f"[OP] Event {event_obj.id} queued; waiting for worker claim before starting post-start timeout"
                            )
                            logged_queue = True
                        time.sleep(0.25)

                    if not state["done_event"].is_set():
                        started_at = state.get("started_at_monotonic")
                        if started_at is not None:
                            elapsed = time.monotonic() - started_at
                            timeout_s = getattr(Config, "ODDSPORTAL_ALERT_WAIT_TIMEOUT", 180)
                            remaining = max(0.0, float(timeout_s) - elapsed)

                            if remaining > 0:
                                logger.info(
                                    f"[OP] Event {event_obj.id} started {elapsed:.1f}s ago; waiting up to {remaining:.1f}s more for completion"
                                )
                                signaled = state["done_event"].wait(timeout=remaining)
                                if signaled:
                                    logger.info(
                                        f"[OP] Worker signaled completion for event {event_obj.id}. Verifying DB availability..."
                                    )
                                else:
                                    logger.warning(
                                        f"[OP] Timed out waiting for OddsPortal for event {event_obj.id} after {timeout_s}s post-start."
                                    )
                            else:
                                logger.warning(
                                    f"[OP] Event {event_obj.id} already exceeded post-start timeout ({elapsed:.1f}s > {timeout_s}s)."
                                )
                        else:
                            logger.warning(
                                f"[OP] Missing started_at tracking for event {event_obj.id}, unblocking immediately."
                            )

                try:
                    op_markets = MarketRepository.get_oddsportal_markets_for_event(event_obj.id)
                    if op_markets:
                        logger.info(
                            f"[OP] OddsPortal data is available for event {event_obj.id} ({len(op_markets)} rows) - OddsPortal section should be included."
                        )
                    else:
                        logger.info(
                            f"[OP] OddsPortal data is NOT available for event {event_obj.id} - OddsPortal section will NOT be included."
                        )
                except Exception as op_check_err:
                    logger.warning(
                        f"[OP] Could not verify OddsPortal DB availability for event {event_obj.id}: {op_check_err}"
                    )
                op_data = op_data_cache.get(event_obj.id) if op_data_cache else None

        if streak_analysis is None and minutes_until_start == 30 and getattr(event_obj, "custom_id", None):
            try:
                matchup_response = api_client.get_h2h_events_for_event(event_obj.custom_id)
                matchup_events = matchup_response.get("events", []) if matchup_response else []
                streak_analysis = build_matchup_streak_context(
                    event_id=event_obj.id,
                    event_custom_id=event_obj.custom_id,
                    event_start_time=event_obj.start_time_utc,
                    sport=event_obj.sport,
                    discovery_source=event_obj.discovery_source,
                    tournament_id=None,
                    competition_name=getattr(event_obj, "competition", None),
                    competition_slug=None,
                    season_id=getattr(event_obj, "season_id", None),
                    season_name=None,
                    season_year=None,
                    participants=f"{event_obj.home_team} vs {event_obj.away_team}",
                    home_team_name=event_obj.home_team,
                    away_team_name=event_obj.away_team,
                    matchup_events=matchup_events,
                    minutes_until_start=minutes_until_start,
                    observations=result.get("observations"),
                    home_team_id=None,
                    away_team_id=None,
                    event_odds=getattr(event_obj, "event_odds", None),
                )
                should_send_streak_alert_flag = bool(streak_analysis and should_send_streak_alert(streak_analysis))
            except Exception as exc:
                logger.error(f"Error generating matchup streak analysis for event {event_obj.id}: {exc}")

        if dual_report is None:
            discovery_source = getattr(event_obj, "discovery_source", None)
            season_id = getattr(event_obj, "season_id", None)
            is_selected_source = discovery_source in Config.DISCOVERY_SOURCES_FOR_ALERTS
            is_tracked_season = season_id in SEASON_ODDSPORTAL_MAP
            if (is_selected_source or is_tracked_season) and minutes_until_start in {30, 0}:
                try:
                    from modules.alerts.dual_process.run_dual_process import prediction_engine
                    dual_report = prediction_engine.evaluate_dual_process(event_obj, minutes_until_start)
                except Exception as exc:
                    logger.error(f"Error running dual process evaluation for event {event_obj.id}: {exc}")

        if odds_response:
            event_data_for_odds = {
                "id": event_obj.id,
                "home_team": event_obj.home_team,
                "away_team": event_obj.away_team,
                "sport": event_obj.sport,
                "competition": getattr(event_obj, "competition", ""),
                "slug": event_obj.slug,
                "discovery_source": getattr(event_obj, "discovery_source", ""),
                "season_id": getattr(event_obj, "season_id", None),
            }
            send_odds_alert(event_data_for_odds, odds_response, minutes_until_start, op_data=op_data)

        if streak_analysis and should_send_streak_alert_flag:
            send_matchup_streak_alerts(pre_start_notifier, [streak_analysis])

        if dual_report and (
            dual_report.process1_prediction
            or dual_report.process2_prediction
            or (dual_report.process1_report and dual_report.process1_status in ["partial", "no_match", "no_candidates"])
        ):
            if minutes_until_start in {30, 0}:
                from modules.alerts.dual_process.run_dual_process import prediction_engine
                prediction_engine.send_alerts(pre_start_notifier, [dual_report])
                if (
                    dual_report.process1_report
                    and dual_report.process1_report.get("status") == "success"
                    and minutes_until_start == 0
                ):
                    prediction_engine.log_process1_prediction_if_needed(
                        event_obj,
                        dual_report,
                        minutes_until_start,
                    )

    if not events_for_alerts:
        return

    logger.info(
        f"🔍 Evaluating {len(events_for_alerts)} events at key moments for matchup streak analysis and dual process alerts..."
    )
    if len(events_for_alerts) == 1:
        results = [events_for_alerts[0]]
    else:
        results = events_for_alerts

    with ThreadPoolExecutor(max_workers=min(4, len(results))) as executor:
        futures = [executor.submit(_send_event_alerts, result) for result in results]
        for future in futures:
            future.result()


def build_event_payload(
    event_obj,
    initial_minutes: int,
    odds_response=None,
    metadata_snapshot: dict = None,
):
    return {
        "event_obj": event_obj,
        "initial_minutes": initial_minutes,
        "observations": None,
        "odds_response": odds_response,
        "metadata_snapshot": metadata_snapshot,
        "season_id": getattr(event_obj, "season_id", None),
        "should_send_streak_alert": False,
        "streak_analysis": None,
        "dual_report": None,
        "minutes_until_start": initial_minutes,
        "success": True,
    }
