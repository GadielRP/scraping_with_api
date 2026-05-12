"""Alert pipeline for the pre-start job."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

from infrastructure.persistence.repositories import MarketRepository
from infrastructure.settings import Config
from modules.alerts import pre_start_notifier
from modules.alerts.alerts_formatter.matchup_streak_alert import send_matchup_streak_alerts
from modules.alerts.alerts_formatter.odds_alert import send_odds_alert
from modules.oddsportal.oddsportal_config import SEASON_ODDSPORTAL_MAP
from modules.alerts.dual_process.run_dual_process import prediction_engine
from modules.jobs.pre_start_check_job.pillar_event_context import build_event_context

logger = logging.getLogger(__name__)


class EventAlertProcessor:
    """Handles the processing, evaluation, and dispatching of alerts for a single event."""

    def __init__(
        self,
        event_repo,
        op_event_states: Optional[dict] = None,
        op_event_ids: Optional[set] = None,
        op_data_cache: Optional[dict] = None,
        debug_mode: Optional[bool] = False,
    ):
        self.event_repo = event_repo
        self.op_event_states = op_event_states
        self.op_event_ids = op_event_ids
        self.op_data_cache = op_data_cache
        self.debug_mode = debug_mode

    def process_event(self, event_payload: dict) -> None:
        """
        Main execution flow for a single event.
        Orchestrates synchronization, evaluation, and dispatching.
        """
        if not event_payload.get("success"):
            return

        event_obj = event_payload.get("event_obj")

        # debuggin prints
        # print("event_obj:")
        # for attr, value in event_obj.__dict__.items():
        #     print(attr, value)

        minutes_until_start = event_payload.get("minutes_until_start")
        odds_response = event_payload.get("odds_response")
        metadata = event_payload.get("metadata_snapshot", {})

        event_context = event_payload.get("event_context")
        if event_context is None:
            event_context = build_event_context(
                event_obj=event_obj,
                minutes_until_start=minutes_until_start,
                metadata_snapshot=metadata,
            )
        if event_context is None:
            logger.warning(
                "Alert pipeline missing_normalized_context for event %s; skipping new-runtime alerts",
                event_obj.id,
            )
            return

        season_id = event_context.season_id
        discovery_source = getattr(event_obj, "discovery_source", None)

        #debugging prints:
        # print("metadata:")
        # print(metadata)

        # Centralized 'Gating' Logic (Calculate once, reuse everywhere)
        is_tracked_season = season_id in SEASON_ODDSPORTAL_MAP
        is_selected_source = discovery_source in Config.DISCOVERY_SOURCES_FOR_ALERTS

        # 1. Synchronization (Wait for external data providers if necessary)
        op_data = self._sync_oddsportal_data(event_obj, odds_response)

        # 2. Evaluation (Perform analysis and generate prediction reports)
        streak_analysis, should_send_streak = self._ensure_matchup_streak_analysis(
            event_payload, event_obj, event_context, season_id, minutes_until_start
        )

       
        if streak_analysis and self.debug_mode == True:
            import os
            import pprint
            
            debug_dir = "debug/matchup_streak_analysis"
            os.makedirs(debug_dir, exist_ok=True)
            
            # Format participants for filename
            safe_participants = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in streak_analysis.participants).replace(' ', '_')
            filename = f"{streak_analysis.event_id}_{safe_participants}.txt"
            filepath = os.path.join(debug_dir, filename)
            
            try:
                with open(filepath, "w", encoding="utf-8") as f:
                    for attr, value in streak_analysis.__dict__.items():
                        f.write(f"{attr}:\n{pprint.pformat(value, width=120)}\n\n")
            except Exception as e:
                logger.error(f"Failed to save streak_analysis debug file: {e}")


        dual_report = self._ensure_dual_process_evaluation(
            event_payload, event_obj, is_tracked_season, is_selected_source, minutes_until_start
        )

        # 3. Dispatch (Send the actual notifications based on evaluation results)
        self._dispatch_alerts(
            event_obj=event_obj,
            event_context=event_context,
            season_id=season_id,
            is_tracked_season=is_tracked_season,
            minutes_until_start=minutes_until_start,
            odds_response=odds_response,
            op_data=op_data,
            streak_analysis=streak_analysis,
            should_send_streak=should_send_streak,
            dual_report=dual_report,
        )

    def _sync_oddsportal_data(self, event_obj, odds_response) -> Optional[dict]:
        """Handles waiting for OddsPortal workers and retrieving market data."""
        if not (odds_response and self.op_event_ids and event_obj.id in self.op_event_ids and self.op_event_states):
            return None

        state = self.op_event_states.get(event_obj.id)
        if not state:
            return None

        # Wait for worker claim
        if not state["done_event"].is_set():
            logged_queue = False
            while not state["started_event"].is_set() and not state["done_event"].is_set():
                if not logged_queue:
                    logger.info(f"[OP] Event {event_obj.id} queued; waiting for worker claim...")
                    logged_queue = True
                time.sleep(0.25)

            # Wait for completion within timeout
            if not state["done_event"].is_set():
                started_at = state.get("started_at_monotonic")
                if started_at is not None:
                    elapsed = time.monotonic() - started_at
                    timeout_s = getattr(Config, "ODDSPORTAL_ALERT_WAIT_TIMEOUT", 180)
                    remaining = max(0.0, float(timeout_s) - elapsed)

                    if remaining > 0:
                        logger.info(f"[OP] Event {event_obj.id} started {elapsed:.1f}s ago; waiting up to {remaining:.1f}s...")
                        if state["done_event"].wait(timeout=remaining):
                            logger.info(f"[OP] Worker signaled completion for event {event_obj.id}.")
                        else:
                            logger.warning(f"[OP] Timed out waiting for OddsPortal for event {event_obj.id}.")
                    else:
                        logger.warning(f"[OP] Event {event_obj.id} exceeded timeout ({elapsed:.1f}s).")

        # Verify DB availability
        try:
            op_markets = MarketRepository.get_oddsportal_markets_for_event(event_obj.id)
            if op_markets:
                logger.info(f"[OP] OddsPortal data available for {event_obj.id} ({len(op_markets)} rows).")
            else:
                logger.info(f"[OP] OddsPortal data NOT available for {event_obj.id}.")
        except Exception as exc:
            logger.warning(f"[OP] Could not verify OddsPortal availability for {event_obj.id}: {exc}")

        return self.op_data_cache.get(event_obj.id) if self.op_data_cache else None

    def _ensure_matchup_streak_analysis(
        self, event_payload: dict, event_obj, event_context, season_id, minutes_until_start: int
    ) -> Tuple[Optional[dict], bool]:
        """Builds or retrieves matchup streak analysis.

        Delegates to the shared ``resolve_matchup_streak_analysis`` helper
        so that pillar_pipeline can reuse the same construction logic.
        """
        from modules.jobs.pre_start_check_job.streak_analysis_resolver import (
            resolve_matchup_streak_analysis,
        )

        return resolve_matchup_streak_analysis(
            event_payload=event_payload,
            event_obj=event_obj,
            season_id=season_id,
            minutes_until_start=minutes_until_start,
            event_context=event_context,
            debug_mode=self.debug_mode,
        )

    def _ensure_dual_process_evaluation(
        self, event_payload: dict, event_obj, is_tracked_season: bool, is_selected_source: bool, minutes_until_start: int
    ):
        """Runs the dual process prediction engine if needed."""
        dual_report = event_payload.get("dual_report")
        if dual_report is not None:
            return dual_report

        if (is_selected_source or is_tracked_season) and minutes_until_start in {30, 0}:
            try:
                
                return prediction_engine.evaluate_dual_process(event_obj, minutes_until_start)
            except Exception as exc:
                logger.error(f"Error running dual process evaluation for event {event_obj.id}: {exc}")

        return None

    def _dispatch_alerts(
        self,
        event_obj,
        event_context,
        season_id,
        is_tracked_season: bool,
        minutes_until_start: int,
        odds_response: Optional[dict],
        op_data: Optional[dict],
        streak_analysis: Optional[dict],
        should_send_streak: bool,
        dual_report: Optional[dict],
    ) -> None:
        """Sends the appropriate alerts to the notifier."""
        # 1. Odds Alerts
        if odds_response:
            event_data_for_odds = {
                "id": event_obj.id,
                "home_team": event_context.home.name,
                "away_team": event_context.away.name,
                "sport": event_obj.sport,
                "competition": event_context.competition.display_name,
                "slug": event_obj.slug,
                "discovery_source": getattr(event_obj, "discovery_source", ""),
                "season_id": season_id,
            }
            send_odds_alert(event_data_for_odds, odds_response, minutes_until_start, op_data=op_data)

        # 2. Matchup Streak Alerts
        if streak_analysis and should_send_streak:
            send_matchup_streak_alerts(pre_start_notifier, [streak_analysis])

        # 3. Dual Process Alerts
        if dual_report and (
            dual_report.process1_prediction
            or dual_report.process2_prediction
            or (dual_report.process1_report and dual_report.process1_status in ["partial", "no_match", "no_candidates"])
        ):
            if minutes_until_start in {30, 0}:
                
                prediction_engine.send_alerts(pre_start_notifier, [dual_report])

                # Log Process 1 prediction if it's kick-off time
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


def evaluate_and_dispatch_alerts_batch(
    events_for_alerts: list,
    key_moments: list,
    event_repo,
    op_event_states=None,
    op_event_ids=None,
    op_data_cache=None,
    debug_mode=False,
):
    """Entry point to evaluate and dispatch alerts for a batch of events."""
    if not events_for_alerts:
        return

    logger.info(
        f"🔍 Evaluating {len(events_for_alerts)} events for matchup streak analysis and dual process alerts..."
    )

    processor = EventAlertProcessor(
        event_repo=event_repo,
        op_event_states=op_event_states,
        op_event_ids=op_event_ids,
        op_data_cache=op_data_cache,
        debug_mode=debug_mode,
    )

    # Parallelize the processing of each event
    with ThreadPoolExecutor(max_workers=min(4, len(events_for_alerts))) as executor:
        futures = [executor.submit(processor.process_event, payload) for payload in events_for_alerts]
        for future in futures:
            try:
                future.result()
            except Exception as exc:
                logger.error(f"Critical failure in alert processing thread: {exc}")
