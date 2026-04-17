"""
Basketball 4Q monitoring flow.

This module is the operational home for the old `set_prediction_system.py`.
It owns the orchestration for NBA 4th-quarter detection and alert delivery.
"""

import logging
from datetime import timedelta
from typing import Dict

from database import db_manager
from infrastructure.persistence.repositories import EventRepository
from modules.alerts import pre_start_notifier
from modules.alerts.alerts_formatter.q4_alert import create_q4_alert_message
from modules.alerts.basketball_4q.predictor import predictor_4q
from models import Event
from sofascore_api2 import api_client
from timezone_utils import get_local_now

logger = logging.getLogger(__name__)


class Basketball4QMonitor:
    """
    Monitor that checks NBA live games and sends a 4Q alert when the fourth
    quarter starts.
    """

    def __init__(self):
        self.tracked_events = set()
        logger.info("Basketball 4Q monitor initialized")

    def last_quarter_check(self, event_response: Dict, home_team: str = None, away_team: str = None) -> bool:
        """
        Check if the 4th quarter has started for a basketball game.
        """
        try:
            if not event_response:
                logger.debug("No event response provided to last_quarter_check")
                return False

            event_data = event_response.get("event", event_response)
            home_score = event_data.get("homeScore", {})
            away_score = event_data.get("awayScore", {})
            status = event_data.get("status", {})
            status_description = status.get("description", "").lower() if status.get("description") else ""
            status_code = status.get("code")

            home_period1 = home_score.get("period1", 0) or 0
            home_period2 = home_score.get("period2", 0) or 0
            home_period3 = home_score.get("period3", 0) or 0
            home_period4 = home_score.get("period4")
            home_current = home_score.get("current", 0) or 0
            home_display = home_score.get("display", home_current)

            away_period1 = away_score.get("period1", 0) or 0
            away_period2 = away_score.get("period2", 0) or 0
            away_period3 = away_score.get("period3", 0) or 0
            away_period4 = away_score.get("period4")
            away_current = away_score.get("current", 0) or 0
            away_display = away_score.get("display", away_current)

            home_name = home_team or "Home"
            away_name = away_team or "Away"

            logger.info(f"📊 Score check for {home_name} vs {away_name}:")
            logger.info(
                f"   Home: Q1={home_period1}, Q2={home_period2}, Q3={home_period3}, Q4={home_period4}, Total={home_display}"
            )
            logger.info(
                f"   Away: Q1={away_period1}, Q2={away_period2}, Q3={away_period3}, Q4={away_period4}, Total={away_display}"
            )
            logger.info(f"   Current Score: {home_display} - {away_display}")
            logger.info(f"   Status: code={status_code}, description='{status.get('description', 'N/A')}'")

            period4_exists = (home_period4 is not None) or (away_period4 is not None)
            status_matches = (status_description == "4th quarter") or (status_code == 16)

            if period4_exists or status_matches:
                if period4_exists and status_matches:
                    logger.info(
                        f"✅ 4th quarter detected: period4 exists (Home Q4={home_period4}, Away Q4={away_period4}) "
                        f"AND status matches (code={status_code}, description='{status.get('description', 'N/A')}') "
                        f"(Total: {home_display} - {away_display})"
                    )
                elif period4_exists:
                    logger.info(
                        f"✅ 4th quarter detected: period4 exists (Home Q4={home_period4}, Away Q4={away_period4}) "
                        f"(Total: {home_display} - {away_display})"
                    )
                else:
                    logger.info(
                        f"✅ 4th quarter detected: status matches (code={status_code}, description='{status.get('description', 'N/A')}') "
                        f"(Total: {home_display} - {away_display})"
                    )
                return True

            logger.info(
                f"⏳ 4th quarter not started: period4 doesn't exist AND status doesn't match "
                f"(period4: Home={home_period4}, Away={away_period4}, "
                f"status: code={status_code}, description='{status.get('description', 'N/A')}') "
                f"(Current: {home_display} - {away_display})"
            )
            return False
        except Exception as e:
            logger.error(f"Error in last_quarter_check: {e}")
            return False

    def check_nba_4th_quarter(self):
        """
        Check NBA games for 4th quarter start and send alerts.
        """
        try:
            logger.info("🏀 Running NBA 4th quarter check...")

            all_nba_events = EventRepository.get_events_started_between_minutes_ago(
                sport="Basketball",
                competition="NBA",
                min_minutes_ago=0,
                max_minutes_ago=140,
                alert_sent=False,
            )

            if not all_nba_events:
                logger.debug("No NBA events found in 0-140 minute window")
                return

            logger.info(f"📋 Found {len(all_nba_events)} NBA event(s) that started within last 140 minutes:")
            for idx, event in enumerate(all_nba_events, 1):
                minutes_since_start = self._calculate_minutes_since_start(event["start_time_utc"])
                logger.info(
                    f"   {idx}. Event {event['id']}: {event['home_team']} vs {event['away_team']} "
                    f"({event['competition']}) - Started {minutes_since_start} minutes ago"
                )

            now = get_local_now()
            check_window_start = now - timedelta(minutes=140)
            check_window_end = now - timedelta(minutes=105)

            from infrastructure.settings import Config
            from oddsportal_config import SEASON_ODDSPORTAL_MAP

            nba_events_to_check = []
            for event in all_nba_events:
                event_start = event["start_time_utc"]
                if Config.FILTER_ALERTS_BY_OP_SEASON and event.get("season_id") not in SEASON_ODDSPORTAL_MAP:
                    logger.debug(f"Skipping event {event['id']} due to OP season filter.")
                    continue

                if check_window_start <= event_start <= check_window_end:
                    nba_events_to_check.append(event)

            if not nba_events_to_check:
                logger.info("⏭️ No NBA events in 105-140 minute window to check for 4th quarter")
                return

            logger.info(
                f"🔍 Filtered to {len(nba_events_to_check)} event(s) in 105-140 minute window (will check for 4th quarter):"
            )
            for idx, event in enumerate(nba_events_to_check, 1):
                minutes_since_start = self._calculate_minutes_since_start(event["start_time_utc"])
                logger.info(
                    f"   {idx}. Event {event['id']}: {event['home_team']} vs {event['away_team']} "
                    f"({minutes_since_start} minutes ago)"
                )

            logger.info("📡 Fetching live basketball events from API...")
            live_response = api_client.get_live_events_response_per_sport("basketball")

            if not live_response or "events" not in live_response:
                logger.warning("❌ Could not fetch live basketball events or response is invalid")
                return

            live_events_map = {}
            for live_event in live_response.get("events", []):
                event_id = live_event.get("id")
                if event_id:
                    live_events_map[event_id] = live_event

            logger.info(f"📡 Fetched {len(live_events_map)} live basketball event(s) from API")

            db_event_ids = {event["id"] for event in nba_events_to_check}
            matching_live_events = {
                event_id: event_data
                for event_id, event_data in live_events_map.items()
                if event_id in db_event_ids
            }

            if not matching_live_events:
                logger.info(
                    f"⏭️ No matching live events found for {len(nba_events_to_check)} DB event(s) in 105-140 minute window"
                )
                return

            logger.info(f"✅ Found {len(matching_live_events)} matching live event(s) to check for 4th quarter")

            alerts_sent = 0

            for event in nba_events_to_check:
                try:
                    event_id = event["id"]
                    home_team = event["home_team"]
                    away_team = event["away_team"]
                    competition = event["competition"]

                    if event_id in self.tracked_events:
                        logger.debug(f"Event {event_id} already alerted (cached), skipping")
                        continue

                    if event_id not in matching_live_events:
                        logger.debug(f"Event {event_id} not found in live events (may have finished or not live)")
                        continue

                    event_response = matching_live_events[event_id]
                    logger.info(f"🔍 Checking 4th quarter for event {event_id}: {home_team} vs {away_team} ({competition})")

                    fourth_quarter_started = self.last_quarter_check(event_response, home_team, away_team)

                    if fourth_quarter_started:
                        home_score_data = event_response.get("homeScore", {})
                        away_score_data = event_response.get("awayScore", {})
                        home_total = home_score_data.get("display") or home_score_data.get("current", 0) or 0
                        away_total = away_score_data.get("display") or away_score_data.get("current", 0) or 0

                        logger.info(
                            f"✅ 4th quarter started for event {event_id}: {home_team} vs {away_team} "
                            f"(Score: {home_total} - {away_total})"
                        )

                        success = self._send_4th_quarter_alert(
                            event_id=event_id,
                            home_team=home_team,
                            away_team=away_team,
                            competition=competition,
                            event_response=event_response,
                        )

                        if success:
                            self._mark_event_alert_sent(event_id)
                            self.tracked_events.add(event_id)
                            alerts_sent += 1
                            logger.info(
                                f"✅ Alert sent successfully for event {event_id}: {home_team} vs {away_team} "
                                f"(Score: {home_total} - {away_total})"
                            )
                        else:
                            logger.warning(f"⚠️ Failed to send alert for event {event_id}: {home_team} vs {away_team}")
                    else:
                        home_score_data = event_response.get("homeScore", {})
                        away_score_data = event_response.get("awayScore", {})
                        home_total = home_score_data.get("display") or home_score_data.get("current", 0) or 0
                        away_total = away_score_data.get("display") or away_score_data.get("current", 0) or 0
                        logger.info(
                            f"⏳ 4th quarter not yet started for event {event_id}: {home_team} vs {away_team} "
                            f"(Current Score: {home_total} - {away_total})"
                        )
                except Exception as e:
                    logger.error(f"Error checking event {event.get('id')}: {e}")
                    continue

            if alerts_sent > 0:
                logger.info(f"🏀 NBA 4th quarter check completed: {alerts_sent} alert(s) sent")
            else:
                logger.info("🏀 NBA 4th quarter check completed: No alerts sent")
        except Exception as e:
            logger.error(f"Error in check_nba_4th_quarter: {e}")

    def _calculate_minutes_since_start(self, start_time_utc) -> int:
        """Calculate minutes since event started."""
        try:
            now = get_local_now()
            time_diff = now - start_time_utc
            return int(time_diff.total_seconds() / 60)
        except Exception as e:
            logger.error(f"Error calculating minutes since start: {e}")
            return 0

    def _mark_event_alert_sent(self, event_id: int) -> bool:
        """Mark an event as having sent the 4th quarter alert in the database."""
        try:
            with db_manager.get_session() as session:
                event = session.query(Event).filter(Event.id == event_id).first()
                if event:
                    event.alert_sent = True
                    session.commit()
                    logger.debug(f"Marked event {event_id} as alert_sent=True in database")
                    return True

                logger.warning(f"Event {event_id} not found in database when trying to mark as alerted")
                return False
        except Exception as e:
            logger.error(f"Error marking event {event_id} as alerted: {e}")
            return False

    def _send_4th_quarter_alert(
        self,
        event_id: int,
        home_team: str,
        away_team: str,
        competition: str,
        event_response: Dict,
    ) -> bool:
        """Send Telegram alert for 4th quarter start with prediction."""
        try:
            home_score_data = event_response.get("homeScore", {})
            away_score_data = event_response.get("awayScore", {})

            q1_home = home_score_data.get("period1", 0) or 0
            q2_home = home_score_data.get("period2", 0) or 0
            q3_home = home_score_data.get("period3", 0) or 0

            q1_away = away_score_data.get("period1", 0) or 0
            q2_away = away_score_data.get("period2", 0) or 0
            q3_away = away_score_data.get("period3", 0) or 0

            current_home = home_score_data.get("display") or home_score_data.get("current", 0) or 0
            current_away = away_score_data.get("display") or away_score_data.get("current", 0) or 0

            tournament_slug = event_response.get("tournament", {}).get("slug", "").lower()
            season_stats_type = event_response.get("seasonStatisticsType", "").lower()

            if "playoff" in tournament_slug or "playoff" in season_stats_type:
                season_stage = "Playoffs"
            elif "preseason" in tournament_slug or "preseason" in season_stats_type:
                season_stage = "Preseason"
            elif "cup" in tournament_slug:
                season_stage = "Cup"
            else:
                season_stage = "Regular Season"

            logger.info(
                f"📅 Season stage determined: '{season_stage}' "
                f"(from tournament slug: '{tournament_slug}', seasonStatsType: '{season_stats_type}')"
            )

            prediction = predictor_4q.predict_4th_quarter(
                home_team=home_team,
                away_team=away_team,
                q1_home=q1_home,
                q2_home=q2_home,
                q3_home=q3_home,
                q1_away=q1_away,
                q2_away=q2_away,
                q3_away=q3_away,
                season_stage=season_stage,
            )

            message = create_q4_alert_message(
                event_id=event_id,
                home_team=home_team,
                away_team=away_team,
                competition=competition,
                season_stage=season_stage,
                current_home=current_home,
                current_away=current_away,
                q1_home=q1_home,
                q2_home=q2_home,
                q3_home=q3_home,
                q1_away=q1_away,
                q2_away=q2_away,
                q3_away=q3_away,
                prediction=prediction,
            )

            success = pre_start_notifier.send_telegram_message(message)

            if success:
                logger.info(f"✅ 4th quarter alert with prediction sent for event {event_id}: {home_team} vs {away_team}")
                logger.info(
                    f"   Prediction: Q4 {prediction.get('predicted_q4_home')} - {prediction.get('predicted_q4_away')}, "
                    f"Final {prediction.get('predicted_final_home')} - {prediction.get('predicted_final_away')}, "
                    f"Confidence: {prediction.get('confidence_level')}"
                )
            else:
                logger.error(f"❌ Failed to send 4th quarter alert for event {event_id}")

            return success
        except Exception as e:
            logger.error(f"Error sending 4th quarter alert for event {event_id}: {e}")
            return False

    def cleanup_tracked_events(self):
        """Clean up old tracked events to prevent memory leaks."""
        try:
            self.tracked_events.clear()
            logger.info("Cleaned up tracked events set")
        except Exception as e:
            logger.error(f"Error cleaning up tracked events: {e}")


basketball_4q_monitor = Basketball4QMonitor()

__all__ = [
    "Basketball4QMonitor",
    "basketball_4q_monitor",
]
