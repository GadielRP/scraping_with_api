"""
Set Prediction System - Alert system for in-game events across multiple sports

This system monitors events that have already started and triggers alerts based on 
sport-specific game state conditions (e.g., 4th quarter in basketball, 3rd period in hockey).

MODULAR DESIGN:
- Each sport has its own check method (e.g., last_quarter_check for basketball)
- Sport/competition can be configured via parameters
- Minute window is flexible per sport needs
- Easy to extend with new sports and conditions
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from repository import EventRepository
from sofascore_api2 import api_client
from alert_system import pre_start_notifier
from sqlalchemy.orm import Session
from sqlalchemy import and_
from models import Event
from database import db_manager
from timezone_utils import get_local_now
from basketball_4q_prediction import predictor_4q

logger = logging.getLogger(__name__)


class SetPredictionSystem:
    """
    Set Prediction System for monitoring in-game events and triggering alerts
    based on sport-specific game state conditions.
    """
    
    def __init__(self):
        """Initialize the set prediction system"""
        self.tracked_events = set()  # Events that have already triggered alerts
        logger.info("Set Prediction System initialized")
    
    def get_events_by_sport_and_minutes_range(
        self,
        sport: str,
        competition: Optional[str] = None,
        min_minutes_ago: int = 105,
        max_minutes_ago: int = 135,
        alert_sent: Optional[bool] = None
    ) -> List[Dict]:
        """
        Get events that started within a specific minute range for a given sport/competition.
        
        This is a modular function that can be used for any sport and competition.
        Similar to get_events_started_recently but with sport/competition filtering.
        
        Args:
            sport: Sport name (e.g., 'Basketball', 'Hockey', 'Football')
            competition: Optional competition filter (e.g., 'NBA', 'NHL'). If None, returns all events for sport.
            min_minutes_ago: Minimum minutes since event started (e.g., 80)
            max_minutes_ago: Maximum minutes since event started (e.g., 100)
            alert_sent: Optional filter for alert_sent flag. If True, only returns events with alert_sent=True.
                       If False, only returns events with alert_sent=False. If None, returns all events.
            
        Returns:
            List of event dictionaries matching the criteria
            
        Example:
            # Get NBA games that started 105-135 minutes ago and haven't sent alert yet
            events = get_events_by_sport_and_minutes_range('Basketball', 'NBA', 105, 135, alert_sent=False)
        """
        try:
            with db_manager.get_session() as session:
                now = get_local_now()
                
                # Calculate time window
                window_start = now - timedelta(minutes=max_minutes_ago)
                window_end = now - timedelta(minutes=min_minutes_ago)
                
                logger.debug(f"Searching for {sport} events (competition: {competition or 'all'}) "
                           f"that started between {max_minutes_ago} and {min_minutes_ago} minutes ago")
                logger.debug(f"Time window: {window_start} to {window_end}")
                
                # Build query with sport filter
                filters = [
                    Event.sport == sport,
                    Event.start_time_utc >= window_start,
                    Event.start_time_utc <= window_end
                ]
                
                # Add alert_sent filter if specified
                if alert_sent is not None:
                    filters.append(Event.alert_sent == alert_sent)
                
                query = session.query(Event).filter(and_(*filters))
                
                # Add competition filter if specified
                if competition:
                    # Competition is stored as comma-separated values, so we need to use LIKE
                    query = query.filter(Event.competition.like(f'%{competition}%'))
                
                events = query.all()
                
                # Convert to list of dictionaries
                result = []
                for event in events:
                    event_data = {
                        'id': event.id,
                        'home_team': event.home_team,
                        'away_team': event.away_team,
                        'competition': event.competition,
                        'start_time_utc': event.start_time_utc,
                        'sport': event.sport,
                        'country': event.country,
                        'slug': event.slug,
                        'custom_id': event.custom_id
                    }
                    result.append(event_data)
                
                if result:
                    logger.info(f"Found {len(result)} {sport} events (competition: {competition or 'all'}) "
                              f"in {max_minutes_ago}-{min_minutes_ago} minute window")
                else:
                    logger.debug(f"No {sport} events (competition: {competition or 'all'}) found in time window")
                
                return result
                
        except Exception as e:
            logger.error(f"Error getting events by sport and minutes range: {e}")
            return []
    
    def last_quarter_check(self, event_response: Dict, home_team: str = None, away_team: str = None) -> bool:
        """
        Check if the last quarter (4th quarter) has started for a basketball game.
        
        This method examines the API response from get_event_details to determine
        if the 4th quarter has begun by checking:
        1. If period4 exists in homeScore or awayScore
        2. If status.description is "4th quarter" OR status.code is 16
        
        Args:
            event_response: Response from api_client.get_event_details(event_id)
            home_team: Optional home team name for logging
            away_team: Optional away team name for logging
            
        Returns:
            True if 4th quarter has started (either condition met), False otherwise
            
        Logic:
            - Checks if period4 exists in homeScore or awayScore (existence check only)
            - Checks if status.description == "4th quarter" OR status.code == 16
            - Returns True if either condition is met
        """
        try:
            if not event_response:
                logger.debug("No event response provided to last_quarter_check")
                return False
            
            # Handle both wrapped and unwrapped response structures
            event_data = event_response.get('event', event_response)
            
            # Extract score data
            home_score = event_data.get('homeScore', {})
            away_score = event_data.get('awayScore', {})
            
            # Extract status data
            status = event_data.get('status', {})
            status_description = status.get('description', '').lower() if status.get('description') else ''
            status_code = status.get('code')
            
            # Extract all period scores for detailed logging
            home_period1 = home_score.get('period1', 0) or 0
            home_period2 = home_score.get('period2', 0) or 0
            home_period3 = home_score.get('period3', 0) or 0
            home_period4 = home_score.get('period4')
            home_current = home_score.get('current', 0) or 0
            home_display = home_score.get('display', home_current)
            
            away_period1 = away_score.get('period1', 0) or 0
            away_period2 = away_score.get('period2', 0) or 0
            away_period3 = away_score.get('period3', 0) or 0
            away_period4 = away_score.get('period4')
            away_current = away_score.get('current', 0) or 0
            away_display = away_score.get('display', away_current)
            
            # Build team names for logging
            home_name = home_team or "Home"
            away_name = away_team or "Away"
            
            # Log detailed score information
            logger.info(f"📊 Score check for {home_name} vs {away_name}:")
            logger.info(f"   Home: Q1={home_period1}, Q2={home_period2}, Q3={home_period3}, Q4={home_period4}, Total={home_display}")
            logger.info(f"   Away: Q1={away_period1}, Q2={away_period2}, Q3={away_period3}, Q4={away_period4}, Total={away_display}")
            logger.info(f"   Current Score: {home_display} - {away_display}")
            logger.info(f"   Status: code={status_code}, description='{status.get('description', 'N/A')}'")
            
            # Check condition 1: period4 exists in homeScore or awayScore
            period4_exists = (home_period4 is not None) or (away_period4 is not None)
            
            # Check condition 2: status.description is "4th quarter" OR status.code is 16
            status_matches = (status_description == '4th quarter') or (status_code == 16)
            
            # Either condition can be true to return True
            if period4_exists or status_matches:
                if period4_exists and status_matches:
                    logger.info(f"✅ 4th quarter detected: period4 exists (Home Q4={home_period4}, Away Q4={away_period4}) "
                              f"AND status matches (code={status_code}, description='{status.get('description', 'N/A')}') "
                              f"(Total: {home_display} - {away_display})")
                elif period4_exists:
                    logger.info(f"✅ 4th quarter detected: period4 exists (Home Q4={home_period4}, Away Q4={away_period4}) "
                              f"(Total: {home_display} - {away_display})")
                else:
                    logger.info(f"✅ 4th quarter detected: status matches (code={status_code}, description='{status.get('description', 'N/A')}') "
                              f"(Total: {home_display} - {away_display})")
                return True
            
            # Log why 4th quarter hasn't started
            logger.info(f"⏳ 4th quarter not started: period4 doesn't exist AND status doesn't match "
                      f"(period4: Home={home_period4}, Away={away_period4}, "
                      f"status: code={status_code}, description='{status.get('description', 'N/A')}') "
                      f"(Current: {home_display} - {away_display})")
            
            return False
            
        except Exception as e:
            logger.error(f"Error in last_quarter_check: {e}")
            return False
    
    def check_nba_4th_quarter(self):
        """
        Check NBA games for 4th quarter start and send alerts.
        
        This method runs every poll interval (5 minutes) and checks NBA games that:
        - Started up to 135 minutes ago (max window for discovery)
        - Are within 105-135 minute window (only these get API checks for 4th quarter)
        - Have entered the 4th quarter (period4 exists with score > 0)
        - Haven't already triggered an alert
        
        Timing rationale:
        - NBA games: 4 quarters × 12 min = 48 min game clock
        - Real-time duration: ~2.5 hours (150 min) due to timeouts, fouls, breaks
        - Q1-Q3 (36 min game clock) + breaks (2+15+2 min) + stoppages = ~100-110 min
        - Q4 typically starts between 105-135 minutes after game start
        
        Workflow:
        1. Get all NBA games that started up to 135 minutes ago (max window)
        2. Log all found events (for visibility)
        3. Filter to only events in 105-135 minute window (for API checks)
        4. Fetch live basketball events once from API (optimization: single call instead of N calls)
        5. Create mapping of event_id -> event_data from live response
        6. Filter live events to only those matching our DB event IDs
        7. For each matching event, check if already alerted
        8. Check if 4th quarter has started using last_quarter_check()
        9. Send alert if condition is met
        10. Track alerted events to avoid duplicates
        """
        try:
            logger.info("🏀 Running NBA 4th quarter check...")
            
            # Get ALL NBA games that started up to 135 minutes ago (max window for discovery)
            # This allows us to see all recent NBA games in logs
            # Filter to only events where alert_sent = False (haven't sent alert yet)
            all_nba_events = self.get_events_by_sport_and_minutes_range(
                sport='Basketball',
                competition='NBA',
                min_minutes_ago=0,  # Start from now
                max_minutes_ago=135,  # Up to 135 minutes ago
                alert_sent=False  # Only get events that haven't sent alert yet
            )
            
            if not all_nba_events:
                logger.debug("No NBA events found in 0-135 minute window")
                return
            
            # Log all found NBA events (for visibility)
            logger.info(f"📋 Found {len(all_nba_events)} NBA event(s) that started within last 135 minutes:")
            for idx, event in enumerate(all_nba_events, 1):
                # Calculate minutes since start for logging
                minutes_since_start = self._calculate_minutes_since_start(event['start_time_utc'])
                logger.info(f"   {idx}. Event {event['id']}: {event['home_team']} vs {event['away_team']} "
                          f"({event['competition']}) - Started {minutes_since_start} minutes ago")
            
            # Filter to only events in the 105-135 minute window (these will get API checks)
            from timezone_utils import get_local_now
            now = get_local_now()
            check_window_start = now - timedelta(minutes=135)
            check_window_end = now - timedelta(minutes=105)
            
            nba_events_to_check = []
            for event in all_nba_events:
                event_start = event['start_time_utc']
                if check_window_start <= event_start <= check_window_end:
                    nba_events_to_check.append(event)
            
            if not nba_events_to_check:
                logger.info(f"⏭️ No NBA events in 105-135 minute window to check for 4th quarter")
                return
            
            logger.info(f"🔍 Filtered to {len(nba_events_to_check)} event(s) in 105-135 minute window (will check for 4th quarter):")
            for idx, event in enumerate(nba_events_to_check, 1):
                minutes_since_start = self._calculate_minutes_since_start(event['start_time_utc'])
                logger.info(f"   {idx}. Event {event['id']}: {event['home_team']} vs {event['away_team']} "
                          f"({minutes_since_start} minutes ago)")
            
            # Fetch live basketball events once (optimization: single API call instead of N calls)
            logger.info("📡 Fetching live basketball events from API...")
            live_response = api_client.get_live_events_response_per_sport('basketball')
            
            if not live_response or 'events' not in live_response:
                logger.warning("❌ Could not fetch live basketball events or response is invalid")
                return
            
            # Create a mapping of event_id -> event_data from live response
            live_events_map = {}
            for live_event in live_response.get('events', []):
                event_id = live_event.get('id')
                if event_id:
                    live_events_map[event_id] = live_event
            
            logger.info(f"📡 Fetched {len(live_events_map)} live basketball event(s) from API")
            
            # Create a set of event IDs we need to check (for fast lookup)
            db_event_ids = {event['id'] for event in nba_events_to_check}
            
            # Filter live events to only those in our DB events list
            matching_live_events = {
                event_id: event_data 
                for event_id, event_data in live_events_map.items() 
                if event_id in db_event_ids
            }
            
            if not matching_live_events:
                logger.info(f"⏭️ No matching live events found for {len(nba_events_to_check)} DB event(s) in 105-135 minute window")
                return
            
            logger.info(f"✅ Found {len(matching_live_events)} matching live event(s) to check for 4th quarter")
            
            alerts_sent = 0
            
            # Process each event from our DB list, using live response data if available
            for event in nba_events_to_check:
                try:
                    event_id = event['id']
                    home_team = event['home_team']
                    away_team = event['away_team']
                    competition = event['competition']
                    
                    # Skip if already alerted for this event (in-memory cache check)
                    # Note: Database filtering already excludes alert_sent=True events, but this provides extra safety
                    if event_id in self.tracked_events:
                        logger.debug(f"Event {event_id} already alerted (cached), skipping")
                        continue
                    
                    # Get event data from live response (if available)
                    if event_id not in matching_live_events:
                        logger.debug(f"Event {event_id} not found in live events (may have finished or not live)")
                        continue
                    
                    event_response = matching_live_events[event_id]
                    logger.info(f"🔍 Checking 4th quarter for event {event_id}: {home_team} vs {away_team} ({competition})")
                    
                    # Check if 4th quarter has started (with detailed score logging)
                    fourth_quarter_started = self.last_quarter_check(event_response, home_team, away_team)
                    
                    if fourth_quarter_started:
                        # Extract current score for final log
                        home_score_data = event_response.get('homeScore', {})
                        away_score_data = event_response.get('awayScore', {})
                        home_total = home_score_data.get('display') or home_score_data.get('current', 0) or 0
                        away_total = away_score_data.get('display') or away_score_data.get('current', 0) or 0
                        
                        logger.info(f"✅ 4th quarter started for event {event_id}: {home_team} vs {away_team} (Score: {home_total} - {away_total})")
                        
                        # Send alert with prediction
                        success = self._send_4th_quarter_alert(
                            event_id=event_id,
                            home_team=home_team,
                            away_team=away_team,
                            competition=competition,
                            event_response=event_response
                        )
                        
                        if success:
                            # Mark event as alerted in database to avoid duplicate notifications
                            self._mark_event_alert_sent(event_id)
                            # Also update in-memory cache for performance
                            self.tracked_events.add(event_id)
                            alerts_sent += 1
                            logger.info(f"✅ Alert sent successfully for event {event_id}: {home_team} vs {away_team} (Score: {home_total} - {away_total})")
                        else:
                            logger.warning(f"⚠️ Failed to send alert for event {event_id}: {home_team} vs {away_team}")
                    else:
                        # Log that we checked but Q4 hasn't started yet
                        home_score_data = event_response.get('homeScore', {})
                        away_score_data = event_response.get('awayScore', {})
                        home_total = home_score_data.get('display') or home_score_data.get('current', 0) or 0
                        away_total = away_score_data.get('display') or away_score_data.get('current', 0) or 0
                        logger.info(f"⏳ 4th quarter not yet started for event {event_id}: {home_team} vs {away_team} (Current Score: {home_total} - {away_total})")
                        
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
        """
        Calculate minutes since event started.
        
        Args:
            start_time_utc: Event start time (datetime object)
            
        Returns:
            Integer representing minutes since start
        """
        try:
            from timezone_utils import get_local_now
            now = get_local_now()
            time_diff = now - start_time_utc
            return int(time_diff.total_seconds() / 60)
        except Exception as e:
            logger.error(f"Error calculating minutes since start: {e}")
            return 0
    
    def _mark_event_alert_sent(self, event_id: int) -> bool:
        """
        Mark an event as having sent the 4th quarter alert in the database.
        
        Args:
            event_id: Event ID to mark as alerted
            
        Returns:
            True if successfully updated, False otherwise
        """
        try:
            with db_manager.get_session() as session:
                event = session.query(Event).filter(Event.id == event_id).first()
                if event:
                    event.alert_sent = True
                    session.commit()
                    logger.debug(f"Marked event {event_id} as alert_sent=True in database")
                    return True
                else:
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
        event_response: Dict
    ) -> bool:
        """
        Send Telegram alert for 4th quarter start with prediction.
        
        Args:
            event_id: Event ID
            home_team: Home team name
            away_team: Away team name
            competition: Competition name
            event_response: Full event details from API
            
        Returns:
            True if alert sent successfully, False otherwise
        """
        try:
            # Extract quarter scores from event response
            home_score_data = event_response.get('homeScore', {})
            away_score_data = event_response.get('awayScore', {})
            
            q1_home = home_score_data.get('period1', 0) or 0
            q2_home = home_score_data.get('period2', 0) or 0
            q3_home = home_score_data.get('period3', 0) or 0
            
            q1_away = away_score_data.get('period1', 0) or 0
            q2_away = away_score_data.get('period2', 0) or 0
            q3_away = away_score_data.get('period3', 0) or 0
            
            current_home = home_score_data.get('display') or home_score_data.get('current', 0) or 0
            current_away = away_score_data.get('display') or away_score_data.get('current', 0) or 0
            
            # Extract season stage from event response
            # Check multiple sources: tournament slug, seasonStatisticsType
            tournament_slug = event_response.get('tournament', {}).get('slug', '').lower()
            season_stats_type = event_response.get('seasonStatisticsType', '').lower()
            
            # Determine season stage - will be mapped to database round values (regular_season or knockouts/playoffs)
            if 'playoff' in tournament_slug or 'playoff' in season_stats_type:
                season_stage = 'Playoffs'  # Will map to 'knockouts/playoffs' in DB
            elif 'preseason' in tournament_slug or 'preseason' in season_stats_type:
                season_stage = 'Preseason'  # Will map to 'regular_season' in DB
            elif 'cup' in tournament_slug:
                season_stage = 'Cup'  # Will map to 'regular_season' in DB
            else:
                season_stage = 'Regular Season'  # Will map to 'regular_season' in DB
            
            logger.info(f"📅 Season stage determined: '{season_stage}' (from tournament slug: '{tournament_slug}', seasonStatsType: '{season_stats_type}')")
            
            # Generate 4th quarter prediction
            prediction = predictor_4q.predict_4th_quarter(
                home_team=home_team,
                away_team=away_team,
                q1_home=q1_home,
                q2_home=q2_home,
                q3_home=q3_home,
                q1_away=q1_away,
                q2_away=q2_away,
                q3_away=q3_away,
                season_stage=season_stage
            )
            
            # Create alert message
            message = f"🏀 <b>4th Quarter Alert - NBA</b>\n\n"
            message += f"🏆 <b>{home_team} vs {away_team}</b>\n"
            message += f"📅 Event ID: {event_id}\n"
            message += f"🏀 {competition} - {season_stage}\n\n"
            
            message += f"📊 <b>Current Score (Q1-Q3):</b>\n"
            message += f"{home_team}: {current_home} ({q1_home}-{q2_home}-{q3_home})\n"
            message += f"{away_team}: {current_away} ({q1_away}-{q2_away}-{q3_away})\n\n"
            
            message += f"⏰ <b>4th Quarter is LIVE!</b>\n\n"
            
            # Add prediction if available
            if not prediction.get('error'):
                # Calculation overview section
                message += f"📐 <b>Calculation Overview:</b>\n\n"
                message += f"<i>Step 1: Historical Analysis</i>\n"
                message += f"• Analyzed {prediction['parameters'].get('sample_size_home', 0)} games for {home_team}\n"
                message += f"• Analyzed {prediction['parameters'].get('sample_size_away', 0)} games for {away_team}\n"
                message += f"• Historical Q4 avg: {home_team} {prediction['parameters']['avg_q4_home']:.1f} pts, {away_team} {prediction['parameters']['avg_q4_away']:.1f} pts\n\n"
                
                message += f"<i>Step 2: Rhythm Factor</i>\n"
                message += f"• Current Q1-Q3 total: {prediction['parameters']['total_q1_q3_combined_current']} pts\n"
                message += f"• Historical Q1-Q3 avg: {prediction['parameters']['avg_q1_q3_combined_historical']:.1f} pts\n"
                message += f"• Rhythm: {prediction['rhythm_factor']:.2f}x (how fast this game is vs historical)\n"
                message += f"→ {prediction['rhythm_factor']:.2f}x means this game is {('faster' if prediction['rhythm_factor'] > 1.0 else 'slower')} than average\n\n"
                
                message += f"<i>Step 3: Base Q4 Prediction</i>\n"
                message += f"• Formula: Historical Q4 avg × Rhythm factor\n"
                message += f"• {home_team}: {prediction['base_q4_home']:.1f} pts ({prediction['parameters']['avg_q4_home']:.1f} × {prediction['rhythm_factor']:.2f})\n"
                message += f"• {away_team}: {prediction['base_q4_away']:.1f} pts ({prediction['parameters']['avg_q4_away']:.1f} × {prediction['rhythm_factor']:.2f})\n\n"
                
                if prediction['parameters']['momentum_applied']:
                    message += f"<i>Step 4: Momentum Adjustment</i>\n"
                    leader = home_team if prediction['score_differential'] > 0 else away_team
                    trailing = away_team if prediction['score_differential'] > 0 else home_team
                    message += f"• Score differential: {abs(prediction['score_differential'])} pts ({leader} leading)\n"
                    message += f"• Adjustment: {leader} × 0.95 (slow down), {trailing} × 1.06 (speed up)\n"
                    message += f"→ Leading teams tend to slow down, trailing teams push harder\n\n"
                
                message += f"🔮 <b>4Q Prediction:</b>\n"
                message += f"{home_team}: {prediction['predicted_q4_home']:.1f} pts\n"
                message += f"{away_team}: {prediction['predicted_q4_away']:.1f} pts\n\n"
                
                message += f"🎯 <b>Final Score Projection:</b>\n"
                message += f"{home_team}: {prediction['predicted_final_home']:.1f}\n"
                message += f"{away_team}: {prediction['predicted_final_away']:.1f}\n\n"
                
                # Confidence indicator
                confidence_emoji = {
                    'HIGH': '🟢',
                    'MEDIUM': '🟡',
                    'LOW': '🔴'
                }.get(prediction['confidence_level'], '⚪')
                
                message += f"{confidence_emoji} <b>Confidence:</b> {prediction['confidence_level']}\n"
                message += f"• Range: {prediction['confidence_range_numeric']:.2f} (lower = more reliable)\n"
                message += f"• Z-Score: {prediction['z_score']:.3f} ({prediction['z_confidence']})\n"
                message += f"• Explosiveness: {prediction.get('explosiveness', 0):.2f} (volatility measure)\n"
            else:
                message += f"⚠️ Prediction unavailable (insufficient historical data)\n\n"
            
            message += f"\n🔔 <i>Perfect timing for set predictions!</i>"
            
            # Send via Telegram using the pre_start_notifier
            success = pre_start_notifier.send_telegram_message(message)
            
            if success:
                logger.info(f"✅ 4th quarter alert with prediction sent for event {event_id}: {home_team} vs {away_team}")
                logger.info(f"   Prediction: Q4 {prediction.get('predicted_q4_home')} - {prediction.get('predicted_q4_away')}, "
                          f"Final {prediction.get('predicted_final_home')} - {prediction.get('predicted_final_away')}, "
                          f"Confidence: {prediction.get('confidence_level')}")
            else:
                logger.error(f"❌ Failed to send 4th quarter alert for event {event_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending 4th quarter alert for event {event_id}: {e}")
            return False
    
    def cleanup_tracked_events(self):
        """
        Clean up old tracked events to prevent memory leaks.
        Should be called periodically (e.g., once per day).
        """
        try:
            # Clear tracked events older than 24 hours
            # Since we only track event IDs, we can safely clear the entire set
            # after a reasonable time period (events won't repeat within 24 hours)
            self.tracked_events.clear()
            logger.info("Cleaned up tracked events set")
        except Exception as e:
            logger.error(f"Error cleaning up tracked events: {e}")


# Global instance
set_prediction_system = SetPredictionSystem()
