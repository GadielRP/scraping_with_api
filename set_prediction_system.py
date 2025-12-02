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
        min_minutes_ago: int = 95,
        max_minutes_ago: int = 115
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
            
        Returns:
            List of event dictionaries matching the criteria
            
        Example:
            # Get NBA games that started 80-100 minutes ago
            events = get_events_by_sport_and_minutes_range('Basketball', 'NBA', 80, 100)
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
                query = session.query(Event).filter(
                    and_(
                        Event.sport == sport,
                        Event.start_time_utc >= window_start,
                        Event.start_time_utc <= window_end
                    )
                )
                
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
    
    def last_quarter_check(self, event_response: Dict) -> bool:
        """
        Check if the last quarter (4th quarter) has started for a basketball game.
        
        This method examines the API response from get_event_details to determine
        if the 4th quarter has begun by checking if period4 exists and has a score > 0.
        
        Args:
            event_response: Response from api_client.get_event_details(event_id)
            
        Returns:
            True if 4th quarter has started (period4 exists with score > 0), False otherwise
            
        Logic:
            - Checks homeScore.period4 and awayScore.period4
            - Returns True if either team has a period4 score > 0
            - Returns False if period4 doesn't exist or both scores are 0
        """
        try:
            if not event_response:
                logger.debug("No event response provided to last_quarter_check")
                return False
            
            # Extract score data
            home_score = event_response.get('homeScore', {})
            away_score = event_response.get('awayScore', {})
            
            # Check if period4 exists and has a value > 0
            home_period4 = home_score.get('period4')
            away_period4 = away_score.get('period4')
            
            logger.debug(f"Period 4 check: home_period4={home_period4}, away_period4={away_period4}")
            
            # If either team has scored in period4, the 4th quarter has started
            if home_period4 is not None and home_period4 > 0:
                logger.info(f"4th quarter detected: home team has {home_period4} points in period4")
                return True
            
            if away_period4 is not None and away_period4 > 0:
                logger.info(f"4th quarter detected: away team has {away_period4} points in period4")
                return True
            
            logger.debug("4th quarter has not started yet (period4 not found or score is 0)")
            return False
            
        except Exception as e:
            logger.error(f"Error in last_quarter_check: {e}")
            return False
    
    def check_nba_4th_quarter(self):
        """
        Check NBA games for 4th quarter start and send alerts.
        
        This method runs every poll interval (5 minutes) and checks NBA games that:
        - Started 95-115 minutes ago (typical time for 4th quarter to begin)
        - Have entered the 4th quarter (period4 exists with score > 0)
        - Haven't already triggered an alert
        
        Timing rationale:
        - NBA games: 4 quarters × 12 min = 48 min game clock
        - Real-time duration: ~2.5 hours (150 min) due to timeouts, fouls, breaks
        - Q1-Q3 (36 min game clock) + breaks (2+15+2 min) + stoppages = ~100-110 min
        - Q4 typically starts between 100-115 minutes after game start
        
        Workflow:
        1. Get NBA games in the 95-115 minute window
        2. For each game, check if already alerted
        3. Fetch event details from API
        4. Check if 4th quarter has started using last_quarter_check()
        5. Send alert if condition is met
        6. Track alerted events to avoid duplicates
        """
        try:
            logger.info("🏀 Running NBA 4th quarter check...")
            
            # Get NBA games that started 95-115 minutes ago
            # Research: NBA games have 48 min game clock, but real-time duration is ~2.5 hours
            # Q1-Q3 (36 min game clock) + breaks (2+15+2 min) + timeouts/fouls = ~100-110 min real time
            # Q4 typically starts between 100-115 minutes after game start
            nba_events = self.get_events_by_sport_and_minutes_range(
                sport='Basketball',
                competition='NBA',
                min_minutes_ago=95,
                max_minutes_ago=115
            )
            
            if not nba_events:
                logger.debug("No NBA events found in 95-115 minute window")
                return
            
            logger.info(f"Found {len(nba_events)} NBA events to check for 4th quarter")
            
            alerts_sent = 0
            
            for event in nba_events:
                try:
                    event_id = event['id']
                    home_team = event['home_team']
                    away_team = event['away_team']
                    competition = event['competition']
                    
                    # Skip if already alerted for this event
                    if event_id in self.tracked_events:
                        logger.debug(f"Event {event_id} already alerted, skipping")
                        continue
                    
                    # Fetch event details from API
                    logger.info(f"Checking 4th quarter for: {home_team} vs {away_team} (ID: {event_id})")
                    event_response = api_client.get_event_details(event_id)
                    
                    if not event_response:
                        logger.warning(f"Could not fetch event details for {event_id}")
                        continue
                    
                    # Check if 4th quarter has started
                    fourth_quarter_started = self.last_quarter_check(event_response)
                    
                    if fourth_quarter_started:
                        logger.info(f"✅ 4th quarter started for event {event_id}: {home_team} vs {away_team}")
                        
                        # Send alert
                        success = self._send_4th_quarter_alert(
                            event_id=event_id,
                            home_team=home_team,
                            away_team=away_team,
                            competition=competition
                        )
                        
                        if success:
                            # Mark event as alerted to avoid duplicate notifications
                            self.tracked_events.add(event_id)
                            alerts_sent += 1
                            logger.info(f"✅ Alert sent for event {event_id}")
                        else:
                            logger.warning(f"Failed to send alert for event {event_id}")
                    else:
                        logger.debug(f"4th quarter not yet started for event {event_id}")
                        
                except Exception as e:
                    logger.error(f"Error checking event {event.get('id')}: {e}")
                    continue
            
            if alerts_sent > 0:
                logger.info(f"🏀 NBA 4th quarter check completed: {alerts_sent} alert(s) sent")
            else:
                logger.info("🏀 NBA 4th quarter check completed: No alerts sent")
                
        except Exception as e:
            logger.error(f"Error in check_nba_4th_quarter: {e}")
    
    def _send_4th_quarter_alert(
        self,
        event_id: int,
        home_team: str,
        away_team: str,
        competition: str
    ) -> bool:
        """
        Send Telegram alert for 4th quarter start.
        
        Args:
            event_id: Event ID
            home_team: Home team name
            away_team: Away team name
            competition: Competition name
            
        Returns:
            True if alert sent successfully, False otherwise
        """
        try:
            # Create alert message
            message = f"🏀 <b>4th Quarter Alert - NBA</b>\n\n"
            message += f"🏆 <b>{home_team} vs {away_team}</b>\n"
            message += f"📅 Event ID: {event_id}\n"
            message += f"🏀 {competition}\n\n"
            message += f"⏰ <b>4th quarter starting now!</b>\n"
            message += f"🔔 Perfect timing for set predictions"
            
            # Send via Telegram using the pre_start_notifier
            success = pre_start_notifier.send_telegram_message(message)
            
            if success:
                logger.info(f"✅ 4th quarter alert sent for event {event_id}: {home_team} vs {away_team}")
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
