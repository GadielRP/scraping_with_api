import logging
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta
from decimal import Decimal

import sofascore_api2  # Import to attach methods to SofaScoreAPI class
from sofascore_api import api_client
from repository import EventRepository, OddsRepository, DailyDiscoveryRepository
from odds_utils import fractional_to_decimal, validate_odds_data
from timezone_utils import get_local_now_aware

logger = logging.getLogger(__name__)

class TodaySportExtractor:
    """
    Extracts and processes today's scheduled sports events with odds.
    Fetches odds first to identify events with betting markets, then fetches
    event details and inserts both into the database.
    """
    
    def __init__(self):
        self.api_client = api_client  # Uses api_client which has methods from both sofascore_api and sofascore_api2
    
    def _process_odds_response(self, odds_response: Dict) -> Dict[int, Dict]:
        """
        Process odds response to extract odds data for each event.
        
        Args:
            odds_response: Response from get_today_sport_events_odds_response with structure:
                           {"odds": {"event_id": {"choices": [...], ...}, ...}}
        
        Returns:
            Dict mapping event_id to processed odds data with initial and final odds
        """
        try:
            if not odds_response or 'odds' not in odds_response:
                logger.warning("No odds data found in response")
                return {}
            
            odds_map = {}
            odds_data = odds_response['odds']
            
            # Handle case where API returns a list (usually empty) instead of a dict
            if isinstance(odds_data, list):
                if not odds_data:
                    logger.debug("Odds data is an empty list, no odds found")
                    return {}
                else:
                    logger.warning(f"Odds data is a list with {len(odds_data)} items, expected dict. First item: {odds_data[0]}")
                    return {}

            for event_id_str, event_odds in odds_data.items():
                try:
                    event_id = int(event_id_str)
                    choices = event_odds.get('choices', [])
                    
                    if not choices:
                        logger.debug(f"No choices found for event {event_id}")
                        continue
                    
                    # Initialize odds data structure
                    processed_odds = {
                        'one_initial': None,
                        'x_initial': None,
                        'two_initial': None,
                        'one_final': None,
                        'x_final': None,
                        'two_final': None
                    }
                    
                    # Process each choice (1, X, 2)
                    for choice in choices:
                        choice_name = choice.get('name', '')
                        initial_fractional = choice.get('initialFractionalValue', '')
                        current_fractional = choice.get('fractionalValue', '')
                        
                        # Convert fractional to decimal
                        initial_decimal = fractional_to_decimal(initial_fractional)
                        current_decimal = fractional_to_decimal(current_fractional)
                        
                        # Map to our standard fields
                        if choice_name == '1':
                            processed_odds['one_initial'] = initial_decimal
                            processed_odds['one_final'] = current_decimal
                        elif choice_name == 'X':
                            processed_odds['x_initial'] = initial_decimal
                            processed_odds['x_final'] = current_decimal
                        elif choice_name == '2':
                            processed_odds['two_initial'] = initial_decimal
                            processed_odds['two_final'] = current_decimal
                    
                    # Only add if we have at least home and away odds
                    if processed_odds['one_initial'] and processed_odds['two_initial']:
                        odds_map[event_id] = processed_odds
                        logger.debug(f"Processed odds for event {event_id}: 1:{processed_odds['one_final']}, X:{processed_odds['x_final']}, 2:{processed_odds['two_final']}")
                    else:
                        logger.debug(f"Incomplete odds for event {event_id}, skipping")
                
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error processing odds for event {event_id_str}: {e}")
                    continue
            
            logger.info(f"Processed odds for {len(odds_map)} events")
            return odds_map
            
        except Exception as e:
            logger.error(f"Error processing odds response: {e}")
            return {}
    
    def _filter_events_with_odds(self, events_response: Dict, odds_event_ids: Set[int]) -> List[Dict]:
        """
        Filter events list to only include events that have odds data.
        
        Args:
            events_response: Response from get_today_sport_events_response
            odds_event_ids: Set of event IDs that have odds
        
        Returns:
            List of event objects that have odds
        """
        try:
            if not events_response or 'events' not in events_response:
                logger.warning("No events found in response")
                return []
            
            all_events = events_response['events']
            filtered_events = []
            
            for event in all_events:
                event_id = event.get('id')
                if event_id and event_id in odds_event_ids:
                    filtered_events.append(event)
            
            logger.info(f"Filtered {len(filtered_events)} events with odds out of {len(all_events)} total events")
            return filtered_events
            
        except Exception as e:
            logger.error(f"Error filtering events: {e}")
            return []
    
    def _filter_upcoming_events(self, events: List[Dict], min_minutes_away: int = 10) -> List[Dict]:
        """
        Filter events to only include those that haven't started yet and are at least min_minutes_away from starting.
        
        Args:
            events: List of event objects with startTimestamp field
            min_minutes_away: Minimum minutes away from current time (default: 10)
        
        Returns:
            List of upcoming event objects
        """
        try:
            if not events:
                return []
            
            # Get current time (timezone-aware)
            current_time = get_local_now_aware()
            current_timestamp = int(current_time.timestamp())
            
            # Calculate minimum start timestamp (current time + 10 minutes)
            min_start_timestamp = current_timestamp + (min_minutes_away * 60)
            
            upcoming_events = []
            filtered_count = 0
            
            for event in events:
                start_timestamp = event.get('startTimestamp')
                
                if not start_timestamp:
                    logger.debug(f"Event {event.get('id', 'unknown')} has no startTimestamp, skipping")
                    filtered_count += 1
                    continue
                
                # Filter: keep only events starting at least min_minutes_away from now
                if start_timestamp >= min_start_timestamp:
                    upcoming_events.append(event)
                else:
                    event_id = event.get('id', 'unknown')
                    time_diff_minutes = (start_timestamp - current_timestamp) / 60
                    logger.debug(f"Filtered out event {event_id}: starts in {time_diff_minutes:.1f} minutes (< {min_minutes_away} min threshold)")
                    filtered_count += 1
            
            logger.info(f"Filtered {len(upcoming_events)} upcoming events (excluded {filtered_count} events that already started or are starting soon)")
            return upcoming_events
            
        except Exception as e:
            logger.error(f"Error filtering upcoming events: {e}")
            return events  # Return original list on error
    
    def _process_and_insert_event(self, event: Dict, odds_data: Dict) -> bool:
        """
        Process a single event and its odds, then insert into database.
        
        Args:
            event: Event object from API response
            odds_data: Processed odds data for this event
        
        Returns:
            True if successful, False otherwise
        """
        try:
            event_id = event.get('id')
            if not event_id:
                logger.warning("Event has no ID, skipping")
                return False
            
            # Extract event information using existing function
            event_data = self.api_client.get_event_information(event, discovery_source='daily_discovery')
            
            if not event_data or not event_data.get('id'):
                logger.warning(f"Could not extract event information for event {event_id}")
                return False
            
            # Upsert event to database
            db_event = EventRepository.upsert_event(event_data)
            if not db_event:
                logger.error(f"Failed to upsert event {event_id} to database")
                return False
            
            logger.info(f"✅ Upserted event {event_id}: {event_data['homeTeam']} vs {event_data['awayTeam']}")
            
            # Validate odds data
            if not validate_odds_data(odds_data):
                logger.warning(f"Invalid odds data for event {event_id}, skipping odds insertion")
                return True  # Event was inserted, odds weren't
            
            # Create odds snapshot (with both initial and final odds)
            snapshot = OddsRepository.create_odds_snapshot(event_id, odds_data)
            if snapshot:
                logger.debug(f"Created odds snapshot for event {event_id}")
            
            # Upsert event odds (stores initial as 'open' and final as 'final')
            event_odds_id = OddsRepository.upsert_event_odds(event_id, odds_data)
            if event_odds_id:
                logger.info(f"✅ Upserted odds for event {event_id}: 1:{odds_data['one_final']}, X:{odds_data.get('x_final')}, 2:{odds_data['two_final']}")
            else:
                logger.warning(f"Failed to upsert event odds for event {event_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing event {event.get('id', 'unknown')}: {e}")
            return False
    
    def extract_todays_events(self, date: str, sports: List[str] = None) -> Dict[str, int]:
        """
        Main function to extract today's events with odds and insert into database.
        
        Args:
            date: Date string in format YYYY-MM-DD
            sports: Optional list of sports to extract. If None, extracts default sports.
        
        Returns:
            Dict with statistics: {'events_processed': X, 'events_inserted': Y, 'odds_inserted': Z}
        """

        if sports is None:
            sports = [
                'basketball',
                'tennis',
                'baseball',
                'ice-hockey',
                'american-football',
                'football',
                'handball',
            ]
        
        logger.info(f"🔍 Starting daily discovery for date: {date}")
        
        # Aggregate statistics across all sports
        total_events_processed = 0
        total_events_inserted = 0
        total_odds_inserted = 0
        
        try:
            for sport in sports:
                try:
                    logger.info(f"🏀 Processing {sport}...")
                    
                    # Step 1: Fetch odds endpoint first to get event IDs with odds
                    logger.info(f"📊 Fetching today's {sport} odds...")
                    odds_response = self.api_client.get_today_sport_events_odds_response(date, sport)
                    
                    if not odds_response:
                        logger.warning(f"No odds response for {sport}, skipping")
                        DailyDiscoveryRepository.update_sport_status(date, sport, 'failed')
                        continue
                    
                    # Step 2: Process odds response to extract event IDs and odds data
                    odds_map = self._process_odds_response(odds_response)
                    
                    if not odds_map:
                        logger.info(f"No events with odds found for {sport}")
                        continue
                    
                    odds_event_ids = set(odds_map.keys())
                    logger.info(f"Found {len(odds_event_ids)} {sport} events with odds")
                    
                    # Step 3: Fetch events endpoint
                    logger.info(f"📅 Fetching today's {sport} events...")
                    events_response = self.api_client.get_today_sport_events_response(date, sport)
                    
                    if not events_response:
                        logger.warning(f"No events response for {sport}, skipping")
                        DailyDiscoveryRepository.update_sport_status(date, sport, 'failed')
                        continue
                    
                    # Step 4: Filter events to only those with odds
                    filtered_events = self._filter_events_with_odds(events_response, odds_event_ids)
                    
                    if not filtered_events:
                        logger.info(f"No matching {sport} events found after filtering")
                        continue
                    
                    # Step 5: Filter events to only upcoming ones (not started, at least 10 minutes away)
                    upcoming_events = self._filter_upcoming_events(filtered_events, min_minutes_away=10)
                    
                    if not upcoming_events:
                        logger.info(f"No upcoming {sport} events found after time filtering")
                        continue
                    
                    # Step 6: Process and insert each event with its odds
                    logger.info(f"Processing {len(upcoming_events)} {sport} events...")
                    sport_events_inserted = 0
                    sport_odds_inserted = 0
                    
                    for event in upcoming_events:
                        event_id = event.get('id')
                        if not event_id:
                            continue
                        
                        # Get odds for this event
                        event_odds = odds_map.get(event_id)
                        if not event_odds:
                            logger.warning(f"No odds found for event {event_id}, skipping")
                            continue
                        
                        # Process and insert event with odds
                        success = self._process_and_insert_event(event, event_odds)
                        if success:
                            sport_events_inserted += 1
                            # Check if odds were actually inserted by validating the odds data
                            if validate_odds_data(event_odds):
                                sport_odds_inserted += 1
                    
                    DailyDiscoveryRepository.update_sport_status(date, sport, 'completed')
                    
                    logger.info(f"✅ {sport} completed: {sport_events_inserted}/{len(upcoming_events)} events inserted, {sport_odds_inserted} with odds")
                    
                    # Aggregate statistics
                    total_events_processed += len(upcoming_events)
                    total_events_inserted += sport_events_inserted
                    total_odds_inserted += sport_odds_inserted
                    
                except Exception as e:
                    logger.error(f"Error processing {sport}: {e}")
                    DailyDiscoveryRepository.update_sport_status(date, sport, 'failed')
                    continue  # Continue with next sport even if one fails
            
            logger.info(f"✅ Daily discovery completed for all sports: {total_events_inserted}/{total_events_processed} events inserted, {total_odds_inserted} with odds")
            
            return {
                'events_processed': total_events_processed,
                'events_inserted': total_events_inserted,
                'odds_inserted': total_odds_inserted
            }
            
        except Exception as e:
            logger.error(f"Error in extract_todays_events: {e}")
            return {'events_processed': 0, 'events_inserted': 0, 'odds_inserted': 0}


# Global instance
today_sport_extractor = TodaySportExtractor()


def run_daily_discovery(sports=None):
    """
    Job function to be called by scheduler.
    Extracts today's events with odds and inserts into database.
    """
    try:
        # Get today's date in YYYY-MM-DD format
        today = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"🚀 Running daily discovery job for {today}")
        
        # Run extraction
        stats = today_sport_extractor.extract_todays_events(today, sports)
        
        logger.info(f"📊 Daily discovery stats: {stats}")
        
        return stats
        
    except Exception as e:
        logger.error(f"Error in run_daily_discovery: {e}")
        return None

