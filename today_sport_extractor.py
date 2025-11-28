import logging
from typing import Dict, List, Optional, Set
from datetime import datetime
from decimal import Decimal

import sofascore_api2  # Import to attach methods to SofaScoreAPI class
from sofascore_api import api_client
from repository import EventRepository, OddsRepository
from odds_utils import fractional_to_decimal, validate_odds_data

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
            odds_response: Response from get_today_basketball_events_odds_response with structure:
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
            events_response: Response from get_today_basketball_events_response
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
    
    def extract_todays_events(self, date: str) -> Dict[str, int]:
        """
        Main function to extract today's events with odds and insert into database.
        
        Args:
            date: Date string in format YYYY-MM-DD
        
        Returns:
            Dict with statistics: {'events_processed': X, 'events_inserted': Y, 'odds_inserted': Z}
        """
        try:
            logger.info(f"🔍 Starting daily discovery for date: {date}")
            
            # Step 1: Fetch odds endpoint first to get event IDs with odds
            logger.info("📊 Fetching today's basketball odds...")
            odds_response = self.api_client.get_today_basketball_events_odds_response(date)
            
            if not odds_response:
                logger.error("Failed to fetch odds response")
                return {'events_processed': 0, 'events_inserted': 0, 'odds_inserted': 0}
            
            # Step 2: Process odds response to extract event IDs and odds data
            odds_map = self._process_odds_response(odds_response)
            
            if not odds_map:
                logger.warning("No events with odds found for today")
                return {'events_processed': 0, 'events_inserted': 0, 'odds_inserted': 0}
            
            odds_event_ids = set(odds_map.keys())
            logger.info(f"Found {len(odds_event_ids)} events with odds")
            
            # Step 3: Fetch events endpoint
            logger.info("📅 Fetching today's basketball events...")
            events_response = self.api_client.get_today_basketball_events_response(date)
            
            if not events_response:
                logger.error("Failed to fetch events response")
                return {'events_processed': 0, 'events_inserted': 0, 'odds_inserted': 0}
            
            # Step 4: Filter events to only those with odds
            filtered_events = self._filter_events_with_odds(events_response, odds_event_ids)
            
            if not filtered_events:
                logger.warning("No matching events found after filtering")
                return {'events_processed': 0, 'events_inserted': 0, 'odds_inserted': 0}
            
            # Step 5: Process and insert each event with its odds
            logger.info(f"Processing {len(filtered_events)} events...")
            events_inserted = 0
            odds_inserted = 0
            
            for event in filtered_events:
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
                    events_inserted += 1
                    # Check if odds were actually inserted by validating the odds data
                    if validate_odds_data(event_odds):
                        odds_inserted += 1
            
            logger.info(f"✅ Daily discovery completed: {events_inserted}/{len(filtered_events)} events inserted, {odds_inserted} with odds")
            
            return {
                'events_processed': len(filtered_events),
                'events_inserted': events_inserted,
                'odds_inserted': odds_inserted
            }
            
        except Exception as e:
            logger.error(f"Error in extract_todays_events: {e}")
            return {'events_processed': 0, 'events_inserted': 0, 'odds_inserted': 0}


# Global instance
today_sport_extractor = TodaySportExtractor()


def run_daily_discovery():
    """
    Job function to be called by scheduler.
    Extracts today's events with odds and inserts into database.
    """
    try:
        # Get today's date in YYYY-MM-DD format
        today = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"🚀 Running daily discovery job for {today}")
        
        # Run extraction
        stats = today_sport_extractor.extract_todays_events(today)
        
        logger.info(f"📊 Daily discovery stats: {stats}")
        
        return stats
        
    except Exception as e:
        logger.error(f"Error in run_daily_discovery: {e}")
        return None

