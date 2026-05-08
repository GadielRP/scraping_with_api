"""
LEGACY: old event_odds / odds_snapshot migration script. Not compatible with final market-based odds architecture.

Collect initial and final odds for yesterday's events.
Reuses existing code from scheduler.py job_results_collection.
"""

import logging
from datetime import datetime, timedelta
from typing import List
from sqlalchemy.orm import joinedload
from sqlalchemy import or_
from repository import EventRepository, OddsRepository
from models import Event, EventOdds
from database import db_manager
from modules.jobs.pre_start_check_job.odds_extraction import extract_final_odds_from_response
from modules.sofascore import api_client
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_events_with_null_opening_odds() -> List[Event]:
    """
    Query for events that have null one_open or two_open odds.
    Returns a list of Event objects with event_odds relationship loaded.
    """
    try:
        with db_manager.get_session() as session:
            # Query events with event_odds relationship loaded
            # Filter for events where one_open OR two_open is NULL
            events = session.query(Event).options(
                joinedload(Event.event_odds)
            ).filter(
                Event.event_odds.has(
                    or_(
                        EventOdds.one_open.is_(None),
                        EventOdds.two_open.is_(None)
                    )
                )
            ).all()
            
            logger.info(f"Found {len(events)} events with null opening odds")
            return events
            
    except Exception as e:
        logger.error(f"Error getting events with null opening odds: {e}")
        return []

def collect_yesterday_odds():
    """
    Collect initial and final odds for events from the previous day that have null opening odds.
    Reuses logic from scheduler.py job_results_collection.
    """
    logger.info("Starting collection of yesterday's odds")
    
    try:
        # Get events from previous day (same logic as job_results_collection)
        yesterday = datetime.now() - timedelta(days=1)
        all_events = EventRepository.get_events_by_date(yesterday.date())
        
        if not all_events:
            logger.info("No events found from previous day")
            return
        
        logger.info(f"Found {len(all_events)} total events from previous day")
        
        # Filter to only events with null opening odds
        events = get_events_with_null_opening_odds()
        
        # Further filter to only yesterday's events with null odds
        yesterday_events = []
        for event in events:
            # Check if event is from yesterday
            if event.start_time_utc.date() == yesterday.date():
                yesterday_events.append(event)
        
        events = yesterday_events
        
        if not events:
            logger.info("No events found with null opening odds from previous day")
            return
        
        logger.info(f"Found {len(events)} events with null opening odds from previous day")
        
        # Process each event (same logic as job_results_collection lines 873-905)
        success_count = 0
        failed_count = 0
        
        for event in events:
            try:
                logger.info(f"Processing event {event.id}: {event.home_team} vs {event.away_team}")
                
                # Fetch final odds with initial extraction enabled
                final_odds_response = api_client.get_event_final_odds(event.id, event.slug)
                
                if final_odds_response:
                    # Extract both initial and final odds
                    final_odds_data = extract_final_odds_from_response(final_odds_response, initial_odds_extraction=True)
                    
                    if final_odds_data:
                        # Update event odds in database
                        upserted_id = OddsRepository.upsert_event_odds(event.id, final_odds_data)
                        
                        if upserted_id:
                            # Create odds snapshot
                            snapshot = OddsRepository.create_odds_snapshot(event.id, final_odds_data)
                            
                            if snapshot:
                                logger.info(f"✅ Odds collected for event {event.id}: {event.home_team} vs {event.away_team}")
                                success_count += 1
                            else:
                                logger.warning(f"Failed to create snapshot for event {event.id}")
                                failed_count += 1
                        else:
                            logger.warning(f"Failed to update odds for event {event.id}")
                            failed_count += 1
                    else:
                        logger.warning(f"No odds data extracted for event {event.id}")
                        failed_count += 1
                else:
                    logger.warning(f"Failed to fetch odds for event {event.id}")
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing event {event.id}: {e}")
                failed_count += 1
        
        # Summary
        logger.info("=" * 60)
        logger.info(f"Collection complete:")
        logger.info(f"  Success: {success_count}")
        logger.info(f"  Failed: {failed_count}")
        logger.info(f"  Total: {len(events)}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error in collect_yesterday_odds: {e}")
        raise

if __name__ == "__main__":
    try:
        collect_yesterday_odds()
        logger.info("Script completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)
