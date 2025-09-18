from database import db_manager
from datetime import datetime, timedelta
from sqlalchemy import or_, and_
from models import Event, EventObservation
from typing import List
import logging
from sofascore_api import api_client
from sport_observations import sport_observations_manager

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def log_print(message):
    """Print to console and log file"""
    print(message)
    logger.info(message)

class GetAllCourts:
    """
    Script to extract and save ground type observations for finished tennis events.
    Uses the same extraction logic that works in the midnight sync job.
    """

    @staticmethod
    def get_all_finished_tennis_events(limit: int = None) -> List[Event]:
        """Get all finished Tennis events (assumed cutoff: 4 hours)."""
        try:
            with db_manager.get_session() as session:
                now = datetime.now()

                query = session.query(Event).filter(
                    and_(
                        Event.sport == 'Tennis',
                        Event.start_time_utc < now - timedelta(hours=4)
                    )
                ).order_by(Event.start_time_utc.desc())
                
                if limit:
                    query = query.limit(limit)

                events = query.all()
                log_print(f"Found {len(events)} finished tennis events")
                return events

        except Exception as e:
            log_print(f"Error getting finished tennis events: {e}")
            return []

    @staticmethod
    def get_events_without_ground_type(limit: int = None) -> List[Event]:
        """Get finished tennis events that don't have ground type observations yet."""
        try:
            with db_manager.get_session() as session:
                now = datetime.now()

                # Get tennis events without ground type observations
                query = session.query(Event).filter(
                    and_(
                        Event.sport == 'Tennis',
                        Event.start_time_utc < now - timedelta(hours=4),
                        ~Event.id.in_(
                            session.query(EventObservation.event_id).filter(
                                EventObservation.observation_type == 'ground_type'
                            )
                        )
                    )
                ).order_by(Event.start_time_utc.desc())
                
                if limit:
                    query = query.limit(limit)

                events = query.all()
                log_print(f"Found {len(events)} tennis events without ground type")
                return events

        except Exception as e:
            log_print(f"Error getting tennis events without ground type: {e}")
            return []

    @staticmethod
    def extract_ground_type_for_event(event: Event) -> bool:
        """
        Extract ground type for a single tennis event using the same logic as midnight sync.
        Returns True if successful, False otherwise.
        """
        try:
            log_print(f"🎾 Processing event {event.id}: {event.home_team} vs {event.away_team}")
            
            # Use the same API call that works in midnight sync
            api_response = api_client._make_request(f"/event/{event.id}")
            
            if not api_response:
                log_print(f"❌ Failed to fetch API response for event {event.id} (likely proxy issue)")
                return False
            
            # Use the same extraction logic that works in sport_observations
            observations = api_client._extract_observations_from_response(api_response)
            
            if observations:
                # Use the same processing logic as midnight sync
                sport_observations_manager.process_event_observations(event, {'observations': observations})
                log_print(f"✅ Successfully extracted ground type for event {event.id}")
                return True
            else:
                log_print(f"⚠️ No ground type found for event {event.id}")
                return False
                
        except Exception as e:
            log_print(f"❌ Error processing event {event.id}: {e}")
            # If it's a proxy error, we'll continue with other events
            if "proxy" in str(e).lower() or "407" in str(e) or "tunnel" in str(e).lower():
                log_print(f"🔄 Proxy issue for event {event.id}, continuing with next event...")
            return False

    @staticmethod
    def process_events(events: List[Event]) -> dict:
        """
        Process a list of tennis events to extract ground type.
        Returns statistics about the processing.
        """
        stats = {'processed': 0, 'successful': 0, 'failed': 0, 'no_ground_type': 0}
        
        for event in events:
            stats['processed'] += 1
            log_print(f"📊 Processing {stats['processed']}/{len(events)}: Event {event.id}")
            
            success = GetAllCourts.extract_ground_type_for_event(event)
            
            if success:
                stats['successful'] += 1
            else:
                stats['failed'] += 1
        
        return stats

    @staticmethod
    def run_test(limit: int = 10):
        """Run a test with a limited number of events."""
        log_print(f"🧪 Running test with {limit} events...")
        
        # Get ALL finished tennis events (not just those without ground type)
        events = GetAllCourts.get_all_finished_tennis_events(limit=limit)
        
        if not events:
            log_print("✅ No finished tennis events found!")
            return
        
        log_print(f"🎾 Found {len(events)} events to process:")
        for event in events:
            log_print(f"  - {event.id}: {event.home_team} vs {event.away_team} ({event.start_time_utc.strftime('%Y-%m-%d %H:%M')})")
        
        # Process events
        stats = GetAllCourts.process_events(events)
        
        # Report results
        log_print(f"📊 Test Results:")
        log_print(f"  - Processed: {stats['processed']}")
        log_print(f"  - Successful: {stats['successful']}")
        log_print(f"  - Failed: {stats['failed']}")
        log_print(f"  - Success Rate: {(stats['successful']/stats['processed']*100):.1f}%")

    @staticmethod
    def run_full():
        """Run the full process for ALL tennis events (regardless of existing ground type)."""
        log_print("🚀 Running full ground type extraction for ALL tennis events...")
        
        # Get ALL finished tennis events (not just those without ground type)
        events = GetAllCourts.get_all_finished_tennis_events()
        
        if not events:
            log_print("✅ No finished tennis events found!")
            return
        
        log_print(f"🎾 Found {len(events)} events to process")
        
        # Process events
        stats = GetAllCourts.process_events(events)
        
        # Report results
        log_print(f"📊 Full Results:")
        log_print(f"  - Processed: {stats['processed']}")
        log_print(f"  - Successful: {stats['successful']}")
        log_print(f"  - Failed: {stats['failed']}")
        log_print(f"  - Success Rate: {(stats['successful']/stats['processed']*100):.1f}%")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "full":
        GetAllCourts.run_full()
    else:
        GetAllCourts.run_test(limit=10)
