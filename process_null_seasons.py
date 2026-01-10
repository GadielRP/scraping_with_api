import logging
import sys
import time
import os
from datetime import datetime, timedelta
from typing import List, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('process_null_seasons.log', encoding='utf-8')
    ]
)
logger = logging.getLogger("process_null_seasons")

from database import db_manager
from models import Event, refresh_materialized_views
from sofascore_api import api_client
from repository import EventRepository, OddsRepository, ResultRepository, MarketRepository
from sport_observations import sport_observations_manager
from timezone_utils import get_local_now

# State file to track last processed event ID
STATE_FILE = 'process_null_seasons_last_id.log'

# Custom exception for 403 errors
class ForbiddenError(Exception):
    """Raised when a 403 error is encountered"""
    pass

def get_state_file_path() -> str:
    """Get the absolute path to the state file in the script directory"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, STATE_FILE)

def load_last_processed_id() -> Optional[int]:
    """
    Load the last processed event ID from the state file.
    
    Returns:
        int if state file exists and contains a valid ID, None otherwise
    """
    try:
        state_file_path = get_state_file_path()
        
        if os.path.exists(state_file_path):
            logger.info(f"✅ State file found at: {state_file_path}")
            with open(state_file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
                if lines:
                    # Get the last non-empty line
                    for line in reversed(lines):
                        line = line.strip()
                        if line:
                            last_id = int(line)
                            logger.info(f"📂 Loaded last processed event ID: {last_id}")
                            return last_id
        else:
            logger.info(f"📂 No state file found - starting fresh")
        return None
    except ValueError as e:
        logger.warning(f"❌ Error parsing event ID from state file: {e}")
        return None
    except Exception as e:
        logger.warning(f"❌ Error loading state file: {e}")
        return None

def save_last_processed_id(event_id: int):
    """
    Save the last processed event ID to the state file.
    
    Args:
        event_id: The event ID that was just processed
    """
    state_file_path = get_state_file_path()
    
    try:
        # Ensure directory exists
        dir_path = os.path.dirname(state_file_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        
        with open(state_file_path, 'a', encoding='utf-8') as f:
            f.write(f"{event_id}\n")
        logger.debug(f"💾 Saved state: Last processed event ID = {event_id}")
    except Exception as e:
        logger.error(f"❌ Error saving state file: {e}")

def get_events_with_null_season(min_id: int = 269, last_processed_id: Optional[int] = None) -> List[tuple]:
    """
    Query for all events where season_id is null and start_time_utc is in the past, sorted by ID.
    
    Args:
        min_id: Minimum event ID to process (default 269)
        last_processed_id: Resume from this ID if provided
        
    Returns:
        List of tuples (event_id, slug) sorted by event_id
    """
    try:
        now = get_local_now()
        with db_manager.get_session() as session:
            query = session.query(Event).filter(
                Event.season_id == None,
                Event.id > min_id,
                Event.start_time_utc < now
            )
            
            # If resuming, start after the last processed ID
            if last_processed_id:
                query = query.filter(Event.id > last_processed_id)
            
            # Sort by ID for consistent processing order
            events = query.order_by(Event.id).all()
            
            if last_processed_id:
                logger.info(f"Found {len(events)} events with season_id null, id > {last_processed_id} and start_time < {now} (resuming)")
            else:
                logger.info(f"Found {len(events)} events with season_id null, id > {min_id} and start_time < {now}")
            
            # Return event IDs and slugs to avoid session issues
            return [(e.id, e.slug) for e in events]
    except Exception as e:
        logger.error(f"Error querying events with null season: {e}")
        return []

def detect_403_error(func):
    """
    Decorator to detect 403 errors in API calls.
    Monitors the logger for 403 error messages.
    """
    def wrapper(*args, **kwargs):
        # Create a custom handler to capture log messages
        class ErrorDetector(logging.Handler):
            def __init__(self):
                super().__init__()
                self.found_403 = False
            
            def emit(self, record):
                if '403' in record.getMessage():
                    self.found_403 = True
        
        detector = ErrorDetector()
        detector.setLevel(logging.WARNING)
        
        # Add handler to api logger
        api_logger = logging.getLogger('sofascore_api')
        api_logger.addHandler(detector)
        
        try:
            result = func(*args, **kwargs)
            if detector.found_403:
                raise ForbiddenError("403 Forbidden error detected")
            return result
        finally:
            api_logger.removeHandler(detector)
    
    return wrapper

@detect_403_error
def process_event(event_id: int, slug: str):
    """
    Process a single event using the midnight sync logic.
    Raises ForbiddenError if a 403 error is encountered.
    """
    try:
        logger.info(f"Processing event {event_id} ({slug})")
        
        # 1. Update final odds (matches Job E logic)
        try:
            final_odds_response = api_client.get_event_final_odds(event_id, slug)
            if final_odds_response:
                final_odds_data = api_client.extract_final_odds_from_response(final_odds_response, initial_odds_extraction=True)
                if final_odds_data:
                    upserted_id = OddsRepository.upsert_event_odds(event_id, final_odds_data)
                    if upserted_id:
                        snapshot = OddsRepository.create_odds_snapshot(event_id, final_odds_data)
                        if snapshot:
                            logger.info(f"✅ Final odds and snapshot updated for event {event_id}")
                    
                    # Save all markets
                    try:
                        MarketRepository.save_markets_from_response(event_id, final_odds_response)
                        logger.info(f"✅ Markets saved for event {event_id}")
                    except Exception as e:
                        logger.warning(f"Error saving markets to DB for event {event_id}: {e}")
                else:
                    logger.warning(f"No final odds data extracted for event {event_id}")
            else:
                logger.debug(f"No final odds response for event {event_id}")
        except Exception as e:
            logger.warning(f"Error updating odds for event {event_id}: {e}")

        # 2. Collect results and update event info (matches Job D/E logic)
        if ResultRepository.get_result_by_event_id(event_id):
            logger.info(f"Results already exist for event {event_id}, but checking/updating event info anyway")
            # Still call API to update season_id if it's null
            api_client.get_event_results(event_id)
        else:
            result_data = api_client.get_event_results(event_id)
            if result_data:
                if ResultRepository.upsert_result(event_id, result_data):
                    logger.info(f"✅ Result updated for event {event_id}: {result_data['home_score']}-{result_data['away_score']}")
                    
                    # Process observations
                    event_obj = EventRepository.get_event_by_id(event_id)
                    if event_obj:
                        sport_observations_manager.process_event_observations(event_obj, result_data)
                        logger.info(f"✅ Observations processed for event {event_id}")
            else:
                logger.warning(f"Could not fetch results for event {event_id}")

    except Exception as e:
        logger.error(f"Error processing event {event_id}: {e}")
        raise  # Re-raise to allow 403 detection

def main():
    logger.info("=" * 80)
    logger.info("NULL SEASON PROCESSING SCRIPT")
    logger.info("=" * 80)
    logger.info("Simulating midnight sync logic for all events with null season_id")
    logger.info("Processing events in ID order with progress tracking")
    logger.info("=" * 80)
    
    try:
        # Load last processed ID
        last_processed_id = load_last_processed_id()
        if last_processed_id:
            logger.info(f"📂 Resuming from last processed event ID: {last_processed_id}")
        else:
            logger.info("📂 Starting fresh (no previous state found)")
        
        # Get events to process
        events_to_process = get_events_with_null_season(min_id=269, last_processed_id=last_processed_id)
        
        if not events_to_process:
            logger.info("No events with null season_id found. Nothing to do.")
            return

        total = len(events_to_process)
        count = 0
        last_saved_id = last_processed_id
        
        logger.info(f"📊 Processing {total} events...")
        logger.info("=" * 80 + "\n")
        
        for event_id, slug in events_to_process:
            count += 1
            
            try:
                logger.info(f"\n[{count}/{total}] Event ID: {event_id}")
                
                # Process the event (with 403 detection)
                process_event(event_id, slug)
                
                # Save progress after each successful event
                save_last_processed_id(event_id)
                last_saved_id = event_id
                
                # Small delay to be respectful to the API
                time.sleep(0.5)
                
            except ForbiddenError as e:
                logger.error(f"\n{'=' * 80}")
                logger.error(f"🚫 403 FORBIDDEN ERROR DETECTED!")
                logger.error(f"{'=' * 80}")
                logger.error(f"Stopped at event ID: {event_id}")
                logger.error(f"Last successfully processed ID: {last_saved_id}")
                logger.error(f"Progress saved. You can resume by running the script again.")
                logger.error(f"{'=' * 80}")
                sys.exit(1)
                
            except KeyboardInterrupt:
                logger.warning(f"\n\n⚠️  Script interrupted by user (Ctrl+C)")
                logger.info(f"💾 Last processed event ID saved: {last_saved_id}")
                logger.info("   You can resume by running the script again.")
                sys.exit(130)
                
            except Exception as e:
                logger.error(f"Error processing event {event_id}: {e}")
                # Continue to next event instead of stopping
                logger.info("⏭️  Continuing to next event...")
                continue

        # 3. Refresh materialized views
        logger.info("\n" + "=" * 80)
        logger.info("🔄 Refreshing alert materialized views...")
        try:
            refresh_materialized_views(db_manager.engine)
            logger.info("✅ Materialized views refreshed")
        except Exception as e:
            logger.error(f"Error refreshing materialized views: {e}")

        logger.info("\n" + "=" * 80)
        logger.info("✅ NULL SEASON PROCESSING COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Total events processed: {count}")
        if last_saved_id:
            logger.info(f"Last processed event ID: {last_saved_id}")
        logger.info("=" * 80)
        
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  Script interrupted by user (Ctrl+C). Exiting...")
        logger.info("💾 Progress has been saved. You can resume by running the script again.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
