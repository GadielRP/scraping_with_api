from sofascore_api import api_client
from optimization import process_events_only
from repository import SeasonRepository, OddsRepository, ResultRepository
from database import db_manager
from typing import Dict, List, Tuple, Optional
import logging
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def initialize_database():
    """
    Initialize database schema - creates tables and applies migrations.
    This ensures the database is ready for NBA seasons processing.
    
    Steps:
    1. Test database connection
    2. Create tables if they don't exist
    3. Run schema migrations (adds missing columns like season_id, round, etc.)
    
    This is similar to main.py's initialize_system() but focused on schema only.
    """
    logger.info("Initializing database schema...")
    
    try:
        # Step 1: Test database connection
        if not db_manager.test_connection():
            logger.error("❌ Database connection failed - cannot proceed")
            return False
        
        # Step 2: Create tables if they don't exist
        # This creates all tables defined in models.py (including new Season table)
        logger.info("Creating database tables if they don't exist...")
        db_manager.create_tables()
        
        # Step 3: Run schema migrations
        # This adds any missing columns to existing tables (e.g., season_id, round to events table)
        logger.info("Checking and applying schema migrations...")
        if not db_manager.check_and_migrate_schema():
            logger.warning("⚠️ Schema migration check failed, but continuing...")
            # Don't return False - allow script to continue even if migration has issues
        
        logger.info("✅ Database schema initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize database: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# NOTE: This list is ONLY used for CLI command matching.
# Actual season names are extracted from event data when processing events.
# Seasons are created automatically in the database from the event data.
NBA_SEASONS = [
    {"season_name": "NBA 2020/2021", "season_id": 34951, "year": 2020},
    {"season_name": "NBA 2021/2022", "season_id": 38191, "year": 2021},
    {"season_name": "NBA 2022/2023", "season_id": 45096, "year": 2022},
    {"season_name": "NBA 2023/2024", "season_id": 54105, "year": 2023},
    {"season_name": "NBA CUP 2023/2024", "season_id": 56094, "year": 2023},
    {"season_name": "NBA 2024/2025", "season_id": 65360, "year": 2024},
    {"season_name": "NBA CUP 2024/2025", "season_id": 69143, "year": 2024},
    {"season_name": "NBA 2025/2026", "season_id": 80229, "year": 2025},
    {"season_name": "NBA CUP 2025/2026", "season_id": 84238, "year": 2025}   
]

NBA_ROUNDS = [
        {
            "round": 189,
            "name": "Eastern conference first round",
            "slug": "eastern-conference-first-round"
        },
        {
            "round": 221,
            "name": "Western conference first round",
            "slug": "western-conference-first-round"
        },
        {
            "round": 195,
            "name": "Eastern conference semifinals",
            "slug": "eastern-conference-semifinals"
        },
        {
            "round": 227,
            "name": "Western conference semifinals",
            "slug": "western-conference-semifinals"
        },
        {
            "round": 217,
            "name": "Western conference finals",
            "slug": "western-conference-finals"
        },
        {
            "round": 183,
            "name": "Eastern conference finals",
            "slug": "eastern-conference-finals"
        },
        {
            "round": 134,
            "name": "NBA Finals",
            "slug": "nba-finals"
        },
        {
            "round": 1,
            "name": "Regular season",
            "slug": "regular_season"
        }
    ]

def initialize_nba_seasons():
    """
    NOTE: This function is kept for backward compatibility but does nothing.
    Seasons are now created automatically from event data when processing events.
    The NBA_SEASONS list is only used for CLI command matching.
    """
    logger.info("NBA seasons will be created automatically from event data during processing.")
    logger.info("The NBA_SEASONS list is only used for CLI command matching.")

def fetch_nba_season_events(season_id: int, season_name: str) -> List[Dict]:
    """
    Fetch all events for a given NBA season by incrementing fetch_number until 404.
    
    Args:
        season_id: The SofaScore season ID
        season_name: The season name for logging (e.g., "2024/2025")
    
    Returns:
        List of event dictionaries extracted from the API responses
    """
    all_events = []
    fetch_number = 0
    NBA_COMPETITION_ID = 132  # NBA unique tournament ID
    
    logger.info(f"Starting to fetch events for NBA season {season_name} (ID: {season_id})")
    
    while True:
        endpoint = f"/unique-tournament/{NBA_COMPETITION_ID}/season/{season_id}/events/last/{fetch_number}"
        logger.info(f"Fetching batch {fetch_number}...")
        
        # Use _make_request with no_retry_on_404=True to stop when we reach the end
        response = api_client._make_request(endpoint, no_retry_on_404=True)
        
        if not response:
            logger.info(f"No more events found (received 404 or error). Total batches fetched: {fetch_number}")
            break
        
        # Extract events from response (similar structure to dropping odds)
        if 'events' in response:
            batch_events = response['events']
            logger.info(f"Batch {fetch_number}: Found {len(batch_events)} events")
            
            # Process each event through get_event_information
            for event in batch_events:
                try:
                    event_data = api_client.get_event_information(event, discovery_source='scraping_on_command')
                    
                    # Validate required fields
                    required_fields = ['id', 'slug', 'startTimestamp', 'sport', 'competition', 'homeTeam', 'awayTeam']
                    if all(event_data.get(field) for field in required_fields):
                        # Store the raw event for later result extraction (avoids redundant API calls)
                        event_data['_raw_event'] = event
                        all_events.append(event_data)
                    else:
                        logger.warning(f"Event {event.get('id')} missing required fields, skipping")
                        
                except Exception as e:
                    logger.error(f"Error processing event in batch {fetch_number}: {e}")
                    continue
        else:
            logger.warning(f"Batch {fetch_number}: No 'events' key in response")
        
        fetch_number += 1
    
    logger.info(f"Fetching complete. Total events extracted: {len(all_events)}")
    return all_events

def process_nba_season(season_id: int, season_name: str):
    """
    Fetch and process all events for a given NBA season.
    
    Args:
        season_id: The SofaScore season ID
        season_name: The season name for logging (e.g., "2024/2025")
    """
    logger.info(f"=" * 80)
    logger.info(f"Processing NBA Season: {season_name} (ID: {season_id})")
    logger.info(f"=" * 80)
    
    # Step 1: Fetch all events
    events = fetch_nba_season_events(season_id, season_name)
    
    if not events:
        logger.warning(f"No events found for season {season_name}")
        return
    
    # Step 2: Sort events by startTimestamp (oldest to newest)
    # The API returns batches with latest games first, but within each batch it's oldest to newest
    # We want consistent chronological order across all batches
    events.sort(key=lambda x: x['startTimestamp'])
    logger.info(f"Events sorted by start timestamp (oldest to newest)")
    
    # Step 3: Process events individually and fetch odds for each right away
    logger.info(f"Starting to process {len(events)} events with discovery_source='scraping_on_command'")
    from repository import EventRepository
    
    processed_count = 0
    skipped_count = 0
    odds_processed_count = 0
    results_processed_count = 0
    
    for event_data in events:
        try:
            # Upsert event
            event = EventRepository.upsert_event(event_data)
            
            if event:
                processed_count += 1
                logger.debug(f"✅ Event {event_data['id']} upserted: {event_data.get('homeTeam')} vs {event_data.get('awayTeam')}")
                
                # Immediately fetch and process odds for this event (following scheduler.py flow)
                try:
                    # Fetch final odds for this event
                    final_odds_response = api_client.get_event_final_odds(event_data['id'], event_data.get('slug'))
                    
                    if final_odds_response:
                        # Process the final odds data
                        final_odds_data = api_client.extract_final_odds_from_response(final_odds_response, initial_odds_extraction=True)
                        
                        if final_odds_data:
                            # Update the event odds with final odds
                            upserted_id = OddsRepository.upsert_event_odds(event_data['id'], final_odds_data)
                            if upserted_id:
                                logger.debug(f"✅ Final odds updated for {event_data.get('homeTeam')} vs {event_data.get('awayTeam')}")
                                
                                # Create final odds snapshot
                                snapshot = OddsRepository.create_odds_snapshot(event_data['id'], final_odds_data)
                                if snapshot:
                                    logger.debug(f"✅ Final odds snapshot created for event {event_data['id']}")
                                    odds_processed_count += 1
                                else:
                                    logger.warning(f"Failed to create odds snapshot for event {event_data['id']}")
                            else:
                                logger.warning(f"Failed to update final odds for event {event_data['id']}")
                        else:
                            logger.warning(f"No final odds data extracted for event {event_data['id']}")
                    else:
                        logger.warning(f"Failed to fetch final odds for event {event_data['id']}")
                except Exception as e:
                    logger.error(f"Error processing odds for event {event_data['id']}: {e}")
                    # Continue processing other events even if odds fail
                
                # Extract and upsert results for this event (using already-fetched data)
                try:
                    # Check if result already exists
                    existing_result = ResultRepository.get_result_by_event_id(event_data['id'])
                    if existing_result:
                        logger.debug(f"Results already exist for event {event_data['id']}, skipping")
                    else:
                        # Extract results from the raw event data we already have (no additional API call needed)
                        raw_event = event_data.get('_raw_event')
                        if raw_event:
                            # Wrap raw event in expected format: {'event': event_data}
                            result_data = api_client.extract_results_from_response({'event': raw_event})
                            
                            if result_data:
                                # Upsert result with home_sets and away_sets
                                if ResultRepository.upsert_result(event_data['id'], result_data):
                                    results_processed_count += 1
                                    logger.debug(f"✅ Results upserted for event {event_data['id']}: {result_data.get('home_score')}-{result_data.get('away_score')}, Winner: {result_data.get('winner')}")
                                    if result_data.get('home_sets') or result_data.get('away_sets'):
                                        logger.debug(f"  Sets - Home: {result_data.get('home_sets')}, Away: {result_data.get('away_sets')}")
                                else:
                                    logger.warning(f"Failed to upsert results for event {event_data['id']}")
                            else:
                                logger.debug(f"No results data extracted for event {event_data['id']} (may not be finished yet)")
                        else:
                            logger.warning(f"No raw event data found for event {event_data['id']}")
                except Exception as e:
                    logger.error(f"Error processing results for event {event_data['id']}: {e}")
                    # Continue processing other events even if results fail
            else:
                skipped_count += 1
                logger.warning(f"Failed to upsert event {event_data['id']}")
                
        except Exception as e:
            skipped_count += 1
            logger.error(f"Error processing event {event_data.get('id')}: {e}")
            continue
    
    logger.info(f"=" * 80)
    logger.info(f"Season {season_name} Processing Complete!")
    logger.info(f"Processed: {processed_count}/{len(events)} events")
    logger.info(f"Skipped: {skipped_count} events")
    logger.info(f"Odds processed: {odds_processed_count}/{processed_count} events")
    logger.info(f"Results processed: {results_processed_count}/{processed_count} events")
    logger.info(f"=" * 80)

def main():
    """Main CLI interface for NBA season processing"""
    parser = argparse.ArgumentParser(description='Extract and process NBA season events from SofaScore API')
    parser.add_argument(
        'season',
        type=str,
        nargs='?',  # Make season optional
        help='Season to process (format: "2024/2025" or season index 0-5)'
    )
    parser.add_argument(
        '--init',
        action='store_true',
        help='[DEPRECATED] Seasons are now created automatically from event data. This flag does nothing.'
    )
    parser.add_argument(
        '--skip-db-init',
        action='store_true',
        help='Skip database initialization (use only if you\'re sure schema is up to date)'
    )
    
    args = parser.parse_args()
    
    # Initialize database schema first (unless skipped)
    if not args.skip_db_init:
        if not initialize_database():
            logger.error("Database initialization failed. Use --skip-db-init to bypass, but this may cause errors.")
            return
    else:
        logger.warning("⚠️ Skipping database initialization - ensure schema is up to date!")
    
    # If --init flag is provided, show message and exit
    if args.init:
        logger.info("Note: Seasons are now created automatically from event data during processing.")
        logger.info("The NBA_SEASONS list is only used for CLI command matching.")
        initialize_nba_seasons()
        return
    
    # If no season provided and no --init flag, show usage
    if not args.season:
        logger.error("Please provide a season to process")
        logger.info("Usage: python nba_seasons_processing.py <season>")
        logger.info("\nAvailable seasons:")
        for i, s in enumerate(NBA_SEASONS):
            logger.info(f"  [{i}] {s['season_name']} (ID: {s['season_id']})")
        return
    
    # Find the season in NBA_SEASONS
    season = None
    
    # Try to match by season_name
    for s in NBA_SEASONS:
        if s['season_name'] == args.season:
            season = s
            break
    
    # If not found, try as index
    if not season:
        try:
            index = int(args.season)
            if 0 <= index < len(NBA_SEASONS):
                season = NBA_SEASONS[index]
        except ValueError:
            pass
    
    if not season:
        logger.error(f"Season '{args.season}' not found!")
        logger.info("Available seasons:")
        for i, s in enumerate(NBA_SEASONS):
            logger.info(f"  [{i}] {s['season_name']} (ID: {s['season_id']})")
        return
    
    # Process the selected season
    # Note: Seasons will be created automatically from event data during processing
    # The season_name here is only used for logging - actual season names come from events
    process_nba_season(season['season_id'], season['season_name'])

if __name__ == "__main__":
    main()