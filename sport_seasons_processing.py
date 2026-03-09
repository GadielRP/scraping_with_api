"""
Sport Seasons Processing - Generic event extractor for any sport/competition

Usage:
    python sport_seasons_processing.py <season_id> --tournament <tournament_id>
    
Example:
    python sport_seasons_processing.py 65360 --tournament 132  # NBA 2024/2025
    python sport_seasons_processing.py 58766 --tournament 17   # Premier League 2024/2025
    
The script fetches all events for a given season and:
1. Creates/updates events in the database
2. Saves all available betting markets (using MarketRepository)
3. Extracts and saves results for finished events
"""

from sofascore_api import api_client
from repository import ResultRepository, MarketRepository
from database import db_manager
from typing import Dict, List
import logging
import argparse
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

season_to_process = [
    {"season_name": "NBA 25/26", "tournament_id": 132, "season_id": 80229},
    {"season_name": "NBA 24/25", "tournament_id": 132, "season_id": 65360},
    {"season_name": "NBA 23/24", "tournament_id": 132, "season_id": 54105},
    {"season_name": "NBA 22/23", "tournament_id": 132, "season_id": 45096},
    {"season_name": "NBA 21/22", "tournament_id": 132, "season_id": 38191},
    {"season_name": "NBA 20/21", "tournament_id": 132, "season_id": 34951},
    {"season_name": "Laliga 2025", "tournament_id": 8, "season_id": 77559},
    {"season_name": "Laliga 2024", "tournament_id": 8, "season_id": 61643},
    {"season_name": "Laliga 2023", "tournament_id": 8, "season_id": 52376},
    {"season_name": "Laliga 2022", "tournament_id": 8, "season_id": 42409},
    {"season_name": "Laliga 2021", "tournament_id": 8, "season_id": 37223},
    {"season_name": "Laliga 2020", "tournament_id": 8, "season_id": 32501},
    {"season_name": "Premier League 2025", "tournament_id": 17, "season_id": 76986},
    {"season_name": "Premier League 2024", "tournament_id": 17, "season_id": 61627},
    {"season_name": "Premier League 2023", "tournament_id": 17, "season_id": 52186},
    {"season_name": "Premier League 2022", "tournament_id": 17, "season_id": 41886},
    {"season_name": "Premier League 2021", "tournament_id": 17, "season_id": 37036},
    {"season_name": "Premier League 2020", "tournament_id": 17, "season_id": 29415},
    {"season_name": "NFL 2025", "tournament_id": 9464, "season_id": 75522},
    {"season_name": "NFL 2024", "tournament_id": 9464, "season_id": 60592},
    {"season_name": "NFL 2023", "tournament_id": 9464, "season_id": 51361},
    {"season_name": "NFL 2022", "tournament_id": 9464, "season_id": 46786},
    {"season_name": "NFL 2021", "tournament_id": 9464, "season_id": 36422},
    {"season_name": "NFL 2020", "tournament_id": 9464, "season_id": 27719},
    {"season_name": "MLB 2025", "tournament_id": 11205, "season_id": 84695},
    {"season_name": "MLB 2024", "tournament_id": 11205, "season_id": 68611},
    {"season_name": "NHL 2025", "tournament_id": 234, "season_id": 78476},
    {"season_name": "SeriaA 2025", "tournament_id": 23, "season_id": 76457},
    {"season_name": "Bundesliga 2025", "tournament_id": 35, "season_id": 77333},
    {"season_name": "League 1", "tournament_id": 12, "season_id": 77356}
]


def initialize_database():
    """
    Initialize database schema - creates tables and applies migrations.
    
    Steps:
    1. Test database connection
    2. Create tables if they don't exist
    3. Run schema migrations (adds missing columns like season_id, round, etc.)
    """
    logger.info("Initializing database schema...")
    
    try:
        if not db_manager.test_connection():
            logger.error("❌ Database connection failed - cannot proceed")
            return False
        
        logger.info("Creating database tables if they don't exist...")
        db_manager.create_tables()
        
        logger.info("Checking and applying schema migrations...")
        if not db_manager.check_and_migrate_schema():
            logger.warning("⚠️ Schema migration check failed, but continuing...")
        
        logger.info("✅ Database schema initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize database: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def fetch_season_events(tournament_id: int, season_id: int) -> List[Dict]:
    """
    Fetch all events for a given season by incrementing fetch_number until 404.
    Works for any sport/competition.
    
    Args:
        tournament_id: The SofaScore unique tournament ID (e.g., 132 for NBA, 17 for Premier League)
        season_id: The SofaScore season ID
    
    Returns:
        List of event dictionaries extracted from the API responses
    """
    all_events = []
    fetch_number = 0
    
    logger.info(f"Starting to fetch events for tournament {tournament_id}, season {season_id}")
    
    while True:
        endpoint = f"/unique-tournament/{tournament_id}/season/{season_id}/events/last/{fetch_number}"
        logger.info(f"Fetching batch {fetch_number}...")
        
        # Use _make_request with no_retry_on_404=True to stop when we reach the end
        response = api_client._make_request(endpoint, no_retry_on_404=True)
        
        if not response:
            logger.info(f"No more events found (received 404 or error). Total batches fetched: {fetch_number}")
            break
        
        # Extract events from response
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

def process_season(tournament_id: int, season_id: int):
    """
    Fetch and process all events for a given season.
    Works for any sport/competition.
    
    Uses the new market-based odds flow (MarketRepository) to save ALL available
    betting markets, not just 1X2 odds.
    
    Args:
        tournament_id: The SofaScore unique tournament ID
        season_id: The SofaScore season ID
    """
    logger.info("=" * 80)
    logger.info(f"Processing Season: Tournament {tournament_id}, Season {season_id}")
    logger.info("=" * 80)
    
    # Step 1: Fetch all events
    events = fetch_season_events(tournament_id, season_id)
    
    if not events:
        logger.warning(f"No events found for tournament {tournament_id}, season {season_id}")
        return
    
    # Step 2: Sort events by startTimestamp (oldest to newest)
    events.sort(key=lambda x: x['startTimestamp'])
    logger.info("Events sorted by start timestamp (oldest to newest)")
    
    # Step 3: Process events individually and fetch odds for each
    logger.info(f"Starting to process {len(events)} events with discovery_source='scraping_on_command'")
    from repository import EventRepository
    
    processed_count = 0
    skipped_count = 0
    markets_processed_count = 0
    markets_skipped_count = 0
    results_processed_count = 0
    
    for event_data in events:
        try:
            # Upsert event
            event = EventRepository.upsert_event(event_data)
            
            if event:
                processed_count += 1
                logger.debug(f"✅ Event {event_data['id']} upserted: {event_data.get('homeTeam')} vs {event_data.get('awayTeam')}")
                
                # Check if event already has markets stored before fetching odds
                existing_market_count = MarketRepository.get_market_count(event_data['id'])
                
                if existing_market_count > 0:
                    # Event already has markets stored, skip odds fetching
                    markets_skipped_count += 1
                    logger.debug(f"⏭️ Event {event_data['id']} already has {existing_market_count} markets stored, skipping odds fetch")
                else:
                    # Fetch and save ALL markets using MarketRepository (new flow)
                    try:
                        final_odds_response = api_client.get_event_final_odds(event_data['id'], event_data.get('slug'))
                        
                        if final_odds_response:
                            # Save all markets using the new market-based flow
                            saved_markets = MarketRepository.save_markets_from_response(event_data['id'], final_odds_response)
                            if saved_markets > 0:
                                markets_processed_count += 1
                                logger.debug(f"✅ Saved {saved_markets} markets for event {event_data['id']}")
                            else:
                                logger.debug(f"No markets saved for event {event_data['id']}")
                        else:
                            logger.debug(f"No odds response for event {event_data['id']}")
                    except Exception as e:
                        logger.error(f"Error processing markets for event {event_data['id']}: {e}")
                
                # Extract and upsert results for this event (using already-fetched data)
                try:
                    existing_result = ResultRepository.get_result_by_event_id(event_data['id'])
                    if existing_result:
                        logger.debug(f"Results already exist for event {event_data['id']}, skipping")
                    else:
                        raw_event = event_data.get('_raw_event')
                        if raw_event:
                            result_data = api_client.extract_results_from_response({'event': raw_event})
                            
                            if result_data and not result_data.get('_canceled'):
                                if ResultRepository.upsert_result(event_data['id'], result_data):
                                    results_processed_count += 1
                                    logger.debug(f"✅ Results upserted for event {event_data['id']}: {result_data.get('home_score')}-{result_data.get('away_score')}")
                                else:
                                    logger.warning(f"Failed to upsert results for event {event_data['id']}")
                            else:
                                logger.debug(f"No results data for event {event_data['id']} (may not be finished)")
                        else:
                            logger.warning(f"No raw event data for event {event_data['id']}")
                except Exception as e:
                    logger.error(f"Error processing results for event {event_data['id']}: {e}")
            else:
                skipped_count += 1
                logger.warning(f"Failed to upsert event {event_data['id']}")
                
        except Exception as e:
            skipped_count += 1
            logger.error(f"Error processing event {event_data.get('id')}: {e}")
            continue
    
    logger.info("=" * 80)
    logger.info(f"Season Processing Complete! (Tournament: {tournament_id}, Season: {season_id})")
    logger.info(f"Processed: {processed_count}/{len(events)} events")
    logger.info(f"Skipped: {skipped_count} events")
    logger.info(f"Markets processed: {markets_processed_count}/{processed_count} events")
    logger.info(f"Markets skipped (already stored): {markets_skipped_count}/{processed_count} events")
    logger.info(f"Results processed: {results_processed_count}/{processed_count} events")
    logger.info("=" * 80)

def main():
    """
    Main CLI interface for season processing.
    
    Usage:
        python sport_seasons_processing.py <season_id> --tournament <tournament_id>
        
    Examples:
        python sport_seasons_processing.py 65360 -t 132      # NBA 2024/2025
        python sport_seasons_processing.py 58766 -t 17       # Premier League 2024/2025
        python sport_seasons_processing.py 61644 -t 8        # La Liga 2024/2025
    
    Common tournament IDs:
        NBA: 132
        Premier League: 17
        La Liga: 8
        Serie A: 23
        Bundesliga: 35
        Ligue 1: 34
        Champions League: 7
        Euroleague: 138
    """
    parser = argparse.ArgumentParser(
        description='Extract and process season events from SofaScore API (any sport)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python sport_seasons_processing.py 65360 -t 132    # NBA 2024/2025
  python sport_seasons_processing.py 58766 -t 17     # Premier League 2024/2025
  
Common tournament IDs:
  NBA: 132 | Premier League: 17 | La Liga: 8 | Serie A: 23
  Bundesliga: 35 | Ligue 1: 34 | Champions League: 7 | Euroleague: 138
        """
    )
    parser.add_argument(
        'season_id',
        type=int,
        nargs='?',
        help='SofaScore season ID to process'
    )
    parser.add_argument(
        '-t', '--tournament',
        type=int,
        required=False,
        help='SofaScore unique tournament ID (e.g., 132 for NBA, 17 for Premier League)'
    )
    parser.add_argument(
        '--auto',
        action='store_true',
        help='Automatically process all seasons defined in season_to_process list'
    )
    parser.add_argument(
        '--season_name',
        type=str,
        help='Process a specific season by name from the season_to_process list'
    )
    parser.add_argument(
        '--skip-db-init',
        action='store_true',
        help='Skip database initialization (use only if schema is up to date)'
    )
    
    args = parser.parse_args()
    
    # Initialize database schema first (unless skipped)
    if not args.skip_db_init:
        if not initialize_database():
            logger.error("Database initialization failed. Use --skip-db-init to bypass.")
            return
    else:
        logger.warning("⚠️ Skipping database initialization - ensure schema is up to date!")
    
    # Logic for different execution modes
    if args.auto:
        logger.info(f"Starting auto-processing for {len(season_to_process)} seasons...")
        for i, season in enumerate(season_to_process):
            t_id = season['tournament_id']
            s_id = season['season_id']
            s_name = season['season_name']
            
            logger.info(f"[{i+1}/{len(season_to_process)}] Auto-processing: {s_name} (ID: {s_id}, Tournament: {t_id})")
            process_season(t_id, s_id)
            
            if i < len(season_to_process) - 1:
                logger.info("Waiting 60 seconds before next season to avoid detection...")
                time.sleep(60)
        
        logger.info("✅ Auto-processing complete!")

    elif args.season_name:
        # Find matching season
        matched_season = next((s for s in season_to_process if s['season_name'].lower() == args.season_name.lower()), None)
        
        if matched_season:
            logger.info(f"Found match for season name: {matched_season['season_name']}")
            process_season(matched_season['tournament_id'], matched_season['season_id'])
        else:
            logger.error(f"❌ No season found with name: '{args.season_name}' in season_to_process list.")
            logger.info("Available seasons:")
            for s in season_to_process:
                logger.info(f" - {s['season_name']}")
    
    elif args.season_id and args.tournament:
        # Process the single season provided via CLI
        process_season(args.tournament, args.season_id)
    
    else:
        logger.error("❌ Missing arguments. Provide EITHER (--auto) OR (--season_name NAME) OR (season_id -t tournament_id)")
        parser.print_help()


if __name__ == "__main__":
    main()