#!/usr/bin/env python3
"""
Historical Results Extraction Script

This script extracts/updates results for events with id > 269, excluding NBA seasons.
It processes events in daily batches to avoid API rate limiting/banning.
It ALWAYS processes events, even if results already exist, to update them with new
extraction methods, data columns, linked tables, and correct any mistakenly extracted odds/results.

Uses the same flow as scheduler.job_results_collection():
- Updates final odds for all events
- Extracts/updates results (upsert - creates or updates)
- Processes observations (court type, rankings, etc.)

Usage:
    # Process all remaining days (updates existing results)
    python extract_historical_results.py
    
    # Test mode: show what would be processed (only first day, no database changes)
    python extract_historical_results.py --test
    
    # Process only 5 days
    python extract_historical_results.py --days 5
    
    # Test mode: process first day and show what would be saved/updated (dry-run)
    python extract_historical_results.py --test --days 1
    
    # Initialize state file with hardcoded date (2025-10-23)
    python extract_historical_results.py --init-state
    
    # Manual mode: extract from MANUAL_START_DATE to MANUAL_END_DATE
    python extract_historical_results.py --manual

CLI Arguments:
    --test       : Test mode - shows what would be processed without saving to database
                   Only processes first day and displays detailed summary
                   Shows CREATE vs UPDATE actions for results
    --days N     : Limit processing to N days (default: all remaining days)
    --init-state : Initialize state file with hardcoded date (2025-10-23)
                   Creates file in script directory. Exits after creation.
    --manual     : Manual mode - extract from MANUAL_START_DATETIME to MANUAL_END_DATETIME
                   Ignores state file and --days parameter
                   Uses datetime range defined in constants (MANUAL_START_DATETIME, MANUAL_END_DATETIME)
                   Can specify both date and time, e.g., datetime(2025, 11, 15, 11, 30) for Nov 15 at 11:30

Note: This script ALWAYS processes events, even if results exist, to ensure
      data is updated with the latest extraction methods and corrections.

Requirements:
    - Database connection configured in config.py
    - All dependencies installed (see requirements.txt)
"""

import logging
import sys
import os
import argparse
import json
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
from sqlalchemy import and_, or_, func, cast, Date

# Import existing project modules
from database import db_manager
from models import Event, Result
from repository import ResultRepository, EventRepository
from sofascore_api import api_client
try:
    from sofascore_api import SofaScoreRateLimitException
except ImportError:
    # If not available, define a dummy class
    class SofaScoreRateLimitException(Exception):
        pass
from sport_observations import sport_observations_manager

# Custom exception for rate limiting (self-contained, doesn't modify sofascore_api.py)
class RateLimitDetected(Exception):
    """Raised when we detect a 403 rate limit error from API responses"""
    pass

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('extract_historical_results.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# NBA seasons to exclude (provided by user)
NBA_SEASONS_TO_EXCLUDE = [
    34951,  # 2020/2021
    38191,  # 2021/2022
    45096,  # 2022/2023
    54105,  # 2023/2024
    65360,  # 2024/2025
    80229,  # 2025/2026
    56094,  # NBA CUP 2023/2024
    69143,  # NBA CUP 2024/2025
    84238,  # NBA CUP 2025/2026
    # seasons to exclude, not nba seasons but placed here for simplicity
    77559,  # Laliga 2025
    61643,  # Laliga 2024
    52376,  # Laliga 2023
    42409,  # Laliga 2022
    37223,  # Laliga 2021
    32501,  # Laliga 2020
    # Premier League seasons
    76986,  # Premier League 2025
    61627,  # Premier League 2024
    52186,  # Premier League 2023
    41886,  # Premier League 2022
    37036,  # Premier League 2021
    29415,  # Premier League 2020
    # NFL seasons
    75522,  # NFL 2025
    60592,  # NFL 2024
    51361,  # NFL 2023
    46786,  # NFL 2022
    36422,  # NFL 2021
    27719,  # NFL 2020
]

# Minimum event ID
LAST_ID = 269

# Track consecutive API failures to detect rate limiting
_consecutive_api_failures = 0
_last_403_check_time = None

def is_rate_limited() -> bool:
    """
    Check if we're being rate limited by looking at recent API behavior.
    Returns True if we detect 403 errors.
    
    This is a self-contained check that doesn't modify sofascore_api.py.
    We detect rate limiting by:
    1. Checking for consecutive None responses from API
    2. Looking for 403 in recent log output (if accessible)
    """
    global _consecutive_api_failures
    
    # If we have too many consecutive failures, likely rate limited
    if _consecutive_api_failures >= 3:
        logger.warning(f"Detected {_consecutive_api_failures} consecutive API failures - likely rate limited")
        return True
    
    return False

def reset_failure_counter():
    """Reset the API failure counter after a successful call"""
    global _consecutive_api_failures
    _consecutive_api_failures = 0

def increment_failure_counter():
    """Increment the API failure counter"""
    global _consecutive_api_failures
    _consecutive_api_failures += 1

# Ensure data directory exists for persistence (relative to /app/ in Docker)
DATA_DIR = 'data' if os.path.exists('data') else ''

# State file to track last processed date (simple log file with just the date - kept for debugging/visual)
STATE_FILE = os.path.join(DATA_DIR, 'extract_historical_results_last_date.log')

# JSON state file to track last processed event ID (allows granular resumption within a day)
STATE_JSON_FILE = os.path.join(DATA_DIR, 'extract_historical_results_state.json')

# Manual mode datetime range constants
# Can specify both date and time, e.g., datetime(2025, 11, 15, 11, 30) for Nov 15 at 11:30
MANUAL_START_DATETIME = datetime(2025, 11, 22, 23, 59, 59)  # Start datetime for manual mode
MANUAL_END_DATETIME = datetime(2025, 11, 25, 23, 59, 59)   # End datetime for manual mode (inclusive)


def get_state_file_path() -> str:
    """
    Get the absolute path to the state file.
    Uses the current working directory to ensure we find the file regardless of where the script is run from.
    """
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, STATE_FILE)


def find_state_file() -> Optional[str]:
    """
    Find the state file in multiple possible locations.
    Returns the first path where the file exists, or None if not found.
    
    Priority order:
    1. Directory where script is located (most reliable)
    2. Current working directory
    3. Root directory (/)
    4. Common server paths
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cwd = os.getcwd()
    
    # List of possible locations to search (prioritize script directory and CWD)
    search_paths = [
        # 1. Directory where script is located (most reliable - same dir as script)
        os.path.join(script_dir, STATE_FILE),
        # 2. Current working directory
        os.path.join(cwd, STATE_FILE),
        # 3. Root directory (/)
        os.path.join('/', STATE_FILE),
        # 4. Common server paths (if running in Docker or directly on server)
        os.path.join('/opt/sofascore', STATE_FILE),
        os.path.join('/app', STATE_FILE),
    ]
    
    # Remove duplicates while preserving order
    seen = set()
    unique_paths = []
    for path in search_paths:
        if path not in seen:
            seen.add(path)
            unique_paths.append(path)
    
    for path in unique_paths:
        if os.path.exists(path):
            return path
    
    return None


def load_last_processed_date() -> Optional[date]:
    """
    Load the last processed date from log file.
    The file contains only the date in yyyy-mm-dd format.
    
    Returns:
        date object if state file exists, None otherwise
    """
    try:
        # Search for state file in multiple locations
        script_dir = os.path.dirname(os.path.abspath(__file__))
        logger.info(f"🔍 Searching for state file...")
        logger.info(f"   Script location: {os.path.abspath(__file__)}")
        logger.info(f"   Script directory: {script_dir}")
        
        # List files in script directory for debugging
        try:
            files_in_script_dir = [f for f in os.listdir(script_dir) if os.path.isfile(os.path.join(script_dir, f))]
            if STATE_FILE in files_in_script_dir:
                logger.info(f"   ✅ Found {STATE_FILE} in script directory!")
            else:
                logger.info(f"   ❌ {STATE_FILE} NOT in script directory")
                logger.info(f"   Files in script directory: {sorted(files_in_script_dir)[:10]}...")  # Show first 10
        except Exception as e:
            logger.debug(f"   Could not list files in script directory: {e}")
        
        state_file_path = find_state_file()
        
        if state_file_path:
            logger.info(f"✅ State file found at: {state_file_path}")
            with open(state_file_path, 'r', encoding='utf-8') as f:
                # Read all lines
                lines = f.readlines()
                logger.info(f"📄 Read {len(lines)} lines from state file")
                
                # Log all lines for debugging
                if lines:
                    logger.info(f"📋 File contents: {[line.strip() for line in lines]}")
                
                if lines:
                    # Get the last non-empty line
                    last_line = None
                    for line in reversed(lines):
                        line = line.strip()
                        if line:
                            last_line = line
                            break
                    
                    if last_line:
                        logger.info(f"📅 Found last processed date: {last_line}")
                        # Parse date from yyyy-mm-dd format
                        parsed_date = datetime.strptime(last_line, '%Y-%m-%d').date()
                        logger.info(f"📂 Loaded last processed date: {parsed_date.strftime('%Y-%m-%d')}")
                        return parsed_date
                    else:
                        logger.warning("⚠️  State file exists but contains only empty lines")
                else:
                    logger.warning("⚠️  State file exists but is empty")
        else:
            # Log all search locations for debugging (same logic as find_state_file)
            script_dir = os.path.dirname(os.path.abspath(__file__))
            cwd = os.getcwd()
            search_paths = [
                os.path.join(script_dir, STATE_FILE),
                os.path.join(cwd, STATE_FILE),
                os.path.join('/', STATE_FILE),
                os.path.join('/opt/sofascore', STATE_FILE),
                os.path.join('/app', STATE_FILE),
            ]
            # Remove duplicates
            seen = set()
            unique_paths = []
            for path in search_paths:
                if path not in seen:
                    seen.add(path)
                    unique_paths.append(path)
            
            logger.warning(f"⚠️  State file not found in any of these locations:")
            for path in unique_paths:
                exists = "✅ EXISTS" if os.path.exists(path) else "❌ NOT FOUND"
                logger.info(f"   {exists}: {path}")
            logger.info(f"📂 Current working directory: {cwd}")
            logger.info(f"📂 Script directory: {script_dir}")
            logger.info(f"📂 Script file: {os.path.abspath(__file__)}")
        return None
    except ValueError as e:
        logger.warning(f"❌ Error parsing date from state file: {e}")
        return None
    except Exception as e:
        logger.warning(f"❌ Error loading state file: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def init_state_file(initial_date: str = '2025-10-23', force: bool = False) -> bool:
    """
    Initialize the state file with a hardcoded date.
    Creates the file in the script directory (same location as the script).
    
    Args:
        initial_date: The date to write to the file (format: yyyy-mm-dd)
        force: If True, overwrite existing file without asking (useful for Docker/non-interactive)
    
    Returns:
        True if file was created successfully, False otherwise
    """
    try:
        # Use script directory as the location (most reliable)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        state_file_path = os.path.join(script_dir, STATE_FILE)
        
        # Check if file already exists
        if os.path.exists(state_file_path):
            logger.warning(f"⚠️  State file already exists at: {state_file_path}")
            with open(state_file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    logger.info(f"   Current content: {content}")
                else:
                    logger.info(f"   Current content: (empty)")
            
            if not force:
                # Only ask for confirmation if not forced (interactive mode)
                try:
                    response = input(f"   Overwrite? (y/N): ").strip().lower()
                    if response != 'y':
                        logger.info("   Skipping file creation.")
                        return False
                except (EOFError, KeyboardInterrupt):
                    # Handle non-interactive mode (Docker, scripts, etc.)
                    logger.warning("   Non-interactive mode detected. Use --force to overwrite.")
                    logger.info("   Skipping file creation.")
                    return False
            else:
                logger.info("   Force mode: overwriting existing file.")
        
        # Ensure directory exists
        dir_path = os.path.dirname(state_file_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        
        # Create file with the hardcoded date
        with open(state_file_path, 'w', encoding='utf-8') as f:
            f.write(f"{initial_date}\n")
        
        logger.info(f"✅ Created state file: {state_file_path}")
        logger.info(f"   Initial date: {initial_date}")
        logger.info(f"   File location: {state_file_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating state file: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def save_last_processed_date(processed_date: date):
    """
    Save the last processed date to log file.
    Appends the date in yyyy-mm-dd format to the log file.
    If the file exists, saves to that location. Otherwise, saves to current working directory.
    
    Args:
        processed_date: The date that was just processed
    """
    date_str = processed_date.strftime('%Y-%m-%d')
    
    # Try to find existing file first
    existing_file = find_state_file()
    if existing_file:
        state_file_path = existing_file
        logger.info(f"💾 Saving state to existing file: {state_file_path}")
    else:
        # If no existing file, save to current working directory (most accessible)
        state_file_path = os.path.join(os.getcwd(), STATE_FILE)
        logger.info(f"💾 No existing state file found, saving to: {state_file_path}")
    
    try:
        # Ensure directory exists (only if there's a directory component)
        dir_path = os.path.dirname(state_file_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        
        with open(state_file_path, 'a', encoding='utf-8') as f:
            f.write(f"{date_str}\n")
        logger.info(f"✅ Saved state: Last processed date = {date_str} (file: {state_file_path})")
    except Exception as e:
        logger.error(f"❌ Error saving state file: {e}")
        import traceback
        logger.error(traceback.format_exc())
        # Try fallback to script directory
        try:
            script_file = get_state_file_path()
            logger.info(f"🔄 Trying fallback location: {script_file}")
            dir_path = os.path.dirname(script_file)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)
            with open(script_file, 'a', encoding='utf-8') as f:
                f.write(f"{date_str}\n")
            logger.info(f"✅ Saved state to fallback location: {script_file}")
        except Exception as e2:
            logger.error(f"❌ Failed to save to fallback location: {e2}")


def get_json_state_file_path() -> str:
    """Get the absolute path to the JSON state file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, STATE_JSON_FILE)


def load_json_state() -> Dict:
    """
    Load the JSON state file containing last processed date and event ID.
    
    Returns:
        Dict with 'last_date' (str, yyyy-mm-dd) and 'last_event_id' (int), or empty dict if not found
    """
    try:
        state_file_path = get_json_state_file_path()
        
        if os.path.exists(state_file_path):
            with open(state_file_path, 'r', encoding='utf-8') as f:
                state = json.load(f)
                logger.info(f"📂 Loaded JSON state: date={state.get('last_date')}, event_id={state.get('last_event_id')}")
                return state
        else:
            logger.info(f"📂 No JSON state file found at {state_file_path}")
            return {}
            
    except Exception as e:
        logger.warning(f"❌ Error loading JSON state file: {e}")
        return {}


def save_json_state(processed_date: date, last_event_id: int):
    """
    Save the current progress (date + event_id) to JSON state file.
    This allows granular resumption within a day.
    
    Args:
        processed_date: The date currently being processed
        last_event_id: The ID of the last successfully processed event
    """
    try:
        state_file_path = get_json_state_file_path()
        
        state = {
            'last_date': processed_date.strftime('%Y-%m-%d'),
            'last_event_id': last_event_id,
            'updated_at': datetime.now().isoformat()
        }
        
        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)
        
        logger.debug(f"💾 Saved JSON state: date={state['last_date']}, event_id={last_event_id}")
        
    except Exception as e:
        logger.error(f"❌ Error saving JSON state file: {e}")


def clear_json_state_event_id():
    """
    Clear the event_id from JSON state (when starting a new day).
    Keeps the date but removes event_id to process from the beginning.
    """
    try:
        state_file_path = get_json_state_file_path()
        
        if os.path.exists(state_file_path):
            with open(state_file_path, 'r', encoding='utf-8') as f:
                state = json.load(f)
            
            # Clear event_id
            state['last_event_id'] = None
            state['updated_at'] = datetime.now().isoformat()
            
            with open(state_file_path, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2)
            
            logger.debug(f"🔄 Cleared event_id from JSON state (starting new day)")
            
    except Exception as e:
        logger.warning(f"Error clearing JSON state event_id: {e}")




def get_events_for_date(target_date: date) -> List[Event]:
    """
    Get all events for a specific date matching the query criteria.
    
    Query matches:
    SELECT * FROM events
    WHERE id > 269
      AND (season_id IS NULL OR season_id NOT IN (34951, 38191, ...))
      AND DATE(start_time_utc) = target_date
    ORDER BY start_time_utc
    
    Args:
        target_date: The date to query events for
        
    Returns:
        List of Event objects ordered by start_time_utc
    """
    try:
        with db_manager.get_session() as session:
            # Calculate date range (start of day to end of day)
            day_start = datetime.combine(target_date, datetime.min.time())
            day_end = day_start + timedelta(days=1)
            
            # Query matching the exact SQL provided by user
            events = session.query(Event).filter(
                and_(
                    Event.id > LAST_ID,
                    or_(
                        Event.season_id.is_(None),
                        ~Event.season_id.in_(NBA_SEASONS_TO_EXCLUDE)
                    ),
                    Event.start_time_utc >= day_start,
                    Event.start_time_utc < day_end
                )
            ).order_by(Event.start_time_utc).all()
            
            return events
            
    except Exception as e:
        logger.error(f"Error querying events for date {target_date}: {e}")
        return []


def get_events_for_datetime_range(start_datetime: datetime, end_datetime: datetime) -> List[Event]:
    """
    Get all events within a datetime range matching the query criteria.
    
    Query matches:
    SELECT * FROM events
    WHERE id > 269
      AND (season_id IS NULL OR season_id NOT IN (34951, 38191, ...))
      AND start_time_utc >= start_datetime
      AND start_time_utc <= end_datetime
    ORDER BY start_time_utc
    
    Args:
        start_datetime: The start datetime (inclusive)
        end_datetime: The end datetime (inclusive)
        
    Returns:
        List of Event objects ordered by start_time_utc
    """
    try:
        with db_manager.get_session() as session:
            # Query matching the criteria with datetime range
            events = session.query(Event).filter(
                and_(
                    Event.id > LAST_ID,
                    or_(
                        Event.season_id.is_(None),
                        ~Event.season_id.in_(NBA_SEASONS_TO_EXCLUDE)
                    ),
                    Event.start_time_utc >= start_datetime,
                    Event.start_time_utc <= end_datetime
                )
            ).order_by(Event.start_time_utc).all()
            
            return events
            
    except Exception as e:
        logger.error(f"Error querying events for datetime range {start_datetime} to {end_datetime}: {e}")
        return []


def get_all_available_dates() -> List[date]:
    """
    Get all unique dates that have events matching the query criteria.
    
    Returns:
        List of date objects ordered chronologically
    """
    try:
        with db_manager.get_session() as session:
            # Get distinct dates from events matching criteria
            dates = session.query(
                cast(Event.start_time_utc, Date).label('event_date')
            ).filter(
                and_(
                    Event.id > LAST_ID,
                    or_(
                        Event.season_id.is_(None),
                        ~Event.season_id.in_(NBA_SEASONS_TO_EXCLUDE)
                    )
                )
            ).distinct().order_by('event_date').all()
            
            # Extract date objects from results
            date_list = [row.event_date for row in dates]
            return date_list
            
    except Exception as e:
        logger.error(f"Error getting available dates: {e}")
        return []


def collect_results_for_events(events: List[Event], day_date: date, test_mode: bool = False, update_odds: bool = True, skip_until_event_id: Optional[int] = None) -> Tuple[Dict[str, int], List[Dict]]:
    """
    Collect results for a list of events (one day's worth).
    Reuses the proven logic from scheduler.job_results_collection()
    ALWAYS processes events, even if results already exist (updates with new extraction methods).
    
    Args:
        events: List of Event objects to process (all from same day)
        day_date: The date being processed (for logging)
        test_mode: If True, don't save to database, just return what would be saved
        update_odds: If True, also update final odds (like scheduler does)
        skip_until_event_id: If provided, skip events until we find this event_id (for resumption)
        
    Returns:
        Tuple of (statistics dict, list of changes that would be made)
        Statistics: {'updated': int, 'skipped': int, 'failed': int}
        Changes: List of dicts with event_id, action, and data
    """
    stats = {'updated': 0, 'skipped': 0, 'failed': 0}
    changes = []  # Track what would be added/updated
    total = len(events)
    
    # Handle skip_until_event_id - find the starting index
    start_idx = 0
    if skip_until_event_id:
        for i, event in enumerate(events):
            if event.id == skip_until_event_id:
                start_idx = i + 1  # Start from the NEXT event (this one was already processed)
                logger.info(f"⏩ Resuming from event {skip_until_event_id} - skipping first {start_idx} events")
                break
        else:
            # Event ID not found in list, process all
            logger.warning(f"⚠️  Resume event_id {skip_until_event_id} not found in today's events - processing all")
            start_idx = 0
    
    mode_text = "🔍 TEST MODE - " if test_mode else ""
    if start_idx > 0:
        logger.info(f"{mode_text}📅 Processing {total - start_idx} events (resuming) for date {day_date.strftime('%Y-%m-%d')}...")
    else:
        logger.info(f"{mode_text}📅 Processing {total} events for date {day_date.strftime('%Y-%m-%d')}...")
    
    # Import here to avoid circular imports
    from repository import OddsRepository, MarketRepository
    
    for idx, event in enumerate(events[start_idx:], start_idx + 1):
        try:
            # Log progress every 10 events or at start
            if (idx - start_idx) % 10 == 1 or idx == start_idx + 1:
                logger.info(f"  Progress: {idx}/{total} events ({stats['updated']} updated, {stats['skipped']} skipped, {stats['failed']} failed)")

            
            # Check if result already exists (for logging purposes only)
            existing_result = ResultRepository.get_result_by_event_id(event.id)
            is_update = existing_result is not None
            
            # Step 1: Update final odds (like scheduler.job_results_collection does)
            odds_updated = False
            if update_odds:
                try:
                    final_odds_response = api_client.get_event_final_odds(event.id, event.slug)
                    
                    # Check if we got rate limited (403)
                    if final_odds_response is None:
                        # Could be 404 (no odds) or 403 (rate limited)
                        # We need to check the last error - but for now, continue
                        # The real 403 check will happen on get_event_results
                        pass
                    
                    if final_odds_response:
                        final_odds_data = api_client.extract_final_odds_from_response(final_odds_response, initial_odds_extraction=True)
                        if final_odds_data:
                            if test_mode:
                                # In test mode, just track what would be updated (don't save)
                                changes.append({
                                    'event_id': event.id,
                                    'action': 'update_odds',
                                    'odds_data': {
                                        'one_final': final_odds_data.get('one_final'),
                                        'x_final': final_odds_data.get('x_final'),
                                        'two_final': final_odds_data.get('two_final')
                                    }
                                })
                                odds_updated = True
                            else:
                                upserted_id = OddsRepository.upsert_event_odds(event.id, final_odds_data)
                                if upserted_id:
                                    snapshot = OddsRepository.create_odds_snapshot(event.id, final_odds_data)
                                    odds_updated = True
                                    logger.debug(f"  ✅ Final odds updated for event {event.id}")
                                    
                                    # Save all markets to markets/market_choices tables (same as scheduler)
                                    # This runs for ALL sports using the existing odds response (no extra API call)
                                    try:
                                        MarketRepository.save_markets_from_response(event.id, final_odds_response)
                                    except Exception as market_error:
                                        logger.warning(f"  Error saving markets for event {event.id}: {market_error}")
                                else:
                                    logger.warning(f"  Failed to update final odds for event {event.id}")
                        else:
                            logger.debug(f"  No final odds data extracted for event {event.id}")
                    else:
                        logger.debug(f"  Failed to fetch final odds for event {event.id}")
                except Exception as odds_error:
                    logger.warning(f"  Error updating odds for event {event.id}: {odds_error}")
            
            # Step 2: Fetch and update results (ALWAYS process, even if exists)
            # In test mode, don't update event information to avoid database changes
            try:
                result_data = api_client.get_event_results(event.id, update_event_info=not test_mode)
            except SofaScoreRateLimitException as e:
                logger.error(f"\n\n❌ 403 Rate Limit Error detected on event {event.id}")
                logger.error(f"   Stopping processing to avoid further rate limiting")
                # Re-raise as our custom exception
                raise RateLimitDetected(f"403 error on event {event.id}")
            
            if not result_data:
                logger.warning(f"  No result data returned for event {event.id}")
                stats['failed'] += 1
                increment_failure_counter()
                
                # Check if we're being rate limited
                if is_rate_limited():
                    logger.error(f"\n\n❌ Rate limiting detected after multiple failures")
                    raise RateLimitDetected(f"Multiple consecutive failures on event {event.id}")
                if test_mode:
                    changes.append({
                        'event_id': event.id,
                        'action': 'failed',
                        'reason': 'No result data from API'
                    })
                continue
            
            # In test mode, track what would be saved/updated
            if test_mode:
                stats['updated'] += 1
                action = 'update' if is_update else 'create'
                change_info = {
                    'event_id': event.id,
                    'action': action,
                    'event_info': {
                        'home_team': event.home_team,
                        'away_team': event.away_team,
                        'sport': event.sport,
                        'competition': event.competition,
                        'start_time': event.start_time_utc.isoformat()
                    },
                    'result_data': {
                        'home_score': result_data.get('home_score'),
                        'away_score': result_data.get('away_score'),
                        'winner': result_data.get('winner'),
                        'home_sets': result_data.get('home_sets'),
                        'away_sets': result_data.get('away_sets')
                    }
                }
                if is_update:
                    change_info['existing_result'] = {
                        'home_score': existing_result.home_score,
                        'away_score': existing_result.away_score,
                        'winner': existing_result.winner
                    }
                changes.append(change_info)
                
                action_text = "UPDATE" if is_update else "CREATE"
                logger.info(f"  🔍 [TEST] Would {action_text} result for Event {event.id}: {event.home_team} vs {event.away_team} = {result_data['home_score']}-{result_data['away_score']}, Winner: {result_data['winner']}")
            else:
                # Store/update result in database (upsert always updates if exists)
                if ResultRepository.upsert_result(event.id, result_data):
                    stats['updated'] += 1
                    reset_failure_counter()  # Reset on success
                    action_text = "UPDATED" if is_update else "CREATED"
                    logger.info(f"  ✅ {action_text} result for Event {event.id}: {event.home_team} vs {event.away_team} = {result_data['home_score']}-{result_data['away_score']}, Winner: {result_data['winner']}")
                    
                    # Save JSON state after each successful event (for granular resumption)
                    save_json_state(day_date, event.id)
                    
                    # OPTIONAL: Process observations (FAIL-SAFE - doesn't break main flow)
                    try:
                        sport_observations_manager.process_event_observations(event, result_data)
                    except Exception as obs_error:
                        logger.warning(f"  Failed to process observations for event {event.id}: {obs_error}")
                else:
                    logger.warning(f"  Failed to store result for event {event.id}")
                    stats['failed'] += 1
        
        except RateLimitDetected:
            # Re-raise to stop the entire day processing
            raise
        except Exception as e:
            logger.error(f"  Error processing event {event.id}: {e}")
            stats['failed'] += 1
            if test_mode:
                changes.append({
                    'event_id': event.id,
                    'action': 'error',
                    'error': str(e)
                })
            continue
    
    mode_text = "🔍 TEST MODE - " if test_mode else ""
    logger.info(f"{mode_text}📅 Completed date {day_date.strftime('%Y-%m-%d')}: {stats['updated']} updated, {stats['skipped']} skipped, {stats['failed']} failed")
    return stats, changes


def print_test_summary(changes: List[Dict], day_date: date):
    """
    Print a summary of what would be added/updated in test mode.
    
    Args:
        changes: List of changes that would be made
        day_date: The date being tested
    """
    logger.info("\n" + "=" * 80)
    logger.info("🔍 TEST MODE SUMMARY - What would be saved/updated in database")
    logger.info("=" * 80)
    logger.info(f"Date: {day_date.strftime('%Y-%m-%d')}")
    logger.info(f"Total changes: {len(changes)}")
    
    # Group changes by action
    creates = [c for c in changes if c.get('action') == 'create']
    updates = [c for c in changes if c.get('action') == 'update']
    odds_updates = [c for c in changes if c.get('action') == 'update_odds']
    failures = [c for c in changes if c.get('action') in ['failed', 'error']]
    
    logger.info(f"\n📊 Breakdown:")
    logger.info(f"  ✅ Would CREATE: {len(creates)} new results")
    logger.info(f"  🔄 Would UPDATE: {len(updates)} existing results")
    logger.info(f"  📊 Would UPDATE ODDS: {len(odds_updates)} events")
    logger.info(f"  ❌ Would FAIL: {len(failures)}")
    
    if creates:
        logger.info(f"\n📝 NEW RESULTS TO BE CREATED ({len(creates)}):")
        for i, change in enumerate(creates[:10], 1):  # Show first 10
            event_info = change.get('event_info', {})
            result_data = change.get('result_data', {})
            logger.info(f"  {i}. Event {change['event_id']}: {event_info.get('home_team')} vs {event_info.get('away_team')}")
            logger.info(f"     Sport: {event_info.get('sport')}, Competition: {event_info.get('competition')}")
            logger.info(f"     Result: {result_data.get('home_score')}-{result_data.get('away_score')}, Winner: {result_data.get('winner')}")
            if result_data.get('home_sets'):
                logger.info(f"     Sets: {result_data.get('home_sets')} / {result_data.get('away_sets')}")
        if len(creates) > 10:
            logger.info(f"     ... and {len(creates) - 10} more results")
    
    if updates:
        logger.info(f"\n🔄 EXISTING RESULTS TO BE UPDATED ({len(updates)}):")
        for i, change in enumerate(updates[:10], 1):  # Show first 10
            event_info = change.get('event_info', {})
            result_data = change.get('result_data', {})
            existing = change.get('existing_result', {})
            logger.info(f"  {i}. Event {change['event_id']}: {event_info.get('home_team')} vs {event_info.get('away_team')}")
            if existing:
                logger.info(f"     Current: {existing.get('home_score')}-{existing.get('away_score')}, Winner: {existing.get('winner')}")
            logger.info(f"     New: {result_data.get('home_score')}-{result_data.get('away_score')}, Winner: {result_data.get('winner')}")
            if result_data.get('home_sets'):
                logger.info(f"     Sets: {result_data.get('home_sets')} / {result_data.get('away_sets')}")
        if len(updates) > 10:
            logger.info(f"     ... and {len(updates) - 10} more results")
    
    if odds_updates:
        logger.info(f"\n📊 ODDS TO BE UPDATED ({len(odds_updates)}):")
        for i, change in enumerate(odds_updates[:5], 1):  # Show first 5
            odds_data = change.get('odds_data', {})
            logger.info(f"  {i}. Event {change['event_id']}: 1={odds_data.get('one_final')}, X={odds_data.get('x_final')}, 2={odds_data.get('two_final')}")
        if len(odds_updates) > 5:
            logger.info(f"     ... and {len(odds_updates) - 5} more")
    
    if failures:
        logger.info(f"\n❌ FAILURES ({len(failures)}):")
        for i, change in enumerate(failures[:5], 1):  # Show first 5
            logger.info(f"  {i}. Event {change['event_id']}: {change.get('reason', change.get('error', 'Unknown error'))}")
        if len(failures) > 5:
            logger.info(f"     ... and {len(failures) - 5} more")
    
    logger.info("=" * 80)


def main():
    """Main entry point for historical results extraction"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Extract historical results for events (excluding NBA seasons)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python extract_historical_results.py                    # Process all remaining days
  python extract_historical_results.py --test             # Test mode: show what would be processed
  python extract_historical_results.py --days 5           # Process only 5 days
  python extract_historical_results.py --test --days 1    # Test mode: process first day (dry-run)
  python extract_historical_results.py --init-state        # Initialize state file with 2025-10-23
  python extract_historical_results.py --manual           # Manual mode: extract from MANUAL_START_DATE to MANUAL_END_DATE
        """
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode: show what would be processed without saving to database'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=None,
        help='Number of days to process (default: all remaining days)'
    )
    parser.add_argument(
        '--init-state',
        action='store_true',
        help='Initialize state file with hardcoded date (2025-10-23). Creates file in script directory.'
    )
    parser.add_argument(
        '--manual',
        action='store_true',
        help='Manual mode: extract from MANUAL_START_DATETIME to MANUAL_END_DATETIME (ignores state file and --days). Supports time specification.'
    )
    
    args = parser.parse_args()
    test_mode = args.test
    max_days = args.days
    manual_mode = args.manual
    
    # Handle --init-state command
    if args.init_state:
        logger.info("=" * 80)
        logger.info("INITIALIZING STATE FILE")
        logger.info("=" * 80)
        if init_state_file(force=True):
            logger.info("✅ State file initialized successfully!")
            logger.info("   You can now run the script normally to resume from 2025-10-23")
        else:
            logger.error("❌ Failed to initialize state file")
            sys.exit(1)
        sys.exit(0)
    
    logger.info("=" * 80)
    logger.info("HISTORICAL RESULTS EXTRACTION SCRIPT")
    if test_mode:
        logger.info("🔍 TEST MODE - No changes will be saved to database")
    if manual_mode:
        logger.info("🔧 MANUAL MODE - Using date range from constants")
    logger.info("=" * 80)
    logger.info(f"Minimum event ID: {LAST_ID}")
    logger.info(f"Excluding NBA season IDs: {NBA_SEASONS_TO_EXCLUDE}")
    logger.info("Processing events in daily batches to avoid API rate limiting")
    logger.info("⚠️  ALWAYS updates existing results (upsert) to refresh with new extraction methods")
    if manual_mode:
        logger.info(f"📅 Manual mode datetime range: {MANUAL_START_DATETIME.strftime('%Y-%m-%d %H:%M:%S')} to {MANUAL_END_DATETIME.strftime('%Y-%m-%d %H:%M:%S')}")
    elif max_days:
        logger.info(f"⚠️  Limiting to {max_days} day(s)")
    logger.info("=" * 80)
    
    try:
        # Step 1: Test database connection
        if not db_manager.test_connection():
            logger.error("Database connection failed. Exiting.")
            sys.exit(1)
        
        logger.info("✅ Database connection successful")
        
        # Handle manual mode separately (different flow)
        if manual_mode:
            logger.info("🔧 MANUAL MODE: Using datetime range from constants")
            logger.info(f"   Start datetime: {MANUAL_START_DATETIME.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"   End datetime: {MANUAL_END_DATETIME.strftime('%Y-%m-%d %H:%M:%S')} (inclusive)")
            logger.info("   ⚠️  Ignoring state file and --days parameter")
            
            # Get events directly from datetime range
            logger.info("🔍 Getting events in datetime range...")
            manual_events = get_events_for_datetime_range(MANUAL_START_DATETIME, MANUAL_END_DATETIME)
            
            if not manual_events:
                logger.warning(f"⚠️  No events found between {MANUAL_START_DATETIME.strftime('%Y-%m-%d %H:%M:%S')} and {MANUAL_END_DATETIME.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info("Exiting.")
                sys.exit(0)
            
            logger.info(f"📊 Found {len(manual_events)} events in datetime range")
            logger.info("=" * 80 + "\n")
            
            # Process all events from the datetime range
            total_stats = {'updated': 0, 'skipped': 0, 'failed': 0}
            all_changes = []
            
            # Group events by date for better logging and processing
            from collections import defaultdict
            events_by_date = defaultdict(list)
            for event in manual_events:
                event_date = event.start_time_utc.date()
                events_by_date[event_date].append(event)
            
            dates_list = sorted(events_by_date.keys())
            logger.info(f"📅 Processing {len(manual_events)} events across {len(dates_list)} date(s)")
            if len(dates_list) > 1:
                logger.info(f"   Date range: {dates_list[0].strftime('%Y-%m-%d')} to {dates_list[-1].strftime('%Y-%m-%d')}")
            logger.info("=" * 80 + "\n")
            
            # Process events grouped by date
            for day_idx, day_date in enumerate(dates_list, 1):
                day_events = events_by_date[day_date]
                logger.info(f"\n{'=' * 80}")
                mode_text = "🔍 TEST MODE - " if test_mode else ""
                logger.info(f"{mode_text}📅 Processing Date {day_idx}/{len(dates_list)}: {day_date.strftime('%Y-%m-%d')} ({len(day_events)} events)")
                logger.info(f"{'=' * 80}")
                
                # Process events for this day
                day_stats, day_changes = collect_results_for_events(day_events, day_date, test_mode=test_mode, update_odds=True)
                
                # Accumulate statistics
                total_stats['updated'] += day_stats['updated']
                total_stats['skipped'] += day_stats['skipped']
                total_stats['failed'] += day_stats['failed']
                all_changes.extend(day_changes)
                
                # Collect changes for test mode summary
                if test_mode and day_idx == 1:
                    print_test_summary(day_changes, day_date)
                    if len(dates_list) > 1:
                        logger.info(f"\n⚠️  TEST MODE: Only processed first day. {len(dates_list) - 1} more day(s) would be processed in normal mode.")
                    break
                
                # Small delay between days to be extra safe with API
                if day_idx < len(dates_list):
                    logger.info(f"  ⏸️  Waiting 2 seconds before next day...")
                    import time
                    time.sleep(2)
            
            # Print final summary
            logger.info("\n" + "=" * 80)
            logger.info("FINAL SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Total events processed: {len(manual_events)}")
            logger.info(f"✅ Updated: {total_stats['updated']}")
            logger.info(f"⏭️  Skipped: {total_stats['skipped']}")
            logger.info(f"❌ Failed: {total_stats['failed']}")
            logger.info("=" * 80)
            
            sys.exit(0)
        
        # Normal mode: continue with date-based processing
        # Step 2: Load last processed date (if exists and not in test mode)
        last_processed_date = None
        if not test_mode:
            last_processed_date = load_last_processed_date()
            if last_processed_date:
                logger.info(f"📂 Resuming from last processed date: {last_processed_date.strftime('%Y-%m-%d')}")
            else:
                logger.info("📂 Starting fresh (no previous state found)")
        else:
            last_processed_date = load_last_processed_date()
            if last_processed_date:
                logger.info(f"📂 Last processed date: {last_processed_date.strftime('%Y-%m-%d')} (will show what would be processed next)")
        
        # Step 3: Get all available dates
        logger.info("🔍 Getting all available dates with events...")
        all_dates = get_all_available_dates()
        
        if not all_dates:
            logger.info("No events found to process. Exiting.")
            sys.exit(0)
        
        logger.info(f"📊 Found {len(all_dates)} unique dates with events")
        
        # Step 4: Filter dates to process
        if last_processed_date and not test_mode:
            # Find the index of the last processed date
            try:
                last_index = all_dates.index(last_processed_date)
                dates_to_process = all_dates[last_index + 1:]  # Start from next date
            except ValueError:
                # Last processed date not in list, process all dates
                logger.warning(f"Last processed date {last_processed_date} not found in available dates. Processing all dates.")
                dates_to_process = all_dates
        elif last_processed_date and test_mode:
            # In test mode, show what would be processed next
            try:
                last_index = all_dates.index(last_processed_date)
                dates_to_process = all_dates[last_index + 1:]
            except ValueError:
                dates_to_process = all_dates
        else:
            dates_to_process = all_dates
        
        # Load JSON state for mid-day resumption (check for in-progress event)
        json_state = load_json_state()
        resume_event_id = None
        resume_date = None
        
        if json_state and not test_mode:
            resume_date_str = json_state.get('last_date')
            resume_event_id = json_state.get('last_event_id')
            
            if resume_date_str and resume_event_id:
                try:
                    resume_date = datetime.strptime(resume_date_str, '%Y-%m-%d').date()
                    # Check if this date is in our processing list
                    if resume_date in dates_to_process:
                        logger.info(f"📂 Found in-progress date {resume_date_str} with last event_id {resume_event_id}")
                        # Ensure we start from the resume_date, not after it
                        resume_date_idx = dates_to_process.index(resume_date)
                        dates_to_process = dates_to_process[resume_date_idx:]
                    else:
                        logger.info(f"📂 Resume date {resume_date_str} already completed, starting from next date")
                        resume_event_id = None  # Don't skip events on new days
                except ValueError:
                    logger.warning(f"Could not parse resume date: {resume_date_str}")
                    resume_event_id = None
        
        if not dates_to_process:
            logger.info("✅ All dates have been processed. Nothing to do.")
            sys.exit(0)
        
        # Apply day limit if specified (only if not in manual mode)
        if not manual_mode and max_days and max_days > 0:
            dates_to_process = dates_to_process[:max_days]
        
        # Show what will be processed
        if test_mode:
            logger.info(f"🔍 TEST MODE: Would process {len(dates_to_process)} date(s) starting from {dates_to_process[0].strftime('%Y-%m-%d')}")
            if len(dates_to_process) > 1:
                logger.info(f"   Dates: {dates_to_process[0].strftime('%Y-%m-%d')} to {dates_to_process[-1].strftime('%Y-%m-%d')}")
        else:
            logger.info(f"📅 Processing {len(dates_to_process)} date(s) starting from {dates_to_process[0].strftime('%Y-%m-%d')}")
        logger.info("=" * 80 + "\n")
        
        # Step 5: Process each date sequentially
        total_stats = {'updated': 0, 'skipped': 0, 'failed': 0}
        total_events_processed = 0
        all_changes = []  # Track all changes in test mode
        
        for day_idx, day_date in enumerate(dates_to_process, 1):
            try:
                logger.info(f"\n{'=' * 80}")
                mode_text = "🔍 TEST MODE - " if test_mode else ""
                logger.info(f"{mode_text}📅 Processing Date {day_idx}/{len(dates_to_process)}: {day_date.strftime('%Y-%m-%d')}")
                logger.info(f"{'=' * 80}")
                
                # Get events for this day
                day_events = get_events_for_date(day_date)
                
                if not day_events:
                    logger.info(f"  ⏭️  No events found for {day_date.strftime('%Y-%m-%d')}, skipping...")
                    # Still save this date as processed (even if no events) - but not in test mode or manual mode
                    if not test_mode and not manual_mode:
                        save_last_processed_date(day_date)
                    continue
                
                logger.info(f"  Found {len(day_events)} events for this date")
                
                # Determine if we should skip events on this day (mid-day resumption)
                # Only skip on the FIRST day we process if we have a resume_event_id
                skip_event_id = None
                if day_idx == 1 and resume_event_id and resume_date and day_date == resume_date:
                    skip_event_id = resume_event_id
                
                # Process events for this day (always update, even if results exist)
                day_stats, day_changes = collect_results_for_events(
                    day_events, day_date, 
                    test_mode=test_mode, 
                    update_odds=True,
                    skip_until_event_id=skip_event_id
                )
                
                # Accumulate statistics
                total_stats['updated'] += day_stats['updated']
                total_stats['skipped'] += day_stats['skipped']
                total_stats['failed'] += day_stats['failed']
                total_events_processed += len(day_events)
                
                # Collect changes for test mode summary
                if test_mode:
                    all_changes.extend(day_changes)
                    # In test mode, only process first day and show summary
                    if day_idx == 1:
                        print_test_summary(day_changes, day_date)
                        if len(dates_to_process) > 1:
                            logger.info(f"\n⚠️  TEST MODE: Only processed first day. {len(dates_to_process) - 1} more day(s) would be processed in normal mode.")
                        break
                
                # Save progress after each day (not in test mode or manual mode)
                if not test_mode and not manual_mode:
                    save_last_processed_date(day_date)
                    # Clear event_id from JSON state since day is complete
                    clear_json_state_event_id()
                    logger.info(f"  💾 Progress saved: {day_date.strftime('%Y-%m-%d')} completed")
                    
                    # Small delay between days to be extra safe with API
                    if day_idx < len(dates_to_process):
                        logger.info(f"  ⏸️  Waiting 2 seconds before next day...")
                        import time
                        time.sleep(2)
                
            except KeyboardInterrupt:
                logger.warning(f"\n\n⚠️  Script interrupted by user (Ctrl+C) after processing {day_idx} day(s)")
                if not test_mode:
                    # Load latest state to show user
                    latest_state = load_json_state()
                    last_event = latest_state.get('last_event_id', 'N/A')
                    logger.info(f"💾 Last processed date: {day_date.strftime('%Y-%m-%d')}")
                    logger.info(f"💾 Last processed event_id: {last_event}")
                    logger.info("   You can resume by running the script again (will continue from last event).")
                sys.exit(130)
            except RateLimitDetected as e:
                logger.error(f"\n\n❌ 403 RATE LIMIT ERROR - Stopping execution")
                logger.error(f"   Error: {e}")
                if not test_mode:
                    # Load latest state to show user
                    latest_state = load_json_state()
                    last_event = latest_state.get('last_event_id', 'N/A')
                    logger.info(f"💾 Last processed date: {day_date.strftime('%Y-%m-%d')}")
                    logger.info(f"💾 Last processed event_id: {last_event}")
                    logger.info("   Progress has been saved. You can resume by running the script again.")
                    logger.info("   The script will continue from the last successfully processed event.")
                sys.exit(1)
            except Exception as e:
                logger.error(f"  ❌ Error processing date {day_date.strftime('%Y-%m-%d')}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                # Continue to next date instead of exiting
                logger.info(f"  ⏭️  Continuing to next date...")
                continue
        
        # Step 6: Print final statistics
        logger.info("\n" + "=" * 80)
        if test_mode:
            logger.info("🔍 TEST MODE COMPLETED - No changes were saved to database")
        else:
            logger.info("EXTRACTION COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Total dates processed: {len(dates_to_process) if not test_mode else 1}")
        logger.info(f"Total events processed: {total_events_processed}")
        logger.info(f"✅ Results updated: {total_stats['updated']}")
        logger.info(f"⏭️  Results skipped (already exist): {total_stats['skipped']}")
        logger.info(f"❌ Results failed: {total_stats['failed']}")
        logger.info("=" * 80)
        
        if not test_mode and dates_to_process:
            logger.info(f"💾 Last processed date: {dates_to_process[-1].strftime('%Y-%m-%d')}")
        
        # Exit with appropriate code
        if total_stats['failed'] > 0:
            logger.warning(f"Completed with {total_stats['failed']} failures")
            sys.exit(1)
        else:
            if test_mode:
                logger.info("✅ Test completed successfully! Run without --test to actually save results.")
            else:
                logger.info("✅ Completed successfully!")
            sys.exit(0)
        
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  Script interrupted by user (Ctrl+C). Exiting...")
        if not test_mode:
            logger.info("💾 Progress has been saved. You can resume by running the script again.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()

