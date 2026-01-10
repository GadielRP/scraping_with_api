"""
Backfill script to collect results and odds for events missing results.

This script processes events from a start date up until yesterday (the day before execution).
It handles API errors gracefully:
- 404 errors: Event is added to deletion batch (event no longer exists)
- 403 errors: Execution stops and progress is saved to resume later
- Other errors: Event is skipped but preserved

Usage:
    python backfill_yesterday_results.py
    python backfill_yesterday_results.py --limit 10  # Process only 10 events
"""

import logging
import json
import time
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

from database import db_manager
from models import Event, Result
from repository import ResultRepository, EventRepository, OddsRepository
from sofascore_api import api_client, SofaScoreNotFoundException, SofaScoreRateLimitException

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Start date for processing events (inclusive)
# Events from this date onwards (up to yesterday) will be processed
START_DATE = datetime(2025, 12, 1)  # January 1, 2026

# Minimum event ID to process (skip older events)
MIN_EVENT_ID = 269

# Progress file to track processed events for resume capability
PROGRESS_FILE = Path("backfill_progress.json")

# Rate limiting between API calls
DELAY_BETWEEN_EVENTS = 0.5  # seconds


# ============================================================================
# PROGRESS TRACKING
# ============================================================================

def load_progress() -> dict:
    """Load progress from file if exists."""
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load progress file: {e}")
    return {
        "last_processed_event_id": None,
        "processed_count": 0,
        "deleted_count": 0,
        "failed_count": 0,
        "last_run": None,
        "stopped_on_403": False
    }


def save_progress(progress: dict):
    """Save progress to file."""
    try:
        progress["last_run"] = datetime.now().isoformat()
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress, f, indent=2)
        logger.info(f"Progress saved to {PROGRESS_FILE}")
    except Exception as e:
        logger.error(f"Could not save progress: {e}")


def clear_progress():
    """Clear progress file after successful completion."""
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
        logger.info("Progress file cleared")


# ============================================================================
# API WRAPPER FUNCTIONS WITH ERROR TYPE DETECTION
# ============================================================================

def fetch_event_results_with_error_handling(event_id: int) -> Tuple[Optional[dict], str]:
    """
    Fetch event results with detailed error type detection.
    
    Returns:
        Tuple of (result_data, error_type)
        - error_type: "success", "404", "403", "error", "not_finished"
    """
    try:
        # The _make_request method returns None for 404, but we need to detect it
        # For now, we check the response - if None, we do a direct check
        endpoint = f"/event/{event_id}"
        
        # Make request directly to detect status code
        from sofascore_api import api_client
        url = f"{api_client.base_url}{endpoint}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        
        api_client._rate_limit()
        response = api_client.session.get(url, headers=headers, timeout=30)
        
        if response.status_code == 404:
            logger.warning(f"Event {event_id}: 404 Not Found - marking for deletion")
            return None, "404"
        
        if response.status_code == 403:
            logger.error(f"Event {event_id}: 403 Forbidden - rate limited")
            return None, "403"
        
        if response.status_code != 200:
            logger.warning(f"Event {event_id}: HTTP {response.status_code}")
            return None, "error"
        
        # Parse response and extract results
        data = response.json()
        result_data = api_client.extract_results_from_response(data)
        
        if result_data:
            # Check if it's a canceled marker
            if isinstance(result_data, dict) and result_data.get('_canceled'):
                return result_data, "canceled"
            return result_data, "success"
        else:
            return None, "not_finished"
            
    except Exception as e:
        logger.error(f"Event {event_id}: Exception - {e}")
        return None, "error"


def fetch_event_odds_with_error_handling(event_id: int, slug: str = None) -> Tuple[Optional[dict], str]:
    """
    Fetch event odds with detailed error type detection.
    
    Returns:
        Tuple of (odds_data, error_type)
        - error_type: "success", "404", "403", "error", "no_odds"
    """
    try:
        # Build endpoint for odds
        if slug:
            endpoint = f"/event/{slug}/odds/1/all"
        else:
            endpoint = f"/event/{event_id}/odds/1/all"
        
        from sofascore_api import api_client
        url = f"{api_client.base_url}{endpoint}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        
        api_client._rate_limit()
        response = api_client.session.get(url, headers=headers, timeout=30)
        
        if response.status_code == 404:
            logger.debug(f"Event {event_id}: Odds 404 - no odds available")
            return None, "404"
        
        if response.status_code == 403:
            logger.error(f"Event {event_id}: Odds 403 - rate limited")
            return None, "403"
        
        if response.status_code != 200:
            logger.warning(f"Event {event_id}: Odds HTTP {response.status_code}")
            return None, "error"
        
        # Parse response and extract odds
        data = response.json()
        odds_data = api_client.extract_final_odds_from_response(data, initial_odds_extraction=True)
        
        if odds_data:
            return odds_data, "success"
        else:
            return None, "no_odds"
            
    except Exception as e:
        logger.error(f"Event {event_id}: Odds exception - {e}")
        return None, "error"


# ============================================================================
# MAIN BACKFILL LOGIC
# ============================================================================

def get_events_to_process(start_date: datetime, end_date: datetime, resume_from_id: int = None) -> List:
    """
    Get events that need results backfilled.
    
    Criteria:
    - id > MIN_EVENT_ID
    - start_time_utc >= start_date
    - start_time_utc < end_date (end of yesterday)
    - No result exists
    
    Events are sorted by start_time_utc for chronological processing.
    """
    with db_manager.get_session() as session:
        query = session.query(Event).outerjoin(Result).filter(
            Event.id > MIN_EVENT_ID,
            Event.start_time_utc >= start_date,
            Event.start_time_utc < end_date,
            Result.event_id == None
        )
        
        # Resume from last processed event if applicable
        if resume_from_id:
            query = query.filter(Event.id > resume_from_id)
        
        # Sort by start_time_utc for chronological processing
        events = query.order_by(Event.start_time_utc.asc(), Event.id.asc()).all()
        
        logger.info(f"Found {len(events)} events needing results between {start_date.date()} and {end_date.date()}")
        return events



def process_event(event, stats: dict, events_to_delete: List[int]) -> str:
    """
    Process a single event: fetch results and odds.
    
    Returns:
        "success", "404", "403", "skipped"
    """
    event_id = event.id
    
    print(f"  [{stats['processed'] + 1}] Event {event_id}: {event.home_team} vs {event.away_team}")
    
    # Step 1: Fetch results
    result_data, result_status = fetch_event_results_with_error_handling(event_id)
    
    if result_status == "403":
        return "403"
    
    if result_status == "404":
        events_to_delete.append(event_id)
        return "404"
    
    if result_status == "not_finished":
        print(f"      ⏭️ Not finished yet")
        return "skipped"
    
    if result_status == "canceled":
        print(f"      🗑️ Event canceled/postponed ({result_data.get('status_description')}) - marking for deletion")
        events_to_delete.append(event_id)
        return "404" # Count as deleted for stats
    
    if result_status == "error":
        print(f"      ⚠️ Error fetching results")
        return "skipped"
    
    # Step 2: Save results
    if result_data:
        saved = ResultRepository.upsert_result(event_id, result_data)
        if saved:
            print(f"      ✅ Result: {result_data['home_score']}-{result_data['away_score']}, Winner: {result_data['winner']}")
        else:
            print(f"      ⚠️ Failed to save result")
            return "skipped"
    
    # Step 3: Fetch and save odds (optional - don't fail the whole event if odds fail)
    odds_data, odds_status = fetch_event_odds_with_error_handling(event_id, event.slug)
    
    if odds_status == "403":
        # Still count as 403 to stop execution
        return "403"
    
    if odds_status == "404":
        # Odds 404 doesn't mean delete the event - could just be no odds available
        print(f"      ℹ️ No odds available (404)")
    elif odds_status == "success" and odds_data:
        # Save odds
        OddsRepository.upsert_event_odds(event_id, odds_data)
        OddsRepository.create_odds_snapshot(event_id, odds_data)
        print(f"      ✅ Odds saved")
    
    return "success"


def backfill_results(limit: int = None):
    """
    Main backfill function.
    
    Args:
        limit: Maximum number of events to process (None = all)
    """
    from timezone_utils import get_local_now
    
    print(f"\n{'='*60}")
    print("BACKFILL: Results & Odds")
    print(f"{'='*60}\n")
    
    # Calculate date range using timezone-aware local time
    local_now = get_local_now()
    
    # Yesterday = today minus 1 day, end of day (23:59:59)
    yesterday_start = (local_now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = (local_now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # End date is end of yesterday (inclusive)
    end_date = yesterday_end
    
    print(f"Configuration:")
    print(f"  Start date: {START_DATE.date()}")
    print(f"  End date (limit): {end_date.date()} {end_date.strftime('%H:%M:%S')}")
    print(f"  Min event ID: {MIN_EVENT_ID}")
    print(f"  Limit: {limit if limit else 'None (process all)'}")
    
    # Load progress
    progress = load_progress()

    if progress.get("last_processed_event_id"):
        print(f"  Resuming from event ID: {progress['last_processed_event_id']}")
    
    print()
    
    # Get events to process
    events = get_events_to_process(
        START_DATE, 
        end_date,
        resume_from_id=progress.get("last_processed_event_id")
    )
    
    if not events:
        print("✅ No events to process!")
        clear_progress()
        return
    
    if limit:
        events = events[:limit]
        print(f"Processing first {limit} events...\n")
    
    # Process events
    stats = {
        'processed': progress.get('processed_count', 0),
        'success': 0,
        'deleted': progress.get('deleted_count', 0),
        'skipped': progress.get('failed_count', 0),
    }
    events_to_delete = []
    stopped_on_403 = False
    last_processed_id = progress.get("last_processed_event_id")
    
    print(f"Processing {len(events)} events...\n")
    
    for event in events:
        try:
            result = process_event(event, stats, events_to_delete)
            
            if result == "403":
                # Stop and save progress
                print(f"\n❌ 403 Error encountered - stopping execution")
                stopped_on_403 = True
                break
            
            if result == "success":
                stats['success'] += 1
            elif result == "404":
                stats['deleted'] += 1
            else:  # skipped
                stats['skipped'] += 1
            
            stats['processed'] += 1
            last_processed_id = event.id
            
            # Rate limiting
            time.sleep(DELAY_BETWEEN_EVENTS)
            
        except KeyboardInterrupt:
            print(f"\n\n🛑 Execution interrupted by user (Ctrl+C)")
            stopped_on_403 = True # Trigger progress saving
            break
        except Exception as e:
            logger.error(f"Unexpected error processing event {event.id}: {e}")
            stats['skipped'] += 1
            continue
    
    # Batch delete 404 events
    if events_to_delete:
        print(f"\n🗑️ Batch deleting {len(events_to_delete)} events that returned 404...")
        deleted_count = EventRepository.batch_delete_events(events_to_delete)
        print(f"   Deleted {deleted_count} events")
    
    # Save or clear progress
    if stopped_on_403:
        save_progress({
            "last_processed_event_id": last_processed_id,
            "processed_count": stats['processed'],
            "deleted_count": stats['deleted'],
            "failed_count": stats['skipped'],
            "stopped_on_403": True
        })
        print(f"\n⚠️ Progress saved. Run again to resume from event {last_processed_id}")
    else:
        clear_progress()
    
    # Print summary
    print(f"\n{'='*60}")
    print("BACKFILL COMPLETE")
    print(f"{'='*60}")
    print(f"  Processed: {stats['processed']}")
    print(f"  Success (results saved): {stats['success']}")
    print(f"  Deleted (404/Canceled): {stats['deleted']}")
    print(f"  Skipped (other): {stats['skipped']}")
    if stopped_on_403:
        print(f"  ⚠️ Process stopped - progress saved")
    print(f"{'='*60}\n")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("BACKFILL SCRIPT: Results & Odds")
    print("=" * 60)
    
    # Test database connection
    try:
        db_manager.test_connection()
        print("✅ Database connection OK\n")
    except Exception as e:
        print(f"❌ Database connection FAILED: {e}")
        sys.exit(1)
    
    # Parse command line arguments
    limit = None
    if len(sys.argv) > 1:
        if sys.argv[1] == "--limit" and len(sys.argv) > 2:
            limit = int(sys.argv[2])
        else:
            try:
                limit = int(sys.argv[1])
            except ValueError:
                pass
    
    backfill_results(limit=limit)
