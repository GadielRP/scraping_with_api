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

CLI Arguments:
    --test    : Test mode - shows what would be processed without saving to database
                Only processes first day and displays detailed summary
                Shows CREATE vs UPDATE actions for results
    --days N  : Limit processing to N days (default: all remaining days)

Note: This script ALWAYS processes events, even if results exist, to ensure
      data is updated with the latest extraction methods and corrections.

Requirements:
    - Database connection configured in config.py
    - All dependencies installed (see requirements.txt)
"""

import logging
import sys
import json
import os
import argparse
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
from sqlalchemy import and_, or_, func, cast, Date

# Import existing project modules
from database import db_manager
from models import Event, Result
from repository import ResultRepository
from sofascore_api import api_client
from sport_observations import sport_observations_manager

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
]

# Minimum event ID
LAST_ID = 269

# State file to track last processed date
STATE_FILE = 'extract_historical_results_state.json'


def load_last_processed_date() -> Optional[date]:
    """
    Load the last processed date from state file.
    
    Returns:
        date object if state file exists, None otherwise
    """
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                state = json.load(f)
                last_date_str = state.get('last_processed_date')
                if last_date_str:
                    return datetime.strptime(last_date_str, '%Y-%m-%d').date()
        return None
    except Exception as e:
        logger.warning(f"Error loading state file: {e}")
        return None


def save_last_processed_date(processed_date: date):
    """
    Save the last processed date to state file.
    
    Args:
        processed_date: The date that was just processed
    """
    try:
        state = {
            'last_processed_date': processed_date.strftime('%Y-%m-%d'),
            'updated_at': datetime.now().isoformat()
        }
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)
        logger.info(f"💾 Saved state: Last processed date = {processed_date.strftime('%Y-%m-%d')}")
    except Exception as e:
        logger.error(f"Error saving state file: {e}")


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


def collect_results_for_events(events: List[Event], day_date: date, test_mode: bool = False, update_odds: bool = True) -> Tuple[Dict[str, int], List[Dict]]:
    """
    Collect results for a list of events (one day's worth).
    Reuses the proven logic from scheduler.job_results_collection()
    ALWAYS processes events, even if results already exist (updates with new extraction methods).
    
    Args:
        events: List of Event objects to process (all from same day)
        day_date: The date being processed (for logging)
        test_mode: If True, don't save to database, just return what would be saved
        update_odds: If True, also update final odds (like scheduler does)
        
    Returns:
        Tuple of (statistics dict, list of changes that would be made)
        Statistics: {'updated': int, 'skipped': int, 'failed': int}
        Changes: List of dicts with event_id, action, and data
    """
    stats = {'updated': 0, 'skipped': 0, 'failed': 0}
    changes = []  # Track what would be added/updated
    total = len(events)
    
    mode_text = "🔍 TEST MODE - " if test_mode else ""
    logger.info(f"{mode_text}📅 Processing {total} events for date {day_date.strftime('%Y-%m-%d')}...")
    
    # Import here to avoid circular imports
    from repository import OddsRepository
    
    for idx, event in enumerate(events, 1):
        try:
            # Log progress every 10 events or at start
            if idx % 10 == 0 or idx == 1:
                logger.info(f"  Progress: {idx}/{total} events ({stats['updated']} updated, {stats['skipped']} skipped, {stats['failed']} failed)")
            
            # Check if result already exists (for logging purposes only)
            existing_result = ResultRepository.get_result_by_event_id(event.id)
            is_update = existing_result is not None
            
            # Step 1: Update final odds (like scheduler.job_results_collection does)
            odds_updated = False
            if update_odds:
                try:
                    final_odds_response = api_client.get_event_final_odds(event.id, event.slug)
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
            result_data = api_client.get_event_results(event.id, update_event_info=not test_mode)
            if not result_data:
                logger.warning(f"  No result data returned for event {event.id}")
                stats['failed'] += 1
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
                    action_text = "UPDATED" if is_update else "CREATED"
                    logger.info(f"  ✅ {action_text} result for Event {event.id}: {event.home_team} vs {event.away_team} = {result_data['home_score']}-{result_data['away_score']}, Winner: {result_data['winner']}")
                    
                    # OPTIONAL: Process observations (FAIL-SAFE - doesn't break main flow)
                    try:
                        sport_observations_manager.process_event_observations(event, result_data)
                    except Exception as obs_error:
                        logger.warning(f"  Failed to process observations for event {event.id}: {obs_error}")
                else:
                    logger.warning(f"  Failed to store result for event {event.id}")
                    stats['failed'] += 1
        
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
    
    args = parser.parse_args()
    test_mode = args.test
    max_days = args.days
    
    logger.info("=" * 80)
    logger.info("HISTORICAL RESULTS EXTRACTION SCRIPT")
    if test_mode:
        logger.info("🔍 TEST MODE - No changes will be saved to database")
    logger.info("=" * 80)
    logger.info(f"Minimum event ID: {LAST_ID}")
    logger.info(f"Excluding NBA season IDs: {NBA_SEASONS_TO_EXCLUDE}")
    logger.info("Processing events in daily batches to avoid API rate limiting")
    logger.info("⚠️  ALWAYS updates existing results (upsert) to refresh with new extraction methods")
    if max_days:
        logger.info(f"⚠️  Limiting to {max_days} day(s)")
    logger.info("=" * 80)
    
    try:
        # Step 1: Test database connection
        if not db_manager.test_connection():
            logger.error("Database connection failed. Exiting.")
            sys.exit(1)
        
        logger.info("✅ Database connection successful")
        
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
        
        # Step 4: Filter dates to process (skip already processed dates)
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
        
        if not dates_to_process:
            logger.info("✅ All dates have been processed. Nothing to do.")
            sys.exit(0)
        
        # Apply day limit if specified
        if max_days and max_days > 0:
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
                    # Still save this date as processed (even if no events) - but not in test mode
                    if not test_mode:
                        save_last_processed_date(day_date)
                    continue
                
                logger.info(f"  Found {len(day_events)} events for this date")
                
                # Process events for this day (always update, even if results exist)
                day_stats, day_changes = collect_results_for_events(day_events, day_date, test_mode=test_mode, update_odds=True)
                
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
                
                # Save progress after each day (not in test mode)
                if not test_mode:
                    save_last_processed_date(day_date)
                    logger.info(f"  💾 Progress saved: {day_date.strftime('%Y-%m-%d')} completed")
                    
                    # Small delay between days to be extra safe with API
                    if day_idx < len(dates_to_process):
                        logger.info(f"  ⏸️  Waiting 2 seconds before next day...")
                        import time
                        time.sleep(2)
                
            except KeyboardInterrupt:
                logger.warning(f"\n\n⚠️  Script interrupted by user (Ctrl+C) after processing {day_idx} day(s)")
                if not test_mode:
                    logger.info(f"💾 Last processed date saved: {day_date.strftime('%Y-%m-%d')}")
                    logger.info("   You can resume by running the script again.")
                sys.exit(130)
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

