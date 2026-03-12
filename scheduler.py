import schedule
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import json
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import Config
from sofascore_api import api_client
import sofascore_api2  # Import to attach new methods to api_client
from repository import EventRepository, OddsRepository, ResultRepository, ObservationRepository
from odds_utils import process_event_odds_from_dropping_odds
from alert_system import pre_start_notifier
import os
from sport_observations import sport_observations_manager
logger = logging.getLogger(__name__)
from alert_engine import alert_engine
from timezone_utils import get_local_now_aware, convert_utc_to_local
from models import refresh_materialized_views
from database import db_manager
from prediction_engine import prediction_engine
from database import db_manager
from models import PredictionLog, refresh_materialized_views
from today_sport_extractor import run_daily_discovery
# Import set prediction system for in-game alerts
from set_prediction_system import set_prediction_system
# Import optimization utilities

from optimization import (
    parallel_team_event_fetching,
    process_with_batch_cleanup,
    process_with_parallel_db_ops,
    process_events_only,
    filter_upcoming_events
)



class JobScheduler:
    """Main job scheduler for the SofaScore odds system"""
    
    def __init__(self):
        self.running = False
        self.thread = None
        self.event_repo = EventRepository()
        self.odds_repo = OddsRepository()
        self.result_repo = ResultRepository()
        
        # Track recently processed rescheduled events to prevent infinite loops
        self.recently_rescheduled = set()  # Set of event_ids processed in last 10 minutes
        self.last_cleanup_time = time.time()  # Track when we last cleaned the set
        
        # Setup jobs
        self._setup_jobs()
    
    def _setup_jobs(self):
        """Setup all scheduled jobs"""
        # Job A - Discovery (at configurable clock times from Config.DISCOVERY_TIMES)
        for time_str in Config.DISCOVERY_TIMES:
            schedule.every().day.at(time_str).do(self.job_discovery)
        
        # Job B - Discovery 2 (at hh:02 to avoid blocking pre-start checks at hh:00)
        # Uses Config.DISCOVERY2_TIMES which runs at 2 minutes past the hour
        for time_str in Config.DISCOVERY2_TIMES:
            schedule.every().day.at(time_str).do(self.job_discovery2)
        
        # Job C - Pre-start check (dynamic interval based on Config.POLL_INTERVAL_MINUTES)
        self._setup_pre_start_jobs()
        
        # Job D - Midnight results collection (at 04:00)
        schedule.every().day.at("04:00").do(self.job_midnight_sync)
        
        # Job E - Daily discovery (at 05:01) - fetches today's scheduled events with odds
        schedule.every().day.at("05:01").do(self.job_daily_discovery)
        
        logger.info("Jobs scheduled:")
        logger.info(f"  - Discovery: daily at {', '.join(Config.DISCOVERY_TIMES)}")
        logger.info(f"  - Discovery 2 (streaks, h2h, winning odds): daily at {', '.join(Config.DISCOVERY2_TIMES)}")
        logger.info(f"  - Pre-start check: every {Config.POLL_INTERVAL_MINUTES} minutes (includes tennis timestamp checks + NBA 4th quarter checks)")
        logger.info("  - Midnight sync: daily at 04:00 (results collection only)")
        logger.info("  - Daily discovery: daily at 05:01 (today's scheduled events with odds)")
    
    def _setup_pre_start_jobs(self):
        """Setup pre-start check jobs every N minutes at exact minute marks (configurable via POLL_INTERVAL_MINUTES)"""
        interval_minutes = Config.POLL_INTERVAL_MINUTES
        
        # Schedule jobs at exact minute marks based on interval
        # For 5 minutes: 00, 05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55
        # For 1 minute: 00, 01, 02, 03, ..., 59 (all 60 minutes)
        for minute in range(0, 60, interval_minutes):
            schedule.every().hour.at(f":{minute:02d}").do(self.job_pre_start_check)
        
        logger.info(f"  - Pre-start check scheduled every {interval_minutes} minutes at exact minute marks (upcoming events + tennis/NBA in-game checks)")
    
    def _cleanup_recently_rescheduled(self):
        """Clean up old entries from recently_rescheduled set to prevent memory leaks"""
        current_time = time.time()
        if current_time - self.last_cleanup_time > 600:  # Clean up every 10 minutes
            self.recently_rescheduled.clear()
            self.last_cleanup_time = current_time
            logger.debug("Cleaned up recently_rescheduled tracking set")

    def start(self):
        """Start the scheduler in a separate thread"""
        if self.running:
            logger.warning("Scheduler is already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()
        logger.info("Job scheduler started")
        
        # Run immediate scan for any games starting very soon
        logger.info("Running immediate pre-start check for any games starting soon...")
        self.job_pre_start_check()
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("Job scheduler stopped")
    
    def _run_scheduler(self):
        """Main scheduler loop"""
        logger.info("Scheduler loop started - monitoring for pending jobs...")
        last_check = time.time()
        
        while self.running:
            try:
                current_time = time.time()
                
                # Check for pending jobs
                pending_jobs = schedule.run_pending()
                
                # Log when jobs are executed (for debugging)
                if pending_jobs:
                    logger.info(f"Executed {len(pending_jobs)} pending jobs")
                    for job in pending_jobs:
                        logger.info(f"  - Executed: {job.job_func.__name__}")
                
                # Log scheduler status every 30 seconds for debugging
                if current_time - last_check >= 30:
                    logger.debug(f"Scheduler heartbeat - {len(schedule.jobs)} jobs scheduled, next run in {schedule.idle_seconds()} seconds")
                    last_check = current_time
                
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                time.sleep(5)

    def job_discovery2(self):
        """Job B: Discover new events from streaks, team streaks, h2h and winning odds events"""
        logger.info("Starting Job B: Event Discovery from streaks, team streaks, h2h and winning odds events")
        try:
            
            # Get high value streaks events - special handling due to different response structure
            high_value_streaks_events_response = api_client.get_high_value_streaks_events()
            if not high_value_streaks_events_response:
                logger.error("Failed to get high value streaks events")
                return
            # Extract events from nested structure (general + head2head arrays)
            extracted_events, extracted_events_high_value_streaks_h2h = api_client.extract_events_from_high_value_streaks(high_value_streaks_events_response)
            if not extracted_events:
                logger.warning("No events found in high value streaks events")
                return
            # Construct a response dict that extract_events_and_odds_from_dropping_response can process
            normalized_response = {"events": extracted_events}
            normalized_response_h2h = {"events": extracted_events_high_value_streaks_h2h}
            high_value_streaks_events, _ = api_client.extract_events_and_odds_from_dropping_response(normalized_response, odds_extraction=False, discovery_source='high_value_streaks')
            high_value_streaks_events_h2h, _ = api_client.extract_events_and_odds_from_dropping_response(normalized_response_h2h, odds_extraction=False, discovery_source='high_value_streaks_h2h')
            
            # Filter high-value streaks events to only include upcoming events (at least 10 min away)
            high_value_streaks_events = filter_upcoming_events(high_value_streaks_events)
            high_value_streaks_events_h2h = filter_upcoming_events(high_value_streaks_events_h2h)
            
            if not high_value_streaks_events:
                logger.warning("No events found after processing high value streaks events")
                return
            if not high_value_streaks_events_h2h:
                logger.warning("No events found after processing high value streaks events h2h")
                return
            # Team streaks events - special handling due to different response structure
            team_streaks_response = api_client.get_team_streaks_events()
            if not team_streaks_response:
                logger.error("Failed to get team streaks events")
                return

            # Extract team IDs from team streaks response
            team_ids = api_client.get_team_ids_from_team_streaks(team_streaks_response)
            if not team_ids:
                logger.warning("No team IDs found in team streaks response")
                return

            logger.info(f"Found {len(team_ids)} teams in team streaks response")

            # Use optimized parallel fetching from optimization module (10 workers for speed)
            team_streaks_events = parallel_team_event_fetching(team_ids, max_workers=10)

            if not team_streaks_events:
                logger.warning("No events found after processing team streaks")
                logger.info("Skipping team streaks processing (no valid events fetched)")
            else:
                logger.info(f"Successfully fetched {len(team_streaks_events)} team streak events")
                
                # Use optimized batch cleanup processing from optimization module (10 workers)
                processed_count, skipped_count = process_with_batch_cleanup(
                    team_streaks_events,
                    discovery_source='team_streaks',
                    max_workers=10
                )
                logger.info(f"team streaks events completed: processed {processed_count}/{len(team_streaks_events)} events, skipped {skipped_count} events")

            # H2H events
            h2h_events_response = api_client.get_h2h_events()
            if not h2h_events_response:
                logger.error("Failed to get h2h events")
                return

            h2h_events, _ = api_client.extract_events_and_odds_from_dropping_response(h2h_events_response, odds_extraction=False, discovery_source='top_h2h')
            
            # Filter H2H events to only include upcoming events (at least 10 min away)
            h2h_events = filter_upcoming_events(h2h_events)
            
            if not h2h_events:
                logger.warning("No events found in h2h events")
                return

            # Winning odds events
            winning_odds_events_response = api_client.get_winning_odds_events()
            if not winning_odds_events_response:
                logger.error("Failed to get winning odds events")
                return

            winning_odds_events, winning_odds_events_odds_map = api_client.extract_events_and_odds_from_dropping_response(winning_odds_events_response, odds_extraction=True, discovery_source='winning_odds')
            
            # Filter winning odds events to only include upcoming events (at least 10 min away)
            winning_odds_events = filter_upcoming_events(winning_odds_events)
            
            if not winning_odds_events:
                logger.warning("No events found in winning odds events")
                return

            # Process each event lists with optimized methods (AGGRESSIVE: 10 workers)
            
            # High value streaks events - Use event-only processing (no odds fetching)
            processed_count, skipped_count = process_events_only(
                high_value_streaks_events,
                discovery_source='high_value_streaks',
                max_workers=10
            )
            logger.info(f"high value streaks events completed: processed {processed_count}/{len(high_value_streaks_events)} events, skipped {skipped_count} events")

            # High value streaks events h2h - Use event-only processing (no odds fetching)
            processed_count, skipped_count = process_events_only(
                high_value_streaks_events_h2h,
                discovery_source='high_value_streaks_h2h',
                max_workers=10
            )
            logger.info(f"high value streaks events h2h completed: processed {processed_count}/{len(high_value_streaks_events_h2h)} events, skipped {skipped_count} events")

            # Team streaks events already processed above with batch cleanup (10 workers)

            # H2H events - Use event-only processing (no odds fetching)
            processed_count, skipped_count = process_events_only(
                h2h_events,
                discovery_source='h2h',
                max_workers=10
            )
            logger.info(f"h2h events completed: processed {processed_count}/{len(h2h_events)} events, skipped {skipped_count} events")

            # Winning odds events - Use parallel DB ops with 10 workers (odds pre-fetched)
            processed_count, skipped_count = process_with_parallel_db_ops(
                winning_odds_events,
                winning_odds_events_odds_map,
                discovery_source='winning_odds',
                max_workers=10
            )
            logger.info(f"winning odds events completed: processed {processed_count}/{len(winning_odds_events)} events, skipped {skipped_count} events")
        except Exception as e:
            logger.error(f"Error in Job B: {e}")

    def job_discovery(self):
        """Job A: Discover new events from dropping odds AND process their odds data in one go"""
        logger.info("Starting Job A: Event Discovery with Odds Processing")
        
        # Define sports to fetch individually (after processing /dropping/all)
        dropping_sports = ["football", "basketball", "volleyball", "american-football", "ice-hockey", "darts", "baseball", "rugby"]
        
        # Track processed event IDs to avoid duplicates
        processed_event_ids = set()
        
        total_processed = 0
        total_skipped = 0
        
        try:
            # Step 1: Fetch and process /dropping/all endpoint first
            logger.info("Step 1: Fetching /dropping/all endpoint")
            response_all = api_client.get_dropping_odds_with_odds_and_events_response()
            if not response_all:
                logger.error("Failed to get dropping odds with odds data from /dropping/all")
            else:
                # Save API response to JSON file for debugging
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                json_filename = os.path.join("debug", f"debug_discovery_all_{timestamp}.json")
                try:
                    os.makedirs("debug", exist_ok=True)  # ensure folder exists
                    with open(json_filename, 'w', encoding='utf-8') as f:
                        json.dump(response_all, f, indent=2, ensure_ascii=False)
                    logger.debug(f"API response saved to {json_filename}")
                except Exception as e:
                    logger.warning(f"Failed to save JSON debug file: {e}")
                
                # Extract events and odds data with discovery_source
                events_all, odds_map_all = api_client.extract_events_and_odds_from_dropping_response(
                    response_all, 
                    odds_extraction=True, 
                    discovery_source='dropping_odds'
                )
                
                if events_all:
                    logger.info(f"Found {len(events_all)} events in /dropping/all endpoint")
                    
                    # Filter to only upcoming events (at least 10 min away)
                    events_all = filter_upcoming_events(events_all)
                    
                    if not events_all:
                        logger.info("No upcoming events found in /dropping/all after filtering")
                    else:
                        # Process events from /dropping/all
                        processed_count, skipped_count = process_with_parallel_db_ops(
                            events_all,
                            odds_map_all,
                            discovery_source='dropping_odds',
                            max_workers=10
                        )
                        total_processed += processed_count
                        total_skipped += skipped_count
                        
                        # Track processed event IDs
                        for event in events_all:
                            processed_event_ids.add(event['id'])
                        
                        logger.info(f"/dropping/all completed: processed {processed_count}/{len(events_all)} events, skipped {skipped_count} events")
                else:
                    logger.warning("No events found in /dropping/all endpoint")

            
            # Step 2: Fetch and process each sport individually, skipping already processed events
            logger.info(f"Step 2: Fetching and processing {len(dropping_sports)} individual sports")
            
            for sport in dropping_sports:
                try:
                    logger.info(f"Fetching dropping odds for sport: {sport}")
                    response_sport = api_client.get_dropping_odds_with_odds_and_events_response(sport=sport)
                    
                    if not response_sport:
                        logger.warning(f"No response for sport {sport}, skipping")
                        continue
                    
                    # Extract events and odds data
                    events_sport, odds_map_sport = api_client.extract_events_and_odds_from_dropping_response(
                        response_sport,
                        odds_extraction=True,
                        discovery_source='dropping_odds'
                    )
                    
                    if not events_sport:
                        logger.info(f"No events found for sport {sport}")
                        continue
                    
                    # Filter to only upcoming events (at least 10 min away)
                    events_sport = filter_upcoming_events(events_sport)
                    
                    if not events_sport:
                        logger.info(f"No upcoming events for sport {sport} after filtering")
                        continue
                    
                    # Filter out events that were already processed from /dropping/all
                    new_events = [e for e in events_sport if e['id'] not in processed_event_ids]
                    skipped_duplicates = len(events_sport) - len(new_events)
                    
                    if skipped_duplicates > 0:
                        logger.info(f"Sport {sport}: Skipping {skipped_duplicates} duplicate events already processed from /dropping/all")
                    
                    if not new_events:
                        logger.info(f"Sport {sport}: All events already processed, skipping")
                        continue
                    
                    # Filter odds_map to only include new events
                    new_odds_map = {
                        str(event_id): odds_data 
                        for event_id, odds_data in odds_map_sport.items() 
                        if int(event_id) in [e['id'] for e in new_events]
                    }
                    
                    logger.info(f"Sport {sport}: Processing {len(new_events)} new events (skipped {skipped_duplicates} duplicates)")
                    
                    # Process new events
                    processed_count, skipped_count = process_with_parallel_db_ops(
                        new_events,
                        new_odds_map,
                        discovery_source='dropping_odds',
                        max_workers=10
                    )
                    
                    total_processed += processed_count
                    total_skipped += skipped_count
                    
                    # Track processed event IDs
                    for event in new_events:
                        processed_event_ids.add(event['id'])
                    
                    logger.info(f"Sport {sport} completed: processed {processed_count}/{len(new_events)} events, skipped {skipped_count} events")
                    
                except Exception as e:
                    logger.error(f"Error processing sport {sport}: {e}")
                    continue
            
            logger.info(f"Job A completed: Total processed {total_processed} events, total skipped {total_skipped} events")
            logger.info(f"Total unique events processed: {len(processed_event_ids)}")
            
        except Exception as e:
            logger.error(f"Error in Job A: {e}")
    
    def job_pre_start_check(self):
        """
        Job C: Pre-start check for events starting within 30 minutes + in-game checks
        
        SMART ODDS EXTRA EXTRACTION: Only extracts odds at key moments (30 min and 0 min before start)
        to avoid unnecessary API calls when odds don't change significantly.
        
        SMART NOTIFICATIONS: Only sends Telegram notifications when odds are extracted at key moments
        (30 min and 0 min), but includes ALL upcoming games in those notifications to avoid missing games.
        
        TIMESTAMP CORRECTION: Also checks events that started within the last 60 minutes for late
        timestamp corrections (Tennis: 60 min window, Other sports: 15 min window).
        
        IN-GAME CHECKS: Monitors ongoing games for key moments:
        """
        logger.info("🚨 PRE-START CHECK EXECUTED at " + datetime.now().strftime("%H:%M:%S"))
        
        try:
            current_time = datetime.now()
            
            # Get tracked season IDs from OddsPortal config
            from oddsportal_config import SEASON_ODDSPORTAL_MAP
            from config import Config as AppConfig
            
            tracked_season_ids = None
            if AppConfig.TRACKED_SEASONS_ONLY:
                tracked_season_ids = list(SEASON_ODDSPORTAL_MAP.keys())
                logger.info(f"Pre-start check restricted to {len(tracked_season_ids)} tracked seasons (TRACKED_SEASONS_ONLY=True)")
            else:
                logger.info("Pre-start check processing ALL seasons (TRACKED_SEASONS_ONLY=False)")
        
            # 1. FETCH UPCOMING EVENTS FIRST (to lock in timing) - Filtered by season_id if enabled
            upcoming_events = self.event_repo.get_events_starting_soon_with_odds(Config.PRE_START_WINDOW_MINUTES, season_ids=tracked_season_ids)
            logger.info(f"Found {len(upcoming_events)} events starting within {Config.PRE_START_WINDOW_MINUTES} minutes (snapshot taken before timestamp checks)")
            
            # 1.1 PRE-CALCULATE TIMING DECISIONS (Critical Step!)
            # We calculate 'minutes_until_start' NOW, before any delays.
            # We store this in a dictionary/list to be used later.
            pre_calculated_timings = {}
            for event in upcoming_events:
                minutes = self._minutes_until_start(event['start_time_utc'])
                pre_calculated_timings[event['id']] = minutes
                
            # 2. CHECK RECENTLY STARTED EVENTS (Timestamp Correction) - Filtered by season_id if enabled
            # This can take time (e.g., 20-30s)...
            events_started_recently = self.event_repo.get_events_started_recently(window_minutes=60, season_ids=tracked_season_ids)
            logger.info(f"Found {len(events_started_recently)} events that started recently (checking for late timestamp corrections)")
            
            # Get set of event IDs that were modified during timestamp correction
            modified_event_ids = self._check_recently_started_events_for_timestamp_corrections(events_started_recently)
            
            # 3. FILTER UPCOMING EVENTS
            if modified_event_ids:
                original_count = len(upcoming_events)
                upcoming_events = [e for e in upcoming_events if e['id'] not in modified_event_ids]
                filtered_count = len(upcoming_events)
                if original_count != filtered_count:
                    logger.info(f"ℹ️ Filtered out {original_count - filtered_count} upcoming events that were just rescheduled/modified")
                    
            logger.info(f"After checking modified events, {len(upcoming_events)} events remain")
            
            # Filter out recently rescheduled events
            before_rescheduled = len(upcoming_events)
            upcoming_events = [e for e in upcoming_events if e['id'] not in self.recently_rescheduled]
            if before_rescheduled != len(upcoming_events):
                logger.info(f"Filtered {before_rescheduled - len(upcoming_events)} recently rescheduled. {len(upcoming_events)} remain")

            events_to_process = []
            event_meta_lookup = {}

            # 4. PROCESS UPCOMING EVENTS
            if not upcoming_events:
                logger.info(f"No upcoming events to process (after filtering)")
            else:
                # Check for NBA 4th quarter
                set_prediction_system.check_nba_4th_quarter()
                
                # Use the "smart" odds extraction logic
                KEY_MOMENTS = [120, 30, 5, 0]
                
                for event_data in upcoming_events:
                    # RETRIEVE PRE-CALCULATED TIMING
                    if event_data['id'] in pre_calculated_timings:
                        minutes_until_start = pre_calculated_timings[event_data['id']]
                        logger.debug(f"Using pre-calculated timing for {event_data['slug']}: {minutes_until_start} mins")
                    else:
                        # Fallback (shouldn't happen for original events)
                        minutes_until_start = self._minutes_until_start(event_data['start_time_utc'])
                    
                    logger.info(f"🚨 UPCOMING GAME ALERT: {event_data['home_team']} vs {event_data['away_team']} starts in {minutes_until_start} minutes")
                    
                    should_extract_odds, metadata_snapshot = self._should_extract_odds_for_event(event_data['id'], minutes_until_start)
                    
                    # ALWAYS refresh from DB after potential timestamp correction in _should_extract_odds_for_event
                    # This ensures event_info uses the corrected time, avoiding safety-check mismatches later.
                    refreshed_event = self.event_repo.get_event_by_id(event_data['id'])
                    if refreshed_event:
                        event_data['season_id'] = refreshed_event.season_id
                        event_data['start_time_utc'] = refreshed_event.start_time_utc
                        logger.debug(f"✅ Refreshed metadata for event {event_data['id']} after timing check")
                    
                    event_info = {
                        'event_id': event_data['id'],
                        'event_data': event_data,
                        'minutes_until_start': minutes_until_start,
                        'should_extract_odds': should_extract_odds,
                        'original_start_time': event_data['start_time_utc'],
                        'metadata_snapshot': metadata_snapshot,  # Cached from timing check API call
                    }
                    events_to_process.append(event_info)
                    event_meta_lookup[event_data['id']] = event_info
            
            # ========================================
            # PARTITION AND START ODDSPORTAL (EARLY)
            # ========================================
            # Extract OddsPortal candidates early and start the thread so it runs in background
            # while the main thread sequentially processes the API odds for all events.
            op_candidates = []
            non_op_candidates = []
            
            for event_info in events_to_process:
                season_id = event_info['event_data'].get('season_id')
                minutes_until_start = event_info.get('minutes_until_start')
                if season_id and season_id in SEASON_ODDSPORTAL_MAP and minutes_until_start == 0:
                    op_candidates.append(event_info)
                else:
                    non_op_candidates.append(event_info)
            
            # Use threading.Event to coordinate OP scraping with alert delivery
            op_done_events = {e['event_id']: threading.Event() for e in op_candidates}
            op_event_ids = set(op_done_events.keys())
            
            if op_candidates:
                # Guard: wait for previous OP cycle if still running
                if hasattr(self, '_active_op_thread') and self._active_op_thread.is_alive():
                    timeout = AppConfig.ODDSPORTAL_PREVIOUS_CYCLE_TIMEOUT
                    logger.warning(f"⏳ Previous OP worker still running — waiting up to {timeout}s for it to finish...")
                    self._active_op_thread.join(timeout=timeout)
                    if self._active_op_thread.is_alive():
                        logger.warning(f"⚠️ Previous OP worker did NOT finish after {timeout}s — proceeding with new cycle anyway")
                    else:
                        logger.info("✅ Previous OP worker finished within timeout — proceeding with new cycle")
                        
                logger.info(f"🚀 Launching OddsPortal scraper in background for {len(op_candidates)} tracked-league events...")
                
                # Launch the OP worker: ONLY scrapes + saves to DB, then signals completion.
                # Main thread handles ALL alert evaluation (including for OP events).
                oddsportal_thread = threading.Thread(
                    target=self._oddsportal_worker_wrapper,
                    args=(op_candidates, op_done_events),
                    name="oddsportal_worker",
                    daemon=False
                )
                oddsportal_thread.start()
                self._active_op_thread = oddsportal_thread
            else:
                op_done_events = {}  # No OP work needed, empty dict
                if events_to_process:
                    logger.info(f"⏭️ No OddsPortal-tracked events among {len(events_to_process)} events")
                    
            # STEP 2.2: EXECUTE ALL REGULAR ODDS EXTRACTIONS (with pre-computed decisions)
            processed_count = 0
            odds_extracted_count = 0
            # Track events with odds extracted to prevent timing drift issues
            events_with_odds_extracted = []
            
            for event_info in events_to_process:
                try:
                    event_data = event_info['event_data']
                    minutes_until_start = event_info['minutes_until_start']
                    should_extract_odds = event_info['should_extract_odds']
                    
                    # Initialize observations per event to avoid cross-event contamination
                    observations = None
                    
                    # SMART ODDS EXTRACTION: Only extract odds at key moments (30 min and 0 min)                  
                    if should_extract_odds:
                        logger.info(f"🎯 EXTRACTING ODDS: {event_data['home_team']} vs {event_data['away_team']} - {minutes_until_start} min until start")
                        
                        # Fetch final odds for this upcoming game using specific event endpoint
                        final_odds_response = api_client.get_event_final_odds(event_data['id'], event_data['slug'])
                        
                        if final_odds_response:
                            # Store odds response for later alert sending (grouped with dual/h2h alerts per event)
                            event_info['final_odds_response'] = final_odds_response
                            
                            # Process the final odds data
                            final_odds_data = api_client.extract_final_odds_from_response(final_odds_response, initial_odds_extraction=True)
                                                     
                            if final_odds_data:
                                # Update the event odds with final odds
                                upserted_id = OddsRepository.upsert_event_odds(event_data['id'], final_odds_data)
                                if upserted_id:
                                    logger.info(f"✅ Final odds updated for {event_data['home_team']} vs {event_data['away_team']}")
                                    
                                    # Create final odds snapshot
                                    snapshot = OddsRepository.create_odds_snapshot(event_data['id'], final_odds_data)
                                    if snapshot:
                                        logger.info(f"✅ Final odds snapshot created for event {event_data['id']}")
                                        odds_extracted_count += 1
                                        
                                        # Save all markets to new markets/market_choices tables
                                        # This runs for ALL sports (including excluded ones)
                                        try:
                                            from repository import MarketRepository
                                            MarketRepository.save_markets_from_response(event_data['id'], final_odds_response)
                                        except Exception as e:
                                            logger.warning(f"Error saving markets to DB for event {event_data['id']}: {e}")
                                            
                                        # OddsPortal Integration has been moved to a dedicated worker
                                        # (runs concurrently, see _run_oddsportal_batch below)
                                        
                                        # Track this event for alert evaluation (capture timing when odds were extracted)
                                        events_with_odds_extracted.append({
                                            'event_id': event_data['id'],
                                            'start_time': event_data['start_time_utc'],
                                            'initial_minutes': minutes_until_start
                                        })

                                else:
                                    logger.warning(f"Failed to update final odds for event {event_data['id']}")
                            else:
                                logger.warning(f"No final odds data extracted for event {event_data['id']}")
                        else:
                            logger.warning(f"Failed to fetch final odds for event {event_data['id']}")
                        
                        # COURT TYPE EXTRACTION: For Tennis/Tennis Doubles events at key moments
                        if event_data['sport'] in ['Tennis', 'Tennis Doubles']:
                            logger.info(f"🎾 Tennis event detected at key moment - extracting court type for event {event_data['id']}")
                            
                            # Check if event already has observations before making API call
                            
                            if not sport_observations_manager.has_observations_for_event(event_data['id']):
                                # Try to use metadata snapshot from timing check first (avoids redundant API call)
                                snapshot = event_info.get('metadata_snapshot')
                                if snapshot and snapshot.get('observations'):
                                    observations = snapshot['observations']
                                    # Store ground_type in sport_observations_manager if present
                                    for obs in observations:
                                        if obs.get('type') == 'ground_type' and obs.get('value'):
                                            ObservationRepository.upsert_observation(event_data['id'], event_data['sport'], 'ground_type', obs['value'])
                                    event_info['observations'] = observations
                                    logger.info(f"✅ Observations from metadata snapshot for event {event_data['id']} (no extra API call)")
                                else:
                                    # Fallback: No snapshot available (e.g., timestamp correction disabled), make API call
                                    observations = api_client.get_event_results(
                                        event_id=event_data['id'],
                                        update_court_type=True
                                    )
                                    
                                    # Store observations in event_info so they persist to the alert evaluation phase
                                    if observations:
                                        event_info['observations'] = observations
                                        logger.info(f"✅ Observations captured for event {event_data['id']} via API fallback")
                            else:
                                logger.info(f"🎾 Event {event_data['id']} already has observations - skipping API call")
                            
                    else:
                        logger.debug(f"⏭️ SKIPPING ODDS EXTRACTION: {event_data['home_team']} vs {event_data['away_team']} - {minutes_until_start} min until start (not a key moment)")
                    
                    processed_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing upcoming event {event_data['id']}: {e}")
                    continue
            
            if processed_count > 0:
                logger.info(f"🚨 Pre-start check completed: {processed_count} games starting soon!")
                if odds_extracted_count > 0:
                    logger.info(f"🎯 Odds extracted for {odds_extracted_count} games (smart extraction active)")
                else:
                    logger.info(f"⏭️ No odds extracted (smart extraction: only at 30min and 0min)")
            else:
                logger.info("Pre-start check completed: No games starting soon")
            
            # Get ALL events at key moments for alert evaluation (both OP and non-OP)
            # Main thread now handles ALL alert evaluation, with OP wait gate for odds alerts
            all_key_moment_event_ids = {
                info['event_id']
                for info in events_to_process
                if info['minutes_until_start'] in KEY_MOMENTS
            }
            
            should_evaluate_alerts = bool(all_key_moment_event_ids)
            if should_evaluate_alerts:
                try:
                    logger.info(f"🔍 Evaluating {len(all_key_moment_event_ids)} events at key moments for alerts (main thread)...")
                    
                    # Refresh materialized views to ensure latest data for alert evaluation
                    logger.info("🔄 Refreshing alert materialized views...")
                    
                    refresh_materialized_views(db_manager.engine)
                    logger.info("✅ Alert materialized views refreshed")
                    
                    
                    # Get Event objects with properly loaded event_odds for alert evaluation
                    # Use tracked events with odds extracted to prevent timing drift issues
                    events_for_alerts = []
                    tracked_event_ids = set()
                    
                    if events_with_odds_extracted:
                        # Use tracked events (events where odds were successfully extracted)
                        logger.info(f"🔍 Using tracked events for alert evaluation ({len(events_with_odds_extracted)} events with odds extracted)")
                        
                        for tracked_event in events_with_odds_extracted:
                            event_id = tracked_event['event_id']
                            if event_id not in all_key_moment_event_ids:
                                continue

                            # Get the Event object to ensure we have fresh DB state
                            event_obj = self.event_repo.get_event_by_id(event_id)
                            if not event_obj:
                                logger.warning(f"Could not find event {event_id} for alert evaluation")
                                continue
                            
                            # Filter out excluded sports from alert evaluation
                            if event_obj.sport in Config.EXCLUDED_SPORTS:
                                logger.info(f"⏭️ SKIPPING ALERT EVALUATION: Event {event_id} ({event_obj.home_team} vs {event_obj.away_team}) is {event_obj.sport} (excluded)")
                                continue
                            
                            # CRITICAL: Check if event was rescheduled after odds extraction
                            if event_obj.start_time_utc != tracked_event['start_time']:
                                logger.warning(f"⏭️ Event {event_id} was rescheduled after odds extraction - skipping alert evaluation")
                                continue
                            
                            # Skip if already processed as rescheduled in this cycle
                            if event_obj.id in self.recently_rescheduled:
                                logger.debug(f"⏭️ Skipping event {event_obj.id} - already rescheduled in this cycle")
                                continue
                            
                            # Use the minutes from when odds were extracted (prevents timing drift)
                            initial_minutes_snapshot = tracked_event.get('initial_minutes')
                            if initial_minutes_snapshot is None:
                                meta = event_meta_lookup.get(event_obj.id)
                                if meta:
                                    initial_minutes_snapshot = meta.get('minutes_until_start')
                            if initial_minutes_snapshot is None:
                                initial_minutes_snapshot = self._minutes_until_start(event_obj.start_time_utc)

                            # Retrieve observations from meta lookup
                            meta_obs = event_meta_lookup.get(event_obj.id)
                            stored_observations = meta_obs.get('observations') if meta_obs else None
                            stored_odds_response = meta_obs.get('final_odds_response') if meta_obs else None
                            stored_metadata_snapshot = meta_obs.get('metadata_snapshot') if meta_obs else None

                            events_for_alerts.append({
                                'event_obj': event_obj,
                                'initial_minutes': initial_minutes_snapshot,
                                'observations': stored_observations,
                                'odds_response': stored_odds_response,
                                'metadata_snapshot': stored_metadata_snapshot,
                                'season_id': getattr(event_obj, 'season_id', None)
                            })
                            tracked_event_ids.add(event_obj.id)
                    else:
                        # Fallback: Use original logic if no events with odds extracted
                        logger.info("🔍 No tracked events found - using fallback logic for alert evaluation")
                        
                        remaining_target_ids = all_key_moment_event_ids - tracked_event_ids

                        for event_data in upcoming_events:
                            if event_data['id'] not in remaining_target_ids:
                                continue

                            # Get the Event object first to ensure we have fresh DB state
                            event_obj = self.event_repo.get_event_by_id(event_data['id'])
                            if not event_obj:
                                continue
                            
                            # Filter out excluded sports from alert evaluation (fallback logic)
                            if event_obj.sport in Config.EXCLUDED_SPORTS:
                                logger.info(f"⏭️ SKIPPING ALERT EVALUATION (fallback): Event {event_obj.id} ({event_obj.home_team} vs {event_obj.away_team}) is {event_obj.sport} (excluded)")
                                continue
                            
                            # Skip if rescheduled in this cycle
                            if event_obj.id in self.recently_rescheduled:
                                logger.debug(f"⏭️ Skipping event {event_obj.id} - already rescheduled in this cycle")
                                continue

                            meta = event_meta_lookup.get(event_data['id'])
                            original_start = meta.get('original_start_time') if meta else None
                            if original_start and event_obj.start_time_utc != original_start:
                                logger.warning(f"⏭️ Event {event_obj.id} start time changed during processing - skipping alert evaluation")
                                continue
                            
                            initial_minutes_snapshot = meta.get('minutes_until_start') if meta else None
                            if initial_minutes_snapshot is None:
                                initial_minutes_snapshot = self._minutes_until_start(event_obj.start_time_utc)

                            # Retrieve observations from meta
                            stored_observations = meta.get('observations') if meta else None
                            stored_odds_response = meta.get('final_odds_response') if meta else None
                            stored_metadata_snapshot = meta.get('metadata_snapshot') if meta else None

                            events_for_alerts.append({
                                'event_obj': event_obj,
                                'initial_minutes': initial_minutes_snapshot,
                                'observations': stored_observations,
                                'odds_response': stored_odds_response,
                                'metadata_snapshot': stored_metadata_snapshot,
                                'season_id': getattr(event_obj, 'season_id', None)
                            })
                            tracked_event_ids.add(event_obj.id)
                    
                    if events_for_alerts:
                        # FILTER: Only process alerts for dropping_odds OR tracked seasons
                        filtered_events = []
                        for payload in events_for_alerts:
                            event_obj = payload['event_obj']
                            is_dropping = event_obj.discovery_source == 'dropping_odds'
                            is_tracked = event_obj.season_id in SEASON_ODDSPORTAL_MAP
                            
                            if is_dropping or is_tracked:
                                filtered_events.append(payload)
                            else:
                                logger.info(f"⏭️ SKIPPING ALERT: Event {event_obj.id} ({event_obj.home_team} vs {event_obj.away_team}) "
                                            f"is neither dropping_odds nor a tracked season (Source: {event_obj.discovery_source}, Season: {event_obj.season_id})")
                        
                        events_for_alerts = filtered_events

                    if events_for_alerts:
                        logger.info(f"🔍 Dispatching {len(events_for_alerts)} filtered events to alert evaluation (H2H ∥ OP scraping)...")
                        self._evaluate_and_send_alerts_batch(
                            events_for_alerts, KEY_MOMENTS,
                            op_done_events=op_done_events,
                            op_event_ids=op_event_ids
                        )
                    else:
                        logger.debug("No events at key moments found for alert evaluation")
                        
                except Exception as e:
                    logger.error(f"Error running alert evaluation: {e}")
            else:
                logger.debug("No events captured at key moments for alert evaluation")
            
        except Exception as e:
            logger.error(f"Error in Job C: {e}")
    
    def _process_single_event_alerts(self, event_payload: Dict, KEY_MOMENTS: list) -> Dict:
        """
        Process a single event through the full alert pipeline:
        1) Odds alert (sent first)
        2) H2H streak analysis
        3) Dual process evaluation
        4) Send H2H streak + dual process alerts
        
        Returns a dict with keys: event_id, streak_analysis, dual_report, odds_response, success
        """
        from streak_alerts import streak_alert_engine
        from modules.prediction import prediction_logger
        
        result = {'event_id': None, 'streak_analysis': None, 'dual_report': None, 'odds_response': None, 'success': False}
        
        try:
            observations = event_payload.get('observations')
            event_obj = event_payload['event_obj']
            result['event_id'] = event_obj.id
            initial_minutes = event_payload.get('initial_minutes')
            if initial_minutes is None:
                initial_minutes = self._minutes_until_start(event_obj.start_time_utc)
            
            minutes_now = self._minutes_until_start(event_obj.start_time_utc)
            minutes_until_start = initial_minutes if initial_minutes in KEY_MOMENTS else minutes_now

            logger.info(
                f"🔍 Processing event {event_obj.id}: {event_obj.home_team} vs {event_obj.away_team} "
                f"(captured {initial_minutes} min, current {minutes_now} min)"
            )
            
            # ========================================
            # ODDS RESPONSE (collected for ordered sending later)
            # ========================================
            odds_response = event_payload.get('odds_response')
            if odds_response:
                logger.info(f"📦 Odds response collected for event {event_obj.id} (will be sent in ordered loop)")
                result['odds_response'] = odds_response
            else:
                logger.debug(f"⏭️ No odds response stored for event {event_obj.id}")
            
            # ========================================
            # H2H STREAK ANALYSIS FOR THIS EVENT
            # ========================================
            streak_analysis = None

            if minutes_until_start == 30:
                if event_obj.custom_id:
                    try:
                        h2h_response = api_client.get_h2h_events_for_event(event_obj.custom_id)
                        if h2h_response and 'events' in h2h_response:
                            h2h_events = h2h_response['events']

                            home_team_id = None
                            away_team_id = None
                            competition_slug = None
                            competition_name = None
                            tournament_id = None
                            season_id = None
                            season_name = None
                            season_year = None

                            # Use metadata snapshot from timing check (avoids redundant get_event_details call)
                            snapshot = event_payload.get('metadata_snapshot')
                            if snapshot:
                                home_team_id = snapshot.get('home_team_id')
                                away_team_id = snapshot.get('away_team_id')
                                tournament_id = snapshot.get('tournament_id')
                                competition_name = snapshot.get('tournament_name')
                                competition_slug = snapshot.get('competition_slug')
                                season_id = snapshot.get('season_id')
                                season_name = snapshot.get('season_name')
                                season_year = snapshot.get('season_year')
                                logger.debug(f"Extracted metadata from snapshot: Home={home_team_id}, Away={away_team_id}, tournament={tournament_id}")
                                logger.debug(f"Extracted season data from snapshot: season_id={season_id}, season_name={season_name}, season_year={season_year}")

                                # Tennis rankings from snapshot
                                if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                                    has_rankings = False
                                    if observations:
                                        has_rankings = any(obs.get('type') == 'rankings' for obs in observations)
                                    if not has_rankings:
                                        home_team_ranking = snapshot.get('home_team_ranking')
                                        away_team_ranking = snapshot.get('away_team_ranking')
                                        if observations is None:
                                            observations = []
                                        observations.append({"type": "rankings", "home_ranking": home_team_ranking, "away_ranking": away_team_ranking})
                                        logger.info(f"✅ Added rankings from snapshot for event {event_obj.id}: home={home_team_ranking}, away={away_team_ranking}")
                                    else:
                                        logger.debug(f"Rankings already exist in observations for event {event_obj.id}")
                            else:
                                # Fallback: No snapshot (e.g., timestamp correction disabled), use API call
                                try:
                                    event_details = api_client.get_event_details(event_obj.id)
                                    if event_details:
                                        if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                                            has_rankings = False
                                            if observations:
                                                has_rankings = any(obs.get('type') == 'rankings' for obs in observations)
                                            if not has_rankings:
                                                home_team_ranking = event_details.get('homeTeam', {}).get('ranking')
                                                away_team_ranking = event_details.get('awayTeam', {}).get('ranking')
                                                if observations is None:
                                                    observations = []
                                                observations.append({"type": "rankings", "home_ranking": home_team_ranking, "away_ranking": away_team_ranking})
                                                logger.info(f"✅ Added rankings from API fallback for event {event_obj.id}")
                                        home_team_id = event_details.get('homeTeam', {}).get('id')
                                        away_team_id = event_details.get('awayTeam', {}).get('id')
                                        tournament_id = event_details.get('tournament', {}).get('id')
                                        competition_name = event_details.get('tournament', {}).get('name')
                                        competition_slug = event_details.get('tournament', {}).get('uniqueTournament', {}).get('slug')
                                        season_id = str(event_details.get('season', {}).get('id', '')) if event_details.get('season', {}).get('id') else None
                                        season_name = event_details.get('season', {}).get('name')
                                        season_year_raw = event_details.get('season', {}).get('year')
                                        from repository import SeasonRepository
                                        season_year = SeasonRepository._parse_year(season_year_raw) if season_year_raw else None
                                    else:
                                        logger.warning(f"Could not fetch event details for {event_obj.id}")
                                except Exception as e:
                                    logger.error(f"Error fetching event details for {event_obj.id}: {e}")

                            tennis_observations = observations if observations else []

                            if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                                has_ground_type = any(obs.get('type') == 'ground_type' for obs in tennis_observations)

                                if not has_ground_type:
                                    logger.info(f"🎾 Getting ground_type for tennis event {event_obj.id} for filtering")
                                    if not sport_observations_manager.has_observations_for_event(event_obj.id):
                                        # Try snapshot observations first
                                        if snapshot and snapshot.get('observations'):
                                            for obs in snapshot['observations']:
                                                if obs.get('type') == 'ground_type':
                                                    tennis_observations.append(obs)
                                                    ObservationRepository.upsert_observation(event_obj.id, event_obj.sport, 'ground_type', obs['value'])
                                                    logger.info(f"🎾 Ground type from snapshot: {obs['value']}")
                                        else:
                                            new_observations = api_client.get_event_results(
                                                event_id=event_obj.id,
                                                update_court_type=True
                                            )
                                            if new_observations:
                                                for obs in new_observations:
                                                    if obs.get('type') == 'ground_type':
                                                        tennis_observations.append(obs)
                                                    elif obs.get('type') == 'rankings':
                                                        existing_rankings = next((o for o in tennis_observations if o.get('type') == 'rankings'), None)
                                                        if not existing_rankings:
                                                            tennis_observations.append(obs)
                                                            logger.info(f"✅ Added rankings from results check for event {event_obj.id}")
                                                        else:
                                                            if existing_rankings.get('home_ranking') is None and existing_rankings.get('away_ranking') is None:
                                                                if obs.get('home_ranking') is not None or obs.get('away_ranking') is not None:
                                                                    existing_rankings['home_ranking'] = obs.get('home_ranking')
                                                                    existing_rankings['away_ranking'] = obs.get('away_ranking')
                                                                    logger.info(f"✅ Updated rankings from results check for event {event_obj.id} (overwrote None values)")
                                    else:
                                        observation = ObservationRepository.get_observation(event_obj.id, 'ground_type')
                                        if observation:
                                            tennis_observations.append({'type': 'ground_type', 'value': observation.observation_value})
                                            logger.info(f"🎾 Using existing ground_type observation: {observation.observation_value}")

                                # Final rankings check using snapshot if still missing
                                has_rankings = any(obs.get('type') == 'rankings' for obs in tennis_observations)
                                if not has_rankings and snapshot:
                                    home_team_ranking = snapshot.get('home_team_ranking')
                                    away_team_ranking = snapshot.get('away_team_ranking')
                                    if home_team_ranking is not None or away_team_ranking is not None:
                                        tennis_observations.append({"type": "rankings", "home_ranking": home_team_ranking, "away_ranking": away_team_ranking})
                                        logger.info(f"✅ Added rankings from snapshot for tennis_observations for event {event_obj.id}")

                            if tennis_observations:
                                rankings_info = next((obs for obs in tennis_observations if isinstance(obs, dict) and obs.get('type') == 'rankings'), None)
                                if rankings_info:
                                    logger.info(f"📊 Passing observations to analyze_h2h_events for event {event_obj.id}: home_ranking={rankings_info.get('home_ranking')}, away_ranking={rankings_info.get('away_ranking')}")
                                else:
                                    logger.warning(f"⚠️ No rankings found in tennis_observations for event {event_obj.id}")

                            streak_analysis = streak_alert_engine.analyze_h2h_events(
                                event_id=event_obj.id,
                                event_custom_id=event_obj.custom_id,
                                event_start_time=event_obj.start_time_utc,
                                sport=event_obj.sport,
                                discovery_source=event_obj.discovery_source,
                                tournament_id=tournament_id,
                                competition_name=competition_name,
                                competition_slug=competition_slug,
                                season_id=season_id,
                                season_name=season_name,
                                participants=f"{event_obj.home_team} vs {event_obj.away_team}",
                                home_team_name=event_obj.home_team,
                                away_team_name=event_obj.away_team,
                                h2h_events=h2h_events,
                                minutes_until_start=minutes_until_start,
                                season_year=season_year,
                                observations=tennis_observations,
                                home_team_id=home_team_id,
                                away_team_id=away_team_id,
                                event_odds=event_obj.event_odds
                            )

                            if streak_analysis and streak_alert_engine.should_send_streak_alert(streak_analysis):
                                logger.info(f"✅ H2H streak analysis completed for event {event_obj.id}: {streak_analysis.current_streak}")
                                
                                # SMART ALERT FILTERING: RESURRECTION LOGIC
                                from config import Config as AppConfig
                                home_result_count = len(streak_analysis.home_team_results) if streak_analysis.home_team_results else 0
                                away_result_count = len(streak_analysis.away_team_results) if streak_analysis.away_team_results else 0
                                min_threshold = AppConfig.STREAK_ALERT_MIN_RESULTS
                                
                                if home_result_count >= min_threshold or away_result_count >= min_threshold:
                                    fresh_event = self.event_repo.get_event_by_id(event_obj.id)
                                    if fresh_event and fresh_event.alert_sent:
                                        logger.info(f"🔄 RESURRECTING EVENT: Event {event_obj.id} has sufficient data (home:{home_result_count}, away:{away_result_count}) - resetting alert_sent=False")
                                        self._reset_event_alert_sent(event_obj.id)
                                        event_obj = self.event_repo.get_event_by_id(event_obj.id)
                            else:
                                logger.debug(f"⏭️ No H2H streak alert for event {event_obj.id}")
                        else:
                            logger.debug(f"No H2H data found for event {event_obj.id} (custom_id: {event_obj.custom_id})")
                    except Exception as e:
                        logger.error(f"Error analyzing H2H streak for event {event_obj.id}: {e}")
                else:
                    logger.debug(f"Event {event_obj.id} has no custom_id - skipping H2H streak analysis")
            else:
                logger.debug(f"⏭️ Skipping H2H streak analysis for event {event_obj.id} - minutes_until_start={minutes_until_start} (only run at 30)")
            
            # ========================================
            # DUAL PROCESS ANALYSIS FOR THIS EVENT
            # ========================================
            dual_report = None
            
            # SMART ALERT FILTERING: CHECK alert_sent FLAG
            fresh_event_for_dual = self.event_repo.get_event_by_id(event_obj.id)
            if fresh_event_for_dual and fresh_event_for_dual.alert_sent:
                logger.info(f"⏭️ EARLY EXIT: Skipping dual process for event {event_obj.id} - marked as low-value (alert_sent=True)")
            elif getattr(event_obj, 'discovery_source', None) != 'dropping_odds':
                discovery_source = getattr(event_obj, 'discovery_source', None)
                logger.info(f"⏭️ Skipping dual process for event {event_obj.id} - discovery_source='{discovery_source}' (only processing 'dropping_odds')")
            else:
                try:
                    event_obj.court_type = None
                    if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                        observation = ObservationRepository.get_observation(event_obj.id, 'ground_type')
                        if observation:
                            event_obj.court_type = observation.observation_value
                            logger.info(f"🎾 Court type for event {event_obj.id}: {event_obj.court_type}")
                        else:
                            logger.info(f"🎾 No court type found for event {event_obj.id}")
                    
                    dual_report = prediction_engine.evaluate_dual_process(event_obj, minutes_until_start)
                    
                    should_send = False
                    reason = ""
                    
                    if dual_report.process1_prediction or dual_report.process2_prediction:
                        should_send = True
                        reason = f"Process1={bool(dual_report.process1_prediction)}, Process2={bool(dual_report.process2_prediction)}"
                    elif dual_report.process1_report and dual_report.process1_status in ['partial', 'no_match', 'no_candidates']:
                        should_send = True
                        reason = f"Process1 found candidates (status: {dual_report.process1_status})"
                    
                    if should_send:
                        logger.info(f"✅ Dual process report added for event {event_obj.id}: {reason}")
                    else:
                        logger.debug(f"⏭️ Skipping dual process report for event {event_obj.id}: No predictions or candidates found")
                        
                except Exception as e:
                    logger.error(f"Error running dual process evaluation for event {event_obj.id}: {e}")
                    
                    # Fallback to Process 1 only if dual process fails
                    if event_obj.discovery_source == 'dropping_odds':
                        logger.info(f"🔄 Falling back to Process 1 only for event {event_obj.id}...")
                        try:
                            alerts = alert_engine.evaluate_upcoming_events([event_obj])
                            if alerts:
                                logger.info(f"📊 Generated {len(alerts)} Process 1 candidate reports (fallback) for event {event_obj.id}")
                                alert_engine.send_alerts(alerts)
                                
                                for alert in alerts:
                                    if alert.get('status') == 'success':
                                        event_id = alert.get('event_id')
                                        if event_id:
                                            event_obj_fallback = self.event_repo.get_event_by_id(event_id)
                                            if event_obj_fallback:
                                                minutes_until_start_fallback = self._minutes_until_start(event_obj_fallback.start_time_utc)
                                                if minutes_until_start_fallback == 0:
                                                    success = prediction_logger.log_prediction(event_obj_fallback, alert)
                                                    if success:
                                                        logger.info(f"✅ Prediction logged for Process 1 event {event_id} (0 minutes from start)")
                                                    else:
                                                        with db_manager.get_session() as session:
                                                            existing = session.query(PredictionLog).filter_by(event_id=event_id).first()
                                                            if existing:
                                                                logger.info(f"ℹ️ Prediction already exists for Process 1 event {event_id} - no action needed")
                                                            else:
                                                                logger.warning(f"❌ Failed to log prediction for Process 1 event {event_id}")
                                                else:
                                                    logger.info(f"⏭️ Skipping prediction logging for event {event_id} - {minutes_until_start_fallback} minutes until start (not 0 minutes)")
                                            else:
                                                logger.warning(f"Could not find event {event_id} for prediction logging")
                            else:
                                logger.debug(f"No Process 1 candidate reports generated (fallback) for event {event_obj.id}")
                        except Exception as e2:
                            logger.error(f"Error in Process 1 fallback for event {event_obj.id}: {e2}")
                    else:
                        discovery_source_fallback = getattr(event_obj, 'discovery_source', None)
                        logger.info(f"⏭️ Skipping Process 1 fallback for event {event_obj.id} - discovery_source='{discovery_source_fallback}' (only processing 'dropping_odds')")
            
            # ========================================
            # COLLECT ALERTS (returned to caller for ordered sending)
            # ========================================
            result['streak_analysis'] = streak_analysis
            result['dual_report'] = dual_report
            result['minutes_until_start'] = minutes_until_start
            result['event_obj'] = event_obj
            result['season_id'] = event_payload.get('season_id')
            result['success'] = True
            
            logger.info(f"✅ Completed processing event {event_obj.id}: {event_obj.home_team} vs {event_obj.away_team}")
            
        except Exception as e:
            logger.error(f"Error processing event {result.get('event_id', 'unknown')}: {e}")
        
        return result
    
    def _process_event_batch(self, batch: list, KEY_MOMENTS: list) -> list:
        """
        Process a batch of events sequentially. Called by each parallel worker.
        Returns a list of result dicts from _process_single_event_alerts.
        """
        results = []
        for event_payload in batch:
            result = self._process_single_event_alerts(event_payload, KEY_MOMENTS)
            results.append(result)
        return results
        
    def _evaluate_and_send_alerts_batch(self, events_for_alerts: list, KEY_MOMENTS: list,
                                         op_done_events=None, op_event_ids=None):
        """
        Helper method that encapsulates the thread-pooled parallel evaluation and sequential grouping of alerts.
        It evaluates upcoming events through the prediction engine and H2H analyzers, and sends notifications.
        
        Args:
            op_done_events: dict of threading.Event signaled when OP scraping finishes per event
            op_event_ids: set of event IDs that are being scraped by the OP worker
        """
        try:
            logger.info(f"🔍 Evaluating {len(events_for_alerts)} events at key moments for H2H and dual process alerts...")
            
            n_events = len(events_for_alerts)
            all_results = []
            
            if n_events == 1:
                # Single event - no parallelism needed
                logger.info("📦 Single event - processing directly (no batching)")
                all_results = [self._process_single_event_alerts(events_for_alerts[0], KEY_MOMENTS)]
            else:
                # Split events into 2 balanced batches for parallel processing
                mid = n_events // 2
                batch_a = events_for_alerts[:mid]
                batch_b = events_for_alerts[mid:]
                logger.info(f"📦 Splitting {n_events} events into 2 batches: A={len(batch_a)}, B={len(batch_b)}")
                
                with ThreadPoolExecutor(max_workers=2, thread_name_prefix="alert_worker") as executor:
                    future_a = executor.submit(self._process_event_batch, batch_a, KEY_MOMENTS)
                    future_b = executor.submit(self._process_event_batch, batch_b, KEY_MOMENTS)
                    
                    # Collect results in order (batch A first, then batch B)
                    for future_label, future in [("A", future_a), ("B", future_b)]:
                        try:
                            batch_results = future.result(timeout=300)  # 5 min timeout per batch
                            all_results.extend(batch_results)
                            logger.info(f"✅ Batch {future_label} completed: {len(batch_results)} events processed")
                        except Exception as e:
                            logger.error(f"❌ Batch {future_label} failed: {e}")
            
            # ========================================
            # SEND ALERTS IN PARALLEL PER EVENT
            # Each event's alert group (Odds → H2H → Dual) runs in its own thread
            # so events don't block each other while waiting for OP data.
            # ========================================
            from odds_alert import odds_alert_processor
            from alert_system import pre_start_notifier
            
            def _send_event_alerts(result):
                """Send all alerts for a single event. Runs in its own thread."""
                if not result.get('success'):
                    return
                
                event_obj = result.get('event_obj')
                streak_analysis = result.get('streak_analysis')
                dual_report = result.get('dual_report')
                odds_response = result.get('odds_response')
                minutes_until_start = result.get('minutes_until_start')
                
                # 1) Send ODDS alert first for this event
                # If this event is an OP-tracked event, wait for OP scraping to finish
                # so the odds alert can include OddsPortal bookie data from the DB.
                if odds_response:
                    if op_event_ids and event_obj.id in op_event_ids and op_done_events:
                        per_event = op_done_events.get(event_obj.id)
                        if per_event and not per_event.is_set():
                            timeout_s = getattr(Config, "ODDSPORTAL_ALERT_WAIT_TIMEOUT", 180)
                            logger.info(f"[OP] Waiting for OddsPortal signal (up to {timeout_s}s) before sending odds alert for event {event_obj.id}...")
                            signaled = per_event.wait(timeout=timeout_s)
                            if signaled:
                                logger.info(f"[OP] Worker signaled completion for event {event_obj.id}. Verifying DB availability...")
                            else:
                                logger.warning(f"[OP] Timed out after {timeout_s}s waiting for OddsPortal for event {event_obj.id}. Sending odds alert now (OddsPortal section may be missing).")

                            # Log whether OP section will actually be included (based on DB availability)
                            try:
                                from repository import MarketRepository
                                op_markets = MarketRepository.get_oddsportal_markets_for_event(event_obj.id)
                                if op_markets:
                                    logger.info(f"[OP] OddsPortal data is available for event {event_obj.id} ({len(op_markets)} rows) - OddsPortal section should be included.")
                                else:
                                    logger.info(f"[OP] OddsPortal data is NOT available for event {event_obj.id} - OddsPortal section will NOT be included.")
                            except Exception as op_check_err:
                                logger.warning(f"[OP] Could not verify OddsPortal DB availability for event {event_obj.id}: {op_check_err}")
                    logger.info(f"📊 Sending odds alert for event {event_obj.id} (1st in group)")
                    try:
                        event_data_for_odds = {
                            'id': event_obj.id,
                            'home_team': event_obj.home_team,
                            'away_team': event_obj.away_team,
                            'sport': event_obj.sport,
                            'competition': getattr(event_obj, 'competition', ''),
                            'slug': event_obj.slug,
                            'discovery_source': getattr(event_obj, 'discovery_source', ''),
                            'season_id': result.get('season_id')
                        }
                        odds_alert_processor.send_odds_alert(event_data_for_odds, odds_response, minutes_until_start)
                    except Exception as e:
                        logger.error(f"Error sending odds alert for event {event_obj.id}: {e}")
                
                # 2) Send H2H streak alert for this event
                if streak_analysis:
                    logger.info(f"📊 Sending H2H streak alert for event {event_obj.id} (2nd in group)")
                    pre_start_notifier.send_h2h_streak_alerts([streak_analysis])
                
                # 3) Send dual process alert for this event
                if dual_report and (dual_report.process1_prediction or dual_report.process2_prediction or 
                                  (dual_report.process1_report and dual_report.process1_status in ['partial', 'no_match', 'no_candidates'])):
                    logger.info(f"📊 Sending dual process alert for event {event_obj.id} (3rd in group)")
                    self._send_dual_process_alerts([dual_report])
                    
                    # Log predictions for successful Process 1 reports
                    from modules.prediction import prediction_logger
                    from database import db_manager
                    from models import PredictionLog
                    
                    if (dual_report.process1_report and 
                        dual_report.process1_report.get('status') == 'success'):
                        
                        if minutes_until_start == 0:
                            success = prediction_logger.log_prediction(event_obj, dual_report.process1_report)
                            if success:
                                logger.info(f"✅ Prediction logged for dual process event {event_obj.id} (0 minutes from start)")
                            else:
                                with db_manager.get_session() as session:
                                    existing = session.query(PredictionLog).filter_by(event_id=event_obj.id).first()
                                    if existing:
                                        logger.info(f"ℹ️ Prediction already exists for dual process event {event_obj.id} - no action needed")
                                    else:
                                        logger.warning(f"❌ Failed to log prediction for dual process event {event_obj.id}")
                        else:
                            logger.info(f"⏭️ Skipping prediction logging for event {event_obj.id} - {minutes_until_start} minutes until start (not 0 minutes)")
            
            # Fire all event alert groups in parallel — each event waits on its OWN OP signal
            alert_results_to_send = [r for r in all_results if r.get('success')]
            if alert_results_to_send:
                with ThreadPoolExecutor(max_workers=len(alert_results_to_send), thread_name_prefix="alert_sender") as alert_executor:
                    alert_futures = {
                        alert_executor.submit(_send_event_alerts, result): result.get('event_obj', {})
                        for result in alert_results_to_send
                    }
                    for future in alert_futures:
                        try:
                            future.result(timeout=300)  # 5 min max per event alert group
                        except Exception as e:
                            event_ref = alert_futures[future]
                            logger.error(f"❌ Alert sending failed for event {getattr(event_ref, 'id', '?')}: {e}")
            
            logger.info(f"✅ Parallel alert processing complete: {len(all_results)} events processed, alerts sent grouped by event")
                
        except Exception as e:
            logger.error(f"Error in event processing batch: {e}")
            
    def _oddsportal_worker_wrapper(self, op_candidates: list, op_done_events: dict):
        """
        Background worker that ONLY scrapes OddsPortal and saves to DB.
        Signals completion in the op_done_events dict so the main thread's alert delivery
        can include the OP data in odds alerts per event.
        """
        logger.info(f"🔥 OP Worker started: scraping {len(op_candidates)} tracked-league events.")
        try:
            self._run_oddsportal_batch(op_candidates, op_done_events)
        except Exception as e:
            import traceback
            logger.error(f"❌ OddsPortal Worker CRASHED: {e}\n{traceback.format_exc()}")
        finally:
            # Safety net: signal any events that weren't signaled yet
            if op_done_events:
                for eid, evt in op_done_events.items():
                    if not evt.is_set():
                        evt.set()
                        logger.warning(f"⚠️ OP Worker: force-signaled event {eid} (wasn't finished before worker exit)")
            logger.info("✅ OP Worker finished scraping, main thread unblocked.")
    
    def _run_oddsportal_batch(self, events_to_process: list, op_done_events: dict = None) -> dict:
        """
        Dedicated OddsPortal worker: pre-scans events for eligibility,
        then scrapes all matches with a single browser session.
        
        Returns dict mapping event_id -> number of markets saved (or None on failure).
        """
        from oddsportal_config import SEASON_ODDSPORTAL_MAP
        from oddsportal_scraper import scrape_multiple_matches_parallel_sync
        from repository import MarketRepository
        from config import Config as AppConfig
        
        op_tasks = []
        for event_info in events_to_process:
            event_data = event_info['event_data']
            season_id = event_data.get('season_id')
            op_info = SEASON_ODDSPORTAL_MAP.get(season_id)
            
            if op_info and event_info.get('should_extract_odds'):
                league_url = f"https://www.oddsportal.com/{op_info['sport']}/{op_info['country']}/{op_info['league']}/"
                op_tasks.append({
                    'event_id': event_data['id'],
                    'league_url': league_url,
                    'home_team': event_data['home_team'],
                    'away_team': event_data['away_team'],
                    'season_id': season_id,
                    'sport': op_info['sport'],
                })
        
        if not op_tasks:
            logger.info("ℹ️ OddsPortal: No eligible events to scrape")
            return {}
        
        logger.info(f"🔍 OddsPortal worker: {len(op_tasks)} events eligible for scraping")
        
        # Shared dict for save results (thread-safe: GIL protects dict mutations)
        saved_counts = {}
        
        # Callback: invoked IMMEDIATELY after each event scrape completes
        # (while still inside the scraper's async loop — before the browser closes)
        def _on_event_scraped(event_id, op_data):
            if op_data:
                try:
                    saved = MarketRepository.save_markets_from_oddsportal(event_id, op_data)
                    saved_counts[event_id] = saved
                    logger.info(f"✅ OddsPortal: Saved {saved} markets/bookies for event {event_id}")
                except Exception as e:
                    logger.error(f"❌ OddsPortal: Error saving data for event {event_id}: {e}")
                    saved_counts[event_id] = None
            else:
                logger.warning(f"⚠️ OddsPortal: No data for event {event_id}")
                saved_counts[event_id] = None
            
            # Signal this specific event immediately — alert thread unblocked
            if op_done_events and event_id in op_done_events:
                op_done_events[event_id].set()
                logger.info(f"🔔 OP: Signaled event {event_id} — alert thread unblocked for this event")
        
        # Scrape all matches (parallel if configured) — callback fires per event
        num_browsers = AppConfig.ODDSPORTAL_PARALLEL_BROWSERS
        logger.info(f"🌐 OddsPortal: Calling scrape_multiple_matches_parallel_sync for {len(op_tasks)} tasks with {num_browsers} browser(s)...")
        op_results = scrape_multiple_matches_parallel_sync(
            op_tasks, 
            num_browsers=num_browsers, 
            debug_dir="oddsportal_debug",
            on_result=_on_event_scraped
        )
        logger.info(f"🌐 OddsPortal: scrape_multiple_matches_parallel_sync returned {len(op_results)} results")
        
        return saved_counts


    def _check_recently_started_events_for_timestamp_corrections(self, events_started_recently: List[Dict]) -> set:
        """
        Check recently started events for timestamp corrections with sport-specific windows.
        
        Tennis/Tennis Doubles: Checks up to 60 minutes after start (every 5 minutes)
        Other sports: Checks up to 15 minutes after start (at 5, 10, 15 minutes)
        
        This catches late changes that occur after the game starts or right at start time.
        Only checks events at specific intervals to avoid timing precision issues.
        
        Args:
            events_started_recently: List of event dictionaries from get_events_started_recently()
            
        Returns:
            set: Set of event IDs that were modified/rescheduled
        """
        modified_event_ids = set()
        try:
            corrected_count = 0
            checked_count = 0
            
            for event_data in events_started_recently:
                try:
                    logger.info(f"Checking recently started event {event_data['slug']} - {event_data['start_time_utc']}")
                    event_id = event_data['id']
                    sport = event_data['sport']
                    stored_start_time = event_data['start_time_utc']
                    
                    # Calculate how long ago this event started (using stored time)
                    minutes_since_start = self._minutes_since_start(stored_start_time)
                    
                    # Convert to positive for easier comparison
                    minutes_ago = abs(minutes_since_start)
                    
                    # Define check intervals based on sport (when to actually check for timestamp corrections)
                    # Tennis changes timestamps frequently, even up to 1 hour after scheduled start
                    if sport in ['Tennis', 'Tennis Doubles']:
                        # Tennis: Check every 5 minutes up to 60 minutes after start
                        CHECK_INTERVALS = list(range(5, 65, 5))  # [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
                        logger.debug(f"🎾 Tennis event {event_id} - using extended check intervals up to 60 minutes")
                    else:
                        # Other sports: Check only at 15 minutes after start
                        CHECK_INTERVALS = [15]
                        
                        # Skip non-tennis events that are outside the 15-minute window
                        if minutes_ago > 15:
                            logger.debug(f"⏭️ Skipping non-tennis event {event_id} - outside 15-minute window ({minutes_ago} minutes ago)")
                            continue
                    
                    # Check if current time aligns with a check interval
                    if minutes_ago not in CHECK_INTERVALS:
                        logger.debug(f"⏭️ Skipping event {event_id} - not at check interval ({minutes_ago} minutes ago, checking at {CHECK_INTERVALS})")
                        continue
                    
                    # Check and update starting time via API
                    # This returns: True if time is correct, False if time was updated, None if API error
                    correct_starting_time = api_client.get_event_results(
                        event_id, 
                        update_time=True, 
                        minutes_until_start=minutes_since_start  # Pass negative value for started events
                    )
                    
                    checked_count += 1
                    
                    if correct_starting_time is None:
                        # API error - skip this event
                        logger.warning(f"⏭️ API error for event {event_id} - skipping timestamp check")
                        continue
                    elif not correct_starting_time:
                        # Starting time was corrected by the API call
                        corrected_count += 1
                        logger.info(f"✅ TIMESTAMP CORRECTED for event {event_id} - starting time was updated after game started")
                    
                    # Track this ID as modified
                    modified_event_ids.add(event_id)
                    
                    # Trigger rescheduled event processing to extract odds and run alerts/H2H
                    # This ensures the event doesn't get lost after timestamp correction
                    # _check_rescheduled_event will mark the event as processed internally
                    logger.info(f"🔄 Processing rescheduled event {event_id} after timestamp correction")
                    self._check_rescheduled_event(event_id)
                
                except Exception as e:
                    logger.error(f"Error checking recently started event {event_data.get('id')}: {e}")
                    continue
            
            if checked_count > 0:
                logger.info(f"📊 Timestamp correction check completed: {checked_count} events checked, {corrected_count} timestamps corrected")
            
            return modified_event_ids
            
        except Exception as e:
            logger.error(f"Error in _check_recently_started_events_for_timestamp_corrections: {e}")
            return modified_event_ids
    
    def _minutes_until_start(self, start_time_utc) -> int:
        """Calculate minutes until event starts"""
        # Note: start_time_utc is actually stored in local timezone (not UTC)
        # This is because datetime.fromtimestamp() creates local time, not UTC
        # So we treat it as local time and make it timezone-aware
        from timezone_utils import TIMEZONE
        if start_time_utc.tzinfo is None:
            # Make the naive datetime timezone-aware in local timezone
            start_local = TIMEZONE.localize(start_time_utc)
        else:
            start_local = start_time_utc

        # Get local current time (aware)
        now = get_local_now_aware()

        # Calculate time difference in minutes
        time_diff = start_local - now
        return round(time_diff.total_seconds() / 60)
    
    def _minutes_since_start(self, start_time_utc) -> int:
        """
        Calculate minutes since event started (returns negative value for consistency).
        Similar to _minutes_until_start but for events that already started.
        
        Args:
            start_time_utc: Event start time (in local timezone despite column name)
            
        Returns:
            Negative integer representing minutes since start (e.g., -5 means started 5 minutes ago)
        """
        from timezone_utils import TIMEZONE
        if start_time_utc.tzinfo is None:
            # Make the naive datetime timezone-aware in local timezone
            start_local = TIMEZONE.localize(start_time_utc)
        else:
            start_local = start_time_utc

        # Get local current time (aware)
        now = get_local_now_aware()

        # Calculate time difference in minutes (negative for past events)
        time_diff = start_local - now
        return round(time_diff.total_seconds() / 60)
    
    def _reset_event_alert_sent(self, event_id: int) -> bool:
        """
        Reset event alert_sent to False (resurrect for future alerts).
        
        This is called when an event was initially marked as low-value (1 market)
        but later found to have sufficient historical data (≥15 results),
        making it valuable for other alert processes.
        
        Args:
            event_id: Event ID to reset
            
        Returns:
            True if successfully reset, False otherwise
        """
        try:
            from models import Event
            
            with db_manager.get_session() as session:
                event = session.query(Event).filter(Event.id == event_id).first()
                if event:
                    event.alert_sent = False
                    session.commit()
                    logger.info(f"✅ Reset alert_sent=False for event {event_id} (resurrected)")
                    return True
                else:
                    logger.warning(f"Event {event_id} not found when resetting alert_sent")
                    return False
        except Exception as e:
            logger.error(f"Error resetting alert_sent for event {event_id}: {e}")
            return False
    
    def _should_extract_odds_for_event(self, event_id: int, minutes_until_start: int) -> tuple:
        """
        Smart logic to determine if odds should be extracted for an event.
        Only extract odds at key moments: 30 minutes and 0 minutes before start.
        
        Args:
            event_id: Event ID
            minutes_until_start: Minutes until the event starts
            
        Returns:
            Tuple of (should_extract: bool, metadata_snapshot: Optional[Dict])
            The metadata_snapshot contains team IDs, rankings, tournament/season info,
            and observations extracted from the same /event/{id} API response used
            for the timestamp check, eliminating redundant API calls.
        """
        # Check if odds extraction is enabled (for testing purposes)
        if not Config.ENABLE_ODDS_EXTRACTION:
            logger.info(f"🚫 ODDS EXTRACTION DISABLED: Skipping odds extraction for event {event_id} (ENABLE_ODDS_EXTRACTION=false)")
            return False, None
        
        # Key moments for odds extraction (removed 1-minute check - now handled by 5-minutes-after logic)
        KEY_MOMENTS = [120, 30, 5, 0]
        
        # Check if current time aligns with a key moment
        should_extract = minutes_until_start in KEY_MOMENTS
        
        # Only check and update starting time if event is in key moments (optimization)
        if not should_extract:
            logger.debug(f"⏭️ Not a key moment for event {event_id}: {minutes_until_start} minutes until start - SKIPPING API CALL AND ODDS EXTRACTION")
            return False, None
        
        # Check if timestamp correction is enabled
        if not Config.ENABLE_TIMESTAMP_CORRECTION:
            logger.info(f"🎯 Key moment detected for event {event_id}: {minutes_until_start} minutes until start - WILL EXTRACT ODDS (timestamp correction disabled)")
            return True, None
        
        # Check and update starting time only for events in key moments
        # return_snapshot=True: extract metadata from the same API response to avoid redundant calls
        correct_starting_time, metadata_snapshot = api_client.get_event_results(event_id, update_time=True, return_snapshot=True)
        
        if correct_starting_time:
            logger.info(f"🎯 Key moment detected for event {event_id}: {minutes_until_start} minutes until start - WILL EXTRACT ODDS")
            return True, metadata_snapshot
        elif correct_starting_time is None:
            # API call failed - skip odds extraction but don't trigger rescheduled logic
            logger.warning(f"⏭️ API error for event {event_id} - skipping odds extraction")
            return False, None
        elif not correct_starting_time:
            # Starting time was actually updated - mark as rescheduled and check if rescheduled game is now in key moments
            logger.info(f"🔄 Starting time changed for event {event_id} - marking as rescheduled and checking if rescheduled game is in key moments")
            self.recently_rescheduled.add(event_id)  # Mark immediately to prevent double processing
            self._check_rescheduled_event(event_id, metadata_snapshot=metadata_snapshot)
            return False, None
        else:
            logger.debug(f"⏭️ Starting Time changed for event {event_id} - skipping odds extraction or not a key moment")
            return False, None
    
    def _check_rescheduled_event(self, event_id: int, metadata_snapshot: dict = None):
        """
        Check if a rescheduled event is now within the 30 or 5 minute window.
        If so, extract odds and process alerts for the rescheduled game.
        Includes complete pre-start job workflow to avoid infinite loops.
        
        Args:
            event_id: Event ID to check
            metadata_snapshot: Optional cached metadata from the timing check API response
        """
        try:
            # Clean up old tracking entries
            self._cleanup_recently_rescheduled()
            
            # Check if we've already processed this rescheduled event recently (prevent infinite loops)
            if event_id in self.recently_rescheduled:
                logger.debug(f"Event {event_id} already processed as rescheduled recently - skipping to prevent infinite loop")
                return
            
            # Get the updated event data
            event = self.event_repo.get_event_by_id(event_id)
            if not event:
                logger.warning(f"Could not find event {event_id} after time update")
                return
            
            # Calculate new minutes until start
            new_minutes_until_start = self._minutes_until_start(event.start_time_utc)
            
            # Check if rescheduled game is now in key moments (30 or 0 minutes), starting now (0), or has already started (negative)
            # We always process rescheduled events to catch late corrections
            if new_minutes_until_start in [30, 0] or new_minutes_until_start < 0:
                if new_minutes_until_start > 0:
                    logger.info(f"🎯 RESCHEDULED GAME ALERT: Event {event_id} is now starting in {new_minutes_until_start} minutes")
                elif new_minutes_until_start == 0:
                    logger.info(f"🎯 RESCHEDULED GAME ALERT: Event {event_id} is starting NOW (0 minutes)")
                else:
                    logger.info(f"🎯 RESCHEDULED GAME ALERT: Event {event_id} has already started {abs(new_minutes_until_start)} minutes ago - extracting latest odds")
                
                # Mark this event as recently processed to prevent infinite loops
                self.recently_rescheduled.add(event_id)
                
                # Extract odds for the rescheduled game (same logic as main pre-start job)
                final_odds_response = api_client.get_event_final_odds(event_id, event.slug)
                if final_odds_response:
                    final_odds_data = api_client.extract_final_odds_from_response(final_odds_response)
                    if final_odds_data:
                        # Update odds and create snapshot
                        upserted_id = OddsRepository.upsert_event_odds(event_id, final_odds_data)
                        if upserted_id:
                            OddsRepository.create_odds_snapshot(event_id, final_odds_data)
                            logger.info(f"✅ Odds extracted for rescheduled event {event_id}")
                            
                            # Only process alerts for future events (betting still possible)
                            if new_minutes_until_start >= 0:
                                logger.info(f"🔍 Processing alerts for future rescheduled event {event_id}")
                                self._process_alerts_for_rescheduled_event(event, metadata_snapshot=metadata_snapshot)
                            else:
                                logger.info(f"⏭️ Skipping alert processing for past event {event_id} (betting no longer possible)")
                        else:
                            logger.warning(f"Failed to update odds for rescheduled event {event_id}")
                    else:
                        logger.warning(f"No odds data extracted for rescheduled event {event_id}")
                else:
                    logger.warning(f"Failed to fetch odds for rescheduled event {event_id}")
            else:
                logger.debug(f"Rescheduled event {event_id} not in key moments and hasn't started yet: {new_minutes_until_start} minutes until start")
                
        except Exception as e:
            logger.error(f"Error checking rescheduled event {event_id}: {e}")
    
    def _process_alerts_for_rescheduled_event(self, event, metadata_snapshot: dict = None):
        """
        Process H2H and dual-process alerts for a rescheduled event.
        This is called after odds have been extracted for the rescheduled game.
        
        Args:
            event: Event database object with start_time_utc, sport, etc.
            metadata_snapshot: Optional cached metadata from the timing check API response
        """
        try:
            # Initialize observations for this rescheduled event
            observations = None
            
            # Filter out excluded sports from alert evaluation
            if event.sport in Config.EXCLUDED_SPORTS:
                logger.info(f"⏭️ SKIPPING ALERT EVALUATION (rescheduled): Event {event.id} ({event.home_team} vs {event.away_team}) is {event.sport} (excluded)")
                return
            
            logger.info(f"🔍 Processing H2H and dual-process alerts for rescheduled event {event.id}")
            
            # Refresh materialized views to ensure latest data for alert evaluation
            logger.info("🔄 Refreshing alert materialized views for rescheduled event...")
            
            refresh_materialized_views(db_manager.engine)
            logger.info("✅ Alert materialized views refreshed")
            
            # Get the Event object with properly loaded event_odds for alert evaluation
            event_obj = self.event_repo.get_event_by_id(event.id)
            if not event_obj or not event_obj.event_odds:
                logger.warning(f"Could not load event_odds for rescheduled event {event.id}")
                return
            
            # Calculate minutes until start for the rescheduled event using FRESH database time
            minutes_until_start = self._minutes_until_start(event_obj.start_time_utc)
            logger.info(f"🔍 Evaluating rescheduled event {event.id} for H2H and dual-process alerts (starts in {minutes_until_start} minutes)...")
            
            # ========================================
            # H2H STREAK ANALYSIS FOR RESCHEDULED EVENT
            # ========================================
            streak_analysis = None
            observations = None
            
            # Check if event has custom_id for H2H analysis
            if event_obj.custom_id:
                try:
                    # Fetch H2H events for this custom_id
                    h2h_response = api_client.get_h2h_events_for_event(event_obj.custom_id)
                    if h2h_response and 'events' in h2h_response:
                        h2h_events = h2h_response['events']
                        
                        # Get team IDs and event details from metadata snapshot (avoids redundant API call)
                        home_team_id = None
                        away_team_id = None
                        competition_slug = None
                        season_id = None
                        season_name = None
                        season_year = None
                        competition_name = None
                        tournament_id = None
                        
                        if metadata_snapshot:
                            home_team_id = metadata_snapshot.get('home_team_id')
                            away_team_id = metadata_snapshot.get('away_team_id')
                            tournament_id = metadata_snapshot.get('tournament_id')
                            competition_name = metadata_snapshot.get('tournament_name')
                            competition_slug = metadata_snapshot.get('competition_slug')
                            season_id = metadata_snapshot.get('season_id')
                            season_name = metadata_snapshot.get('season_name')
                            season_year = metadata_snapshot.get('season_year')
                            logger.debug(f"Extracted metadata from snapshot for rescheduled event: Home={home_team_id}, Away={away_team_id}, tournament={tournament_id}")
                            
                            # Tennis rankings from snapshot
                            if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                                has_rankings = False
                                if observations:
                                    has_rankings = any(obs.get('type') == 'rankings' for obs in observations)
                                if not has_rankings:
                                    home_team_ranking = metadata_snapshot.get('home_team_ranking')
                                    away_team_ranking = metadata_snapshot.get('away_team_ranking')
                                    if observations is None:
                                        observations = []
                                    observations.append({"type": "rankings", "home_ranking": home_team_ranking, "away_ranking": away_team_ranking})
                                    logger.info(f"✅ Added rankings from snapshot for rescheduled event {event_obj.id}")
                        else:
                            # Fallback: No snapshot available, use API call
                            try:
                                event_details = api_client.get_event_details(event_obj.id)
                                if event_details:
                                    if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                                        has_rankings = False
                                        if observations:
                                            has_rankings = any(obs.get('type') == 'rankings' for obs in observations)
                                        if not has_rankings:
                                            home_team_ranking = event_details.get('homeTeam', {}).get('ranking')
                                            away_team_ranking = event_details.get('awayTeam', {}).get('ranking')
                                            if observations is None:
                                                observations = []
                                            observations.append({"type": "rankings", "home_ranking": home_team_ranking, "away_ranking": away_team_ranking})
                                            logger.info(f"✅ Added rankings from API fallback for rescheduled event {event_obj.id}")
                                    home_team_id = event_details.get('homeTeam', {}).get('id')
                                    away_team_id = event_details.get('awayTeam', {}).get('id')
                                    competition_name = event_details.get('tournament', {}).get('name')
                                    competition_slug = event_details.get('tournament', {}).get('uniqueTournament', {}).get('slug')
                                    tournament_id = event_details.get('tournament', {}).get('id')
                                    season_id = str(event_details.get('season', {}).get('id', '')) if event_details.get('season', {}).get('id') else None
                                    season_name = event_details.get('season', {}).get('name')
                                    season_year_raw = event_details.get('season', {}).get('year')
                                    from repository import SeasonRepository
                                    season_year = SeasonRepository._parse_year(season_year_raw) if season_year_raw else None
                                else:
                                    logger.warning(f"Could not fetch event details for rescheduled event {event_obj.id}")
                            except Exception as e:
                                logger.error(f"Error fetching event details for rescheduled event {event_obj.id}: {e}")
                        
                        # Ensure observations for tennis events
                        tennis_observations = observations if observations else []
                        
                        if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                            has_ground_type = any(obs.get('type') == 'ground_type' for obs in tennis_observations)
                            
                            if not has_ground_type:
                                logger.info(f"🎾 Getting ground_type for tennis rescheduled event {event_obj.id}")
                                if not sport_observations_manager.has_observations_for_event(event_obj.id):
                                    # Try snapshot observations first
                                    if metadata_snapshot and metadata_snapshot.get('observations'):
                                        for obs in metadata_snapshot['observations']:
                                            if obs.get('type') == 'ground_type':
                                                tennis_observations.append(obs)
                                                ObservationRepository.upsert_observation(event_obj.id, event_obj.sport, 'ground_type', obs['value'])
                                                logger.info(f"🎾 Ground type from snapshot for rescheduled event: {obs['value']}")
                                    else:
                                        new_observations = api_client.get_event_results(
                                            event_id=event_obj.id,
                                            update_court_type=True
                                        )
                                        if new_observations:
                                            for obs in new_observations:
                                                if obs.get('type') == 'ground_type':
                                                    tennis_observations.append(obs)
                                                elif obs.get('type') == 'rankings':
                                                    existing_rankings = next((o for o in tennis_observations if o.get('type') == 'rankings'), None)
                                                    if not existing_rankings:
                                                        tennis_observations.append(obs)
                                                        logger.info(f"✅ Added rankings from results check for rescheduled event {event_obj.id}")
                                                    else:
                                                        if existing_rankings.get('home_ranking') is None and existing_rankings.get('away_ranking') is None:
                                                            if obs.get('home_ranking') is not None or obs.get('away_ranking') is not None:
                                                                existing_rankings['home_ranking'] = obs.get('home_ranking')
                                                                existing_rankings['away_ranking'] = obs.get('away_ranking')
                                                                logger.info(f"✅ Updated rankings for rescheduled event {event_obj.id}")
                                else:
                                    observation = ObservationRepository.get_observation(event_obj.id, 'ground_type')
                                    if observation:
                                        tennis_observations.append({'type': 'ground_type', 'value': observation.observation_value})
                                        logger.info(f"🎾 Using existing ground_type observation for rescheduled event: {observation.observation_value}")
                            
                            # Ensure rankings exist in tennis_observations
                            has_rankings = any(obs.get('type') == 'rankings' for obs in tennis_observations)
                            if not has_rankings and metadata_snapshot:
                                home_team_ranking = metadata_snapshot.get('home_team_ranking')
                                away_team_ranking = metadata_snapshot.get('away_team_ranking')
                                if home_team_ranking is not None or away_team_ranking is not None:
                                    tennis_observations.append({"type": "rankings", "home_ranking": home_team_ranking, "away_ranking": away_team_ranking})
                                    logger.info(f"✅ Added rankings from snapshot for rescheduled tennis event {event_obj.id}")
                        
                        # Analyze H2H events with team results
                        from streak_alerts import streak_alert_engine
                        streak_analysis = streak_alert_engine.analyze_h2h_events(
                            event_id=event_obj.id,
                            event_custom_id=event_obj.custom_id,
                            event_start_time=event_obj.start_time_utc,
                            sport=event_obj.sport,
                            discovery_source=event_obj.discovery_source,
                            tournament_id=tournament_id,
                            competition_name=competition_name,
                            competition_slug=competition_slug,
                            season_id=season_id,
                            season_name=season_name,
                            season_year=season_year,
                            observations=tennis_observations,
                            participants=f"{event_obj.home_team} vs {event_obj.away_team}",
                            home_team_name=event_obj.home_team,
                            away_team_name=event_obj.away_team,
                            h2h_events=h2h_events,
                            minutes_until_start=minutes_until_start,
                            home_team_id=home_team_id,
                            away_team_id=away_team_id,
                            event_odds=event_obj.event_odds
                        )
                        
                        if streak_analysis and streak_alert_engine.should_send_streak_alert(streak_analysis):
                            logger.info(f"✅ H2H streak analysis completed for rescheduled event {event_obj.id}: {streak_analysis.current_streak}")
                        else:
                            logger.debug(f"⏭️ No H2H streak alert for rescheduled event {event_obj.id}")
                    else:
                        logger.debug(f"No H2H data found for rescheduled event {event_obj.id}")
                except Exception as e:
                    logger.error(f"Error analyzing H2H streak for rescheduled event {event_obj.id}: {e}")
            else:
                logger.debug(f"Rescheduled event {event_obj.id} has no custom_id - skipping H2H")
            
            # ========================================
            # DUAL PROCESS ANALYSIS FOR RESCHEDULED EVENT
            # ========================================
            
            # Only process dual process for events with discovery_source='dropping_odds'
            dual_report = None
            should_send = False
            reason = ""
            
            discovery_source = getattr(event_obj, 'discovery_source', None)
            if discovery_source != 'dropping_odds':
                logger.info(f"⏭️ Skipping dual process for rescheduled event {event_obj.id} - discovery_source='{discovery_source}' (only processing 'dropping_odds')")
            else:
                # Enrich event object with court type for Tennis/Tennis Doubles events
                event_obj.court_type = None  # Default value
                if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                    observation = ObservationRepository.get_observation(event_obj.id, 'ground_type')
                    if observation:
                        event_obj.court_type = observation.observation_value
                        logger.info(f"🎾 Court type for rescheduled event {event_obj.id}: {event_obj.court_type}")
                    else:
                        logger.info(f"🎾 No court type found for rescheduled event {event_obj.id}")
                
                # Use the same dual-process evaluation as normal events with CORRECTED timing
                from prediction_engine import prediction_engine
                dual_report = prediction_engine.evaluate_dual_process(event_obj, minutes_until_start)
                
                # Only send if at least one process has a prediction OR if Process 1 found candidates
                # This ensures we show Process 1 findings even when no clear prediction is made
                if dual_report.process1_prediction or dual_report.process2_prediction:
                    should_send = True
                    reason = f"Process1={bool(dual_report.process1_prediction)}, Process2={bool(dual_report.process2_prediction)}"
                elif dual_report.process1_report and dual_report.process1_status in ['partial', 'no_match', 'no_candidates']:
                    # Process 1 found candidates but no clear prediction - still show the report
                    should_send = True
                    reason = f"Process1 found candidates (status: {dual_report.process1_status})"
            
            # ========================================
            # SEND ALERTS FOR RESCHEDULED EVENT (H2H + DUAL IN PAIR)
            # ========================================
            
            # Send H2H streak alert if available
            if streak_analysis:
                from streak_alerts import streak_alert_engine
                if streak_alert_engine.should_send_streak_alert(streak_analysis):
                    logger.info(f"📊 Sending H2H streak alert for rescheduled event {event.id}")
                    pre_start_notifier.send_h2h_streak_alerts([streak_analysis])
            
            # Send dual process alert if available
            if should_send:
                logger.info(f"✅ Dual process report generated for rescheduled event {event.id}: {reason}")
                logger.info(f"📊 Sending dual process alert for rescheduled event {event.id}")
                # Send dual process alerts using the same method as normal events
                self._send_dual_process_alerts([dual_report])
                
                # Log predictions for successful Process 1 reports (same logic as normal events)
                from modules.prediction import prediction_logger
                if (dual_report.process1_report and 
                    dual_report.process1_report.get('status') == 'success' and
                    minutes_until_start == 0):  # Only log at 0 minutes
                    success = prediction_logger.log_prediction(event_obj, dual_report.process1_report)
                    if success:
                        logger.info(f"✅ Prediction logged for rescheduled event {event.id} (0 minutes from start)")
                    else:
                        # Check if it's a duplicate (already exists) vs. actual failure
                        from models import PredictionLog
                        with db_manager.get_session() as session:
                            existing = session.query(PredictionLog).filter_by(event_id=event.id).first()
                            if existing:
                                logger.info(f"ℹ️ Prediction already exists for rescheduled event {event.id} - no action needed")
                            else:
                                logger.warning(f"❌ Failed to log prediction for rescheduled event {event.id}")
            else:
                logger.debug(f"⏭️ No dual process report generated for rescheduled event {event.id}: No predictions or candidates found")
                
        except Exception as e:
            logger.error(f"Error processing dual-process alerts for rescheduled event {event.id}: {e}")
            
    def job_results_collection(self):
        """Job E: Collect results for finished events from the previous day"""
        logger.info("Starting Job E: Results collection for finished events")
        
        try:
            yesterday = datetime.now() - timedelta(days=1)
            events = EventRepository.get_events_by_date(yesterday)
            
            if not events:
                logger.info("No events found from previous day")
                return
            
            # Update final odds for all events from previous day
            odds_updated_count = 0
            for event_data in events:
                try:
                    final_odds_response = api_client.get_event_final_odds(event_data.id, event_data.slug)
                    if final_odds_response:
                        final_odds_data = api_client.extract_final_odds_from_response(final_odds_response, initial_odds_extraction=True)
                        if final_odds_data:
                            upserted_id = OddsRepository.upsert_event_odds(event_data.id, final_odds_data)
                            if upserted_id:
                                snapshot = OddsRepository.create_odds_snapshot(event_data.id, final_odds_data)
                                if snapshot:
                                    odds_updated_count += 1
                                    logger.info(f"✅ Final odds updated for {event_data.home_team} vs {event_data.away_team}")
                                
                                # Save all markets to new markets/market_choices tables
                                # This runs for ALL sports (same as pre-start check)
                                try:
                                    from repository import MarketRepository
                                    MarketRepository.save_markets_from_response(event_data.id, final_odds_response)
                                except Exception as e:
                                    logger.warning(f"Error saving markets to DB for event {event_data.id}: {e}")
                            else:
                                logger.warning(f"Failed to update final odds for event {event_data.id}")
                        else:
                            logger.warning(f"No final odds data extracted for event {event_data.id}")
                    else:
                        logger.debug(f"No final odds response for event {event_data.id}")
                except Exception as e:
                    logger.warning(f"Error updating odds for event {event_data.id}: {e}")
                    continue
            
            logger.info(f"📊 Final odds updated for {odds_updated_count}/{len(events)} events")

            logger.info(f"Processing {len(events)} events from previous day")
            stats = self._collect_results_for_events(events, "Job E")
            logger.info(f"Job E completed: {stats['updated']} updated, {stats['skipped']} skipped, {stats['failed']} failed")
            
        except Exception as e:
            logger.error(f"Error in Job E: {e}")

    def _collect_results_for_events(self, events: List, job_name: str = "Results Collection") -> Dict[str, int]:
        """Helper method to collect results for a list of events."""
        stats = {'updated': 0, 'skipped': 0, 'failed': 0}
        
        for event in events:
            try:
                if ResultRepository.get_result_by_event_id(event.id):
                    logger.info(f"Results exist for event {event.id}, skipping")
                    stats['skipped'] += 1
                    continue
                
                result_data = api_client.get_event_results(event.id)
                if not result_data:
                    stats['failed'] += 1
                    continue
                
                # MAIN RESULT PROCESSING (unchanged)
                if ResultRepository.upsert_result(event.id, result_data):
                    stats['updated'] += 1
                    logger.info(f"✅ {job_name}: {event.id} = {result_data['home_score']}-{result_data['away_score']}, Winner: {result_data['winner']}")
                    
                    # OPTIONAL: Process observations (FAIL-SAFE - doesn't break main flow)
                    
                    sport_observations_manager.process_event_observations(event, result_data)
                
            except Exception as e:
                logger.error(f"Error in {job_name} for event {event.id}: {e}")
                stats['failed'] += 1
        
        return stats
    

    def job_results_collection_all_finished(self):
        """Job E2: Comprehensive results collection for ALL finished events."""
        logger.info("Starting Job E2: Comprehensive results collection")
        
        try:
            events = EventRepository.get_all_finished_events()
            if not events:
                logger.info("No finished events found")
                return
            
            logger.info(f"Processing {len(events)} finished events")
            stats = self._collect_results_for_events(events, "Job E2")
            logger.info(f"Job E2 completed: {stats['updated']} updated, {stats['skipped']} skipped, {stats['failed']} failed")
            
        except Exception as e:
            logger.error(f"Error in Job E2: {e}")

    def job_midnight_sync(self):
        """Job D: Midnight results collection for finished events"""
        logger.info("Starting Job D: Midnight results collection")
        
        try:
            # Only collect results from previous day - odds don't change after games finish
            logger.info("📊 Collecting results from finished events...")
            self.job_results_collection()
            

            # Update prediction logs with actual results
            logger.info("📊 Updating prediction logs with actual results...")
            from modules.prediction import prediction_logger
            
            # Use the prediction logging module directly
            stats = prediction_logger.update_predictions_with_results()
            
            if 'error' in stats:
                logger.error(f"Error updating prediction logs: {stats['error']}")
            else:
                logger.info(f"📊 Prediction logs updated: {stats['updated']} completed, {stats['cancelled']} cancelled")
            
            # Refresh materialized views for alerts after results are updated
            logger.info("🔄 Refreshing alert materialized views...")
            from models import refresh_materialized_views
            from database import db_manager
            refresh_materialized_views(db_manager.engine)
            logger.info("✅ Alert data refreshed")
            
        except Exception as e:
            logger.error(f"Error in Job D: {e}")
    
    def job_daily_discovery(self):
        """Job E: Daily discovery of today's scheduled events with odds (runs at 05:01)"""
        logger.info("Starting Job E: Daily discovery of today's scheduled events")
        
        # Clean up yesterday's OddsPortal league cache
        try:
            from repository import OddsPortalCacheRepository
            OddsPortalCacheRepository.cleanup_old_caches()
        except Exception as e:
            logger.warning(f"⚠️ Failed to cleanup OddsPortal cache: {e}")
        
        try:
            # Run the daily discovery extractor
            stats = run_daily_discovery()
            
            if stats:
                logger.info(f"✅ Daily discovery completed successfully: {stats}")
            else:
                logger.warning("Daily discovery completed with no results")
            
        except Exception as e:
            logger.error(f"Error in Job E (Daily Discovery): {e}")
    
    def run_job_discovery_now(self):
        """Run Job A immediately"""
        logger.info("Running Job A immediately")
        self.job_discovery()
    
    def run_job_discovery2_now(self):
        """Run Job B immediately"""
        logger.info("Running Job B immediately")
        self.job_discovery2()
    
    def run_job_pre_start_check_now(self):
        """Run Job C immediately"""
        logger.info("Running Job C immediately")
        self.job_pre_start_check()
        
        # Wait for the OP worker thread if we are running in one-off execution
        if hasattr(self, '_active_op_thread') and self._active_op_thread and self._active_op_thread.is_alive():
            logger.info("⏳ Waiting for OddsPortal background worker to finish before exiting...")
            self._active_op_thread.join()
            logger.info("✅ OddsPortal background worker finished.")
    
    def run_job_midnight_sync_now(self):
        """Run Job D immediately"""
        logger.info("Running Job D immediately")
        self.job_midnight_sync()
    
    def run_job_results_collection_now(self):
        """Run Job E immediately"""
        logger.info("Running Job E immediately")
        self.job_results_collection()

    def job_results_collection_for_date(self, target_date):
        """Job E (date-specific): Collect results for finished events from a specific date"""
        logger.info(f"Starting results collection for date: {target_date}")
        
        try:
            events = EventRepository.get_events_by_date(target_date)
            
            if not events:
                logger.info(f"No events found for {target_date}")
                return
            
            # Update final odds for all events from the target date
            odds_updated_count = 0
            for event_data in events:
                try:
                    final_odds_response = api_client.get_event_final_odds(event_data.id, event_data.slug)
                    if final_odds_response:
                        final_odds_data = api_client.extract_final_odds_from_response(final_odds_response, initial_odds_extraction=True)
                        if final_odds_data:
                            upserted_id = OddsRepository.upsert_event_odds(event_data.id, final_odds_data)
                            if upserted_id:
                                snapshot = OddsRepository.create_odds_snapshot(event_data.id, final_odds_data)
                                if snapshot:
                                    odds_updated_count += 1
                                    logger.info(f"✅ Final odds updated for {event_data.home_team} vs {event_data.away_team}")
                                
                                # Save all markets to new markets/market_choices tables
                                try:
                                    from repository import MarketRepository
                                    MarketRepository.save_markets_from_response(event_data.id, final_odds_response)
                                except Exception as e:
                                    logger.warning(f"Error saving markets to DB for event {event_data.id}: {e}")
                            else:
                                logger.warning(f"Failed to update final odds for event {event_data.id}")
                        else:
                            logger.warning(f"No final odds data extracted for event {event_data.id}")
                    else:
                        logger.debug(f"No final odds response for event {event_data.id}")
                except Exception as e:
                    logger.warning(f"Error updating odds for event {event_data.id}: {e}")
                    continue
            
            logger.info(f"📊 Final odds updated for {odds_updated_count}/{len(events)} events")

            logger.info(f"Processing {len(events)} events from {target_date}")
            stats = self._collect_results_for_events(events, f"Results Collection ({target_date})")
            logger.info(f"Results collection for {target_date} completed: {stats['updated']} updated, {stats['skipped']} skipped, {stats['failed']} failed")
            
        except Exception as e:
            logger.error(f"Error in results collection for {target_date}: {e}")

    def run_job_results_collection_for_date_now(self, target_date):
        """Run date-specific results collection immediately"""
        logger.info(f"Running results collection for {target_date} immediately")
        self.job_results_collection_for_date(target_date)
    
    def run_job_results_collection_all_now(self):
        """Run Job E2 immediately"""
        logger.info("Running Job E2 immediately")
        self.job_results_collection_all_finished()
    
    def run_job_daily_discovery_now(self):
        """Run Job E (Daily Discovery) immediately"""
        logger.info("Running Job E (Daily Discovery) immediately")
        self.job_daily_discovery()
    
    def get_scheduled_jobs(self) -> List[Dict]:
        """Get information about scheduled jobs with enhanced formatting"""
        jobs = []
        
        for job in schedule.jobs:
            job_info = {
                'function': job.job_func.__name__,
                'interval': str(job.interval),
                'unit': job.unit,
                'at_time': job.at_time,
                'next_run': job.next_run
            }
            
            # Create a more descriptive display format
            if job.job_func.__name__ == 'job_discovery':
                if job.at_time:
                    job_info['display'] = f"Discovery: Daily at {job.at_time}"
                else:
                    job_info['display'] = f"Discovery: Every {job.interval} {job.unit}"
            elif job.job_func.__name__ == 'job_pre_start_check':
                if job.at_time:
                    job_info['display'] = f"Pre-start check (+ NBA 4th quarter): Every 5 minutes at {job.at_time}"
                else:
                    job_info['display'] = f"Pre-start check: Every {job.interval} {job.unit}"
            elif job.job_func.__name__ == 'job_midnight_sync':
                if job.at_time:
                    job_info['display'] = f"Midnight sync: Daily at {job.at_time}"
                else:
                    job_info['display'] = f"Midnight sync: Every {job.interval} {job.unit}"
            elif job.job_func.__name__ == 'job_daily_discovery':
                if job.at_time:
                    job_info['display'] = f"Daily discovery: Daily at {job.at_time}"
                else:
                    job_info['display'] = f"Daily discovery: Every {job.interval} {job.unit}"
            else:
                job_info['display'] = f"{job.job_func.__name__}: Every {job.interval} {job.unit}"
            
            # For pre-start check jobs, calculate the next run time more accurately
            if job.job_func.__name__ == 'job_pre_start_check' and job.at_time:
                job_info['next_run'] = self._calculate_next_pre_start_time(job.at_time)
            
            jobs.append(job_info)
        
        return jobs
    
    def _calculate_next_pre_start_time(self, at_time) -> datetime:
        """Calculate the next pre-start check time based on the current time and the target minute"""
        now = datetime.now()
        
        # Handle both string and time objects
        if isinstance(at_time, str):
            target_minute = int(at_time.split(':')[1])
        elif hasattr(at_time, 'minute'):
            target_minute = at_time.minute
        else:
            # Fallback to current time
            return now + timedelta(minutes=5)
        
        # Find the next occurrence of this minute
        next_time = now.replace(minute=target_minute, second=0, microsecond=0)
        
        # If we've already passed this minute this hour, move to the next hour
        if next_time <= now:
            next_time = next_time + timedelta(hours=1)
        
        return next_time
    
    def _send_dual_process_alerts(self, dual_reports: List) -> None:
        """
        Send dual process alerts via Telegram using enhanced alert system.
        
        Args:
            dual_reports: List of DualProcessReport objects
        """
        try:
            from alert_system import pre_start_notifier
            
            # Use alert_system to send dual process alerts (modular approach)
            success = pre_start_notifier.send_dual_process_alerts(dual_reports)
            
            if success:
                logger.info(f"✅ Dual process alerts sent successfully")
            else:
                logger.warning(f"❌ Failed to send dual process alerts")
                    
        except Exception as e:
            logger.error(f"Error in _send_dual_process_alerts: {e}")
    

# Global scheduler instance
job_scheduler = JobScheduler()