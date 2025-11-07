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

# Import optimization utilities
from optimization import (
    parallel_team_event_fetching,
    process_with_batch_cleanup,
    process_with_parallel_db_ops,
    process_events_only
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
        
        logger.info("Jobs scheduled:")
        logger.info(f"  - Discovery: daily at {', '.join(Config.DISCOVERY_TIMES)}")
        logger.info(f"  - Discovery 2 (streaks, h2h, winning odds): daily at {', '.join(Config.DISCOVERY2_TIMES)}")
        logger.info(f"  - Pre-start check: every {Config.POLL_INTERVAL_MINUTES} minutes")
        logger.info("  - Midnight sync: daily at 04:00 (results collection only)")
    
    def _setup_pre_start_jobs(self):
        """Setup pre-start check jobs every N minutes at exact minute marks (configurable via POLL_INTERVAL_MINUTES)"""
        interval_minutes = Config.POLL_INTERVAL_MINUTES
        
        # Schedule jobs at exact minute marks based on interval
        # For 5 minutes: 00, 05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55
        # For 1 minute: 00, 01, 02, 03, ..., 59 (all 60 minutes)
        for minute in range(0, 60, interval_minutes):
            schedule.every().hour.at(f":{minute:02d}").do(self.job_pre_start_check)
        
        logger.info(f"  - Pre-start check scheduled every {interval_minutes} minutes at exact minute marks (checks upcoming events + timestamp corrections for recently started events)")
    
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
            if not h2h_events:
                logger.warning("No events found in h2h events")
                return

            # Winning odds events
            winning_odds_events_response = api_client.get_winning_odds_events()
            if not winning_odds_events_response:
                logger.error("Failed to get winning odds events")
                return

            winning_odds_events, winning_odds_events_odds_map = api_client.extract_events_and_odds_from_dropping_response(winning_odds_events_response, odds_extraction=True, discovery_source='winning_odds')
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
        
        try:
            # Get dropping odds with odds data in a single call
            response = api_client.get_dropping_odds_with_odds()
            if not response:
                logger.error("Failed to get dropping odds with odds data")
                return
            
            # Save API response to JSON file for debugging
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_filename = os.path.join("debug", f"debug_discovery_{timestamp}.json")
            try:
                os.makedirs("debug", exist_ok=True)  # ensure folder exists
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(response, f, indent=2, ensure_ascii=False)
                logger.debug(f"API response saved to {json_filename}")
            except Exception as e:
                logger.warning(f"Failed to save JSON debug file: {e}")
            
            # Extract events and odds data with discovery_source
            events, odds_map = api_client.extract_events_and_odds_from_dropping_response(response, odds_extraction=True, discovery_source='dropping_odds')
            if not events:
                logger.warning("No events found in dropping odds")
                return
            logger.info(f"Found {len(events)} events in dropping odds")

            # Use optimized parallel DB ops with 10 workers (odds pre-fetched in odds_map)
            processed_count, skipped_count = process_with_parallel_db_ops(
                events,
                odds_map,
                discovery_source='dropping_odds',
                max_workers=10
            )
            logger.info(f"Job A completed: processed {processed_count}/{len(events)} events, skipped {skipped_count} events")
            
            
        except Exception as e:
            logger.error(f"Error in Job A: {e}")
    
    def job_pre_start_check(self):
        """
        Job C: Pre-start check for events starting within 30 minutes
        
        SMART ODDS EXTRACTION: Only extracts odds at key moments (30 min and 5 min before start)
        to avoid unnecessary API calls when odds don't change significantly.
        
        SMART NOTIFICATIONS: Only sends Telegram notifications when odds are extracted at key moments
        (30 min and 5 min), but includes ALL upcoming games in those notifications to avoid missing games.
        
        TIMESTAMP CORRECTION: Also checks events that started within the last 5 minutes for late
        timestamp corrections that may occur after the game starts.
        """
        logger.info("🚨 PRE-START CHECK EXECUTED at " + datetime.now().strftime("%H:%M:%S"))
        
        
        try:
            # STEP 1: Check recently started events for timestamp corrections
            # Tennis events: 60 minutes window (they change timestamps frequently up to 1 hour after start)
            # Other sports: 15 minutes window (sufficient for most sports)
            events_started_recently = EventRepository.get_events_started_recently(window_minutes=60)
            if events_started_recently:
                logger.info(f"Found {len(events_started_recently)} events that started recently (checking for late timestamp corrections)")
                self._check_recently_started_events_for_timestamp_corrections(events_started_recently)
            
            # STEP 2: Get events starting within the next 30 minutes WITH their odds data
            events_with_odds = EventRepository.get_events_starting_soon_with_odds(Config.PRE_START_WINDOW_MINUTES)
            if not events_with_odds:
                logger.debug("No events starting within the next 30 minutes")
                return
            
            logger.info(f"Found {len(events_with_odds)} events starting within {Config.PRE_START_WINDOW_MINUTES} minutes")
            
            # STEP 2.1: CAPTURE ALL TIMING DECISIONS UPFRONT (before any API calls)
            # This prevents events from slipping out of key moment windows due to slow API calls
            events_to_process = []
            for event_data in events_with_odds:
                minutes_until_start = self._minutes_until_start(event_data['start_time_utc'])
                logger.info(f"🚨 UPCOMING GAME ALERT: {event_data['home_team']} vs {event_data['away_team']} starts in {minutes_until_start} minutes")
                
                # Pre-compute timing decision (capture at current time, not after API delays)
                should_extract_odds = self._should_extract_odds_for_event(event_data['id'], minutes_until_start)
                
                events_to_process.append({
                    'event_data': event_data,
                    'minutes_until_start': minutes_until_start,
                    'should_extract_odds': should_extract_odds
                })
            
            # STEP 2.2: EXECUTE ALL ODDS EXTRACTIONS (with pre-computed decisions)
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
                    
                    # SMART ODDS EXTRACTION: Only extract odds at key moments (30 min and 5 min)                  
                    if should_extract_odds:
                        logger.info(f"🎯 EXTRACTING ODDS: {event_data['home_team']} vs {event_data['away_team']} - {minutes_until_start} min until start")
                        
                        # Fetch final odds for this upcoming game using specific event endpoint
                        final_odds_response = api_client.get_event_final_odds(event_data['id'], event_data['slug'])
                        
                        if final_odds_response:
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
                                        
                                        # Track this event for alert evaluation (capture timing when odds were extracted)
                                        events_with_odds_extracted.append({
                                            'event_id': event_data['id'],
                                            'start_time': event_data['start_time_utc'],
                                            'minutes_until_start': minutes_until_start
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
                                # No observations exist, proceed with API call to extract court type
                                observations = api_client.get_event_results(
                                    event_id=event_data['id'],
                                    update_court_type=True
                                )
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
                    logger.info(f"⏭️ No odds extracted (smart extraction: only at 30min and 5min)")
            else:
                logger.info("Pre-start check completed: No games starting soon")
            
            # Run alert evaluation for events at key moments (30 or 5 minutes)
            # This ensures alerts are sent when odds are extracted OR when odds extraction is disabled for testing
            should_evaluate_alerts = events_with_odds and (odds_extracted_count > 0 or not Config.ENABLE_ODDS_EXTRACTION)
            if should_evaluate_alerts:
                try:
                    if not Config.ENABLE_ODDS_EXTRACTION:
                        logger.info("🔍 Evaluating upcoming events for pattern alerts (odds extraction disabled for testing)...")
                    else:
                        logger.info("🔍 Evaluating upcoming events for pattern alerts...")
                    
                    # Refresh materialized views to ensure latest data for alert evaluation
                    logger.info("🔄 Refreshing alert materialized views...")
                    
                    refresh_materialized_views(db_manager.engine)
                    logger.info("✅ Alert materialized views refreshed")
                    
                    
                    # Get Event objects with properly loaded event_odds for alert evaluation
                    # Use tracked events with odds extracted to prevent timing drift issues
                    events_for_alerts = []
                    
                    if events_with_odds_extracted:
                        # Use tracked events (events where odds were successfully extracted)
                        logger.info(f"🔍 Using tracked events for alert evaluation ({len(events_with_odds_extracted)} events with odds extracted)")
                        
                        for tracked_event in events_with_odds_extracted:
                            # Get the Event object to ensure we have fresh DB state
                            event_obj = self.event_repo.get_event_by_id(tracked_event['event_id'])
                            if not event_obj:
                                logger.warning(f"Could not find event {tracked_event['event_id']} for alert evaluation")
                                continue
                            
                            # CRITICAL: Check if event was rescheduled after odds extraction
                            if event_obj.start_time_utc != tracked_event['start_time']:
                                logger.warning(f"⏭️ Event {tracked_event['event_id']} was rescheduled after odds extraction - skipping alert evaluation")
                                continue
                            
                            # Skip if already processed as rescheduled in this cycle
                            if event_obj.id in self.recently_rescheduled:
                                logger.debug(f"⏭️ Skipping event {event_obj.id} - already rescheduled in this cycle")
                                continue
                            
                            # Use the minutes from when odds were extracted (prevents timing drift)
                            events_for_alerts.append(event_obj)
                    else:
                        # Fallback: Use original logic if no events with odds extracted
                        logger.info("🔍 No tracked events found - using fallback logic for alert evaluation")
                        
                        for event_data in events_with_odds:
                            # Get the Event object first to ensure we have fresh DB state
                            event_obj = self.event_repo.get_event_by_id(event_data['id'])
                            if not event_obj:
                                continue
                            
                            # Skip if rescheduled in this cycle
                            if event_obj.id in self.recently_rescheduled:
                                logger.debug(f"⏭️ Skipping event {event_obj.id} - already rescheduled in this cycle")
                                continue
                            
                            # Calculate minutes from fresh DB time, not stale event_data
                            minutes_until_start = self._minutes_until_start(event_obj.start_time_utc)
                            if minutes_until_start in [30, 5]:  # Key moments for odds extraction
                                events_for_alerts.append(event_obj)
                    
                    if events_for_alerts:
                        logger.info(f"🔍 Evaluating {len(events_for_alerts)} events at key moments for H2H and dual process alerts...")
                        
                        # ========================================
                        # PROCESS EACH EVENT INDIVIDUALLY (H2H + DUAL ALERTS IN PAIRS)
                        # ========================================
                        try:
                            from streak_alerts import streak_alert_engine
                            
                            for event_obj in events_for_alerts:
                                try:
                                    # Initialize observations per event to avoid cross-event contamination
                                    observations = None
                                    
                                    logger.info(f"🔍 Processing event {event_obj.id}: {event_obj.home_team} vs {event_obj.away_team}")
                                    
                                    # Calculate minutes from FRESH database time, not stale event data
                                    minutes_until_start = self._minutes_until_start(event_obj.start_time_utc)
                                    
                                    # ========================================
                                    # H2H STREAK ANALYSIS FOR THIS EVENT
                                    # ========================================
                                    streak_analysis = None
                                    
                                    # Check if event has custom_id for H2H analysis
                                    if event_obj.custom_id:
                                        try:
                                            # Fetch H2H events for this custom_id
                                            h2h_response = api_client.get_h2h_events_for_event(event_obj.custom_id)
                                            if h2h_response and 'events' in h2h_response:
                                                h2h_events = h2h_response['events']
                                                
                                                # Get team IDs from the current event details (not H2H response)
                                                home_team_id = None
                                                away_team_id = None
                                                competition_slug = None
                                                competition_name = None
                                                event_details = None  # Inicializar para evitar errores si falla el try
                                                
                                                try:
                                                    # Fetch current event details to get correct team IDs
                                                    event_details = api_client.get_event_details(event_obj.id)

                                                    if event_details:
                                                        # Para Tennis/Tennis Doubles, agregar rankings a observations si no existen
                                                        if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                                                            # Verificar si ya existe rankings en observations
                                                            has_rankings = False
                                                            if observations:
                                                                has_rankings = any(obs.get('type') == 'rankings' for obs in observations)
                                                            
                                                            # Si no tiene rankings, extraerlos de event_details y agregarlos
                                                            if not has_rankings:
                                                                home_team_ranking = event_details.get('homeTeam', {}).get('ranking')
                                                                away_team_ranking = event_details.get('awayTeam', {}).get('ranking')
                                                                
                                                                # Inicializar observations si es None
                                                                if observations is None:
                                                                    observations = []
                                                                
                                                                observations.append({"type": "rankings", "home_ranking": home_team_ranking, "away_ranking": away_team_ranking})
                                                                logger.info(f"✅ Added rankings to observations for event {event_obj.id}: home={home_team_ranking}, away={away_team_ranking}")
                                                            else:
                                                                logger.debug(f"Rankings already exist in observations for event {event_obj.id}")
                                                        
                                                        # Extraer team IDs y otros datos del evento
                                                        home_team_id = event_details.get('homeTeam', {}).get('id')
                                                        away_team_id = event_details.get('awayTeam', {}).get('id')
                                                        competition_name = event_details.get('tournament', {}).get('uniqueTournament', {}).get('name')
                                                        competition_slug = event_details.get('tournament', {}).get('uniqueTournament', {}).get('slug')
                                                        logger.debug(f"Extracted team IDs from current event: Home={home_team_id}, Away={away_team_id}")
                                                    else:
                                                        logger.warning(f"Could not fetch event details for {event_obj.id}")
                                                except Exception as e:
                                                    logger.error(f"Error fetching event details for {event_obj.id}: {e}")
                                                
                                                # Ensure observations are available for tennis events (needed for ground_type filtering)
                                                # Start with observations if they exist (might already have rankings)
                                                tennis_observations = observations if observations else []
                                                
                                                if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                                                    # Check if ground_type already exists in tennis_observations
                                                    has_ground_type = any(obs.get('type') == 'ground_type' for obs in tennis_observations)
                                                    
                                                    if not has_ground_type:
                                                        logger.info(f"🎾 Getting ground_type for tennis event {event_obj.id} for filtering")
                                                        if not sport_observations_manager.has_observations_for_event(event_obj.id):
                                                            # No observations exist in DB, make API call to extract court type
                                                            new_observations = api_client.get_event_results(
                                                                event_id=event_obj.id,
                                                                update_court_type=True
                                                            )
                                                            if new_observations:
                                                                # Merge new observations with existing tennis_observations
                                                                for obs in new_observations:
                                                                    if obs.get('type') == 'ground_type':
                                                                        tennis_observations.append(obs)
                                                        else:
                                                            # Get existing ground_type from database
                                                            observation = ObservationRepository.get_observation(event_obj.id, 'ground_type')
                                                            if observation:
                                                                tennis_observations.append({'type': 'ground_type', 'value': observation.observation_value})
                                                                logger.info(f"🎾 Using existing ground_type observation: {observation.observation_value}")
                                                    
                                                    # Ensure rankings exist in tennis_observations
                                                    has_rankings = any(obs.get('type') == 'rankings' for obs in tennis_observations)
                                                    if not has_rankings and event_details:
                                                        # Add rankings from event_details if available
                                                        home_team_ranking = event_details.get('homeTeam', {}).get('ranking')
                                                        away_team_ranking = event_details.get('awayTeam', {}).get('ranking')
                                                        if home_team_ranking is not None or away_team_ranking is not None:
                                                            tennis_observations.append({"type": "rankings", "home_ranking": home_team_ranking, "away_ranking": away_team_ranking})
                                                            logger.info(f"✅ Added rankings to tennis_observations for event {event_obj.id}: home={home_team_ranking}, away={away_team_ranking}")
                                                
                                                # Log observations antes de pasar a analyze_h2h_events
                                                if tennis_observations:
                                                    rankings_info = next((obs for obs in tennis_observations if isinstance(obs, dict) and obs.get('type') == 'rankings'), None)
                                                    if rankings_info:
                                                        logger.info(f"📊 Passing observations to analyze_h2h_events for event {event_obj.id}: home_ranking={rankings_info.get('home_ranking')}, away_ranking={rankings_info.get('away_ranking')}")
                                                    else:
                                                        logger.warning(f"⚠️ No rankings found in tennis_observations for event {event_obj.id}")
                                                else:
                                                    logger.warning(f"⚠️ tennis_observations is None for event {event_obj.id}")
                                                
                                                # Analyze H2H events with team results
                                                streak_analysis = streak_alert_engine.analyze_h2h_events(
                                                    event_id=event_obj.id,
                                                    event_custom_id=event_obj.custom_id,
                                                    event_start_time=event_obj.start_time_utc,
                                                    sport=event_obj.sport,
                                                    discovery_source=event_obj.discovery_source,
                                                    competition_name=competition_name,
                                                    competition_slug=competition_slug,
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
                                                    logger.info(f"✅ H2H streak analysis completed for event {event_obj.id}: {streak_analysis.current_streak}")
                                                else:
                                                    logger.debug(f"⏭️ No H2H streak alert for event {event_obj.id}")
                                            else:
                                                logger.debug(f"No H2H data found for event {event_obj.id} (custom_id: {event_obj.custom_id})")
                                        except Exception as e:
                                            logger.error(f"Error analyzing H2H streak for event {event_obj.id}: {e}")
                                    else:
                                        logger.debug(f"Event {event_obj.id} has no custom_id - skipping H2H streak analysis")
                                    
                                    # ========================================
                                    # DUAL PROCESS ANALYSIS FOR THIS EVENT
                                    # ========================================
                                    dual_report = None
                                    
                                    try:
                                        # Enrich event object with court type for Tennis/Tennis Doubles events
                                        event_obj.court_type = None  # Default value
                                        if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                                            observation = ObservationRepository.get_observation(event_obj.id, 'ground_type')
                                            if observation:
                                                event_obj.court_type = observation.observation_value
                                                logger.info(f"🎾 Court type for event {event_obj.id}: {event_obj.court_type}")
                                            else:
                                                logger.info(f"🎾 No court type found for event {event_obj.id}")
                                        
                                        dual_report = prediction_engine.evaluate_dual_process(event_obj, minutes_until_start)
                                        
                                        # Only add dual report if at least one process has a prediction OR if Process 1 found candidates
                                        # This ensures we show Process 1 findings even when no clear prediction is made
                                        should_send = False
                                        reason = ""
                                        
                                        if dual_report.process1_prediction or dual_report.process2_prediction:
                                            should_send = True
                                            reason = f"Process1={bool(dual_report.process1_prediction)}, Process2={bool(dual_report.process2_prediction)}"
                                        elif dual_report.process1_report and dual_report.process1_status in ['partial', 'no_match', 'no_candidates']:
                                            # Process 1 found candidates but no clear prediction - still show the report
                                            should_send = True
                                            reason = f"Process1 found candidates (status: {dual_report.process1_status})"
                                        
                                        if should_send:
                                            logger.info(f"✅ Dual process report added for event {event_obj.id}: {reason}")
                                        else:
                                            logger.debug(f"⏭️ Skipping dual process report for event {event_obj.id}: No predictions or candidates found")
                                            
                                    except Exception as e:
                                        logger.error(f"Error running dual process evaluation for event {event_obj.id}: {e}")
                                        
                                        # Fallback to Process 1 only if dual process fails for this event
                                        logger.info(f"🔄 Falling back to Process 1 only for event {event_obj.id}...")
                                        try:
                                            alerts = alert_engine.evaluate_upcoming_events([event_obj])
                                            if alerts:
                                                logger.info(f"📊 Generated {len(alerts)} Process 1 candidate reports (fallback) for event {event_obj.id}")
                                                alert_engine.send_alerts(alerts)
                                                
                                                # Log predictions for successful Process 1 reports (fallback)
                                                from modules.prediction import prediction_logger
                                                
                                                for alert in alerts:
                                                    # Only log predictions with success status
                                                    if alert.get('status') == 'success':
                                                        event_id = alert.get('event_id')
                                                        if event_id:
                                                            # Get the event object
                                                            event_obj_fallback = self.event_repo.get_event_by_id(event_id)
                                                            if event_obj_fallback:
                                                                # Check if event is exactly 5 minutes from start
                                                                minutes_until_start_fallback = self._minutes_until_start(event_obj_fallback.start_time_utc)
                                                                if minutes_until_start_fallback == 5:
                                                                    # Log the prediction only at 5 minutes
                                                                    success = prediction_logger.log_prediction(event_obj_fallback, alert)
                                                                    if success:
                                                                        logger.info(f"✅ Prediction logged for Process 1 event {event_id} (5 minutes from start)")
                                                                    else:
                                                                        # Check if it's a duplicate (already exists) vs. actual failure
                                                                        
                                                                        with db_manager.get_session() as session:
                                                                            existing = session.query(PredictionLog).filter_by(event_id=event_id).first()
                                                                            if existing:
                                                                                logger.info(f"ℹ️ Prediction already exists for Process 1 event {event_id} - no action needed")
                                                                            else:
                                                                                logger.warning(f"❌ Failed to log prediction for Process 1 event {event_id}")
                                                                else:
                                                                    logger.info(f"⏭️ Skipping prediction logging for event {event_id} - {minutes_until_start_fallback} minutes until start (not 5 minutes)")
                                                            else:
                                                                logger.warning(f"Could not find event {event_id} for prediction logging")
                                            else:
                                                logger.debug(f"No Process 1 candidate reports generated (fallback) for event {event_obj.id}")
                                        except Exception as e2:
                                            logger.error(f"Error in Process 1 fallback for event {event_obj.id}: {e2}")
                                    
                                    # ========================================
                                    # SEND ALERTS FOR THIS EVENT (H2H + DUAL IN PAIR)
                                    # ========================================
                                    
                                    # Send H2H streak alert if available
                                    if streak_analysis and streak_alert_engine.should_send_streak_alert(streak_analysis):
                                        logger.info(f"📊 Sending H2H streak alert for event {event_obj.id}")
                                        pre_start_notifier.send_h2h_streak_alerts([streak_analysis])
                                    
                                    # Send dual process alert if available
                                    if dual_report and (dual_report.process1_prediction or dual_report.process2_prediction or 
                                                      (dual_report.process1_report and dual_report.process1_status in ['partial', 'no_match', 'no_candidates'])):
                                        logger.info(f"📊 Sending dual process alert for event {event_obj.id}")
                                        self._send_dual_process_alerts([dual_report])
                                        
                                        # Log predictions for successful Process 1 reports
                                        from modules.prediction import prediction_logger
                                        
                                        # Only log predictions from Process 1 with success status
                                        if (dual_report.process1_report and 
                                            dual_report.process1_report.get('status') == 'success'):
                                            
                                            # Check if event is exactly 5 minutes from start
                                            if minutes_until_start == 5:
                                                # Log the prediction only at 5 minutes
                                                success = prediction_logger.log_prediction(event_obj, dual_report.process1_report)
                                                if success:
                                                    logger.info(f"✅ Prediction logged for dual process event {event_obj.id} (5 minutes from start)")
                                                else:
                                                    # Check if it's a duplicate (already exists) vs. actual failure
                                                    
                                                    with db_manager.get_session() as session:
                                                        existing = session.query(PredictionLog).filter_by(event_id=event_obj.id).first()
                                                        if existing:
                                                            logger.info(f"ℹ️ Prediction already exists for dual process event {event_obj.id} - no action needed")
                                                        else:
                                                            logger.warning(f"❌ Failed to log prediction for dual process event {event_obj.id}")
                                            else:
                                                logger.info(f"⏭️ Skipping prediction logging for event {event_obj.id} - {minutes_until_start} minutes until start (not 5 minutes)")
                                    
                                    logger.info(f"✅ Completed processing event {event_obj.id}: {event_obj.home_team} vs {event_obj.away_team}")
                                    
                                except Exception as e:
                                    logger.error(f"Error processing event {event_obj.id}: {e}")
                                    continue
                                
                        except Exception as e:
                            logger.error(f"Error in event processing: {e}")
                    else:
                        logger.debug("No events at key moments found for alert evaluation")
                        
                except Exception as e:
                    logger.error(f"Error running alert evaluation: {e}")
            elif events_with_odds and odds_extracted_count == 0:
                logger.info("📊 NO ALERT EVALUATION: Events found but no odds extracted (not at key moments)")
            else:
                logger.debug("No events found for alert evaluation")
            
            # Note: Prediction logging is handled within the alert evaluation above
            
        except Exception as e:
            logger.error(f"Error in Job C: {e}")
    
    def _check_recently_started_events_for_timestamp_corrections(self, events_started_recently: List[Dict]):
        """
        Check recently started events for timestamp corrections with sport-specific windows.
        
        Tennis/Tennis Doubles: Checks up to 60 minutes after start (every 5 minutes)
        Other sports: Checks up to 15 minutes after start (at 5, 10, 15 minutes)
        
        This catches late changes that occur after the game starts or right at start time.
        Only checks events at specific intervals to avoid timing precision issues.
        
        Args:
            events_started_recently: List of event dictionaries from get_events_started_recently()
        """
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
                        # Other sports: Check only at 5, 10, 15 minutes after start
                        CHECK_INTERVALS = [5, 10, 15]
                        
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
            
        except Exception as e:
            logger.error(f"Error in _check_recently_started_events_for_timestamp_corrections: {e}")
    
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
    
    def _should_extract_odds_for_event(self, event_id: int, minutes_until_start: int) -> bool:
        """
        Smart logic to determine if odds should be extracted for an event.
        Only extract odds at key moments: 30 minutes and 5 minutes before start.
        
        Args:
            event_id: Event ID
            minutes_until_start: Minutes until the event starts
            
        Returns:
            True if odds should be extracted, False otherwise
        """
        # Check if odds extraction is enabled (for testing purposes)
        if not Config.ENABLE_ODDS_EXTRACTION:
            logger.info(f"🚫 ODDS EXTRACTION DISABLED: Skipping odds extraction for event {event_id} (ENABLE_ODDS_EXTRACTION=false)")
            return False
        
        # Key moments for odds extraction (removed 1-minute check - now handled by 5-minutes-after logic)
        KEY_MOMENTS = [30, 5]
        
        # Check if current time aligns with a key moment
        should_extract = minutes_until_start in KEY_MOMENTS
        
        # Only check and update starting time if event is in key moments (optimization)
        if not should_extract:
            logger.debug(f"⏭️ Not a key moment for event {event_id}: {minutes_until_start} minutes until start - SKIPPING API CALL AND ODDS EXTRACTION")
            return False
        
        # Check if timestamp correction is enabled
        if not Config.ENABLE_TIMESTAMP_CORRECTION:
            logger.info(f"🎯 Key moment detected for event {event_id}: {minutes_until_start} minutes until start - WILL EXTRACT ODDS (timestamp correction disabled)")
            return True
        
        # Check and update starting time only for events in key moments
        correct_starting_time = api_client.get_event_results(event_id, update_time=True)
        
        if correct_starting_time:
            logger.info(f"🎯 Key moment detected for event {event_id}: {minutes_until_start} minutes until start - WILL EXTRACT ODDS")
            return True
        elif correct_starting_time is None:
            # API call failed - skip odds extraction but don't trigger rescheduled logic
            logger.warning(f"⏭️ API error for event {event_id} - skipping odds extraction")
            return False
        elif not correct_starting_time:
            # Starting time was actually updated - mark as rescheduled and check if rescheduled game is now in key moments
            logger.info(f"🔄 Starting time changed for event {event_id} - marking as rescheduled and checking if rescheduled game is in key moments")
            self.recently_rescheduled.add(event_id)  # Mark immediately to prevent double processing
            self._check_rescheduled_event(event_id)
            return False
        else:
            logger.debug(f"⏭️ Starting Time changed for event {event_id} - skipping odds extraction or not a key moment")
            return False
    
    def _check_rescheduled_event(self, event_id: int):
        """
        Check if a rescheduled event is now within the 30 or 5 minute window.
        If so, extract odds and process alerts for the rescheduled game.
        Includes complete pre-start job workflow to avoid infinite loops.
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
            
            # Check if rescheduled game is now in key moments (30 or 5 minutes), starting now (0), or has already started (negative)
            # We always process rescheduled events to catch late corrections
            if new_minutes_until_start in [30, 5, 0] or new_minutes_until_start < 0:
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
                                self._process_alerts_for_rescheduled_event(event)
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
    
    def _process_alerts_for_rescheduled_event(self, event):
        """
        Process alerts for a rescheduled event that was just updated with new odds.
        Uses the same H2H + dual-process flow as normal events for consistent alert format.
        This completes the pre-start job workflow to prevent infinite loops.
        """
        try:
            # Initialize observations for this rescheduled event
            observations = None
            
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
                        
                        # Get team IDs and event details
                        home_team_id = None
                        away_team_id = None
                        competition_slug = None
                        competition_name = None
                        event_details = None
                        
                        try:
                            # Fetch current event details to get correct team IDs
                            event_details = api_client.get_event_details(event_obj.id)
                            
                            if event_details:
                                # For Tennis, add rankings to observations
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
                                        logger.info(f"✅ Added rankings to observations for rescheduled event {event_obj.id}: home={home_team_ranking}, away={away_team_ranking}")
                                
                                # Extract team IDs and other event data
                                home_team_id = event_details.get('homeTeam', {}).get('id')
                                away_team_id = event_details.get('awayTeam', {}).get('id')
                                competition_name = event_details.get('tournament', {}).get('uniqueTournament', {}).get('name')
                                competition_slug = event_details.get('tournament', {}).get('uniqueTournament', {}).get('slug')
                            else:
                                logger.warning(f"Could not fetch event details for rescheduled event {event_obj.id}")
                        except Exception as e:
                            logger.error(f"Error fetching event details for rescheduled event {event_obj.id}: {e}")
                        
                        # Ensure observations for tennis events
                        # Start with observations if they exist (might already have rankings)
                        tennis_observations = observations if observations else []
                        
                        if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                            # Check if ground_type already exists in tennis_observations
                            has_ground_type = any(obs.get('type') == 'ground_type' for obs in tennis_observations)
                            
                            if not has_ground_type:
                                logger.info(f"🎾 Getting ground_type for tennis rescheduled event {event_obj.id}")
                                if not sport_observations_manager.has_observations_for_event(event_obj.id):
                                    # No observations exist in DB, make API call to extract court type
                                    new_observations = api_client.get_event_results(
                                        event_id=event_obj.id,
                                        update_court_type=True
                                    )
                                    if new_observations:
                                        # Merge new observations with existing tennis_observations
                                        for obs in new_observations:
                                            if obs.get('type') == 'ground_type':
                                                tennis_observations.append(obs)
                                else:
                                    # Get existing ground_type from database
                                    observation = ObservationRepository.get_observation(event_obj.id, 'ground_type')
                                    if observation:
                                        tennis_observations.append({'type': 'ground_type', 'value': observation.observation_value})
                                        logger.info(f"🎾 Using existing ground_type observation for rescheduled event: {observation.observation_value}")
                            
                            # Ensure rankings exist in tennis_observations
                            has_rankings = any(obs.get('type') == 'rankings' for obs in tennis_observations)
                            if not has_rankings and event_details:
                                # Add rankings from event_details if available
                                home_team_ranking = event_details.get('homeTeam', {}).get('ranking')
                                away_team_ranking = event_details.get('awayTeam', {}).get('ranking')
                                if home_team_ranking is not None or away_team_ranking is not None:
                                    tennis_observations.append({"type": "rankings", "home_ranking": home_team_ranking, "away_ranking": away_team_ranking})
                                    logger.info(f"✅ Added rankings to tennis_observations for rescheduled event {event_obj.id}: home={home_team_ranking}, away={away_team_ranking}")
                        
                        # Analyze H2H events with team results
                        from streak_alerts import streak_alert_engine
                        streak_analysis = streak_alert_engine.analyze_h2h_events(
                            event_id=event_obj.id,
                            event_custom_id=event_obj.custom_id,
                            event_start_time=event_obj.start_time_utc,
                            sport=event_obj.sport,
                            discovery_source=event_obj.discovery_source,
                            competition_name=competition_name,
                            competition_slug=competition_slug,
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
            should_send = False
            reason = ""
            
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
                    minutes_until_start == 5):  # Only log at 5 minutes
                    success = prediction_logger.log_prediction(event_obj, dual_report.process1_report)
                    if success:
                        logger.info(f"✅ Prediction logged for rescheduled event {event.id} (5 minutes from start)")
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
            events = EventRepository.get_events_by_date(yesterday.date())
            
            if not events:
                logger.info("No events found from previous day")
                return
            # update final odds for all events, temporal chunk of code. delete when done using it. this is for the midnight sync to get the final odds for all events.
            for event_data in events:
                final_odds_response = api_client.get_event_final_odds(event_data.id, event_data.slug)
                if final_odds_response:
                    final_odds_data = api_client.extract_final_odds_from_response(final_odds_response, initial_odds_extraction=True)
                    if final_odds_data:
                        upserted_id = OddsRepository.upsert_event_odds(event_data.id, final_odds_data)
                        if upserted_id:
                            snapshot = OddsRepository.create_odds_snapshot(event_data.id, final_odds_data)
                            logger.info(f"✅ Final odds updated for {event_data.home_team} vs {event_data.away_team}")
                        else:
                            logger.warning(f"Failed to update final odds for event {event_data.id}")
                    else:
                        logger.warning(f"No final odds data extracted for event {event_data.id}")
                else:
                    logger.warning(f"Failed to fetch final odds for event {event_data.id}")
            # end of temporal chunk of code. delete when done using it.
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
                    logger.debug(f"Results exist for event {event.id}, skipping")
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
    
    def run_job_midnight_sync_now(self):
        """Run Job D immediately"""
        logger.info("Running Job D immediately")
        self.job_midnight_sync()
    
    def run_job_results_collection_now(self):
        """Run Job E immediately"""
        logger.info("Running Job E immediately")
        self.job_results_collection()
    
    def run_job_results_collection_all_now(self):
        """Run Job E2 immediately"""
        logger.info("Running Job E2 immediately")
        self.job_results_collection_all_finished()
    
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
                    job_info['display'] = f"Pre-start check: Every 5 minutes at {job.at_time}"
                else:
                    job_info['display'] = f"Pre-start check: Every {job.interval} {job.unit}"
            elif job.job_func.__name__ == 'job_midnight_sync':
                if job.at_time:
                    job_info['display'] = f"Midnight sync: Daily at {job.at_time}"
                else:
                    job_info['display'] = f"Midnight sync: Every {job.interval} {job.unit}"
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
