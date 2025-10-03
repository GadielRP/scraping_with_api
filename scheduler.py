import schedule
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import json
from config import Config
from sofascore_api import api_client
from repository import EventRepository, OddsRepository, ResultRepository, ObservationRepository
from odds_utils import process_event_odds_from_dropping_odds
from alert_system import pre_start_notifier
import os
from sport_observations import sport_observations_manager
logger = logging.getLogger(__name__)
from alert_engine import alert_engine

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
        
        # Job C - Pre-start check (dynamic interval based on Config.POLL_INTERVAL_MINUTES)
        self._setup_pre_start_jobs()
        
        # Job D - Midnight results collection (at 04:00)
        schedule.every().day.at("04:00").do(self.job_midnight_sync)
        
        logger.info("Jobs scheduled:")
        logger.info(f"  - Discovery: daily at {', '.join(Config.DISCOVERY_TIMES)}")
        logger.info(f"  - Pre-start check: every {Config.POLL_INTERVAL_MINUTES} minutes")
        logger.info("  - Midnight sync: daily at 04:00 (results collection only)")
    
    def _setup_pre_start_jobs(self):
        """Setup pre-start check jobs every 1 minute at the exact minute mark to catch 1-minute timestamp corrections"""
        # Schedule jobs at exact minute marks (00, 01, 02, 03, 04, 05, 06, 07, 08, 09, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59)
        for minute in range(60):
            schedule.every().hour.at(f":{minute:02d}").do(self.job_pre_start_check)
        
        logger.info(f"  - Pre-start check scheduled every 1 minute at exact minute marks (to catch 1-minute timestamp corrections)")
    
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
            
            # Extract events and odds data
            events, odds_map = api_client.extract_events_and_odds_from_dropping_response(response)
            if not events:
                logger.warning("No events found in dropping odds")
                return
            
            logger.info(f"Found {len(events)} events in dropping odds")
            
            # Process each event with its odds data
            processed_count = 0
            skipped_count = 0
            for event_data in events:
                try:
                    id = str(event_data['id'])

                    
                    # Upsert event
                    event = EventRepository.upsert_event(event_data)
                    if not event:
                        logger.warning(f"Failed to upsert event {id}")
                        skipped_count += 1
                        continue
                    
                    # Process odds data from the single API call
                    odds_data = process_event_odds_from_dropping_odds(id, odds_map)
                    if not odds_data:
                        logger.warning(f"No odds data found for event {id}")
                        skipped_count += 1
                        continue
                    
                    # Create snapshot
                    snapshot = OddsRepository.create_odds_snapshot(int(id), odds_data)
                    if not snapshot:
                        logger.error(f"Failed to create odds snapshot for event {id}")
                        skipped_count += 1
                        continue
                    
                    # Upsert event odds
                    upserted_id = OddsRepository.upsert_event_odds(int(id), odds_data)
                    if not upserted_id:
                        logger.error(f"Failed to upsert event odds for event {id}")
                        skipped_count += 1
                        continue
                    
                    # Note: Alert system removed - now only pre-start notifications are sent
                    
                    processed_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing event {event_data.get('id')}: {e}")
                    skipped_count += 1
                    continue
            
            logger.info(f"Job A completed: processed {processed_count}/{len(events)} events")
            
        except Exception as e:
            logger.error(f"Error in Job A: {e}")
    

    
    def job_pre_start_check(self):
        """
        Job C: Pre-start check for events starting within 30 minutes
        
        SMART ODDS EXTRACTION: Only extracts odds at key moments (30 min and 5 min before start)
        to avoid unnecessary API calls when odds don't change significantly.
        
        SMART NOTIFICATIONS: Only sends Telegram notifications when odds are extracted at key moments
        (30 min and 5 min), but includes ALL upcoming games in those notifications to avoid missing games.
        """
        logger.info("🚨 PRE-START CHECK EXECUTED at " + datetime.now().strftime("%H:%M:%S"))
        
        
        try:
            # Get events starting within the next 30 minutes WITH their odds data
            events_with_odds = EventRepository.get_events_starting_soon_with_odds(Config.PRE_START_WINDOW_MINUTES)
            if not events_with_odds:
                logger.debug("No events starting within the next 30 minutes")
                return
            
            logger.info(f"Found {len(events_with_odds)} events starting within {Config.PRE_START_WINDOW_MINUTES} minutes")
            
            # Process each upcoming event
            processed_count = 0
            odds_extracted_count = 0
            for event_data in events_with_odds:
                try:
                    minutes_until_start = self._minutes_until_start(event_data['start_time_utc'])
                    logger.info(f"🚨 UPCOMING GAME ALERT: {event_data['home_team']} vs {event_data['away_team']} starts in {minutes_until_start} minutes")
                    
                    # SMART ODDS EXTRACTION: Only extract odds at key moments (30 min and 5 min)                  
                    if self._should_extract_odds_for_event(event_data['id'], minutes_until_start):
                        logger.info(f"🎯 EXTRACTING ODDS: {event_data['home_team']} vs {event_data['away_team']} - {minutes_until_start} min until start")
                        
                        # Fetch final odds for this upcoming game using specific event endpoint
                        final_odds_response = api_client.get_event_final_odds(event_data['id'], event_data['slug'])
                        
                        if final_odds_response:
                            # Process the final odds data
                            final_odds_data = api_client.extract_final_odds_from_response(final_odds_response)
                                                     
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
                                no_value = api_client.get_event_results(
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
                    from models import refresh_materialized_views
                    from database import db_manager
                    refresh_materialized_views(db_manager.engine)
                    logger.info("✅ Alert materialized views refreshed")
                    
                    
                    # Get Event objects with properly loaded event_odds for alert evaluation
                    # Only evaluate events that are at key moments (30 or 5 minutes)
                    events_for_alerts = []
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
                        if minutes_until_start in [30, 5, 1]:  # Key moments including 1-minute timestamp correction
                            events_for_alerts.append(event_obj)
                    
                    if events_for_alerts:
                        logger.info(f"🔍 Evaluating {len(events_for_alerts)} events at key moments for dual process alerts...")
                        
                        # Execute Dual Process (Process 1 + Process 2)
                        try:
                            from prediction_engine import prediction_engine
                            dual_reports = []
                            
                            for event_obj in events_for_alerts:
                                # Calculate minutes from FRESH database time, not stale event data
                                minutes_until_start = self._minutes_until_start(event_obj.start_time_utc)
                                
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
                                    dual_reports.append(dual_report)
                                    logger.info(f"✅ Dual process report added for event {event_obj.id}: {reason}")
                                else:
                                    logger.debug(f"⏭️ Skipping dual process report for event {event_obj.id}: No predictions or candidates found")
                            
                            if dual_reports:
                                logger.info(f"📊 Generated {len(dual_reports)} dual process reports")
                                # Send dual process alerts using enhanced alert system
                                self._send_dual_process_alerts(dual_reports)
                                
                                # Log predictions for successful Process 1 reports
                                from modules.prediction import prediction_logger
                                
                                for dual_report in dual_reports:
                                    # Only log predictions from Process 1 with success status
                                    if (dual_report.process1_report and 
                                        dual_report.process1_report.get('status') == 'success'):
                                        
                                        # Get the event object
                                        event_obj = self.event_repo.get_event_by_id(dual_report.event_id)
                                        if event_obj:
                                            # Check if event is exactly 5 minutes from start
                                            minutes_until_start = self._minutes_until_start(event_obj.start_time_utc)
                                            if minutes_until_start == 5:
                                                # Log the prediction only at 5 minutes
                                                success = prediction_logger.log_prediction(event_obj, dual_report.process1_report)
                                                if success:
                                                    logger.info(f"✅ Prediction logged for dual process event {dual_report.event_id} (5 minutes from start)")
                                                else:
                                                    # Check if it's a duplicate (already exists) vs. actual failure
                                                    from database import db_manager
                                                    from models import PredictionLog
                                                    with db_manager.get_session() as session:
                                                        existing = session.query(PredictionLog).filter_by(event_id=dual_report.event_id).first()
                                                        if existing:
                                                            logger.info(f"ℹ️ Prediction already exists for dual process event {dual_report.event_id} - no action needed")
                                                        else:
                                                            logger.warning(f"❌ Failed to log prediction for dual process event {dual_report.event_id}")
                                            else:
                                                logger.info(f"⏭️ Skipping prediction logging for event {dual_report.event_id} - {minutes_until_start} minutes until start (not 5 minutes)")
                                        else:
                                            logger.warning(f"Could not find event {dual_report.event_id} for prediction logging")
                            else:
                                logger.debug("No dual process reports generated")
                                
                        except Exception as e:
                            logger.error(f"Error running dual process evaluation: {e}")
                            
                            # Fallback to Process 1 only if dual process fails
                            logger.info("🔄 Falling back to Process 1 only...")
                            alerts = alert_engine.evaluate_upcoming_events(events_for_alerts)
                            if alerts:
                                logger.info(f"📊 Generated {len(alerts)} Process 1 candidate reports (fallback)")
                                alert_engine.send_alerts(alerts)
                                
                                # Log predictions for successful Process 1 reports (fallback)
                                from modules.prediction import prediction_logger
                                
                                for alert in alerts:
                                    # Only log predictions with success status
                                    if alert.get('status') == 'success':
                                        event_id = alert.get('event_id')
                                        if event_id:
                                            # Get the event object
                                            event_obj = self.event_repo.get_event_by_id(event_id)
                                            if event_obj:
                                                # Check if event is exactly 5 minutes from start
                                                minutes_until_start = self._minutes_until_start(event_obj.start_time_utc)
                                                if minutes_until_start == 5:
                                                    # Log the prediction only at 5 minutes
                                                    success = prediction_logger.log_prediction(event_obj, alert)
                                                    if success:
                                                        logger.info(f"✅ Prediction logged for Process 1 event {event_id} (5 minutes from start)")
                                                    else:
                                                        # Check if it's a duplicate (already exists) vs. actual failure
                                                        from database import db_manager
                                                        from models import PredictionLog
                                                        with db_manager.get_session() as session:
                                                            existing = session.query(PredictionLog).filter_by(event_id=event_id).first()
                                                            if existing:
                                                                logger.info(f"ℹ️ Prediction already exists for Process 1 event {event_id} - no action needed")
                                                            else:
                                                                logger.warning(f"❌ Failed to log prediction for Process 1 event {event_id}")
                                                else:
                                                    logger.info(f"⏭️ Skipping prediction logging for event {event_id} - {minutes_until_start} minutes until start (not 5 minutes)")
                                            else:
                                                logger.warning(f"Could not find event {event_id} for prediction logging")
                            else:
                                logger.debug("No Process 1 candidate reports generated (fallback)")
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
    
    def _minutes_until_start(self, start_time_utc) -> int:
        """Calculate minutes until event starts"""
        # Use local time since SofaScore provides local times
        now = datetime.now()
        time_diff = start_time_utc - now
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
        
        # Key moments for odds extraction
        KEY_MOMENTS = [30, 5, 1]
        
        # Check if current time aligns with a key moment
        should_extract = minutes_until_start in KEY_MOMENTS
        
        if minutes_until_start == 1:
            # For 1-minute events, always check and update starting time (no odds extraction)
            logger.info(f"🕐 1-minute event detected for event {event_id} - checking for final timestamp correction")
            correct_starting_time = api_client.get_event_results(event_id, update_time=True, minutes_until_start=minutes_until_start)
            if correct_starting_time is None:
                logger.warning(f"⏭️ API error for 1-minute event {event_id} - skipping timestamp correction")
                return False
            elif not correct_starting_time:
                # Starting time was updated - check if rescheduled game is now in key moments
                logger.info(f"🔄 Starting time changed for 1-minute event {event_id} - checking if rescheduled game is in key moments")
                self.recently_rescheduled.add(event_id)  # Mark immediately to prevent double processing
                self._check_rescheduled_event(event_id)
            return False  # Never extract odds at 1 minute, only timestamp correction
        
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
            
            # Check if rescheduled game is now in key moments (30 or 5 minutes) OR has already started (negative minutes)
            if new_minutes_until_start in [30, 5] or new_minutes_until_start < 0:
                if new_minutes_until_start >= 0:
                    logger.info(f"🎯 RESCHEDULED GAME ALERT: Event {event_id} is now starting in {new_minutes_until_start} minutes")
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
        Uses the same dual-process flow as normal events for consistent alert format.
        This completes the pre-start job workflow to prevent infinite loops.
        """
        try:
            logger.info(f"🔍 Processing dual-process alerts for rescheduled event {event.id}")
            
            # Refresh materialized views to ensure latest data for alert evaluation
            logger.info("🔄 Refreshing alert materialized views for rescheduled event...")
            from models import refresh_materialized_views
            from database import db_manager
            refresh_materialized_views(db_manager.engine)
            logger.info("✅ Alert materialized views refreshed")
            
            # Get the Event object with properly loaded event_odds for alert evaluation
            event_obj = self.event_repo.get_event_by_id(event.id)
            if not event_obj or not event_obj.event_odds:
                logger.warning(f"Could not load event_odds for rescheduled event {event.id}")
                return
            
            # Calculate minutes until start for the rescheduled event using FRESH database time
            minutes_until_start = self._minutes_until_start(event_obj.start_time_utc)
            logger.info(f"🔍 Evaluating rescheduled event {event.id} for dual-process alerts (starts in {minutes_until_start} minutes)...")
            
            # Enrich event object with court type for Tennis/Tennis Doubles events
            event_obj.court_type = None  # Default value
            if event_obj.sport in ['Tennis', 'Tennis Doubles']:
                from repository import ObservationRepository
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
            
            if should_send:
                logger.info(f"✅ Dual process report generated for rescheduled event {event.id}: {reason}")
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
