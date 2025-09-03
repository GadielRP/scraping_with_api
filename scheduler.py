import schedule
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import json
from config import Config
from sofascore_api import api_client
from repository import EventRepository, OddsRepository, ResultRepository
from odds_utils import process_event_odds_from_dropping_odds, process_event_odds
from alert_system import pre_start_notifier

logger = logging.getLogger(__name__)

class JobScheduler:
    """Main job scheduler for the SofaScore odds system"""
    
    def __init__(self):
        self.running = False
        self.thread = None
        self.event_repo = EventRepository()
        self.odds_repo = OddsRepository()
        self.result_repo = ResultRepository()
        
        # Setup jobs
        self._setup_jobs()
    
    def _setup_jobs(self):
        """Setup all scheduled jobs"""
        # Job A - Discovery (at configurable clock times from Config.DISCOVERY_TIMES)
        from config import Config
        for time_str in Config.DISCOVERY_TIMES:
            schedule.every().day.at(time_str).do(self.job_discovery)
        
        # Job C - Pre-start check (dynamic interval based on Config.POLL_INTERVAL_MINUTES)
        self._setup_pre_start_jobs()
        
        # Job D - Midnight results collection (at 00:05)
        schedule.every().day.at("00:05").do(self.job_midnight_sync)
        
        logger.info("Jobs scheduled:")
        logger.info(f"  - Discovery: daily at {', '.join(Config.DISCOVERY_TIMES)}")
        logger.info(f"  - Pre-start check: every {Config.POLL_INTERVAL_MINUTES} minutes")
        logger.info("  - Midnight sync: daily at 00:05 (results collection only)")
    
    def _setup_pre_start_jobs(self):
        """Setup pre-start check jobs at exact 5-minute clock intervals (00, 05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)"""
        # Schedule jobs at exact 5-minute intervals on the clock
        schedule.every().hour.at(":00").do(self.job_pre_start_check)
        schedule.every().hour.at(":05").do(self.job_pre_start_check)
        schedule.every().hour.at(":10").do(self.job_pre_start_check)
        schedule.every().hour.at(":15").do(self.job_pre_start_check)
        schedule.every().hour.at(":20").do(self.job_pre_start_check)
        schedule.every().hour.at(":25").do(self.job_pre_start_check)
        schedule.every().hour.at(":30").do(self.job_pre_start_check)
        schedule.every().hour.at(":35").do(self.job_pre_start_check)
        schedule.every().hour.at(":40").do(self.job_pre_start_check)
        schedule.every().hour.at(":45").do(self.job_pre_start_check)
        schedule.every().hour.at(":50").do(self.job_pre_start_check)
        schedule.every().hour.at(":55").do(self.job_pre_start_check)
        
        logger.info(f"  - Pre-start check scheduled every 5 minutes at clock intervals")

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
            import json
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_filename = f"debug_discovery_{timestamp}.json"
            try:
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
                    event = self.event_repo.upsert_event(event_data)
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
                    snapshot = self.odds_repo.create_odds_snapshot(int(id), odds_data)
                    if not snapshot:
                        logger.error(f"Failed to create odds snapshot for event {id}")
                        skipped_count += 1
                        continue
                    
                    # Upsert event odds
                    upserted_id = self.odds_repo.upsert_event_odds(int(id), odds_data)
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
    
    def job_ingest_event_odds(self, id: int, event_slug: str = None):
        """Job B: Ingest odds for a specific event (LEGACY - now handled in Job A)"""
        logger.info(f"Starting Job B: Ingest odds for event {id} (LEGACY MODE)")
        
        try:
            # Get odds changes using legacy method
            response = api_client.get_event_odds_changes(id)
            if not response:
                logger.error(f"Failed to get odds changes for event {id}")
                return
            
            # Extract odds changes
            changed_odds = api_client.extract_odds_changes(response)
            if not changed_odds:
                logger.warning(f"No odds changes found for event {id}")
                return
            
            # Process odds using legacy method
            odds_data = process_event_odds(changed_odds)
            if not odds_data:
                logger.warning(f"Failed to process odds for event {id}")
                return
            
            # Create snapshot
            snapshot = self.odds_repo.create_odds_snapshot(id, odds_data)
            if not snapshot:
                logger.error(f"Failed to create odds snapshot for event {id}")
                return
            
            # Upsert event odds
            upserted_id = self.odds_repo.upsert_event_odds(id, odds_data)
            if not upserted_id:
                logger.error(f"Failed to upsert event odds for event {id}")
                return
            
            # Note: Alert system removed - now only pre-start notifications are sent
            
            logger.info(f"Job B completed for event {id}")
            
        except Exception as e:
            logger.error(f"Error in Job B for event {id}: {e}")
    
    def job_pre_start_check(self):
        """
        Job C: Pre-start check for events starting within 30 minutes
        
        SMART ODDS EXTRACTION: Only extracts odds at key moments (30 min and 5 min before start)
        to avoid unnecessary API calls when odds don't change significantly.
        
        SMART NOTIFICATIONS: Only sends Telegram notifications when odds are extracted at key moments
        (30 min and 5 min), but includes ALL upcoming games in those notifications to avoid missing games.
        """
        logger.info("🚨 PRE-START CHECK EXECUTED at " + datetime.now().strftime("%H:%M:%S"))
        logger.info("Starting Job C: Pre-start check for upcoming games")
        
        try:
            # Get events starting within the next 30 minutes WITH their odds data
            events_with_odds = self.event_repo.get_events_starting_soon_with_odds(Config.PRE_START_WINDOW_MINUTES)
            if not events_with_odds:
                logger.debug("No events starting within the next 30 minutes")
                return
            
            logger.info(f"Found {len(events_with_odds)} events starting within {Config.PRE_START_WINDOW_MINUTES} minutes")
            
            # Prepare upcoming events data for notifications (ALL games, with updated odds when available)
            upcoming_events_data = []
            
            # Process each upcoming event
            processed_count = 0
            odds_extracted_count = 0
            for event_data in events_with_odds:
                try:
                    minutes_until_start = self._minutes_until_start(event_data['start_time_utc'])
                    logger.info(f"🚨 UPCOMING GAME ALERT: {event_data['home_team']} vs {event_data['away_team']} starts in {minutes_until_start} minutes")
                    
                    # SMART ODDS EXTRACTION: Only extract odds at key moments (30 min and 5 min)
                    should_extract_odds = self._should_extract_odds_for_event(event_data['id'], minutes_until_start)
                    
                    # Prepare event data for notification (will be updated with fresh odds if extracted)
                    notification_event_data = {
                        'home_team': event_data['home_team'],
                        'away_team': event_data['away_team'],
                        'competition': event_data['competition'],
                        'start_time': event_data['start_time_utc'].strftime("%H:%M"),
                        'minutes_until_start': minutes_until_start,
                        'odds': event_data.get('odds')  # Start with existing odds from database
                    }
                    
                    if should_extract_odds:
                        logger.info(f"🎯 EXTRACTING ODDS: {event_data['home_team']} vs {event_data['away_team']} - {minutes_until_start} min until start")
                        
                        # Fetch final odds for this upcoming game using specific event endpoint
                        final_odds_response = api_client.get_event_final_odds(event_data['id'], event_data['slug'])
                        
                        if final_odds_response:
                            # Process the final odds data
                            final_odds_data = api_client.extract_final_odds_from_response(final_odds_response)
                            if final_odds_data:
                                # Update the event odds with final odds
                                upserted_id = self.odds_repo.upsert_event_odds(event_data['id'], final_odds_data)
                                if upserted_id:
                                    logger.info(f"✅ Final odds updated for {event_data['home_team']} vs {event_data['away_team']}")
                                    
                                    # Create final odds snapshot
                                    snapshot = self.odds_repo.create_odds_snapshot(event_data['id'], final_odds_data)
                                    if snapshot:
                                        logger.info(f"✅ Final odds snapshot created for event {event_data['id']}")
                                        odds_extracted_count += 1
                                        
                                        # Update notification data with fresh odds (merge with existing odds)
                                        existing_odds = notification_event_data['odds'] or {}
                                        merged_odds = {
                                            # Keep existing opening odds
                                            'one_open': existing_odds.get('one_open'),
                                            'x_open': existing_odds.get('x_open'),
                                            'two_open': existing_odds.get('two_open'),
                                            # Add fresh final odds
                                            'one_final': final_odds_data.get('one_final'),
                                            'x_final': final_odds_data.get('x_final'),
                                            'two_final': final_odds_data.get('two_final')
                                        }
                                        notification_event_data['odds'] = merged_odds
                                        logger.info(f"📱 Updated odds for notification: {event_data['home_team']} vs {event_data['away_team']} (odds extracted at {minutes_until_start} min)")
                                else:
                                    logger.warning(f"Failed to update final odds for event {event_data['id']}")
                            else:
                                logger.warning(f"No final odds data extracted for event {event_data['id']}")
                        else:
                            logger.warning(f"Failed to fetch final odds for event {event_data['id']}")
                    else:
                        logger.debug(f"⏭️ SKIPPING ODDS EXTRACTION: {event_data['home_team']} vs {event_data['away_team']} - {minutes_until_start} min until start (not a key moment)")
                    
                    # ALWAYS add to notifications (with existing odds if no extraction, or fresh odds if extracted)
                    upcoming_events_data.append(notification_event_data)
                    logger.info(f"📱 Added to notifications: {event_data['home_team']} vs {event_data['away_team']} (odds: {'fresh' if should_extract_odds else 'existing'})")
                    
                    processed_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing upcoming event {event_data['id']}: {e}")
                    continue
            
            # Send notifications about upcoming games ONLY when odds are extracted
            if upcoming_events_data and Config.NOTIFICATIONS_ENABLED and odds_extracted_count > 0:
                logger.info(f"Sending notifications for {len(upcoming_events_data)} upcoming games (odds extracted for {odds_extracted_count} games)")
                notification_sent = pre_start_notifier.notify_upcoming_games(upcoming_events_data)
                if notification_sent:
                    logger.info("✅ Upcoming games notifications sent successfully")
                else:
                    logger.warning("⚠️ Failed to send some upcoming games notifications")
            elif upcoming_events_data and Config.NOTIFICATIONS_ENABLED and odds_extracted_count == 0:
                logger.info(f"📱 NO NOTIFICATIONS SENT: {len(upcoming_events_data)} games found but no odds extracted (not at key moments)")
            elif upcoming_events_data and not Config.NOTIFICATIONS_ENABLED:
                logger.info("Notifications disabled - would have sent notifications for upcoming games")
            
            if processed_count > 0:
                logger.info(f"🚨 Pre-start check completed: {processed_count} games starting soon!")
                if odds_extracted_count > 0:
                    logger.info(f"🎯 Odds extracted for {odds_extracted_count} games (smart extraction active)")
                    logger.info(f"📱 Notifications sent for {len(upcoming_events_data)} games (only when odds extracted)")
                else:
                    logger.info(f"⏭️ No odds extracted (smart extraction: only at 30min and 5min)")
                    logger.info(f"📱 No notifications sent (not at key moments)")
            else:
                logger.info("Pre-start check completed: No games starting soon")
            
        except Exception as e:
            logger.error(f"Error in Job C: {e}")
    
    def _minutes_until_start(self, start_time_utc) -> int:
        """Calculate minutes until event starts"""
        from datetime import datetime
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
        # Key moments for odds extraction
        KEY_MOMENTS = [30, 5]
        
        # Check if current time aligns with a key moment
        should_extract = minutes_until_start in KEY_MOMENTS
        
        if should_extract:
            logger.info(f"🎯 Key moment detected for event {event_id}: {minutes_until_start} minutes until start - WILL EXTRACT ODDS")
        else:
            logger.debug(f"⏭️ Not a key moment for event {event_id}: {minutes_until_start} minutes until start - SKIPPING ODDS EXTRACTION")
        
        return should_extract
    
    def job_results_collection(self):
        """Job E: Collect results for finished events from the previous day"""
        logger.info("Starting Job E: Results collection for finished events")
        
        try:
            yesterday = datetime.now() - timedelta(days=1)
            events = self.event_repo.get_events_by_date(yesterday.date())
            
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
                if self.result_repo.get_result_by_event_id(event.id):
                    logger.debug(f"Results exist for event {event.id}, skipping")
                    stats['skipped'] += 1
                    continue
                
                result_data = api_client.get_event_results(event.id)
                if not result_data:
                    stats['failed'] += 1
                    continue
                
                if self.result_repo.upsert_result(event.id, result_data):
                    stats['updated'] += 1
                    logger.info(f"✅ {job_name}: {event.id} = {result_data['home_score']}-{result_data['away_score']}, Winner: {result_data['winner']}")
                
            except Exception as e:
                logger.error(f"Error in {job_name} for event {event.id}: {e}")
                stats['failed'] += 1
        
        return stats

    def job_results_collection_all_finished(self):
        """Job E2: Comprehensive results collection for ALL finished events."""
        logger.info("Starting Job E2: Comprehensive results collection")
        
        try:
            events = self.event_repo.get_all_finished_events()
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

# Global scheduler instance
job_scheduler = JobScheduler()
