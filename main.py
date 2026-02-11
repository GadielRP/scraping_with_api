#!/usr/bin/env python3
"""
SofaScore Odds Alert System - Main Application

This is the main entry point for the SofaScore odds monitoring and alert system.
It provides a command-line interface for running the various jobs and managing the system.
"""

import argparse
import logging
import os
import sys
import signal
import time
from datetime import datetime

from config import Config

# Setup logging early to ensure all modules use the correct configuration
def _get_log_path():
    """Build the dynamic log file path based on current local date (timezone-aware).
    
    Structure: logs/{MM_MonthName}/week_{N}/sofascore_odds.log
    Week calculation: days 1-7 → week_1, 8-14 → week_2, 15-21 → week_3, 22-31 → week_4
    """
    from timezone_utils import get_local_now_aware
    
    MONTH_NAMES = [
        '', '01_January', '02_February', '03_March', '04_April',
        '05_May', '06_June', '07_July', '08_August',
        '09_September', '10_October', '11_November', '12_December'
    ]
    
    now = get_local_now_aware()
    month_folder = MONTH_NAMES[now.month]
    week_number = min((now.day - 1) // 7 + 1, 4)
    
    log_dir = os.path.join('logs', month_folder, f'week_{week_number}')
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, 'sofascore_odds.log')


class _WeeklyRotatingFileHandler(logging.FileHandler):
    """File handler that auto-rotates into weekly/monthly directories.
    
    On each emit(), checks if the current local date has moved to a different
    week/month. If so, closes the old file and opens a new one in the correct
    directory. Cost is one lightweight date comparison per log line.
    """
    
    def __init__(self, **kwargs):
        self._current_path = _get_log_path()
        super().__init__(self._current_path, **kwargs)
    
    def emit(self, record):
        new_path = _get_log_path()
        if new_path != self._current_path:
            # Week/month changed — rotate to new file
            self.close()
            self._current_path = new_path
            self.baseFilename = os.path.abspath(new_path)
            self.stream = self._open()
        super().emit(record)


def setup_logging():
    """Setup logging configuration with weekly-rotated log files.
    
    Logs are stored in: logs/{MM_MonthName}/week_{N}/sofascore_odds.log
    Uses timezone_utils for Mexico City local time to determine the folder.
    """
    # Clear any existing handlers to ensure clean setup
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create formatters
    formatter = logging.Formatter(Config.LOG_FORMAT)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, Config.LOG_LEVEL))
    console_handler.setFormatter(formatter)
    
    # Create weekly-rotating file handler with UTF-8 encoding
    file_handler = _WeeklyRotatingFileHandler(mode='a', encoding='utf-8')
    file_handler.setLevel(getattr(logging, Config.LOG_LEVEL))
    file_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger.setLevel(getattr(logging, Config.LOG_LEVEL))
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    # Force immediate flush for console output
    console_handler.flush()
    file_handler.flush()
    
    logging.info("Logging system initialized successfully")

# Initialize logging before importing other modules
setup_logging()

from database import db_manager
from models import create_or_replace_views, create_or_replace_materialized_views, refresh_materialized_views
from scheduler import job_scheduler
from alert_system import pre_start_notifier
from repository import EventRepository, OddsRepository


def initialize_system():
    """Initialize the system components"""
    logger = logging.getLogger(__name__)
    
    try:
        # Step 1: Test database connection
        if not db_manager.test_connection():
            logger.error("Database connection failed")
            return False
        
        # Step 2: Create tables if they don't exist (without materialized views)
        db_manager.create_tables()
        
        # Step 3: Run schema migrations BEFORE creating views
        # This ensures all columns exist before views try to reference them
        if not db_manager.check_and_migrate_schema():
            logger.warning("Schema migration check failed, but continuing...")
        
        # Step 4: Create/update regular views (can reference migrated columns)
        create_or_replace_views(db_manager.engine)
        
        # Step 5: Create/update materialized views (after migrations, so discovery_source exists)
        create_or_replace_materialized_views(db_manager.engine)
        
        logger.info("System initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize system: {e}")
        return False

def run_discovery():
    """Run event discovery job"""
    logger = logging.getLogger(__name__)
    logger.info("Running event discovery...")
    job_scheduler.run_job_discovery_now()

def run_discovery2():
    """Run event discovery 2 job (streaks, h2h, winning odds)"""
    logger = logging.getLogger(__name__)
    logger.info("Running event discovery 2 (streaks, h2h, winning odds)...")
    job_scheduler.run_job_discovery2_now()

def run_pre_start_check():
    """Run pre-start check job"""
    logger = logging.getLogger(__name__)
    logger.info("Running pre-start check...")
    job_scheduler.run_job_pre_start_check_now()

def run_midnight_sync():
    """Run midnight results collection job"""
    logger = logging.getLogger(__name__)
    logger.info("Running midnight results collection...")
    job_scheduler.run_job_midnight_sync_now()

def run_results_collection():
    """Run results collection job"""
    logger = logging.getLogger(__name__)
    logger.info("Running results collection...")
    job_scheduler.run_job_results_collection_now()

# Test notification functionality removed - system is working correctly

def run_results_collection_all():
    """Run comprehensive results collection for all finished events"""
    logger = logging.getLogger(__name__)
    logger.info("Running comprehensive results collection...")
    job_scheduler.run_job_results_collection_all_now()

def run_results_for_date(date_str: str):
    """Run results collection for a specific date (yyyy-mm-dd)"""
    logger = logging.getLogger(__name__)
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        logger.error(f"Invalid date format: '{date_str}'. Use yyyy-mm-dd format.")
        sys.exit(1)
    logger.info(f"Running results collection for date: {target_date}")
    job_scheduler.run_job_results_collection_for_date_now(target_date)

def run_daily_discovery():
    """Run daily discovery job (today's scheduled events with odds)"""
    logger = logging.getLogger(__name__)
    logger.info("Running daily discovery (today's scheduled events with odds)...")
    job_scheduler.run_job_daily_discovery_now()

def run_backfill_results(limit: int = 100):
    """Run backfill results script to collect missing data"""
    logger = logging.getLogger(__name__)
    logger.info(f"Running backfill results (limit: {limit})...")
    
    try:
        from backfill_results import main as backfill_main
        # Mock sys.argv to pass limit if needed, or call direct
        # For simplicity, we'll just import and run it
        # The script handles its own arguments if we use a subprocess or mock sys.argv
        import subprocess
        cmd = [sys.executable, "backfill_results.py", "--limit", str(limit)]
        logger.info(f"Executing: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
    except Exception as e:
        logger.error(f"Error running backfill results: {e}")

def run_alerts():
    """Run alert evaluation on upcoming events"""
    logger = logging.getLogger(__name__)
    logger.info("Running alert evaluation...")
    
    try:
        from alert_engine import alert_engine
        from repository import EventRepository
        
        # Get upcoming events (within 30 minutes)
        event_repo = EventRepository()
        upcoming_events = event_repo.get_events_starting_soon(30)
        
        if not upcoming_events:
            logger.info("No upcoming events found for alert evaluation")
            return
        
        logger.info(f"Evaluating {len(upcoming_events)} upcoming events for alerts")
        
        # Evaluate alerts
        alerts = alert_engine.evaluate_upcoming_events(upcoming_events)
        
        if alerts:
            logger.info(f"Generated {len(alerts)} alerts")
            # Send alerts
            alert_engine.send_alerts(alerts)
        else:
            logger.info("No alerts generated")
            
    except Exception as e:
        logger.error(f"Error running alerts: {e}")

def refresh_alert_data():
    """Refresh materialized views for alert processing"""
    logger = logging.getLogger(__name__)
    logger.info("Refreshing alert materialized views...")
    
    try:
        refresh_materialized_views(db_manager.engine)
        logger.info("Alert data refreshed successfully")
    except Exception as e:
        logger.error(f"Error refreshing alert data: {e}")


def start_scheduler():
    """Start the job scheduler"""
    logger = logging.getLogger(__name__)
    logger.info("Starting job scheduler...")
    
    # Start the job scheduler (system already initialized in main)
    job_scheduler.start()
       
    # Print startup information
    print("\n🚀 SofaScore Odds System Started Successfully!")
    print("=" * 50)
    print("📅 Scheduled Jobs:")
    print(f"  • Discovery (dropping odds): Daily at {', '.join(Config.DISCOVERY_TIMES)}")
    print(f"  • Discovery 2 (streaks, top team streaks, h2h, winning odds): Daily at {', '.join(Config.DISCOVERY2_TIMES)}")
    
    # Calculate and display dynamic pre-start check times
    interval_minutes = Config.POLL_INTERVAL_MINUTES
    intervals_per_hour = 60 // interval_minutes
    fixed_times = [f"{i * interval_minutes:02d}" for i in range(intervals_per_hour)]
    
    print(f"  • Pre-start check: Every 5 minutes at clock intervals (00, 05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)")
    print("    - Scans for games starting within 30 minutes")
    print("    - Fetches final odds only when games are starting soon")
    
    print("  • Results collection: Daily at 00:05 (collect results from finished games)")
    print("  • Daily discovery: Daily at 05:01 (today's scheduled events with odds)")
    print("\n⏰ Next Discovery Run:")
    
    # Calculate next discovery time using configurable times
    from datetime import datetime, timedelta
    # Use local time since SofaScore provides local times
    now = datetime.now()
    next_discovery = None
    
    for time_str in Config.DISCOVERY_TIMES:
        hour, minute = map(int, time_str.split(':'))
        next_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if next_time <= now:
            next_time += timedelta(days=1)
        if next_discovery is None or next_time < next_discovery:
            next_discovery = next_time
    
    if next_discovery:
        print(f"  • {next_discovery.strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("\n💡 System is running in background. Press Ctrl+C to stop.")
    print("=" * 50)
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal, stopping scheduler...")
        job_scheduler.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Keep the main thread alive while scheduler runs in background
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        job_scheduler.stop()

def show_status():
    """Show system status"""
    logger = logging.getLogger(__name__)
    
    try:
        # Database status
        db_status = "Connected" if db_manager.test_connection() else "Disconnected"
        
        # Get some basic stats
        event_repo = EventRepository()
        odds_repo = OddsRepository()
        
        with db_manager.get_session() as session:
            from models import Event, EventOdds, Result
            event_count = session.query(Event).count()
            odds_count = session.query(EventOdds).count()
            result_count = session.query(Result).count()
        
        # Get scheduled jobs
        jobs = job_scheduler.get_scheduled_jobs()
        
        print("\n=== SofaScore Odds System Status ===")
        print(f"Database: {db_status}")
        print(f"Events in database: {event_count}")
        print(f"Events with odds: {odds_count}")
        print(f"Events with results: {result_count}")
        print(f"Pre-start notifications: Active")
        print(f"\nScheduled Jobs:")
        for job in jobs:
            # Use the enhanced display format if available, otherwise fall back to old format
            if 'display' in job:
                print(f"  - {job['display']}")
            else:
                print(f"  - {job['function']}: {job['interval']} {job['unit']}")
            
            if job['next_run']:
                print(f"    Next run: {job['next_run']}")
        
        print("\n" + "=" * 40)
        
    except Exception as e:
        logger.error(f"Error showing status: {e}")

def show_events(limit: int = 10):
    """Show recent events"""
    logger = logging.getLogger(__name__)
    
    try:
        with db_manager.get_session() as session:
            from models import Event, EventOdds
            from sqlalchemy.orm import joinedload
            
            events = session.query(Event).options(
                joinedload(Event.event_odds)
            ).order_by(Event.start_time_utc.desc()).limit(limit).all()
        
        print(f"\n=== Recent Events (showing {len(events)}) ===")
        for event in events:
            odds = event.event_odds
            print(f"\nEvent ID: {event.id}")
            print(f"Teams: {event.home_team} vs {event.away_team}")
            print(f"Competition: {event.competition}")
            print(f"Start Time: {event.start_time_utc}")
            
            if odds:
                print(f"Odds - Open: 1={odds.one_open}, X={odds.x_open}, 2={odds.two_open}")
                print(f"Odds - Final: 1={odds.one_final}, X={odds.x_final}, 2={odds.two_final}")
            else:
                print("No odds data available")
        
        print("\n" + "=" * 40)
        
    except Exception as e:
        logger.error(f"Error showing events: {e}")

# Note: Alert system removed - now only pre-start notifications are sent

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='SofaScore Odds Alert System')
    parser.add_argument('command', choices=[
        'start', 'discovery', 'discovery2', 'pre-start', 'midnight', 'results', 'results-date', 'results-all', 'daily-discovery', 
        'backfill-results', 'status', 'events', 'alerts', 'refresh-alerts'
    ], help='Command to run')
    parser.add_argument('--limit', type=int, default=10, help='Limit for events display')
    parser.add_argument('--date', type=str, default=None, help='Target date in yyyy-mm-dd format (for results-date command)')
    
    args = parser.parse_args()
    
    # Logging is already setup at module import
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting SofaScore Odds System with command: {args.command}")
    
    try:
        if args.command == 'start':
            if initialize_system():
                
                # Then start scheduler
                start_scheduler()
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'discovery':
            if initialize_system():
                run_discovery()
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'discovery2':
            if initialize_system():
                run_discovery2()
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'pre-start':
            if initialize_system():
                run_pre_start_check()
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'midnight':
            if initialize_system():
                run_midnight_sync()
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'results':
            if initialize_system():
                run_results_collection()
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'results-date':
            if not args.date:
                logger.error("--date argument is required for results-date command (format: yyyy-mm-dd)")
                sys.exit(1)
            if initialize_system():
                run_results_for_date(args.date)
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'results-all':
            if initialize_system():
                run_results_collection_all()
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'daily-discovery':
            if initialize_system():
                run_daily_discovery()
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'backfill-results':
            if initialize_system():
                run_backfill_results(args.limit)
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'status':
            if initialize_system():
                show_status()
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'events':
            if initialize_system():
                show_events(args.limit)
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'alerts':
            if initialize_system():
                run_alerts()
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
        elif args.command == 'refresh-alerts':
            if initialize_system():
                refresh_alert_data()
            else:
                logger.error("Failed to initialize system")
                sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error running command {args.command}: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
