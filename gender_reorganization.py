"""
Gender Reorganization Module

Responsibilities:
- Reorganize all of the events by adding them a gender column (M, F, or unknown)

Usage:
- Run from workspace root directory: C:\\Users\\gadie\\Documents\\projects\\sofascore
- DB Correction with preview: python gender_reorganization.py --correct-gender --dry-run
- DB Correction (apply changes): python gender_reorganization.py --correct-gender
"""
from sqlalchemy import and_
import logging
import argparse
import time
import psycopg
from typing import Dict, List, Optional
from datetime import datetime
from timezone_utils import get_local_now
from sofascore_api import api_client
from config import Config
# Import database dependencies
from database import db_manager
from models import Event
from repository import EventRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GenderReorganization:
    """Main gender reorganization engine"""
    
    def __init__(self, dry_run: bool = False):
        """
        Initialize the gender reorganization engine.
        
        Args:
            dry_run: If True, only preview changes without applying them
        """
        self.dry_run = dry_run
        self.reorganization_stats = {
            'total_events': 0,
            'successful_updates': 0,
            'failed_updates': 0,
            'api_errors': 0,
            'skipped_404': 0,  # Events that returned 404 (deleted/not found)
            'M': 0,  # Male events
            'F': 0,  # Female events
            'mixed': 0,  # Mixed gender events
            'unknown': 0,  # Unknown gender events
        }
    
    def ensure_gender_column_exists(self) -> bool:
        """
        Check if gender column exists in events table, create it if not.
        This is an integrated migration that runs before the main logic.
        
        Uses the existing database connection from config (DATABASE_URL).
        
        Returns:
            True if column exists or was created successfully, False on error
        """
        try:
            logger.info("Checking if gender column exists in events table...")
            
            # Use the DATABASE_URL from config
            # Format: postgresql://user:password@host:port/dbname
            database_url = Config.DATABASE_URL
            
            # Convert SQLAlchemy format to psycopg format
            # SQLAlchemy uses: postgresql+psycopg://...
            # psycopg uses: postgresql://...
            if '+psycopg' in database_url:
                database_url = database_url.replace('postgresql+psycopg://', 'postgresql://')
            
            # Connect to PostgreSQL database using DATABASE_URL
            conn = psycopg.connect(database_url)
            
            with conn.cursor() as cur:
                # Check if gender column exists
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'events' AND column_name = 'gender'
                """)
                
                result = cur.fetchone()
                
                if result:
                    logger.info("✅ Gender column already exists")
                    conn.close()
                    return True
                
                logger.info("Gender column does not exist - creating it now...")
                
                # Create the gender column
                cur.execute("""
                    ALTER TABLE events 
                    ADD COLUMN gender VARCHAR(10) NOT NULL DEFAULT 'unknown'
                """)
                
                # Create index for better query performance
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_events_gender ON events(gender)
                """)
                
                # Commit the changes
                conn.commit()
                
                logger.info("✅ Gender column created successfully with default value 'unknown'")
                logger.info("✅ Index idx_events_gender created for performance")
                
                conn.close()
                return True
                
        except Exception as e:
            logger.error(f"Error ensuring gender column exists: {e}")
            return False

    def get_all_events_from_db(self, only_unknown: bool = False, limit: int | None = None, offset: int | None = None):
        try:
            with db_manager.get_session() as session:
                q = session.query(Event)
                q = q.filter(
                    and_(
                        Event.id > 269,
                        (Event.gender == 'unknown') | (Event.gender.is_(None)) if only_unknown else True
                    )
                )
                q = q.order_by(Event.id.asc())
                if offset:
                    q = q.offset(offset)
                if limit:
                    q = q.limit(limit)
                events = q.all()
                logger.info(f"Found {len(events)} events (only_unknown={only_unknown}, limit={limit}, offset={offset})")
                return events
        except Exception as e:
            logger.error(f"Error querying events from database: {e}")
            return []

    def get_event_response(self, event_id: int) -> Optional[Dict]:
        """
        Fetch event response from /event/{id} endpoint using the API client.
        This reuses the existing get_event_results method infrastructure.
        
        Special handling for 404 errors:
        - 404 means the event was deleted/removed from SofaScore
        - We skip these events immediately without retry
        - Returns 'SKIP_404' marker instead of None
        
        Args:
            event_id: The ID of the event to fetch
            
        Returns:
            API response dictionary, None on error, or 'SKIP_404' if event not found
        """
        try:
            logger.debug(f"Fetching API data for event {event_id}")
            
            # Use curl_cffi directly to get the status code for 404 detection
            url = f"{api_client.base_url}/event/{event_id}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json, text/plain, */*",
            }
            
            # Use the api_client's session to make the request
            api_client._rate_limit()  # Apply rate limiting
            response_obj = api_client.session.get(url, headers=headers, timeout=30)
            
            # Handle 404 specifically - event deleted or doesn't exist
            if response_obj.status_code == 404:
                logger.warning(f"⚠️ Event {event_id} not found (404) - event may have been deleted. Skipping.")
                self.reorganization_stats['skipped_404'] += 1
                return 'SKIP_404'  # Special marker for 404s
            
            # Handle successful response
            if response_obj.status_code == 200:
                return response_obj.json()
            
            # Other errors (500, 503, etc.) - these are API errors, not skips
            logger.warning(f"API error {response_obj.status_code} for event {event_id}")
            self.reorganization_stats['api_errors'] += 1
            return None
            
        except Exception as e:
            logger.error(f"Error fetching event response for {event_id}: {e}")
            self.reorganization_stats['api_errors'] += 1
            return None

    def extract_and_update_gender(self, event_id: int, home_team: str, away_team: str) -> bool:
        """
        Extract gender from API response and update the event in database.
        
        Args:
            event_id: The ID of the event to update
            home_team: Home team name (for logging)
            away_team: Away team name (for logging)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Fetch API response
            response = self.get_event_response(event_id)
            
            # Handle 404 skip marker - event was deleted
            if response == 'SKIP_404':
                return False  # Skip this event (already counted in stats)
            
            # Handle other errors
            if not response:
                return False
            
            # Extract gender using the existing get_gender method
            event_data = response.get('event', {})
            home_team_data = event_data.get('homeTeam', {})
            away_team_data = event_data.get('awayTeam', {})
            
            # Use the existing get_gender logic from api_client
            gender = api_client.get_gender(home_team_data, away_team_data)
            
            logger.info(f"Event {event_id} ({home_team} vs {away_team}): Gender = {gender}")
            
            # Update statistics
            if gender in self.reorganization_stats:
                self.reorganization_stats[gender] += 1
            
            # Update database (skip if dry-run)
            if not self.dry_run:
                success = self.update_event_gender_in_db(event_id, gender)
                if success:
                    self.reorganization_stats['successful_updates'] += 1
                    return True
                else:
                    self.reorganization_stats['failed_updates'] += 1
                    return False
            else:
                # Dry run: just count as successful (would have worked)
                self.reorganization_stats['successful_updates'] += 1
                logger.info(f"[DRY RUN] Would update event {event_id} to gender '{gender}'")
                return True
            
        except Exception as e:
            logger.error(f"Error extracting and updating gender for event {event_id}: {e}")
            self.reorganization_stats['failed_updates'] += 1
            return False

    def update_event_gender_in_db(self, event_id: int, gender: str) -> bool:
        """
        Update the gender field of an event in the database.
        
        Args:
            event_id: The ID of the event to update
            gender: The gender value to set ('M', 'F', 'mixed', or 'unknown')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with db_manager.get_session() as session:
                event = session.query(Event).filter(Event.id == event_id).first()
                
                if not event:
                    logger.warning(f"Event {event_id} not found in database")
                    return False
                
                # Update the gender field
                event.gender = gender
                event.updated_at = get_local_now()
                
                # Commit happens automatically when context manager exits successfully
                # But we'll explicitly commit here to be sure
                session.commit()
                logger.info(f"✅ Successfully updated gender for event {event_id} to '{gender}'")
                return True
                
        except Exception as e:
            logger.error(f"Error updating gender in database for event {event_id}: {e}")
            return False

    def run_gender_correction(self) -> Dict:
        """
        Main method to run gender correction for all events.
        Includes automatic database migration to create gender column if needed.
        
        Returns:
            Dictionary with statistics about the operation
        """
        logger.info("=" * 60)
        if self.dry_run:
            logger.info("STARTING GENDER CORRECTION (DRY RUN - NO CHANGES WILL BE APPLIED)")
        else:
            logger.info("STARTING GENDER CORRECTION")
        logger.info("=" * 60)
        
        # Step 1: Ensure gender column exists (integrated migration)
        if not self.ensure_gender_column_exists():
            logger.error("❌ Failed to ensure gender column exists. Aborting.")
            return self.reorganization_stats
        
        logger.info("")  # Empty line for readability
        
        # Step 2: Get all events from database
        events = self.get_all_events_from_db()
        self.reorganization_stats['total_events'] = len(events)
        
        if not events:
            logger.warning("No events found in database")
            return self.reorganization_stats
        
        logger.info(f"Processing {len(events)} events...")
        
        # Process each event with rate limiting
        for idx, event in enumerate(events, 1):
            try:
                logger.info(f"Processing event {idx}/{len(events)}: {event.id}")
                
                # Extract and update gender
                self.extract_and_update_gender(event.id, event.home_team, event.away_team)
                
                # Rate limiting: sleep between requests to avoid overwhelming the API
                # Note: The api_client already has rate limiting, but we add a small delay here too
                if idx < len(events):  # Don't sleep after the last event
                    time.sleep(0.5)  # 0.5 second delay between events
                
                # Progress update every 10 events
                if idx % 10 == 0:
                    logger.info(f"Progress: {idx}/{len(events)} events processed")
                    
            except Exception as e:
                logger.error(f"Unexpected error processing event {event.id}: {e}")
                self.reorganization_stats['failed_updates'] += 1
                continue
        
        # Print final statistics
        self.print_statistics()
        
        return self.reorganization_stats

    def verify_database_updates(self) -> Dict:
        """
        Query the database to verify that gender updates were actually applied.
        This helps diagnose if the database updates are working correctly.
        
        Returns:
            Dictionary with actual gender distribution from database
        """
        try:
            logger.info("")
            logger.info("=" * 60)
            logger.info("VERIFYING DATABASE UPDATES")
            logger.info("=" * 60)
            
            with db_manager.get_session() as session:
                # Query gender distribution from database
                from sqlalchemy import func
                
                results = session.query(
                    Event.gender,
                    func.count(Event.id).label('count')
                ).group_by(Event.gender).all()
                
                db_distribution = {row.gender: row.count for row in results}
                
                logger.info("Actual gender distribution in database:")
                for gender, count in db_distribution.items():
                    logger.info(f"  {gender}: {count}")
                
                # Compare with expected stats
                if not self.dry_run:
                    logger.info("")
                    logger.info("Comparison with processing stats:")
                    logger.info(f"  Expected M: {self.reorganization_stats['M']}, DB has: {db_distribution.get('M', 0)}")
                    logger.info(f"  Expected F: {self.reorganization_stats['F']}, DB has: {db_distribution.get('F', 0)}")
                    logger.info(f"  Expected mixed: {self.reorganization_stats['mixed']}, DB has: {db_distribution.get('mixed', 0)}")
                    logger.info(f"  Expected unknown: {self.reorganization_stats['unknown']}, DB has: {db_distribution.get('unknown', 0)}")
                
                logger.info("=" * 60)
                return db_distribution
                
        except Exception as e:
            logger.error(f"Error verifying database updates: {e}")
            return {}
    
    def print_statistics(self):
        """Print final statistics of the gender correction operation"""
        logger.info("=" * 60)
        logger.info("GENDER CORRECTION COMPLETED")
        logger.info("=" * 60)
        logger.info(f"Total events processed: {self.reorganization_stats['total_events']}")
        logger.info(f"Successful updates: {self.reorganization_stats['successful_updates']}")
        logger.info(f"Failed updates: {self.reorganization_stats['failed_updates']}")
        logger.info(f"API errors: {self.reorganization_stats['api_errors']}")
        logger.info(f"Events skipped (404 - deleted): {self.reorganization_stats['skipped_404']}")
        logger.info("-" * 60)
        logger.info("Gender Distribution (from API processing):")
        logger.info(f"  Male (M): {self.reorganization_stats['M']}")
        logger.info(f"  Female (F): {self.reorganization_stats['F']}")
        logger.info(f"  Mixed: {self.reorganization_stats['mixed']}")
        logger.info(f"  Unknown: {self.reorganization_stats['unknown']}")
        logger.info("=" * 60)


def main():
    """
    Main entry point for the gender reorganization script.
    Run from the workspace root: C:\\Users\\gadie\\Documents\\projects\\sofascore
    """
    parser = argparse.ArgumentParser(
        description="Gender Reorganization CLI - Reorganize events by gender"
    )

    parser.add_argument(
        "--correct-gender",
        action="store_true",
        help="Run gender correction for all events"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without saving them to database"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify gender distribution in database"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of events to process (useful for testing)"
    )

    args = parser.parse_args()
    engine = GenderReorganization(dry_run=args.dry_run)

    if args.correct_gender:
        logger.info("Running gender correction process...")
        if not engine.ensure_gender_column_exists():
            logger.error("❌ Failed to ensure gender column exists. Aborting.")
            return
        events = engine.get_all_events_from_db(only_unknown=True)

        if args.limit:
            logger.info(f"⚙️ Limiting to first {args.limit} events for testing.")
            events = events[:args.limit]
            engine.reorganization_stats['total_events'] = len(events)

        # Procesar con límite aplicado
        for idx, event in enumerate(events, 1):
            logger.info(f"Processing event {idx}/{len(events)}: {event.id}")
            engine.extract_and_update_gender(event.id, event.home_team, event.away_team)
            if idx < len(events):
                time.sleep(0.5)

        engine.print_statistics()

        if not args.dry_run:
            engine.verify_database_updates()

    elif args.verify:
        logger.info("Verifying gender distribution in database...")
        engine.verify_database_updates()

    else:
        parser.print_help()


if __name__ == '__main__':
    main()