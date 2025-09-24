#!/usr/bin/env python3
"""
CSV to PostgreSQL Migration Script
Migrates data from CSV file to PostgreSQL database following the SofaScore schema.

Author: AI Assistant
Date: 2024
Purpose: Migrate Excel/CSV data to PostgreSQL database
"""

import pandas as pd
import sys
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import os
import pytz

# Import your existing models
from models import Base, Event, EventOdds, Result
from config import Config

# Get configuration values
DATABASE_URL = Config.DATABASE_URL
TIMEZONE = Config.TIMEZONE

# Configure logging
logging.basicConfig(
    level=logging.WARNING,  # Only show warnings and errors
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CSVToPostgreSQLMigrator:
    """
    Handles migration of CSV data to PostgreSQL database.
    
    This class provides methods to:
    - Read CSV data
    - Validate data against database schema
    - Insert/update data in PostgreSQL
    - Handle errors and provide detailed logging
    """
    
    def __init__(self, csv_file_path: str, database_url: str = DATABASE_URL):
        """
        Initialize the migrator.
        
        Args:
            csv_file_path: Path to the CSV file to migrate
            database_url: PostgreSQL connection string
        """
        self.csv_file_path = csv_file_path
        self.database_url = database_url
        self.engine = None
        self.session = None
        
        # Timezone configuration
        self.timezone = pytz.timezone(TIMEZONE)
        
        # Statistics tracking
        self.stats = {
            'total_rows': 0,
            'events_created': 0,
            'events_updated': 0,
            'odds_created': 0,
            'odds_updated': 0,
            'results_created': 0,
            'results_updated': 0,
            'errors': 0,
            'conflicts': 0,
            'failed_rows': []
        }
    
    def connect_to_database(self):
        """Establish connection to PostgreSQL database."""
        try:
            self.engine = create_engine(self.database_url)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            logger.info("Successfully connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def read_csv_data(self) -> pd.DataFrame:
        """
        Read and validate CSV data.
        
        Returns:
            pandas.DataFrame: Cleaned CSV data
        """
        try:
            logger.info(f"Reading CSV file: {self.csv_file_path}")
            df = pd.read_csv(self.csv_file_path)
            
            # Basic validation
            if df.empty:
                raise ValueError("CSV file is empty")
            
            logger.info(f"Found {len(df)} rows in CSV")
            self.stats['total_rows'] = len(df)
            
            # Clean data
            df = self._clean_dataframe(df)
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to read CSV: {e}")
            raise
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate the DataFrame.
        
        Args:
            df: Raw DataFrame from CSV
            
        Returns:
            pandas.DataFrame: Cleaned DataFrame
        """
        logger.info("Cleaning and validating data...")
        
        # Replace empty strings with None
        df = df.replace('', None)
        
        # Convert date columns with timezone handling
        if 'start_time_utc' in df.columns:
            # Convert to datetime first
            df['start_time_utc'] = pd.to_datetime(df['start_time_utc'], errors='coerce', dayfirst=True)
            
            # CSV times are already in Mexico City timezone, no conversion needed
            # Just ensure they are naive datetime objects for database storage
            logger.info(f"CSV timestamps are already in {TIMEZONE} timezone - no conversion needed")
        
        # Handle other timestamp columns that might have NaN values
        timestamp_columns = ['ended_at', 'created_at', 'updated_at', 'last_sync_at']
        for col in timestamp_columns:
            if col in df.columns:
                # Convert to datetime, handling NaN values
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                # Replace NaN with None (which becomes NULL in database)
                df[col] = df[col].where(pd.notna(df[col]), None)
                logger.info(f"Processed timestamp column: {col}")
        
        # Convert numeric columns
        numeric_columns = [
            'one_open', 'one_final', 'x_open', 'x_final', 'two_open', 'two_final',
            'var_one', 'var_x', 'var_two', 'home_score', 'away_score'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Handle missing values for required fields
        required_fields = ['slug', 'start_time_utc', 'sport', 'competition', 'home_team', 'away_team']
        for field in required_fields:
            if field in df.columns:
                missing_count = df[field].isna().sum()
                if missing_count > 0:
                    logger.warning(f"Found {missing_count} missing values in required field: {field}")
        
        logger.info("Data cleaning completed")
        return df
    
    def _get_local_now(self):
        """
        Get current time in Mexico City timezone.
        
        Returns:
            datetime: Current time in local timezone (naive, for database storage)
        """
        # Get current UTC time
        utc_now = datetime.utcnow()
        # Convert to Mexico City timezone
        local_now = utc_now.replace(tzinfo=pytz.UTC).astimezone(self.timezone)
        # Return naive datetime (without timezone info) for database storage
        return local_now.replace(tzinfo=None)
    
    def _compare_event_content(self, existing_event, event_data, row):
        """Compare all fields between existing event and CSV data."""
        try:
            # Compare basic event fields
            if (existing_event.slug != row['slug'] or
                existing_event.start_time_utc != row['start_time_utc'] or
                existing_event.sport != row['sport'] or
                existing_event.competition != row['competition'] or
                existing_event.country != row.get('country') or
                existing_event.home_team != row['home_team'] or
                existing_event.away_team != row['away_team']):
                return False
            
            # Compare odds data
            existing_odds = self.session.query(EventOdds).filter(EventOdds.event_id == existing_event.id).first()
            if existing_odds:
                if (existing_odds.one_open != row['one_open'] or
                    existing_odds.one_final != row['one_final'] or
                    existing_odds.x_open != row['x_open'] or
                    existing_odds.x_final != row['x_final'] or
                    existing_odds.two_open != row['two_open'] or
                    existing_odds.two_final != row['two_final'] or
                    existing_odds.var_one != row['var_one'] or
                    existing_odds.var_x != row['var_x'] or
                    existing_odds.var_two != row['var_two']):
                    return False
            
            # Compare results data
            existing_result = self.session.query(Result).filter(Result.event_id == existing_event.id).first()
            if existing_result:
                if (existing_result.home_score != row['home_score'] or
                    existing_result.away_score != row['away_score'] or
                    existing_result.winner != row['winner']):
                    return False
            
            return True
            
        except Exception as e:
            print(f"      Error comparing content: {e}")
            return False
    
    def _process_single_row(self, index: int, row: pd.Series) -> bool:
        """
        Process a single row with individual transaction handling.
        
        Args:
            index: Row index
            row: DataFrame row
            
        Returns:
            bool: True if successful, False if failed
        """
        try:
            # Get event_id from CSV
            csv_event_id = int(row['event_id'])
            
            # Check if event with this ID already exists
            existing_event = self.session.query(Event).filter(
                Event.id == csv_event_id
            ).first()
            
            # Prepare event data
            event_data = {
                'custom_id': row.get('custom_id'),
                'slug': row['slug'],
                'start_time_utc': row['start_time_utc'],
                'sport': row['sport'],
                'competition': row['competition'],
                'country': row.get('country'),
                'home_team': row['home_team'],
                'away_team': row['away_team'],
                'created_at': self._get_local_now() if pd.isna(row.get('created_at')) else row['created_at'],
                'updated_at': self._get_local_now() if pd.isna(row.get('updated_at')) else row['updated_at']
            }
            
            if existing_event:
                # Compare ALL fields to check if content is exactly the same
                content_matches = self._compare_event_content(existing_event, event_data, row)
                
                if content_matches:
                    print(f"  ⏭️  Skipping existing event {csv_event_id}: {row['slug']} (content matches)")
                    return True
                else:
                    # Complete replacement: delete existing odds and results, then recreate
                    print(f"  🔄 COMPLETE REPLACEMENT for event {csv_event_id}: {existing_event.slug} → {row['slug']}")
                    print(f"      Content differs - updating with CSV data")
                    
                    # Delete existing odds and results
                    self.session.query(EventOdds).filter(EventOdds.event_id == existing_event.id).delete()
                    self.session.query(Result).filter(Result.event_id == existing_event.id).delete()
                    
                    # Update all fields with CSV data
                    for key, value in event_data.items():
                        if key != 'id':  # Don't update the ID
                            setattr(existing_event, key, value)
                    
                    self.stats['events_updated'] += 1
                    event_id = existing_event.id
                    
                    # Create new odds and results for this event
                    self._migrate_event_odds(row, event_id)
                    self._migrate_event_results(row, event_id)
            else:
                # Create new event with auto-generated ID
                new_event = Event(**event_data)
                self.session.add(new_event)
                self.session.flush()  # Get the auto-generated ID
                event_id = new_event.id
                self.stats['events_created'] += 1
                print(f"  ➕ Created event {event_id}: {row['slug']}")
                
                # Migrate odds and results for this new event
                self._migrate_event_odds(row, event_id)
                self._migrate_event_results(row, event_id)
            
            # Commit this individual row
            self.session.commit()
            return True
            
        except Exception as e:
            logger.error(f"❌ Error processing event at row {index + 1}: {e}")
            logger.error(f"  Row data: {row.to_dict()}")
            logger.error(f"  Error type: {type(e).__name__}")
            logger.error(f"  Error details: {str(e)}")
            
            # Rollback only this row
            self.session.rollback()
            self.stats['errors'] += 1
            self.stats['failed_rows'].append({
                'row': index + 1,
                'slug': row.get('slug', 'Unknown'),
                'error': str(e),
                'error_type': type(e).__name__
            })
            return False
    
    def migrate_events(self, df: pd.DataFrame):
        """
        Migrate events data to the events table.
        
        Args:
            df: DataFrame containing CSV data
        """
        print(f"🔄 Processing {len(df)} rows...")
        
        for index, row in df.iterrows():
            print(f"\n📋 Row {index + 1}/{len(df)}: {row.get('slug', 'Unknown')}")
            
            # Process each row individually with its own transaction
            success = self._process_single_row(index, row)
            
            if not success:
                print(f"❌ Row {index + 1} failed, continuing...")
                continue
            
            # Show progress every 25 rows
            if (index + 1) % 25 == 0:
                print(f"✅ Progress: {index + 1}/{len(df)} rows processed...")
    
    def _migrate_event_odds(self, row: pd.Series, event_id: int):
        """
        Migrate odds data for a specific event.
        
        Args:
            row: DataFrame row containing event data
            event_id: ID of the event
        """
        try:
            # Check if odds already exist
            existing_odds = self.session.query(EventOdds).filter(
                EventOdds.event_id == event_id
            ).first()
            
            # Prepare odds data
            odds_data = {
                'event_id': event_id,
                'market': row.get('market', '1X2'),
                'one_open': row.get('one_open'),
                'one_final': row.get('one_final'),
                'x_open': row.get('x_open'),
                'x_final': row.get('x_final'),
                'two_open': row.get('two_open'),
                'two_final': row.get('two_final'),
                'last_sync_at': self._get_local_now() if pd.isna(row.get('last_sync_at')) else row['last_sync_at']
            }
            
            if existing_odds:
                # Update existing odds
                for key, value in odds_data.items():
                    if key != 'event_id':  # Don't update the event_id
                        setattr(existing_odds, key, value)
                self.stats['odds_updated'] += 1
                print(f"    📝 Updated odds for event {event_id}")
            else:
                # Create new odds
                new_odds = EventOdds(**odds_data)
                self.session.add(new_odds)
                self.stats['odds_created'] += 1
                print(f"    ➕ Created odds for event {event_id}")
                
        except Exception as e:
            logger.error(f"❌ Error migrating odds for event {event_id}: {e}")
            self.stats['errors'] += 1
    
    def _migrate_event_results(self, row: pd.Series, event_id: int):
        """
        Migrate results data for a specific event.
        
        Args:
            row: DataFrame row containing event data
            event_id: ID of the event
        """
        try:
            # Only migrate if we have score data
            if pd.isna(row.get('home_score')) or pd.isna(row.get('away_score')):
                return
            
            # Check if results already exist
            existing_result = self.session.query(Result).filter(
                Result.event_id == event_id
            ).first()
            
            # Prepare results data
            results_data = {
                'event_id': event_id,
                'home_score': int(row['home_score']) if not pd.isna(row['home_score']) else None,
                'away_score': int(row['away_score']) if not pd.isna(row['away_score']) else None,
                'winner': row.get('winner'),
                'ended_at': row.get('ended_at') if pd.notna(row.get('ended_at')) else None,
                'updated_at': self._get_local_now() if pd.isna(row.get('updated_at')) else row['updated_at']
            }
            
            if existing_result:
                # Update existing results
                for key, value in results_data.items():
                    if key != 'event_id':  # Don't update the event_id
                        setattr(existing_result, key, value)
                self.stats['results_updated'] += 1
                print(f"    📝 Updated results for event {event_id}")
            else:
                # Create new results
                new_result = Result(**results_data)
                self.session.add(new_result)
                self.stats['results_created'] += 1
                print(f"    ➕ Created results for event {event_id}")
                
        except Exception as e:
            logger.error(f"❌ Error migrating results for event {event_id}: {e}")
            self.stats['errors'] += 1
    
    def run_migration(self):
        """
        Execute the complete migration process.
        """
        try:
            logger.info("Starting CSV to PostgreSQL migration...")
            
            # Step 1: Connect to database
            self.connect_to_database()
            
            # Step 2: Read CSV data
            df = self.read_csv_data()
            
            # Step 3: Migrate data
            logger.info("Starting data migration process...")
            self.migrate_events(df)
            
            # Note: Each row is committed individually, no global commit needed
            logger.info("Migration completed successfully!")
            
            # Step 5: Print statistics
            self._print_statistics()
            
        except Exception as e:
            logger.error(f"❌ Migration failed: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
            if self.session:
                logger.info("Rolling back all changes...")
                self.session.rollback()
            raise
        finally:
            if self.session:
                logger.info("Closing database session...")
                self.session.close()
    
    def _print_statistics(self):
        """Print migration statistics and save to JSON."""
        success_rate = ((self.stats['total_rows'] - self.stats['errors']) / self.stats['total_rows']) * 100
        
        # No JSON file creation needed
        
        # Print summary to console
        print(f"\n📊 MIGRATION RESULTS:")
        print(f"   Total rows processed: {self.stats['total_rows']}")
        print(f"   Events created: {self.stats['events_created']}")
        print(f"   Events updated: {self.stats['events_updated']}")
        print(f"   Odds created: {self.stats['odds_created']}")
        print(f"   Odds updated: {self.stats['odds_updated']}")
        print(f"   Results created: {self.stats['results_created']}")
        print(f"   Results updated: {self.stats['results_updated']}")
        print(f"   Errors encountered: {self.stats['errors']}")
        print(f"   Conflicts detected: {self.stats['conflicts']}")
        print(f"   Success rate: {success_rate:.1f}%")
        
        if self.stats['failed_rows']:
            print(f"\n❌ FAILED ROWS ({len(self.stats['failed_rows'])}):")
            for failed_row in self.stats['failed_rows']:
                print(f"   Row {failed_row['row']}: {failed_row['slug']} - {failed_row['error_type']}: {failed_row['error']}")

def main():
    """
    Main function to run the migration.
    """
    # Configuration
    CSV_FILE = "Bets-Hoja24.csv"
    
    # Check if CSV file exists
    if not os.path.exists(CSV_FILE):
        logger.error(f"❌ CSV file not found: {CSV_FILE}")
        sys.exit(1)
    
    # Create and run migrator
    migrator = CSVToPostgreSQLMigrator(CSV_FILE)
    
    try:
        migrator.run_migration()
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

