import logging
from typing import List, Optional, Dict
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from decimal import Decimal

from models import Event, OddsSnapshot, EventOdds, Result, EventObservation, Season, Market, MarketChoice, Bookie
from database import db_manager
from odds_utils import validate_odds_data
from timezone_utils import get_local_now

logger = logging.getLogger(__name__)

try:
    from oddsportal_config import BOOKIE_ALIASES
except ImportError:
    BOOKIE_ALIASES = {}


NBA_SEASONS = [
    {"season_name": "NBA 2020/2021", "season_id": 34951, "year": 2020, "nba_cup_season_id": 0}, 
    {"season_name": "NBA 2021/2022", "season_id": 38191, "year": 2021, "nba_cup_season_id": 0},
    {"season_name": "NBA 2022/2023", "season_id": 45096, "year": 2022, "nba_cup_season_id": 0},
    {"season_name": "NBA 2023/2024", "season_id": 54105, "year": 2023, "nba_cup_season_id": 56094},
    {"season_name": "NBA 2024/2025", "season_id": 65360, "year": 2024, "nba_cup_season_id": 69143},
    {"season_name": "NBA 2025/2026", "season_id": 80229, "year": 2025, "nba_cup_season_id": 84238},
]


class EventRepository:
    """Repository for event-related database operations"""
    
    @staticmethod
    def upsert_event(event_data: Dict) -> Optional[Event]:
        """Insert or update an event"""
        try:
            with db_manager.get_session() as session:
                # Initialize round variable outside conditional to avoid UnboundLocalError
                round = event_data.get('round')
                
                # If season data is provided, create/get season first
                if 'season_id' in event_data and event_data['season_id']:
                    season_id = event_data['season_id']
                    season_name = event_data.get('season_name')
                    season_year = event_data.get('season_year')
                    sport = event_data.get('sport')
                    competition_name = event_data.get('competition')
                    
                    # Parse year if it's a string (e.g., "2024/2025")
                    if season_year and isinstance(season_year, str):
                        year = SeasonRepository._parse_year(season_year)
                    elif season_year:
                        year = season_year
                    else:
                        year = None
                    
                    if season_id in [season['nba_cup_season_id'] for season in NBA_SEASONS] and 'nba cup' in competition_name.lower():
                        round = 'knockouts/playoffs'
                    
                    # Create/get season if we have enough info
                    if season_name and year and sport:
                        SeasonRepository.get_or_create_season(season_id, season_name, year, sport)
                    elif season_name and sport:
                        # Try to parse year from season_name if year not provided
                        parsed_year = SeasonRepository._parse_year(season_name)
                        if parsed_year:
                            SeasonRepository.get_or_create_season(season_id, season_name, parsed_year, sport)
                
                # Check if event exists
                event = session.query(Event).filter(Event.id == event_data['id']).first()
                
                if event:
                    # Update existing event
                    event.custom_id = event_data.get('customId')
                    event.slug = event_data['slug']
                    event.start_time_utc = datetime.fromtimestamp(event_data['startTimestamp'])
                    event.sport = event_data['sport']
                    event.competition = event_data['competition']
                    event.country = event_data.get('country')
                    event.home_team = event_data['homeTeam']
                    event.away_team = event_data['awayTeam']
                    # Ensure gender is valid: not None, max 10 chars, default to "unknown"
                    gender = event_data.get('gender') or 'unknown'
                    event.gender = gender[:10] if len(gender) > 10 else gender
                    # Update discovery_source: always overwrite for 'dropping_odds' (highest priority source)
                    # For other sources, preserve the original discovery source
                    if event_data.get('discovery_source') == 'dropping_odds':
                        # Always overwrite with dropping_odds (most important source - takes priority over any existing source)
                        old_source = event.discovery_source
                        if old_source != 'dropping_odds':
                            event.discovery_source = 'dropping_odds'
                            logger.debug(f"Overwrote discovery_source to 'dropping_odds' for event {event_data['id']} (was: {old_source})")
                    
                    # Update season_id and round if provided
                    # This allows the results sync to fill in missing information for existing events
                    if event_data.get('season_id'):
                        event.season_id = event_data['season_id']
                    if round:
                        existing = (event.round or "").lower()
                        incoming = str(round).lower()

                        # Prevent downgrade
                        if existing == "knockouts/playoffs" and incoming == "regular_season":
                            pass
                        else:
                            event.round = round

                    event.updated_at = get_local_now()
                    logger.info(f"Updated event {event_data['id']}")
                else:
                    # Ensure gender is valid: not None, max 10 chars, default to "unknown"
                    gender = event_data.get('gender') or 'unknown'
                    gender = gender[:10] if len(gender) > 10 else gender
                    
                    # Create new event
                    event = Event(
                        id=event_data['id'],
                        custom_id=event_data.get('customId'),
                        slug=event_data['slug'],
                        start_time_utc=datetime.fromtimestamp(event_data['startTimestamp']),
                        sport=event_data['sport'],
                        competition=event_data['competition'],
                        country=event_data.get('country'),
                        home_team=event_data['homeTeam'],
                        away_team=event_data['awayTeam'],
                        gender=gender,
                        discovery_source=event_data.get('discovery_source', 'dropping_odds'),
                        season_id=event_data.get('season_id'),
                        round=round
                    )
                    session.add(event)
                    logger.debug(f"Created new event {event_data['id']}")
                
                return event
                
        except Exception as e:
            logger.error(f"Error upserting event {event_data.get('id')}: {e}")
            return None
    
    @staticmethod
    def get_event_by_id(event_id: int) -> Optional[Event]:
        """Get event by ID with event_odds loaded"""
        try:
            with db_manager.get_session() as session:
                from sqlalchemy.orm import joinedload
                return session.query(Event).options(joinedload(Event.event_odds)).filter(Event.id == event_id).first()
        except Exception as e:
            logger.error(f"Error getting event {event_id}: {e}")
            return None
    
    @staticmethod
    def update_event_starting_time(event_id: int, new_start_time: datetime) -> bool:
        """Update the starting time of an event"""
        try:
            with db_manager.get_session() as session:
                event = session.query(Event).filter(Event.id == event_id).first()
                if event:
                    event.start_time_utc = new_start_time
                    event.updated_at = get_local_now()
                    session.commit()
                    logger.info(f"Updated starting time for event {event_id} to {new_start_time}")
                    return True
                else:
                    logger.warning(f"Event {event_id} not found for starting time update")
                    return False
        except Exception as e:
            logger.error(f"Error updating starting time for event {event_id}: {e}")
            return False
    
    @staticmethod
    def delete_event(event_id: int) -> bool:
        """Delete an event and all its related data (odds, results, observations)"""
        try:
            with db_manager.get_session() as session:
                # Get the event first to check if it exists and capture season_id
                event = session.query(Event).filter(Event.id == event_id).first()
                if not event:
                    logger.warning(f"Event {event_id} not found for deletion")
                    return False
                
                # Capture season_id before deletion for orphaned season cleanup
                season_id_to_check = event.season_id
                
                # Delete related data first (due to foreign key constraints)
                # Delete odds snapshots
                session.query(OddsSnapshot).filter(OddsSnapshot.event_id == event_id).delete()
                
                # Delete event odds
                session.query(EventOdds).filter(EventOdds.event_id == event_id).delete()
                
                # Delete results
                session.query(Result).filter(Result.event_id == event_id).delete()
                
                # Delete observations
                session.query(EventObservation).filter(EventObservation.event_id == event_id).delete()
                
                # Finally delete the event
                session.query(Event).filter(Event.id == event_id).delete()
                
                # Clean up orphaned season if this was the last event for that season
                # This handles the case where events are deleted after 404 errors during odds fetching
                if season_id_to_check:
                    # Check if any other events exist for this season
                    remaining_events = session.query(Event).filter(Event.season_id == season_id_to_check).count()
                    if remaining_events == 0:
                        # No events left for this season, delete it
                        session.query(Season).filter(Season.id == season_id_to_check).delete()
                        logger.info(f"🧹 Cleaned up orphaned season {season_id_to_check} after event deletion")
                
                logger.info(f"✅ Deleted event {event_id} and all related data")
                return True
                
        except Exception as e:
            logger.error(f"Error deleting event {event_id}: {e}")
            return False
    
    @staticmethod
    def batch_delete_events(event_ids: List[int]) -> int:
        """
        Batch delete multiple events and all their related data in a single transaction.
        Much faster than deleting events one by one.
        
        Args:
            event_ids: List of event IDs to delete
            
        Returns:
            Number of events successfully deleted
        """
        if not event_ids:
            return 0
        
        try:
            with db_manager.get_session() as session:
                # Delete related data first (due to foreign key constraints)
                # Use bulk delete with IN clause for efficiency
                
                # Delete odds snapshots
                session.query(OddsSnapshot).filter(OddsSnapshot.event_id.in_(event_ids)).delete(synchronize_session=False)
                
                # Delete event odds
                session.query(EventOdds).filter(EventOdds.event_id.in_(event_ids)).delete(synchronize_session=False)
                
                # Delete results
                session.query(Result).filter(Result.event_id.in_(event_ids)).delete(synchronize_session=False)
                
                # Delete observations
                session.query(EventObservation).filter(EventObservation.event_id.in_(event_ids)).delete(synchronize_session=False)
                
                # Finally delete the events
                deleted_count = session.query(Event).filter(Event.id.in_(event_ids)).delete(synchronize_session=False)
                
                # Clean up orphaned seasons (seasons with exactly 0 events)
                # This handles the case where events are deleted after 404 errors during odds fetching
                # Get all seasons and check event count for each
                all_seasons = session.query(Season).all()
                orphaned_season_ids = []
                
                for season in all_seasons:
                    # Count events for this season - only delete if count is exactly 0
                    event_count = session.query(Event).filter(Event.season_id == season.id).count()
                    if event_count == 0:
                        orphaned_season_ids.append(season.id)
                
                # Only delete seasons that have exactly 0 events
                if orphaned_season_ids:
                    deleted_seasons_count = session.query(Season).filter(
                        Season.id.in_(orphaned_season_ids)
                    ).delete(synchronize_session=False)
                    logger.info(f"🧹 Cleaned up {deleted_seasons_count} orphaned season(s) with 0 events after batch deletion")
                
                logger.info(f"✅ Batch deleted {deleted_count} events and all related data")
                return deleted_count
                
        except Exception as e:
            logger.error(f"Error batch deleting events: {e}")
            return 0
    

    def get_events_starting_soon(self, window_minutes: int = 30):
        """Get events starting within the specified window as Event objects"""
        try:
            with db_manager.get_session() as session:
                from datetime import datetime, timedelta
                from sqlalchemy.orm import joinedload
                
                # Use local time since SofaScore provides local times (despite column name)
                now = datetime.now()
                start_window = now + timedelta(minutes=window_minutes)
                
                events = session.query(Event).options(
                    joinedload(Event.event_odds)
                ).filter(
                    Event.start_time_utc.between(now, start_window)
                ).all()
                
                return events
        except Exception as e:
            logger.error(f"Error getting events starting soon: {e}")
            return []

    @staticmethod
    def get_events_starting_soon_with_odds(window_minutes: int = 30) -> List[Dict]:
        """
        Get events starting within the specified window WITH their odds data.
        Returns a list of dictionaries containing event info and odds.
        """
        try:
            with db_manager.get_session() as session:
                # Use local time since SofaScore provides local times
                now = datetime.now()
                window_start = now
                window_end = now + timedelta(minutes=window_minutes)
                
                # Debug logging
                logger.debug(f"Looking for events with odds between {window_start} and {window_end}")
                
                # JOIN Event with EventOdds to get events with odds data
                from sqlalchemy.orm import joinedload
                
                events_with_odds = session.query(Event).options(
                    joinedload(Event.event_odds)
                ).filter(
                    and_(
                        Event.start_time_utc >= window_start,
                        Event.start_time_utc <= window_end
                    )
                ).all()
                
                # Convert to list of dictionaries with event info and odds
                result = []
                for event in events_with_odds:
                    event_data = {
                        'id': event.id,
                        'home_team': event.home_team,
                        'away_team': event.away_team,
                        'competition': event.competition,
                        'start_time_utc': event.start_time_utc,
                        'sport': event.sport,
                        'country': event.country,
                        'slug': event.slug,
                        'season_id': event.season_id,
                        'odds': None  # Will be populated if odds exist
                    }
                    
                    # Add odds data if available
                    if hasattr(event, 'event_odds') and event.event_odds:
                        odds = event.event_odds
                        event_data['odds'] = {
                            'one_open': odds.one_open,
                            'x_open': odds.x_open,
                            'two_open': odds.two_open,
                            'one_final': odds.one_final,
                            'x_final': odds.x_final,
                            'two_final': odds.two_final
                        }
                        logger.debug(f"Event {event.id}: Odds found - 1:{odds.one_final}, X:{odds.x_final}, 2:{odds.two_final}")
                    else:
                        logger.debug(f"Event {event.id}: No odds data available")
                    
                    result.append(event_data)
                
                # Additional debug: log what we found
                if result:
                    for event_data in result:
                        time_until_start = event_data['start_time_utc'] - now
                        minutes_until_start = round(time_until_start.total_seconds() / 60)
                        odds_status = "with odds" if event_data['odds'] else "without odds"
                        logger.debug(f"Event {event_data['id']}: {event_data['home_team']} vs {event_data['away_team']} starts in {minutes_until_start} minutes ({odds_status})")
                else:
                    logger.debug("No events found in time window")
                return result
                
        except Exception as e:
            logger.error(f"Error getting events starting soon with odds: {e}")
            return []
    
    @staticmethod
    def get_events_started_recently(window_minutes: int = 15) -> List[Dict]:
        """
        Get events that started within the last N minutes (default 15).
        Used to catch late timestamp corrections that occur after the game starts.
        Returns a list of dictionaries containing event info.
        """
        try:     
            with db_manager.get_session() as session:
                # Use consistent timezone utility for time calculations
                now = get_local_now()
                # Round window_start down to seconds (remove microseconds) to catch events that started exactly at that time
                window_start = now - timedelta(minutes=window_minutes, seconds=10)
                window_start = window_start.replace(microsecond=0)  # Round down to second precision
                window_end = now  # Up to current time
                
                # Get events that started recently (within the last N minutes)
                # Use > instead of >= for window_end to avoid catching events that haven't started yet
                events_started_recently = session.query(Event).filter(
                    and_(
                        Event.start_time_utc >= window_start,
                        Event.start_time_utc < now  # Must have already started (strictly less than now)
                    )
                ).all()
                
                # Convert to list of dictionaries with event info
                result = []
                for event in events_started_recently:
                    event_data = {
                        'id': event.id,
                        'home_team': event.home_team,
                        'away_team': event.away_team,
                        'competition': event.competition,
                        'start_time_utc': event.start_time_utc,
                        'sport': event.sport,
                        'country': event.country,
                        'slug': event.slug
                    }
                    result.append(event_data)
                
                return result
                
        except Exception as e:
            logger.error(f"Error getting events started recently: {e}")
            return []
    
    @staticmethod
    def get_todays_events() -> List[Event]:
        """Get all events for today"""
        try:
            with db_manager.get_session() as session:
                # Use local time since SofaScore provides local times
                today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                today_end = today_start + timedelta(days=1)
                
                events = session.query(Event).filter(
                    and_(
                        Event.start_time_utc >= today_start,
                        Event.start_time_utc < today_end
                    )
                ).all()
                
                logger.info(f"Found {len(events)} events for today")
                return events
                
        except Exception as e:
            logger.error(f"Error getting today's events: {e}")
            return []
    
    @staticmethod
    def get_events_by_date(target_date) -> List[Event]:
        """Get all events for a specific date"""
        try:
            with db_manager.get_session() as session:
                # Convert date to datetime range
                if hasattr(target_date, 'date'):
                    target_date = target_date.date()
                
                day_start = datetime.combine(target_date, datetime.min.time())
                day_end = day_start + timedelta(days=1)
                
                events = session.query(Event).filter(
                    and_(
                        Event.start_time_utc >= day_start,
                        Event.start_time_utc < day_end
                    )
                ).all()
                
                logger.info(f"Found {len(events)} events for {target_date}")
                return events
                
        except Exception as e:
            logger.error(f"Error getting events for date {target_date}: {e}")
            return []
    
    @staticmethod
    def get_all_finished_events() -> List[Event]:
        """Get all events that should be finished based on sport-specific durations."""
        try:
            with db_manager.get_session() as session:
                now = datetime.now()
                
                # Sport-specific cutoff times (Reason: Different sports have different durations)
                events = session.query(Event).filter(
                    or_(
                        # Football/Soccer: 2.5 hours (90min + extra time + halftime)
                        and_(Event.sport.in_(['Football', 'Futsal']), Event.start_time_utc < now - timedelta(hours=2.5)),
                        # Tennis: 4 hours (can be very long matches)
                        and_(Event.sport == 'Tennis', Event.start_time_utc < now - timedelta(hours=4)),
                        # Baseball: 4 hours (can be very long games)
                        and_(Event.sport == 'Baseball', Event.start_time_utc < now - timedelta(hours=4)),
                        # Basketball: 3 hours (48min + timeouts + overtime)
                        and_(Event.sport == 'Basketball', Event.start_time_utc < now - timedelta(hours=3)),
                        # Other sports: 3 hours default
                        and_(~Event.sport.in_(['Football', 'Futsal', 'Tennis', 'Baseball', 'Basketball']), Event.start_time_utc < now - timedelta(hours=3))
                    )
                ).all()
                
                logger.info(f"Found {len(events)} finished events")
                return events
                
        except Exception as e:
            logger.error(f"Error getting finished events: {e}")
            return []

class SeasonRepository:
    """Repository for season-related database operations"""
    
    @staticmethod
    def _parse_season_name(season_name: str, unique_tournament_name: str) -> str:
        if not season_name:
            return season_name  # or return "" if you prefer

        # Check if there is at least one alphabetical character anywhere
        has_alpha = any(ch.isalpha() for ch in season_name)

        # If there are NO alphabetical characters, prefix the unique_tournament_name
        if not has_alpha:
            return f"{unique_tournament_name} {season_name}"

        # Otherwise, leave it as is
        return season_name


    @staticmethod
    def _parse_year(year_str: str) -> int:
        if not year_str:
            return None

        try:
            import re
            year_str = str(year_str)

            # First, try to find a 4-digit year pattern (e.g., "2020", "2023", "1999")
            # Look for patterns like "2020/2021" or "2020-2021" or just "2020"
            four_digit_pattern = r'\b(19|20)\d{2}\b'
            four_digit_match = re.search(four_digit_pattern, year_str)
            
            if four_digit_match:
                year_int = int(four_digit_match.group())
                return year_int
            
            # If no 4-digit year found, look for 2-digit years (e.g., "20/21", "24/25")
            # Extract the first number before a slash or the first number in the string
            if '/' in year_str:
                # Split by '/' and get the first part (e.g., "NBA 20" from "NBA 20/21")
                year_part = year_str.split('/')[0].strip()
            else:
                year_part = year_str.strip()

            # Extract all digits from the year_part
            digits = re.findall(r'\d+', year_part)
            if digits:
                year_int = int(digits[0])
                # Convert 2-digit years to 4-digit years (e.g., 20 -> 2020, 24 -> 2024)
                if year_int < 100:
                    # Assume years 00-99 are 2000-2099
                    return 2000 + year_int
                # If already 4 digits or more, return as is
                return year_int

            # Last resort: try to convert the whole year_part to int
            year_int = int(year_part)
            if year_int < 100:
                return 2000 + year_int
            return year_int

        except (ValueError, TypeError):
            logger.warning(f"Could not parse year string: {year_str}")
            return None
    
    @staticmethod
    def get_or_create_season(season_id: int, name: str, year: int, sport: str) -> Optional[Season]:
        """
        Get existing season or create new one if it doesn't exist.
        Updates season info if it changed.
        
        Args:
            season_id: SofaScore season ID (unique identifier)
            name: Season name (e.g., "NBA 24/25")
            year: Season year (e.g., 2024)
            sport: Sport name (e.g., "Basketball")
            
        Returns:
            Season object if successful, None otherwise
        """
        if not season_id:
            return None
        
        try:
            with db_manager.get_session() as session:
                # Check if season exists
                season = session.query(Season).filter(Season.id == season_id).first()
                
                if season:
                    # Update existing season if info changed
                    updated = False
                    if season.name != name:
                        season.name = name
                        updated = True
                    if season.year != year:
                        season.year = year
                        updated = True
                    if season.sport != sport:
                        season.sport = sport
                        updated = True
                    
                    if updated:
                        logger.debug(f"Updated season {season_id}: {name}")
                    return season
                else:
                    # Create new season
                    season = Season(
                        id=season_id,
                        name=name,
                        year=year,
                        sport=sport
                    )
                    session.add(season)
                    logger.debug(f"Created new season {season_id}: {name} (Year: {year}, Sport: {sport})")
                    return season
                    
        except Exception as e:
            logger.error(f"Error getting/creating season {season_id}: {e}")
            return None

class OddsRepository:
    """Repository for odds-related database operations"""
    
    @staticmethod
    def create_odds_snapshot(event_id: int, odds_data: Dict) -> Optional[OddsSnapshot]:
        """Create a new odds snapshot"""
        try:
            # Validate odds data (function handles both complete and final odds)
            if not validate_odds_data(odds_data):
                logger.debug(f"Invalid odds data for event {event_id}")
                return None
            
            with db_manager.get_session() as session:
                # Create snapshot with appropriate odds data
                if ('one_open' in odds_data and 'one_cur' in odds_data) or ('one_initial' in odds_data and 'one_final' in odds_data):
                    # Complete odds snapshot (from discovery) - has both opening and current odds
                    # Handle both 'open'/'cur' and 'initial'/'final' naming conventions
                    one_open = odds_data.get('one_open') or odds_data.get('one_initial')
                    x_open = odds_data.get('x_open') or odds_data.get('x_initial')
                    two_open = odds_data.get('two_open') or odds_data.get('two_initial')
                    one_cur = odds_data.get('one_cur') or odds_data.get('one_final')
                    x_cur = odds_data.get('x_cur') or odds_data.get('x_final')
                    two_cur = odds_data.get('two_cur') or odds_data.get('two_final')
                    
                    snapshot = OddsSnapshot(
                        event_id=event_id,
                        collected_at=get_local_now(),
                        market='1X2',
                        one_open=one_open,
                        x_open=x_open,
                        two_open=two_open,
                        one_cur=one_cur,
                        x_cur=x_cur,
                        two_cur=two_cur
                    )
                else:
                    # Final odds snapshot (from pre-start or discovery final odds) - only current/final odds
                    # Map one_final to one_cur, etc. for consistency
                    one_cur = odds_data.get('one_final') or odds_data.get('one_cur')
                    x_cur = odds_data.get('x_final') or odds_data.get('x_cur')
                    two_cur = odds_data.get('two_final') or odds_data.get('two_cur')
                    
                    snapshot = OddsSnapshot(
                        event_id=event_id,
                        collected_at=get_local_now(),
                        market='1X2',
                        one_open=None,  # Not available for final odds only
                        x_open=None,    # Not available for final odds only
                        two_open=None,  # Not available for final odds only
                        one_cur=one_cur,
                        x_cur=x_cur,
                        two_cur=two_cur
                    )
                
                # Store raw fractional data
                snapshot.set_raw_fractional(odds_data.get('raw_fractional', {}))
                
                session.add(snapshot)
                
                
                logger.info(f"Created odds snapshot for event {event_id}")
                return snapshot
                
        except Exception as e:
            logger.error(f"Error creating odds snapshot for event {event_id}: {e}")
            return None
    
    @staticmethod
    def upsert_event_odds(event_id: int, odds_data: Dict) -> Optional[EventOdds]:
        """Insert or update event odds"""
        try:
            with db_manager.get_session() as session:
                event_odds = session.query(EventOdds).filter(EventOdds.event_id == event_id).first()
                
                if event_odds:
                    # Update existing record - handle both 'open' and 'initial' naming conventions
                    if odds_data.get('one_open') is not None:
                        event_odds.one_open = odds_data['one_open']
                    elif odds_data.get('one_initial') is not None:
                        event_odds.one_open = odds_data['one_initial']
                    
                    if odds_data.get('x_open') is not None:
                        event_odds.x_open = odds_data['x_open']
                    elif odds_data.get('x_initial') is not None:
                        event_odds.x_open = odds_data['x_initial']
                    
                    if odds_data.get('two_open') is not None:
                        event_odds.two_open = odds_data['two_open']
                    elif odds_data.get('two_initial') is not None:
                        event_odds.two_open = odds_data['two_initial']
                    
                    # Always update final odds (handle both naming conventions)
                    event_odds.one_final = odds_data.get('one_final') or odds_data.get('one_cur')
                    event_odds.x_final = odds_data.get('x_final') or odds_data.get('x_cur')
                    event_odds.two_final = odds_data.get('two_final') or odds_data.get('two_cur')
                    event_odds.last_sync_at = get_local_now()
                    
                    logger.debug(f"Updated event odds for event {event_id}")
                else:
                    # Create new record - handle both 'open' and 'initial' naming conventions
                    event_odds = EventOdds(
                        event_id=event_id,
                        market='1X2',
                        one_open=odds_data.get('one_open') or odds_data.get('one_initial'),
                        x_open=odds_data.get('x_open') or odds_data.get('x_initial'),
                        two_open=odds_data.get('two_open') or odds_data.get('two_initial'),
                        one_final=odds_data.get('one_final') or odds_data.get('one_cur'),
                        x_final=odds_data.get('x_final') or odds_data.get('x_cur'),
                        two_final=odds_data.get('two_final') or odds_data.get('two_cur'),
                        last_sync_at=get_local_now()
                    )
                    session.add(event_odds)
                    logger.debug(f"Created new event odds for event {event_id}")
                
                # Store event_id before commit to avoid session issues
                stored_event_id = event_odds.event_id
                
                # Return the event_id instead of the object to avoid session issues
                return stored_event_id
                
        except Exception as e:
            logger.error(f"Error upserting event odds for event {event_id}: {e}")
            return None
    
    @staticmethod
    def get_event_odds(event_id: int) -> Optional[EventOdds]:
        """Get current odds for an event with event relationship loaded"""
        try:
            with db_manager.get_session() as session:
                from sqlalchemy.orm import joinedload
                return session.query(EventOdds).options(joinedload(EventOdds.event)).filter(EventOdds.event_id == event_id).first()
        except Exception as e:
            logger.error(f"Error getting event odds for event {event_id}: {e}")
            return None
    
    @staticmethod
    def get_odds_snapshots(event_id: int, limit: int = 10) -> List[OddsSnapshot]:
        """Get recent odds snapshots for an event"""
        try:
            with db_manager.get_session() as session:
                snapshots = session.query(OddsSnapshot).filter(
                    OddsSnapshot.event_id == event_id
                ).order_by(OddsSnapshot.collected_at.desc()).limit(limit).all()
                
                return snapshots
                
        except Exception as e:
            logger.error(f"Error getting odds snapshots for event {event_id}: {e}")
            return []


class ResultRepository:
    """Repository for result-related database operations"""
    
    @staticmethod
    def upsert_result(event_id: int, result_data: Dict) -> Optional[Result]:
        """Insert or update a result"""
        try:
            with db_manager.get_session() as session:
                result = session.query(Result).filter(Result.event_id == event_id).first()
                
                if result:
                    # Update existing result
                    result.home_score = result_data.get('home_score')
                    result.away_score = result_data.get('away_score')
                    result.winner = result_data.get('winner')
                    result.home_sets = result_data.get('home_sets')
                    result.away_sets = result_data.get('away_sets')
                else:
                    # Create new result
                    result = Result(
                        event_id=event_id,
                        home_score=result_data.get('home_score'),
                        away_score=result_data.get('away_score'),
                        winner=result_data.get('winner'),
                        home_sets=result_data.get('home_sets'),
                        away_sets=result_data.get('away_sets')
                    )
                    session.add(result)
                
                
                return result
                
        except Exception as e:
            logger.error(f"Error upserting result for event {event_id}: {e}")
            return None
    
    @staticmethod
    def get_result_by_event_id(event_id: int) -> Optional[Result]:
        """Get result by event ID"""
        try:
            with db_manager.get_session() as session:
                event = session.query(Result).filter(Result.event_id == event_id).first()
                return event
        except Exception as e:
            logger.error(f"Error getting result for event {event_id}: {e}")
            return None


class ObservationRepository:
    """Repository for event observation-related database operations"""
    
    @staticmethod
    def upsert_observation(event_id: int, sport: str, observation_type: str, observation_value: str) -> Optional[EventObservation]:
        """
        Insert or update an event observation.
        FAIL-SAFE: Returns None on any error, doesn't break main flow.
        """
        try:
            with db_manager.get_session() as session:
                # Check if observation exists
                observation = session.query(EventObservation).filter(
                    and_(
                        EventObservation.event_id == event_id,
                        EventObservation.observation_type == observation_type
                    )
                ).first()
                
                if observation:
                    # Update existing observation
                    observation.observation_value = observation_value
                    observation.sport = sport
                    observation.updated_at = get_local_now()
                    logger.debug(f"Updated observation {observation_type} for event {event_id}")
                else:
                    # Create new observation
                    observation = EventObservation(
                        event_id=event_id,
                        observation_type=observation_type,
                        observation_value=observation_value,
                        sport=sport
                    )
                    session.add(observation)
                    logger.debug(f"Created new observation {observation_type} for event {event_id}")
                
                return observation
                
        except Exception as e:
            logger.warning(f"Error upserting observation {observation_type} for event {event_id}: {e}")
            # FAIL-SAFE: Return None, don't break main processing
            return None
    
    @staticmethod
    def get_observation(event_id: int, observation_type: str) -> Optional[EventObservation]:
        """
        Get a specific observation for an event.
        FAIL-SAFE: Returns None if not found or on error.
        """
        try:
            with db_manager.get_session() as session:
                return session.query(EventObservation).filter(
                    and_(
                        EventObservation.event_id == event_id,
                        EventObservation.observation_type == observation_type
                    )
                ).first()
        except Exception as e:
            logger.warning(f"Error getting observation {observation_type} for event {event_id}: {e}")
            # FAIL-SAFE: Return None, don't break main processing
            return None
    
    @staticmethod
    def get_all_observations(event_id: int) -> List[EventObservation]:
        """
        Get all observations for an event.
        FAIL-SAFE: Returns empty list on error.
        """
        try:
            with db_manager.get_session() as session:
                return session.query(EventObservation).filter(
                    EventObservation.event_id == event_id
                ).all()
        except Exception as e:
            logger.warning(f"Error getting observations for event {event_id}: {e}")
            # FAIL-SAFE: Return empty list, don't break main processing
            return []
    
    @staticmethod
    def get_observations_by_type(observation_type: str, sport: str = None) -> List[EventObservation]:
        """
        Get all observations of a specific type, optionally filtered by sport.
        FAIL-SAFE: Returns empty list on error.
        """
        try:
            with db_manager.get_session() as session:
                query = session.query(EventObservation).filter(
                    EventObservation.observation_type == observation_type
                )
                
                if sport:
                    query = query.filter(EventObservation.sport == sport)
                
                return query.all()
        except Exception as e:
            logger.warning(f"Error getting observations by type {observation_type}: {e}")
            # FAIL-SAFE: Return empty list, don't break main processing
            return []


class MarketRepository:
    """
    Repository for storing and retrieving dynamic odds markets.
    
    Each event can have multiple markets (Full time, Match goals 2.5, Asian handicap, etc.)
    Each market has multiple choices stored in MarketChoice table.
    """
    
    @staticmethod
    def _fractional_to_decimal(fractional: str) -> float:
        """
        Convert fractional odds to decimal.
        
        Examples:
            "53/100" -> 1.53
            "27/10" -> 3.7
            "17/4" -> 5.25
        """
        try:
            if not fractional or '/' not in fractional:
                return None
            
            numerator, denominator = fractional.split('/')
            # Decimal odds = (numerator / denominator) + 1
            return round(float(numerator) / float(denominator) + 1, 3)
        except (ValueError, ZeroDivisionError):
            return None
    
    @staticmethod
    def save_markets_from_response(event_id: int, odds_response: Dict, bookie_id: int = 1) -> int:
        """
        Save all markets from an odds API response to the database.
        
        Args:
            event_id: Event ID to associate markets with
            odds_response: Raw API odds response containing 'markets' array
            bookie_id: Bookie ID to associate markets with (default: 1 = SofaScore)
            
        Returns:
            Number of markets saved
        """
        try:
            markets_data = odds_response.get('markets', [])
            if not markets_data:
                logger.debug(f"No markets in odds response for event {event_id}")
                return 0
            
            saved_count = 0
            
            with db_manager.get_session() as session:
                for market_data in markets_data:
                    try:
                        # Extract market info
                        market_name = market_data.get('marketName')
                        choice_group = market_data.get('choiceGroup')  # e.g., "2.5" for Over/Under
                        
                        if not market_name:
                            continue
                        
                        # Check if market already exists (upsert by event+bookie+name+line)
                        existing_market = session.query(Market).filter(
                            and_(
                                Market.event_id == event_id,
                                Market.bookie_id == bookie_id,
                                Market.market_name == market_name,
                                Market.choice_group == choice_group
                            )
                        ).first()
                        
                        if existing_market:
                            # Update existing market
                            market = existing_market
                            market.market_group = market_data.get('marketGroup')
                            market.market_period = market_data.get('marketPeriod')
                            market.collected_at = get_local_now()
                        else:
                            # Create new market
                            market = Market(
                                event_id=event_id,
                                bookie_id=bookie_id,
                                market_name=market_name,
                                market_group=market_data.get('marketGroup'),
                                market_period=market_data.get('marketPeriod'),
                                choice_group=choice_group,
                                collected_at=get_local_now()
                            )
                            session.add(market)
                            session.flush()  # Get market_id for choices
                        
                        # Process choices for this market
                        # IMPORTANT: Deduplicate choices by name first (API sometimes returns duplicates)
                        # Keep only the first occurrence of each choice name
                        choices_data = market_data.get('choices', [])
                        seen_choice_names = {}
                        for choice_data in choices_data:
                            choice_name = choice_data.get('name')
                            if choice_name and choice_name not in seen_choice_names:
                                seen_choice_names[choice_name] = choice_data
                        
                        for choice_name, choice_data in seen_choice_names.items():
                            # Convert fractional odds to decimal
                            initial_fractional = choice_data.get('initialFractionalValue')
                            current_fractional = choice_data.get('fractionalValue')
                            initial_odds = MarketRepository._fractional_to_decimal(initial_fractional)
                            current_odds = MarketRepository._fractional_to_decimal(current_fractional)
                            change = choice_data.get('change', 0)
                            
                            # Check if choice already exists (upsert)
                            existing_choice = session.query(MarketChoice).filter(
                                and_(
                                    MarketChoice.market_id == market.market_id,
                                    MarketChoice.choice_name == choice_name
                                )
                            ).first()
                            
                            if existing_choice:
                                # Update existing choice (only current_odds and change)
                                existing_choice.current_odds = current_odds
                                existing_choice.change = change
                            else:
                                # Create new choice
                                choice = MarketChoice(
                                    market_id=market.market_id,
                                    choice_name=choice_name,
                                    initial_odds=initial_odds,
                                    current_odds=current_odds,
                                    change=change
                                )
                                session.add(choice)
                                session.flush()  # Flush to avoid duplicate key errors within same transaction
                        
                        saved_count += 1
                        
                    except Exception as e:
                        logger.warning(f"Error processing market for event {event_id}: {e}")
                        # Rollback to recover from failed transaction
                        session.rollback()
                        continue
                
                session.commit()
                logger.info(f"✅ Saved {saved_count} markets for event {event_id}")
                return saved_count
                
        except Exception as e:
            logger.error(f"Error saving markets for event {event_id}: {e}")
            return 0
    
    @staticmethod
    def get_markets_for_event(event_id: int) -> List[Market]:
        """
        Get all markets for an event with their choices loaded.
        
        Args:
            event_id: Event ID
            
        Returns:
            List of Market objects with choices
        """
        try:
            with db_manager.get_session() as session:
                from sqlalchemy.orm import joinedload
                markets = session.query(Market).options(
                    joinedload(Market.choices)
                ).filter(Market.event_id == event_id).all()
                
                return markets
        except Exception as e:
            logger.error(f"Error getting markets for event {event_id}: {e}")
            return []
    
    @staticmethod
    def get_market_count(event_id: int) -> int:
        """
        Get the number of unique markets for an event.
        
        Args:
            event_id: Event ID
            
        Returns:
            Count of markets
        """
        try:
            with db_manager.get_session() as session:
                count = session.query(Market).filter(Market.event_id == event_id).count()
                return count
        except Exception as e:
            return 0
    
    @staticmethod
    def _get_or_create_bookie(session: Session, name: str) -> Bookie:
        """Get or create a bookie by name, utilizing aliases."""
        # mapping to standard DB names
        db_name = BOOKIE_ALIASES.get(name, name)
        
        # Try to find by name
        bookie = session.query(Bookie).filter(Bookie.name == db_name).first()
        if bookie:
            return bookie
            
        # Try to find by slug (fallback)
        slug = db_name.lower().replace(' ', '-').replace('.', '')
        bookie = session.query(Bookie).filter(Bookie.slug == slug).first()
        if bookie:
            return bookie
            
        # Create new
        bookie = Bookie(name=db_name, slug=slug)
        session.add(bookie)
        session.flush()
        return bookie

    @staticmethod
    def save_markets_from_oddsportal(event_id: int, odds_data: object) -> int:
        """
        Save markets from OddsPortal scraper data.
        
        Args:
            event_id: Event ID
            odds_data: MatchOddsData object from scraper
            
        Returns:
            Number of saved market/bookie combinations
        """
        try:
            if not odds_data or not odds_data.bookie_odds:
                return 0
                
            saved_count = 0
            
            with db_manager.get_session() as session:
                # 1. Process standard bookies
                for b_odds in odds_data.bookie_odds:
                    try:
                        bookie = MarketRepository._get_or_create_bookie(session, b_odds.name)
                        
                        # We only get 1X2 market from this scraper for now
                        market_name = "Full time"
                        market_group = "1X2"
                        market_period = "Full-time"
                        
                        # Get/Create Market
                        market = session.query(Market).filter(
                             and_(
                                 Market.event_id == event_id,
                                 Market.bookie_id == bookie.bookie_id,
                                 Market.market_name == market_name,
                                 Market.choice_group == None
                             )
                        ).first()
                        
                        if not market:
                            market = Market(
                                event_id=event_id,
                                bookie_id=bookie.bookie_id,
                                market_name=market_name,
                                market_group=market_group,
                                market_period=market_period,
                                choice_group=None,
                                collected_at=get_local_now()
                            )
                            session.add(market)
                            session.flush()
                        else:
                            market.collected_at = get_local_now()
                            
                        # Upsert Choices
                        choices_map = {
                            "1": b_odds.odds_1,
                            "X": b_odds.odds_x,
                            "2": b_odds.odds_2
                        }
                        
                        for choice_name, val_str in choices_map.items():
                            if not val_str or val_str == '-':
                                continue
                                
                            try:
                                current_odds = float(val_str)
                            except ValueError:
                                continue
                                
                            choice = session.query(MarketChoice).filter(
                                and_(
                                    MarketChoice.market_id == market.market_id,
                                    MarketChoice.choice_name == choice_name
                                )
                            ).first()
                            
                            if choice:
                                if abs(float(choice.current_odds or 0) - current_odds) > 0.001:
                                    choice.change = 1 if current_odds > float(choice.current_odds or 0) else -1
                                    choice.current_odds = current_odds
                            else:
                                choice = MarketChoice(
                                    market_id=market.market_id,
                                    choice_name=choice_name,
                                    initial_odds=current_odds,
                                    current_odds=current_odds,
                                    change=0
                                )
                                session.add(choice)
                                
                        saved_count += 1
                        
                    except Exception as e:
                        logger.warning(f"Error saving bookie {b_odds.name} for event {event_id}: {e}")
                        continue
                        
                # 2. Process Betfair Exchange (if present)
                if odds_data.betfair:
                    try:
                        bookie = MarketRepository._get_or_create_bookie(session, "Betfair Exchange")
                        
                        # Get/Create Market
                        market = session.query(Market).filter(
                             and_(
                                 Market.event_id == event_id,
                                 Market.bookie_id == bookie.bookie_id,
                                 Market.market_name == "Full time",
                                 Market.choice_group == None
                             )
                        ).first()
                        
                        if not market:
                            market = Market(
                                event_id=event_id,
                                bookie_id=bookie.bookie_id,
                                market_name="Full time",
                                market_group="1X2",
                                market_period="Full-time",
                                choice_group=None,
                                collected_at=get_local_now()
                            )
                            session.add(market)
                            session.flush()
                        else:
                            market.collected_at = get_local_now()
                            
                        # We use 'Back' odds for standard comparison
                        choices_map = {
                            "1": odds_data.betfair.back_1,
                            "X": odds_data.betfair.back_x,
                            "2": odds_data.betfair.back_2
                        }
                        
                        for choice_name, val_str in choices_map.items():
                             if not val_str or val_str == '-' or not val_str.strip():
                                 continue
                             try:
                                 current_odds = float(val_str)
                             except ValueError:
                                 continue
                                 
                             choice = session.query(MarketChoice).filter(
                                and_(
                                    MarketChoice.market_id == market.market_id,
                                    MarketChoice.choice_name == choice_name
                                )
                            ).first()
                            
                             if choice:
                                if abs(float(choice.current_odds or 0) - current_odds) > 0.001:
                                    choice.change = 1 if current_odds > float(choice.current_odds or 0) else -1
                                    choice.current_odds = current_odds
                             else:
                                choice = MarketChoice(
                                    market_id=market.market_id,
                                    choice_name=choice_name,
                                    initial_odds=current_odds,
                                    current_odds=current_odds,
                                    change=0
                                )
                                session.add(choice)
                        
                        saved_count += 1
                        
                    except Exception as e:
                        logger.warning(f"Error saving Betfair Exchange for event {event_id}: {e}")

                session.commit()
                return saved_count
                
        except Exception as e:
            logger.error(f"Error saving OddsPortal markets for event {event_id}: {e}")
            return 0

    
    @staticmethod
    def delete_markets_for_event(event_id: int) -> bool:
        """
        Delete all markets and choices for an event.
        
        Args:
            event_id: Event ID
            
        Returns:
            True if successful
        """
        try:
            with db_manager.get_session() as session:
                # Choices will be deleted automatically due to CASCADE
                deleted = session.query(Market).filter(Market.event_id == event_id).delete()
                session.commit()
                logger.debug(f"Deleted {deleted} markets for event {event_id}")
                return True
        except Exception as e:
            logger.error(f"Error deleting markets for event {event_id}: {e}")
            return False
