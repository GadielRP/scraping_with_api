import logging
from typing import List, Optional, Dict
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from decimal import Decimal

from models import Event, OddsSnapshot, EventOdds, Result, EventObservation
from database import db_manager
from odds_utils import validate_odds_data

logger = logging.getLogger(__name__)

class EventRepository:
    """Repository for event-related database operations"""
    
    @staticmethod
    def upsert_event(event_data: Dict) -> Optional[Event]:
        """Insert or update an event"""
        try:
            with db_manager.get_session() as session:
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
                    event.updated_at = datetime.utcnow()
                    logger.debug(f"Updated event {event_data['id']}")
                else:
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
                        away_team=event_data['awayTeam']
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
                    event.updated_at = datetime.utcnow()
                    session.commit()
                    logger.info(f"Updated starting time for event {event_id} to {new_start_time}")
                    return True
                else:
                    logger.warning(f"Event {event_id} not found for starting time update")
                    return False
        except Exception as e:
            logger.error(f"Error updating starting time for event {event_id}: {e}")
            return False
    

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
                
                logger.info(f"Found {len(result)} events starting within {window_minutes} minutes")
                return result
                
        except Exception as e:
            logger.error(f"Error getting events starting soon with odds: {e}")
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
                if 'one_open' in odds_data and 'one_cur' in odds_data:
                    # Complete odds snapshot (from discovery) - has both opening and current odds
                    snapshot = OddsSnapshot(
                        event_id=event_id,
                        collected_at=datetime.utcnow(),
                        market='1X2',
                        one_open=odds_data.get('one_open'),
                        x_open=odds_data.get('x_open'),
                        two_open=odds_data.get('two_open'),
                        one_cur=odds_data.get('one_cur'),
                        x_cur=odds_data.get('x_cur'),
                        two_cur=odds_data.get('two_cur')
                    )
                else:
                    # Final odds snapshot (from pre-start or discovery final odds) - only current/final odds
                    # Map one_final to one_cur, etc. for consistency
                    one_cur = odds_data.get('one_final') or odds_data.get('one_cur')
                    x_cur = odds_data.get('x_final') or odds_data.get('x_cur')
                    two_cur = odds_data.get('two_final') or odds_data.get('two_cur')
                    
                    snapshot = OddsSnapshot(
                        event_id=event_id,
                        collected_at=datetime.utcnow(),
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
                    # Update existing record
                    if odds_data.get('one_open') is not None:
                        event_odds.one_open = odds_data['one_open']
                    if odds_data.get('x_open') is not None:
                        event_odds.x_open = odds_data['x_open']
                    if odds_data.get('two_open') is not None:
                        event_odds.two_open = odds_data['two_open']
                    
                    # Always update final odds (handle both naming conventions)
                    event_odds.one_final = odds_data.get('one_final') or odds_data.get('one_cur')
                    event_odds.x_final = odds_data.get('x_final') or odds_data.get('x_cur')
                    event_odds.two_final = odds_data.get('two_final') or odds_data.get('two_cur')
                    event_odds.last_sync_at = datetime.utcnow()
                    
                    logger.debug(f"Updated event odds for event {event_id}")
                else:
                    # Create new record
                    event_odds = EventOdds(
                        event_id=event_id,
                        market='1X2',
                        one_open=odds_data.get('one_open'),
                        x_open=odds_data.get('x_open'),
                        two_open=odds_data.get('two_open'),
                        one_final=odds_data.get('one_final') or odds_data.get('one_cur'),
                        x_final=odds_data.get('x_final') or odds_data.get('x_cur'),
                        two_final=odds_data.get('two_final') or odds_data.get('two_cur'),
                        last_sync_at=datetime.utcnow()
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
                    result.ended_at = result_data.get('ended_at')
                    result.updated_at = datetime.utcnow()
                else:
                    # Create new result
                    result = Result(
                        event_id=event_id,
                        home_score=result_data.get('home_score'),
                        away_score=result_data.get('away_score'),
                        winner=result_data.get('winner'),
                        ended_at=result_data.get('ended_at')
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
                return session.query(Result).filter(Result.event_id == event_id).first()
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
                    observation.updated_at = datetime.utcnow()
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
