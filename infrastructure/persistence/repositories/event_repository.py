import logging
from typing import List, Optional, Dict
from datetime import datetime, timedelta
from sqlalchemy import and_, or_, event, DDL
from sqlalchemy.orm import Session, joinedload

from models import Event, OddsSnapshot, EventOdds, Result, EventObservation, Season, Base
from database import db_manager
from timezone_utils import get_local_now
from .season_repository import SeasonRepository

logger = logging.getLogger(__name__)

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
                round_info = event_data.get('round')
                
                if 'season_id' in event_data and event_data['season_id']:
                    season_id = event_data['season_id']
                    season_name = event_data.get('season_name')
                    season_year = event_data.get('season_year')
                    sport = event_data.get('sport')
                    competition_name = event_data.get('competition') or ""
                    
                    if season_year and isinstance(season_year, str):
                        year = SeasonRepository._parse_year(season_year)
                    elif season_year:
                        year = season_year
                    else:
                        year = None
                    
                    if season_id in [season['nba_cup_season_id'] for season in NBA_SEASONS] and 'nba cup' in competition_name.lower():
                        round_info = 'knockouts/playoffs'
                    
                    if season_name and year and sport:
                        SeasonRepository.get_or_create_season(season_id, season_name, year, sport)
                    elif season_name and sport:
                        parsed_year = SeasonRepository._parse_year(season_name)
                        if parsed_year:
                            SeasonRepository.get_or_create_season(season_id, season_name, parsed_year, sport)
                
                event_obj = session.query(Event).filter(Event.id == event_data['id']).first()
                
                if event_obj:
                    event_obj.custom_id = event_data.get('customId')
                    event_obj.slug = event_data['slug']
                    event_obj.start_time_utc = datetime.fromtimestamp(event_data['startTimestamp'])
                    event_obj.sport = event_data['sport']
                    event_obj.competition = event_data['competition']
                    event_obj.country = event_data.get('country')
                    event_obj.home_team = event_data['homeTeam']
                    event_obj.away_team = event_data['awayTeam']
                    gender = event_data.get('gender') or 'unknown'
                    event_obj.gender = gender[:10] if len(gender) > 10 else gender
                    
                    if event_data.get('discovery_source') == 'dropping_odds':
                        old_source = event_obj.discovery_source
                        if old_source != 'dropping_odds':
                            event_obj.discovery_source = 'dropping_odds'
                            logger.debug(f"Overwrote discovery_source to 'dropping_odds' for event {event_data['id']} (was: {old_source})")
                    
                    if event_data.get('season_id'):
                        event_obj.season_id = event_data['season_id']
                    if round_info:
                        existing = (event_obj.round or "").lower()
                        incoming = str(round_info).lower()
                        if existing == "knockouts/playoffs" and incoming == "regular_season":
                            pass
                        else:
                            event_obj.round = round_info

                    event_obj.updated_at = get_local_now()
                    logger.info(f"Updated event {event_data['id']}")
                else:
                    gender = event_data.get('gender') or 'unknown'
                    gender = gender[:10] if len(gender) > 10 else gender
                    
                    event_obj = Event(
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
                        round=round_info
                    )
                    session.add(event_obj)
                    logger.debug(f"Created new event {event_data['id']}")
                
                return event_obj
                
        except Exception as e:
            logger.error(f"Error upserting event {event_data.get('id')}: {e}")
            return None
    
    @staticmethod
    def get_event_by_id(event_id: int) -> Optional[Event]:
        """Get event by ID with event_odds loaded"""
        try:
            with db_manager.get_session() as session:
                return session.query(Event).options(joinedload(Event.event_odds)).filter(Event.id == event_id).first()
        except Exception as e:
            logger.error(f"Error getting event {event_id}: {e}")
            return None
    
    @staticmethod
    def update_event_starting_time(event_id: int, new_start_time: datetime) -> bool:
        """Update the starting time of an event"""
        try:
            with db_manager.get_session() as session:
                event_obj = session.query(Event).filter(Event.id == event_id).first()
                if event_obj:
                    event_obj.start_time_utc = new_start_time
                    event_obj.updated_at = get_local_now()
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
                event_obj = session.query(Event).filter(Event.id == event_id).first()
                if not event_obj:
                    logger.warning(f"Event {event_id} not found for deletion")
                    return False
                
                season_id_to_check = event_obj.season_id
                session.query(OddsSnapshot).filter(OddsSnapshot.event_id == event_id).delete()
                session.query(EventOdds).filter(EventOdds.event_id == event_id).delete()
                session.query(Result).filter(Result.event_id == event_id).delete()
                session.query(EventObservation).filter(EventObservation.event_id == event_id).delete()
                session.query(Event).filter(Event.id == event_id).delete()
                
                if season_id_to_check:
                    remaining_events = session.query(Event).filter(Event.season_id == season_id_to_check).count()
                    if remaining_events == 0:
                        session.query(Season).filter(Season.id == season_id_to_check).delete()
                        logger.info(f"🧹 Cleaned up orphaned season {season_id_to_check} after event deletion")
                
                logger.info(f"✅ Deleted event {event_id} and all related data")
                return True
                
        except Exception as e:
            logger.error(f"Error deleting event {event_id}: {e}")
            return False
    
    @staticmethod
    def batch_delete_events(event_ids: List[int]) -> int:
        """Batch delete multiple events"""
        if not event_ids:
            return 0
        
        try:
            with db_manager.get_session() as session:
                session.query(OddsSnapshot).filter(OddsSnapshot.event_id.in_(event_ids)).delete(synchronize_session=False)
                session.query(EventOdds).filter(EventOdds.event_id.in_(event_ids)).delete(synchronize_session=False)
                session.query(Result).filter(Result.event_id.in_(event_ids)).delete(synchronize_session=False)
                session.query(EventObservation).filter(EventObservation.event_id.in_(event_ids)).delete(synchronize_session=False)
                deleted_count = session.query(Event).filter(Event.id.in_(event_ids)).delete(synchronize_session=False)
                
                all_seasons = session.query(Season).all()
                orphaned_season_ids = []
                for season in all_seasons:
                    event_count = session.query(Event).filter(Event.season_id == season.id).count()
                    if event_count == 0:
                        orphaned_season_ids.append(season.id)
                
                if orphaned_season_ids:
                    deleted_seasons_count = session.query(Season).filter(Season.id.in_(orphaned_season_ids)).delete(synchronize_session=False)
                    logger.info(f"🧹 Cleaned up {deleted_seasons_count} orphaned season(s) with 0 events after batch deletion")
                
                logger.info(f"✅ Batch deleted {deleted_count} events and all related data")
                return deleted_count
                
        except Exception as e:
            logger.error(f"Error batch deleting events: {e}")
            return 0

    def get_events_starting_soon(self, window_minutes: int = 30):
        """Get events starting soon"""
        try:
            with db_manager.get_session() as session:
                now = datetime.now()
                window_start = now.replace(second=0, microsecond=0) - timedelta(minutes=5)
                start_window = now + timedelta(minutes=window_minutes)
                
                return session.query(Event).options(joinedload(Event.event_odds)).filter(
                    Event.start_time_utc.between(window_start, start_window)
                ).all()
        except Exception as e:
            logger.error(f"Error getting events starting soon: {e}")
            return []

    @staticmethod
    def get_events_starting_soon_with_odds(window_minutes: int = 30, season_ids: Optional[List[int]] = None) -> List[Dict]:
        """Get events starting soon with odds data"""
        try:
            with db_manager.get_session() as session:
                now = datetime.now()
                window_start = now.replace(second=0, microsecond=0) - timedelta(minutes=5)
                window_end = now + timedelta(minutes=window_minutes)
                
                query = session.query(Event).options(joinedload(Event.event_odds)).filter(
                    and_(Event.start_time_utc >= window_start, Event.start_time_utc <= window_end)
                )
                
                if season_ids:
                    query = query.filter(Event.season_id.in_(season_ids))

                events_with_odds = query.all()
                result = []
                for event_obj in events_with_odds:
                    event_data = {
                        'id': event_obj.id,
                        'home_team': event_obj.home_team,
                        'away_team': event_obj.away_team,
                        'competition': event_obj.competition,
                        'start_time_utc': event_obj.start_time_utc,
                        'sport': event_obj.sport,
                        'country': event_obj.country,
                        'slug': event_obj.slug,
                        'season_id': event_obj.season_id,
                        'odds': None
                    }
                    if hasattr(event_obj, 'event_odds') and event_obj.event_odds:
                        odds = event_obj.event_odds
                        event_data['odds'] = {
                            'one_open': odds.one_open,
                            'x_open': odds.x_open,
                            'two_open': odds.two_open,
                            'one_final': odds.one_final,
                            'x_final': odds.x_final,
                            'two_final': odds.two_final
                        }
                    result.append(event_data)
                return result
        except Exception as e:
            logger.error(f"Error getting events starting soon with odds: {e}")
            return []
    
    @staticmethod
    def get_events_started_recently(window_minutes: int = 15, season_ids: Optional[List[int]] = None) -> List[Dict]:
        """Get events started recently"""
        try:     
            with db_manager.get_session() as session:
                now = get_local_now()
                window_start = now - timedelta(minutes=window_minutes, seconds=10)
                window_start = window_start.replace(microsecond=0)
                
                query = session.query(Event).filter(
                    and_(Event.start_time_utc >= window_start, Event.start_time_utc < now)
                )
                
                if season_ids:
                    query = query.filter(Event.season_id.in_(season_ids))

                events_started_recently = query.all()
                result = []
                for event_obj in events_started_recently:
                    result.append({
                        'id': event_obj.id,
                        'home_team': event_obj.home_team,
                        'away_team': event_obj.away_team,
                        'competition': event_obj.competition,
                        'start_time_utc': event_obj.start_time_utc,
                        'sport': event_obj.sport,
                        'country': event_obj.country,
                        'slug': event_obj.slug,
                        'season_id': event_obj.season_id
                    })
                return result
        except Exception as e:
            logger.error(f"Error getting events started recently: {e}")
            return []
    
    @staticmethod
    def get_todays_events() -> List[Event]:
        """Get all events for today"""
        try:
            with db_manager.get_session() as session:
                today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                today_end = today_start + timedelta(days=1)
                return session.query(Event).filter(
                    and_(Event.start_time_utc >= today_start, Event.start_time_utc < today_end)
                ).all()
        except Exception as e:
            logger.error(f"Error getting today's events: {e}")
            return []
    
    @staticmethod
    def get_events_by_date(target_date) -> List[Event]:
        """Get all events for a specific date"""
        try:
            with db_manager.get_session() as session:
                if hasattr(target_date, 'date'):
                    target_date = target_date.date()
                day_start = datetime.combine(target_date, datetime.min.time())
                day_end = day_start + timedelta(days=1)
                return session.query(Event).filter(
                    and_(Event.start_time_utc >= day_start, Event.start_time_utc < day_end)
                ).all()
        except Exception as e:
            logger.error(f"Error getting events for date {target_date}: {e}")
            return []
    
    @staticmethod
    def get_all_finished_events() -> List[Event]:
        """Get all events that should be finished"""
        try:
            with db_manager.get_session() as session:
                now = datetime.now()
                return session.query(Event).filter(
                    or_(
                        and_(Event.sport.in_(['Football', 'Futsal']), Event.start_time_utc < now - timedelta(hours=2.5)),
                        and_(Event.sport == 'Tennis', Event.start_time_utc < now - timedelta(hours=4)),
                        and_(Event.sport == 'Baseball', Event.start_time_utc < now - timedelta(hours=4)),
                        and_(Event.sport == 'Basketball', Event.start_time_utc < now - timedelta(hours=3)),
                        and_(~Event.sport.in_(['Football', 'Futsal', 'Tennis', 'Baseball', 'Basketball']), Event.start_time_utc < now - timedelta(hours=3))
                    )
                ).all()
        except Exception as e:
            logger.error(f"Error getting finished events: {e}")
            return []
