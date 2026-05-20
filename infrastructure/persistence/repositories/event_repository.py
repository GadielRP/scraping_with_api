import logging
from typing import List, Optional, Dict
from datetime import datetime, timedelta
from sqlalchemy import and_, or_, event, DDL
from sqlalchemy.orm import Session, joinedload

from infrastructure.persistence.models import Competition, Event, Result, EventObservation, Season, Base
from infrastructure.persistence.database import db_manager
from shared.timezone_utils import get_local_now
from .season_repository import SeasonRepository
from .participant_repository import ParticipantRepository
from .competition_repository import CompetitionRepository

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
    def _display_home_team(event_obj: Event) -> str:
        if not event_obj.home_participant:
            raise ValueError(f"Missing normalized home participant for event_id={event_obj.id}")
        return event_obj.home_participant.name

    @staticmethod
    def _display_away_team(event_obj: Event) -> str:
        if not event_obj.away_participant:
            raise ValueError(f"Missing normalized away participant for event_id={event_obj.id}")
        return event_obj.away_participant.name

    @staticmethod
    def _display_competition(event_obj: Event) -> str:
        if not event_obj.competition_ref:
            raise ValueError(f"Missing normalized competition for event_id={event_obj.id}")
        return event_obj.competition_ref.display_name

    @staticmethod
    def _build_normalized_event_data(event_obj: Event) -> Dict:
        home_participant = event_obj.home_participant
        away_participant = event_obj.away_participant
        competition_ref = event_obj.competition_ref

        if not home_participant or not away_participant or not competition_ref:
            raise ValueError(f"Missing normalized participants/competition for event_id={event_obj.id}")

        return {
            "id": event_obj.id,
            "home_team": EventRepository._display_home_team(event_obj),
            "away_team": EventRepository._display_away_team(event_obj),
            "competition": EventRepository._display_competition(event_obj),
            "start_time_utc": event_obj.start_time_utc,
            "sport": event_obj.sport,
            "country": event_obj.country,
            "slug": event_obj.slug,
            "custom_id": event_obj.custom_id,
            "season_id": event_obj.season_id,
            "home_participant_id": event_obj.home_participant_id,
            "away_participant_id": event_obj.away_participant_id,
            "competition_id": event_obj.competition_id,
            "home_source_participant_id": home_participant.source_participant_id,
            "away_source_participant_id": away_participant.source_participant_id,
            "competition_source_tournament_id": competition_ref.source_tournament_id,
            "competition_source_unique_tournament_id": competition_ref.source_unique_tournament_id,
        }

    @staticmethod
    def _build_event_data_with_legacy_fallback(event_obj: Event) -> Dict:
        """Build event payloads for runtime use, falling back to legacy display fields when needed.

        This is a temporary compatibility bridge while historical rows are backfilled.
        Legacy-derived payloads are marked explicitly so they can be removed later.
        """
        home_participant = event_obj.__dict__.get("home_participant")
        away_participant = event_obj.__dict__.get("away_participant")
        competition_ref = event_obj.__dict__.get("competition_ref")

        home_team = home_participant.name if home_participant else (event_obj.home_team or None)
        away_team = away_participant.name if away_participant else (event_obj.away_team or None)
        competition_name = (
            competition_ref.display_name if competition_ref else (event_obj.competition or None)
        )

        if not home_team or not away_team or not competition_name:
            raise ValueError(f"Missing normalized participants/competition for event_id={event_obj.id}")

        legacy_compat_used = not (home_participant and away_participant and competition_ref)

        return {
            "id": event_obj.id,
            "home_team": home_team,
            "away_team": away_team,
            "competition": competition_name,
            "start_time_utc": event_obj.start_time_utc,
            "sport": event_obj.sport,
            "country": event_obj.country,
            "slug": event_obj.slug,
            "custom_id": event_obj.custom_id,
            "season_id": event_obj.season_id,
            "home_participant_id": event_obj.home_participant_id,
            "away_participant_id": event_obj.away_participant_id,
            "competition_id": event_obj.competition_id,
            "home_source_participant_id": (
                home_participant.source_participant_id if home_participant else None
            ),
            "away_source_participant_id": (
                away_participant.source_participant_id if away_participant else None
            ),
            "competition_source_tournament_id": (
                competition_ref.source_tournament_id if competition_ref else None
            ),
            "competition_source_unique_tournament_id": (
                competition_ref.source_unique_tournament_id if competition_ref else None
            ),
            "context_status": "legacy_compat" if legacy_compat_used else "normalized",
            "legacy_compat_used": legacy_compat_used,
        }
    
    @staticmethod
    def upsert_event(event_data: Dict) -> Optional[Event]:
        """Insert or update an event"""
        try:
            event_payload = event_data.get('event', event_data) if event_data else {}
            home_participant_data = event_data.get('home_participant') if event_data and 'event' in event_data else None
            away_participant_data = event_data.get('away_participant') if event_data and 'event' in event_data else None
            competition_data = event_data.get('competition_ref') if event_data and 'event' in event_data else None

            event_id = event_payload.get('id')
            if not event_id:
                logger.warning("Skipping event upsert because event id is missing")
                return None

            if event_payload.get('startTimestamp') is None:
                logger.warning("Skipping event %s upsert because startTimestamp is missing", event_id)
                return None

            with db_manager.get_session() as session:
                round_info = event_payload.get('round')

                home_participant = None
                away_participant = None
                competition = None
                
                if 'season_id' in event_payload and event_payload['season_id']:
                    season_id = event_payload['season_id']
                    season_name = event_payload.get('season_name')
                    season_year = event_payload.get('season_year')
                    sport = event_payload.get('sport')
                    competition_name = event_payload.get('competition') or ""
                    
                    if season_year and isinstance(season_year, str):
                        year = SeasonRepository._parse_year(season_year)
                    elif season_year:
                        year = season_year
                    else:
                        year = None
                    
                    if season_id in [season['nba_cup_season_id'] for season in NBA_SEASONS] and 'nba cup' in competition_name.lower():
                        round_info = 'knockouts/playoffs'
                    
                    if season_name and year and sport:
                        SeasonRepository.get_or_create_season_in_session(session, season_id, season_name, year, sport)
                    elif season_name and sport:
                        parsed_year = SeasonRepository._parse_year(season_name)
                        if parsed_year:
                            SeasonRepository.get_or_create_season_in_session(session, season_id, season_name, parsed_year, sport)

                if home_participant_data and home_participant_data.get('source_participant_id') is not None:
                    home_participant = ParticipantRepository.upsert_participant(session, home_participant_data)
                elif home_participant_data:
                    logger.warning(
                        "Event %s has no home participant id; LEGACY_DB_SHIM_REMOVE_AFTER_SCHEMA_MIGRATION is still required for home_team persistence",
                        event_id,
                    )

                if away_participant_data and away_participant_data.get('source_participant_id') is not None:
                    away_participant = ParticipantRepository.upsert_participant(session, away_participant_data)
                elif away_participant_data:
                    logger.warning(
                        "Event %s has no away participant id; LEGACY_DB_SHIM_REMOVE_AFTER_SCHEMA_MIGRATION is still required for away_team persistence",
                        event_id,
                    )

                if competition_data and competition_data.get('source_tournament_id') is not None:
                    competition = CompetitionRepository.upsert_competition(session, competition_data)
                elif competition_data:
                    logger.warning(
                        "Event %s has no tournament id; LEGACY_DB_SHIM_REMOVE_AFTER_SCHEMA_MIGRATION is still required for competition persistence",
                        event_id,
                    )
                
                event_obj = session.query(Event).filter(Event.id == event_id).first()
                
                if event_obj:
                    event_obj.custom_id = event_payload.get('customId')
                    event_obj.slug = event_payload.get('slug') or event_obj.slug
                    event_obj.start_time_utc = datetime.fromtimestamp(event_payload['startTimestamp'])
                    event_obj.sport = event_payload.get('sport') or event_obj.sport
                    event_obj.country = event_payload.get('country')
                    # LEGACY_DB_SHIM_REMOVE_AFTER_SCHEMA_MIGRATION: keep legacy column writes until the DB schema no longer requires them.
                    event_obj.competition = event_payload.get('competition') or event_obj.competition
                    # LEGACY_DB_SHIM_REMOVE_AFTER_SCHEMA_MIGRATION: keep legacy column writes until the DB schema no longer requires them.
                    event_obj.home_team = event_payload.get('homeTeam') or event_obj.home_team
                    # LEGACY_DB_SHIM_REMOVE_AFTER_SCHEMA_MIGRATION: keep legacy column writes until the DB schema no longer requires them.
                    event_obj.away_team = event_payload.get('awayTeam') or event_obj.away_team
                    gender = event_payload.get('gender') or 'unknown'
                    event_obj.gender = gender[:10] if len(gender) > 10 else gender
                    if home_participant:
                        event_obj.home_participant_id = home_participant.participant_id
                    if away_participant:
                        event_obj.away_participant_id = away_participant.participant_id
                    if competition:
                        event_obj.competition_id = competition.competition_id
                    
                    if event_payload.get('discovery_source') == 'dropping_odds':
                        old_source = event_obj.discovery_source
                        if old_source != 'dropping_odds':
                            event_obj.discovery_source = 'dropping_odds'
                            logger.debug(f"Overwrote discovery_source to 'dropping_odds' for event {event_id} (was: {old_source})")
                    
                    if event_payload.get('season_id'):
                        event_obj.season_id = event_payload['season_id']
                    if round_info:
                        existing = (event_obj.round or "").lower()
                        incoming = str(round_info).lower()
                        if existing == "knockouts/playoffs" and incoming == "regular_season":
                            pass
                        else:
                            event_obj.round = round_info

                    event_obj.updated_at = get_local_now()
                    logger.info(f"Updated event {event_id}")
                else:
                    gender = event_payload.get('gender') or 'unknown'
                    gender = gender[:10] if len(gender) > 10 else gender
                    
                    event_obj = Event(
                        id=event_id,
                        custom_id=event_payload.get('customId'),
                        slug=event_payload.get('slug') or str(event_id),
                        start_time_utc=datetime.fromtimestamp(event_payload['startTimestamp']),
                        sport=event_payload.get('sport') or 'Unknown',
                        # LEGACY_DB_SHIM_REMOVE_AFTER_SCHEMA_MIGRATION: keep legacy column writes until the DB schema no longer requires them.
                        competition=event_payload.get('competition') or 'Unknown',
                        country=event_payload.get('country'),
                        # LEGACY_DB_SHIM_REMOVE_AFTER_SCHEMA_MIGRATION: keep legacy column writes until the DB schema no longer requires them.
                        home_team=event_payload.get('homeTeam') or 'Unknown',
                        # LEGACY_DB_SHIM_REMOVE_AFTER_SCHEMA_MIGRATION: keep legacy column writes until the DB schema no longer requires them.
                        away_team=event_payload.get('awayTeam') or 'Unknown',
                        gender=gender,
                        discovery_source=event_payload.get('discovery_source', 'dropping_odds'),
                        season_id=event_payload.get('season_id'),
                        round=round_info,
                        home_participant_id=home_participant.participant_id if home_participant else None,
                        away_participant_id=away_participant.participant_id if away_participant else None,
                        competition_id=competition.competition_id if competition else None
                    )
                    session.add(event_obj)
                    logger.debug(f"Created new event {event_id}")
                
                return event_obj
                
        except Exception as e:
            event_payload = event_data.get('event', event_data) if event_data else {}
            logger.error(f"Error upserting event {event_payload.get('id')}: {e}")
            return None
    
    @staticmethod
    def get_event_by_id(event_id: int) -> Optional[Event]:
        """Get event by ID with display relationships loaded."""
        try:
            with db_manager.get_session() as session:
                return (
                    session.query(Event)
                    .options(
                        joinedload(Event.home_participant),
                        joinedload(Event.away_participant),
                        joinedload(Event.competition_ref),
                        joinedload(Event.season),
                    )
                    .filter(Event.id == event_id)
                    .first()
                )
        except Exception as e:
            logger.error(f"Error getting event {event_id}: {e}")
            return None
            
    @staticmethod
    def get_events_started_between_minutes_ago(
        sport: str,
        competition: Optional[str] = None,
        min_minutes_ago: int = 105,
        max_minutes_ago: int = 140,
        alert_sent: Optional[bool] = None
    ) -> List[Dict]:
        """
        Get events that started within a specific minute range for a given sport/competition.
        
        This is a modular function that can be used for any sport and competition.
        Similar to get_events_started_recently but with sport/competition filtering.
        
        Args:
            sport: Sport name (e.g., 'Basketball', 'Hockey', 'Football')
            competition: Optional competition filter (e.g., 'NBA', 'NHL'). If None, returns all events for sport.
            min_minutes_ago: Minimum minutes since event started (e.g., 80)
            max_minutes_ago: Maximum minutes since event started (e.g., 100)
            alert_sent: Optional filter for alert_sent flag. If True, only returns events with alert_sent=True.
                       If False, only returns events with alert_sent=False. If None, returns all events.
            
        Returns:
            List of event dictionaries matching the criteria
            
        Example:
            # Get NBA games that started 105-140 minutes ago and haven't sent alert yet
            events = get_events_started_between_minutes_ago('Basketball', 'NBA', 105, 140, alert_sent=False)
        """
        try:
            with db_manager.get_session() as session:
                now = get_local_now()
                
                # Calculate time window
                window_start = now - timedelta(minutes=max_minutes_ago)
                window_end = now - timedelta(minutes=min_minutes_ago)
                
                logger.info(f"Searching for {sport} events (competition: {competition or 'all'}) "
                           f"that started between {max_minutes_ago} and {min_minutes_ago} minutes ago")
                logger.debug(f"Time window: {window_start} to {window_end}")
                
                # Build query with sport filter
                filters = [
                    Event.sport == sport,
                    Event.start_time_utc >= window_start,
                    Event.start_time_utc <= window_end
                ]
                
                # Add alert_sent filter if specified
                if alert_sent is not None:
                    filters.append(Event.alert_sent == alert_sent)
                
                query = (
                    session.query(Event)
                    .options(
                        joinedload(Event.home_participant),
                        joinedload(Event.away_participant),
                        joinedload(Event.competition_ref),
                    )
                    .filter(and_(*filters))
                )
                
                # Add competition filter if specified
                if competition:
                    query = query.join(Event.competition_ref).filter(
                        or_(
                            Competition.display_name.ilike(f"%{competition}%"),
                            Competition.canonical_name.ilike(f"%{competition}%"),
                            Competition.slug.ilike(f"%{competition}%"),
                            Competition.unique_slug.ilike(f"%{competition}%"),
                        )
                    )
                
                events = query.all()
                
                # Convert to list of dictionaries
                result = []
                for event in events:
                    try:
                        result.append(EventRepository._build_event_data_with_legacy_fallback(event))
                    except ValueError as exc:
                        logger.warning("Skipping event %s in minutes-range query: %s", event.id, exc)
                
                if result:
                    logger.info(f"Found {len(result)} {sport} events (competition: {competition or 'all'}) "
                              f"in {max_minutes_ago}-{min_minutes_ago} minute window")
                else:
                    logger.debug(f"No {sport} events (competition: {competition or 'all'}) found in time window")
                
                return result
                
        except Exception as e:
            logger.error(f"Error getting events by sport and minutes range: {e}")
            return []

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
                
                return session.query(Event).options(
                    joinedload(Event.home_participant),
                    joinedload(Event.away_participant),
                    joinedload(Event.competition_ref),
                ).filter(
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
                
                query = session.query(Event).options(
                    joinedload(Event.home_participant),
                    joinedload(Event.away_participant),
                    joinedload(Event.competition_ref),
                ).filter(
                    and_(Event.start_time_utc >= window_start, Event.start_time_utc <= window_end)
                )
                
                if season_ids:
                    query = query.filter(Event.season_id.in_(season_ids))

                events_with_odds = query.all()
                from .dual_process_odds_repository import DualProcessOddsRepository

                odds_by_event_id = DualProcessOddsRepository.get_event_odds_map(
                    [event_obj.id for event_obj in events_with_odds]
                )
                result = []
                for event_obj in events_with_odds:
                    try:
                        event_data = EventRepository._build_event_data_with_legacy_fallback(event_obj)
                    except ValueError as exc:
                        logger.warning("Skipping event %s in starting-soon query: %s", event_obj.id, exc)
                        continue
                    event_data['odds'] = None
                    odds = odds_by_event_id.get(event_obj.id)
                    if odds:
                        event_data['odds'] = {
                            'one_open': odds.one_open,
                            'x_open': odds.x_open,
                            'two_open': odds.two_open,
                            'one_final': odds.one_final,
                            'x_final': odds.x_final,
                            'two_final': odds.two_final,
                            'market_id': odds.market_id,
                            'market_name': odds.market_name,
                            'market_group': odds.market_group,
                            'market_period': odds.market_period,
                        }
                    result.append(event_data)
                return result
        except Exception as e:
            logger.error(f"Error getting events starting soon with odds: {e}")
            return []
    
    @staticmethod
    def get_events_started_recently(window_minutes: int = 15, season_ids: Optional[List[int]] = None) -> List[Dict]:
        """Get recently started events without associated results."""
        try:     
            with db_manager.get_session() as session:
                now = get_local_now()
                window_start = now - timedelta(minutes=window_minutes, seconds=10)
                window_start = window_start.replace(microsecond=0)
                
                query = (
                    session.query(Event)
                    .outerjoin(Result, Result.event_id == Event.id)
                    .options(
                        joinedload(Event.home_participant),
                        joinedload(Event.away_participant),
                        joinedload(Event.competition_ref),
                    )
                    .filter(
                        and_(
                            Event.start_time_utc >= window_start,
                            Event.start_time_utc < now,
                            Result.event_id.is_(None),
                        )
                    )
                )
                
                if season_ids:
                    query = query.filter(Event.season_id.in_(season_ids))

                events_started_recently = query.all()
                result = []
                for event_obj in events_started_recently:
                    try:
                        result.append(EventRepository._build_event_data_with_legacy_fallback(event_obj))
                    except ValueError as exc:
                        logger.warning("Skipping event %s in recently-started query: %s", event_obj.id, exc)
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
                return session.query(Event).options(
                    joinedload(Event.home_participant),
                    joinedload(Event.away_participant),
                    joinedload(Event.competition_ref),
                ).filter(
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
                return session.query(Event).options(
                    joinedload(Event.home_participant),
                    joinedload(Event.away_participant),
                    joinedload(Event.competition_ref),
                ).filter(
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
                return session.query(Event).options(
                    joinedload(Event.home_participant),
                    joinedload(Event.away_participant),
                    joinedload(Event.competition_ref),
                ).filter(
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

    @staticmethod
    def mark_event_as_alerted(event_id: int) -> bool:
        """
        Mark event as alert_sent=True in database.
        
        Args:
            event_id: Event ID to mark
            
        Returns:
            True if successfully marked, False otherwise
        """
        try:
            with db_manager.get_session() as session:
                event_obj = session.query(Event).filter(Event.id == event_id).first()
                if event_obj:
                    event_obj.alert_sent = True
                    session.commit()
                    logger.info(f"✅ Marked event {event_id} as alert_sent=True")
                    return True
                else:
                    logger.warning(f"Event {event_id} not found when marking as alerted")
                    return False
        except Exception as e:
            logger.error(f"Error marking event {event_id} as alerted: {e}")
            return False
