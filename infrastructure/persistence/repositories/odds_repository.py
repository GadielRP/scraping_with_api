import logging
from typing import List, Optional, Dict
from datetime import datetime, timedelta

from infrastructure.persistence.models import OddsSnapshot, EventOdds
from database import db_manager
from timezone_utils import get_local_now
from odds_utils import validate_odds_data

logger = logging.getLogger(__name__)


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
