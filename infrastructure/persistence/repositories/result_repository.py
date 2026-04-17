import logging
from typing import Optional, Dict

from infrastructure.persistence.models import Result
from database import db_manager

logger = logging.getLogger(__name__)


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
