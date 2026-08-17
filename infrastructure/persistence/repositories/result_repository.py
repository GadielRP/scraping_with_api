import logging
from typing import Optional, Dict, List, Tuple

from infrastructure.persistence.models import Result
from infrastructure.persistence.database import db_manager

logger = logging.getLogger(__name__)


class ResultRepository:
    """Repository for result-related database operations"""

    @staticmethod
    def batch_upsert_results(results_data: List[Tuple[int, Dict]]) -> int:
        """Batch insert or update results in a single transaction/session."""
        if not results_data:
            return 0
        try:
            event_ids = [event_id for event_id, _ in results_data]
            with db_manager.get_session() as session:
                existing_results = {
                    r.event_id: r
                    for r in session.query(Result).filter(Result.event_id.in_(event_ids)).all()
                }
                upserted_count = 0
                for event_id, r_data in results_data:
                    if event_id in existing_results:
                        res = existing_results[event_id]
                        res.home_score = r_data.get('home_score')
                        res.away_score = r_data.get('away_score')
                        res.winner = r_data.get('winner')
                        res.home_sets = r_data.get('home_sets')
                        res.away_sets = r_data.get('away_sets')
                    else:
                        res = Result(
                            event_id=event_id,
                            home_score=r_data.get('home_score'),
                            away_score=r_data.get('away_score'),
                            winner=r_data.get('winner'),
                            home_sets=r_data.get('home_sets'),
                            away_sets=r_data.get('away_sets'),
                        )
                        session.add(res)
                    upserted_count += 1
                session.commit()
                return upserted_count
        except Exception as e:
            logger.error(f"Error in batch_upsert_results: {e}")
            return 0

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
