#!/usr/bin/env python3
"""
Prediction Logging System
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import joinedload

logger = logging.getLogger(__name__)


class PredictionLogger:
    """Handles all prediction logging operations."""

    def __init__(self):
        logger.info("Prediction Logger initialized")

    def log_prediction(self, event, prediction_data: Dict) -> bool:
        """Log a successful prediction to the prediction_logs table."""
        try:
            from infrastructure.persistence.database import db_manager
            from infrastructure.persistence.models import Event, PredictionLog

            with db_manager.get_session() as session:
                normalized_event = (
                    session.query(Event)
                    .options(
                        joinedload(Event.home_participant),
                        joinedload(Event.away_participant),
                        joinedload(Event.competition_ref),
                    )
                    .filter(Event.id == event.id)
                    .first()
                )
                if (
                    not normalized_event
                    or not normalized_event.home_participant
                    or not normalized_event.away_participant
                    or not normalized_event.competition_ref
                ):
                    logger.warning(
                        "Missing normalized participants/competition for prediction logging on event %s",
                        event.id,
                    )
                    return False

                existing_prediction = session.query(PredictionLog).filter_by(event_id=event.id).first()
                if existing_prediction:
                    logger.info(
                        "Prediction already exists for event %s (status: %s) - skipping duplicate",
                        event.id,
                        existing_prediction.status,
                    )
                    return False

                status = prediction_data.get("status", "unknown")
                if status != "success":
                    logger.debug("Event %s status is '%s', not 'success' - skipping prediction logging", event.id, status)
                    return True

                primary_prediction = prediction_data.get("primary_prediction", "")
                primary_confidence = prediction_data.get("primary_confidence", "")
                tier1_count = prediction_data.get("tier1_candidates", {}).get("count", 0)
                tier2_count = prediction_data.get("tier2_candidates", {}).get("count", 0)

                prediction_winner, prediction_point_diff = self._extract_prediction_details(primary_prediction)

                prediction_log = PredictionLog(
                    event_id=event.id,
                    prediction_type="process1",
                    confidence_level=primary_confidence.lower() if primary_confidence else "medium",
                    prediction_winner=prediction_winner,
                    prediction_point_diff=prediction_point_diff,
                    tier1_count=tier1_count,
                    tier2_count=tier2_count,
                    sport=normalized_event.sport,
                    participants=f"{normalized_event.home_participant.name} vs {normalized_event.away_participant.name}",
                    competition=normalized_event.competition_ref.display_name,
                    status="pending",
                )

                logger.info("Prediction log insert:")
                logger.info("   event_id: %s", event.id)
                logger.info("   prediction_type: 'process1'")
                logger.info(
                    "   confidence_level: '%s'",
                    primary_confidence.lower() if primary_confidence else "medium",
                )
                logger.info("   prediction_winner: '%s'", prediction_winner)
                logger.info("   prediction_point_diff: %s", prediction_point_diff)
                logger.info("   tier1_count: %s", tier1_count)
                logger.info("   tier2_count: %s", tier2_count)
                logger.info("   sport: '%s'", normalized_event.sport)
                logger.info(
                    "   participants: '%s vs %s'",
                    normalized_event.home_participant.name,
                    normalized_event.away_participant.name,
                )
                logger.info("   competition: '%s'", normalized_event.competition_ref.display_name)
                logger.info("   status: 'pending'")

                session.add(prediction_log)
                session.commit()

                logger.info("Prediction logged for event %s: %s (confidence: %s)", event.id, prediction_winner, primary_confidence)
                return True
        except Exception as e:
            logger.error("Error logging prediction for event %s: %s", event.id, e)
            return False

    def _extract_prediction_details(self, prediction_text: str) -> Tuple[Optional[str], Optional[int]]:
        """Extract winner and point difference from prediction text."""
        try:
            if not prediction_text:
                return None, None

            from modules.alerts.dual_process.run_dual_process import prediction_engine

            prediction_winner = prediction_engine.extract_winner_from_process1_prediction(prediction_text)

            prediction_point_diff = None
            if prediction_text:
                import re

                diff_match = re.search(r"(?:by point differential of:|diff:)\s*(\d+(?:\.\d+)?)", prediction_text)
                if diff_match:
                    prediction_point_diff = int(float(diff_match.group(1)))
                elif prediction_winner == "X":
                    prediction_point_diff = 0

            return prediction_winner, prediction_point_diff
        except Exception as e:
            logger.error("Error extracting prediction details from '%s': %s", prediction_text, e)
            return None, None

    def update_predictions_with_results(self) -> Dict[str, int]:
        """Update prediction logs with actual results from completed events."""
        try:
            from datetime import timedelta

            from infrastructure.persistence.database import db_manager
            from infrastructure.persistence.models import Event, PredictionLog, Result

            with db_manager.get_session() as session:
                yesterday = datetime.now() - timedelta(days=1)
                yesterday_date = yesterday.date()

                logger.info("Updating predictions for events from %s", yesterday_date)

                pending_predictions = session.query(PredictionLog).join(Event, PredictionLog.event_id == Event.id).filter(
                    PredictionLog.status == "pending",
                    Event.start_time_utc >= yesterday_date,
                    Event.start_time_utc < yesterday_date + timedelta(days=1),
                ).all()

                if not pending_predictions:
                    logger.info("No pending predictions found for events from %s", yesterday_date)
                    return {"updated": 0, "cancelled": 0, "total": 0}

                logger.info("Found %s pending predictions to update for events from %s", len(pending_predictions), yesterday_date)

                updated_count = 0
                cancelled_count = 0

                for prediction in pending_predictions:
                    try:
                        result = session.query(Result).filter_by(event_id=prediction.event_id).first()

                        if result and result.home_score is not None and result.away_score is not None:
                            prediction.actual_result = f"{result.home_score}-{result.away_score}"
                            prediction.actual_winner = result.winner
                            prediction.actual_point_diff = abs(result.home_score - result.away_score)
                            prediction.status = "completed"
                            updated_count += 1
                            logger.info(
                                "Updated prediction for event %s: %s (winner: %s)",
                                prediction.event_id,
                                prediction.actual_result,
                                prediction.actual_winner,
                            )
                        else:
                            prediction.status = "cancelled"
                            cancelled_count += 1
                            logger.info("Marked prediction as cancelled for event %s (no result found)", prediction.event_id)
                    except Exception as e:
                        logger.error("Error updating prediction for event %s: %s", prediction.event_id, e)
                        prediction.status = "cancelled"
                        cancelled_count += 1
                        continue

                session.commit()

                stats = {
                    "updated": updated_count,
                    "cancelled": cancelled_count,
                    "total": len(pending_predictions),
                }

                logger.info("Prediction logs updated: %s completed, %s cancelled", updated_count, cancelled_count)
                return stats
        except Exception as e:
            logger.error("Error updating prediction logs with results: %s", e)
            return {"updated": 0, "cancelled": 0, "total": 0, "error": str(e)}


prediction_logger = PredictionLogger()
