#!/usr/bin/env python3
"""
Prediction Logging System

PREDICTION LOGGING BOUNDARIES:
=============================
START: This file contains all prediction logging functionality
END: Prediction logging system ends at the end of this file

FUNCTIONALITY:
- Logs successful predictions from Process 1 alerts
- Updates predictions with actual results during midnight sync
- Handles all edge cases (cancellations, missing results, duplicates)
- Maintains data integrity with proper validations

INTEGRATION:
- Used by scheduler.py for prediction logging and result updates
- Integrates with prediction_engine.py for winner extraction
- Follows @rules.mdc Code Structure & Modularity guidelines
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class PredictionLogger:
    """Handles all prediction logging operations"""
    
    def __init__(self):
        logger.info("📊 Prediction Logger initialized")
    
    def log_prediction(self, event, prediction_data: Dict) -> bool:
        """
        Log a successful prediction to the prediction_logs table.
        
        Args:
            event: Event object with event details
            prediction_data: Dictionary containing prediction information from alert report
            
        Returns:
            True if prediction was logged successfully, False otherwise
        """
        try:
            from database import db_manager
            from models import PredictionLog
            
            # Check if prediction already exists for this event
            with db_manager.get_session() as session:
                existing_prediction = session.query(PredictionLog).filter_by(event_id=event.id).first()
                if existing_prediction:
                    logger.info(f"⚠️ Prediction already exists for event {event.id} (status: {existing_prediction.status}) - skipping duplicate")
                    return False
                
                # Extract prediction data from the report
                status = prediction_data.get('status', 'unknown')
                if status != 'success':
                    logger.debug(f"Event {event.id} status is '{status}', not 'success' - skipping prediction logging")
                    return True
                
                # Extract prediction details
                primary_prediction = prediction_data.get('primary_prediction', '')
                primary_confidence = prediction_data.get('primary_confidence', '')
                tier1_count = prediction_data.get('tier1_candidates', {}).get('count', 0)
                tier2_count = prediction_data.get('tier2_candidates', {}).get('count', 0)
                selected_tier = prediction_data.get('selected_tier', '')
                
                # Extract winner and point difference using prediction_engine logic
                prediction_winner, prediction_point_diff = self._extract_prediction_details(primary_prediction)
                
                # Create prediction log entry
                prediction_log = PredictionLog(
                    event_id=event.id,
                    prediction_type='process1',
                    confidence_level=primary_confidence.lower() if primary_confidence else 'medium',
                    prediction_winner=prediction_winner,
                    prediction_point_diff=prediction_point_diff,
                    tier1_count=tier1_count,
                    tier2_count=tier2_count,
                    
                    sport=event.sport,
                    participants=f"{event.home_team} vs {event.away_team}",
                    competition=event.competition,
                    status='pending'
                )
                
                # Log detailed information about what's being inserted
                logger.info(f"📊 DETAILED PREDICTION LOG INSERT:")
                logger.info(f"   event_id: {event.id}")
                logger.info(f"   prediction_type: 'process1'")
                logger.info(f"   confidence_level: '{primary_confidence.lower() if primary_confidence else 'medium'}'")
                logger.info(f"   prediction_winner: '{prediction_winner}'")
                logger.info(f"   prediction_point_diff: {prediction_point_diff}")
                logger.info(f"   tier1_count: {tier1_count}")
                logger.info(f"   tier2_count: {tier2_count}")
                
                logger.info(f"   sport: '{event.sport}'")
                logger.info(f"   participants: '{event.home_team} vs {event.away_team}'")
                logger.info(f"   competition: '{event.competition}'")
                logger.info(f"   status: 'pending'")
                
                session.add(prediction_log)
                session.commit()
                
                logger.info(f"✅ Prediction logged for event {event.id}: {prediction_winner} (confidence: {primary_confidence})")
                return True
                
        except Exception as e:
            logger.error(f"Error logging prediction for event {event.id}: {e}")
            return False
    
    def _extract_prediction_details(self, prediction_text: str) -> Tuple[Optional[str], Optional[int]]:
        """
        Extract winner and point difference from prediction text using prediction_engine logic.
        
        Args:
            prediction_text: Prediction text from Process 1
            
        Returns:
            Tuple of (winner_side, point_diff) or (None, None)
        """
        try:
            if not prediction_text:
                return None, None
            
            # Import the proven extraction logic from prediction_engine
            from prediction_engine import prediction_engine
            
            # Extract winner using the proven method
            prediction_winner = prediction_engine._extract_winner_from_process1_prediction(prediction_text)
            
            # Extract point difference from prediction text
            prediction_point_diff = None
            if prediction_text:
                import re
                # Look for patterns like "by point differential of: 2.00" or "diff: 2"
                diff_match = re.search(r'(?:by point differential of:|diff:)\s*(\d+(?:\.\d+)?)', prediction_text)
                if diff_match:
                    prediction_point_diff = int(float(diff_match.group(1)))
                elif prediction_winner == 'X':  # Draw
                    prediction_point_diff = 0
            
            return prediction_winner, prediction_point_diff
            
        except Exception as e:
            logger.error(f"Error extracting prediction details from '{prediction_text}': {e}")
            return None, None
    
    def update_predictions_with_results(self) -> Dict[str, int]:
        """
        Update prediction logs with actual results from completed events.
        
        Returns:
            Dictionary with update statistics
        """
        try:
            from database import db_manager
            from models import PredictionLog, Result
            
            with db_manager.get_session() as session:
                # Get all pending predictions
                pending_predictions = session.query(PredictionLog).filter_by(status='pending').all()
                
                if not pending_predictions:
                    logger.info("No pending predictions found to update")
                    return {'updated': 0, 'cancelled': 0, 'total': 0}
                
                logger.info(f"Found {len(pending_predictions)} pending predictions to update")
                
                updated_count = 0
                cancelled_count = 0
                
                for prediction in pending_predictions:
                    try:
                        # Get the result for this event
                        result = session.query(Result).filter_by(event_id=prediction.event_id).first()
                        
                        if result and result.home_score is not None and result.away_score is not None:
                            # Event has a result - update prediction log
                            prediction.actual_result = f"{result.home_score}-{result.away_score}"
                            prediction.actual_winner = result.winner
                            prediction.actual_point_diff = abs(result.home_score - result.away_score) if result.home_score is not None and result.away_score is not None else None
                            
                            prediction.status = 'completed'
                            
                            updated_count += 1
                            logger.info(f"✅ Updated prediction for event {prediction.event_id}: {prediction.actual_result} (winner: {prediction.actual_winner})")
                            
                        else:
                            # No result found - mark as cancelled
                            prediction.status = 'cancelled'
                            cancelled_count += 1
                            logger.info(f"❌ Marked prediction as cancelled for event {prediction.event_id} (no result found)")
                            
                    except Exception as e:
                        logger.error(f"Error updating prediction for event {prediction.event_id}: {e}")
                        # Mark as cancelled if there's an error
                        prediction.status = 'cancelled'
                        cancelled_count += 1
                        continue
                
                # Commit all changes
                session.commit()
                
                stats = {
                    'updated': updated_count,
                    'cancelled': cancelled_count,
                    'total': len(pending_predictions)
                }
                
                logger.info(f"📊 Prediction logs updated: {updated_count} completed, {cancelled_count} cancelled")
                return stats
                
        except Exception as e:
            logger.error(f"Error updating prediction logs with results: {e}")
            return {'updated': 0, 'cancelled': 0, 'total': 0, 'error': str(e)}

# Global prediction logger instance
prediction_logger = PredictionLogger()
