#!/usr/bin/env python3
"""
Prediction Engine - Dual Process Orchestrator

PREDICTION ENGINE BOUNDARIES:
=============================
START: This file contains the dual process orchestration logic
END: Prediction engine ends at the end of this file

DUAL PROCESS INTEGRATION:
The prediction engine orchestrates both Process 1 and Process 2:
- Process 1: Historical pattern matching using database queries
- Process 2: Sport-specific formula evaluation using in-memory calculations

COMPARISON LOGIC:
- AGREE: Both processes predict same winner_side
- DISAGREE: Processes predict different winner_side
- PARTIAL: Only one process generated a valid prediction

ENHANCED REPORTING:
- Separate reports for each process
- Final verdict with comparison results
- Detailed logging for debugging
"""

import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class ComparisonVerdict(Enum):
    """Comparison verdict between processes"""
    AGREE = "AGREE"
    DISAGREE = "DISAGREE" 
    PARTIAL = "PARTIAL"
    ERROR = "ERROR"

@dataclass
class DualProcessReport:
    """Combined report from both processes"""
    event_id: int
    participants: str
    sport: str
    
    # Process 1 results
    process1_report: Optional[Dict]
    process1_prediction: Optional[Tuple[str, int]]  # (winner_side, point_diff)
    process1_status: str
    
    # Process 2 results  
    process2_report: Optional[Dict]
    process2_prediction: Optional[Tuple[str, int]]  # (winner_side, point_diff)
    process2_status: str
    
    # Final comparison
    verdict: ComparisonVerdict
    final_prediction: Optional[Tuple[str, int]]  # (winner_side, point_diff)
    agreement_details: str
    
    # Metadata
    minutes_until_start: Optional[int]
    timestamp: str
    court_type: Optional[str] = None  # Court type for Tennis/Tennis Doubles events

class PredictionEngine:
    """Orchestrator for dual process prediction system"""
    
    def __init__(self):
        logger.info("🚀 Prediction Engine initialized - Dual Process ready")
    
    def evaluate_dual_process(self, event, minutes_until_start: int = None) -> DualProcessReport:
        """
        Execute both Process 1 and Process 2 and compare results.
        
        Args:
            event: Event object to evaluate (must have court_type attribute for Tennis events)
            minutes_until_start: Minutes until event starts
            
        Returns:
            DualProcessReport with complete evaluation and comparison
        """
        from datetime import datetime
        
        try:
            logger.info(f"🔄 Dual Process evaluation starting for event {event.id} ({event.home_team} vs {event.away_team})")
            
            # Execute Process 1 (Historical Pattern Matching)
            process1_report, process1_prediction, process1_status = self._execute_process1(event, minutes_until_start)
            
            # Execute Process 2 (Sport-Specific Formulas)
            process2_report, process2_prediction, process2_status = self._execute_process2(event)
            
            # Compare results
            verdict, final_prediction, agreement_details = self._compare_predictions(
                process1_prediction, process2_prediction
            )
            
            # Create dual process report
            dual_report = DualProcessReport(
                event_id=event.id,
                participants=f"{event.home_team} vs {event.away_team}",
                sport=event.sport,
                court_type=getattr(event, 'court_type', None),  # Get court_type from event object
                process1_report=process1_report,
                process1_prediction=process1_prediction,
                process1_status=process1_status,
                process2_report=process2_report,
                process2_prediction=process2_prediction,
                process2_status=process2_status,
                verdict=verdict,
                final_prediction=final_prediction,
                agreement_details=agreement_details,
                minutes_until_start=minutes_until_start,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            
            logger.info(f"🎯 Dual Process completed for event {event.id}: {verdict.value}")
            return dual_report
            
        except Exception as e:
            logger.error(f"❌ Error in dual process evaluation for event {event.id}: {e}")
            return self._create_error_report(event, str(e), minutes_until_start)
    
    def _execute_process1(self, event, minutes_until_start: int) -> Tuple[Optional[Dict], Optional[Tuple[str, int]], str]:
        """
        Execute Process 1 (Historical Pattern Matching).
        
        Args:
            event: Event object (must have court_type attribute for Tennis events)
            minutes_until_start: Minutes until event starts
        
        Returns:
            Tuple of (report_dict, prediction_tuple, status_string)
        """
        try:
            logger.info(f"🔍 Executing Process 1 for event {event.id}")
            
            from alert_engine import alert_engine
            
            # Evaluate using Process 1 (event object has court_type attribute)
            alerts = alert_engine.evaluate_single_event(event, minutes_until_start)
            
            if alerts and len(alerts) > 0:
                alert_report = alerts[0]  # Get first alert report
                
                # Extract prediction from Process 1 report
                primary_prediction = alert_report.get('primary_prediction')
                status = alert_report.get('status', 'unknown')
                
                # Check if we have a primary prediction (success case)
                if primary_prediction and status == 'success':
                    # Parse Process 1 prediction
                    winner_side = self._extract_winner_from_process1_prediction(primary_prediction)
                    if winner_side:
                        process1_prediction = (winner_side, 1)  # Always point_diff=1 for comparison
                        logger.info(f"✅ Process 1 prediction: {winner_side}")
                        return alert_report, process1_prediction, 'success'
                
                # For partial/no_match cases, we still want to show the report even without prediction
                # This allows dual process to show Process 1 findings even when no clear prediction
                logger.info(f"⚠️ Process 1 generated report but no clear prediction (status: {status})")
                return alert_report, None, status
            else:
                logger.info(f"⚠️ Process 1 found no candidates for event {event.id}")
                return None, None, 'no_candidates'
                
        except Exception as e:
            logger.error(f"❌ Error executing Process 1 for event {event.id}: {e}")
            return None, None, f'error: {str(e)}'
    
    def _execute_process2(self, event) -> Tuple[Optional[Dict], Optional[Tuple[str, int]], str]:
        """
        Execute Process 2 (Sport-Specific Formulas).
        
        Returns:
            Tuple of (report_dict, prediction_tuple, status_string)
        """
        try:
            logger.info(f"🧪 Executing Process 2 for event {event.id}")
            
            from process2 import Process2Engine
            
            process2_engine = Process2Engine()
            process2_report = process2_engine.evaluate_event(event)
            
            if process2_report:
                # Convert Process2Report to dict for consistency
                report_dict = {
                    'event_id': process2_report.event_id,
                    'sport': process2_report.sport,
                    'participants': process2_report.participants,
                    'variables_calculated': process2_report.variables_calculated,
                    'activated_formulas': [
                        {
                            'formula_name': f.formula_name,
                            'winner_side': f.winner_side,
                            'point_diff': f.point_diff
                        }
                        for f in process2_report.activated_formulas
                    ],
                    'primary_prediction': process2_report.primary_prediction,
                    'total_formulas_checked': process2_report.total_formulas_checked,
                    'formulas_activated_count': process2_report.formulas_activated_count,
                    'status': process2_report.status
                }
                
                prediction = process2_report.primary_prediction
                status = process2_report.status
                
                if prediction and status == 'success':
                    logger.info(f"✅ Process 2 prediction: {prediction[0]}")
                    return report_dict, prediction, 'success'
                else:
                    logger.info(f"⚠️ Process 2 no prediction: {status}")
                    return report_dict, None, status
            else:
                logger.info(f"⚠️ Process 2 returned no report for event {event.id}")
                return None, None, 'no_report'
                
        except Exception as e:
            logger.error(f"❌ Error executing Process 2 for event {event.id}: {e}")
            return None, None, f'error: {str(e)}'
    
    def _compare_predictions(self, process1_prediction: Optional[Tuple[str, int]], 
                           process2_prediction: Optional[Tuple[str, int]]) -> Tuple[ComparisonVerdict, Optional[Tuple[str, int]], str]:
        """
        Compare predictions from both processes.
        
        Args:
            process1_prediction: (winner_side, point_diff) from Process 1
            process2_prediction: (winner_side, point_diff) from Process 2
            
        Returns:
            Tuple of (verdict, final_prediction, agreement_details)
        """
        try:
            # Both processes have predictions
            if process1_prediction and process2_prediction:
                p1_winner, p1_diff = process1_prediction
                p2_winner, p2_diff = process2_prediction
                
                # Compare only winner_side (as specified)
                if p1_winner == p2_winner:
                    logger.info(f"🤝 AGREEMENT: Both processes predict {p1_winner}")
                    return (
                        ComparisonVerdict.AGREE,
                        process1_prediction,  # Use Process 1 as final prediction
                        f"Both processes agree: {p1_winner} wins"
                    )
                else:
                    logger.info(f"⚔️ DISAGREEMENT: Process 1 predicts {p1_winner}, Process 2 predicts {p2_winner}")
                    return (
                        ComparisonVerdict.DISAGREE,
                        None,  # No final prediction on disagreement
                        f"Disagreement: Process 1 predicts {p1_winner}, Process 2 predicts {p2_winner}"
                    )
            
            # Only Process 1 has prediction
            elif process1_prediction and not process2_prediction:
                logger.info(f"📊 PARTIAL: Only Process 1 has prediction: {process1_prediction[0]}")
                return (
                    ComparisonVerdict.PARTIAL,
                    process1_prediction,
                    "Only Process 1 generated prediction"
                )
            
            # Only Process 2 has prediction
            elif not process1_prediction and process2_prediction:
                logger.info(f"🧪 PARTIAL: Only Process 2 has prediction: {process2_prediction[0]}")
                return (
                    ComparisonVerdict.PARTIAL,
                    process2_prediction,
                    "Only Process 2 generated prediction"
                )
            
            # Neither process has prediction
            else:
                logger.info(f"❌ NO PREDICTIONS: Neither process generated a prediction")
                return (
                    ComparisonVerdict.PARTIAL,
                    None,
                    "Neither process generated prediction"
                )
                
        except Exception as e:
            logger.error(f"❌ Error comparing predictions: {e}")
            return (
                ComparisonVerdict.ERROR,
                None,
                f"Error in comparison: {str(e)}"
            )
    
    def _extract_winner_from_process1_prediction(self, prediction_text: str) -> Optional[str]:
        """
        Extract winner_side from Process 1 prediction text.
        
        Args:
            prediction_text: Prediction text from Process 1
            
        Returns:
            Winner side ('1', 'X', '2') or None
        """
        try:
            if not prediction_text:
                return None
            
            prediction_lower = prediction_text.lower()
            
            # Check for draw (exact match first)
            if prediction_lower == 'draw':
                return 'X'
            
            # Check for home win (exact match first)
            if prediction_lower.startswith('home'):
                return '1'
            
            # Check for away win (exact match first)
            if prediction_lower.startswith('away'):
                return '2'
            
            # Check for draw in text
            if 'draw' in prediction_lower or 'empate' in prediction_lower:
                return 'X'
            
            # Check for home win in text
            if 'home' in prediction_lower or 'local' in prediction_lower:
                return '1'
            
            # Check for away win in text
            if 'away' in prediction_lower or 'visita' in prediction_lower:
                return '2'
            
            # Try to extract from pattern like "Home wins by..."
            if 'wins' in prediction_lower:
                if prediction_lower.startswith('home'):
                    return '1'
                elif prediction_lower.startswith('away'):
                    return '2'
            
            logger.warning(f"⚠️ Could not extract winner from prediction: '{prediction_text}'")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error extracting winner from prediction '{prediction_text}': {e}")
            return None
    
    def _create_error_report(self, event, error_message: str, minutes_until_start: int = None) -> DualProcessReport:
        """Create error report for failed dual process evaluation"""
        from datetime import datetime
        
        return DualProcessReport(
            event_id=event.id if hasattr(event, 'id') else 0,
            participants=f"{getattr(event, 'home_team', '?')} vs {getattr(event, 'away_team', '?')}",
            sport=getattr(event, 'sport', 'Unknown'),
            court_type=getattr(event, 'court_type', None),  # Get court_type from event object
            process1_report=None,
            process1_prediction=None,
            process1_status='error',
            process2_report=None,
            process2_prediction=None,
            process2_status='error',
            verdict=ComparisonVerdict.ERROR,
            final_prediction=None,
            agreement_details=f"Error: {error_message}",
            minutes_until_start=minutes_until_start,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

# Global prediction engine instance
prediction_engine = PredictionEngine()
