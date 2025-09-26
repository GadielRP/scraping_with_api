#!/usr/bin/env python3
"""
Process 2 Engine - Sport-Specific Rules Engine

PROCESS 2 ENGINE BOUNDARIES:
============================
START: This file contains the main Process 2 engine
END: Process 2 engine ends at the end of this file

PROCESS 2 ENGINE RESPONSIBILITIES:
- Evaluate current events using sport-specific formulas
- Calculate sport-specific variables in-memory
- Execute formula methods and collect results
- Return compatible format with Process 1
- Handle errors gracefully without breaking main flow
"""

import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

from .sports.football import FootballFormulas

logger = logging.getLogger(__name__)

@dataclass
class FormulaResult:
    """Result from a formula activation"""
    formula_name: str
    winner_side: str  # '1', 'X', '2'
    point_diff: int   # Always 1 for now, future enhancement
    variables_used: Optional[Dict] = None  # For debugging

@dataclass
class Process2Report:
    """Complete Process 2 evaluation report"""
    event_id: int
    sport: str
    participants: str
    variables_calculated: Dict  # β, ζ, γ, δ, ε
    activated_formulas: List[FormulaResult]
    primary_prediction: Optional[Tuple[str, int]]  # (winner_side, point_diff)
    total_formulas_checked: int
    formulas_activated_count: int
    status: str  # 'success', 'no_formulas_activated', 'error'

class Process2Engine:
    """Main Process 2 engine for sport-specific rule evaluation"""
    
    def __init__(self):
        logger.info("🏗️ Process 2 Engine initialized")
    
    def evaluate_event(self, event) -> Optional[Process2Report]:
        """
        Evaluate a single event using Process 2 sport-specific rules.
        
        Args:
            event: Event object with odds and variations
            
        Returns:
            Process2Report with evaluation results or None if error
        """
        try:
            sport = event.sport.lower()
            logger.info(f"🔍 Process 2 evaluating event {event.id} ({event.home_team} vs {event.away_team}) - Sport: {sport}")
            
            if sport == 'football':
                return self._evaluate_football(event)
            else:
                logger.info(f"⏭️ Process 2: Sport '{sport}' not supported yet, skipping")
                return None
                
        except Exception as e:
            logger.error(f"❌ Process 2 error evaluating event {event.id}: {e}")
            return None
    
    def _evaluate_football(self, event) -> Process2Report:
        """
        Evaluate football event using football-specific formulas.
        
        Args:
            event: Football event to evaluate
            
        Returns:
            Process2Report with football evaluation results
        """
        try:
            # Load event odds if not already loaded
            if not hasattr(event, 'event_odds') or event.event_odds is None:
                try:
                    with db_manager.get_session() as session:
                        from models import EventOdds
                        event.event_odds = session.query(EventOdds).filter_by(event_id=event.id).first()
                except Exception as e:
                    logger.error(f"❌ Error loading event odds for {event.id}: {e}")
                    return self._create_error_report(event, "Failed to load event odds")
            
            if not event.event_odds:
                logger.warning(f"⚠️ No odds found for football event {event.id}")
                return self._create_error_report(event, "No odds data available")
            
            # Extract variations
            var_one = float(event.event_odds.var_one or 0)
            var_x = float(event.event_odds.var_x or 0)  
            var_two = float(event.event_odds.var_two or 0)
            
            logger.info(f"📊 Football variations: var_one={var_one:.3f}, var_x={var_x:.3f}, var_two={var_two:.3f}")
            
            # Initialize football formulas
            football_formulas = FootballFormulas(var_one, var_x, var_two)
            
            # Store calculated variables for report
            variables_calculated = {
                'var_one': var_one,
                'var_x': var_x, 
                'var_two': var_two,
                'β': football_formulas.β,
                'ζ': football_formulas.ζ,
                'γ': football_formulas.γ,
                'δ': football_formulas.δ,
                'ε': football_formulas.ε
            }
            
            # Execute all formulas
            activated_formulas = []
            all_formulas = football_formulas.get_all_formulas()
            
            logger.info(f"🧪 Executing {len(all_formulas)} football formulas...")
            
            for formula_method in all_formulas:
                try:
                    result = formula_method()
                    if result:
                        formula_result = FormulaResult(
                            formula_name=formula_method.__name__,
                            winner_side=result[0],
                            point_diff=result[1],
                            variables_used=variables_calculated
                        )
                        activated_formulas.append(formula_result)
                        logger.info(f"✅ Formula {formula_method.__name__} activated: {result[0]} wins")
                        
                except Exception as e:
                    logger.error(f"❌ Error executing formula {formula_method.__name__}: {e}")
                    continue
            
            # Determine primary prediction
            primary_prediction = self._determine_primary_prediction(activated_formulas)
            
            # Determine status
            if activated_formulas:
                status = 'success'
                logger.info(f"🎯 Process 2 SUCCESS: {len(activated_formulas)} formulas activated for event {event.id}")
            else:
                status = 'no_formulas_activated'
                logger.info(f"⚠️ Process 2 NO ACTIVATION: No formulas activated for event {event.id}")
            
            # Create report
            return Process2Report(
                event_id=event.id,
                sport=event.sport,
                participants=f"{event.home_team} vs {event.away_team}",
                variables_calculated=variables_calculated,
                activated_formulas=activated_formulas,
                primary_prediction=primary_prediction,
                total_formulas_checked=len(all_formulas),
                formulas_activated_count=len(activated_formulas),
                status=status
            )
            
        except Exception as e:
            logger.error(f"❌ Error in football evaluation for event {event.id}: {e}")
            return self._create_error_report(event, str(e))
    
    def _determine_primary_prediction(self, activated_formulas: List[FormulaResult]) -> Optional[Tuple[str, int]]:
        """
        Determine primary prediction from activated formulas.
        For now, use simple majority vote. Future: weighted voting.
        
        Args:
            activated_formulas: List of activated formula results
            
        Returns:
            Primary prediction as (winner_side, point_diff) or None
        """
        if not activated_formulas:
            return None
        
        # Count votes by winner_side
        votes = {}
        for formula_result in activated_formulas:
            winner = formula_result.winner_side
            if winner not in votes:
                votes[winner] = []
            votes[winner].append(formula_result)
        
        # Find winner with most votes
        if votes:
            most_voted_winner = max(votes.keys(), key=lambda k: len(votes[k]))
            vote_count = len(votes[most_voted_winner])
            
            logger.info(f"🗳️ Primary prediction: {most_voted_winner} with {vote_count} votes out of {len(activated_formulas)} formulas")
            
            # For now, always return point_diff=1
            return (most_voted_winner, 1)
        
        return None
    
    def _create_error_report(self, event, error_message: str) -> Process2Report:
        """Create error report for failed evaluation"""
        return Process2Report(
            event_id=event.id,
            sport=event.sport if hasattr(event, 'sport') else 'Unknown',
            participants=f"{getattr(event, 'home_team', '?')} vs {getattr(event, 'away_team', '?')}",
            variables_calculated={},
            activated_formulas=[],
            primary_prediction=None,
            total_formulas_checked=0,
            formulas_activated_count=0,
            status=f'error: {error_message}'
        )
    
    def evaluate_multiple_events(self, events: List) -> List[Process2Report]:
        """
        Evaluate multiple events using Process 2.
        
        Args:
            events: List of events to evaluate
            
        Returns:
            List of Process2Report objects
        """
        reports = []
        
        for event in events:
            try:
                report = self.evaluate_event(event)
                if report:
                    reports.append(report)
            except Exception as e:
                logger.error(f"❌ Error evaluating event {event.id} in batch: {e}")
                continue
        
        logger.info(f"📊 Process 2 batch evaluation completed: {len(reports)} reports generated from {len(events)} events")
        return reports

# Import db_manager at the end to avoid circular imports
try:
    from database import db_manager
except ImportError:
    logger.warning("⚠️ Could not import db_manager, some functionality may be limited")
