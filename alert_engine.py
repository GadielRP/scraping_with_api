#!/usr/bin/env python3
"""
Alert Engine - Pattern-based odds alerts using historical data

This module evaluates upcoming events against historical patterns to predict outcomes.
Uses materialized views for fast querying and supports two primary alert rules:
1. Identical results: exact same score
2. Similar results: same winner and point difference

CURRENT IMPLEMENTATION:
- Tier 1: EXACT identical variations matching (var_one, var_x, var_two must match exactly)
- Tier 2: SIMILAR variations matching (each variation within ±0.04 tolerance)
- Comprehensive candidate reporting for all found matches
"""

import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass

from database import db_manager
from alert_system import pre_start_notifier

logger = logging.getLogger(__name__)

@dataclass
class AlertMatch:
    """Represents a historical match that fits the pattern"""
    event_id: int
    participants: str
    result_text: str
    winner_side: str
    point_diff: int
    var_one: float
    var_x: Optional[float]
    var_two: float

@dataclass
class AlertPrediction:
    """Represents a prediction based on historical matches"""
    rule_type: str  # 'identical' or 'similar'
    prediction: str  # Human-readable prediction
    winner_side: str  # '1', 'X', '2'
    point_diff: Optional[int]
    exact_score: Optional[str]  # For identical results
    sample_count: int
    confidence: str  # 'high' if identical, 'medium' if similar

class AlertEngine:
    """Main alert engine for pattern-based predictions"""
    
    def __init__(self):
        self.MIN_SAMPLES = 1   # Minimum historical matches needed
        self.TIER2_TOLERANCE = 0.04  # Tolerance for similar variations matching
        
    def evaluate_upcoming_events(self, upcoming_events: List) -> List[Dict]:
        """
        Evaluate all upcoming events for alert patterns.
        Returns list of alerts to send.
        """
        alerts = []
        
        for event in upcoming_events:
            try:
                # Calculate minutes until start for this event
                from datetime import datetime
                now = datetime.now()
                time_diff = event.start_time_utc - now
                minutes_until_start = round(time_diff.total_seconds() / 60)
                
                event_alerts = self.evaluate_single_event(event, minutes_until_start)
                alerts.extend(event_alerts)
            except Exception as e:
                logger.error(f"Error evaluating event {event.id}: {e}")
                continue
                
        return alerts
    
    def evaluate_single_event(self, event, minutes_until_start: int = None) -> List[Dict]:
        """
        Evaluate a single upcoming event for alert patterns.
        Returns list of alert dictionaries.
        
        Args:
            event: Event object to evaluate
            minutes_until_start: Minutes until the event starts (calculated if not provided)
        """
        # Load event odds if not already loaded
        if not hasattr(event, 'event_odds') or event.event_odds is None:
            try:
                with db_manager.get_session() as session:
                    from models import EventOdds
                    event.event_odds = session.query(EventOdds).filter_by(event_id=event.id).first()
            except Exception as e:
                logger.error(f"Error loading event odds for {event.id}: {e}")
                return []
                
        if not event.event_odds:
            logger.debug(f"No odds found for event {event.id}")
            return []
        
        # Get current event's odds variations
        current_vars = self._get_event_variations(event.id)
        if not current_vars:
            logger.debug(f"No variations found for event {event.id}")
            return []
            
        cur_v1, cur_vx, cur_v2, var_shape = current_vars
        # Convert Decimal to float for calculations
        cur_v1 = float(cur_v1 or 0)
        cur_vx = float(cur_vx) if cur_vx is not None else None
        cur_v2 = float(cur_v2 or 0)

        # Informational summary for current event context
        try:
            participants = f"{getattr(event, 'home_team', '?')} vs {getattr(event, 'away_team', '?')}"
        except Exception:
            participants = "? vs ?"
        logger.info(
            f"🚨 Event {event.id} ({participants}) vars: d1={cur_v1:.2f}, dx={(cur_vx if cur_vx is not None else 0):.2f}, d2={cur_v2:.2f}, "
            f"shape={'3-way' if var_shape else 'no-draw'}"
        )
        
        # Find candidates for both tiers
        tier1_candidates = self._find_tier1_candidates(
            sport=event.sport,
            var_shape=var_shape,
            cur_v1=cur_v1,
            cur_vx=cur_vx,
            cur_v2=cur_v2
        )
        
        tier2_candidates = self._find_tier2_candidates(
            sport=event.sport,
            var_shape=var_shape,
            cur_v1=cur_v1,
            cur_vx=cur_vx,
            cur_v2=cur_v2
        )
        
        # Log candidate findings
        logger.info(f"Found {len(tier1_candidates)} Tier 1 (exact) candidates for event {event.id}")
        logger.info(f"Found {len(tier2_candidates)} Tier 2 (similar) candidates for event {event.id}")
        
        # Create comprehensive candidate report
        if tier1_candidates or tier2_candidates:
            candidate_report = self._create_candidate_report(
                event=event,
                tier1_candidates=tier1_candidates,
                tier2_candidates=tier2_candidates,
                current_vars=(cur_v1, cur_vx, cur_v2),
                minutes_until_start=minutes_until_start
            )
            return [candidate_report]
        
        logger.info(f"No candidates found for event {event.id}")
        return []
    
    def _get_event_variations(self, event_id: int) -> Optional[Tuple]:
        """Get variations for an event from event_odds table"""
        try:
            with db_manager.get_session() as session:
                from models import EventOdds
                odds = session.query(EventOdds).filter_by(event_id=event_id).first()
                if not odds:
                    return None
                    
                var_shape = odds.var_x is not None
                return (odds.var_one, odds.var_x, odds.var_two, var_shape)
                
        except Exception as e:
            logger.error(f"Error getting variations for event {event_id}: {e}")
            return None
    
    def _find_tier1_candidates(self, sport: str, var_shape: bool, 
                               cur_v1: float, cur_vx: Optional[float], 
                               cur_v2: float) -> List[AlertMatch]:
        """Find historical events with EXACTLY identical variations"""
        try:
            with db_manager.get_session() as session:
                from sqlalchemy import text
                
                # Search for EXACTLY identical variations
                logger.info(f"Searching for EXACTLY identical variations...")
                dx_display = f"{cur_vx:.2f}" if cur_vx is not None else "NULL"
                logger.info(f"Current variations: d1={cur_v1:.2f}, dx={dx_display}, d2={cur_v2:.2f}")
                
                # Handle None cur_vx case to avoid PostgreSQL parameter type ambiguity
                if cur_vx is None:
                    exact_sql = text("""
                    SELECT event_id, participants, result_text, winner_side, point_diff,
                           var_one, var_x, var_two
                    FROM mv_alert_events
                    WHERE sport = :sport
                      AND var_shape = :var_shape
                      AND var_one = :cur_v1
                      AND var_two = :cur_v2
                      AND var_x IS NULL
                    """)
                    exact_result = session.execute(exact_sql, {
                        'sport': sport,
                        'var_shape': var_shape,
                        'cur_v1': cur_v1,
                        'cur_v2': cur_v2
                    })
                else:
                    exact_sql = text("""
                    SELECT event_id, participants, result_text, winner_side, point_diff,
                           var_one, var_x, var_two
                    FROM mv_alert_events
                    WHERE sport = :sport
                      AND var_shape = :var_shape
                      AND var_one = :cur_v1
                      AND var_two = :cur_v2
                      AND var_x = :cur_vx
                    """)
                    exact_result = session.execute(exact_sql, {
                        'sport': sport,
                        'var_shape': var_shape,
                        'cur_v1': cur_v1,
                        'cur_v2': cur_v2,
                        'cur_vx': cur_vx
                    })
                
                exact_candidates = exact_result.fetchall()
                logger.info(f"Found {len(exact_candidates)} candidates with EXACTLY identical variations")
                
                # Process exact matches
                exact_matches = []
                for row in exact_candidates:
                    dx_display = f"{row.var_x:.2f}" if row.var_x is not None else "NULL"
                    logger.info(
                        f"EXACT MATCH: event_id={row.event_id} vars=(d1={row.var_one:.2f}, dx={dx_display}, d2={row.var_two:.2f}) "
                        f"| result={row.result_text}, winner={row.winner_side}, point_diff={row.point_diff}"
                    )
                    exact_matches.append(AlertMatch(
                        event_id=row.event_id,
                        participants=row.participants,
                        result_text=row.result_text,
                        winner_side=row.winner_side,
                        point_diff=row.point_diff,
                        var_one=float(row.var_one),
                        var_x=float(row.var_x) if row.var_x is not None else None,
                        var_two=float(row.var_two)
                    ))
                
                if exact_matches:
                    logger.info(f"SUCCESS: Found {len(exact_matches)} exact matches - using these for alert evaluation")
                else:
                    logger.info(f"No exact matches found")
                
                return exact_matches
                
        except Exception as e:
            logger.error(f"Error finding historical matches: {e}")
            return []
    
    def _find_tier2_candidates(self, sport: str, var_shape: bool, 
                               cur_v1: float, cur_vx: Optional[float], 
                               cur_v2: float) -> List[AlertMatch]:
        """Find historical events with SIMILAR variations (within ±0.04 tolerance)"""
        try:
            with db_manager.get_session() as session:
                from sqlalchemy import text
                
                # Search for SIMILAR variations (within tolerance)
                logger.info(f"Searching for SIMILAR variations (tolerance: ±{self.TIER2_TOLERANCE})...")
                dx_display = f"{cur_vx:.2f}" if cur_vx is not None else "NULL"
                logger.info(f"Current variations: d1={cur_v1:.2f}, dx={dx_display}, d2={cur_v2:.2f}")
                
                # Handle None cur_vx case to avoid PostgreSQL parameter type ambiguity
                if cur_vx is None:
                    similar_sql = text("""
                    SELECT event_id, participants, result_text, winner_side, point_diff,
                           var_one, var_x, var_two
                    FROM mv_alert_events
                    WHERE sport = :sport
                      AND var_shape = :var_shape
                      AND ABS(var_one - :cur_v1) <= :tolerance
                      AND ABS(var_two - :cur_v2) <= :tolerance
                      AND var_x IS NULL
                    """)
                    similar_result = session.execute(similar_sql, {
                        'sport': sport,
                        'var_shape': var_shape,
                        'cur_v1': cur_v1,
                        'cur_v2': cur_v2,
                        'tolerance': self.TIER2_TOLERANCE
                    })
                else:
                    similar_sql = text("""
                    SELECT event_id, participants, result_text, winner_side, point_diff,
                           var_one, var_x, var_two
                    FROM mv_alert_events
                    WHERE sport = :sport
                      AND var_shape = :var_shape
                      AND ABS(var_one - :cur_v1) <= :tolerance
                      AND ABS(var_two - :cur_v2) <= :tolerance
                      AND var_x IS NOT NULL AND ABS(var_x - :cur_vx) <= :tolerance
                    """)
                    similar_result = session.execute(similar_sql, {
                        'sport': sport,
                        'var_shape': var_shape,
                        'cur_v1': cur_v1,
                        'cur_v2': cur_v2,
                        'cur_vx': cur_vx,
                        'tolerance': self.TIER2_TOLERANCE
                    })
                
                similar_candidates = similar_result.fetchall()
                logger.info(f"Found {len(similar_candidates)} candidates with SIMILAR variations")
                
                # Process similar matches
                similar_matches = []
                for row in similar_candidates:
                    dx_display = f"{row.var_x:.2f}" if row.var_x is not None else "NULL"
                    # Calculate differences (convert Decimal to float for calculations)
                    d1_diff = abs(float(row.var_one) - cur_v1)
                    d2_diff = abs(float(row.var_two) - cur_v2)
                    dx_diff = abs(float(row.var_x) - cur_vx) if row.var_x is not None and cur_vx is not None else 0
                    
                    logger.info(
                        f"SIMILAR MATCH: event_id={row.event_id} vars=(d1={row.var_one:.2f}, dx={dx_display}, d2={row.var_two:.2f}) "
                        f"| diffs=(d1={d1_diff:.3f}, dx={dx_diff:.3f}, d2={d2_diff:.3f}) "
                        f"| result={row.result_text}, winner={row.winner_side}, point_diff={row.point_diff}"
                    )
                    similar_matches.append(AlertMatch(
                        event_id=row.event_id,
                        participants=row.participants,
                        result_text=row.result_text,
                        winner_side=row.winner_side,
                        point_diff=row.point_diff,
                        var_one=float(row.var_one),
                        var_x=float(row.var_x) if row.var_x is not None else None,
                        var_two=float(row.var_two)
                    ))
                
                if similar_matches:
                    logger.info(f"SUCCESS: Found {len(similar_matches)} similar matches - using these for alert evaluation")
                else:
                    logger.info(f"No similar matches found")
                
                return similar_matches
                
        except Exception as e:
            logger.error(f"Error finding similar historical matches: {e}")
            return []
    
    def _evaluate_identical_results(self, matches: List[AlertMatch]) -> Optional[AlertPrediction]:
        """Check if all matches have identical exact scores"""
        if not matches:
            return None
            
        # Group by exact result
        result_groups = {}
        for match in matches:
            result = match.result_text
            if result not in result_groups:
                result_groups[result] = []
            result_groups[result].append(match)
        
        # Log group composition
        group_summary = ", ".join([f"{k}:{len(v)}" for k, v in result_groups.items()])
        logger.info(f"Identical rule grouping by exact score -> {group_summary if group_summary else 'no groups'}")

        # Check for unanimity (all matches have same exact score)
        if len(result_groups) == 1:
            result_text = list(result_groups.keys())[0]
            sample_match = matches[0]
            
            # Format prediction based on sport type
            if sample_match.point_diff is not None and sample_match.point_diff > 0:
                winner_name = {
                    '1': 'Home',
                    'X': 'Draw', 
                    '2': 'Away'
                }.get(sample_match.winner_side, 'Unknown')
                
                if sample_match.winner_side == 'X':
                    prediction_text = "Draw"
                else:
                    prediction_text = f"{winner_name} wins by {sample_match.point_diff}"
            else:
                prediction_text = f"Exact score: {result_text}"
            
            return AlertPrediction(
                rule_type='identical',
                prediction=prediction_text,
                winner_side=sample_match.winner_side,
                point_diff=sample_match.point_diff,
                exact_score=result_text,
                sample_count=len(matches),
                confidence='high'
            )
        
        return None
    
    def _evaluate_similar_results(self, matches: List[AlertMatch]) -> Optional[AlertPrediction]:
        """Check if all matches have same winner and point difference"""
        if not matches:
            return None
            
        # Group by winner + point_diff
        winner_groups = {}
        for match in matches:
            key = (match.winner_side, match.point_diff)
            if key not in winner_groups:
                winner_groups[key] = []
            winner_groups[key].append(match)
        
        # Log group composition
        group_summary = ", ".join([f"(winner={k[0]}, diff={k[1]}):{len(v)}" for k, v in winner_groups.items()])
        logger.info(f"Similar rule grouping by winner+diff -> {group_summary if group_summary else 'no groups'}")

        # Check for unanimity (all matches have same winner and point diff)
        if len(winner_groups) == 1:
            (winner_side, point_diff), group_matches = list(winner_groups.items())[0]
            
            winner_name = {
                '1': 'Home',
                'X': 'Draw', 
                '2': 'Away'
            }.get(winner_side, 'Unknown')
            
            prediction = f"{winner_name} wins by {point_diff}"
            if winner_side == 'X':
                prediction = "Draw"
            
            return AlertPrediction(
                rule_type='similar',
                prediction=prediction,
                winner_side=winner_side,
                point_diff=point_diff,
                exact_score=None,
                sample_count=len(matches),
                confidence='medium'
            )
        
        return None
    
    def _create_candidate_report(self, event, tier1_candidates: List[AlertMatch], 
                                tier2_candidates: List[AlertMatch], 
                                current_vars: Tuple, minutes_until_start: int = None) -> Dict:
        """Create comprehensive candidate report for all found matches"""
        cur_v1, cur_vx, cur_v2 = current_vars
        
        # Format current event variations
        vars_display = f"Δ1: {cur_v1:.2f}"
        if cur_vx is not None:
            vars_display += f", ΔX: {cur_vx:.2f}"
        vars_display += f", Δ2: {cur_v2:.2f}"
        
        # Format odds display
        odds_display = f"1: {event.event_odds.one_open}→{event.event_odds.one_final}"
        if event.event_odds.x_open and event.event_odds.x_final:
            odds_display += f", X: {event.event_odds.x_open}→{event.event_odds.x_final}"
        odds_display += f", 2: {event.event_odds.two_open}→{event.event_odds.two_final}"
        
        # Process Tier 1 candidates
        tier1_matches_data = []
        tier1_identical_results = None
        tier1_similar_results = None
        
        if tier1_candidates:
            logger.info(f"Evaluating {len(tier1_candidates)} Tier 1 candidates for identical results...")
            tier1_identical_results = self._evaluate_identical_results(tier1_candidates)
            logger.info(f"Tier 1 identical results: {'TRIGGER' if tier1_identical_results else 'no'}")
            
            if not tier1_identical_results:
                logger.info(f"Evaluating {len(tier1_candidates)} Tier 1 candidates for similar results...")
                tier1_similar_results = self._evaluate_similar_results(tier1_candidates)
                logger.info(f"Tier 1 similar results: {'TRIGGER' if tier1_similar_results else 'no'}")
            
            # Format Tier 1 matches data
            for match in tier1_candidates:
                # Format individual match variations
                match_vars_display = f"Δ1: {match.var_one:.2f}"
                if match.var_x is not None:
                    match_vars_display += f", ΔX: {match.var_x:.2f}"
                match_vars_display += f", Δ2: {match.var_two:.2f}"
                
                tier1_matches_data.append({
                    'participants': match.participants,
                    'result_text': match.result_text,
                    'variations': {
                        'var_one': match.var_one,
                        'var_x': match.var_x,
                        'var_two': match.var_two
                    }
                })
        
        # Process Tier 2 candidates
        tier2_matches_data = []
        tier2_identical_results = None
        tier2_similar_results = None
        
        if tier2_candidates:
            logger.info(f"Evaluating {len(tier2_candidates)} Tier 2 candidates for identical results...")
            tier2_identical_results = self._evaluate_identical_results(tier2_candidates)
            logger.info(f"Tier 2 identical results: {'TRIGGER' if tier2_identical_results else 'no'}")
            
            if not tier2_identical_results:
                logger.info(f"Evaluating {len(tier2_candidates)} Tier 2 candidates for similar results...")
                tier2_similar_results = self._evaluate_similar_results(tier2_candidates)
                logger.info(f"Tier 2 similar results: {'TRIGGER' if tier2_similar_results else 'no'}")
            
            # Format Tier 2 matches data
            for match in tier2_candidates:
                # Format individual match variations
                match_vars_display = f"Δ1: {match.var_one:.2f}"
                if match.var_x is not None:
                    match_vars_display += f", ΔX: {match.var_x:.2f}"
                match_vars_display += f", Δ2: {match.var_two:.2f}"
                
                tier2_matches_data.append({
                    'participants': match.participants,
                    'result_text': match.result_text,
                    'variations': {
                        'var_one': match.var_one,
                        'var_x': match.var_x,
                        'var_two': match.var_two
                    }
                })
        
        # Determine overall status
        has_successful_alert = any([
            tier1_identical_results, tier1_similar_results,
            tier2_identical_results, tier2_similar_results
        ])
        
        # Determine primary prediction (prioritize Tier 1 over Tier 2)
        primary_prediction = None
        if tier1_identical_results:
            primary_prediction = tier1_identical_results
        elif tier1_similar_results:
            primary_prediction = tier1_similar_results
        elif tier2_identical_results:
            primary_prediction = tier2_identical_results
        elif tier2_similar_results:
            primary_prediction = tier2_similar_results
        
        return {
            'event_id': event.id,
            'rule_key': f"candidate_report_{event.id}",
            'participants': f"{event.home_team} vs {event.away_team}",
            'competition': event.competition,
            'sport': event.sport,
            'start_time': event.start_time_utc.strftime("%H:%M"),
            'minutes_until_start': minutes_until_start,
            'odds_display': odds_display,
            'vars_display': vars_display,
            'has_draw_odds': cur_vx is not None,  # True for 3-way sports, False for no-draw sports
            'status': 'success' if has_successful_alert else 'no_match',  # Changed 'failed' to 'no_match'
            'primary_prediction': primary_prediction.prediction if primary_prediction else None,
            'primary_confidence': primary_prediction.confidence if primary_prediction else None,
            'tier1_candidates': {
                'count': len(tier1_candidates),
                'matches': tier1_matches_data,
                'identical_results': tier1_identical_results,
                'similar_results': tier1_similar_results
            },
            'tier2_candidates': {
                'count': len(tier2_candidates),
                'matches': tier2_matches_data,
                'identical_results': tier2_identical_results,
                'similar_results': tier2_similar_results
            }
        }
    
    
    def send_alerts(self, alerts: List[Dict]) -> bool:
        """Send alerts via Telegram and log them"""
        if not alerts:
            return True
            
        success_count = 0
        
        for alert in alerts:
            try:
                # Send Telegram notification using the candidate report template
                message = pre_start_notifier.create_candidate_report_message(alert)
                sent = pre_start_notifier.send_telegram_message(message)
                
                if sent:
                    success_count += 1
                    logger.info(f"✅ Alert sent: {alert['participants']} - {alert.get('primary_prediction', 'N/A')}")
                else:
                    logger.warning(f"❌ Failed to send alert for event {alert['event_id']}")
                    
            except Exception as e:
                logger.error(f"Error sending alert for event {alert['event_id']}: {e}")
                continue
        
        logger.info(f"Sent {success_count}/{len(alerts)} alerts successfully")
        return success_count > 0
    
    

# Global alert engine instance
alert_engine = AlertEngine()
