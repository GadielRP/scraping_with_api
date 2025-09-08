#!/usr/bin/env python3
"""
Alert Engine - Pattern-based odds alerts using historical data

This module evaluates upcoming events against historical patterns to predict outcomes.
Uses materialized views for fast querying and supports two primary alert rules:
1. Identical results: exact same score
2. Similar results: same winner and point difference
"""

import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass

from database import db_manager
from models import AlertLog
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
        self.TOLERANCE = 0.12  # Sum of differences tolerance
        self.MIN_SAMPLES = 1   # Minimum historical matches needed
        
    def evaluate_upcoming_events(self, upcoming_events: List) -> List[Dict]:
        """
        Evaluate all upcoming events for alert patterns.
        Returns list of alerts to send.
        """
        alerts = []
        
        for event in upcoming_events:
            try:
                event_alerts = self.evaluate_single_event(event)
                alerts.extend(event_alerts)
            except Exception as e:
                logger.error(f"Error evaluating event {event.id}: {e}")
                continue
                
        return alerts
    
    def evaluate_single_event(self, event) -> List[Dict]:
        """
        Evaluate a single upcoming event for alert patterns.
        Returns list of alert dictionaries.
        """
        # Load event odds if not already loaded
        if not hasattr(event, 'event_odds') or event.event_odds is None:
            with db_manager.get_session() as session:
                from models import EventOdds
                event.event_odds = session.query(EventOdds).filter_by(event_id=event.id).first()
                
        if not event.event_odds:
            logger.debug(f"No odds found for event {event.id}")
            return []
        
        # Get current event's odds variations
        current_vars = self._get_event_variations(event.id)
        if not current_vars:
            logger.debug(f"No variations found for event {event.id}")
            return []
            
        cur_v1, cur_vx, cur_v2, var_shape = current_vars
        cur_total = (cur_v1 or 0) + (cur_vx or 0) + (cur_v2 or 0)
        
        # Find historical matches
        matches = self._find_historical_matches(
            sport=event.sport,
            var_shape=var_shape,
            cur_v1=cur_v1,
            cur_vx=cur_vx,
            cur_v2=cur_v2,
            cur_total=cur_total
        )
        
        if len(matches) < self.MIN_SAMPLES:
            logger.debug(f"Not enough historical matches for event {event.id}: {len(matches)}")
            return []
            
        logger.info(f"Found {len(matches)} historical matches for event {event.id}")
        
        # Evaluate alert rules
        alerts = []
        
        # Rule 1: Identical results
        identical_prediction = self._evaluate_identical_results(matches)
        if identical_prediction:
            alert = self._create_alert(
                event=event,
                prediction=identical_prediction,
                matches=matches,
                current_vars=(cur_v1, cur_vx, cur_v2)
            )
            alerts.append(alert)
            
        # Rule 2: Similar results (only if identical didn't trigger)
        elif len(matches) >= self.MIN_SAMPLES:
            similar_prediction = self._evaluate_similar_results(matches)
            if similar_prediction:
                alert = self._create_alert(
                    event=event,
                    prediction=similar_prediction,
                    matches=matches,
                    current_vars=(cur_v1, cur_vx, cur_v2)
                )
                alerts.append(alert)
        
        return alerts
    
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
    
    def _find_historical_matches(self, sport: str, var_shape: bool, 
                               cur_v1: float, cur_vx: Optional[float], 
                               cur_v2: float, cur_total: float) -> List[AlertMatch]:
        """Find historical events matching the variation pattern"""
        try:
            with db_manager.get_session() as session:
                # Fast candidate query using indexes
                sql = """
                SELECT event_id, participants, result_text, winner_side, point_diff,
                       var_one, var_x, var_two, var_total
                FROM mv_alert_events
                WHERE sport = :sport
                  AND var_shape = :var_shape
                  AND var_total BETWEEN :min_total AND :max_total
                """
                
                result = session.execute(sql, {
                    'sport': sport,
                    'var_shape': var_shape,
                    'min_total': cur_total - self.TOLERANCE,
                    'max_total': cur_total + self.TOLERANCE
                })
                
                candidates = result.fetchall()
                logger.debug(f"Found {len(candidates)} candidates for {sport}")
                
                # Apply exact sum-of-differences rule
                matches = []
                cur_vx_safe = cur_vx or 0
                
                for row in candidates:
                    v1, vx, v2 = row.var_one, row.var_x or 0, row.var_two
                    sum_diff = abs((v1 - cur_v1) + (vx - cur_vx_safe) + (v2 - cur_v2))
                    
                    if sum_diff <= self.TOLERANCE:
                        matches.append(AlertMatch(
                            event_id=row.event_id,
                            participants=row.participants,
                            result_text=row.result_text,
                            winner_side=row.winner_side,
                            point_diff=row.point_diff,
                            var_one=row.var_one,
                            var_x=row.var_x,
                            var_two=row.var_two
                        ))
                
                logger.debug(f"After exact matching: {len(matches)} matches")
                return matches
                
        except Exception as e:
            logger.error(f"Error finding historical matches: {e}")
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
        
        # Check for unanimity (all matches have same exact score)
        if len(result_groups) == 1:
            result_text = list(result_groups.keys())[0]
            sample_match = matches[0]
            
            return AlertPrediction(
                rule_type='identical',
                prediction=f"Exact score: {result_text}",
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
    
    def _create_alert(self, event, prediction: AlertPrediction, 
                     matches: List[AlertMatch], current_vars: Tuple) -> Dict:
        """Create alert dictionary for notification"""
        cur_v1, cur_vx, cur_v2 = current_vars
        
        # Format odds display
        odds_display = f"1: {event.event_odds.one_open}→{event.event_odds.one_final}"
        if cur_vx is not None:
            odds_display += f", X: {event.event_odds.x_open}→{event.event_odds.x_final}"
        odds_display += f", 2: {event.event_odds.two_open}→{event.event_odds.two_final}"
        
        # Format variations display
        vars_display = f"Δ1: {cur_v1:.2f}"
        if cur_vx is not None:
            vars_display += f", ΔX: {cur_vx:.2f}"
        vars_display += f", Δ2: {cur_v2:.2f}"
        
        return {
            'event_id': event.id,
            'rule_key': f"{prediction.rule_type}_{event.id}",
            'participants': f"{event.home_team} vs {event.away_team}",
            'competition': event.competition,
            'sport': event.sport,
            'start_time': event.start_time_utc.strftime("%H:%M"),
            'prediction': prediction.prediction,
            'confidence': prediction.confidence,
            'sample_count': prediction.sample_count,
            'odds_display': odds_display,
            'vars_display': vars_display,
            'rule_type': prediction.rule_type
        }
    
    def send_alerts(self, alerts: List[Dict]) -> bool:
        """Send alerts via Telegram and log them"""
        if not alerts:
            return True
            
        success_count = 0
        
        for alert in alerts:
            try:
                # Check if already sent (dedupe)
                if self._is_alert_already_sent(alert['event_id'], alert['rule_key']):
                    logger.debug(f"Alert already sent for event {alert['event_id']}, rule {alert['rule_key']}")
                    continue
                
                # Send Telegram notification
                message = self._format_telegram_message(alert)
                sent = pre_start_notifier.send_telegram_message(message)
                
                if sent:
                    # Log successful alert
                    self._log_alert(alert)
                    success_count += 1
                    logger.info(f"✅ Alert sent: {alert['participants']} - {alert['prediction']}")
                else:
                    logger.warning(f"❌ Failed to send alert for event {alert['event_id']}")
                    
            except Exception as e:
                logger.error(f"Error sending alert for event {alert['event_id']}: {e}")
                continue
        
        logger.info(f"Sent {success_count}/{len(alerts)} alerts successfully")
        return success_count > 0
    
    def _format_telegram_message(self, alert: Dict) -> str:
        """Format alert as Telegram message"""
        confidence_emoji = "🎯" if alert['confidence'] == 'high' else "📊"
        rule_name = "Identical Results" if alert['rule_type'] == 'identical' else "Similar Pattern"
        
        message = f"""
{confidence_emoji} **{rule_name} Alert**

🏆 **{alert['participants']}**
🏟️ {alert['competition']} ({alert['sport']})
⏰ Starts at {alert['start_time']}

📈 **Prediction:** {alert['prediction']}
📊 **Historical samples:** {alert['sample_count']}
🎲 **Confidence:** {alert['confidence'].title()}

💰 **Odds:** {alert['odds_display']}
📉 **Variations:** {alert['vars_display']}

*Based on historical pattern analysis*
""".strip()
        
        return message
    
    def _is_alert_already_sent(self, event_id: int, rule_key: str) -> bool:
        """Check if alert was already sent for this event and rule"""
        try:
            with db_manager.get_session() as session:
                existing = session.query(AlertLog).filter_by(
                    event_id=event_id,
                    rule_key=rule_key
                ).first()
                return existing is not None
        except Exception as e:
            logger.error(f"Error checking alert log: {e}")
            return False
    
    def _log_alert(self, alert: Dict):
        """Log sent alert to database"""
        try:
            with db_manager.get_session() as session:
                log_entry = AlertLog(
                    event_id=alert['event_id'],
                    rule_key=alert['rule_key'],
                    triggered_at=datetime.utcnow()
                )
                log_entry.set_payload(alert)
                session.add(log_entry)
                session.commit()
        except Exception as e:
            logger.error(f"Error logging alert: {e}")

# Global alert engine instance
alert_engine = AlertEngine()
