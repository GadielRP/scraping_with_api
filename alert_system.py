import logging
from typing import Dict, Optional, List
from decimal import Decimal
from datetime import datetime

from odds_utils import calculate_odds_movement
from repository import AlertRepository
from models import EventOdds

logger = logging.getLogger(__name__)

class AlertRule:
    """Base class for alert rules"""
    
    def __init__(self, rule_key: str, description: str):
        self.rule_key = rule_key
        self.description = description
    
    def evaluate(self, event_odds: EventOdds) -> Optional[Dict]:
        """
        Evaluate the rule against event odds.
        
        Args:
            event_odds: EventOdds object to evaluate
        
        Returns:
            Alert payload if triggered, None otherwise
        """
        raise NotImplementedError("Subclasses must implement evaluate method")

class SignificantDropRule(AlertRule):
    """Alert when odds drop significantly"""
    
    def __init__(self, threshold_percent: float = 10.0):
        super().__init__(
            rule_key="significant_drop",
            description=f"Odds dropped by more than {threshold_percent}%"
        )
        self.threshold_percent = threshold_percent
    
    def evaluate(self, event_odds: EventOdds) -> Optional[Dict]:
        """Check for significant drops in any odds"""
        alerts = []
        
        # Check home team odds (1)
        if event_odds.one_open and event_odds.one_final:
            movement = calculate_odds_movement(event_odds.one_open, event_odds.one_final)
            if movement and movement < -self.threshold_percent:
                alerts.append({
                    'choice': '1',
                    'movement': movement,
                    'open_odds': float(event_odds.one_open),
                    'final_odds': float(event_odds.one_final)
                })
        
        # Check draw odds (X)
        if event_odds.x_open and event_odds.x_final:
            movement = calculate_odds_movement(event_odds.x_open, event_odds.x_final)
            if movement and movement < -self.threshold_percent:
                alerts.append({
                    'choice': 'X',
                    'movement': movement,
                    'open_odds': float(event_odds.x_open),
                    'final_odds': float(event_odds.x_final)
                })
        
        # Check away team odds (2)
        if event_odds.two_open and event_odds.two_final:
            movement = calculate_odds_movement(event_odds.two_open, event_odds.two_final)
            if movement and movement < -self.threshold_percent:
                alerts.append({
                    'choice': '2',
                    'movement': movement,
                    'open_odds': float(event_odds.two_open),
                    'final_odds': float(event_odds.two_final)
                })
        
        if alerts:
            return {
                'rule_type': self.rule_key,
                'threshold': self.threshold_percent,
                'alerts': alerts,
                'event_info': {
                    'home_team': event_odds.event.home_team,
                    'away_team': event_odds.event.away_team,
                    'competition': event_odds.event.competition,
                    'start_time': event_odds.event.start_time_utc.isoformat()
                }
            }
        
        return None

class OddsConvergenceRule(AlertRule):
    """Alert when odds converge significantly"""
    
    def __init__(self, convergence_threshold: float = 0.5):
        super().__init__(
            rule_key="odds_convergence",
            description=f"Odds converged to within {convergence_threshold} difference"
        )
        self.convergence_threshold = convergence_threshold
    
    def evaluate(self, event_odds: EventOdds) -> Optional[Dict]:
        """Check if odds have converged significantly"""
        if not all([event_odds.one_final, event_odds.two_final]):
            return None
        
        # Calculate differences between final odds
        diff_1_2 = abs(float(event_odds.one_final) - float(event_odds.two_final))
        
        # Include X if available
        if event_odds.x_final:
            diff_1_x = abs(float(event_odds.one_final) - float(event_odds.x_final))
            diff_x_2 = abs(float(event_odds.x_final) - float(event_odds.two_final))
            
            if (diff_1_2 <= self.convergence_threshold or 
                diff_1_x <= self.convergence_threshold or 
                diff_x_2 <= self.convergence_threshold):
                
                return {
                    'rule_type': self.rule_key,
                    'threshold': self.convergence_threshold,
                    'differences': {
                        'home_away': diff_1_2,
                        'home_draw': diff_1_x,
                        'draw_away': diff_x_2
                    },
                    'final_odds': {
                        'home': float(event_odds.one_final),
                        'draw': float(event_odds.x_final),
                        'away': float(event_odds.two_final)
                    },
                    'event_info': {
                        'home_team': event_odds.event.home_team,
                        'away_team': event_odds.event.away_team,
                        'competition': event_odds.event.competition,
                        'start_time': event_odds.event.start_time_utc.isoformat()
                    }
                }
        else:
            # No draw option (e.g., basketball)
            if diff_1_2 <= self.convergence_threshold:
                return {
                    'rule_type': self.rule_key,
                    'threshold': self.convergence_threshold,
                    'differences': {
                        'home_away': diff_1_2
                    },
                    'final_odds': {
                        'home': float(event_odds.one_final),
                        'away': float(event_odds.two_final)
                    },
                    'event_info': {
                        'home_team': event_odds.event.home_team,
                        'away_team': event_odds.event.away_team,
                        'competition': event_odds.event.competition,
                        'start_time': event_odds.event.start_time_utc.isoformat()
                    }
                }
        
        return None

class ExtremeOddsRule(AlertRule):
    """Alert when odds reach extreme values"""
    
    def __init__(self, min_odds: float = 1.1, max_odds: float = 50.0):
        super().__init__(
            rule_key="extreme_odds",
            description=f"Odds outside normal range ({min_odds} - {max_odds})"
        )
        self.min_odds = min_odds
        self.max_odds = max_odds
    
    def evaluate(self, event_odds: EventOdds) -> Optional[Dict]:
        """Check for extreme odds values"""
        alerts = []
        
        # Check all final odds
        for choice, odds in [('1', event_odds.one_final), 
                           ('X', event_odds.x_final), 
                           ('2', event_odds.two_final)]:
            if odds:
                odds_float = float(odds)
                if odds_float < self.min_odds or odds_float > self.max_odds:
                    alerts.append({
                        'choice': choice,
                        'odds': odds_float,
                        'type': 'too_low' if odds_float < self.min_odds else 'too_high'
                    })
        
        if alerts:
            return {
                'rule_type': self.rule_key,
                'min_odds': self.min_odds,
                'max_odds': self.max_odds,
                'alerts': alerts,
                'event_info': {
                    'home_team': event_odds.event.home_team,
                    'away_team': event_odds.event.away_team,
                    'competition': event_odds.event.competition,
                    'start_time': event_odds.event.start_time_utc.isoformat()
                }
            }
        
        return None

class AlertEngine:
    """Main alert engine that evaluates all rules"""
    
    def __init__(self):
        self.rules = [
            SignificantDropRule(threshold_percent=10.0),
            OddsConvergenceRule(convergence_threshold=0.5),
            ExtremeOddsRule(min_odds=1.1, max_odds=50.0)
        ]
        self.alert_repo = AlertRepository()
    
    def evaluate_event(self, event_odds: EventOdds) -> List[Dict]:
        """
        Evaluate all rules for an event.
        
        Args:
            event_odds: EventOdds object to evaluate
        
        Returns:
            List of triggered alerts
        """
        triggered_alerts = []
        
        for rule in self.rules:
            try:
                alert_payload = rule.evaluate(event_odds)
                if alert_payload:
                    # Create alert in database
                    alert = self.alert_repo.create_alert(
                        event_id=event_odds.event_id,
                        rule_key=rule.rule_key,
                        payload=alert_payload
                    )
                    
                    if alert:
                        triggered_alerts.append({
                            'rule_key': rule.rule_key,
                            'description': rule.description,
                            'payload': alert_payload,
                            'triggered_at': alert.triggered_at
                        })
                        
                        logger.info(f"Alert triggered for event {event_odds.event_id}: {rule.rule_key}")
                
            except Exception as e:
                logger.error(f"Error evaluating rule {rule.rule_key} for event {event_odds.event_id}: {e}")
                continue
        
        return triggered_alerts
    
    def get_recent_alerts(self, hours: int = 24) -> List[Dict]:
        """Get recent alerts with full details"""
        alerts = self.alert_repo.get_recent_alerts(hours)
        
        alert_details = []
        for alert in alerts:
            alert_details.append({
                'id': alert.id,
                'event_id': alert.event_id,
                'rule_key': alert.rule_key,
                'triggered_at': alert.triggered_at,
                'payload': alert.get_payload()
            })
        
        return alert_details
    
    def add_rule(self, rule: AlertRule):
        """Add a custom rule to the engine"""
        self.rules.append(rule)
        logger.info(f"Added custom rule: {rule.rule_key}")
    
    def remove_rule(self, rule_key: str):
        """Remove a rule by key"""
        self.rules = [rule for rule in self.rules if rule.rule_key != rule_key]
        logger.info(f"Removed rule: {rule_key}")

# Global alert engine instance
alert_engine = AlertEngine()
