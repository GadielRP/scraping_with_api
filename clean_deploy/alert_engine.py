#!/usr/bin/env python3
"""
Alert Engine - Pattern-based odds alerts using historical data

PROCESS 1 BOUNDARIES:
====================
START: This file contains the complete Process 1 implementation
END: Process 1 ends at the end of this file

PROCESS 1 DEFINITION:
Process 1 is the system of odds pattern analysis that evaluates historical events 
to predict future outcomes using variation tiers and result tiers.

PROCESS 1 ARCHITECTURE:
- Variation Tiers (1, 2): Exact vs Similar odds variations
- Result Tiers (A, B, C): Identical vs Similar vs Same Winner results
- Tier Selection Logic: Tier 1 prioritized over Tier 2
- Weighted Confidence: 100%/75%/50% for Tiers A/B/C
- Status Logic: SUCCESS/NO MATCH/NO CANDIDATES

CURRENT IMPLEMENTATION (Process 1):
- Tier 1: EXACT identical variations matching (var_one, var_x, var_two must match exactly)
- Tier 2: SIMILAR variations matching (each variation within ±0.04 tolerance, inclusive)
- Tier A: All candidates have identical exact results
- Tier B: All candidates have same winner and point difference
- Tier C: All candidates have same winning side (with weighted average point diff)
- Comprehensive candidate reporting for all found matches

PROCESS 2 PREPARATION:
Process 2 will be sport-specific rules that complement Process 1 with more granular analysis.
Process 2 boundaries will be defined in future files.
"""

import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
from collections import defaultdict

from database import db_manager
from alert_system import pre_start_notifier

logger = logging.getLogger(__name__)

# Constants
RULE_WEIGHTS = {'A': 4, 'B': 3, 'C': 2}
MAX_WEIGHT = 4
TIER2_TOLERANCE = 0.04
MIN_SAMPLES = 1

WINNER_NAMES = {
    '1': 'Home',
    'X': 'Draw', 
    '2': 'Away'
}

CONFIDENCE_LEVELS = {
    'identical': 'high',
    'similar': 'medium',
    'same_winning_side': 'low'
}

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
    sport: str = 'Tennis'  # Default sport, will be set from search context
    is_symmetrical: bool = True  # True for exact matches (Tier 1) and symmetrical similar matches (Tier 2)

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
        self.MIN_SAMPLES = MIN_SAMPLES
        self.TIER2_TOLERANCE = TIER2_TOLERANCE
        
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
        
        # Find candidates for both tiers, excluding current event
        current_event_id = event.id
        tier1_candidates = self._find_tier1_candidates(
            sport=event.sport,
            var_shape=var_shape,
            cur_v1=cur_v1,
            cur_vx=cur_vx,
            cur_v2=cur_v2,
            exclude_event_ids=[current_event_id]
        )
        
        # Extract Tier 1 event IDs to exclude from Tier 2 search, plus current event
        tier1_event_ids = [candidate.event_id for candidate in tier1_candidates]
        tier1_event_ids.append(current_event_id)  # Also exclude current event from Tier 2
        
        tier2_candidates = self._find_tier2_candidates(
            sport=event.sport,
            var_shape=var_shape,
            cur_v1=cur_v1,
            cur_vx=cur_vx,
            cur_v2=cur_v2,
            exclude_event_ids=tier1_event_ids
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
                               cur_v2: float, exclude_event_ids: List[int] = None) -> List[AlertMatch]:
        """Find historical events with EXACTLY identical variations"""
        return self._find_candidates(sport, var_shape, cur_v1, cur_vx, cur_v2, 
                                   is_exact=True, exclude_event_ids=exclude_event_ids)
    
    def _find_tier2_candidates(self, sport: str, var_shape: bool, 
                               cur_v1: float, cur_vx: Optional[float], 
                               cur_v2: float, exclude_event_ids: List[int] = None) -> List[AlertMatch]:
        """Find historical events with SIMILAR variations (within ±0.04 tolerance, inclusive)"""
        return self._find_candidates(sport, var_shape, cur_v1, cur_vx, cur_v2, 
                                   is_exact=False, exclude_event_ids=exclude_event_ids)
    
    def _find_candidates(self, sport: str, var_shape: bool, cur_v1: float, 
                        cur_vx: Optional[float], cur_v2: float, is_exact: bool, 
                        exclude_event_ids: List[int] = None) -> List[AlertMatch]:
        """Unified candidate search for both exact and similar variations"""
        try:
            with db_manager.get_session() as session:
                from sqlalchemy import text
                
                search_type = "EXACTLY identical" if is_exact else "SIMILAR"
                tolerance_info = "" if is_exact else f" (tolerance: ±{self.TIER2_TOLERANCE})"
                logger.info(f"Searching for {search_type} variations{tolerance_info}...")
                
                dx_display = f"{cur_vx:.2f}" if cur_vx is not None else "NULL"
                logger.info(f"Current variations: d1={cur_v1:.2f}, dx={dx_display}, d2={cur_v2:.2f}")
                
                if exclude_event_ids:
                    logger.info(f"Excluding {len(exclude_event_ids)} Tier 1 event IDs: {exclude_event_ids}")
                
                # Build SQL query and parameters
                sql_query, params = self._build_candidate_sql(
                    sport, var_shape, cur_v1, cur_vx, cur_v2, is_exact, exclude_event_ids
                )
                
                result = session.execute(text(sql_query), params)
                candidates = result.fetchall()
                
                logger.info(f"Found {len(candidates)} candidates with {search_type.upper()} variations")
                
                # Process matches
                matches = self._process_candidate_matches(candidates, cur_v1, cur_vx, cur_v2, is_exact, sport)
                
                if matches:
                    logger.info(f"SUCCESS: Found {len(matches)} {search_type.lower()} matches")
                else:
                    logger.info(f"No {search_type.lower()} matches found")
                
                return matches
                
        except Exception as e:
            error_type = "exact" if is_exact else "similar"
            logger.error(f"Error finding {error_type} historical matches: {e}")
            return []
    
    def _build_candidate_sql(self, sport: str, var_shape: bool, cur_v1: float, 
                           cur_vx: Optional[float], cur_v2: float, is_exact: bool, 
                           exclude_event_ids: List[int] = None) -> Tuple[str, Dict]:
        """Build SQL query and parameters for candidate search"""
        # Build exclusion clause
        exclude_clause = ""
        if exclude_event_ids:
            exclude_ids_str = ','.join(map(str, exclude_event_ids))
            exclude_clause = f" AND event_id NOT IN ({exclude_ids_str})"
        
        # Base parameters
        params = {
            'sport': sport,
            'var_shape': var_shape,
            'cur_v1': cur_v1,
            'cur_v2': cur_v2
        }
        
        if cur_vx is None:
            # No-draw sports (Tennis, etc.)
            if is_exact:
                var_conditions = "var_one = :cur_v1 AND var_two = :cur_v2 AND var_x IS NULL"
            else:
                params['tolerance'] = self.TIER2_TOLERANCE
                var_conditions = "ABS(var_one - :cur_v1) <= :tolerance AND ABS(var_two - :cur_v2) <= :tolerance AND var_x IS NULL"
        else:
            # 3-way sports (Football, etc.)
            params['cur_vx'] = cur_vx
            if is_exact:
                var_conditions = "var_one = :cur_v1 AND var_two = :cur_v2 AND var_x = :cur_vx"
            else:
                params['tolerance'] = self.TIER2_TOLERANCE
                var_conditions = "ABS(var_one - :cur_v1) <= :tolerance AND ABS(var_two - :cur_v2) <= :tolerance AND var_x IS NOT NULL AND ABS(var_x - :cur_vx) <= :tolerance"
        
        sql = f"""
                    SELECT event_id, participants, result_text, winner_side, point_diff,
                           var_one, var_x, var_two
                    FROM mv_alert_events
                    WHERE sport = :sport
                      AND var_shape = :var_shape
          AND {var_conditions}{exclude_clause}
        """
        
        return sql, params
    
    def _process_candidate_matches(self, candidates, cur_v1: float, 
                                 cur_vx: Optional[float], cur_v2: float, 
                                 is_exact: bool, sport: str = 'Tennis') -> List[AlertMatch]:
        """Process candidate matches into AlertMatch objects"""
        matches = []
        match_type = "EXACT" if is_exact else "SIMILAR"
        
        for row in candidates:
            dx_display = f"{row.var_x:.2f}" if row.var_x is not None else "NULL"
            
            # Check if variations are symmetrical (only for Tier 2 similar matches)
            is_symmetrical = True  # Default to True for exact matches (Tier 1)
            if not is_exact:
                is_symmetrical = self._check_symmetrical_variations(
                    cur_v1, cur_vx, cur_v2,
                    float(row.var_one), float(row.var_x) if row.var_x is not None else None, float(row.var_two)
                )
            
            # Log match details
            if is_exact:
                logger.info(
                    f"{match_type} MATCH: event_id={row.event_id} vars=(d1={row.var_one:.2f}, dx={dx_display}, d2={row.var_two:.2f}) "
                    f"| result={row.result_text}, winner={row.winner_side}, point_diff={row.point_diff}"
                )
            else:
                # Calculate differences for similar matches
                d1_diff = abs(float(row.var_one) - cur_v1)
                d2_diff = abs(float(row.var_two) - cur_v2)
                dx_diff = abs(float(row.var_x) - cur_vx) if row.var_x is not None and cur_vx is not None else 0
                
                symmetry_status = "SYMMETRICAL" if is_symmetrical else "UNSYMMETRICAL"
                logger.info(
                    f"{match_type} MATCH: event_id={row.event_id} vars=(d1={row.var_one:.2f}, dx={dx_display}, d2={row.var_two:.2f}) "
                    f"| diffs=(d1={d1_diff:.3f}, dx={dx_diff:.3f}, d2={d2_diff:.3f}) "
                    f"| {symmetry_status} | result={row.result_text}, winner={row.winner_side}, point_diff={row.point_diff}"
                )
            
            matches.append(AlertMatch(
                event_id=row.event_id,
                participants=row.participants,
                result_text=row.result_text,
                winner_side=row.winner_side,
                point_diff=row.point_diff,
                var_one=float(row.var_one),
                var_x=float(row.var_x) if row.var_x is not None else None,
                var_two=float(row.var_two),
                sport=sport,
                is_symmetrical=is_symmetrical
            ))
                
        return matches
    
    
    def _evaluate_identical_results(self, matches: List[AlertMatch]) -> Optional[AlertPrediction]:
        """Check if all matches have identical exact scores"""
        return self._evaluate_rule(matches, 'identical', lambda m: m.result_text, 'exact score')
    
    def _evaluate_similar_results(self, matches: List[AlertMatch]) -> Optional[AlertPrediction]:
        """Check if matches have same winner and point difference (Tier B rule) - requires at least 2 candidates"""
        if not matches:
            return None
            
        # Group matches by (winner_side, point_diff)
        winner_diff_groups = defaultdict(list)
        for match in matches:
            key = (match.winner_side, match.point_diff)
            winner_diff_groups[key].append(match)
        
        # Find the most common (winner_side, point_diff) pattern
        if not winner_diff_groups:
            return None
            
        most_common_pattern = max(winner_diff_groups.keys(), key=lambda k: len(winner_diff_groups[k]))
        most_common_matches = winner_diff_groups[most_common_pattern]
        
        # Tier B requires at least 2 candidates with the same (winner_side, point_diff)
        if len(most_common_matches) < 2:
            return None
        
        # Create prediction
        sample_match = most_common_matches[0]
        prediction_text = self._create_prediction_text(sample_match, sample_match.point_diff, 'similar')
        
        return AlertPrediction(
            rule_type='similar',
            prediction=prediction_text,
            winner_side=sample_match.winner_side,
            point_diff=sample_match.point_diff,
            exact_score=None,
            sample_count=len(most_common_matches),
            confidence='medium'
        )
    
    def _evaluate_same_winning_side(self, matches: List[AlertMatch]) -> Optional[AlertPrediction]:
        """Check if matches have same winning side (Tier C rule) - requires at least 2 candidates"""
        if not matches:
            return None
            
        # Group matches by winning side
        winner_groups = defaultdict(list)
        for match in matches:
            winner_groups[match.winner_side].append(match)
        
        # Find the most common winning side
        if not winner_groups:
            return None
            
        most_common_winner = max(winner_groups.keys(), key=lambda k: len(winner_groups[k]))
        most_common_matches = winner_groups[most_common_winner]
        
        # Tier C requires at least 2 candidates with the same winning side
        if len(most_common_matches) < 2:
            return None
        
        # Calculate weighted average point differential
        point_diff = self._calculate_weighted_avg_point_diff(most_common_matches)
        
        # Create prediction
        sample_match = most_common_matches[0]
        prediction_text = self._create_prediction_text(sample_match, point_diff, 'same_winning_side')
        
        return AlertPrediction(
            rule_type='same_winning_side',
            prediction=prediction_text,
            winner_side=sample_match.winner_side,
            point_diff=point_diff,
            exact_score=None,
            sample_count=len(most_common_matches),
            confidence='low'
        )
    
    def _evaluate_rule(self, matches: List[AlertMatch], rule_type: str, group_key_func, 
                      group_desc: str, use_weighted_avg: bool = False) -> Optional[AlertPrediction]:
        """Unified rule evaluation logic"""
        if not matches:
            return None
            
        # Group matches by the specified key function
        groups = defaultdict(list)
        for match in matches:
            key = group_key_func(match)
            groups[key].append(match)
        
        # Log group composition
        group_summary = ", ".join([f"{k}:{len(v)}" for k, v in groups.items()])
        logger.info(f"{rule_type.title()} rule grouping by {group_desc} -> {group_summary if group_summary else 'no groups'}")
        
        # Check for unanimity (all matches in same group)
        if len(groups) == 1:
            group_matches = list(groups.values())[0]
            sample_match = group_matches[0]
            
            # Calculate point differential
            if use_weighted_avg:
                point_diff = self._calculate_weighted_avg_point_diff(group_matches)
            else:
                point_diff = sample_match.point_diff
            
            # Create prediction
            prediction_text = self._create_prediction_text(sample_match, point_diff, rule_type)
            
            return AlertPrediction(
                rule_type=rule_type,
                prediction=prediction_text,
                winner_side=sample_match.winner_side,
                point_diff=point_diff,
                exact_score=sample_match.result_text if rule_type == 'identical' else None,
                sample_count=len(matches),
                confidence=CONFIDENCE_LEVELS[rule_type]
            )
        
        return None
    
    def _create_mixed_prediction(self, candidates: List[AlertMatch], rule_type: str, match_count: int) -> AlertPrediction:
        """Create prediction when only some candidates match a rule"""
        if rule_type == 'identical':
            # Find the most common result among candidates
            result_groups = {}
            for match in candidates:
                result = match.result_text
            if result not in result_groups:
                result_groups[result] = []
            result_groups[result].append(match)
        
            # Find the result with the most candidates
            most_common_result = max(result_groups.keys(), key=lambda k: len(result_groups[k]))
            most_common_matches = result_groups[most_common_result]
            
            # Calculate weighted average point differential using rule weights
            weighted_avg_point_diff = self._calculate_weighted_avg_point_diff_mixed(candidates)
            
            # Create prediction based on the most common result
            sample_match = most_common_matches[0]
            winner_name = WINNER_NAMES.get(sample_match.winner_side, 'Unknown')
                
            if sample_match.winner_side == 'X':
                prediction_text = "Draw"
            else:
                prediction_text = f"{winner_name} wins by point differential of: {weighted_avg_point_diff:.2f}"
            
            return AlertPrediction(
                rule_type='identical',
                prediction=prediction_text,
                winner_side=sample_match.winner_side,
                point_diff=weighted_avg_point_diff,
                exact_score=most_common_result,
                sample_count=match_count,
                confidence='high'
            )
        
        # For other rule types, use the existing evaluation methods
        if rule_type == 'similar':
            return self._evaluate_similar_results(candidates)
        elif rule_type == 'same_winner':
            return self._evaluate_same_winning_side(candidates)
        
        return None
    
    def _count_candidates_matching_rule(self, candidates: List[AlertMatch], rule_type: str) -> int:
        """Count how many candidates match a specific rule with simplified logic"""
        if not candidates:
            return 0
        
        if rule_type == 'identical':
            # Tier A: Count candidates with identical exact results (requires at least 2)
            result_groups = defaultdict(list)
            for match in candidates:
                result_groups[match.result_text].append(match)
            
            # Count only groups with at least 2 members (identical results)
            return sum(len(group) for group in result_groups.values() if len(group) >= 2)
        
        elif rule_type == 'similar':
            # Tier B: Count candidates that have the same (winner_side, point_diff)
            # Requires at least 2 candidates with the SAME (winner_side, point_diff)
            winner_diff_groups = defaultdict(list)
            for match in candidates:
                key = (match.winner_side, match.point_diff)
                winner_diff_groups[key].append(match)
            
            # Find the group with the most candidates that have the same (winner_side, point_diff)
            if not winner_diff_groups:
                return 0
            
            largest_group_size = max(len(group) for group in winner_diff_groups.values())
            return largest_group_size if largest_group_size >= 2 else 0
        
        elif rule_type == 'same_winner':
            # Tier C: Count candidates with same winning side (requires at least 2)
            winner_groups = defaultdict(list)
            for match in candidates:
                winner_groups[match.winner_side].append(match)
            
            # Find the most common winning side
            if not winner_groups:
                return 0
            
            most_common_winner = max(winner_groups.keys(), key=lambda k: len(winner_groups[k]))
            most_common_count = len(winner_groups[most_common_winner])
            return most_common_count if most_common_count >= 2 else 0
        
        return 0
    
    
    def _get_candidates_by_rule_tiers(self, candidates: List[AlertMatch]) -> Dict[str, List[AlertMatch]]:
        """Get candidates grouped by their rule tier (A, B, C) with PRIORITY-BASED exclusions"""
        tier_candidates = {'A': [], 'B': [], 'C': []}
        
        # Track which candidates have been assigned to higher priority tiers
        assigned_candidates = set()
        
        # Tier A: Identical exact results (highest priority)
        tier_a_candidates = self._get_tier_a_candidates(candidates)
        tier_candidates['A'] = tier_a_candidates
        assigned_candidates.update(match.event_id for match in tier_a_candidates)
        
        # Tier B: Same winner and point difference (EXCLUDING Tier A candidates)
        remaining_after_a = [match for match in candidates if match.event_id not in assigned_candidates]
        tier_b_candidates = self._get_tier_b_candidates(candidates, remaining_after_a)
        tier_candidates['B'] = tier_b_candidates
        assigned_candidates.update(match.event_id for match in tier_b_candidates)
        
        # Tier C: Same winning side (EXCLUDING Tier A and B candidates)
        remaining_after_b = [match for match in candidates if match.event_id not in assigned_candidates]
        tier_c_candidates = self._get_tier_c_candidates(candidates, remaining_after_b)
        tier_candidates['C'] = tier_c_candidates
        
        return tier_candidates
    
    def _get_tier_a_candidates(self, candidates: List[AlertMatch]) -> List[AlertMatch]:
        """Get Tier A candidates: Identical exact results (highest priority, requires at least 2 candidates)"""
        tier_a_candidates = []
        
        result_groups = defaultdict(list)
        for match in candidates:
            result_groups[match.result_text].append(match)
        
        for group in result_groups.values():
            if len(group) >= 2:  # Require at least 2 candidates with identical results
                tier_a_candidates.extend(group)
        
        return tier_a_candidates
    
    def _get_tier_b_candidates(self, all_candidates: List[AlertMatch], remaining_candidates: List[AlertMatch]) -> List[AlertMatch]:
        """Get Tier B candidates: Same winner+point_diff as majority pattern (requires at least 2 candidates)"""
        if not remaining_candidates:
            return []
        
        # Find the most common (winner_side, point_diff) from REMAINING candidates only
        remaining_winner_diff_groups = defaultdict(list)
        for match in remaining_candidates:
            key = (match.winner_side, match.point_diff)
            remaining_winner_diff_groups[key].append(match)
        
        if not remaining_winner_diff_groups:
            return []
        
        # Find the most common winner+point_diff pattern among remaining candidates
        most_common_winner_diff = max(remaining_winner_diff_groups.keys(), 
                                    key=lambda k: len(remaining_winner_diff_groups[k]))
        
        # Tier B requires at least 2 candidates with the same (winner_side, point_diff)
        if len(remaining_winner_diff_groups[most_common_winner_diff]) < 2:
            return []
        
        # Assign remaining candidates to Tier B if they match the most common pattern
        tier_b_candidates = []
        for match in remaining_candidates:
            if (match.winner_side, match.point_diff) == most_common_winner_diff:
                tier_b_candidates.append(match)
        
        return tier_b_candidates
    
    def _get_tier_c_candidates(self, all_candidates: List[AlertMatch], remaining_candidates: List[AlertMatch]) -> List[AlertMatch]:
        """Get Tier C candidates: Same winning side as majority from ALL original candidates (requires at least 2 candidates)"""
        if not remaining_candidates:
            return []
        
        # Find the most common winning side from ALL original candidates
        all_winner_groups = defaultdict(list)
        for match in all_candidates:
            all_winner_groups[match.winner_side].append(match)
        
        if not all_winner_groups:
            return []
        
        # Find the most common winning side from ALL original candidates
        most_common_winner = max(all_winner_groups.keys(), 
                               key=lambda k: len(all_winner_groups[k]))
        
        # Tier C requires at least 2 candidates with the same winning side
        if len(all_winner_groups[most_common_winner]) < 2:
            return []
        
        # Only assign remaining candidates if they match the majority winner from ALL candidates
        tier_c_candidates = []
        for match in remaining_candidates:
            if match.winner_side == most_common_winner:
                tier_c_candidates.append(match)
        
        return tier_c_candidates
    
    def _get_rule_activations(self, candidates: List[AlertMatch]) -> Dict[str, Dict]:
        """Get detailed rule activation information for reporting"""
        rule_activations = {}
        
        # Get candidates by rule tiers
        tier_candidates = self._get_candidates_by_rule_tiers(candidates)
        
        # Process each rule tier
        for tier, matches in tier_candidates.items():
            if matches:
                rule_activations[tier] = {
                    'count': len(matches),
                    'weight': RULE_WEIGHTS[tier],
                    'candidates': [
                        {
                            'event_id': match.event_id,
                            'participants': match.participants,
                            'result_text': match.result_text,
                            'winner_side': match.winner_side,
                            'point_diff': match.point_diff
                        }
                        for match in matches
                    ]
                }
        
        return rule_activations
    
    
    def _calculate_weighted_avg_point_diff_mixed(self, candidates: List[AlertMatch]) -> float:
        """Calculate weighted average point differential for mixed rule matches using rule weights"""
        if not candidates:
            return 0
        
        # Get candidates by rule tier using existing logic
        tier_candidates = self._get_candidates_by_rule_tiers(candidates)
        
        # Calculate weighted average
        total_weighted_diff = sum(
            match.point_diff * RULE_WEIGHTS[tier]
            for tier, matches in tier_candidates.items()
            for match in matches
        )
        total_weight = sum(
            len(matches) * RULE_WEIGHTS[tier]
            for tier, matches in tier_candidates.items()
        )
        
        return total_weighted_diff / total_weight if total_weight > 0 else 0
    
    def _calculate_weighted_avg_point_diff(self, matches: List[AlertMatch]) -> float:
        """Calculate weighted average point differential for Tier C rule"""
        total_weighted_diff = 0
        total_weight = 0
        weight = RULE_WEIGHTS['C']  # Use Tier C weight
        
        for match in matches:
            total_weighted_diff += match.point_diff * weight
            total_weight += weight
        
        return total_weighted_diff / total_weight if total_weight > 0 else 0
    
    def _calculate_weighted_avg_point_diff(self, matches: List[AlertMatch]) -> float:
        """Calculate weighted average point differential for Tier C rule"""
        total_weighted_diff = 0
        total_weight = 0
        weight = RULE_WEIGHTS['C']  # Use Tier C weight
        
        for match in matches:
            total_weighted_diff += match.point_diff * weight
            total_weight += weight
        
        return total_weighted_diff / total_weight if total_weight > 0 else 0
    
    def _create_prediction_text(self, match: AlertMatch, point_diff, rule_type: str) -> str:
        """Create prediction text based on rule type and match data"""
        winner_name = WINNER_NAMES.get(match.winner_side, 'Unknown')
        
        if match.winner_side == 'X':
            return "Draw"
        
        if rule_type == 'identical':
            if match.point_diff and match.point_diff > 0:
                return f"{winner_name} wins by point differential of: {match.point_diff}"
            else:
                return f"Exact score: {match.result_text}"
        else:
            # Similar or same winning side rules
            if rule_type == 'same_winning_side':
                return f"{winner_name} wins by point differential of: {point_diff:.2f}"
            else:
                return f"{winner_name} wins by point differential of: {point_diff}"
    
    def _check_symmetrical_variations(self, cur_v1: float, cur_vx: Optional[float], cur_v2: float,
                                    cand_v1: float, cand_vx: Optional[float], cand_v2: float) -> bool:
        """
        Check if candidate variations are symmetrical to current variations.
        Symmetrical means all variations move in the same direction by the same amount.
        
        Example: Current (0.37, -0.30, -1.13) vs Candidate (0.35, -0.32, -1.15)
        All variations moved by -0.02, so they are symmetrical.
        
        Args:
            cur_v1, cur_vx, cur_v2: Current event variations
            cand_v1, cand_vx, cand_v2: Candidate event variations
            
        Returns:
            True if variations are symmetrical, False otherwise
        """
        # Calculate differences for each variation
        d1_diff = cand_v1 - cur_v1
        d2_diff = cand_v2 - cur_v2
        dx_diff = (cand_vx - cur_vx) if cand_vx is not None and cur_vx is not None else 0
        
        # For 2-way sports (no draw), only check d1 and d2
        if cur_vx is None and cand_vx is None:
            # Check if both variations move in the same direction by the same amount
            return abs(d1_diff - d2_diff) < 0.001  # Allow for tiny floating point differences
        
        # For 3-way sports (with draw), check all three variations
        else:
            # Check if all three variations move in the same direction by the same amount
            return (abs(d1_diff - d2_diff) < 0.001 and 
                    abs(d1_diff - dx_diff) < 0.001 and 
                    abs(d2_diff - dx_diff) < 0.001)
    
    def _create_prediction_text(self, match: AlertMatch, point_diff, rule_type: str) -> str:
        """Create prediction text based on rule type and match data"""
        winner_name = WINNER_NAMES.get(match.winner_side, 'Unknown')
        
        if match.winner_side == 'X':
            return "Draw"
        
        if rule_type == 'identical':
            if match.point_diff and match.point_diff > 0:
                return f"{winner_name} wins by point differential of: {match.point_diff}"
            else:
                return f"Exact score: {match.result_text}"
        else:
            # Similar or same winning side rules
            if rule_type == 'same_winning_side':
                return f"{winner_name} wins by point differential of: {point_diff:.2f}"
            else:
                return f"{winner_name} wins by point differential of: {point_diff}"
    
    def _evaluate_candidates_with_new_logic(self, tier1_candidates: List[AlertMatch], 
                                           tier2_candidates: List[AlertMatch]) -> Dict:
        """
        Evaluate candidates using new tier selection and weighted logic
        
        Returns:
            Dict with evaluation results including tier used, rule matched, prediction, and confidence
        """
        # Tier selection logic: Use Tier 1 if available, otherwise Tier 2
        if tier1_candidates:
            selected_tier = "Tier 1 (exact variations)"
            selected_candidates = tier1_candidates
            logger.info(f"🎯 Using {selected_tier} for evaluation ({len(selected_candidates)} candidates)")
        elif tier2_candidates:
            selected_tier = "Tier 2 (similar variations)"
            selected_candidates = tier2_candidates
            logger.info(f"🎯 Using {selected_tier} for evaluation ({len(selected_candidates)} candidates)")
        else:
            return {
                'status': 'no_candidates',
                'selected_tier': None,
                'rule_matched': None,
                'prediction': None,
                'confidence': 0,
                'successful_candidates': 0,
                'total_candidates': 0
            }
        
        # Filter out non-symmetrical candidates for Tier 2 (exact matches are always symmetrical)
        if selected_tier == "Tier 2 (similar variations)":
            symmetrical_candidates = [c for c in selected_candidates if c.is_symmetrical]
            non_symmetrical_candidates = [c for c in selected_candidates if not c.is_symmetrical]
            
            if non_symmetrical_candidates:
                logger.info(f"🔍 Filtering out {len(non_symmetrical_candidates)} non-symmetrical candidates from success calculations")
                for candidate in non_symmetrical_candidates:
                    dx_display = f"{candidate.var_x:.2f}" if candidate.var_x is not None else "N/A"
                    logger.info(f"   ❌ Non-symmetrical: {candidate.participants} (vars: Δ1={candidate.var_one:.2f}, ΔX={dx_display}, Δ2={candidate.var_two:.2f})")
            
            # Use only symmetrical candidates for rule evaluation and success calculations
            selected_candidates = symmetrical_candidates
            logger.info(f"🎯 Using {len(selected_candidates)} symmetrical candidates for rule evaluation")
        
        # Evaluate rules in priority order: A (identical) > B (similar) > C (same winning side)
        rule_a_result = self._evaluate_identical_results(selected_candidates)
        rule_b_result = self._evaluate_similar_results(selected_candidates)
        rule_c_result = self._evaluate_same_winning_side(selected_candidates)
        
        # Count candidates that match each rule, with priority (A > B > C)
        tier_a_matches = self._count_candidates_matching_rule(selected_candidates, 'identical')
        tier_b_matches = self._count_candidates_matching_rule(selected_candidates, 'similar')
        tier_c_matches = self._count_candidates_matching_rule(selected_candidates, 'same_winner')
        
        # Calculate total UNIQUE candidates that match at least one rule
        # We need to count unique candidates, not sum up all rule matches
        unique_matching_candidates = set()
        
        # Add candidates that match Tier A (identical results)
        if tier_a_matches > 0:
            result_groups = defaultdict(list)
            for match in selected_candidates:
                result_groups[match.result_text].append(match)
            for group in result_groups.values():
                if len(group) >= 2:  # Require at least 2 candidates with identical results
                    unique_matching_candidates.update(match.event_id for match in group)
        
        # Add candidates that match Tier B (same winner+point_diff)
        if tier_b_matches > 0:
            winner_diff_groups = defaultdict(list)
            for match in selected_candidates:
                key = (match.winner_side, match.point_diff)
                winner_diff_groups[key].append(match)
            # Find the largest group with same (winner_side, point_diff)
            if winner_diff_groups:
                largest_group = max(winner_diff_groups.values(), key=len)
                if len(largest_group) >= 2:
                    unique_matching_candidates.update(match.event_id for match in largest_group)
        
        # Add candidates that match Tier C (same winner)
        if tier_c_matches > 0:
            winner_groups = defaultdict(list)
            for match in selected_candidates:
                winner_groups[match.winner_side].append(match)
            if winner_groups:
                most_common_winner = max(winner_groups.keys(), key=lambda k: len(winner_groups[k]))
                unique_matching_candidates.update(match.event_id for match in winner_groups[most_common_winner])
        
        total_matching_candidates = len(unique_matching_candidates)
        
        # Initialize prediction_result to avoid UnboundLocalError
        prediction_result = None
        
        # Determine status: SUCCESS only if ALL candidates match at least one rule AND a prediction is generated
        # Special case: If only 1 candidate after filtering, it's PARTIAL (insufficient for any tier)
        if len(selected_candidates) == 1:
            # Single candidate case - insufficient for any tier activation
            status = 'partial'
            confidence = 0
            prediction_result = None
            successful_candidates = 1
            total_candidates = 1
        elif total_matching_candidates == len(selected_candidates) and len(selected_candidates) > 0:
            # Calculate weighted confidence based on PRIORITY-BASED tier assignments
            # Get the actual tier assignments (with exclusions)
            tier_assignments = self._get_candidates_by_rule_tiers(selected_candidates)
            
            # Calculate confidence using each candidate's HIGHEST priority tier only
            weighted_successes = (len(tier_assignments['A']) * RULE_WEIGHTS['A'] + 
                                len(tier_assignments['B']) * RULE_WEIGHTS['B'] + 
                                len(tier_assignments['C']) * RULE_WEIGHTS['C'])
            max_possible_weight = len(selected_candidates) * MAX_WEIGHT
            confidence = (weighted_successes / max_possible_weight) * 100 if max_possible_weight > 0 else 0
            
            # Use the highest priority rule that has the most matches for prediction
            if tier_a_matches > 0:
                # Create prediction based on Tier A matches even if not all candidates match
                prediction_result = self._create_mixed_prediction(selected_candidates, 'identical', tier_a_matches)
                logger.info(f"✅ Rule A matched: {tier_a_matches}/{len(selected_candidates)} candidates have identical results")
            elif tier_b_matches > 0:
                prediction_result = rule_b_result
                logger.info(f"✅ Rule B matched: {tier_b_matches}/{len(selected_candidates)} candidates have similar results")
            elif tier_c_matches > 0:
                prediction_result = rule_c_result
                logger.info(f"✅ Rule C matched: {tier_c_matches}/{len(selected_candidates)} candidates have same winning side")
            
            successful_candidates = len(selected_candidates)
            total_candidates = len(selected_candidates)
            
            # Check if a prediction was actually generated
            if prediction_result is not None:
                status = 'success'
            else:
                # All candidates processed but no prediction generated (insufficient candidates for any tier)
                status = 'partial'
                confidence = 0
        else:
            # Some candidates failed to match any rule OR no candidates left after filtering
            successful_candidates = total_matching_candidates
            total_candidates = len(selected_candidates)
            confidence = 0
            
            # Check if this is a PARTIAL case (insufficient candidates for any tier) vs NO MATCH (candidates failed rules
            if total_matching_candidates == len(selected_candidates) and len(selected_candidates) > 0:
                # All candidates processed but no prediction generated (insufficient candidates for any tier)
                status = 'partial'
            else:
                # Some candidates failed to match any rule
                status = 'no_match'
            
            prediction_result = None
        
        # Get rule activation details for reporting
        rule_activations = self._get_rule_activations(selected_candidates)
        
        # Calculate non-symmetrical candidates count for Tier 2
        non_symmetrical_count = 0
        if selected_tier == "Tier 2 (similar variations)":
            non_symmetrical_count = len([c for c in tier2_candidates if not c.is_symmetrical])
        
        return {
            'status': status,
            'selected_tier': selected_tier,
            'prediction': prediction_result,
            'confidence': confidence,
            'successful_candidates': successful_candidates,
            'total_candidates': total_candidates,
            'non_symmetrical_candidates': non_symmetrical_count,
            'rule_activations': rule_activations,
            'tier1_candidates': tier1_candidates,
            'tier2_candidates': tier2_candidates
        }
    
    def _create_candidate_report(self, event, tier1_candidates: List[AlertMatch], 
                                tier2_candidates: List[AlertMatch], 
                                current_vars: Tuple, minutes_until_start: int = None) -> Dict:
        """Create comprehensive candidate report using new tier selection and weighted logic"""
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
        
        # Use new evaluation logic
        evaluation_result = self._evaluate_candidates_with_new_logic(tier1_candidates, tier2_candidates)
        
        # Format candidate data for display
        tier1_matches_data = self._format_candidate_data(tier1_candidates)
        tier2_matches_data = self._format_candidate_data(tier2_candidates)
        
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
            'has_draw_odds': cur_vx is not None,
            'status': evaluation_result['status'],
            'selected_tier': evaluation_result['selected_tier'],
            'primary_prediction': evaluation_result['prediction'].prediction if evaluation_result['prediction'] else None,
            'primary_confidence': f"{evaluation_result['confidence']:.1f}%",
            'successful_candidates': evaluation_result['successful_candidates'],
            'total_candidates': evaluation_result['total_candidates'],
            'rule_activations': evaluation_result['rule_activations'],
            'tier1_candidates': {
                'count': len(tier1_candidates),
                'matches': tier1_matches_data
            },
            'tier2_candidates': {
                'count': len(tier2_candidates),
                'matches': tier2_matches_data
            }
        }
    
    def _format_candidate_data(self, candidates: List[AlertMatch]) -> List[Dict]:
        """Format candidate matches for display"""
        return [
            {
                'event_id': match.event_id,
                'sport': match.sport,
                'participants': match.participants,
                'result_text': match.result_text,
                'is_symmetrical': match.is_symmetrical,
                'variations': {
                    'var_one': match.var_one,
                    'var_x': match.var_x,
                    'var_two': match.var_two
                }
            }
            for match in candidates
        ]
    
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

# PROCESS 1 END BOUNDARY
# ======================
# Process 1 implementation ends here.
# Process 2 will be implemented in separate files with clear boundaries.
