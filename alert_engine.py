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
TIER2_TOLERANCE = 0.040001  # Slightly higher to handle floating point precision (legacy component-based)
L1_TAU_DEFAULT = 0.04  # Default L1 distance threshold for similarity search
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
    gender: str
    result_text: str
    winner_side: str
    point_diff: int
    one_open: float
    x_open: float
    two_open: float
    one_final: float
    x_final: float
    two_final: float
    var_one: float
    var_x: Optional[float]
    var_two: float
    sport: str = 'Tennis'  # Default sport, will be set from search context
    is_symmetrical: bool = True  # True for exact matches (Tier 1) and symmetrical similar matches (Tier 2)
    competition: str = 'Unknown'  # Competition/tournament name
    # Variation differences from current event (for display purposes)
    var_diffs: Optional[Dict[str, float]] = None  # {'d1': 0.02, 'dx': 0.01, 'd2': 0.02}
    # L1 distance from current event (for L1-based similarity search)
    distance_l1: Optional[float] = None  # Sum of absolute differences: |Δvar_one| + |Δvar_x| + |Δvar_two|
    # Court type for Tennis events (for filtering by playing surface)
    court_type: Optional[str] = None  # "Hardcourt indoor", "Red clay", "Grass", etc.

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
            event: Event object to evaluate (must have court_type attribute for Tennis filtering)
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
            gender=event.gender,
            var_shape=var_shape,
            current_odds=event.event_odds,
            exclude_event_ids=[current_event_id]
        )
        
        # Extract Tier 1 event IDs to exclude from Tier 2 search, plus current event
        tier1_event_ids = [candidate.event_id for candidate in tier1_candidates]
        tier1_event_ids.append(current_event_id)  # Also exclude current event from Tier 2
        
        # Use L1 distance-based similarity search with initial odds filtering
        tier2_candidates = self._find_l1_similar_candidates(
            sport=event.sport,
            gender=event.gender,
            var_shape=var_shape,
            cur_v1=cur_v1,
            cur_vx=cur_vx,
            cur_v2=cur_v2,
            exclude_event_ids=tier1_event_ids,
            tau=L1_TAU_DEFAULT,
            current_odds=event.event_odds
        )
        
        # Log candidate findings
        logger.info(f"Found {len(tier1_candidates)} Tier 1 (exact) candidates for event {event.id}")
        logger.info(f"Found {len(tier2_candidates)} Tier 2 (L1-similar, τ={L1_TAU_DEFAULT}) candidates for event {event.id}")
        
        # COURT TYPE FILTERING: Filter candidates by court type for Tennis/Tennis Doubles
        current_court_type = getattr(event, 'court_type', None)
        if current_court_type and event.sport in ['Tennis', 'Tennis Doubles']:
            logger.info(f"🎾 Applying court type filter for {event.sport}: '{current_court_type}'")
            tier1_candidates = self._filter_candidates_by_court_type(tier1_candidates, current_court_type, event.sport)
            tier2_candidates = self._filter_candidates_by_court_type(tier2_candidates, current_court_type, event.sport)
            logger.info(f"After court type filter: {len(tier1_candidates)} Tier 1, {len(tier2_candidates)} Tier 2 candidates")
        
        # Create comprehensive candidate report
        if tier1_candidates or tier2_candidates:
            # Pre-evaluate to check if we have valid candidates after filtering
            evaluation_result = self._evaluate_candidates_with_new_logic(tier1_candidates, tier2_candidates)
            
            # Create report for ALL statuses (including no_candidates)
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
    
    def _get_event_sport(self, event_id: int) -> Optional[str]:
        """Get sport for an event from mv_alert_events"""
        try:
            with db_manager.get_session() as session:
                from sqlalchemy import text
                result = session.execute(
                    text("SELECT sport FROM mv_alert_events WHERE event_id = :event_id LIMIT 1"),
                    {'event_id': event_id}
                )
                row = result.fetchone()
                return row.sport if row else None
                
        except Exception as e:
            logger.error(f"Error getting sport for event {event_id}: {e}")
            return None
    
    def _process_l1_candidates(self, candidates, cur_v1: float, cur_vx: Optional[float], 
                              cur_v2: float, tau: float, sport: str, 
                              max_candidates: int) -> List[AlertMatch]:
        """Process candidates that already passed L1 distance filtering in SQL"""
        matches = []
        logger.info(f"🔍 DEBUG: Processing {len(candidates)} candidates (already L1-filtered by SQL)")
        
        for row in candidates:
            # Calculate L1 distance for display and sorting
            cand_v1 = float(row.var_one)
            cand_vx = float(row.var_x) if row.var_x is not None else None
            cand_v2 = float(row.var_two)
            
            # Calculate component differences
            dx_1 = abs(cand_v1 - cur_v1)
            dx_x = abs((0.0 if cand_vx is None else cand_vx) - (0.0 if cur_vx is None else cur_vx))
            dx_2 = abs(cand_v2 - cur_v2)
            
            # Calculate L1 distance
            dist_l1 = dx_1 + dx_x + dx_2
            dist_l1 = round(dist_l1, 6)  # Round to avoid floating point precision issues
            
            # Calculate signed differences for display
            d1_diff_signed = cand_v1 - cur_v1
            d2_diff_signed = cand_v2 - cur_v2
            dx_diff_signed = (cand_vx if cand_vx is not None else 0.0) - (cur_vx if cur_vx is not None else 0.0)
            
            var_diffs = {
                'd1': round(d1_diff_signed, 3),
                'd2': round(d2_diff_signed, 3),
                'dx': round(dx_diff_signed, 3) if cur_vx is not None or cand_vx is not None else None
            }
            
            # Check symmetry for Tennis sports
            is_symmetrical = True  # Default for non-Tennis sports
            if sport.lower() == 'tennis':
                is_symmetrical = self._check_symmetrical_variations(
                    cur_v1, cur_vx, cur_v2,
                    cand_v1, cand_vx, cand_v2
                )
            
            # Log match details
            dx_display = f"{cand_vx:.2f}" if cand_vx is not None else "NULL"
            dx_diff_display = f"{var_diffs['dx']:.3f}" if var_diffs['dx'] is not None else "0.000"
            symmetry_status = "SYMMETRICAL" if is_symmetrical else "UNSYMMETRICAL"
            """ for debugging purposes, take quotes off when need to see the match details
            logger.info(
                f"L1 MATCH: event_id={row.event_id} vars=(d1={cand_v1:.2f}, dx={dx_display}, d2={cand_v2:.2f}) "
                f"| diffs=(d1={var_diffs['d1']:+.3f}, dx={dx_diff_display}, d2={var_diffs['d2']:+.3f}) "
                f"| L1={dist_l1:.4f} | {symmetry_status} | result={row.result_text}, winner={row.winner_side}, point_diff={row.point_diff}"
            )
            """
            
            matches.append(AlertMatch(
                event_id=row.event_id,
                participants=row.participants,
                gender=getattr(row, 'gender', 'unknown'),  # Get gender from query result
                result_text=row.result_text,
                winner_side=row.winner_side,
                point_diff=row.point_diff,
                one_open=float(row.one_open) if row.one_open is not None else 0.0,
                x_open=float(row.x_open) if row.x_open is not None else 0.0,
                two_open=float(row.two_open) if row.two_open is not None else 0.0,
                one_final=float(row.one_final) if row.one_final is not None else 0.0,
                x_final=float(row.x_final) if row.x_final is not None else 0.0,
                two_final=float(row.two_final) if row.two_final is not None else 0.0,
                var_one=cand_v1,
                var_x=cand_vx,
                var_two=cand_v2,
                sport=sport,
                is_symmetrical=is_symmetrical,  # Apply sport-specific symmetry logic
                competition=row.competition or 'Unknown',
                var_diffs=var_diffs,
                distance_l1=dist_l1,
                court_type=getattr(row, 'court_type', None)  # Get court_type from query result
            ))
        
        # Sort by L1 distance (ascending) and then by component differences for stability
        matches.sort(key=lambda m: (m.distance_l1, abs(m.var_diffs['d1']), abs(m.var_diffs['d2'])))
        
        # Limit results
        if len(matches) > max_candidates:
            logger.info(f"Limiting L1 results to top {max_candidates} closest matches")
            matches = matches[:max_candidates]
        
        #logger.info(f"🔍 DEBUG: Final L1 matches returned: {len(matches)}")
        return matches
    
    def _find_tier1_candidates(self, sport: str, gender: str, var_shape: bool, 
                               current_odds, exclude_event_ids: List[int] = None) -> List[AlertMatch]:
        """Find historical events with EXACTLY identical odds (initial and final)"""
        return self._find_candidates(sport, gender, var_shape, current_odds, 
                                   is_exact=True, exclude_event_ids=exclude_event_ids)
    
    
    def _find_l1_similar_candidates(self, sport: str, gender: str, var_shape: bool, 
                                   cur_v1: float, cur_vx: Optional[float], 
                                   cur_v2: float, exclude_event_ids: List[int] = None,
                                   tau: float = L1_TAU_DEFAULT,
                                   current_odds: Optional[object] = None) -> List[AlertMatch]:
        """Find historical events with SIMILAR variations using L1 distance threshold"""
        try:
            with db_manager.get_session() as session:
                from sqlalchemy import text
                
                logger.info(f"Searching for L1 similar variations (τ={tau})...")
                dx_display = f"{cur_vx:.2f}" if cur_vx is not None else "NULL"
                logger.info(f"Current variations: d1={cur_v1:.2f}, dx={dx_display}, d2={cur_v2:.2f}")
                logger.info(f"Filtering by sport='{sport}' and gender='{gender}'")
                
                # Log initial odds filtering if available
                if current_odds:
                    logger.info(f"Initial odds filtering: 1={current_odds.one_open} (±0.50), 2={current_odds.two_open} (±0.50)")
                    if current_odds.x_open is not None:
                        logger.info(f"Initial odds filtering: X={current_odds.x_open} (±0.50)")
                    logger.info(f"Total odds tolerance: 1.50 (0.50 per odds)")
                
                if exclude_event_ids:
                    logger.info(f"Excluding {len(exclude_event_ids)} event IDs: {exclude_event_ids}")
                
                # Build SQL query for L1 prefilter with initial odds filtering
                sql_query, params = self._build_l1_prefilter_sql(
                    sport=sport,
                    gender=gender,
                    var_shape=var_shape,
                    cur_v1=cur_v1,
                    cur_vx=cur_vx,
                    cur_v2=cur_v2,
                    tau=tau,
                    by_shape=True,  # Use same var_shape for consistency with existing logic
                    exclude_event_ids=exclude_event_ids,
                    max_candidates=500,
                    current_odds=current_odds
                )
                
                result = session.execute(text(sql_query), params)
                candidates = result.fetchall()
                
                logger.info(f"Found {len(candidates)} L1-similar candidates")
                
                # Process matches with L1 distance calculation
                matches = self._process_l1_candidates(
                    candidates=candidates,
                    cur_v1=cur_v1,
                    cur_vx=cur_vx,
                    cur_v2=cur_v2,
                    tau=tau,
                    sport=sport,
                    max_candidates=500
                )
                
                if matches:
                    logger.info(f"SUCCESS: Found {len(matches)} L1-similar matches")
                else:
                    logger.info(f"No L1-similar matches found")
                
                return matches
                
        except Exception as e:
            logger.error(f"Error finding L1-similar historical matches: {e}")
            return []

    
    def _find_candidates(self, sport: str, gender: str, var_shape: bool, 
                        search_data, is_exact: bool, exclude_event_ids: List[int] = None) -> List[AlertMatch]:
        """Unified candidate search for both exact odds and similar variations"""
        try:
            with db_manager.get_session() as session:
                from sqlalchemy import text
                
                if is_exact:
                    # Tier 1: Search for exact odds
                    search_type = "EXACTLY identical odds"
                    current_odds = search_data
                    logger.info(f"Searching for {search_type}...")
                    logger.info(f"Current odds: 1={current_odds.one_open}→{current_odds.one_final}, "
                               f"X={current_odds.x_open}→{current_odds.x_final if current_odds.x_open else 'N/A'}, "
                               f"2={current_odds.two_open}→{current_odds.two_final}")
                    logger.info(f"Filtering by sport='{sport}' and gender='{gender}'")
                    
                    # Build SQL query for exact odds
                    sql_query, params = self._build_candidate_sql(
                        sport, gender, var_shape, current_odds, is_exact, exclude_event_ids
                    )
                else:
                    # Tier 2: Search for similar variations (unchanged)
                    search_type = "SIMILAR variations"
                    tolerance_info = f" (tolerance: ±{self.TIER2_TOLERANCE})"
                    cur_v1, cur_vx, cur_v2 = search_data
                    logger.info(f"Searching for {search_type}{tolerance_info}...")
                    
                    dx_display = f"{cur_vx:.2f}" if cur_vx is not None else "NULL"
                    logger.info(f"Current variations: d1={cur_v1:.2f}, dx={dx_display}, d2={cur_v2:.2f}")
                    logger.info(f"Filtering by sport='{sport}' and gender='{gender}'")
                    
                    # Build SQL query for similar variations
                    sql_query, params = self._build_candidate_sql(
                        sport, gender, var_shape, cur_v1, cur_vx, cur_v2, is_exact, exclude_event_ids
                    )
                
                if exclude_event_ids:
                    logger.info(f"Excluding {len(exclude_event_ids)} event IDs: {exclude_event_ids}")
                
                result = session.execute(text(sql_query), params)
                candidates = result.fetchall()
                
                logger.info(f"Found {len(candidates)} candidates with {search_type.upper()}")
                
                # Process matches
                if is_exact:
                    # For exact odds, we don't need variation parameters for processing
                    matches = self._process_candidate_matches(candidates, 0, None, 0, is_exact, sport)
                else:
                    # For similar variations, use the variation parameters
                    cur_v1, cur_vx, cur_v2 = search_data
                    matches = self._process_candidate_matches(candidates, cur_v1, cur_vx, cur_v2, is_exact, sport)
                
                if matches:
                    logger.info(f"SUCCESS: Found {len(matches)} {search_type.lower()} matches")
                else:
                    logger.info(f"No {search_type.lower()} matches found")
                
                return matches
                
        except Exception as e:
            error_type = "exact odds" if is_exact else "similar variations"
            logger.error(f"Error finding {error_type} historical matches: {e}")
            return []
    
    def _build_l1_prefilter_sql(self, sport: str, gender: str, var_shape: bool, cur_v1: float,
                              cur_vx: Optional[float], cur_v2: float, tau: float,
                              by_shape: bool = True, exclude_event_ids: List[int] = None,
                              max_candidates: int = 500, current_odds: Optional[object] = None) -> Tuple[str, Dict]:
        """Build SQL query for L1 distance prefiltering using L1 distance constraints"""
        # Build exclusion clause
        exclude_clause = ""
        if exclude_event_ids:
            exclude_ids_str = ','.join(map(str, exclude_event_ids))
            exclude_clause = f" AND mae.event_id NOT IN ({exclude_ids_str})"
        
        # Base parameters
        params = {
            'sport': sport,
            'gender': gender,
            'tau': tau,
            'cur_v1': cur_v1,
            'cur_v2': cur_v2,
            'max_candidates': max_candidates
        }
        
        # Build var_shape condition
        var_shape_condition = ""
        if by_shape:
            params['var_shape'] = var_shape
            var_shape_condition = "AND mae.var_shape = :var_shape"
        
        # Build variation conditions for L1 distance prefilter (more accurate than L∞ box)
        if cur_vx is None:
            # No-draw sports (Tennis, etc.) - L1 distance: |var_one - cur_v1| + |var_two - cur_v2|
            if by_shape:
                var_conditions = "(ABS(mae.var_one - :cur_v1) + ABS(mae.var_two - :cur_v2)) <= :tau AND mae.var_x IS NULL"
            else:
                # Allow mixing with 3-way sports when by_shape=False
                var_conditions = "(ABS(mae.var_one - :cur_v1) + ABS(mae.var_two - :cur_v2)) <= :tau"
        else:
            # 3-way sports (Football, etc.) - L1 distance: |var_one - cur_v1| + |var_x - cur_vx| + |var_two - cur_v2|
            params['cur_vx'] = cur_vx
            if by_shape:
                var_conditions = "(ABS(mae.var_one - :cur_v1) + ABS(mae.var_x - :cur_vx) + ABS(mae.var_two - :cur_v2)) <= :tau AND mae.var_x IS NOT NULL"
            else:
                # Allow mixing with no-draw sports when by_shape=False
                var_conditions = "(ABS(mae.var_one - :cur_v1) + ABS(COALESCE(mae.var_x, 0) - COALESCE(:cur_vx, 0)) + ABS(mae.var_two - :cur_v2)) <= :tau"
        
        # Build initial odds filtering conditions (0.50 range per odds, total 1.50)
        initial_odds_conditions = ""
        if current_odds:
            # Add initial odds parameters
            params.update({
                'cur_one_open': current_odds.one_open,
                'cur_two_open': current_odds.two_open,
                'odds_tolerance': 0.15  # 0.50 range per odds
            })
            
            # Build initial odds conditions
            initial_odds_conditions = "AND ABS(mae.one_open - :cur_one_open) <= :odds_tolerance AND ABS(mae.two_open - :cur_two_open) <= :odds_tolerance"
            
            # Add X odds condition for 3-way sports
            if cur_vx is not None and current_odds.x_open is not None:
                params['cur_x_open'] = current_odds.x_open
                initial_odds_conditions += " AND ABS(mae.x_open - :cur_x_open) <= :odds_tolerance"
            elif cur_vx is None:
                # For no-draw sports, ensure X odds are NULL
                initial_odds_conditions += " AND mae.x_open IS NULL"
        
        # Build SQL with L1 distance ordering for better candidate selection
        if cur_vx is None:
            # For no-draw sports, order by L1 distance: |var_one - cur_v1| + |var_two - cur_v2|
            order_by_clause = "(ABS(mae.var_one - :cur_v1) + ABS(mae.var_two - :cur_v2))"
        else:
            # For 3-way sports, order by L1 distance: |var_one - cur_v1| + |var_x - cur_vx| + |var_two - cur_v2|
            order_by_clause = "(ABS(mae.var_one - :cur_v1) + ABS(mae.var_x - :cur_vx) + ABS(mae.var_two - :cur_v2))"
        
        sql = f"""
                    SELECT mae.event_id, mae.participants, mae.result_text, mae.winner_side, mae.point_diff,
                           mae.one_open, mae.x_open, mae.two_open, mae.one_final, mae.x_final, mae.two_final,
                           mae.var_one, mae.var_x, mae.var_two, mae.competition,
                           eo.observation_value as court_type
                    FROM mv_alert_events mae
                    LEFT JOIN event_observations eo ON mae.event_id = eo.event_id 
                      AND eo.observation_type = 'ground_type'
                    WHERE mae.sport = :sport
                      AND mae.gender = :gender
                      {var_shape_condition}
                      AND {var_conditions}
                      {initial_odds_conditions}
                      {exclude_clause}
                    ORDER BY {order_by_clause}
                    LIMIT :max_candidates
        """
        
        return sql, params

    def _build_candidate_sql(self, sport: str, gender: str, var_shape: bool, 
                           search_data, is_exact: bool, exclude_event_ids: List[int] = None) -> Tuple[str, Dict]:
        """Build SQL query and parameters for candidate search"""
        # Build exclusion clause
        exclude_clause = ""
        if exclude_event_ids:
            exclude_ids_str = ','.join(map(str, exclude_event_ids))
            exclude_clause = f" AND mae.event_id NOT IN ({exclude_ids_str})"
        
        # Base parameters
        params = {
            'sport': sport,
            'gender': gender,
            'var_shape': var_shape
        }
        
        if is_exact:
            # Tier 1: Search for exact odds
            current_odds = search_data
            params.update({
                'cur_one_open': current_odds.one_open,
                'cur_two_open': current_odds.two_open,
                'cur_one_final': current_odds.one_final,
                'cur_two_final': current_odds.two_final
            })
            
            if var_shape:
                # 3-way sports (Football, etc.) - include X odds
                params.update({
                    'cur_x_open': current_odds.x_open,
                    'cur_x_final': current_odds.x_final
                })
                odds_conditions = ("mae.one_open = :cur_one_open AND mae.two_open = :cur_two_open AND "
                                 "mae.one_final = :cur_one_final AND mae.two_final = :cur_two_final AND "
                                 "mae.x_open = :cur_x_open AND mae.x_final = :cur_x_final")
            else:
                # No-draw sports (Tennis, etc.) - exclude X odds
                odds_conditions = ("mae.one_open = :cur_one_open AND mae.two_open = :cur_two_open AND "
                                 "mae.one_final = :cur_one_final AND mae.two_final = :cur_two_final AND "
                                 "mae.x_open IS NULL AND mae.x_final IS NULL")
        else:
            # Tier 2: Search for similar variations (unchanged)
            cur_v1, cur_vx, cur_v2 = search_data
            params.update({
                'cur_v1': cur_v1,
                'cur_v2': cur_v2
            })
            
            if cur_vx is None:
                # No-draw sports (Tennis, etc.)
                params['tolerance'] = self.TIER2_TOLERANCE
                odds_conditions = "ABS(mae.var_one - :cur_v1) <= :tolerance AND ABS(mae.var_two - :cur_v2) <= :tolerance AND mae.var_x IS NULL"
            else:
                # 3-way sports (Football, etc.)
                params.update({
                    'cur_vx': cur_vx,
                    'tolerance': self.TIER2_TOLERANCE
                })
                odds_conditions = "ABS(mae.var_one - :cur_v1) <= :tolerance AND ABS(mae.var_two - :cur_v2) <= :tolerance AND mae.var_x IS NOT NULL AND ABS(mae.var_x - :cur_vx) <= :tolerance"
        
        sql = f"""
                    SELECT mae.event_id, mae.participants, mae.result_text, mae.winner_side, mae.point_diff,
                           mae.one_open, mae.x_open, mae.two_open, mae.one_final, mae.x_final, mae.two_final,
                           mae.var_one, mae.var_x, mae.var_two, mae.competition,
                           eo.observation_value as court_type
                    FROM mv_alert_events mae
                    LEFT JOIN event_observations eo ON mae.event_id = eo.event_id 
                      AND eo.observation_type = 'ground_type'
                    WHERE mae.sport = :sport
                      AND mae.gender = :gender
                      AND mae.var_shape = :var_shape
                      AND {odds_conditions}{exclude_clause}
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
            
            # Calculate variation differences for display purposes
            var_diffs = None
            if not is_exact:  # Only calculate differences for similar matches (Tier 2)
                # Calculate signed differences for display (preserving sign)
                d1_diff_signed = float(row.var_one) - cur_v1
                d2_diff_signed = float(row.var_two) - cur_v2
                dx_diff_signed = float(row.var_x) - cur_vx if row.var_x is not None and cur_vx is not None else 0
                
                # Calculate absolute differences for symmetry logic (unchanged)
                d1_diff_abs = abs(d1_diff_signed)
                d2_diff_abs = abs(d2_diff_signed)
                dx_diff_abs = abs(dx_diff_signed) if row.var_x is not None and cur_vx is not None else 0
                
                var_diffs = {
                    'd1': round(d1_diff_signed, 3),  # Store signed difference for display
                    'd2': round(d2_diff_signed, 3),  # Store signed difference for display
                    'dx': round(dx_diff_signed, 3) if row.var_x is not None and cur_vx is not None else None  # Store signed difference for display
                }
            
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
                symmetry_status = "SYMMETRICAL" if is_symmetrical else "UNSYMMETRICAL"
                dx_diff_display = f"{var_diffs['dx']:.3f}" if var_diffs['dx'] is not None else "0.000"
                logger.info(
                    f"{match_type} MATCH: event_id={row.event_id} vars=(d1={row.var_one:.2f}, dx={dx_display}, d2={row.var_two:.2f}) "
                    f"| diffs=(d1={var_diffs['d1']:+.3f}, dx={dx_diff_display}, d2={var_diffs['d2']:+.3f}) "
                    f"| {symmetry_status} | result={row.result_text}, winner={row.winner_side}, point_diff={row.point_diff}"
                )
            
            matches.append(AlertMatch(
                event_id=row.event_id,
                participants=row.participants,
                gender=getattr(row, 'gender', 'unknown'),  # Get gender from query result
                result_text=row.result_text,
                winner_side=row.winner_side,
                point_diff=row.point_diff,
                one_open=float(row.one_open) if row.one_open is not None else 0.0,
                x_open=float(row.x_open) if row.x_open is not None else 0.0,
                two_open=float(row.two_open) if row.two_open is not None else 0.0,
                one_final=float(row.one_final) if row.one_final is not None else 0.0,
                x_final=float(row.x_final) if row.x_final is not None else 0.0,
                two_final=float(row.two_final) if row.two_final is not None else 0.0,
                var_one=float(row.var_one),
                var_x=float(row.var_x) if row.var_x is not None else None,
                var_two=float(row.var_two),
                sport=sport,
                is_symmetrical=is_symmetrical,
                competition=row.competition or 'Unknown',
                var_diffs=var_diffs,
                court_type=getattr(row, 'court_type', None)  # Get court_type from query result
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
                #logger.info(f"🔍 DEBUG: Tier C - No winner groups found")
                return 0
            
            most_common_winner = max(winner_groups.keys(), key=lambda k: len(winner_groups[k]))
            most_common_count = len(winner_groups[most_common_winner])
            result = most_common_count if most_common_count >= 2 else 0
            #logger.info(f"🔍 DEBUG: Tier C - Most common winner: {most_common_winner}, count: {most_common_count}, result: {result}")
            return result
        
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
        """Get Tier B candidates: Same winner+point_diff as majority pattern from ALL candidates (requires at least 2 candidates)"""
        if not remaining_candidates:
            return []
        
        # Find the most common (winner_side, point_diff) from ALL candidates (not just remaining)
        all_winner_diff_groups = defaultdict(list)
        for match in all_candidates:
            key = (match.winner_side, match.point_diff)
            all_winner_diff_groups[key].append(match)
        
        if not all_winner_diff_groups:
            return []
        
        # Find the most common winner+point_diff pattern among ALL candidates
        most_common_winner_diff = max(all_winner_diff_groups.keys(), 
                                    key=lambda k: len(all_winner_diff_groups[k]))
        
        # Tier B requires at least 2 candidates with the same (winner_side, point_diff)
        if len(all_winner_diff_groups[most_common_winner_diff]) < 2:
            return []
        
        # Only assign REMAINING candidates that match the most common pattern from ALL candidates
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
        
        # Use round() to handle floating point precision issues
        result = total_weighted_diff / total_weight if total_weight > 0 else 0
        return round(result, 6)  # Round to 6 decimal places to avoid precision issues
    
    def _calculate_weighted_avg_point_diff(self, matches: List[AlertMatch]) -> float:
        """Calculate weighted average point differential for Tier C rule"""
        total_weighted_diff = 0
        total_weight = 0
        weight = RULE_WEIGHTS['C']  # Use Tier C weight
        
        for match in matches:
            total_weighted_diff += match.point_diff * weight
            total_weight += weight
        
        # Use round() to handle floating point precision issues
        result = total_weighted_diff / total_weight if total_weight > 0 else 0
        return round(result, 6)  # Round to 6 decimal places to avoid precision issues
    
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
    
    def _filter_candidates_by_court_type(self, candidates: List[AlertMatch], 
                                         current_court_type: Optional[str], 
                                         sport: str) -> List[AlertMatch]:
        """
        Filter candidates by court type for Tennis/Tennis Doubles events.
        COMPLETELY FAIL-SAFE: Returns all candidates if filtering fails or if not Tennis.
        
        Args:
            candidates: List of candidate matches to filter
            current_court_type: Court type of the current upcoming event
            sport: Sport name (e.g., 'Tennis', 'Football', etc.)
            
        Returns:
            Filtered list of candidates matching the court type (or all candidates if not applicable)
        """
        try:
            # Only filter for Tennis/Tennis Doubles sports
            if sport not in ['Tennis', 'Tennis Doubles']:
                logger.debug(f"Court type filtering not applicable for sport: {sport}")
                return candidates
            
            # If no current court type provided, return all candidates (fail-safe)
            if not current_court_type:
                logger.info(f"🎾 No court type provided for filtering - returning all {len(candidates)} candidates")
                return candidates
            
            # Filter candidates by matching court type
            filtered_candidates = [
                candidate for candidate in candidates 
                if candidate.court_type == current_court_type
            ]
            
            # Log filtering results
            filtered_count = len(filtered_candidates)
            original_count = len(candidates)
            removed_count = original_count - filtered_count
            
            if removed_count > 0:
                logger.info(f"🎾 Court type filter: '{current_court_type}' - kept {filtered_count}/{original_count} candidates ({removed_count} filtered out)")
                
                # Log which candidates were filtered out
                for candidate in candidates:
                    if candidate not in filtered_candidates:
                        logger.info(f"   ❌ Filtered out: {candidate.participants} (court: {candidate.court_type or 'Unknown'})")
                
                # Log which candidates were kept (maintained)
                if filtered_candidates:
                    logger.info(f"🎾 Candidates that passed the court type filter:")
                    for candidate in filtered_candidates:
                        logger.info(f"   ✅ Kept: {candidate.participants} (court: {candidate.court_type or 'Unknown'})")
            else:
                logger.info(f"🎾 Court type filter: '{current_court_type}' - all {original_count} candidates match")
            
            return filtered_candidates
            
        except Exception as e:
            logger.warning(f"Error filtering candidates by court type: {e}")
            # FAIL-SAFE: Return all candidates on error
            return candidates
    
    def _check_symmetrical_variations(self, cur_v1: float, cur_vx: Optional[float], cur_v2: float,
                                    cand_v1: float, cand_vx: Optional[float], cand_v2: float) -> bool:
        """
        Check if candidate variations are symmetrical to current variations.
        Symmetrical means all variations move by the same AMOUNT (direction doesn't matter).
        
        Example: Current (0.37, -0.30, -1.13) vs Candidate (0.35, -0.32, -1.15)
        All variations moved by amount 0.02, so they are symmetrical.
        
        Args:
            cur_v1, cur_vx, cur_v2: Current event variations
            cand_v1, cand_vx, cand_v2: Candidate event variations
            
        Returns:
            True if variations are symmetrical, False otherwise
        """
        # Calculate absolute differences for each variation (amount only, ignore direction)
        d1_abs_diff = abs(abs(cand_v1) - abs(cur_v1))
        d2_abs_diff = abs(abs(cand_v2) - abs(cur_v2))
        dx_abs_diff = abs(abs(cand_vx) - abs(cur_vx)) if cand_vx is not None and cur_vx is not None else 0
        
        # For 2-way sports (no draw), only check d1 and d2
        if cur_vx is None and cand_vx is None:
            # Check if both variations moved by the same amount
            return abs(d1_abs_diff - d2_abs_diff) < 0.0011  # Slightly higher to handle floating point precision
        
        # For 3-way sports (with draw), check all three variations
        else:
            # Check if all three variations moved by the same amount
            return (abs(d1_abs_diff - d2_abs_diff) < 0.0011 and 
                    abs(d1_abs_diff - dx_abs_diff) < 0.0011 and 
                    abs(d2_abs_diff - dx_abs_diff) < 0.0011)
    
    def _evaluate_candidates_with_new_logic(self, tier1_candidates: List[AlertMatch], 
                                           tier2_candidates: List[AlertMatch]) -> Dict:
        """
        Evaluate candidates using new tier selection and weighted logic
        
        TEMPORARY MODIFICATION: Only use Tier 1 candidates for status, confidence, and summary calculations.
        Tier 2 candidates are still included in the report for display purposes but don't affect evaluation.
        
        Returns:
            Dict with evaluation results including tier used, rule matched, prediction, and confidence
        """
        # TEMPORARY: Only use Tier 1 candidates for evaluation calculations
        # Tier 2 candidates are kept for display but excluded from status/confidence/summary
        evaluation_candidates = []
        selected_tier = ""
        
        # Only include Tier 1 (identical) candidates for evaluation
        if tier1_candidates:
            evaluation_candidates.extend(tier1_candidates)
            selected_tier = "Tier 1 (exact)"
        
        # TEMPORARY: Skip Tier 2 candidates for evaluation but keep them for display
        if tier2_candidates:
            # Apply symmetry filter to ALL Tier 2 candidates (for display purposes)
            symmetrical_tier2 = [c for c in tier2_candidates if c.is_symmetrical]
            logger.info(f"🔍 DEBUG: Tier 2 candidates found: {len(tier2_candidates)} (symmetrical: {len(symmetrical_tier2)})")
            logger.info(f"⚠️ TEMPORARY: Tier 2 candidates excluded from evaluation - only used for display")
            
            # Log non-symmetrical candidates that were filtered out
            non_symmetrical_tier2 = [c for c in tier2_candidates if not c.is_symmetrical]
            if non_symmetrical_tier2:
                logger.info(f"🔍 Filtered out {len(non_symmetrical_tier2)} non-symmetrical Tier 2 candidates")
                for candidate in non_symmetrical_tier2:
                    logger.info(f"   ❌ Non-symmetrical: {candidate.participants} (court: {candidate.court_type or 'Unknown'})")
        
        if not evaluation_candidates:
            return {
                'status': 'no_candidates',
                'selected_tier': None,
                'rule_matched': None,
                'prediction': None,
                'confidence': 0,
                'successful_candidates': 0,
                'total_candidates': 0,
                'non_symmetrical_candidates': len([c for c in tier2_candidates if not c.is_symmetrical]),
                'rule_activations': {},
                'tier1_candidates': tier1_candidates,
                'tier2_candidates': tier2_candidates
            }
        
        # TEMPORARY: Use only Tier 1 candidates for evaluation calculations
        selected_candidates = evaluation_candidates
        logger.info(f"🎯 Using Tier 1 candidates only for evaluation: {len(selected_candidates)} total ({selected_tier})")
        
        # Count non-symmetrical candidates that were filtered out
        non_symmetrical_count = len([c for c in tier2_candidates if not c.is_symmetrical])
        
        # Evaluate rules in priority order: A (identical) > B (similar) > C (same winning side)
        rule_a_result = self._evaluate_identical_results(selected_candidates)
        rule_b_result = self._evaluate_similar_results(selected_candidates)
        rule_c_result = self._evaluate_same_winning_side(selected_candidates)
        
        # Count candidates that match each rule, with priority (A > B > C)
        tier_a_matches = self._count_candidates_matching_rule(selected_candidates, 'identical')
        tier_b_matches = self._count_candidates_matching_rule(selected_candidates, 'similar')
        tier_c_matches = self._count_candidates_matching_rule(selected_candidates, 'same_winner')
        
        # DEBUG: Log the counting results
        #logger.info(f"🔍 DEBUG: Rule counting results - A: {tier_a_matches}, B: {tier_b_matches}, C: {tier_c_matches}")

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
        
        # Determine status based on candidate count and rule matching
        if len(selected_candidates) == 0:
            # No candidates left after filtering (court type + symmetry filters)
            status = 'no_candidates'
            successful_candidates = 0
            total_candidates = 0
            confidence = 0
            prediction_result = None
            logger.info(f"🔍 DEBUG: Status: {status} (all candidates filtered out)")
        elif len(selected_candidates) == 1:
            # Single candidate case - insufficient for prediction (need at least 2)
            status = 'partial'
            confidence = 0
            prediction_result = None  # No prediction for single candidate
            successful_candidates = 1
            total_candidates = 1
            logger.info(f"🔍 DEBUG: Status: {status} (single candidate - insufficient for prediction)")
        elif total_matching_candidates == len(selected_candidates):
            # All candidates match at least one rule - evaluate for SUCCESS
            # Calculate weighted confidence based on PRIORITY-BASED tier assignments
            tier_assignments = self._get_candidates_by_rule_tiers(selected_candidates)
            
            # Calculate confidence using each candidate's HIGHEST priority tier only
            weighted_successes = (len(tier_assignments['A']) * RULE_WEIGHTS['A'] + 
                                len(tier_assignments['B']) * RULE_WEIGHTS['B'] + 
                                len(tier_assignments['C']) * RULE_WEIGHTS['C'])
            max_possible_weight = len(selected_candidates) * MAX_WEIGHT
            confidence = (weighted_successes / max_possible_weight) * 100 if max_possible_weight > 0 else 0
            confidence = round(confidence, 1)  # Round to 1 decimal place to avoid precision issues
            
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
                logger.info(f"🔍 DEBUG: Status: {status}")
            else:
                # All candidates processed but no prediction generated (insufficient candidates for any tier)
                status = 'partial'
                confidence = 0
                logger.info(f"🔍 DEBUG: Status: {status}")
        else:
            # Some candidates failed to match any rule
            status = 'no_match'
            successful_candidates = total_matching_candidates
            total_candidates = len(selected_candidates)
            confidence = 0
            prediction_result = None
            logger.info(f"🔍 DEBUG: Status: {status} (candidates failed rules)")
        
        # Get rule activation details for reporting
        rule_activations = self._get_rule_activations(selected_candidates)
        
        # Non-symmetrical count already calculated above during filtering
        
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
        # Limit Tier 2 candidates to 6 to avoid Telegram character limit
        tier2_candidates_limited = tier2_candidates[:6] if tier2_candidates else []
        if len(tier2_candidates) > 16:
            logger.info(f"📊 Limited Tier 2 candidates display to 6 (from {len(tier2_candidates)} total) to avoid Telegram character limit")
        tier2_matches_data = self._format_candidate_data(tier2_candidates_limited)
        
        return {
            'event_id': event.id,
            'rule_key': f"candidate_report_{event.id}",
            'participants': f"{event.home_team} vs {event.away_team}",
            'competition': event.competition,
            'sport': event.sport,
            'discovery_source': event.discovery_source,
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
                'count': len(tier2_candidates_limited),
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
                'competition': match.competition,
                'court_type': match.court_type,  # Include court type for Tennis events
                'variations': {
                    'var_one': match.var_one,
                    'var_x': match.var_x,
                    'var_two': match.var_two
                },
                'var_diffs': match.var_diffs,
                'distance_l1': match.distance_l1,  # Include L1 distance for L1-based matches
                'one_open': match.one_open,
                'x_open': match.x_open,
                'two_open': match.two_open,
                'one_final': match.one_final,
                'x_final': match.x_final,
                'two_final': match.two_final
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
