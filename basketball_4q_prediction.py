"""
Basketball 4th Quarter Prediction System

Implements the prediction logic for 4th quarter scores based on:
- Historical team performance in specific season stages
- Current game rhythm (Q1-Q3 performance)
- Momentum adjustments based on score differential
- Statistical confidence ranges
"""

import logging
from typing import Dict, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import db_manager

logger = logging.getLogger(__name__)


def map_season_stage_to_db_round(season_stage: str) -> str:
    """
    Map season stage from API format to database round values.
    
    Database only has: 'regular_season' or 'knockouts/playoffs'
    
    Args:
        season_stage: Season stage from API (e.g., 'Regular Season', 'Playoffs', 'Preseason')
        
    Returns:
        Database round value: 'regular_season' or 'knockouts/playoffs'
    """
    season_stage_lower = season_stage.lower()
    
    # Map to database values
    if 'playoff' in season_stage_lower or 'knockout' in season_stage_lower:
        return 'knockouts/playoffs'
    else:
        # Default to regular_season for Regular Season, Preseason, Cup, etc.
        return 'regular_season'


class Basketball4QPredictor:
    """
    Predicts 4th quarter scores for basketball games based on historical data
    and current game state.
    """
    
    def __init__(self):
        """Initialize the predictor."""
        pass
    
    def get_historical_q4_stats(
        self,
        team_name: str,
        season_stage: str,
        session: Session
    ) -> Tuple[Optional[float], Optional[float], int]:
        """
        Get average and standard deviation for Q4 points for a specific team and season stage.
        
        Args:
            team_name: Team name to query
            season_stage: Season stage (e.g., 'Regular Season', 'Playoffs', 'Preseason')
            session: Database session
            
        Returns:
            Tuple of (average_q4, stddev_q4) or (None, None) if no data
        """
        try:
            # Map season stage to database round value
            db_round = map_season_stage_to_db_round(season_stage)
            
            # Prepare query parameters
            team_pattern = f'%{team_name}%'
            
            logger.info(f"🔍 Querying Q4 historical stats for team: '{team_name}' in season stage: '{season_stage}'")
            logger.info(f"   Mapped to database round: '{db_round}'")
            logger.info(f"   SQL WHERE clauses:")
            logger.info(f"      - home_team ILIKE '{team_pattern}' OR away_team ILIKE '{team_pattern}'")
            logger.info(f"      - round = '{db_round}' (exact match, not ILIKE)")
            logger.info(f"      - quarter_4_home IS NOT NULL (for home games)")
            logger.info(f"      - quarter_4_away IS NOT NULL (for away games)")
            logger.info(f"      - home_team ILIKE '{team_pattern}' (filter home games where team is home)")
            logger.info(f"      - away_team ILIKE '{team_pattern}' (filter away games where team is away)")
            
            query = text("""
                SELECT 
                    AVG(points_q4) AS avg_q4,
                    STDDEV_POP(points_q4) AS stddev_q4,
                    COUNT(*) AS sample_size
                FROM (
                    SELECT quarter_4_home AS points_q4
                    FROM basketball_results
                    WHERE (home_team ILIKE :team_name OR away_team ILIKE :team_name)
                      AND round = :db_round
                      AND quarter_4_home IS NOT NULL
                      AND home_team ILIKE :team_name
                    
                    UNION ALL
                    
                    SELECT quarter_4_away AS points_q4
                    FROM basketball_results
                    WHERE (home_team ILIKE :team_name OR away_team ILIKE :team_name)
                      AND round = :db_round
                      AND quarter_4_away IS NOT NULL
                      AND away_team ILIKE :team_name
                ) t
            """)
            
            result = session.execute(query, {
                'team_name': team_pattern,
                'db_round': db_round
            }).fetchone()
            
            if result and result.sample_size and result.sample_size > 0:
                avg_q4 = float(result.avg_q4) if result.avg_q4 is not None else None
                stddev_q4 = float(result.stddev_q4) if result.stddev_q4 is not None else None
                sample_size = int(result.sample_size) if result.sample_size else 0
                logger.info(f"✅ Found {sample_size} historical Q4 records")
                logger.info(f"   Results: avg_q4={avg_q4:.2f}, stddev_q4={stddev_q4:.2f}")
                return avg_q4, stddev_q4, sample_size
            else:
                sample_size = int(result.sample_size) if result and result.sample_size else 0
                logger.warning(f"❌ No Q4 historical data found for team '{team_name}' in '{season_stage}'")
                logger.warning(f"   Query returned {sample_size} records")
                return None, None, sample_size
                
        except Exception as e:
            logger.error(f"Error getting Q4 stats for team '{team_name}': {e}")
            # Rollback the session to recover from failed transaction
            try:
                session.rollback()
                logger.debug(f"Session rolled back after error in get_historical_q4_stats")
            except Exception as rollback_error:
                logger.error(f"Error rolling back session: {rollback_error}")
            return None, None, 0
    
    def get_historical_q1_q3_combined_avg(
        self,
        season_stage: str,
        session: Session
    ) -> Optional[float]:
        """
        Get average combined Q1-Q3 total points for both teams in a specific season stage.
        
        Follows the guide: Calculate historical TOTAL average of Q1-Q3 combined.
        Uses basketball_results view which already has quarters parsed into columns.
        
        Args:
            season_stage: Season stage to filter
            session: Database session
            
        Returns:
            Average combined Q1-Q3 points or None if no data
        """
        try:
            # Map season stage to database round value
            db_round = map_season_stage_to_db_round(season_stage)
            
            logger.info(f"🔍 Querying historical Q1-Q3 combined average for season stage: '{season_stage}'")
            logger.info(f"   Mapped to database round: '{db_round}'")
            logger.info(f"   Using basketball_results view (quarters already parsed)")
            logger.info(f"   SQL WHERE clauses:")
            logger.info(f"      - round = '{db_round}' (exact match, not ILIKE)")
            logger.info(f"      - quarter_1_home IS NOT NULL")
            logger.info(f"      - quarter_2_home IS NOT NULL")
            logger.info(f"      - quarter_3_home IS NOT NULL")
            logger.info(f"      - quarter_1_away IS NOT NULL")
            logger.info(f"      - quarter_2_away IS NOT NULL")
            logger.info(f"      - quarter_3_away IS NOT NULL")
            logger.info(f"   Calculation: Sum of all Q1-Q3 quarters (home + away) from basketball_results view")
            
            # Use basketball_results view directly - quarters are already parsed into columns
            # Follows guide: AVG(points_q1_local + points_q2_local + points_q3_local + 
            #                   points_q1_visitante + points_q2_visitante + points_q3_visitante)
            # The view should already have INTEGER columns, but we'll use COALESCE for safety
            query = text("""
                SELECT 
                    AVG(q1_q3_total) AS avg_q1_q3_combined,
                    COUNT(*) AS sample_size
                FROM (
                    SELECT 
                        (COALESCE(quarter_1_home, 0) + 
                         COALESCE(quarter_2_home, 0) + 
                         COALESCE(quarter_3_home, 0) +
                         COALESCE(quarter_1_away, 0) + 
                         COALESCE(quarter_2_away, 0) + 
                         COALESCE(quarter_3_away, 0)) AS q1_q3_total
                    FROM basketball_results
                    WHERE round = :db_round
                      AND quarter_1_home IS NOT NULL
                      AND quarter_2_home IS NOT NULL
                      AND quarter_3_home IS NOT NULL
                      AND quarter_1_away IS NOT NULL
                      AND quarter_2_away IS NOT NULL
                      AND quarter_3_away IS NOT NULL
                ) subquery
                WHERE q1_q3_total IS NOT NULL
                  AND q1_q3_total > 0
            """)
            
            result = session.execute(query, {'db_round': db_round}).fetchone()
            
            if result and result.avg_q1_q3_combined:
                avg = float(result.avg_q1_q3_combined)
                logger.info(f"✅ Found {result.sample_size} historical games with complete Q1-Q3 data")
                logger.info(f"   Historical Q1-Q3 combined average: {avg:.2f} points")
                return avg
            else:
                logger.warning(f"❌ No Q1-Q3 combined data found for '{season_stage}'")
                logger.warning(f"   Query returned {result.sample_size if result else 0} records")
                return None
                
        except Exception as e:
            logger.error(f"Error getting Q1-Q3 combined average: {e}")
            # Rollback the session to recover from failed transaction
            try:
                session.rollback()
                logger.debug(f"Session rolled back after error in get_historical_q1_q3_combined_avg")
            except Exception as rollback_error:
                logger.error(f"Error rolling back session: {rollback_error}")
            return None
    
    def predict_4th_quarter(
        self,
        home_team: str,
        away_team: str,
        q1_home: int,
        q2_home: int,
        q3_home: int,
        q1_away: int,
        q2_away: int,
        q3_away: int,
        season_stage: str = "Regular Season"
    ) -> Dict:
        """
        Generate 4th quarter prediction based on current game state and historical data.
        
        Args:
            home_team: Home team name
            away_team: Away team name
            q1_home, q2_home, q3_home: Home team quarter scores
            q1_away, q2_away, q3_away: Away team quarter scores
            season_stage: Season stage (default: "Regular Season")
            
        Returns:
            Dictionary with prediction results:
            - predicted_q4_home: Predicted Q4 points for home team
            - predicted_q4_away: Predicted Q4 points for away team
            - predicted_final_home: Predicted final score for home team
            - predicted_final_away: Predicted final score for away team
            - confidence_range: Numerical confidence range
            - confidence_level: "HIGH", "MEDIUM", or "LOW"
            - rhythm_factor: Calculated rhythm factor
            - score_differential: Current score differential (home - away)
            - parameters: Dict with all calculated parameters
        """
        try:
            logger.info(f"🏀 Starting 4Q prediction for: {home_team} vs {away_team}")
            logger.info(f"   Season Stage: {season_stage}")
            logger.info(f"   Current Q1-Q3 Scores: Home {q1_home + q2_home + q3_home} ({q1_home}-{q2_home}-{q3_home}), Away {q1_away + q2_away + q3_away} ({q1_away}-{q2_away}-{q3_away})")
            
            with db_manager.get_session() as session:
                # STEP 1: Get historical Q4 stats for both teams
                logger.info(f"\n📊 STEP 1: Querying historical Q4 stats for HOME team: {home_team}")
                avg_q4_home, stddev_q4_home, sample_size_home = self.get_historical_q4_stats(home_team, season_stage, session)
                
                logger.info(f"\n📊 STEP 2: Querying historical Q4 stats for AWAY team: {away_team}")
                avg_q4_away, stddev_q4_away, sample_size_away = self.get_historical_q4_stats(away_team, season_stage, session)
                
                if avg_q4_home is None or avg_q4_away is None:
                    logger.warning(f"⚠️ Missing Q4 stats for one or both teams, using defaults")
                    logger.warning(f"   Home team Q4: avg={avg_q4_home or 25.0:.1f} (default), stddev={stddev_q4_home or 5.0:.1f} (default)")
                    logger.warning(f"   Away team Q4: avg={avg_q4_away or 25.0:.1f} (default), stddev={stddev_q4_away or 5.0:.1f} (default)")
                    avg_q4_home = avg_q4_home or 25.0  # Default average
                    avg_q4_away = avg_q4_away or 25.0
                    stddev_q4_home = stddev_q4_home or 5.0  # Default stddev
                    stddev_q4_away = stddev_q4_away or 5.0
                else:
                    logger.info(f"✅ Q4 Stats retrieved:")
                    logger.info(f"   Home ({home_team}): avg={avg_q4_home:.2f}, stddev={stddev_q4_home:.2f}")
                    logger.info(f"   Away ({away_team}): avg={avg_q4_away:.2f}, stddev={stddev_q4_away:.2f}")
                
                # STEP 3: Get historical Q1-Q3 combined average
                logger.info(f"\n📊 STEP 3: Querying historical Q1-Q3 combined average")
                avg_q1_q3_combined = self.get_historical_q1_q3_combined_avg(season_stage, session)
                if avg_q1_q3_combined is None:
                    logger.warning("⚠️ Missing historical Q1-Q3 data, using default: 150.0")
                    avg_q1_q3_combined = 150.0  # Default combined average
                else:
                    logger.info(f"✅ Historical Q1-Q3 combined average: {avg_q1_q3_combined:.2f} points")
                
                total_q1_q3_home = q1_home + q2_home + q3_home
                total_q1_q3_away = q1_away + q2_away + q3_away
                total_q1_q3_combined = total_q1_q3_home + total_q1_q3_away
                logger.info(f"   Current Q1-Q3 totals: Home={total_q1_q3_home}, Away={total_q1_q3_away}, Combined={total_q1_q3_combined}")
                
                # STEP 5: Calculate rhythm factor
                rhythm_factor = total_q1_q3_combined / avg_q1_q3_combined if avg_q1_q3_combined > 0 else 1.0
                logger.info(f"   Rhythm Factor: {total_q1_q3_combined:.1f} / {avg_q1_q3_combined:.1f} = {rhythm_factor:.3f}x")
                
                # STEP 6: Base Q4 prediction (before momentum adjustment)
                pred_q4_home_base = avg_q4_home * rhythm_factor
                pred_q4_away_base = avg_q4_away * rhythm_factor
                logger.info(f"   Base Q4 Prediction (before momentum): Home={pred_q4_home_base:.1f}, Away={pred_q4_away_base:.1f}")
                
                # STEP 7: Apply momentum adjustment based on score differential
                score_diff = total_q1_q3_home - total_q1_q3_away
                logger.info(f"   Score Differential (Home - Away): {score_diff} points")
                
                pred_q4_home_adj = pred_q4_home_base
                pred_q4_away_adj = pred_q4_away_base
                
                if score_diff > 8:
                    # Home team ahead by >8: slow down home, speed up away
                    logger.info(f"   ⚡ Momentum adjustment: Home leading by {score_diff} (>8), applying adjustments")
                    logger.info(f"      Home: {pred_q4_home_base:.1f} × 0.95 = {pred_q4_home_base * 0.95:.1f}")
                    logger.info(f"      Away: {pred_q4_away_base:.1f} × 1.06 = {pred_q4_away_base * 1.06:.1f}")
                    pred_q4_home_adj = pred_q4_home_base * 0.95
                    pred_q4_away_adj = pred_q4_away_base * 1.06
                elif score_diff < -8:
                    # Away team ahead by >8: slow down away, speed up home
                    logger.info(f"   ⚡ Momentum adjustment: Away leading by {abs(score_diff)} (>8), applying adjustments")
                    logger.info(f"      Away: {pred_q4_away_base:.1f} × 0.95 = {pred_q4_away_base * 0.95:.1f}")
                    logger.info(f"      Home: {pred_q4_home_base:.1f} × 1.06 = {pred_q4_home_base * 1.06:.1f}")
                    pred_q4_away_adj = pred_q4_away_base * 0.95
                    pred_q4_home_adj = pred_q4_home_base * 1.06
                else:
                    # No adjustment for -8 <= diff <= 8
                    logger.info(f"   ⚡ No momentum adjustment (score diff {score_diff} is within -8 to +8 range)")
                
                # STEP 8: Calculate explosiveness (volatility)
                logger.info(f"\n📊 STEP 8: Calculating explosiveness (volatility)")
                explosiveness_home = stddev_q4_home * 0.5
                explosiveness_away = stddev_q4_away * 0.5
                logger.info(f"   Explosiveness: Home={explosiveness_home:.2f} (stddev {stddev_q4_home:.2f} × 0.5), Away={explosiveness_away:.2f} (stddev {stddev_q4_away:.2f} × 0.5)")
                
                # STEP 9: Predicted final scores
                logger.info(f"\n📊 STEP 9: Calculating final score projections")
                predicted_final_home = total_q1_q3_home + pred_q4_home_adj
                predicted_final_away = total_q1_q3_away + pred_q4_away_adj
                logger.info(f"   Final Home: {total_q1_q3_home} (Q1-Q3) + {pred_q4_home_adj:.1f} (Q4) = {predicted_final_home:.1f}")
                logger.info(f"   Final Away: {total_q1_q3_away} (Q1-Q3) + {pred_q4_away_adj:.1f} (Q4) = {predicted_final_away:.1f}")
                
                # STEP 10: Confidence range calculation
                logger.info(f"\n📊 STEP 10: Calculating confidence metrics")
                confidence_range_numeric = (explosiveness_home + explosiveness_away) / 2
                logger.info(f"   Confidence Range: ({explosiveness_home:.2f} + {explosiveness_away:.2f}) / 2 = {confidence_range_numeric:.2f}")
                
                if confidence_range_numeric < 3:
                    confidence_level = "HIGH"
                elif confidence_range_numeric <= 6:
                    confidence_level = "MEDIUM"
                else:
                    confidence_level = "LOW"
                logger.info(f"   Confidence Level: {confidence_level} (range < 3 = HIGH, 3-6 = MEDIUM, > 6 = LOW)")
                
                # STEP 11: Z-score calculation for additional confidence metric
                logger.info(f"\n📊 STEP 11: Calculating Z-score (distance from historical mean)")
                # Average the predicted Q4 for both teams
                avg_predicted_q4 = (pred_q4_home_adj + pred_q4_away_adj) / 2
                avg_historical_q4 = (avg_q4_home + avg_q4_away) / 2
                avg_stddev_q4 = (stddev_q4_home + stddev_q4_away) / 2
                
                distance_from_mean = abs(avg_predicted_q4 - avg_historical_q4)
                z_score = distance_from_mean / avg_stddev_q4 if avg_stddev_q4 > 0 else 0
                
                logger.info(f"   Avg Predicted Q4: ({pred_q4_home_adj:.1f} + {pred_q4_away_adj:.1f}) / 2 = {avg_predicted_q4:.2f}")
                logger.info(f"   Avg Historical Q4: ({avg_q4_home:.2f} + {avg_q4_away:.2f}) / 2 = {avg_historical_q4:.2f}")
                logger.info(f"   Distance from Mean: |{avg_predicted_q4:.2f} - {avg_historical_q4:.2f}| = {distance_from_mean:.2f}")
                logger.info(f"   Avg Stddev Q4: ({stddev_q4_home:.2f} + {stddev_q4_away:.2f}) / 2 = {avg_stddev_q4:.2f}")
                logger.info(f"   Z-Score: {distance_from_mean:.2f} / {avg_stddev_q4:.2f} = {z_score:.3f}")
                
                # Additional confidence classification based on z-score
                if z_score <= 0.5:
                    z_confidence = "HIGH"
                elif z_score <= 1.0:
                    z_confidence = "MEDIUM"
                else:
                    z_confidence = "LOW"
                logger.info(f"   Z-Confidence: {z_confidence} (z ≤ 0.5 = HIGH, 0.5-1.0 = MEDIUM, > 1.0 = LOW)")
                
                # Final result
                result = {
                    'predicted_q4_home': round(pred_q4_home_adj, 1),
                    'predicted_q4_away': round(pred_q4_away_adj, 1),
                    'predicted_final_home': round(predicted_final_home, 1),
                    'predicted_final_away': round(predicted_final_away, 1),
                    'base_q4_home': round(pred_q4_home_base, 1),
                    'base_q4_away': round(pred_q4_away_base, 1),
                    'confidence_range_numeric': round(confidence_range_numeric, 2),
                    'confidence_level': confidence_level,
                    'z_score': round(z_score, 3),
                    'z_confidence': z_confidence,
                    'distance_from_mean': round(distance_from_mean, 2),
                    'rhythm_factor': round(rhythm_factor, 3),
                    'score_differential': score_diff,
                    'explosiveness': round((explosiveness_home + explosiveness_away) / 2, 2),
                    'parameters': {
                        'avg_q4_home': round(avg_q4_home, 2),
                        'avg_q4_away': round(avg_q4_away, 2),
                        'stddev_q4_home': round(stddev_q4_home, 2),
                        'stddev_q4_away': round(stddev_q4_away, 2),
                        'avg_q1_q3_combined_historical': round(avg_q1_q3_combined, 2),
                        'total_q1_q3_home': total_q1_q3_home,
                        'total_q1_q3_away': total_q1_q3_away,
                        'total_q1_q3_combined_current': total_q1_q3_combined,
                        'explosiveness_home': round(explosiveness_home, 2),
                        'explosiveness_away': round(explosiveness_away, 2),
                        'momentum_applied': abs(score_diff) > 8,
                        'sample_size_home': sample_size_home,
                        'sample_size_away': sample_size_away
                    }
                }
                
                logger.info(f"\n✅ PREDICTION COMPLETE:")
                logger.info(f"   Q4 Prediction: {home_team} {result['predicted_q4_home']:.1f} - {result['predicted_q4_away']:.1f} {away_team}")
                logger.info(f"   Final Projection: {result['predicted_final_home']:.1f} - {result['predicted_final_away']:.1f}")
                logger.info(f"   Confidence: {result['confidence_level']} (range: {result['confidence_range_numeric']:.2f}, z-score: {result['z_score']:.3f} = {result['z_confidence']})")
                logger.info(f"   Rhythm Factor: {result['rhythm_factor']:.3f}x, Score Differential: {result['score_differential']}")
                
                return result
                
        except Exception as e:
            logger.error(f"Error generating 4Q prediction: {e}")
            return {
                'error': str(e),
                'predicted_q4_home': None,
                'predicted_q4_away': None,
                'predicted_final_home': None,
                'predicted_final_away': None,
                'confidence_level': 'LOW',
                'confidence_range_numeric': 0
            }


# Global predictor instance
predictor_4q = Basketball4QPredictor()

