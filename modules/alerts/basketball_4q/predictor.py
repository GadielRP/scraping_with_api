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

from database import db_manager
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def map_season_stage_to_db_round(season_stage: str) -> str:
    """
    Map season stage from API format to database round values.

    Database only has: 'regular_season' or 'knockouts/playoffs'
    """
    season_stage_lower = season_stage.lower()

    if "playoff" in season_stage_lower or "knockout" in season_stage_lower:
        return "knockouts/playoffs"
    return "regular_season"


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
        session: Session,
    ) -> Tuple[Optional[float], Optional[float], int]:
        """Get average and standard deviation for Q4 points for a specific team."""
        try:
            db_round = map_season_stage_to_db_round(season_stage)
            team_pattern = f"%{team_name}%"

            logger.info(f"🔍 Querying Q4 historical stats for team: '{team_name}' in season stage: '{season_stage}'")
            logger.info(f"   Mapped to database round: '{db_round}'")

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

            result = session.execute(query, {"team_name": team_pattern, "db_round": db_round}).fetchone()

            if result and result.sample_size and result.sample_size > 0:
                avg_q4 = float(result.avg_q4) if result.avg_q4 is not None else None
                stddev_q4 = float(result.stddev_q4) if result.stddev_q4 is not None else None
                sample_size = int(result.sample_size) if result.sample_size else 0
                logger.info(f"✅ Found {sample_size} historical Q4 records")
                logger.info(f"   Results: avg_q4={avg_q4:.2f}, stddev_q4={stddev_q4:.2f}")
                return avg_q4, stddev_q4, sample_size

            sample_size = int(result.sample_size) if result and result.sample_size else 0
            logger.warning(f"❌ No Q4 historical data found for team '{team_name}' in '{season_stage}'")
            logger.warning(f"   Query returned {sample_size} records")
            return None, None, sample_size
        except Exception as e:
            logger.error(f"Error getting Q4 stats for team '{team_name}': {e}")
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error rolling back session: {rollback_error}")
            return None, None, 0

    def get_historical_q1_q3_combined_avg(
        self,
        season_stage: str,
        session: Session,
    ) -> Optional[float]:
        """Get average combined Q1-Q3 total points for both teams in a specific season stage."""
        try:
            db_round = map_season_stage_to_db_round(season_stage)

            logger.info(f"🔍 Querying historical Q1-Q3 combined average for season stage: '{season_stage}'")
            logger.info(f"   Mapped to database round: '{db_round}'")

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

            result = session.execute(query, {"db_round": db_round}).fetchone()

            if result and result.avg_q1_q3_combined:
                avg = float(result.avg_q1_q3_combined)
                logger.info(f"✅ Found {result.sample_size} historical games with complete Q1-Q3 data")
                logger.info(f"   Historical Q1-Q3 combined average: {avg:.2f} points")
                return avg

            logger.warning(f"❌ No Q1-Q3 combined data found for '{season_stage}'")
            logger.warning(f"   Query returned {result.sample_size if result else 0} records")
            return None
        except Exception as e:
            logger.error(f"Error getting Q1-Q3 combined average: {e}")
            try:
                session.rollback()
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
        season_stage: str = "Regular Season",
    ) -> Dict:
        """Generate 4th quarter prediction based on current game state and historical data."""
        try:
            logger.info(f"🏀 Starting 4Q prediction for: {home_team} vs {away_team}")
            logger.info(f"   Season Stage: {season_stage}")
            logger.info(
                f"   Current Q1-Q3 Scores: Home {q1_home + q2_home + q3_home} ({q1_home}-{q2_home}-{q3_home}), "
                f"Away {q1_away + q2_away + q3_away} ({q1_away}-{q2_away}-{q3_away})"
            )

            with db_manager.get_session() as session:
                logger.info(f"\n📊 STEP 1: Querying historical Q4 stats for HOME team: {home_team}")
                avg_q4_home, stddev_q4_home, sample_size_home = self.get_historical_q4_stats(home_team, season_stage, session)

                logger.info(f"\n📊 STEP 2: Querying historical Q4 stats for AWAY team: {away_team}")
                avg_q4_away, stddev_q4_away, sample_size_away = self.get_historical_q4_stats(away_team, season_stage, session)

                if avg_q4_home is None or avg_q4_away is None:
                    logger.warning("⚠️ Missing Q4 stats for one or both teams, using defaults")
                    avg_q4_home = avg_q4_home or 25.0
                    avg_q4_away = avg_q4_away or 25.0
                    stddev_q4_home = stddev_q4_home or 5.0
                    stddev_q4_away = stddev_q4_away or 5.0
                else:
                    logger.info("✅ Q4 Stats retrieved:")
                    logger.info(f"   Home ({home_team}): avg={avg_q4_home:.2f}, stddev={stddev_q4_home:.2f}")
                    logger.info(f"   Away ({away_team}): avg={avg_q4_away:.2f}, stddev={stddev_q4_away:.2f}")

                logger.info("\n📊 STEP 3: Querying historical Q1-Q3 combined average")
                avg_q1_q3_combined = self.get_historical_q1_q3_combined_avg(season_stage, session)
                if avg_q1_q3_combined is None:
                    logger.warning("⚠️ Missing historical Q1-Q3 data, using default: 150.0")
                    avg_q1_q3_combined = 150.0
                else:
                    logger.info(f"✅ Historical Q1-Q3 combined average: {avg_q1_q3_combined:.2f} points")

                total_q1_q3_home = q1_home + q2_home + q3_home
                total_q1_q3_away = q1_away + q2_away + q3_away
                total_q1_q3_combined = total_q1_q3_home + total_q1_q3_away
                logger.info(
                    f"   Current Q1-Q3 totals: Home={total_q1_q3_home}, Away={total_q1_q3_away}, "
                    f"Combined={total_q1_q3_combined}"
                )

                rhythm_factor = total_q1_q3_combined / avg_q1_q3_combined if avg_q1_q3_combined > 0 else 1.0
                logger.info(f"   Rhythm Factor: {total_q1_q3_combined:.1f} / {avg_q1_q3_combined:.1f} = {rhythm_factor:.3f}x")

                pred_q4_home_base = avg_q4_home * rhythm_factor
                pred_q4_away_base = avg_q4_away * rhythm_factor
                logger.info(
                    f"   Base Q4 Prediction (before momentum): Home={pred_q4_home_base:.1f}, "
                    f"Away={pred_q4_away_base:.1f}"
                )

                score_diff = total_q1_q3_home - total_q1_q3_away
                logger.info(f"   Score Differential (Home - Away): {score_diff} points")

                pred_q4_home_adj = pred_q4_home_base
                pred_q4_away_adj = pred_q4_away_base

                if score_diff > 8:
                    logger.info(f"   ⚡ Momentum adjustment: Home leading by {score_diff} (>8), applying adjustments")
                    pred_q4_home_adj = pred_q4_home_base * 0.95
                    pred_q4_away_adj = pred_q4_away_base * 1.06
                elif score_diff < -8:
                    logger.info(f"   ⚡ Momentum adjustment: Away leading by {abs(score_diff)} (>8), applying adjustments")
                    pred_q4_away_adj = pred_q4_away_base * 0.95
                    pred_q4_home_adj = pred_q4_home_base * 1.06
                else:
                    logger.info(f"   ⚡ No momentum adjustment (score diff {score_diff} is within -8 to +8 range)")

                logger.info("\n📊 STEP 8: Calculating explosiveness (volatility)")
                explosiveness_home = stddev_q4_home * 0.5
                explosiveness_away = stddev_q4_away * 0.5

                logger.info("\n📊 STEP 9: Calculating final score projections")
                predicted_final_home = total_q1_q3_home + pred_q4_home_adj
                predicted_final_away = total_q1_q3_away + pred_q4_away_adj

                logger.info("\n📊 STEP 10: Calculating confidence metrics")
                confidence_range_numeric = (explosiveness_home + explosiveness_away) / 2
                if confidence_range_numeric < 3:
                    confidence_level = "HIGH"
                elif confidence_range_numeric <= 6:
                    confidence_level = "MEDIUM"
                else:
                    confidence_level = "LOW"

                logger.info("\n📊 STEP 11: Calculating Z-score (distance from historical mean)")
                avg_predicted_q4 = (pred_q4_home_adj + pred_q4_away_adj) / 2
                avg_historical_q4 = (avg_q4_home + avg_q4_away) / 2
                avg_stddev_q4 = (stddev_q4_home + stddev_q4_away) / 2

                distance_from_mean = abs(avg_predicted_q4 - avg_historical_q4)
                z_score = distance_from_mean / avg_stddev_q4 if avg_stddev_q4 > 0 else 0

                if z_score <= 0.5:
                    z_confidence = "HIGH"
                elif z_score <= 1.0:
                    z_confidence = "MEDIUM"
                else:
                    z_confidence = "LOW"

                result = {
                    "predicted_q4_home": round(pred_q4_home_adj, 1),
                    "predicted_q4_away": round(pred_q4_away_adj, 1),
                    "predicted_final_home": round(predicted_final_home, 1),
                    "predicted_final_away": round(predicted_final_away, 1),
                    "base_q4_home": round(pred_q4_home_base, 1),
                    "base_q4_away": round(pred_q4_away_base, 1),
                    "confidence_range_numeric": round(confidence_range_numeric, 2),
                    "confidence_level": confidence_level,
                    "z_score": round(z_score, 3),
                    "z_confidence": z_confidence,
                    "distance_from_mean": round(distance_from_mean, 2),
                    "rhythm_factor": round(rhythm_factor, 3),
                    "score_differential": score_diff,
                    "explosiveness": round((explosiveness_home + explosiveness_away) / 2, 2),
                    "parameters": {
                        "avg_q4_home": round(avg_q4_home, 2),
                        "avg_q4_away": round(avg_q4_away, 2),
                        "stddev_q4_home": round(stddev_q4_home, 2),
                        "stddev_q4_away": round(stddev_q4_away, 2),
                        "avg_q1_q3_combined_historical": round(avg_q1_q3_combined, 2),
                        "total_q1_q3_home": total_q1_q3_home,
                        "total_q1_q3_away": total_q1_q3_away,
                        "total_q1_q3_combined_current": total_q1_q3_combined,
                        "explosiveness_home": round(explosiveness_home, 2),
                        "explosiveness_away": round(explosiveness_away, 2),
                        "momentum_applied": abs(score_diff) > 8,
                        "sample_size_home": sample_size_home,
                        "sample_size_away": sample_size_away,
                    },
                }

                logger.info("\n✅ PREDICTION COMPLETE:")
                logger.info(
                    f"   Q4 Prediction: {home_team} {result['predicted_q4_home']:.1f} - "
                    f"{result['predicted_q4_away']:.1f} {away_team}"
                )
                logger.info(
                    f"   Final Projection: {result['predicted_final_home']:.1f} - "
                    f"{result['predicted_final_away']:.1f}"
                )
                logger.info(
                    f"   Confidence: {result['confidence_level']} "
                    f"(range: {result['confidence_range_numeric']:.2f}, z-score: {result['z_score']:.3f} = {result['z_confidence']})"
                )
                logger.info(
                    f"   Rhythm Factor: {result['rhythm_factor']:.3f}x, Score Differential: {result['score_differential']}"
                )

                return result
        except Exception as e:
            logger.error(f"Error generating 4Q prediction: {e}")
            return {
                "error": str(e),
                "predicted_q4_home": None,
                "predicted_q4_away": None,
                "predicted_final_home": None,
                "predicted_final_away": None,
                "confidence_level": "LOW",
                "confidence_range_numeric": 0,
            }


predictor_4q = Basketball4QPredictor()

__all__ = [
    "Basketball4QPredictor",
    "map_season_stage_to_db_round",
    "predictor_4q",
]
