"""Historical candidate lookup for Process 1."""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories import OddsRepository

logger = logging.getLogger(__name__)


@dataclass
class AlertMatch:
    """Represents a historical match with exactly identical odds variations."""

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
    sport: str = "Tennis"
    is_symmetrical: bool = True
    competition: str = "Unknown"
    var_diffs: Optional[Dict[str, float]] = None
    distance_l1: Optional[float] = None
    court_type: Optional[str] = None


class Process1CandidateSearch:
    """Search helpers for exact historical candidates used by Process 1."""

    def get_event_variations(self, event_id: int, event_odds=None) -> Optional[Tuple]:
        """Get variations for an event from event_odds."""
        try:
            odds = event_odds or OddsRepository.get_event_odds(event_id)
            if not odds:
                return None

            var_shape = odds.var_x is not None
            return (odds.var_one, odds.var_x, odds.var_two, var_shape)
        except Exception as e:
            logger.error(f"Error getting variations for event {event_id}: {e}")
            return None

    def get_event_sport(self, event_id: int) -> Optional[str]:
        """Get sport for an event from mv_alert_events."""
        try:
            with db_manager.get_session() as session:
                from sqlalchemy import text

                result = session.execute(
                    text("SELECT sport FROM mv_alert_events WHERE event_id = :event_id LIMIT 1"),
                    {"event_id": event_id},
                )
                row = result.fetchone()
                return row.sport if row else None
        except Exception as e:
            logger.error(f"Error getting sport for event {event_id}: {e}")
            return None

    def find_tier1_candidates(
        self,
        sport: str,
        gender: str,
        var_shape: bool,
        current_odds,
        exclude_event_ids: Optional[List[int]] = None,
        discovery_source: str = "dropping_odds",
    ) -> List[AlertMatch]:
        """Find historical events with exactly identical odds."""
        return self.find_candidates(
            sport=sport,
            gender=gender,
            var_shape=var_shape,
            current_odds=current_odds,
            is_exact=True,
            exclude_event_ids=exclude_event_ids,
            discovery_source=discovery_source,
        )

    def find_candidates(
        self,
        sport: str,
        gender: str,
        var_shape: bool,
        current_odds,
        is_exact: bool,
        exclude_event_ids: Optional[List[int]] = None,
        discovery_source: str = "dropping_odds",
    ) -> List[AlertMatch]:
        """Find historical events with exactly identical odds."""
        try:
            with db_manager.get_session() as session:
                from sqlalchemy import text

                search_type = "EXACTLY identical odds"
                logger.info(f"Searching for {search_type}...")
                logger.info(
                    f"Current odds: 1={current_odds.one_open}->{current_odds.one_final}, "
                    f"X={current_odds.x_open}->{current_odds.x_final if current_odds.x_open is not None else 'N/A'}, "
                    f"2={current_odds.two_open}->{current_odds.two_final}"
                )
                logger.info(
                    f"Filtering by sport='{sport}', gender='{gender}', and discovery_source='{discovery_source}'"
                )

                sql_query, params = self._build_candidate_sql(
                    sport=sport,
                    gender=gender,
                    var_shape=var_shape,
                    current_odds=current_odds,
                    is_exact=is_exact,
                    exclude_event_ids=exclude_event_ids,
                    discovery_source=discovery_source,
                )

                if exclude_event_ids:
                    logger.info(f"Excluding {len(exclude_event_ids)} event IDs: {exclude_event_ids}")

                result = session.execute(text(sql_query), params)
                candidates = result.fetchall()

                logger.info(f"Found {len(candidates)} candidates with {search_type.upper()}")

                matches = self._process_candidate_matches(candidates, sport=sport)

                if matches:
                    logger.info(f"SUCCESS: Found {len(matches)} {search_type.lower()} matches")
                else:
                    logger.info(f"No {search_type.lower()} matches found")

                return matches
        except Exception as e:
            logger.error(f"Error finding exact odds historical matches: {e}")
            return []

    def _build_candidate_sql(
        self,
        sport: str,
        gender: str,
        var_shape: bool,
        current_odds,
        is_exact: bool,
        exclude_event_ids: Optional[List[int]] = None,
        discovery_source: str = "dropping_odds",
    ) -> Tuple[str, Dict]:
        """Build SQL query and parameters for exact candidate search."""
        exclude_clause = ""
        if exclude_event_ids:
            exclude_ids_str = ",".join(map(str, exclude_event_ids))
            exclude_clause = f" AND mae.event_id NOT IN ({exclude_ids_str})"

        params = {
            "sport": sport,
            "gender": gender,
            "var_shape": var_shape,
            "discovery_source": discovery_source,
            "cur_one_open": current_odds.one_open,
            "cur_two_open": current_odds.two_open,
            "cur_one_final": current_odds.one_final,
            "cur_two_final": current_odds.two_final,
        }

        if var_shape:
            params.update(
                {
                    "cur_x_open": current_odds.x_open,
                    "cur_x_final": current_odds.x_final,
                }
            )
            odds_conditions = (
                "mae.one_open = :cur_one_open AND mae.two_open = :cur_two_open AND "
                "mae.one_final = :cur_one_final AND mae.two_final = :cur_two_final AND "
                "mae.x_open = :cur_x_open AND mae.x_final = :cur_x_final"
            )
        else:
            odds_conditions = (
                "mae.one_open = :cur_one_open AND mae.two_open = :cur_two_open AND "
                "mae.one_final = :cur_one_final AND mae.two_final = :cur_two_final AND "
                "mae.x_open IS NULL AND mae.x_final IS NULL"
            )

        sql = f"""
                    SELECT mae.event_id, mae.participants, mae.gender, mae.result_text, mae.winner_side, mae.point_diff,
                           mae.one_open, mae.x_open, mae.two_open, mae.one_final, mae.x_final, mae.two_final,
                           mae.var_one, mae.var_x, mae.var_two, mae.competition,
                           eo.observation_value as court_type
                    FROM mv_alert_events mae
                    LEFT JOIN event_observations eo ON mae.event_id = eo.event_id
                      AND eo.observation_type = 'ground_type'
                    WHERE mae.sport = :sport
                      AND mae.gender = :gender
                      AND mae.var_shape = :var_shape
                      AND mae.discovery_source = :discovery_source
                      AND {odds_conditions}{exclude_clause}
        """

        return sql, params

    def _process_candidate_matches(self, candidates, sport: str = "Tennis") -> List[AlertMatch]:
        """Convert candidate rows into AlertMatch objects."""
        matches = []

        for row in candidates:
            dx_display = f"{row.var_x:.2f}" if row.var_x is not None else "NULL"

            logger.info(
                f"EXACT MATCH: event_id={row.event_id} vars=(d1={row.var_one:.2f}, dx={dx_display}, d2={row.var_two:.2f}) "
                f"| result={row.result_text}, winner={row.winner_side}, point_diff={row.point_diff}"
            )

            matches.append(
                AlertMatch(
                    event_id=row.event_id,
                    participants=row.participants,
                    gender=getattr(row, "gender", "unknown"),
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
                    is_symmetrical=True,
                    competition=row.competition or "Unknown",
                    var_diffs=None,
                    distance_l1=None,
                    court_type=getattr(row, "court_type", None),
                )
            )

        return matches

    def filter_candidates_by_court_type(
        self,
        candidates: List[AlertMatch],
        current_court_type: Optional[str],
        sport: str,
    ) -> List[AlertMatch]:
        """
        Filter candidates by court type for Tennis/Tennis Doubles events.

        Returns all candidates if filtering is not applicable or fails.
        """
        try:
            if sport not in ["Tennis", "Tennis Doubles"]:
                logger.debug(f"Court type filtering not applicable for sport: {sport}")
                return candidates

            if not current_court_type:
                logger.info(f"[COURT] No court type provided for filtering - returning all {len(candidates)} candidates")
                return candidates

            filtered_candidates = [
                candidate
                for candidate in candidates
                if candidate.court_type == current_court_type
            ]

            filtered_count = len(filtered_candidates)
            original_count = len(candidates)
            removed_count = original_count - filtered_count

            if removed_count > 0:
                logger.info(
                    f"[COURT] Court type filter: '{current_court_type}' - kept {filtered_count}/{original_count} candidates "
                    f"({removed_count} filtered out)"
                )

                for candidate in candidates:
                    if candidate not in filtered_candidates:
                        logger.info(
                            f"   FILTERED OUT: {candidate.participants} (court: {candidate.court_type or 'Unknown'})"
                        )

                if filtered_candidates:
                    logger.info("[COURT] Candidates that passed the court type filter:")
                    for candidate in filtered_candidates:
                        logger.info(
                            f"   KEPT: {candidate.participants} (court: {candidate.court_type or 'Unknown'})"
                        )
            else:
                logger.info(f"[COURT] Court type filter: '{current_court_type}' - all {original_count} candidates match")

            return filtered_candidates
        except Exception as e:
            logger.warning(f"Error filtering candidates by court type: {e}")
            return candidates


__all__ = ["AlertMatch", "Process1CandidateSearch"]
