"""Process 2 engine."""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from infrastructure.persistence.repositories import DualProcessOddsRepository

from .sports import FootballFormulas

logger = logging.getLogger(__name__)


@dataclass
class FormulaResult:
    """Result from a formula activation."""

    formula_name: str
    winner_side: str
    point_diff: int
    variables_used: Optional[Dict] = None


@dataclass
class Process2Report:
    """Complete Process 2 evaluation report."""

    event_id: int
    sport: str
    participants: str
    variables_calculated: Dict
    activated_formulas: List[FormulaResult]
    primary_prediction: Optional[Tuple[str, int]]
    total_formulas_checked: int
    formulas_activated_count: int
    status: str


class Process2Engine:
    """Main Process 2 engine for sport-specific rule evaluation."""

    def __init__(self):
        logger.info("[PROCESS2] Engine initialized")

    @staticmethod
    def _get_normalized_event_parts(event) -> Tuple[str, str, str]:
        home_participant = event.__dict__.get("home_participant")
        away_participant = event.__dict__.get("away_participant")
        competition_ref = event.__dict__.get("competition_ref")

        if not home_participant or not away_participant or not competition_ref:
            raise ValueError(f"Missing normalized participants/competition for event_id={getattr(event, 'id', '?')}")

        return home_participant.name, away_participant.name, competition_ref.display_name

    def evaluate_event(self, event) -> Optional[Process2Report]:
        """Evaluate a single event using Process 2 sport-specific rules."""
        try:
            home_team, away_team, _competition_name = self._get_normalized_event_parts(event)
            sport = event.sport.lower()
            logger.info(
                "[PROCESS2] Evaluating event %s (%s vs %s) - Sport: %s",
                event.id,
                home_team,
                away_team,
                sport,
            )

            if sport == "football":
                return self._evaluate_football(event)

            logger.info("[PROCESS2] Sport '%s' not supported yet, skipping", sport)
            return None
        except Exception as e:
            logger.error("[PROCESS2] Error evaluating event %s: %s", getattr(event, "id", "?"), e)
            return None

    def _evaluate_football(self, event) -> Process2Report:
        """Evaluate football event using football-specific formulas."""
        try:
            event_odds = self._ensure_dual_process_odds_loaded(event)
            if not event_odds:
                logger.warning(
                    "[PROCESS2] No dual-process market odds found for event %s; expected market_name/group in "
                    "Config.MARKETS_DUAL_PROCESS, period in Config.PERIODS_DUAL_PROCESS, bookie_id=1.",
                    event.id,
                )
                return self._create_error_report(event, "No odds data available")

            var_one = float(event_odds.var_one or 0)
            var_x = float(event_odds.var_x or 0)
            var_two = float(event_odds.var_two or 0)

            logger.info(
                "[PROCESS2] Football variations: var_one=%.3f, var_x=%.3f, var_two=%.3f",
                var_one,
                var_x,
                var_two,
            )

            football_formulas = FootballFormulas(var_one, var_x, var_two)
            variables_calculated = {
                "var_one": var_one,
                "var_x": var_x,
                "var_two": var_two,
                "beta": football_formulas.beta,
                "zeta": football_formulas.zeta,
                "gamma": football_formulas.gamma,
                "delta": football_formulas.delta,
                "epsilon": football_formulas.epsilon,
                "β": football_formulas.beta,
                "ζ": football_formulas.zeta,
                "γ": football_formulas.gamma,
                "δ": football_formulas.delta,
                "ε": football_formulas.epsilon,
            }

            activated_formulas = []
            all_formulas = football_formulas.get_all_formulas()
            logger.info("[PROCESS2] Executing %s football formulas...", len(all_formulas))

            for formula_method in all_formulas:
                try:
                    result = formula_method()
                    if result:
                        activated_formulas.append(
                            FormulaResult(
                                formula_name=formula_method.__name__,
                                winner_side=result[0],
                                point_diff=result[1],
                                variables_used=variables_calculated,
                            )
                        )
                        logger.info("[PROCESS2] Formula %s activated: %s wins", formula_method.__name__, result[0])
                except Exception as e:
                    logger.error("[PROCESS2] Error executing formula %s: %s", formula_method.__name__, e)

            primary_prediction = self._determine_primary_prediction(activated_formulas)
            status = "success" if activated_formulas else "no_formulas_activated"

            if activated_formulas:
                logger.info(
                    "[PROCESS2] SUCCESS: %s formulas activated for event %s",
                    len(activated_formulas),
                    event.id,
                )
            else:
                logger.info("[PROCESS2] No formulas activated for event %s", event.id)

            return Process2Report(
                event_id=event.id,
                sport=event.sport,
                participants=f"{home_team} vs {away_team}",
                variables_calculated=variables_calculated,
                activated_formulas=activated_formulas,
                primary_prediction=primary_prediction,
                total_formulas_checked=len(all_formulas),
                formulas_activated_count=len(activated_formulas),
                status=status,
            )
        except Exception as e:
            logger.error("[PROCESS2] Error in football evaluation for event %s: %s", event.id, e)
            return self._create_error_report(event, str(e))

    def _ensure_dual_process_odds_loaded(self, event):
        """Load odds into the event object if they are missing."""
        if hasattr(event, "dual_process_odds") and event.dual_process_odds is not None:
            return event.dual_process_odds

        try:
            event.dual_process_odds = DualProcessOddsRepository.get_event_odds(event.id)
            return event.dual_process_odds
        except Exception as e:
            logger.error("[PROCESS2] Error loading dual-process market odds for %s: %s", event.id, e)
            return None

    def _determine_primary_prediction(self, activated_formulas: List[FormulaResult]) -> Optional[Tuple[str, int]]:
        """Determine primary prediction from activated formulas."""
        if not activated_formulas:
            return None

        votes = {}
        for formula_result in activated_formulas:
            votes.setdefault(formula_result.winner_side, []).append(formula_result)

        most_voted_winner = max(votes.keys(), key=lambda key: len(votes[key]))
        vote_count = len(votes[most_voted_winner])

        logger.info(
            "[PROCESS2] Primary prediction: %s with %s votes out of %s formulas",
            most_voted_winner,
            vote_count,
            len(activated_formulas),
        )
        return (most_voted_winner, 1)

    def _create_error_report(self, event, error_message: str) -> Process2Report:
        """Create error report for failed evaluation."""
        try:
            home_team, away_team, _competition_name = self._get_normalized_event_parts(event)
            participants = f"{home_team} vs {away_team}"
        except Exception:
            participants = f"Missing normalized participants for event_id={getattr(event, 'id', '?')}"

        return Process2Report(
            event_id=event.id,
            sport=event.sport if hasattr(event, "sport") else "Unknown",
            participants=participants,
            variables_calculated={},
            activated_formulas=[],
            primary_prediction=None,
            total_formulas_checked=0,
            formulas_activated_count=0,
            status=f"error: {error_message}",
        )

    def evaluate_multiple_events(self, events: List) -> List[Process2Report]:
        """Evaluate multiple events using Process 2."""
        reports = []

        for event in events:
            try:
                report = self.evaluate_event(event)
                if report:
                    reports.append(report)
            except Exception as e:
                logger.error("[PROCESS2] Error evaluating event %s in batch: %s", getattr(event, "id", "?"), e)

        logger.info(
            "[PROCESS2] Batch evaluation completed: %s reports generated from %s events",
            len(reports),
            len(events),
        )
        return reports


__all__ = ["FormulaResult", "Process2Engine", "Process2Report"]
