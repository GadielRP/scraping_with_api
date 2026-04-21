"""Dual-process orchestration flow."""

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple

from modules.alerts.dual_process.process_1 import alert_engine
from modules.alerts.dual_process.process_2 import Process2Engine

logger = logging.getLogger(__name__)


class ComparisonVerdict(Enum):
    """Comparison verdict between Process 1️⃣ and Process 2️⃣."""

    AGREE = "AGREE"
    DISAGREE = "DISAGREE"
    PARTIAL = "PARTIAL"
    ERROR = "ERROR"


@dataclass
class DualProcessReport:
    """Combined report from both processes."""

    event_id: int
    participants: str
    sport: str
    discovery_source: str
    process1_report: Optional[Dict]
    process1_prediction: Optional[Tuple[str, int]]
    process1_status: str
    process2_report: Optional[Dict]
    process2_prediction: Optional[Tuple[str, int]]
    process2_status: str
    verdict: ComparisonVerdict
    final_prediction: Optional[Tuple[str, int]]
    agreement_details: str
    minutes_until_start: Optional[int]
    timestamp: str
    court_type: Optional[str] = None


class DualProcessRunner:
    """Orchestrator for Process 1️⃣ plus Process 2️⃣."""

    def __init__(self):
        logger.info("[DUAL PROCESS] Runner initialized")

    def evaluate_dual_process(self, event, minutes_until_start: int = None) -> DualProcessReport:
        """Execute both processes and compare their results."""
        try:
            logger.info(
                "[DUAL PROCESS] Evaluation starting for event %s (%s vs %s)",
                event.id,
                event.home_team,
                event.away_team,
            )

            process1_report, process1_prediction, process1_status = self._execute_process1(event, minutes_until_start)
            process2_report, process2_prediction, process2_status = self._execute_process2(event)
            verdict, final_prediction, agreement_details = self._compare_predictions(
                process1_prediction,
                process2_prediction,
            )

            dual_report = DualProcessReport(
                event_id=event.id,
                participants=f"{event.home_team} vs {event.away_team}",
                sport=event.sport,
                discovery_source=event.discovery_source,
                court_type=getattr(event, "court_type", None),
                process1_report=process1_report,
                process1_prediction=process1_prediction,
                process1_status=process1_status,
                process2_report=process2_report,
                process2_prediction=process2_prediction,
                process2_status=process2_status,
                verdict=verdict,
                final_prediction=final_prediction,
                agreement_details=agreement_details,
                minutes_until_start=minutes_until_start,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

            logger.info("[DUAL PROCESS] Evaluation completed for event %s: %s", event.id, verdict.value)
            return dual_report
        except Exception as e:
            logger.error("[DUAL PROCESS] Error evaluating event %s: %s", getattr(event, "id", "?"), e)
            return self._create_error_report(event, str(e), minutes_until_start)

    def _execute_process1(self, event, minutes_until_start: int) -> Tuple[Optional[Dict], Optional[Tuple[str, int]], str]:
        """Execute Process 1️⃣."""
        try:
            logger.info("[DUAL PROCESS] Executing Process 1️⃣️⃣ for event %s", event.id)
            alerts = alert_engine.evaluate_single_event(event, minutes_until_start)

            if alerts and len(alerts) > 0:
                alert_report = alerts[0]
                primary_prediction = alert_report.get("primary_prediction")
                status = alert_report.get("status", "unknown")

                if primary_prediction and status == "success":
                    winner_side = self.extract_winner_from_process1_prediction(primary_prediction)
                    if winner_side:
                        process1_prediction = (winner_side, 1)
                        logger.info("[DUAL PROCESS] Process 1️⃣️⃣ prediction: %s", winner_side)
                        return alert_report, process1_prediction, "success"

                logger.info("[DUAL PROCESS] Process 1️⃣️⃣ report generated without clear prediction (status: %s)", status)
                return alert_report, None, status

            logger.info("[DUAL PROCESS] Process 1️⃣️⃣ found no candidates for event %s", event.id)
            return None, None, "no_candidates"
        except Exception as e:
            logger.error("[DUAL PROCESS] Error executing Process 1️⃣️⃣ for event %s: %s", event.id, e)
            return None, None, f"error: {str(e)}"

    def _execute_process2(self, event) -> Tuple[Optional[Dict], Optional[Tuple[str, int]], str]:
        """Execute Process 2️⃣."""
        try:
            logger.info("[DUAL PROCESS] Executing Process 2️⃣ for event %s", event.id)

            process2_engine = Process2Engine()
            process2_report = process2_engine.evaluate_event(event)

            if process2_report:
                report_dict = {
                    "event_id": process2_report.event_id,
                    "sport": process2_report.sport,
                    "participants": process2_report.participants,
                    "variables_calculated": process2_report.variables_calculated,
                    "activated_formulas": [
                        {
                            "formula_name": formula.formula_name,
                            "winner_side": formula.winner_side,
                            "point_diff": formula.point_diff,
                        }
                        for formula in process2_report.activated_formulas
                    ],
                    "primary_prediction": process2_report.primary_prediction,
                    "total_formulas_checked": process2_report.total_formulas_checked,
                    "formulas_activated_count": process2_report.formulas_activated_count,
                    "status": process2_report.status,
                }

                prediction = process2_report.primary_prediction
                status = process2_report.status

                if prediction and status == "success":
                    logger.info("[DUAL PROCESS] Process 2️⃣ prediction: %s", prediction[0])
                    return report_dict, prediction, "success"

                logger.info("[DUAL PROCESS] Process 2️⃣ produced no final prediction (status: %s)", status)
                return report_dict, None, status

            logger.info("[DUAL PROCESS] Process 2️⃣ returned no report for event %s", event.id)
            return None, None, "no_report"
        except Exception as e:
            logger.error("[DUAL PROCESS] Error executing Process 2️⃣ for event %s: %s", event.id, e)
            return None, None, f"error: {str(e)}"

    def _compare_predictions(
        self,
        process1_prediction: Optional[Tuple[str, int]],
        process2_prediction: Optional[Tuple[str, int]],
    ) -> Tuple[ComparisonVerdict, Optional[Tuple[str, int]], str]:
        """Compare predictions from both processes."""
        try:
            if process1_prediction and process2_prediction:
                p1_winner, _ = process1_prediction
                p2_winner, _ = process2_prediction

                if p1_winner == p2_winner:
                    logger.info("[DUAL PROCESS] AGREEMENT: both processes predict %s", p1_winner)
                    return (
                        ComparisonVerdict.AGREE,
                        process1_prediction,
                        f"Both processes agree: {p1_winner} wins",
                    )

                logger.info(
                    "[DUAL PROCESS] DISAGREEMENT: Process 1️⃣ predicts %s, Process 2️⃣ predicts %s",
                    p1_winner,
                    p2_winner,
                )
                return (
                    ComparisonVerdict.DISAGREE,
                    None,
                    f"Disagreement: Process 1️⃣ predicts {p1_winner}, Process 2️⃣ predicts {p2_winner}",
                )

            if process1_prediction and not process2_prediction:
                logger.info("[DUAL PROCESS] PARTIAL: only Process 1️⃣ has prediction %s", process1_prediction[0])
                return (
                    ComparisonVerdict.PARTIAL,
                    process1_prediction,
                    "Only Process 1️⃣ generated prediction",
                )

            if not process1_prediction and process2_prediction:
                logger.info("[DUAL PROCESS] PARTIAL: only Process 2️⃣ has prediction %s", process2_prediction[0])
                return (
                    ComparisonVerdict.PARTIAL,
                    process2_prediction,
                    "Only Process 2️⃣ generated prediction",
                )

            logger.info("[DUAL PROCESS] Neither process generated a prediction")
            return (
                ComparisonVerdict.PARTIAL,
                None,
                "Neither process generated prediction",
            )
        except Exception as e:
            logger.error("[DUAL PROCESS] Error comparing predictions: %s", e)
            return (
                ComparisonVerdict.ERROR,
                None,
                f"Error in comparison: {str(e)}",
            )

    def extract_winner_from_process1_prediction(self, prediction_text: str) -> Optional[str]:
        """Extract winner side from Process 1️⃣ prediction text."""
        try:
            if not prediction_text:
                return None

            prediction_lower = prediction_text.lower()

            if prediction_lower == "draw":
                return "X"
            if prediction_lower.startswith("home"):
                return "1"
            if prediction_lower.startswith("away"):
                return "2"
            if "draw" in prediction_lower or "empate" in prediction_lower:
                return "X"
            if "home" in prediction_lower or "local" in prediction_lower:
                return "1"
            if "away" in prediction_lower or "visita" in prediction_lower:
                return "2"
            if "wins" in prediction_lower:
                if prediction_lower.startswith("home"):
                    return "1"
                if prediction_lower.startswith("away"):
                    return "2"

            logger.warning("[DUAL PROCESS] Could not extract winner from prediction: '%s'", prediction_text)
            return None
        except Exception as e:
            logger.error("[DUAL PROCESS] Error extracting winner from prediction '%s': %s", prediction_text, e)
            return None

    def _create_error_report(self, event, error_message: str, minutes_until_start: int = None) -> DualProcessReport:
        """Create error report for failed dual-process evaluation."""
        return DualProcessReport(
            event_id=event.id if hasattr(event, "id") else 0,
            participants=f"{getattr(event, 'home_team', '?')} vs {getattr(event, 'away_team', '?')}",
            sport=getattr(event, "sport", "Unknown"),
            discovery_source=getattr(event, "discovery_source", "unknown"),
            court_type=getattr(event, "court_type", None),
            process1_report=None,
            process1_prediction=None,
            process1_status="error",
            process2_report=None,
            process2_prediction=None,
            process2_status="error",
            verdict=ComparisonVerdict.ERROR,
            final_prediction=None,
            agreement_details=f"Error: {error_message}",
            minutes_until_start=minutes_until_start,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

    def send_alerts(self, notifier, dual_reports: List[DualProcessReport]) -> bool:
        """Send dual-process alerts through the formatter layer."""
        try:
            from modules.alerts.alerts_formatter.dual_process_alert import send_dual_process_alerts

            return send_dual_process_alerts(notifier, dual_reports)
        except Exception as e:
            logger.error("[DUAL PROCESS] Error sending dual-process alerts: %s", e)
            return False

    def log_process1_prediction_if_needed(self, event, dual_report: DualProcessReport, minutes_until_start: int) -> bool:
        """Log Process 1️⃣ predictions when the successful report is sent at minute 0."""
        try:
            if not dual_report or not dual_report.process1_report:
                return False
            if dual_report.process1_report.get("status") != "success":
                return False
            if minutes_until_start != 0:
                return False

            from modules.prediction import prediction_logger

            return prediction_logger.log_prediction(event, dual_report.process1_report)
        except Exception as e:
            logger.error(
                "[DUAL PROCESS] Error logging Process 1️⃣ prediction for event %s: %s",
                getattr(event, "id", "?"),
                e,
            )
            return False


prediction_engine = DualProcessRunner()

__all__ = [
    "ComparisonVerdict",
    "DualProcessReport",
    "DualProcessRunner",
    "prediction_engine",
]
