"""Process 1 engine orchestration."""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from infrastructure.persistence.repositories import DualProcessOddsRepository
from modules.alerts import pre_start_notifier
from modules.alerts.alerts_formatter.dual_process_alert import create_candidate_report_message

from .candidate_search import AlertMatch, Process1CandidateSearch
from .evaluator import MIN_SAMPLES, Process1Evaluator

logger = logging.getLogger(__name__)


class AlertEngine:
    """Main Process 1 engine for pattern-based predictions."""

    def __init__(
        self,
        candidate_search: Optional[Process1CandidateSearch] = None,
        evaluator: Optional[Process1Evaluator] = None,
    ):
        self.MIN_SAMPLES = MIN_SAMPLES
        self.candidate_search = candidate_search or Process1CandidateSearch()
        self.evaluator = evaluator or Process1Evaluator()

    @staticmethod
    def _get_normalized_event_parts(event, event_context=None) -> Tuple[str, str, str]:
        home_participant = event.__dict__.get("home_participant")
        away_participant = event.__dict__.get("away_participant")
        competition_ref = event.__dict__.get("competition_ref")

        home_team = None
        away_team = None
        competition_name = None

        if event_context is not None:
            home_team = getattr(getattr(event_context, "home", None), "name", None)
            away_team = getattr(getattr(event_context, "away", None), "name", None)
            competition_name = getattr(getattr(event_context, "competition", None), "display_name", None) or getattr(
                getattr(event_context, "competition", None),
                "canonical_name",
                None,
            )

        if home_team is None and home_participant is not None:
            home_team = getattr(home_participant, "name", None)
        if away_team is None and away_participant is not None:
            away_team = getattr(away_participant, "name", None)
        if competition_name is None and competition_ref is not None:
            competition_name = getattr(competition_ref, "display_name", None) or getattr(competition_ref, "canonical_name", None)

        home_team = home_team or getattr(event, "home_team", None)
        away_team = away_team or getattr(event, "away_team", None)
        competition_name = competition_name or getattr(event, "competition", None)

        if not home_team or not away_team or not competition_name:
            raise ValueError(f"Missing normalized participants/competition for event_id={getattr(event, 'id', '?')}")

        return home_team, away_team, competition_name

    def evaluate_upcoming_events(self, upcoming_events: List) -> List[Dict]:
        """Evaluate all upcoming events for Process 1 alerts."""
        alerts = []

        for event in upcoming_events:
            try:
                now = datetime.now()
                time_diff = event.start_time_utc - now
                minutes_until_start = round(time_diff.total_seconds() / 60)

                event_alerts = self.evaluate_single_event(event, minutes_until_start)
                alerts.extend(event_alerts)
            except Exception as e:
                logger.error(f"Error evaluating event {event.id}: {e}")
                continue

        return alerts

    def evaluate_single_event(self, event, minutes_until_start: int = None, event_context=None) -> List[Dict]:
        """Evaluate a single event and return candidate reports."""
        event_odds = self._ensure_dual_process_odds_loaded(event)
        if not event_odds:
            logger.info(
                "No dual-process market odds found for event %s; expected market_name/group in "
                "Config.MARKETS_DUAL_PROCESS, period in Config.PERIODS_DUAL_PROCESS, bookie_id=1.",
                event.id,
            )
            return []

        current_vars = self.candidate_search.get_event_variations(event.id, event_odds=event_odds)
        if not current_vars:
            logger.debug(f"No variations found for event {event.id}")
            return []

        cur_v1, cur_vx, cur_v2, var_shape = current_vars
        cur_v1 = float(cur_v1 or 0)
        cur_vx = float(cur_vx) if cur_vx is not None else None
        cur_v2 = float(cur_v2 or 0)

        try:
            home_team, away_team, competition_name = self._get_normalized_event_parts(event, event_context=event_context)
            participants = f"{home_team} vs {away_team}"
        except Exception:
            logger.warning("Missing normalized participants/competition for event %s", event.id)
            return []

        logger.info(
            f"[P1] Event {event.id} ({participants}) vars: d1={cur_v1:.2f}, dx={(cur_vx if cur_vx is not None else 0):.2f}, "
            f"d2={cur_v2:.2f}, shape={'3-way' if var_shape else 'no-draw'}"
        )

        tier1_candidates = self.candidate_search.find_tier1_candidates(
            sport=event.sport,
            gender=event.gender,
            var_shape=var_shape,
            current_odds=event_odds,
            exclude_event_ids=[event.id],
            discovery_source="dropping_odds",
        )

        logger.info(f"Found {len(tier1_candidates)} exact candidates for event {event.id}")

        current_court_type = getattr(event, "court_type", None)
        if current_court_type and event.sport in ["Tennis", "Tennis Doubles"]:
            logger.info(f"[COURT] Applying court type filter for {event.sport}: '{current_court_type}'")
            tier1_candidates = self.candidate_search.filter_candidates_by_court_type(
                candidates=tier1_candidates,
                current_court_type=current_court_type,
                sport=event.sport,
            )
            logger.info(f"After court type filter: {len(tier1_candidates)} candidates")

        if tier1_candidates:
            candidate_report = self._create_candidate_report(
                event=event,
                tier1_candidates=tier1_candidates,
                current_vars=(cur_v1, cur_vx, cur_v2),
                minutes_until_start=minutes_until_start,
                participants=participants,
                competition_name=competition_name,
            )
            return [candidate_report]

        logger.info(f"No candidates found for event {event.id}")
        return []

    def _ensure_dual_process_odds_loaded(self, event):
        """Load odds into the event object if they are missing."""
        if hasattr(event, "dual_process_odds") and event.dual_process_odds is not None:
            return event.dual_process_odds

        try:
            event.dual_process_odds = DualProcessOddsRepository.get_event_odds(event.id)
            return event.dual_process_odds
        except Exception as e:
            logger.error(f"Error loading dual-process market odds for {event.id}: {e}")
            return None

    def _create_candidate_report(
        self,
        event,
        tier1_candidates: List[AlertMatch],
        current_vars: Tuple,
        minutes_until_start: int = None,
        participants: str = "",
        competition_name: str = "",
    ) -> Dict:
        """Create the report payload consumed by Process 1 and Dual Process formatters."""
        cur_v1, cur_vx, cur_v2 = current_vars

        vars_display = f"D1: {cur_v1:.2f}"
        if cur_vx is not None:
            vars_display += f", DX: {cur_vx:.2f}"
        vars_display += f", D2: {cur_v2:.2f}"

        event_odds = self._ensure_dual_process_odds_loaded(event)
        odds_display = f"1: {event_odds.one_open}->{event_odds.one_final}"
        if event_odds.x_open and event_odds.x_final:
            odds_display += f", X: {event_odds.x_open}->{event_odds.x_final}"
        odds_display += f", 2: {event_odds.two_open}->{event_odds.two_final}"

        evaluation_result = self.evaluator.evaluate_candidates_with_new_logic(tier1_candidates)
        tier1_matches_data = self._format_candidate_data(tier1_candidates)

        return {
            "event_id": event.id,
            "rule_key": f"candidate_report_{event.id}",
            "participants": participants,
            "competition": competition_name,
            "sport": event.sport,
            "discovery_source": event.discovery_source,
            "start_time": event.start_time_utc.strftime("%H:%M"),
            "minutes_until_start": minutes_until_start,
            "odds_display": odds_display,
            "vars_display": vars_display,
            "has_draw_odds": cur_vx is not None,
            "status": evaluation_result["status"],
            "selected_tier": evaluation_result["selected_tier"],
            "primary_prediction": (
                evaluation_result["prediction"].prediction if evaluation_result["prediction"] else None
            ),
            "primary_confidence": f"{evaluation_result['confidence']:.1f}%",
            "successful_candidates": evaluation_result["successful_candidates"],
            "total_candidates": evaluation_result["total_candidates"],
            "rule_activations": evaluation_result["rule_activations"],
            "tier1_candidates": {
                "count": len(tier1_candidates),
                "matches": tier1_matches_data,
            },
            "tier2_candidates": {
                "count": 0,
                "matches": [],
            },
        }

    def _format_candidate_data(self, candidates: List[AlertMatch]) -> List[Dict]:
        """Format candidate matches for display."""
        return [
            {
                "event_id": match.event_id,
                "sport": match.sport,
                "participants": match.participants,
                "result_text": match.result_text,
                "is_symmetrical": match.is_symmetrical,
                "competition": match.competition,
                "court_type": match.court_type,
                "variations": {
                    "var_one": match.var_one,
                    "var_x": match.var_x,
                    "var_two": match.var_two,
                },
                "var_diffs": match.var_diffs,
                "distance_l1": match.distance_l1,
                "one_open": match.one_open,
                "x_open": match.x_open,
                "two_open": match.two_open,
                "one_final": match.one_final,
                "x_final": match.x_final,
                "two_final": match.two_final,
            }
            for match in candidates
        ]

    def send_alerts(self, alerts: List[Dict]) -> bool:
        """Send alerts via Telegram and log them."""
        if not alerts:
            return True

        success_count = 0

        for alert in alerts:
            try:
                message = create_candidate_report_message(alert)
                sent = pre_start_notifier.send_telegram_message(message)

                if sent:
                    success_count += 1
                    logger.info(f"Alert sent: {alert['participants']} - {alert.get('primary_prediction', 'N/A')}")
                else:
                    logger.warning(f"Failed to send alert for event {alert['event_id']}")
            except Exception as e:
                logger.error(f"Error sending alert for event {alert['event_id']}: {e}")
                continue

        logger.info(f"Sent {success_count}/{len(alerts)} alerts successfully")
        return success_count > 0


alert_engine = AlertEngine()

__all__ = ["AlertEngine", "alert_engine"]
