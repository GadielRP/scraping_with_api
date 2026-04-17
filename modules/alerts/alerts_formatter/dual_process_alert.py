"""Formatting helpers for Process 1 and Dual Process alerts."""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def create_candidate_report_message(report_data: Dict) -> str:
    """Create the Process 1 alert message."""
    status = report_data.get("status", "unknown")
    primary_prediction = report_data.get("primary_prediction")
    odds_display = report_data.get("odds_display", "Not available")
    vars_display = report_data.get("vars_display", "Not available")
    has_draw_odds = report_data.get("has_draw_odds", False)
    confidence = report_data.get("primary_confidence", "Not available")
    tier1_data = report_data.get("tier1_candidates", {})
    tier1_count = tier1_data.get("count", 0)

    status_headers = {
        "success": "✅ PROCESS 1 - SUCCESS",
        "partial": "⚠️ PROCESS 1 - PARTIAL",
        "no_match": "❌ PROCESS 1 - NO MATCH",
        "no_candidates": "❓ PROCESS 1 - NO  VALID CANDIDATES",
    }
    header = status_headers.get(status, "❓ PROCESS 1 - UNKNOWN STATUS")

    message = f"{header}\n\n"
    message += "📈 Current Variations:\n"
    message += f"{vars_display}\n\n"
    message += "💰 Current Odds:\n"
    message += f"{odds_display}\n\n"
    message += "🔍Summary:\n"
    message += f"Candidates (exact): {tier1_count}\n"
    message += f"Confidence: {confidence}\n\n"

    rule_activations = report_data.get("rule_activations", {})
    if rule_activations:
        message += _format_rule_activations(rule_activations)

    if tier1_count > 0:
        message += _format_tier_candidates(
            "🎯",
            "Exact Matches",
            tier1_count,
            tier1_data.get("matches", []),
            has_draw_odds,
        )

    if primary_prediction:
        message += f"🎯: {primary_prediction}\n"
    elif status == "partial":
        message += "⚠️ Need at least 2 candidates\n"
    else:
        message += "❌ No Prediction\n"

    return message


def _format_tier_candidates(icon: str, title: str, count: int, matches: List[Dict], has_draw_odds: bool) -> str:
    """Format tier candidates for display."""
    message = f"\n{icon} {title} ({count}):\n"

    for i, match in enumerate(matches, 1):
        var_display = _format_variations_display(match.get("variations", {}), has_draw_odds)
        competition_parts = match.get("competition", "Unknown").split(",")
        competition = competition_parts[-1].strip() if competition_parts else "Unknown"

        message += f"\n{i}. {match.get('participants', 'Unknown')} ({competition}):\n"
        message += f"R: {match.get('result_text', 'N/A')}\n"
        message += f"Open: {match.get('one_open', 'N/A')}, {match.get('x_open', 'N/A')}, {match.get('two_open', 'N/A')}\n"
        message += f"Final: {match.get('one_final', 'N/A')}, {match.get('x_final', 'N/A')}, {match.get('two_final', 'N/A')}\n"
        message += f"Δ: {var_display}\n"

        var_diffs = match.get("var_diffs")
        if var_diffs:
            diff_display = _format_variation_differences(var_diffs, has_draw_odds)
            message += f"Diff: {diff_display}\n"
            message += f"L1: {match.get('distance_l1', 'N/A')}\n"

        candidate_event_id = match.get("event_id")
        candidate_sport = match.get("sport")
        logger.info(
            "DEBUG: Processing candidate %s - event_id=%s, sport='%s'",
            i,
            candidate_event_id,
            candidate_sport,
        )

        from sport_observations import sport_observations_manager

        sport_info = sport_observations_manager.format_sport_info_for_candidates(
            candidate_event_id,
            candidate_sport,
        )
        if sport_info:
            message += f"{sport_info}\n"

    return message + "\n"


def _format_variations_display(variations: Dict, has_draw_odds: bool) -> str:
    """Format variations display based on sport type."""
    var_one = variations.get("var_one", "N/A")
    var_x = variations.get("var_x")
    var_two = variations.get("var_two", "N/A")

    var_display = f"Δ1: {var_one}"
    if has_draw_odds:
        var_display += f", ΔX: {var_x:.2f}" if var_x is not None else ", ΔX: N/A"
    var_display += f", Δ2: {var_two}"
    return var_display


def _format_variation_differences(var_diffs: Dict, has_draw_odds: bool) -> str:
    """Format variation differences display based on sport type."""
    d1_diff = var_diffs.get("d1", 0)
    d2_diff = var_diffs.get("d2", 0)
    dx_diff = var_diffs.get("dx")

    diff_display = f"Δ1: {d1_diff:+.3f}"
    if has_draw_odds and dx_diff is not None:
        diff_display += f", ΔX: {dx_diff:+.3f}"
    diff_display += f", Δ2: {d2_diff:+.3f}"
    return diff_display


def _format_rule_activations(rule_activations: Dict) -> str:
    """Format rule activations for display."""
    if not rule_activations:
        return ""

    message = "📋 Rule Activations:\n"
    rule_descriptions = {
        "A": "Identical Results",
        "B": "Similar Results",
        "C": "Same Winning Side",
    }

    for tier, activation in rule_activations.items():
        count = activation["count"]
        weight = activation["weight"]
        description = rule_descriptions.get(tier, f"Tier {tier}")
        message += f"Tier {tier} ({description}): {count} candidates (weight: {weight})\n"
        for candidate in activation["candidates"]:
            message += f" - {candidate['participants']} → {candidate['result_text']}\n"

    message += "\n"
    return message


def create_dual_process_message(dual_report) -> str:
    """Create Telegram message for a Dual Process report."""
    try:
        verdict_value = getattr(dual_report.verdict, "value", dual_report.verdict)
        verdict_key = str(verdict_value).upper()
        verdict_headers = {
            "AGREE": "✅ DUAL PROCESS - AGREEMENT",
            "DISAGREE": "⚔️ DUAL PROCESS - DISAGREEMENT",
            "PARTIAL": "⚠️ DUAL PROCESS - PARTIAL RESULT",
            "ERROR": "❌ DUAL PROCESS - ERROR",
        }

        header = verdict_headers.get(verdict_key, "❓ DUAL PROCESS - UNKNOWN")
        message = f"{header}\n"
        discovery_source = getattr(dual_report.discovery_source, "value", dual_report.discovery_source)
        message += f"🏆 {dual_report.event_id} {dual_report.participants}\n"
        message += f"🔍 {str(discovery_source).title().replace('_', ' ')}\n"

        competition = "Unknown"
        if getattr(dual_report, "process1_report", None):
            competition = dual_report.process1_report.get("competition", "Unknown")

        sport = str(dual_report.sport)
        if sport == "Football":
            message += f"⚽({competition})"
        elif sport == "Basketball":
            message += f"🏀({competition})"
        elif sport == "Tennis":
            message += f"🎾({competition})"
        elif sport == "Hockey":
            message += f"🏒({competition})"
        elif sport == "Baseball":
            message += f"⚾({competition})"
        elif sport == "Handball":
            message += f"🤾 {sport} ({competition})"
        elif sport == "Rugby":
            message += f"🏉({competition})"
        elif sport == "American Football":
            message += f"🏈({competition})"
        elif sport == "Volleyball":
            message += f"🏐({competition})"
        else:
            message += f"🏟️ {sport} ({competition})"

        minutes_until_start = getattr(dual_report, "minutes_until_start", None)
        if minutes_until_start is not None and minutes_until_start == 0:
            message += "\n🕔 Event is startig now!"
        elif minutes_until_start is not None and minutes_until_start < 0:
            message += "\n🕔 Event is Live!"
        elif minutes_until_start is not None:
            message += f"\n🕔 {minutes_until_start} min."

        if sport in ["Tennis", "Tennis Doubles"] and getattr(dual_report, "court_type", None):
            message += f"\n📢Obs: {dual_report.court_type}"

        message += "\n\n"

        if getattr(dual_report, "process1_report", None):
            process1_message = create_candidate_report_message(dual_report.process1_report)
            for line in process1_message.split("\n"):
                if line.strip():
                    message += f"{line}\n"
                else:
                    message += "\n"
        else:
            message += f"❌ No Process 1 report available ({dual_report.process1_status})\n"

        message += "\n🧪 Process 2 (Sport Formulas):\n"
        if getattr(dual_report, "process2_prediction", None):
            p2_winner = dual_report.process2_prediction[0]
            winner_text = {"1": "Home", "X": "Draw", "2": "Away"}.get(p2_winner, p2_winner)
            if winner_text == "Draw":
                message += f" ✅ Prediction: {winner_text}\n"
            else:
                message += f" ✅ Prediction: {winner_text} wins\n"

            if getattr(dual_report, "process2_report", None):
                variables = dual_report.process2_report.get("variables_calculated", {})
                if variables:
                    message += (
                        f" 📊 Variables: β={variables.get('β', 0):.3f}, "
                        f"ζ={variables.get('ζ', 0):.3f}, γ={variables.get('γ', 0):.3f}\n"
                    )
                    message += (
                        f" 📊 Variables: δ={variables.get('δ', 0):.3f}, "
                        f"ε={variables.get('ε', 0):.3f}\n"
                    )

                if "activated_formulas" in dual_report.process2_report:
                    formulas = dual_report.process2_report["activated_formulas"]
                    message += f" 📋 Formulas activated: {len(formulas)}\n"
                    for formula in formulas:
                        formula_name = formula.get("formula_name", "Unknown")
                        winner_side = formula.get("winner_side", "?")
                        point_diff = formula.get("point_diff", 0)
                        clean_name = formula_name.replace("formula_", "").replace("_", " ").title()
                        winner_text = {"1": "Home", "X": "Draw", "2": "Away"}.get(winner_side, winner_side)
                        message += f"{clean_name}: {winner_text} wins (diff: {point_diff})\n"

                total_formulas = dual_report.process2_report.get("total_formulas_checked", 0)
                activated_count = dual_report.process2_report.get("formulas_activated_count", 0)
                message += f"🧮 Formulas checked: {activated_count}/{total_formulas}\n"
        else:
            message += f"❌ No prediction ({dual_report.process2_status})\n"

        if getattr(dual_report, "final_prediction", None):
            final_winner = dual_report.final_prediction[0]
            winner_text = {"1": "Home", "X": "Draw", "2": "Away"}.get(final_winner, final_winner)
            if winner_text == "Draw":
                message += f"🏆 Final Prediction: {winner_text}\n"
            else:
                message += f"🏆 Final Prediction: {winner_text} wins\n"

        return message
    except Exception as e:
        logger.error("Error creating dual process message: %s", e)
        return f"❌ Error creating dual process message for event {dual_report.event_id}: {str(e)}"


def send_dual_process_alerts(notifier: Any, dual_reports: List) -> bool:
    """Send dual process alerts via Telegram."""
    if not dual_reports:
        return True

    success_count = 0

    try:
        from infrastructure.settings import Config
        from oddsportal_config import SEASON_ODDSPORTAL_MAP
        from infrastructure.persistence.repositories import EventRepository
    except ImportError:
        Config = None
        EventRepository = None

    for dual_report in dual_reports:
        if Config and Config.FILTER_ALERTS_BY_OP_SEASON:
            event = EventRepository.get_event_by_id(dual_report.event_id)
            if not event or event.season_id not in SEASON_ODDSPORTAL_MAP:
                logger.debug(
                    "Skipping dual process alert for event %s due to OP season filter.",
                    dual_report.event_id,
                )
                continue

        if dual_report.process1_status != "success":
            logger.info(
                "Skipping dual process alert for event %s because Process 1 status is not success (%s)",
                dual_report.event_id,
                dual_report.process1_status,
            )
            continue

        try:
            message = create_dual_process_message(dual_report)
            sent = notifier.send_telegram_message(message)
            if sent:
                success_count += 1
                verdict_value = getattr(dual_report.verdict, "value", dual_report.verdict)
                logger.info(
                    "Dual process alert sent for event %s: %s",
                    dual_report.event_id,
                    verdict_value,
                )
            else:
                logger.warning("💔 Failed to send dual process alert for event, process 1 did not succeed %s", dual_report.event_id)
        except Exception as e:
            logger.error("Error sending dual process alert for event %s: %s", dual_report.event_id, e)
            continue

    logger.info("Sent %s/%s dual process alerts successfully", success_count, len(dual_reports))
    return success_count > 0


__all__ = [
    "create_candidate_report_message",
    "create_dual_process_message",
    "send_dual_process_alerts",
]
