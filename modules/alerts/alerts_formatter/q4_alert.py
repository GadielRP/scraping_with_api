"""Formatting helpers for the basketball 4Q alert."""

from typing import Dict


def create_q4_alert_message(
    event_id: int,
    home_team: str,
    away_team: str,
    competition: str,
    season_stage: str,
    current_home: int,
    current_away: int,
    q1_home: int,
    q2_home: int,
    q3_home: int,
    q1_away: int,
    q2_away: int,
    q3_away: int,
    prediction: Dict,
) -> str:
    """Build the Telegram message for the basketball 4Q alert."""
    message = f"🏀 <b>4th Quarter Alert - NBA</b>\n\n"
    message += f"🏆 <b>{home_team} vs {away_team}</b>\n"
    message += f"📅 Event ID: {event_id}\n"
    message += f"🏀 {competition} - {season_stage}\n\n"

    message += f"📊 <b>Current Score (Q1-Q3):</b>\n"
    message += f"{home_team}: {current_home} ({q1_home}-{q2_home}-{q3_home})\n"
    message += f"{away_team}: {current_away} ({q1_away}-{q2_away}-{q3_away})\n\n"

    message += f"⏰ <b>4th Quarter is LIVE!</b>\n\n"

    if prediction and not prediction.get("error"):
        message += f"📐 <b>Calculation Overview:</b>\n\n"
        message += f"<i>Step 1: Historical Analysis</i>\n"
        message += f"• Analyzed {prediction['parameters'].get('sample_size_home', 0)} games for {home_team}\n"
        message += f"• Analyzed {prediction['parameters'].get('sample_size_away', 0)} games for {away_team}\n"
        message += (
            f"• Historical Q4 avg: {home_team} {prediction['parameters']['avg_q4_home']:.1f} pts, "
            f"{away_team} {prediction['parameters']['avg_q4_away']:.1f} pts\n\n"
        )

        message += f"<i>Step 2: Rhythm Factor</i>\n"
        message += f"• Current Q1-Q3 total: {prediction['parameters']['total_q1_q3_combined_current']} pts\n"
        message += f"• Historical Q1-Q3 avg: {prediction['parameters']['avg_q1_q3_combined_historical']:.1f} pts\n"
        message += f"• Rhythm: {prediction['rhythm_factor']:.2f}x (how fast this game is vs historical)\n"
        message += (
            f"→ {prediction['rhythm_factor']:.2f}x means this game is "
            f"{('faster' if prediction['rhythm_factor'] > 1.0 else 'slower')} than average\n\n"
        )

        message += f"<i>Step 3: Base Q4 Prediction</i>\n"
        message += f"• Formula: Historical Q4 avg × Rhythm factor\n"
        message += (
            f"• {home_team}: {prediction['base_q4_home']:.1f} pts "
            f"({prediction['parameters']['avg_q4_home']:.1f} × {prediction['rhythm_factor']:.2f})\n"
        )
        message += (
            f"• {away_team}: {prediction['base_q4_away']:.1f} pts "
            f"({prediction['parameters']['avg_q4_away']:.1f} × {prediction['rhythm_factor']:.2f})\n\n"
        )

        if prediction['parameters']['momentum_applied']:
            message += f"<i>Step 4: Momentum Adjustment</i>\n"
            leader = home_team if prediction['score_differential'] > 0 else away_team
            trailing = away_team if prediction['score_differential'] > 0 else home_team
            message += f"• Score differential: {abs(prediction['score_differential'])} pts ({leader} leading)\n"
            message += f"• Adjustment: {leader} × 0.95 (slow down), {trailing} × 1.06 (speed up)\n"
            message += f"→ Leading teams tend to slow down, trailing teams push harder\n\n"

        message += f"🔮 <b>4Q Prediction:</b>\n"
        message += f"{home_team}: {prediction['predicted_q4_home']:.1f} pts\n"
        message += f"{away_team}: {prediction['predicted_q4_away']:.1f} pts\n\n"

        message += f"🎯 <b>Final Score Projection:</b>\n"
        message += f"{home_team}: {prediction['predicted_final_home']:.1f}\n"
        message += f"{away_team}: {prediction['predicted_final_away']:.1f}\n\n"

        confidence_emoji = {
            "HIGH": "🟢",
            "MEDIUM": "🟡",
            "LOW": "🔴",
        }.get(prediction["confidence_level"], "⚪")

        message += f"{confidence_emoji} <b>Confidence:</b> {prediction['confidence_level']}\n"
        message += f"• Range: {prediction['confidence_range_numeric']:.2f} (lower = more reliable)\n"
        message += f"• Z-Score: {prediction['z_score']:.3f} ({prediction['z_confidence']})\n"
        message += f"• Explosiveness: {prediction.get('explosiveness', 0):.2f} (volatility measure)\n"
    else:
        message += f"⚠️ Prediction unavailable (insufficient historical data)\n\n"

    message += f"\n🔔 <i>Perfect timing for set predictions!</i>"
    return message


__all__ = ["create_q4_alert_message"]
