"""Odds alerts rendered exclusively from committed canonical quote state."""
from __future__ import annotations

import logging
from collections import defaultdict
from html import escape
from typing import Sequence

from infrastructure.persistence.repositories import EventRepository
from infrastructure.persistence.repositories.market.market_odds_read_repository import MarketOddsReadRepository
from infrastructure.persistence.repositories.market.market_odds_read_models import ChoiceOddsState, MarketOddsState
from modules.alerts import pre_start_notifier
from modules.competition.tracked_competitions import is_tracked_competition

logger = logging.getLogger(__name__)
ODDS_ALERT_ENABLED = True
ALLOWED_ODDS_ALERT_MINUTES = {30, 5}


def send_odds_alert(event_data: dict, minutes_until_start: int | None = None) -> bool:
    """Read committed quotes, apply alert eligibility, render and dispatch."""
    if not ODDS_ALERT_ENABLED or minutes_until_start not in ALLOWED_ODDS_ALERT_MINUTES:
        return False
    try:
        result = MarketOddsReadRepository().get_market_odds_state(event_data["id"])
        if not result.markets or result.has_blocking_diagnostics:
            return False
        if (len(result.markets) == 1
                and result.markets[0].canonical_market_key == "1x2_full_time"
                and not is_tracked_competition(event_data.get("competition_id"))):
            EventRepository.mark_event_as_alerted(event_data["id"])
            return False
        message = create_odds_alert_message(event_data, result.markets, minutes_until_start)
        if not pre_start_notifier.telegram_enabled:
            return False
        return bool(pre_start_notifier.send_telegram_message(message))
    except Exception:
        logger.exception("Failed to read or send odds alert event_id=%s", event_data.get("id"))
        return False


def create_odds_alert_message(
    event_data: dict, markets: Sequence[MarketOddsState], minutes_until_start: int | None = None,
) -> str:
    """Format resolved values; source selection and persistence belong upstream."""
    sport_emojis = {
        "Football": "⚽", "Basketball": "🏀", "Tennis": "🎾",
        "Hockey": "🏒", "Ice hockey": "🏒", "Baseball": "⚾", "Handball": "🤼",
        "Rugby": "🏉", "American Football": "🏈", "American football": "🏈",
        "Volleyball": "🏐", "Tennis doubles": "🎾",
    }
    sport_emoji = sport_emojis.get(event_data.get("sport"), "🏟️")
    message = "📊 <b>ODDS ALERT</b>\n\n"
    message += f"{sport_emoji} <b>{escape(event_data.get('home_team', 'Unknown'))} vs {escape(event_data.get('away_team', 'Unknown'))}</b>\n"
    if event_data.get("competition"):
        message += f"🏆 {escape(event_data['competition'])}\n"
    if event_data.get("discovery_source"):
        formatted_source = event_data["discovery_source"].title().replace("_", " ")
        message += f"🔍 {escape(formatted_source)}\n"
    if minutes_until_start is not None:
        timing = "Event is Live!" if minutes_until_start < 0 else (
            "Event is starting now!" if minutes_until_start == 0 else f"{minutes_until_start} min until start")
        message += f"🕒 <b>{timing}</b>\n"
    message += f"🆔 Event: {event_data.get('id', 'Unknown')}\n\n"
    return message + format_market_odds(markets)


def _format_odds_value(value) -> str:
    if value is None:
        return "N/A"
    rendered = f"{value:.3f}"
    return rendered[:-1] if rendered.endswith("0") else rendered


def _format_choice(choice: ChoiceOddsState) -> str:
    if choice.opening is not None and choice.current is not None:
        movement = {-1: "↓", 0: "=", 1: "↑"}[choice.movement]
        return f"{_format_odds_value(choice.opening.value)}→{_format_odds_value(choice.current.value)}{movement}"
    if choice.opening is not None:
        return f"{_format_odds_value(choice.opening.value)}→N/A"
    return _format_odds_value(choice.current.value if choice.current else None)


def format_market_odds(markets: Sequence[MarketOddsState]) -> str:
    """Render all bookmakers with one typed contract, including exchange sides."""
    sections = defaultdict(list)
    for market in markets:
        sections[(market.market_name, market.market_period)].append(market)
    result = ""
    for (name, _period), section in sorted(sections.items()):
        result += f"📊 <b>{escape(name)}</b>\n"
        for market in sorted(section, key=lambda item: (
            item.bookie_name.casefold(), item.line_value is not None,
            item.line_value or 0, item.source or "", item.exchange_side or "", item.market_id,
        )):
            label = escape(market.bookie_name)
            if market.source is not None:
                label += f" ({escape((market.exchange_side or 'Unspecified').title())}, {escape(market.source)})"
            if market.line_value is not None:
                label += f" [{market.line_value}]"
            if market.is_live:
                label += " (LIVE)"
            choices = " | ".join(f"{escape(choice.choice_name)}: {_format_choice(choice)}" for choice in market.choices)
            result += f"  {label}: {choices}\n"
        result += "\n"
    return result or "No markets available\n"
