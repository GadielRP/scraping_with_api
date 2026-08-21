"""
Odds Alert Formatter - Telegram notification for all available odds markets

This module manages the business logic to filter, format, and dispatch odds alerts.
It uses OddsExtractor to parse data before formatting.
"""

import logging
from typing import Dict, List, Sequence

from infrastructure.persistence.repositories import EventRepository, MarketRepository
from infrastructure.persistence.repositories.market.market_read_models import (
    ExternalChoiceQuote,
    ExternalMarketQuoteBlock,
)
from modules.alerts import pre_start_notifier
from modules.competition.tracked_competitions import is_tracked_competition
from modules.oddsportal.oddsportal_config import ODDSPORTAL_COMPETITION_ROUTES

logger = logging.getLogger(__name__)

# To control global enablement of odds alerts
ODDS_ALERT_ENABLED = True

def send_odds_alert(event_data: Dict, odds_response: Dict, minutes_until_start: int = None) -> bool:
    """
    Process odds response and send alert via Telegram.
    
    This is the main entry point called from scheduler.py.
    
    Args:
        event_data: Event information dictionary
        odds_response: Raw provider odds payload.
        minutes_until_start: Minutes until event starts (optional)
        
    Returns:
        True if alert sent successfully, False otherwise
    """
    try:
        # --- START: PRECISION ALERT GATE ---
        # Odds alerts only send at key moments 30 and -5
        ALLOWED_ODDS_ALERT_MINUTES = {30, -5}
        if minutes_until_start not in ALLOWED_ODDS_ALERT_MINUTES:
            logger.info(f"📵 Skipping odds alert sending process for event {event_data.get('id')} at minute {minutes_until_start}; allowed minutes are {ALLOWED_ODDS_ALERT_MINUTES}")
            return False
        # --- END: PRECISION ALERT GATE ---

        competition_id = event_data.get("competition_id")

        if not ODDS_ALERT_ENABLED:
            logger.debug("Odds alert processor is disabled")
            return False
        
        if not odds_response:
            logger.warning(f"No odds response provided for event {event_data.get('id')}")
            return False
        
        # Extract all markets from the response
        from modules.jobs.pre_start_check_job.odds_extraction import odds_extractor
        markets = odds_extractor.extract_all_markets(odds_response)
        
        # SMART ALERT FILTERING: Handle 0 markets
        if len(markets) == 0:
            logger.warning(f"🗑️ NO MARKETS: Event {event_data.get('id')} has 0 markets - should be deleted from database")
            return False
        
        # SMART ALERT FILTERING: Handle low-value events (1 market AND it's "Full time")
        if len(markets) == 1:
            market = markets[0]
            market_name = market.get('market_name', '')
            if is_tracked_competition(competition_id):
                logger.info(
                    "📊 Event %s has 1 market but competition_id=%s is "
                    "tracked - forcing alert send",
                    event_data.get("id"),
                    competition_id,
                )
            elif market_name == 'Full time':
                logger.info(f"⏭️ LOW-VALUE EVENT: Event {event_data.get('id')} has only 1 market (Full time) - marking alert_sent=True and skipping odds alert")
                EventRepository.mark_event_as_alerted(event_data.get('id'))
                return False
            else:
                logger.info(f"📊 Event {event_data.get('id')} has 1 market but it's '{market_name}' (not Full time) - processing normally")
        
        # Create the formatted message
        message = create_odds_alert_message(event_data, markets, minutes_until_start)
        
        # --- EXTERNAL BOOKIES INTEGRATION ---
        try:
            if competition_id in ODDSPORTAL_COMPETITION_ROUTES:
                external_markets = MarketRepository.get_external_markets_for_event(event_data.get('id'))
                if external_markets:
                    external_section = _format_external_markets_section(external_markets)
                    message += external_section
                    logger.info(f"📊 Added external markets section to alert for event {event_data.get('id')}")
        except Exception as op_err:
            logger.error(f"Error adding external markets section to alert: {op_err}")

        # Send via Telegram
        if not pre_start_notifier.telegram_enabled:
            logger.warning("Telegram notifications not configured - cannot send odds alert")
            return False
        
        success = pre_start_notifier.send_telegram_message(message)
        
        if success:
            logger.info(f"✅ Odds alert sent for event {event_data.get('id')}: {event_data.get('home_team')} vs {event_data.get('away_team')}")
        else:
            logger.warning(f"❌ Failed to send odds alert for event {event_data.get('id')}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error in send_odds_alert for event {event_data.get('id')}: {e}")
        return False

def create_odds_alert_message(event_data: Dict, markets: List[Dict], minutes_until_start: int = None) -> str:
    """Create formatted Telegram message for odds alert."""

    # log market input object
    logger.info(f" markets: {markets}")
    try:
        home_team = event_data.get('home_team', 'Unknown')
        away_team = event_data.get('away_team', 'Unknown')
        sport = event_data.get('sport', 'Unknown')
        event_id = event_data.get('id', 'Unknown')
        competition = event_data.get('competition', '')
        discovery_source = event_data.get('discovery_source', '')
        
        sport_emojis = {
            'Football': '⚽', 'Basketball': '🏀', 'Tennis': '🎾',
            'Hockey': '🏒', 'Baseball': '⚾', 'Handball': '🤼',
            'Rugby': '🏉', 'American Football': '🏈', 'Volleyball': '🏐'
        }
        sport_emoji = sport_emojis.get(sport, '🏟️')
        
        message = f"📊 <b>ODDS ALERT</b>\n\n"
        message += f"{sport_emoji} <b>{home_team} vs {away_team}</b>\n"
        
        if competition:
            message += f"🏆 {competition}\n"
        
        if discovery_source:
            formatted_source = discovery_source.title().replace('_', ' ')
            message += f"🔍 {formatted_source}\n"
        
        if minutes_until_start < 0:
            message += f"🕒 <b>Event is Live!</b>\n"
        elif minutes_until_start is not None and minutes_until_start == 0:
            message += f"🕒 <b>Event is starting now!</b>\n"
        elif minutes_until_start is not None:
            message += f"🕒 <b>{minutes_until_start} min until start</b>\n"
        
        message += f"🆔 Event: {event_id}\n\n"
        
        if not markets:
            message += "❌ No markets available\n"
            return message
            
        message += f"🔵 <b>SOFASCORE'S ODDS:</b>\n"
        for market in markets:
            market_name = market.get('market_name', 'Unknown')
            choice_group = market.get('choice_group')
            
            live_label = " (LIVE)" if market.get('is_live') else ""
            message += f"📊 <b>{market_name}{live_label}</b>\n"
            
            if choice_group:
                message += f"  <i>Line: {choice_group}</i>\n"
            
            message += _format_market_choices(market, indent="  ")
            message += "\n"
        
        return message
        
    except Exception as e:
        logger.error(f"Error creating odds alert message: {e}")
        return f"❌ Error creating odds alert message: {str(e)}"

def _format_odds_value(val) -> str:
    """Format an odds value showing 2 or 3 decimals depending on significance."""
    if val is None:
        return "N/A"
    try:
        fval = float(val)
        s3 = f"{fval:.3f}"
        if s3.endswith('0'):
            return f"{fval:.2f}"
        return s3
    except (TypeError, ValueError):
        return str(val)

def _format_market_choices(market: Dict, indent: str = "  ") -> str:
    """Format choices for a single market."""
    result = ""
    for choice in market.get('choices', []):
        name = choice.get('name', '?')
        initial = choice.get('initial_odds')
        current = choice.get('current_odds')
        movement = choice.get('movement', '=')
        
        if initial and current:
            initial_str = _format_odds_value(initial)
            current_str = _format_odds_value(current)
            result += f"{indent}{name}: {initial_str} → {current_str} {movement}\n"
        elif current:
            current_str = _format_odds_value(current)
            result += f"{indent}{name}: {current_str}\n"
        else:
            result += f"{indent}{name}: N/A\n"
    
    return result

def _format_external_markets_section(
    external_markets: Sequence[ExternalMarketQuoteBlock],
) -> str:
    """Format canonical quote-aware external bookmaker odds."""
    if not external_markets:
        return ""
    if not all(isinstance(item, ExternalMarketQuoteBlock) for item in external_markets):
        raise TypeError("External odds reader requires ExternalMarketQuoteBlock values")
    return _format_external_quote_blocks(external_markets)


def _format_quote_choice(choice: ExternalChoiceQuote) -> str:
    movement = {-1: "↓", 0: "=", 1: "↑"}.get(choice.movement, "")
    if choice.initial is not None and choice.current is not None:
        return (
            f"{_format_odds_value(choice.initial)}→"
            f"{_format_odds_value(choice.current)}{movement}"
        )
    if choice.initial is not None:
        return f"{_format_odds_value(choice.initial)}→N/A"
    if choice.current is not None:
        return _format_odds_value(choice.current)
    return "N/A"


def _format_external_quote_blocks(blocks: Sequence[ExternalMarketQuoteBlock]) -> str:
    """Render the quote-aware contract without inferring source or side."""
    from collections import defaultdict

    grouped_headers = defaultdict(list)
    for block in blocks:
        header_key = (
            "field_priority",
            "",
        ) if block.aggregation == "field_priority" else ("exchange", block.source or "unknown")
        grouped_headers[header_key].append(block)

    result = ""
    for (aggregation, source), header_blocks in sorted(grouped_headers.items()):
        if aggregation == "field_priority":
            result += "\n🟡 <b>CONSOLIDATED ODDS</b>\n\n"
        else:
            result += f"\n🟡 <b>{source.upper().replace('_', ' ')} EXCHANGE ODDS</b>\n\n"

        market_sections = defaultdict(list)
        for block in header_blocks:
            market_sections[(block.market_group or "Unknown", block.market_period)].append(block)
        for (market_group, market_period), market_blocks in sorted(market_sections.items()):
            display_group = (
                "Full Time" if market_group == "1X2" and market_period == "Full Time"
                else market_period if market_group == "1X2"
                else f"{market_group} - {market_period}"
            )
            result += f"📊 <b>{display_group}</b>\n"
            for block in sorted(
                market_blocks,
                key=lambda item: (
                    item.choice_group is not None,
                    item.choice_group or "",
                    item.bookie_name.casefold(),
                    {None: 0, "back": 1, "lay": 2}.get(item.exchange_side, 9),
                    item.market_id,
                ),
            ):
                display = block.bookie_name
                if block.aggregation == "exchange":
                    display += f" ({(block.exchange_side or 'Unspecified').title()})"
                elif block.market_group in {"Asian Handicap", "Over/Under"} and block.choice_group:
                    display += f" [{block.choice_group}]"
                if block.is_live:
                    display += " (LIVE)"

                rendered_choices = []
                for choice in block.choices:
                    rendered_choices.append(_format_quote_choice(choice))
                result += f"  {display}: {' | '.join(rendered_choices)}\n"
            result += "\n"
    return result
