"""
Odds Alert Formatter - Telegram notification for all available odds markets

This module manages the business logic to filter, format, and dispatch odds alerts.
It uses OddsExtractor to parse data before formatting.
"""

import logging
from typing import Dict, List, Optional
from decimal import Decimal

from infrastructure.persistence.repositories import EventRepository, MarketRepository
from modules.alerts import pre_start_notifier
from modules.competition.tracked_competitions import is_tracked_competition
from modules.oddsportal.oddsportal_config import ODDSPORTAL_COMPETITION_ROUTES

logger = logging.getLogger(__name__)

# To control global enablement of odds alerts
ODDS_ALERT_ENABLED = True

def send_odds_alert(event_data: Dict, odds_response: Dict, minutes_until_start: int = None, op_data=None) -> bool:
    """
    Process odds response and send alert via Telegram.
    
    This is the main entry point called from scheduler.py.
    
    Args:
        event_data: Event information dictionary
        odds_response: Raw provider odds payload.
        minutes_until_start: Minutes until event starts (optional)
        op_data: Optional MatchOddsData object
        
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
                    external_section = _format_external_markets_section(external_markets, event_data, op_data=op_data)
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
            
        message += f"Sofascore's odds:\n"
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

def _format_external_markets_section(external_markets: List[Dict], event_data: Dict = None, op_data=None) -> str:
    """Format external bookmakers odds section of the alert message."""
    if not external_markets:
        return ""
        
    result = ""
    
    home_team = event_data.get('home_team', 'Home') if event_data else 'Home'
    away_team = event_data.get('away_team', 'Away') if event_data else 'Away'
    
    from collections import defaultdict
    
    # Group markets by source
    markets_by_source = defaultdict(list)
    for m in external_markets:
        source = m.get('source', 'oddsportal')
        markets_by_source[source].append(m)
        
    for source in sorted(markets_by_source.keys()):
        source_display = source.upper().replace('_', ' ')
        result += f"\n🟡 <b>{source_display} ODDS</b>\n\n"
        
        grouped_markets = defaultdict(list)
        for m in markets_by_source[source]:
            market_group = m.get('market_group', 'Unknown')
            market_period = m.get('market_period', 'Unknown')
            grouped_markets[(market_group, market_period)].append(m)
            
        for (market_group, market_period), markets in sorted(grouped_markets.items()):
            if market_group == '1X2':
                display_group = "Full Time" if market_period == 'Full Time' else market_period
            else:
                display_group = f"{market_group} - {market_period}"
                
            result += f"📊 <b>{display_group}</b>\n"
            
            # Sort markets by bookie name: numbers first, then alphabetical (case-insensitive)
            markets = sorted(markets, key=lambda x: x['bookie_name'].lower())
            
            for m in markets:
                bookie_name = m['bookie_name']
                choice_group = m.get('choice_group')
                is_live = m.get('is_live', False)
                choices = m['choices']
                
                order_map = {'1': 1, '1X': 2, 'X': 3, 'X2': 4, '2': 5, '12': 6, 'Over': 7, 'Under': 8, 'Yes': 9, 'No': 10}
                choices = sorted(choices, key=lambda c: order_map.get(c.get('name', ''), 99))
                
                bookie_time = None
                if op_data and hasattr(op_data, 'extractions'):
                    for ext in op_data.extractions:
                        if ext.market_group == market_group and ext.market_period == market_period:
                            for bo in ext.bookie_odds:
                                if bo.name == bookie_name and getattr(bo, 'movement_odds_time', None):
                                    bookie_time = bo.movement_odds_time
                                    break
                            
                            if not bookie_time and ext.betfair and 'betfair' in bookie_name.lower():
                                if getattr(ext.betfair, 'movement_odds_time', None):
                                    bookie_time = ext.betfair.movement_odds_time
                            break
                        
                time_str = f" 🕒 {bookie_time}" if bookie_time else ""
                live_label = " (LIVE)" if is_live else ""
                
                if 'betfair' in bookie_name.lower() and choice_group:
                    bookie_display = f"{bookie_name} ({choice_group}){live_label}{time_str}"
                elif market_group in ['Asian Handicap', 'Over/Under'] and choice_group:
                    bookie_display = f"{bookie_name} [{choice_group}]{live_label}{time_str}"
                else:
                    bookie_display = f"{bookie_name}{live_label}{time_str}"

                choice_strs = []
                for c in choices:
                    name = c.get('name', '?')
                    initial = c.get('initial')
                    current = c.get('current')
                    movement = c.get('movement', '=')
                    
                    if market_group == 'Asian Handicap':
                        if name == '1': name = home_team
                        elif name == '2': name = away_team
                            
                    if initial is not None and current is not None:
                        choice_strs.append(f"{_format_odds_value(initial)}→{_format_odds_value(current)}{movement}")
                    elif initial is not None:
                        # OddsPortal is temporarily opening-only. Exchange
                        # markets may therefore have a valid opening without a
                        # current snapshot; retain that evidence in the alert.
                        choice_strs.append(
                            f"{_format_odds_value(initial)}→N/A"
                        )
                    elif current is not None:
                        choice_strs.append(f"{_format_odds_value(current)}")
                    else:
                        choice_strs.append("N/A")
                
                line_body = " | ".join(choice_strs)
                result += f"  {bookie_display}: {line_body}\n"
                
            result += "\n"
            
    return result

# Backwards compatibility alias
_format_oddsportal_section = _format_external_markets_section
