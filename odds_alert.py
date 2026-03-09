"""
Odds Alert System - Telegram notification for all available odds markets

This module processes the complete odds response from pre-start checks and sends
formatted alerts with all available betting markets.

INTEGRATION POINT:
==================
Called from scheduler.py after line 489:
    final_odds_response = api_client.get_event_final_odds(event_data['id'], event_data['slug'])
    
    # NEW: Send odds alert with all markets
    from odds_alert import odds_alert_processor
    odds_alert_processor.send_odds_alert(event_data, final_odds_response)

This module does NOT modify any existing flow - it's an additional notification layer.
"""

import logging
from typing import Dict, List, Optional
from decimal import Decimal

from odds_utils import fractional_to_decimal

logger = logging.getLogger(__name__)


class OddsAlertProcessor:
    """
    Processes odds response and creates formatted alerts for all markets.
    
    Extracts all betting markets from the API response including:
    - Full time (1X2 or Home/Away)
    - Quarter/Period winners
    - Half time
    - Point spread / Handicap
    - Game total (Over/Under)
    - And any other markets available in the response
    """
    
    def __init__(self):
        """Initialize the odds alert processor."""
        self.enabled = True
    
    def extract_all_markets(self, odds_response: Dict) -> List[Dict]:
        """
        Extract ALL markets from the odds response.
        
        Unlike extract_final_odds_from_response() which only extracts Full time odds,
        this function extracts every market available in the response.
        
        Args:
            odds_response: The raw API response containing market data
            
        Returns:
            List of dictionaries with processed market data, each containing:
            - market_name: Name of the market (e.g., "Full time", "1st quarter winner")
            - market_group: Group classification (e.g., "Home/Away", "Over/Under")
            - market_period: Period the market applies to (e.g., "Match", "1st quarter")
            - is_live: Whether this is a live market
            - choice_group: For over/under markets, the line (e.g., "229.5")
            - choices: List of choice data with initial and current odds
        """
        try:
            if not odds_response or 'markets' not in odds_response:
                logger.warning("No markets found in odds response")
                return []
            
            processed_markets = []
            
            for market in odds_response.get('markets', []):
                try:
                    market_data = self._process_single_market(market)
                    if market_data:
                        processed_markets.append(market_data)
                except Exception as e:
                    logger.error(f"Error processing market {market.get('marketName', 'Unknown')}: {e}")
                    continue
            
            logger.info(f"✅ Extracted {len(processed_markets)} markets from odds response")
            return processed_markets
            
        except Exception as e:
            logger.error(f"Error extracting all markets: {e}")
            return []
    
    def _process_single_market(self, market: Dict) -> Optional[Dict]:
        """
        Process a single market from the response.
        
        Args:
            market: Raw market dictionary from API response
            
        Returns:
            Processed market data or None if invalid
        """
        try:
            choices = market.get('choices', [])
            if not choices:
                return None
            
            # Process all choices in this market
            processed_choices = []
            for choice in choices:
                initial_fractional = choice.get('initialFractionalValue', '')
                current_fractional = choice.get('fractionalValue', '')
                
                # Convert fractional to decimal
                initial_decimal = fractional_to_decimal(initial_fractional) if initial_fractional else None
                current_decimal = fractional_to_decimal(current_fractional) if current_fractional else None
                
                # Determine odds movement direction
                change = choice.get('change', 0)
                if change > 0:
                    movement = '↑'
                elif change < 0:
                    movement = '↓'
                else:
                    movement = '='
                
                processed_choices.append({
                    'name': choice.get('name', 'Unknown'),
                    'initial_odds': initial_decimal,
                    'current_odds': current_decimal,
                    'initial_fractional': initial_fractional,
                    'current_fractional': current_fractional,
                    'movement': movement,
                    'change': change
                })
            
            return {
                'market_name': market.get('marketName', 'Unknown'),
                'market_group': market.get('marketGroup', ''),
                'market_period': market.get('marketPeriod', 'Match'),
                'is_live': market.get('isLive', False),
                'choice_group': market.get('choiceGroup'),  # For over/under lines
                'choices': processed_choices
            }
            
        except Exception as e:
            logger.error(f"Error processing single market: {e}")
            return None
    
    def create_odds_alert_message(self, event_data: Dict, markets: List[Dict], minutes_until_start: int = None) -> str:
        """
        Create formatted Telegram message for odds alert.
        
        Args:
            event_data: Event information containing:
                - id: Event ID
                - home_team: Home team name
                - away_team: Away team name
                - sport: Sport type
                - competition: Competition name (optional)
                - discovery_source: Discovery source (optional, e.g., "dropping_odds", "high_value_streaks")
            markets: List of processed market data from extract_all_markets()
            minutes_until_start: Minutes until event starts (optional)
            
        Returns:
            Formatted message string for Telegram with HTML formatting
        """
        try:
            # Header
            home_team = event_data.get('home_team', 'Unknown')
            away_team = event_data.get('away_team', 'Unknown')
            sport = event_data.get('sport', 'Unknown')
            event_id = event_data.get('id', 'Unknown')
            competition = event_data.get('competition', '')
            discovery_source = event_data.get('discovery_source', '')
            
            # Sport emoji
            sport_emojis = {
                'Football': '⚽',
                'Basketball': '🏀',
                'Tennis': '🎾',
                'Hockey': '🏒',
                'Baseball': '⚾',
                'Handball': '🤼',
                'Rugby': '🏉',
                'American Football': '🏈',
                'Volleyball': '🏐'
            }
            sport_emoji = sport_emojis.get(sport, '🏟️')
            
            message = f"📊 <b>ODDS ALERT</b>\n\n"
            message += f"{sport_emoji} <b>{home_team} vs {away_team}</b>\n"
            
            if competition:
                message += f"🏆 {competition}\n"
            
            if discovery_source:
                # Format discovery source: "dropping_odds" -> "Dropping Odds"
                formatted_source = discovery_source.title().replace('_', ' ')
                message += f"🔍 {formatted_source}\n"
            
            if minutes_until_start is not None and minutes_until_start == 0:
                message += f"🕒 Event is starting now!\n"
            elif minutes_until_start is not None:
                message += f"🕒 {minutes_until_start} min until start\n"
            
            message += f"🆔 Event: {event_id}\n\n"
            
            if not markets:
                message += "❌ No markets available\n"
                return message
            message += f"Sofascore's odds:\n"
            # Display each market with its actual name from the API
            for market in markets:
                market_name = market.get('market_name', 'Unknown')
                choice_group = market.get('choice_group')
                
                # Show market name as header
                message += f"📊 <b>{market_name}</b>\n"
                
                # If this market has a choice_group (like Over/Under with a line), show it
                if choice_group:
                    message += f"  <i>Line: {choice_group}</i>\n"
                
                # Show the choices for this market
                message += self._format_market_choices(market, indent="  ")
                message += "\n"
            
            return message
            
        except Exception as e:
            logger.error(f"Error creating odds alert message: {e}")
            return f"❌ Error creating odds alert message: {str(e)}"
    
    def _format_market_choices(self, market: Dict, indent: str = "  ") -> str:
        """
        Format choices for a single market.
        
        Args:
            market: Processed market data
            indent: Indentation string for formatting
            
        Returns:
            Formatted string with all choices
        """
        result = ""
        for choice in market.get('choices', []):
            name = choice.get('name', '?')
            initial = choice.get('initial_odds')
            current = choice.get('current_odds')
            movement = choice.get('movement', '=')
            
            # Format odds display
            if initial and current:
                initial_str = f"{initial:.2f}" if isinstance(initial, (Decimal, float)) else str(initial)
                current_str = f"{current:.2f}" if isinstance(current, (Decimal, float)) else str(current)
                result += f"{indent}{name}: {initial_str} → {current_str} {movement}\n"
            elif current:
                current_str = f"{current:.2f}" if isinstance(current, (Decimal, float)) else str(current)
                result += f"{indent}{name}: {current_str}\n"
            else:
                result += f"{indent}{name}: N/A\n"
        
        return result
    
    def _format_oddsportal_section(self, op_markets: List[Dict], event_data: Dict = None) -> str:
        """
        Format the OddsPortal section of the alert message.
        
        Args:
            op_markets: List of dictionary from MarketRepository.get_oddsportal_markets_for_event
            event_data: Event dictionary to extract team names for formatting
            
        Returns:
            Formatted string for the OddsPortal section
        """
        if not op_markets:
            return ""
            
        result = "\n🟡 <b>ODDSPORTAL ODDS</b>\n\n"
        
        home_team = event_data.get('home_team', 'Home') if event_data else 'Home'
        away_team = event_data.get('away_team', 'Away') if event_data else 'Away'
        
        # Group markets by (market_group, market_period)
        from collections import defaultdict
        grouped_markets = defaultdict(list)
        
        # op_markets is already sorted by group -> period -> choice_group -> bookie
        # Group them logically
        for m in op_markets:
            market_group = m.get('market_group', 'Unknown')
            market_period = m.get('market_period', 'Unknown')
            grouped_markets[(market_group, market_period)].append(m)
            
        for (market_group, market_period), markets in grouped_markets.items():
            # Format the header
            if market_group == '1X2':
                display_group = "Full time" if market_period == 'Full-time' else market_period
            else:
                display_group = f"{market_group} - {market_period}"
                
            result += f"📊 <b>{display_group}</b>\n"
            
            for m in markets:
                bookie_name = m['bookie_name']
                choice_group = m.get('choice_group')
                choices = m['choices']
                
                # If Betfair, add Back/Lay info
                if 'betfair' in bookie_name.lower() and choice_group:
                    bookie_display = f"{bookie_name} ({choice_group})"
                # For Asian Handicap or Over/Under, optionally show the line next to the bookie name or inside if appropriate.
                elif market_group in ['Asian Handicap', 'Over/Under'] and choice_group:
                    bookie_display = f"{bookie_name} [{choice_group}]"
                else:
                    bookie_display = bookie_name

                # Format choices for this bookie
                choice_strs = []
                for c in choices:
                    name = c.get('name', '?')
                    initial = c.get('initial')
                    current = c.get('current')
                    movement = c.get('movement', '=')
                    
                    # Optionally format name if it's Asian Handicap
                    if market_group == 'Asian Handicap':
                        if name == '1':
                            name = home_team
                        elif name == '2':
                            name = away_team
                            
                    # Remove the choice name if it's 1X2 or Over/Under. For AH, include name to distinguish home/away.
                    if market_group in ['1X2']:
                        prefix = "" # "1: ", "X: " would go here, but let's just make it compact like original code
                    else:
                        prefix = ""
                        
                    if initial is not None and current is not None:
                        choice_strs.append(f"{initial:.2f}→{current:.2f}{movement}")
                    elif current is not None:
                        choice_strs.append(f"{current:.2f}")
                    else:
                        choice_strs.append("N/A")
                
                line_body = " | ".join(choice_strs)
                result += f"  {bookie_display}: {line_body}\n"
                
            result += "\n"
            
        return result

    
    def send_odds_alert(self, event_data: Dict, odds_response: Dict, minutes_until_start: int = None) -> bool:
        """
        Process odds response and send alert via Telegram.
        
        This is the main entry point called from scheduler.py.
        
        Smart Alert Filtering:
        - 0 markets: Log warning (TODO: delete event from database)
        - 1 market AND marketName="Full time": Mark as low-value, skip alert
        - 2+ markets: Process normally
        
        Args:
            event_data: Event information dictionary containing:
                - id: Event ID
                - home_team: Home team name
                - away_team: Away team name
                - sport: Sport type
                - competition: Competition name (optional)
                - discovery_source: Discovery source (optional, e.g., "dropping_odds", "high_value_streaks")
            odds_response: Raw API odds response from get_event_final_odds()
            minutes_until_start: Minutes until event starts (optional)
            
        Returns:
            True if alert sent successfully, False otherwise
        """
        try:
            if not self.enabled:
                logger.debug("Odds alert processor is disabled")
                return False
            
            if not odds_response:
                logger.warning(f"No odds response provided for event {event_data.get('id')}")
                return False
            
            # Extract all markets from the response
            markets = self.extract_all_markets(odds_response)
            
            # NOTE: Market saving to DB now happens in scheduler.py during odds extraction
            # (before alert evaluation), so ALL sports get their markets saved

            
            # SMART ALERT FILTERING: Handle 0 markets (likely 404 response - event should be deleted)
            if len(markets) == 0:
                logger.warning(f"🗑️ NO MARKETS: Event {event_data.get('id')} has 0 markets - should be deleted from database")
                # TODO: Implement event deletion logic
                # self._delete_event_from_database(event_data.get('id'))
                return False
            
            # SMART ALERT FILTERING: Check for low-value events (1 market AND it's "Full time")
            if len(markets) == 1:
                market = markets[0]
                market_name = market.get('market_name', '')
                # Full time market has marketId=1 and marketName="Full time"
                if market_name == 'Full time':
                    logger.info(f"⏭️ LOW-VALUE EVENT: Event {event_data.get('id')} has only 1 market (Full time) - marking alert_sent=True and skipping odds alert")
                    self._mark_event_alert_sent(event_data.get('id'))
                    return False  # Don't send alert
                else:
                    # Single market but NOT "Full time" - process normally (edge case)
                    logger.info(f"📊 Event {event_data.get('id')} has 1 market but it's '{market_name}' (not Full time) - processing normally")
            
            # Create the formatted message (2+ markets or 1 non-Full-time market)
            message = self.create_odds_alert_message(event_data, markets, minutes_until_start)
            
            # --- ODDSPORTAL INTEGRATION ---
            # If the event's season is tracked in OddsPortal, fetch and append bookie odds
            try:
                from oddsportal_config import SEASON_ODDSPORTAL_MAP
                from repository import MarketRepository
                
                season_id = event_data.get('season_id')
                if season_id and season_id in SEASON_ODDSPORTAL_MAP:
                    op_markets = MarketRepository.get_oddsportal_markets_for_event(event_data.get('id'))
                    if op_markets:
                        op_section = self._format_oddsportal_section(op_markets, event_data)
                        message += op_section
                        logger.info(f"📊 Added OddsPortal section to alert for event {event_data.get('id')}")
            except Exception as op_err:
                logger.error(f"Error adding OddsPortal section to alert: {op_err}")

            
            # Send via Telegram using the existing alert system
            from alert_system import pre_start_notifier
            
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
    
    def _mark_event_alert_sent(self, event_id: int) -> bool:
        """
        Mark event as alert_sent=True in database.
        
        This flags the event as "low-value" so other alert processes
        (Dual Process, 4Q) will skip it.
        
        Args:
            event_id: Event ID to mark
            
        Returns:
            True if successfully marked, False otherwise
        """
        try:
            from database import db_manager
            from models import Event
            
            with db_manager.get_session() as session:
                event = session.query(Event).filter(Event.id == event_id).first()
                if event:
                    event.alert_sent = True
                    session.commit()
                    logger.info(f"✅ Marked event {event_id} as alert_sent=True (low-value event)")
                    return True
                else:
                    logger.warning(f"Event {event_id} not found when marking alert_sent")
                    return False
        except Exception as e:
            logger.error(f"Error marking event {event_id} as alert_sent: {e}")
            return False

    # TODO: Implement when ready
    # def _delete_event_from_database(self, event_id: int) -> bool:
    #     """Delete event from database (0 markets = invalid event)."""
    #     try:
    #         from database import db_manager
    #         from models import Event
    #         
    #         with db_manager.get_session() as session:
    #             event = session.query(Event).filter(Event.id == event_id).first()
    #             if event:
    #                 session.delete(event)
    #                 session.commit()
    #                 logger.info(f"🗑️ Deleted event {event_id} from database (0 markets)")
    #                 return True
    #             return False
    #     except Exception as e:
    #         logger.error(f"Error deleting event {event_id}: {e}")
    #         return False


# Global instance for easy import
odds_alert_processor = OddsAlertProcessor()

