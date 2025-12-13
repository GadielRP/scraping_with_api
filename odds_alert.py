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
            
            if minutes_until_start is not None:
                message += f"🕒 {minutes_until_start} min until start\n"
            
            message += f"🆔 Event: {event_id}\n\n"
            
            if not markets:
                message += "❌ No markets available\n"
                return message
            
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
    
    def send_odds_alert(self, event_data: Dict, odds_response: Dict, minutes_until_start: int = None) -> bool:
        """
        Process odds response and send alert via Telegram.
        
        This is the main entry point called from scheduler.py.
        
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
            
            if not markets:
                logger.info(f"No markets to alert for event {event_data.get('id')}")
                return False
            
            # Create the formatted message
            message = self.create_odds_alert_message(event_data, markets, minutes_until_start)
            
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


# Global instance for easy import
odds_alert_processor = OddsAlertProcessor()

