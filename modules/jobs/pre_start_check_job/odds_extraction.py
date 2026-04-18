"""
Odds Extraction Submodule - Extracts and processes odds data from API response.

This module processes the complete odds response from pre-start checks to extract
all available betting markets.
"""

import logging
from typing import Dict, List, Optional
from decimal import Decimal

from shared.odds_utils import fractional_to_decimal

logger = logging.getLogger(__name__)


def extract_final_odds_from_response(response: Dict, initial_odds_extraction: bool = False) -> Optional[Dict]:
    """Extract final odds from a full-time market response."""
    try:
        if not response or "markets" not in response:
            logger.warning("No markets found in final odds response")
            return None

        for market in response["markets"]:
            if market.get("isLive") is False and market.get("marketName") == "Full time":
                choices = market.get("choices", [])
                if not choices:
                    continue

                odds_data = {}
                available_choices = []
                initial_odds_data = {} if initial_odds_extraction else None

                for choice in choices:
                    name = choice.get("name")
                    current_fractional = choice.get("fractionalValue")
                    initial_fractional = choice.get("initialFractionalValue")

                    if not current_fractional:
                        logger.warning("Missing fractional value for choice %s", name)
                        continue

                    available_choices.append(name)
                    odds_data[f"{name}_final"] = fractional_to_decimal(current_fractional)

                    if initial_odds_extraction and initial_fractional:
                        initial_odds_data[f"{name}_initial"] = fractional_to_decimal(initial_fractional)
                    elif initial_odds_extraction and not initial_fractional:
                        logger.warning("Missing initial fractional value for choice %s", name)

                if len(available_choices) >= 2:
                    if len(available_choices) == 3 and len(odds_data) == 3:
                        choice_names = list(odds_data.keys())
                        result = {
                            "one_final": odds_data[choice_names[0]],
                            "x_final": odds_data[choice_names[1]],
                            "two_final": odds_data[choice_names[2]],
                        }

                        if initial_odds_extraction and initial_odds_data and len(initial_odds_data) == 3:
                            initial_choice_names = list(initial_odds_data.keys())
                            result.update(
                                {
                                    "one_initial": initial_odds_data[initial_choice_names[0]],
                                    "x_initial": initial_odds_data[initial_choice_names[1]],
                                    "two_initial": initial_odds_data[initial_choice_names[2]],
                                }
                            )
                        return result

                    if len(available_choices) == 2 and len(odds_data) == 2:
                        choice_names = list(odds_data.keys())
                        result = {
                            "one_final": odds_data[choice_names[0]],
                            "x_final": None,
                            "two_final": odds_data[choice_names[1]],
                        }

                        if initial_odds_extraction and initial_odds_data and len(initial_odds_data) == 2:
                            initial_choice_names = list(initial_odds_data.keys())
                            result.update(
                                {
                                    "one_initial": initial_odds_data[initial_choice_names[0]],
                                    "x_initial": None,
                                    "two_initial": initial_odds_data[initial_choice_names[1]],
                                }
                            )
                        return result

                    if len(available_choices) > 3 and len(odds_data) >= 2:
                        choice_names = list(odds_data.keys())
                        result = {
                            "one_final": odds_data[choice_names[0]],
                            "x_final": None,
                            "two_final": odds_data[choice_names[1]],
                        }

                        if initial_odds_extraction and initial_odds_data and len(initial_odds_data) >= 2:
                            initial_choice_names = list(initial_odds_data.keys())
                            result.update(
                                {
                                    "one_initial": initial_odds_data[initial_choice_names[0]],
                                    "x_initial": None,
                                    "two_initial": initial_odds_data[initial_choice_names[1]],
                                }
                            )
                        return result

        logger.warning("No suitable market found for final odds extraction")
        return None
    except Exception as exc:
        logger.error("Error extracting final odds: %s", exc)
        return None

class OddsExtractor:
    """
    Processes raw odds response to extract structured market data.
    """
    
    def extract_all_markets(self, odds_response: Dict) -> List[Dict]:
        """
        Extract ALL markets from the odds response.
        
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

# Global instance
odds_extractor = OddsExtractor()
