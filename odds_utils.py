import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, List, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

def fractional_to_decimal(fractional_value: str) -> Optional[Decimal]:
    """
    Convert fractional odds to decimal format.
    
    Formula: decimal = 1 + (a/b)
    
    Args:
        fractional_value: String in format "a/b" (e.g., "3/5", "7/2")
    
    Returns:
        Decimal value rounded to 2 decimal places, or None if invalid
    """
    try:
        if not fractional_value or '/' not in fractional_value:
            logger.warning(f"Invalid fractional value: {fractional_value}")
            return None
        
        # Parse numerator and denominator
        parts = fractional_value.split('/')
        if len(parts) != 2:
            logger.warning(f"Invalid fractional format: {fractional_value}")
            return None
        
        numerator = float(parts[0])
        denominator = float(parts[1])
        
        # Validate inputs
        if denominator == 0:
            logger.error(f"Division by zero in fractional value: {fractional_value}")
            return None
        
        if numerator < 0 or denominator < 0:
            logger.warning(f"Negative values in fractional: {fractional_value}")
            return None
        
        # Calculate decimal odds
        decimal_value = 1 + (numerator / denominator)
        
        # Round to 2 decimal places
        decimal_decimal = Decimal(str(decimal_value)).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
        
        return decimal_decimal
        
    except (ValueError, TypeError) as e:
        logger.error(f"Error converting fractional {fractional_value}: {e}")
        return None





def process_event_odds_from_dropping_odds(event_id: str, odds_map: Dict) -> Dict:
    """
    Process odds data from the dropping odds oddsMap for an event and return structured odds data.
    
    Args:
        event_id: Event ID as string
        odds_map: The oddsMap dictionary from dropping odds API response
    
    Returns:
        Dictionary with processed odds data
    """
    try:
        if event_id not in odds_map:
            logger.warning(f"Event {event_id} not found in odds map")
            return {}
        
        event_odds = odds_map[event_id]
        odds_data_raw = event_odds.get('odds', {})
        choices = odds_data_raw.get('choices', [])
        
        if not choices:
            logger.warning(f"No choices found for event {event_id}")
            return {}
        
        # Initialize odds data
        odds_data = {
            'one_open': None,
            'x_open': None,
            'two_open': None,
            'one_cur': None,
            'x_cur': None,
            'two_cur': None,
            'raw_fractional': {
                'oddsMap_data': event_odds,
                'choices': choices
            }
        }
        
        # Extract odds for each choice
        for choice in choices:
            choice_name = choice.get('name', '')
            initial_fractional = choice.get('initialFractionalValue', '')
            current_fractional = choice.get('fractionalValue', '')
            
            # Convert fractional to decimal
            initial_decimal = fractional_to_decimal(initial_fractional)
            current_decimal = fractional_to_decimal(current_fractional)
            
            # Map to our fields
            if choice_name == '1':
                odds_data['one_open'] = initial_decimal
                odds_data['one_cur'] = current_decimal
            elif choice_name == 'X':
                odds_data['x_open'] = initial_decimal
                odds_data['x_cur'] = current_decimal
            elif choice_name == '2':
                odds_data['two_open'] = initial_decimal
                odds_data['two_cur'] = current_decimal
        
        # Log summary
        logger.info(f"Processed odds - Open: 1={odds_data['one_open']}, X={odds_data['x_open']}, 2={odds_data['two_open']}")
        logger.info(f"Processed odds - Current: 1={odds_data['one_cur']}, X={odds_data['x_cur']}, 2={odds_data['two_cur']}")
        
        return odds_data
        
    except Exception as e:
        logger.error(f"Error processing odds for event {event_id}: {e}")
        return {}





def validate_odds_data(odds_data: Dict) -> bool:
    """
    Validate processed odds data for consistency.
    Handles both complete odds data (from discovery) and final odds data (from pre-start checks).
    
    Args:
        odds_data: Dictionary with odds data
    
    Returns:
        True if valid, False otherwise
    """
    # Check if this is complete odds data (has both opening and current odds)
    if 'one_open' in odds_data and 'one_cur' in odds_data:
        # Complete odds validation - check opening and current odds fields
        required_fields = ['one_open', 'two_open', 'one_cur', 'two_cur']
        validation_type = "complete odds"
        
        for field in required_fields:
            if field not in odds_data or odds_data[field] is None:
                logger.debug(f"Missing required {validation_type} field: {field}")
                return False
    else:
        # Final odds validation - check current/final odds fields (either one_cur or one_final)
        if 'one_final' in odds_data:
            # For final odds, only require 1 and 2 (home and away)
            # X (draw) is optional as some sports don't have draw options
            required_fields = ['one_final', 'two_final']
            validation_type = "final odds (pre-start)"
        else:
            # For current odds, only require 1 and 2 (home and away)
            # X (draw) is optional as some sports don't have draw options
            required_fields = ['one_cur', 'two_cur']
            validation_type = "final odds (discovery)"
        
        for field in required_fields:
            if field not in odds_data or odds_data[field] is None:
                logger.debug(f"Missing required {validation_type} field: {field}")
                return False
    
    # Check for reasonable odds values (between 1.001 and 1000) - ONLY for numeric fields
    # Include optional draw fields if they exist and are not None
    numeric_fields = required_fields.copy()
    if odds_data.get('x_open') is not None:
        numeric_fields.append('x_open')
    if odds_data.get('x_cur') is not None:
        numeric_fields.append('x_cur')
    if odds_data.get('x_final') is not None:
        numeric_fields.append('x_final')
    
    for field in numeric_fields:
        value = odds_data.get(field)
        if value is not None and isinstance(value, (int, float, Decimal)) and (value < 1.001 or value > 1000):
            logger.warning(f"Odds value out of reasonable range: {field}={value}")
            return False
    
    return True
