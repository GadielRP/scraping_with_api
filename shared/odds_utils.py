import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

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


