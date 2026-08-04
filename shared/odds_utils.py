import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional

logger = logging.getLogger(__name__)

def fractional_to_decimal(fractional_value: str) -> Optional[Decimal]:
    """
    Convert fractional odds to decimal format.
    
    Formula: decimal = 1 + (a/b)
    
    Args:
        fractional_value: String in format "a/b" (e.g., "3/5", "7/2")
    
    Returns:
        Decimal value rounded to 3 decimal places, or None if invalid
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
        
        numerator = Decimal(parts[0].strip())
        denominator = Decimal(parts[1].strip())
        
        # Validate inputs
        if denominator == 0:
            logger.error(f"Division by zero in fractional value: {fractional_value}")
            return None
        
        if numerator < 0 or denominator < 0:
            logger.warning(f"Negative values in fractional: {fractional_value}")
            return None
        
        # Calculate decimal odds
        decimal_value = Decimal("1") + (numerator / denominator)
        
        # Round to 3 decimal places to preserve full precision
        decimal_decimal = decimal_value.quantize(
            Decimal('0.001'), rounding=ROUND_HALF_UP
        )
        
        return decimal_decimal
        
    except (InvalidOperation, ValueError, TypeError) as e:
        logger.error(f"Error converting fractional {fractional_value}: {e}")
        return None


def normalize_odds_value(value) -> Optional[str]:
    """Return decimal odds as a canonical string from decimal or fractional input.

    Provider display preferences are intentionally not configured. The token
    itself is authoritative: values containing one slash are interpreted as
    fractional odds; all other numeric tokens are interpreted as decimal odds.
    Invalid values, signed movement deltas, and odds outside the supported
    decimal range return ``None``.
    """
    if value in (None, "", "-"):
        return None

    raw_value = str(value).strip()
    if not raw_value or raw_value.startswith(("+", "-")):
        return None

    if "/" in raw_value:
        decimal_value = fractional_to_decimal(raw_value)
    else:
        try:
            decimal_value = Decimal(raw_value.replace(",", "."))
        except (InvalidOperation, ValueError):
            return None

    if (
        decimal_value is None
        or not decimal_value.is_finite()
        or not Decimal("1") <= decimal_value <= Decimal("1001")
    ):
        return None

    normalized = format(decimal_value.normalize(), "f")
    return normalized.rstrip("0").rstrip(".") if "." in normalized else normalized
