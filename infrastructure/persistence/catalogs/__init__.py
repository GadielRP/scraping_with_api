"""Static persistence catalogs used to seed canonical reference data."""

from .canonical_market_types import (
    CANONICAL_MARKET_TYPE_SEEDS,
    get_canonical_market_type_seed,
    get_canonical_market_type_seeds,
)

__all__ = [
    "CANONICAL_MARKET_TYPE_SEEDS",
    "get_canonical_market_type_seed",
    "get_canonical_market_type_seeds",
]
