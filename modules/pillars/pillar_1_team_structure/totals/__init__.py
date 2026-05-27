"""P1 Totals package."""

from .totals import (
    P1TotalsLayerOutput,
    P1TotalsOutput,
    TOTALS_TEMPORAL_WINDOW_CONFIG,
    TOTALS_TEMPORAL_WINDOW_NAMES,
    _resolve_totals_temporal_window_config,
    _resolve_window_size_from_ratio,
    calculate_p1_totals,
)

__all__ = [
    "P1TotalsLayerOutput",
    "P1TotalsOutput",
    "TOTALS_TEMPORAL_WINDOW_CONFIG",
    "TOTALS_TEMPORAL_WINDOW_NAMES",
    "_resolve_totals_temporal_window_config",
    "_resolve_window_size_from_ratio",
    "calculate_p1_totals",
]
