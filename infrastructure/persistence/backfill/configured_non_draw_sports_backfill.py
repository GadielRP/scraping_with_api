"""Canonical-period backfill selected by explicitly configured sports.

The mutation logic and persisted-draw guard are inherited from
``CanonicalPeriodBackfillService``. This module only changes the population
selector.
"""

from __future__ import annotations

from dataclasses import replace
from infrastructure.persistence.backfill.canonical_period_backfill import (
    FULL_TIME_PERIOD_VARIANT_PAIRS,
    BackfillScope,
    CanonicalPeriodBackfillService,
)


STRATEGY_NAME = "canonical_period_backfill_by_configured_non_draw_sports_v1"

# Conservative first cohort. These sports decide a completed match, but the
# list remains configurable so competition-specific exceptions can be handled
# without changing code or the ingestion schema.
DEFAULT_NON_DRAW_SPORTS = frozenset(
    {"basketball", "tennis", "tennis doubles", "volleyball"}
)


class ConfiguredNonDrawSportsBackfillService(CanonicalPeriodBackfillService):
    """Backfill decisive full-match markets for configured no-draw sports."""

    strategy_name = STRATEGY_NAME

    def __init__(
        self,
        *,
        scope: BackfillScope,
        period_pairs: dict[int, int] | None = None,
    ) -> None:
        if not scope.sport_names:
            raise ValueError("non-draw sports strategy requires at least one sport")
        normalized_sports = frozenset(
            str(sport).strip().lower()
            for sport in scope.sport_names
            if str(sport).strip()
        )
        if not normalized_sports:
            raise ValueError("non-draw sports strategy requires at least one sport")
        normalized_scope = replace(scope, sport_names=normalized_sports)
        super().__init__(
            scope=normalized_scope,
            period_pairs=period_pairs or FULL_TIME_PERIOD_VARIANT_PAIRS,
        )
