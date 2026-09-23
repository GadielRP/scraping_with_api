"""Canonical-period backfill selected by explicitly configured sports.

The mutation logic is inherited from ``CanonicalPeriodBackfillService``. This
module only changes the population selector and adds a fail-closed guard for
events whose persisted result is explicitly marked as a draw.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from infrastructure.persistence.backfill.canonical_period_backfill import (
    FULL_TIME_PERIOD_VARIANT_PAIRS,
    BackfillScope,
    CanonicalPeriodBackfillService,
)
from infrastructure.persistence.models import Result


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

    def _event_guard_detail(self, session, event_id: int) -> str | None:
        winner = (
            session.query(Result.winner)
            .filter(Result.event_id == int(event_id))
            .scalar()
        )
        if str(winner or "").strip().upper() == "X":
            return (
                f"event {event_id} has persisted result winner='X'; "
                "sport configuration is not safe for this event"
            )
        return None

    def _event_guard_details(
        self, session, event_ids: Iterable[int]
    ) -> dict[int, str]:
        """Check all results in one query for the current audit page."""
        ids = [int(event_id) for event_id in event_ids]
        if not ids:
            return {}
        draw_ids = {
            int(event_id)
            for event_id, winner in session.query(Result.event_id, Result.winner)
            .filter(Result.event_id.in_(ids))
            .all()
            if str(winner or "").strip().upper() == "X"
        }
        return {
            event_id: (
                f"event {event_id} has persisted result winner='X'; "
                "sport configuration is not safe for this event"
            )
            for event_id in draw_ids
        }
