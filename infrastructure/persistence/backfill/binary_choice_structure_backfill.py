"""Canonical-period backfill selected by binary-choice (2-way) market structure.

This strategy targets sports and leagues (such as Ice Hockey across all
competitions, or other 2-way sports) where moneyline markets were persisted as
regulation/full-time (`home_away_full_time`, ID 5), but structurally represent
decisive match winners (`home_away_full_time_including_overtime`, ID 8).

When qualified, the strategy propagates the full-time period variant mapping
across all companion families (Totals, Handicaps, Team Totals, BTTS) using
`FULL_TIME_PERIOD_VARIANT_PAIRS`.

Guards:
- Persisted result winner = 'X' fails closed.
- Any Home/Away market (ID 5) must have strictly 2 choices.
- No choice in a Home/Away market may represent a draw/tie.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Iterable

from sqlalchemy import func
from sqlalchemy.orm import selectinload

from infrastructure.persistence.backfill.canonical_period_backfill import (
    FULL_TIME_PERIOD_VARIANT_PAIRS,
    BackfillScope,
    CanonicalPeriodBackfillService,
)
from infrastructure.persistence.models import Result
from infrastructure.persistence.odds_models import Market, MarketChoice

STRATEGY_NAME = "canonical_period_backfill_by_binary_choice_structure_v1"

DEFAULT_BINARY_CHOICE_SPORTS: frozenset[str] = frozenset({"ice hockey"})

DRAW_CHOICE_NAMES: frozenset[str] = frozenset(
    {"x", "draw", "tie", "empate", "nul", "unentschieden", "pareggio"}
)


class BinaryChoiceStructureBackfillService(CanonicalPeriodBackfillService):
    """Backfill decisive full-match markets validated by binary choice structure."""

    strategy_name = STRATEGY_NAME

    def __init__(
        self,
        *,
        scope: BackfillScope,
        period_pairs: dict[int, int] | None = None,
        require_binary_home_away: bool = False,
    ) -> None:
        normalized_sports = (
            frozenset(
                str(sport).strip().lower()
                for sport in scope.sport_names
                if str(sport).strip()
            )
            if scope.sport_names is not None
            else None
        )
        if normalized_sports is None and scope.competition_ids is None:
            normalized_sports = DEFAULT_BINARY_CHOICE_SPORTS

        normalized_scope = replace(scope, sport_names=normalized_sports)
        super().__init__(
            scope=normalized_scope,
            period_pairs=period_pairs or FULL_TIME_PERIOD_VARIANT_PAIRS,
        )
        self.require_binary_home_away = require_binary_home_away

    def audit_metadata(self) -> dict[str, Any]:
        base = super().audit_metadata()
        base["parameters"]["require_binary_home_away"] = self.require_binary_home_away
        return base

    def _validate_home_away_markets(
        self, markets: list[Market], event_id: int
    ) -> str | None:
        home_away_markets = [
            m for m in markets if int(m.market_type_id) in (5, 8)
        ]
        if self.require_binary_home_away and not home_away_markets:
            return (
                f"event {event_id} has no Home/Away market (ID 5 or 8) "
                "to anchor binary structure validation"
            )

        for market in home_away_markets:
            choices = list(market.choices)
            if len(choices) != 2:
                return (
                    f"event {event_id} market {market.market_id} (type {market.market_type_id}) "
                    f"has {len(choices)} choices; expected exactly 2 for binary Home/Away"
                )
            for choice in choices:
                name = str(choice.choice_name or "").strip().lower()
                if name in DRAW_CHOICE_NAMES:
                    return (
                        f"event {event_id} market {market.market_id} contains "
                        f"draw choice {choice.choice_name!r}; binary structure violated"
                    )
        return None

    def _event_guard_detail(self, session, event_id: int) -> str | None:
        winner = (
            session.query(Result.winner)
            .filter(Result.event_id == int(event_id))
            .scalar()
        )
        if str(winner or "").strip().upper() == "X":
            return (
                f"event {event_id} has persisted result winner='X'; "
                "binary choice structure is not safe for this event"
            )

        markets = (
            session.query(Market)
            .options(selectinload(Market.choices))
            .filter(
                Market.event_id == int(event_id),
                Market.market_type_id.in_((5, 8)),
                Market.is_live.is_(self.scope.is_live),
            )
            .all()
        )
        return self._validate_home_away_markets(markets, int(event_id))

    def _event_guard_details(
        self, session, event_ids: Iterable[int]
    ) -> dict[int, str]:
        ids = [int(event_id) for event_id in event_ids]
        if not ids:
            return {}

        guard_details: dict[int, str] = {}

        # 1. Result draw guard
        draw_ids = {
            int(event_id)
            for event_id, winner in session.query(Result.event_id, Result.winner)
            .filter(Result.event_id.in_(ids))
            .all()
            if str(winner or "").strip().upper() == "X"
        }
        for event_id in draw_ids:
            guard_details[event_id] = (
                f"event {event_id} has persisted result winner='X'; "
                "binary choice structure is not safe for this event"
            )

        # 2. Market choice structure guard
        remaining_ids = [eid for eid in ids if eid not in guard_details]
        if remaining_ids:
            markets = (
                session.query(Market)
                .options(selectinload(Market.choices))
                .filter(
                    Market.event_id.in_(remaining_ids),
                    Market.market_type_id.in_((5, 8)),
                    Market.is_live.is_(self.scope.is_live),
                )
                .all()
            )
            markets_by_event: dict[int, list[Market]] = {
                eid: [] for eid in remaining_ids
            }
            for m in markets:
                markets_by_event[int(m.event_id)].append(m)

            for event_id, event_markets in markets_by_event.items():
                detail = self._validate_home_away_markets(event_markets, event_id)
                if detail:
                    guard_details[event_id] = detail

        return guard_details
