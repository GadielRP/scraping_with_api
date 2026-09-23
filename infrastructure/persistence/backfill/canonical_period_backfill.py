"""Cohort-based canonical-period backfill over already persisted odds.

This module deliberately does not participate in ingestion.  It uses the
existing canonical market catalog and applies one event per transaction.  A
caller supplies the external manifest/checkpoint lifecycle from
``checkpoint.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import selectinload

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event, Result
from infrastructure.persistence.odds_models import (
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)
from infrastructure.persistence.repositories.market.market_choice_quote_merge_policy import (
    QuoteCandidateState,
    QuoteMergeMode,
    decide_quote_merge,
    existing_state_from_quote,
)
from infrastructure.persistence.backfill.strategy import BackfillConflict


# These are existing catalog IDs. They are maintenance policy, not ingestion
# metadata. Every pair changes a regulation/full-time market to the equivalent
# decisive match market (overtime, extra innings, or the sport's equivalent).
FULL_TIME_PERIOD_VARIANT_PAIRS: dict[int, int] = {
    5: 8,
    12: 14,
    21: 23,
    22: 24,
    25: 27,
    33: 34,
}

# Backwards-compatible public name used by the original competition strategy.
PERIOD_VARIANT_PAIRS = FULL_TIME_PERIOD_VARIANT_PAIRS

DEFAULT_COMPETITION_IDS = frozenset({176, 129, 5153, 14861, 527, 526, 525, 429})


@dataclass(frozen=True)
class BackfillScope:
    competition_ids: frozenset[int] | None = None
    sport_names: frozenset[str] | None = None
    round_name: str | None = "regular_season"
    bookie_ids: frozenset[int] | None = None
    is_live: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "competition_ids": (
                sorted(self.competition_ids)
                if self.competition_ids is not None
                else None
            ),
            "sports": sorted(self.sport_names) if self.sport_names is not None else None,
            "round": self.round_name,
            "bookie_ids": sorted(self.bookie_ids) if self.bookie_ids is not None else None,
            "is_live": self.is_live,
        }


def _line_key(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _same_identity(left: Market, right: Market) -> bool:
    return (
        int(left.bookie_id) == int(right.bookie_id)
        and bool(left.is_live) == bool(right.is_live)
        and _line_key(left.line_value) == _line_key(right.line_value)
    )


def _quote_key(quote: MarketChoiceQuote) -> tuple[str, str | None, int]:
    return (
        str(quote.source),
        str(quote.exchange_side).strip().lower() if quote.exchange_side else None,
        int(quote.exchange_level or 0),
    )


def _quote_index(
    quotes: Iterable[MarketChoiceQuote],
) -> dict[tuple[str, str | None, int], MarketChoiceQuote]:
    indexed: dict[tuple[str, str | None, int], MarketChoiceQuote] = {}
    for quote in quotes:
        key = _quote_key(quote)
        if key in indexed:
            raise BackfillConflict(
                f"duplicate quote identity choice={quote.choice_id} key={key}"
            )
        indexed[key] = quote
    return indexed


def _snapshot_ids(markets: Iterable[Market]) -> set[int]:
    return {
        int(snapshot.snapshot_id)
        for market in markets
        for choice in market.choices
        for quote in choice.quotes
        for snapshot in quote.snapshots
        if snapshot.snapshot_id is not None
    }


class CanonicalPeriodBackfillService:
    """Audit and apply a fixed canonical-period cohort."""

    strategy_name = "canonical_period_backfill_by_fixed_competition_ids_v1"

    def __init__(
        self,
        *,
        scope: BackfillScope,
        period_pairs: dict[int, int] | None = None,
    ) -> None:
        self.scope = scope
        self.period_pairs = dict(period_pairs or PERIOD_VARIANT_PAIRS)

    def _candidate_event_query(
        self,
        session,
        *,
        cursor: dict[str, Any] | None = None,
        upper_bound: dict[str, Any] | None = None,
        limit: int | None = None,
    ):
        selectors = []
        if self.scope.competition_ids is not None:
            selectors.append(Event.competition_id.in_(self.scope.competition_ids))
        if self.scope.sport_names is not None:
            selectors.append(
                func.lower(func.trim(Event.sport)).in_(self.scope.sport_names)
            )
        if not selectors:
            raise ValueError("backfill scope must define competition_ids or sport_names")
        query = (
            session.query(Event.id, Event.starts_at)
            .join(Market, Market.event_id == Event.id)
            .filter(
                *selectors,
                Market.is_live.is_(self.scope.is_live),
                Market.market_type_id.in_(tuple(self.period_pairs)),
            )
        )
        if self.scope.round_name is not None:
            query = query.filter(Event.round == self.scope.round_name)
        if self.scope.bookie_ids is not None:
            query = query.filter(Market.bookie_id.in_(self.scope.bookie_ids))
        if cursor is not None:
            cursor_starts_at = datetime.fromisoformat(str(cursor["starts_at"]))
            cursor_event_id = int(cursor["event_id"])
            query = query.filter(
                or_(
                    Event.starts_at > cursor_starts_at,
                    and_(
                        Event.starts_at == cursor_starts_at,
                        Event.id > cursor_event_id,
                    ),
                )
            )
        if upper_bound is not None:
            upper_starts_at = datetime.fromisoformat(str(upper_bound["starts_at"]))
            upper_event_id = int(upper_bound["event_id"])
            query = query.filter(
                or_(
                    Event.starts_at < upper_starts_at,
                    and_(
                        Event.starts_at == upper_starts_at,
                        Event.id <= upper_event_id,
                    ),
                )
            )
        query = query.distinct().order_by(Event.starts_at.asc(), Event.id.asc())
        return query.limit(limit) if limit is not None else query

    def audit_metadata(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy_name,
            "scope": self.scope.as_dict(),
            "period_pairs": dict(sorted(self.period_pairs.items())),
            "parameters": {"period_pairs": dict(sorted(self.period_pairs.items()))},
        }

    def audit_upper_bound(self) -> dict[str, Any] | None:
        with db_manager.get_session() as session:
            row = self._candidate_event_query(session).order_by(None).order_by(
                Event.starts_at.desc(), Event.id.desc()
            ).first()
            if row is None:
                return None
            event_id, starts_at = row
            return {
                "starts_at": starts_at.isoformat(),
                "event_id": int(event_id),
            }

    def _load_event_markets(self, session, event_id: int) -> list[Market]:
        query = (
            session.query(Market)
            .options(
                selectinload(Market.choices)
                .selectinload(MarketChoice.quotes)
                .selectinload(MarketChoiceQuote.snapshots)
            )
            .filter(
                Market.event_id == int(event_id),
                Market.is_live.is_(self.scope.is_live),
                Market.market_type_id.in_(
                    tuple(set(self.period_pairs) | set(self.period_pairs.values()))
                ),
            )
            .order_by(Market.market_id.asc())
        )
        if self.scope.bookie_ids is not None:
            query = query.filter(Market.bookie_id.in_(self.scope.bookie_ids))
        return query.all()

    def _market_summary(self, market: Market, target: Market | None) -> dict[str, Any]:
        return {
            "market_id": int(market.market_id),
            "bookie_id": int(market.bookie_id),
            "old_market_type_id": int(market.market_type_id),
            "new_market_type_id": int(self.period_pairs[int(market.market_type_id)]),
            "line_value": str(market.line_value) if market.line_value is not None else None,
            "is_live": bool(market.is_live),
            "target_market_id": int(target.market_id) if target is not None else None,
            "choices": len(market.choices),
            "quotes": sum(len(choice.quotes) for choice in market.choices),
            "snapshots": len(_snapshot_ids([market])),
            "classification": "MERGE_REQUIRED" if target is not None else "COHORT_APPROVED",
        }

    def _event_guard_detail(self, session, event_id: int) -> str | None:
        """Optional strategy-specific fail-closed guard for one event."""
        return None

    def _event_guard_details(
        self, session, event_ids: Iterable[int]
    ) -> dict[int, str]:
        """Return guard failures for a page of events.

        The default preserves compatibility with strategies that only expose
        a per-event guard. Strategies with a database-backed guard can
        override this hook and perform one set-based query per audit page.
        """
        details: dict[int, str] = {}
        for event_id in event_ids:
            detail = self._event_guard_detail(session, int(event_id))
            if detail:
                details[int(event_id)] = detail
        return details

    def _audit_event(
        self,
        session,
        event_id: int,
        starts_at: datetime,
        *,
        guard_detail: str | None = None,
    ) -> dict[str, Any]:
        markets = self._load_event_markets(session, int(event_id))
        old_markets = [m for m in markets if int(m.market_type_id) in self.period_pairs]
        summaries = []
        for market in old_markets:
            target_type_id = self.period_pairs[int(market.market_type_id)]
            matching_targets = [
                candidate
                for candidate in markets
                if int(candidate.market_type_id) == target_type_id
                and _same_identity(market, candidate)
            ]
            if len(matching_targets) > 1:
                summaries.append(
                    {
                        **self._market_summary(market, None),
                        "classification": "CONFLICTING",
                        "detail": f"multiple target markets for market {market.market_id}",
                    }
                )
            else:
                summaries.append(
                    self._market_summary(
                        market, matching_targets[0] if matching_targets else None
                    )
                )
        event = {
            "event_id": int(event_id),
            "starts_at": starts_at.isoformat(),
            "status": (
                "CONFLICT"
                if guard_detail
                or any(item["classification"] == "CONFLICTING" for item in summaries)
                else "PENDING"
            ),
            "markets": summaries,
        }
        if guard_detail:
            event["guard_detail"] = guard_detail
        return event

    def _audit_events(
        self, session, rows: Iterable[tuple[int, datetime]]
    ) -> list[dict[str, Any]]:
        """Audit one bounded page while keeping page-level work set-based."""
        rows = list(rows)
        guard_details = self._event_guard_details(
            session, (int(event_id) for event_id, _ in rows)
        )
        return [
            self._audit_event(
                session,
                int(event_id),
                starts_at,
                guard_detail=guard_details.get(int(event_id)),
            )
            for event_id, starts_at in rows
        ]

    def audit_page(
        self,
        *,
        page_size: int,
        cursor: dict[str, Any] | None,
        upper_bound: dict[str, Any] | None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        if page_size < 1:
            raise ValueError("page_size must be greater than zero")
        with db_manager.get_session() as session:
            rows = self._candidate_event_query(
                session,
                cursor=cursor,
                upper_bound=upper_bound,
                limit=page_size,
            ).all()
            events = self._audit_events(session, rows)
        if not rows:
            return [], cursor
        event_id, starts_at = rows[-1]
        return events, {"starts_at": starts_at.isoformat(), "event_id": int(event_id)}

    def audit(self) -> dict[str, Any]:
        """Build a deterministic manifest without modifying the database."""
        events: list[dict[str, Any]] = []
        with db_manager.get_session() as session:
            for event_id, starts_at in self._candidate_event_query(session).all():
                events.extend(self._audit_events(session, [(event_id, starts_at)]))
        return {
            "strategy": self.strategy_name,
            "scope": self.scope.as_dict(),
            "period_pairs": dict(sorted(self.period_pairs.items())),
            "parameters": {
                "period_pairs": dict(sorted(self.period_pairs.items()))
            },
            "events": events,
        }

    @staticmethod
    def _merge_quote(session, source: MarketChoiceQuote, target: MarketChoiceQuote) -> None:
        decision = decide_quote_merge(
            existing=existing_state_from_quote(target),
            candidate=QuoteCandidateState(
                initial_price=source.initial_odds,
                initial_captured_at=source.initial_captured_at,
                current_price=source.current_odds,
                current_captured_at=source.current_updated_at,
                main_line=source.main_line,
                source_market_id=source.source_market_id,
                source_outcome_id=source.source_outcome_id,
                bookmaker_outcome_id=source.bookmaker_outcome_id,
                source_limit=source.source_limit,
            ),
            mode=QuoteMergeMode.BACKFILL_FILL_ONLY,
        )
        if decision.has_conflicts:
            raise BackfillConflict(
                f"quote collision source={source.quote_id} target={target.quote_id}: "
                f"{', '.join(decision.conflicts)}"
            )
        if decision.apply_initial:
            target.initial_odds = decision.initial_odds
            target.initial_captured_at = decision.initial_captured_at
        elif decision.apply_initial_timestamp_only:
            target.initial_captured_at = decision.initial_captured_at
        if decision.apply_current:
            target.current_odds = decision.current_odds
            target.current_updated_at = decision.current_updated_at
        if decision.apply_source_limit:
            target.source_limit = decision.source_limit
        for field_name, value in decision.metadata_updates.items():
            setattr(target, field_name, value)
        for snapshot in list(source.snapshots):
            snapshot.quote = target
        session.flush()
        session.delete(source)

    def _merge_market(self, session, source: Market, target: Market) -> None:
        if not _same_identity(source, target):
            raise BackfillConflict(
                f"market identity mismatch source={source.market_id} target={target.market_id}"
            )
        target_choices = {choice.choice_name: choice for choice in target.choices}
        for source_choice in list(source.choices):
            target_choice = target_choices.get(source_choice.choice_name)
            if target_choice is None:
                source_choice.market = target
                target_choices[source_choice.choice_name] = source_choice
                continue
            target_quotes = _quote_index(target_choice.quotes)
            _quote_index(source_choice.quotes)
            for source_quote in list(source_choice.quotes):
                target_quote = target_quotes.get(_quote_key(source_quote))
                if target_quote is None:
                    source_quote.choice = target_choice
                    target_quotes[_quote_key(source_quote)] = source_quote
                else:
                    self._merge_quote(session, source_quote, target_quote)
            session.flush()
            remaining_quotes = session.query(MarketChoiceQuote.quote_id).filter(
                MarketChoiceQuote.choice_id == source_choice.choice_id
            ).count()
            if remaining_quotes == 0:
                session.delete(source_choice)
        session.flush()
        remaining_choices = session.query(MarketChoice.choice_id).filter(
            MarketChoice.market_id == source.market_id
        ).count()
        if remaining_choices:
            raise BackfillConflict(f"source market {source.market_id} still has choices")
        session.delete(source)

    def _lock_event_rows(self, session, event_id: int) -> None:
        """Lock the event's market tree for the duration of one transaction."""
        session.query(Event.id).filter(Event.id == int(event_id)).with_for_update().all()
        market_query = session.query(Market.market_id).filter(
            Market.event_id == int(event_id),
            Market.is_live.is_(self.scope.is_live),
            Market.market_type_id.in_(
                tuple(set(self.period_pairs) | set(self.period_pairs.values()))
            ),
        )
        if self.scope.bookie_ids is not None:
            market_query = market_query.filter(Market.bookie_id.in_(self.scope.bookie_ids))
        market_ids = [int(row[0]) for row in market_query.with_for_update().all()]
        if not market_ids:
            return

        choice_ids = [
            int(row[0])
            for row in session.query(MarketChoice.choice_id)
            .filter(MarketChoice.market_id.in_(market_ids))
            .with_for_update()
            .all()
        ]
        if not choice_ids:
            return
        quote_ids = [
            int(row[0])
            for row in session.query(MarketChoiceQuote.quote_id)
            .filter(MarketChoiceQuote.choice_id.in_(choice_ids))
            .with_for_update()
            .all()
        ]
        if quote_ids:
            session.query(MarketChoiceSnapshot.snapshot_id).filter(
                MarketChoiceSnapshot.quote_id.in_(quote_ids)
            ).with_for_update().all()

    def _validate_event_scope(self, session, event_id: int) -> None:
        event = session.query(Event).filter(Event.id == int(event_id)).one_or_none()
        if event is None:
            raise BackfillConflict(f"event {event_id} no longer exists")
        if (
            self.scope.competition_ids is not None
            and event.competition_id not in self.scope.competition_ids
        ):
            raise BackfillConflict(f"event {event_id} changed competition scope")
        if (
            self.scope.sport_names is not None
            and str(event.sport).strip().lower() not in self.scope.sport_names
        ):
            raise BackfillConflict(f"event {event_id} changed sport scope")
        if self.scope.round_name is not None and event.round != self.scope.round_name:
            raise BackfillConflict(f"event {event_id} changed round scope")

    def apply_event(
        self,
        event_id: int,
        *,
        expected_state: dict[str, Any] | None = None,
        expected_market_ids: set[int] | None = None,
        expected_market_summaries: dict[int, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Apply one event atomically; callers checkpoint only after return."""
        if expected_state is not None:
            expected_markets = expected_state.get("markets", [])
            expected_market_ids = {
                int(market["market_id"]) for market in expected_markets
            }
            expected_market_summaries = {
                int(market["market_id"]): market for market in expected_markets
            }
        with db_manager.get_session() as session:
            self._lock_event_rows(session, int(event_id))
            self._validate_event_scope(session, int(event_id))
            guard_detail = self._event_guard_detail(session, int(event_id))
            if guard_detail:
                raise BackfillConflict(guard_detail)
            markets = self._load_event_markets(session, int(event_id))
            snapshot_ids_before = _snapshot_ids(markets)
            current_old_markets = [
                m for m in markets if int(m.market_type_id) in self.period_pairs
            ]
            if expected_market_ids is not None:
                unexpected = {
                    int(m.market_id)
                    for m in current_old_markets
                    if int(m.market_id) not in expected_market_ids
                }
                if unexpected:
                    raise BackfillConflict(
                        f"event {event_id} changed after audit; unexpected old markets="
                        f"{sorted(unexpected)}"
                    )
                old_markets = [
                    m for m in current_old_markets if int(m.market_id) in expected_market_ids
                ]
            else:
                old_markets = current_old_markets

            if expected_market_summaries is not None:
                for source in old_markets:
                    expected = expected_market_summaries.get(int(source.market_id))
                    if expected is None:
                        raise BackfillConflict(
                            f"event {event_id} is missing audit metadata for market "
                            f"{source.market_id}"
                        )
                    expected_line = _line_key(expected.get("line_value"))
                    if (
                        int(source.bookie_id) != int(expected["bookie_id"])
                        or int(source.market_type_id) != int(expected["old_market_type_id"])
                        or bool(source.is_live) != bool(expected["is_live"])
                        or _line_key(source.line_value) != expected_line
                    ):
                        raise BackfillConflict(
                            f"event {event_id} market {source.market_id} changed after audit"
                        )
                    expected_target_id = expected.get("target_market_id")
                    current_targets = [
                        candidate
                        for candidate in markets
                        if int(candidate.market_type_id)
                        == self.period_pairs[int(source.market_type_id)]
                        and _same_identity(source, candidate)
                    ]
                    if expected_target_id is None and current_targets:
                        raise BackfillConflict(
                            f"event {event_id} gained a target market after audit"
                        )
                    if expected_target_id is not None and (
                        len(current_targets) != 1
                        or int(current_targets[0].market_id) != int(expected_target_id)
                    ):
                        raise BackfillConflict(
                            f"event {event_id} target market {expected_target_id} changed"
                        )
            if not old_markets:
                return {"event_id": int(event_id), "status": "ALREADY_CORRECT", "markets": 0}

            changed = 0
            for source in list(old_markets):
                target_type_id = self.period_pairs[int(source.market_type_id)]
                targets = [
                    candidate
                    for candidate in markets
                    if candidate is not source
                    and int(candidate.market_type_id) == target_type_id
                    and _same_identity(source, candidate)
                ]
                if len(targets) > 1:
                    raise BackfillConflict(
                        f"event {event_id} has multiple targets for market {source.market_id}"
                    )
                if targets:
                    self._merge_market(session, source, targets[0])
                else:
                    source.market_type_id = target_type_id
                changed += 1
                session.flush()

            if snapshot_ids_before:
                retained = session.query(func.count(MarketChoiceSnapshot.snapshot_id)).filter(
                    MarketChoiceSnapshot.snapshot_id.in_(snapshot_ids_before)
                ).scalar()
                if int(retained or 0) != len(snapshot_ids_before):
                    raise BackfillConflict(
                        f"event {event_id} lost snapshots: before={len(snapshot_ids_before)} after={retained}"
                    )
            return {"event_id": int(event_id), "status": "APPLIED", "markets": changed}
