"""Reusable, declarative extraction of single-minute market snapshots.

This module owns the mechanics shared by every pillar: target-minute
selection, canonical market matching, bookmaker/container selection, choice
lookup, scalar validation and quote lineage. Pillars remain responsible for
assembling their domain snapshot and applying their own completeness policy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from modules.pillars.odds_trajectory_context import (
    BookieOddsTrajectory,
    ChoiceOddsTrajectory,
    MarketLineOddsTrajectory,
    OddsTrajectoryContext,
)


# Hardcoded development/simulation overrides. ``None`` preserves the flow's
# normal selection policy. A configured minute is strict and never falls back.
HARDCODED_TARGET_MINUTE_BY_FLOW: dict[str, int | None] = {
    "pillar_2": None,
    "pillar_3": None,
}


@dataclass(frozen=True)
class QuoteTrace:
    target_minute: int
    snapshot_id: int | None
    collected_at: datetime | None
    changed_at: datetime | None
    minutes_before_start: int | None
    quote_id: int | None
    market_group: str
    market_period: str
    market_name: str
    choice_group: str | None
    bookie_id: int | None
    bookie_name: str
    source: str | None
    exchange_side: str | None
    exchange_level: int
    choice_name: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_minute": self.target_minute,
            "snapshot_id": self.snapshot_id,
            "collected_at": self.collected_at.isoformat() if self.collected_at else None,
            "changed_at": self.changed_at.isoformat() if self.changed_at else None,
            "minutes_before_start": self.minutes_before_start,
            "quote_id": self.quote_id,
            "market_group": self.market_group,
            "market_period": self.market_period,
            "market_name": self.market_name,
            "choice_group": self.choice_group,
            "bookie_id": self.bookie_id,
            "bookie_name": self.bookie_name,
            "source": self.source,
            "exchange_side": self.exchange_side,
            "exchange_level": self.exchange_level,
            "choice_name": self.choice_name,
        }


@dataclass(frozen=True)
class QuotePoint:
    odds_price: Decimal
    exchange_size: Decimal | None
    trace: QuoteTrace


@dataclass(frozen=True)
class MarketIdentity:
    market_group: str
    market_period: str
    market_name: str


@dataclass(frozen=True)
class ChoiceRequest:
    key: str
    choice_name: str
    input_name: str
    exchange_size_input_name: str | None = None


@dataclass(frozen=True)
class MarketSnapshotRequest:
    identities: tuple[MarketIdentity, ...]
    bookie_id: int
    choices: tuple[ChoiceRequest, ...]
    line_input_name: str | None = None
    exchange_side: str | None = None
    exchange_level: int = 0


@dataclass(frozen=True)
class MarketCandidate:
    market_line: MarketLineOddsTrajectory
    bookie: BookieOddsTrajectory
    line: Decimal | None
    choices: dict[str, QuotePoint | None]

    @property
    def market_period(self) -> str:
        return self.market_line.market_period

    def is_complete(self, request: MarketSnapshotRequest) -> bool:
        if request.line_input_name is not None and self.line is None:
            return False
        return all(self.choices.get(choice.key) is not None for choice in request.choices)


@dataclass(frozen=True)
class MarketSnapshotExtraction:
    target_minute: int
    candidates: tuple[MarketCandidate, ...]
    missing_inputs: tuple[str, ...] = ()
    invalid_inputs: tuple[str, ...] = ()
    ambiguous_inputs: tuple[str, ...] = ()
    container_ambiguities: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class TargetMinuteSelection:
    target_minute: int | None
    reason: str | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)


def _normalize(value: object) -> str:
    return " ".join(str(value or "").replace("-", " ").casefold().split())


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def select_target_minute(
    context: OddsTrajectoryContext | None,
    *,
    flow_id: str,
    expected_event_id: int | None = None,
    allowed_target_minutes: Iterable[int] | None = None,
) -> TargetMinuteSelection:
    """Select one strict minute for all requests made by a flow."""
    if context is None:
        return TargetMinuteSelection(None, "missing_odds_trajectory_context")
    if not context.available:
        return TargetMinuteSelection(None, "odds_trajectory_unavailable")
    if (
        expected_event_id is not None
        and context.event_id is not None
        and int(context.event_id) != int(expected_event_id)
    ):
        return TargetMinuteSelection(
            None,
            "event_id_mismatch",
            {"trajectory_event_id": context.event_id},
        )

    override = HARDCODED_TARGET_MINUTE_BY_FLOW.get(flow_id)
    if override is not None:
        return TargetMinuteSelection(
            int(override),
            diagnostics={"selection": "hardcoded_override", "flow_id": flow_id},
        )

    allowed = (
        None
        if allowed_target_minutes is None
        else {int(minute) for minute in allowed_target_minutes}
    )
    candidates = [
        int(minute)
        for minute in context.target_minutes_present
        if allowed is None or int(minute) in allowed
    ]
    if not candidates:
        return TargetMinuteSelection(
            None,
            "no_target_minutes_present",
            {
                "flow_id": flow_id,
                "target_minutes_present": list(context.target_minutes_present),
                "allowed_target_minutes": sorted(allowed) if allowed is not None else None,
            },
        )
    return TargetMinuteSelection(
        min(candidates),
        diagnostics={"selection": "latest_available", "flow_id": flow_id},
    )


def _matches_identity(
    market_line: MarketLineOddsTrajectory,
    identities: tuple[MarketIdentity, ...],
) -> bool:
    actual = (
        _normalize(market_line.market_group),
        _normalize(market_line.market_period),
        _normalize(market_line.market_name),
    )
    return any(
        actual
        == (
            _normalize(identity.market_group),
            _normalize(identity.market_period),
            _normalize(identity.market_name),
        )
        for identity in identities
    )


def _iter_market_lines(
    context: OddsTrajectoryContext,
) -> Iterable[MarketLineOddsTrajectory]:
    for periods in context.markets.values():
        for market_names in periods.values():
            for market_lines in market_names.values():
                yield from market_lines.values()


def _matching_choices(
    bookie: BookieOddsTrajectory,
    choice_name: str,
) -> list[ChoiceOddsTrajectory]:
    expected = _normalize(choice_name)
    return [
        choice
        for current_name, choice in bookie.choices.items()
        if _normalize(current_name) == expected
    ]


def _read_quote(
    *,
    market_line: MarketLineOddsTrajectory,
    bookie: BookieOddsTrajectory,
    request: ChoiceRequest,
    target_minute: int,
    missing: set[str],
    invalid: set[str],
    ambiguous: set[str],
) -> QuotePoint | None:
    choices = _matching_choices(bookie, request.choice_name)
    if not choices:
        missing.add(request.input_name)
        if request.exchange_size_input_name:
            missing.add(request.exchange_size_input_name)
        return None
    if len(choices) > 1:
        ambiguous.add(request.input_name)
        if request.exchange_size_input_name:
            ambiguous.add(request.exchange_size_input_name)
        return None

    choice = choices[0]
    if target_minute not in choice.odds_values:
        missing.add(request.input_name)
        if request.exchange_size_input_name:
            missing.add(request.exchange_size_input_name)
        return None
    odds_price = _decimal(choice.odds_values[target_minute])
    if odds_price is None or odds_price <= 0:
        invalid.add(request.input_name)
        return None

    meta = choice.meta_by_minute.get(target_minute)
    exchange_size = _decimal(getattr(meta, "exchange_size", None))
    if request.exchange_size_input_name:
        if exchange_size is None:
            missing.add(request.exchange_size_input_name)
            return None
        if exchange_size < 0:
            invalid.add(request.exchange_size_input_name)
            return None

    return QuotePoint(
        odds_price=odds_price,
        exchange_size=exchange_size,
        trace=QuoteTrace(
            target_minute=target_minute,
            snapshot_id=getattr(meta, "snapshot_id", None),
            collected_at=getattr(meta, "collected_at", None),
            changed_at=getattr(meta, "changed_at", None),
            minutes_before_start=getattr(meta, "minutes_before_start", None),
            quote_id=getattr(meta, "quote_id", None) or choice.quote_id,
            market_group=market_line.market_group,
            market_period=market_line.market_period,
            market_name=market_line.market_name,
            choice_group=market_line.choice_group,
            bookie_id=bookie.bookie_id,
            bookie_name=bookie.bookie_name,
            source=bookie.source,
            exchange_side=bookie.exchange_side,
            exchange_level=bookie.exchange_level,
            choice_name=choice.choice_name,
        ),
    )


def extract_market_snapshot(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    request: MarketSnapshotRequest,
) -> MarketSnapshotExtraction:
    """Extract every structural candidate matching a declarative request."""
    missing: set[str] = set()
    invalid: set[str] = set()
    ambiguous: set[str] = set()
    candidates: list[MarketCandidate] = []
    container_ambiguities: list[dict[str, Any]] = []

    for market_line in _iter_market_lines(context):
        if not _matches_identity(market_line, request.identities):
            continue
        matching_bookies = [
            bookie
            for bookie in market_line.bookies.values()
            if bookie.bookie_id == request.bookie_id
            and bookie.exchange_side == request.exchange_side
            and bookie.exchange_level == request.exchange_level
        ]
        if len(matching_bookies) > 1:
            affected = {choice.input_name for choice in request.choices}
            affected.update(
                choice.exchange_size_input_name
                for choice in request.choices
                if choice.exchange_size_input_name
            )
            if request.line_input_name:
                affected.add(request.line_input_name)
            ambiguous.update(affected)
            container_ambiguities.append(
                {
                    "market_group": market_line.market_group,
                    "market_period": market_line.market_period,
                    "market_name": market_line.market_name,
                    "choice_group": market_line.choice_group,
                    "bookie_id": request.bookie_id,
                    "sources": sorted(
                        str(bookie.source or "unknown")
                        for bookie in matching_bookies
                    ),
                }
            )
            continue
        if not matching_bookies:
            continue

        bookie = matching_bookies[0]
        line = None
        if request.line_input_name:
            line = _decimal(market_line.choice_group)
            if line is None:
                if market_line.choice_group is None or not str(market_line.choice_group).strip():
                    missing.add(request.line_input_name)
                else:
                    invalid.add(request.line_input_name)

        points = {
            choice_request.key: _read_quote(
                market_line=market_line,
                bookie=bookie,
                request=choice_request,
                target_minute=target_minute,
                missing=missing,
                invalid=invalid,
                ambiguous=ambiguous,
            )
            for choice_request in request.choices
        }
        candidates.append(
            MarketCandidate(
                market_line=market_line,
                bookie=bookie,
                line=line,
                choices=points,
            )
        )

    if not candidates and not container_ambiguities:
        if request.line_input_name:
            missing.add(request.line_input_name)
        for choice in request.choices:
            missing.add(choice.input_name)
            if choice.exchange_size_input_name:
                missing.add(choice.exchange_size_input_name)

    return MarketSnapshotExtraction(
        target_minute=target_minute,
        candidates=tuple(candidates),
        missing_inputs=tuple(sorted(missing)),
        invalid_inputs=tuple(sorted(invalid)),
        ambiguous_inputs=tuple(sorted(ambiguous)),
        container_ambiguities=tuple(container_ambiguities),
    )


__all__ = [
    "HARDCODED_TARGET_MINUTE_BY_FLOW",
    "ChoiceRequest",
    "MarketCandidate",
    "MarketIdentity",
    "MarketSnapshotExtraction",
    "MarketSnapshotRequest",
    "QuotePoint",
    "QuoteTrace",
    "TargetMinuteSelection",
    "extract_market_snapshot",
    "select_target_minute",
]
