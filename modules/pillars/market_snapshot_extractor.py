"""Reusable, declarative extraction of single-minute market snapshots.

This module owns declarative, single-minute market extraction mechanics:
canonical market matching, bookmaker/container selection, choice lookup,
scalar validation and quote lineage. Target selection is owned by the shared
trajectory-selection policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from modules.pillars.odds_trajectory_context import (
    BookieOddsTrajectory,
    ChoiceOddsTrajectory,
    MarketLineOddsTrajectory,
    OddsTrajectoryContext,
)


@dataclass(frozen=True, slots=True)
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
    line_value: str | None
    bookie_id: int | None
    bookie_name: str
    source: str | None
    exchange_side: str | None
    exchange_level: int
    choice_name: str
    canonical_market_key: str | None = None
    market_type_id: int | None = None
    is_live: bool = False
    market_id: int | None = None
    source_collected_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_minute": self.target_minute,
            "snapshot_id": self.snapshot_id,
            "collected_at": (
                self.collected_at.isoformat() if self.collected_at else None
            ),
            "changed_at": self.changed_at.isoformat() if self.changed_at else None,
            "minutes_before_start": self.minutes_before_start,
            "quote_id": self.quote_id,
            "market_group": self.market_group,
            "market_period": self.market_period,
            "market_name": self.market_name,
            "line_value": self.line_value,
            "bookie_id": self.bookie_id,
            "bookie_name": self.bookie_name,
            "source": self.source,
            "exchange_side": self.exchange_side,
            "exchange_level": self.exchange_level,
            "choice_name": self.choice_name,
            "canonical_market_key": self.canonical_market_key,
            "market_type_id": self.market_type_id,
            "is_live": self.is_live,
            "market_id": self.market_id,
            "source_collected_at": (
                self.source_collected_at.isoformat()
                if self.source_collected_at
                else None
            ),
        }


@dataclass(frozen=True, slots=True)
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


@dataclass(frozen=True, slots=True)
class MarketCandidate:
    market_line: MarketLineOddsTrajectory
    bookie: BookieOddsTrajectory
    line: Decimal | None
    choices: dict[str, QuotePoint | None]

    def contract_trace(self, target_minute: int) -> dict[str, Any]:
        """Line provenance is usable independently from optional outcome prices."""
        line, book = self.market_line, self.bookie
        meta = next(
            (
                choice.meta_by_minute[target_minute]
                for choice in book.choices.values()
                if target_minute in choice.meta_by_minute
            ),
            None,
        )
        return {
            "target_minute": target_minute,
            "market_group": line.market_group,
            "market_period": line.market_period,
            "market_name": line.market_name,
            "line_value": line.line_value,
            "canonical_market_key": line.canonical_market_key,
            "market_type_id": line.market_type_id,
            "is_live": line.is_live,
            "market_id": book.market_id or line.market_id,
            "bookie_id": book.bookie_id,
            "bookie_name": book.bookie_name,
            "source": book.source,
            "exchange_side": book.exchange_side,
            "exchange_level": book.exchange_level,
            "snapshot_id": getattr(meta, "snapshot_id", None),
            "quote_id": getattr(meta, "quote_id", None),
            "collected_at": (
                meta.collected_at.isoformat() if meta and meta.collected_at else None
            ),
            "source_collected_at": (
                meta.changed_at.isoformat() if meta and meta.changed_at else None
            ),
        }

    @property
    def market_period(self) -> str:
        return self.market_line.market_period

    def is_complete(self, request: MarketSnapshotRequest) -> bool:
        if request.line_input_name is not None and self.line is None:
            return False
        return all(
            self.choices.get(choice.key) is not None for choice in request.choices
        )


@dataclass(frozen=True)
class MarketSnapshotExtraction:
    target_minute: int
    candidates: tuple[MarketCandidate, ...]
    missing_inputs: tuple[str, ...] = ()
    invalid_inputs: tuple[str, ...] = ()
    ambiguous_inputs: tuple[str, ...] = ()
    container_ambiguities: tuple[dict[str, Any], ...] = ()


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


def _identity_key(
    identity: MarketIdentity | MarketLineOddsTrajectory,
) -> tuple[str, str, str]:
    return (
        _normalize(identity.market_group),
        _normalize(identity.market_period),
        _normalize(identity.market_name),
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
        for choice in bookie.choices.values()
        if _normalize(choice.choice_name) == expected and choice.main_line is not False
    ]


def _read_projected_quote(
    *,
    market_line: MarketLineOddsTrajectory,
    bookie: BookieOddsTrajectory,
    request: ChoiceRequest,
    target_minute: int,
    missing: set[str],
    invalid: set[str],
    ambiguous: set[str],
) -> QuotePoint | None:
    """Read one configured-target projection without scanning raw snapshots."""
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
    if odds_price is None or odds_price <= 1:
        invalid.add(request.input_name)
        return None

    meta = choice.meta_by_minute.get(target_minute)
    exchange_size = _decimal(getattr(meta, "exchange_size", None))
    if exchange_size is not None and exchange_size < 0:
        invalid.add(
            request.exchange_size_input_name or request.input_name + ":exchange_size"
        )
        exchange_size = None
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
            line_value=market_line.line_value,
            bookie_id=bookie.bookie_id,
            bookie_name=bookie.bookie_name,
            source=bookie.source,
            exchange_side=bookie.exchange_side,
            exchange_level=bookie.exchange_level,
            choice_name=choice.choice_name,
            canonical_market_key=market_line.canonical_market_key,
            market_type_id=market_line.market_type_id,
            is_live=market_line.is_live,
            market_id=bookie.market_id or market_line.market_id,
            source_collected_at=getattr(meta, "changed_at", None),
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

    # Normalize the small request vocabulary once, instead of once per line.
    identities = {_identity_key(identity) for identity in request.identities}
    pairs = {
        (_normalize(i.market_group), _normalize(i.market_period))
        for i in request.identities
    }
    lines = (
        (line for pair in pairs for line in context.market_index.get(pair, ()))
        if context.market_index
        else _iter_market_lines(context)
    )
    for market_line in lines:
        if context.market_index:
            if (
                _normalize(market_line.market_group),
                _normalize(market_line.market_period),
            ) not in pairs:
                continue
        elif _identity_key(market_line) not in identities:
            continue
        matching_bookies = [
            bookie
            for bookie in market_line.bookies.values()
            if bookie.bookie_id == request.bookie_id
            and bookie.exchange_side == request.exchange_side
            and (
                bool(context.market_index)
                or bookie.exchange_level == request.exchange_level
            )
        ]
        # Exchange depth belongs to an observation, not to the contract. The
        # provider may expose the best level at different depths per outcome.
        sources = {bookie.source for bookie in matching_bookies}
        levels = [bookie.exchange_level for bookie in matching_bookies]
        if len(matching_bookies) > 1 and (
            not context.market_index
            or len(sources) > 1
            or len(set(levels)) != len(levels)
        ):
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
                    "line_value": market_line.line_value,
                    "bookie_id": request.bookie_id,
                    "sources": sorted(
                        str(bookie.source or "unknown") for bookie in matching_bookies
                    ),
                }
            )
            continue
        if not matching_bookies:
            continue

        bookie = min(matching_bookies, key=lambda item: item.exchange_level)
        line = None
        if request.line_input_name:
            line = _decimal(market_line.line_value)
            if line is None:
                if (
                    market_line.line_value is None
                    or not str(market_line.line_value).strip()
                ):
                    missing.add(request.line_input_name)
                else:
                    invalid.add(request.line_input_name)

        points = {
            choice_request.key: _read_projected_quote(
                market_line=market_line,
                bookie=min(
                    (
                        item
                        for item in matching_bookies
                        if any(
                            target_minute in choice.odds_values
                            for choice in _matching_choices(
                                item, choice_request.choice_name
                            )
                        )
                    ),
                    key=lambda item: item.exchange_level,
                    default=bookie,
                ),
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
    "ChoiceRequest",
    "MarketCandidate",
    "MarketIdentity",
    "MarketSnapshotExtraction",
    "MarketSnapshotRequest",
    "QuotePoint",
    "QuoteTrace",
    "extract_market_snapshot",
]
