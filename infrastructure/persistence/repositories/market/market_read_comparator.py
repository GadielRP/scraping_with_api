"""Pure legacy-versus-quotes comparison used during the read cutover."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Iterable, Optional

from infrastructure.persistence.repositories.market.market_read_models import (
    ExternalMarketQuoteBlock,
    MarketQuoteReadDiagnostic,
    MarketReadShadowComparison,
    MarketReadShadowDifference,
)


@dataclass(frozen=True, slots=True)
class _ComparableChoice:
    identity: tuple
    initial: Optional[Decimal]
    current: Optional[Decimal]
    source: Optional[str]
    captured_at: Optional[datetime]
    legacy_side_encoding: bool = False


def _decimal(value) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.001"))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _identity_text(identity: tuple) -> str:
    return "|".join("" if item is None else str(item) for item in identity)


def _legacy_values(markets: Iterable[dict]) -> list[_ComparableChoice]:
    result = []
    for market in markets:
        side = None
        choice_group = market.get("choice_group")
        side_encoded = str(choice_group or "").casefold() in {"back", "lay"}
        if side_encoded:
            side = str(choice_group).casefold()
            choice_group = None
        for choice in market.get("choices", []):
            result.append(
                _ComparableChoice(
                    identity=(
                        market.get("bookie_name"),
                        market.get("market_name"),
                        market.get("market_group"),
                        market.get("market_period"),
                        choice_group,
                        side,
                        choice.get("name"),
                    ),
                    initial=_decimal(choice.get("initial")),
                    current=_decimal(choice.get("current")),
                    source=str(market.get("source") or "").casefold() or None,
                    captured_at=None,
                    legacy_side_encoding=side_encoded,
                )
            )
    return result


def _quote_values(blocks: Iterable[ExternalMarketQuoteBlock]) -> list[_ComparableChoice]:
    result = []
    for block in blocks:
        for choice in block.choices:
            origins = tuple(
                origin
                for origin in (choice.initial_origin, choice.current_origin)
                if origin is not None
            )
            timestamps = [origin.captured_at for origin in origins if origin.captured_at]
            sources = {origin.source for origin in origins}
            source = block.source
            if source is None and len(sources) == 1:
                source = next(iter(sources))
            result.append(
                _ComparableChoice(
                    identity=(
                        block.bookie_name,
                        block.market_name,
                        block.market_group,
                        block.market_period,
                        block.choice_group,
                        block.exchange_side,
                        choice.choice_name,
                    ),
                    initial=_decimal(choice.initial),
                    current=_decimal(choice.current),
                    source=source,
                    captured_at=max(timestamps) if timestamps else None,
                )
            )
    return result


def _difference_code(
    *,
    timestamp: Optional[datetime],
    legacy_stop_write_at: Optional[datetime],
    default_code: str,
) -> tuple[str, bool]:
    if legacy_stop_write_at and timestamp and timestamp >= legacy_stop_write_at:
        return "expected_frozen_legacy", False
    if legacy_stop_write_at is None and timestamp is not None:
        return "unclassified_difference", True
    return default_code, True


def _append_diagnostic_differences(
    differences: list[MarketReadShadowDifference],
    diagnostics: Iterable[MarketQuoteReadDiagnostic],
) -> None:
    mapping = {
        "redundant_unsided_quote_suppressed": (
            "redundant_unsided_suppressed",
            False,
        ),
        "unexpected_duplicate": ("unexpected_duplicate", True),
        "unexpected_level": ("ambiguous_quote", True),
        "unsided_quote_in_exchange_market": ("ambiguous_quote", True),
    }
    for diagnostic in diagnostics:
        mapped = mapping.get(diagnostic.code)
        if mapped is None:
            continue
        code, blocking = mapped
        identity = (
            f"market={diagnostic.market_id}|choice={diagnostic.choice_id}|"
            f"quotes={','.join(map(str, diagnostic.quote_ids))}"
        )
        differences.append(
            MarketReadShadowDifference(
                code=code,
                blocking=blocking,
                identity=identity,
                detail=diagnostic.detail,
            )
        )


def compare_external_market_reads(
    *,
    event_id: int,
    legacy_markets: Iterable[dict],
    quote_blocks: Iterable[ExternalMarketQuoteBlock],
    quote_diagnostics: Iterable[MarketQuoteReadDiagnostic] = (),
    legacy_stop_write_at: Optional[datetime] = None,
    legacy_duration_ms: float = 0.0,
    quote_duration_ms: float = 0.0,
) -> MarketReadShadowComparison:
    legacy_list = list(legacy_markets)
    quote_list = list(quote_blocks)
    legacy_records = _legacy_values(legacy_list)
    quote_records = _quote_values(quote_list)
    legacy_by_identity: dict[tuple, list[_ComparableChoice]] = {}
    quotes_by_identity: dict[tuple, list[_ComparableChoice]] = {}
    for record in legacy_records:
        legacy_by_identity.setdefault(record.identity, []).append(record)
    for record in quote_records:
        quotes_by_identity.setdefault(record.identity, []).append(record)

    differences: list[MarketReadShadowDifference] = []
    _append_diagnostic_differences(differences, quote_diagnostics)

    for identity in sorted(set(legacy_by_identity) | set(quotes_by_identity), key=str):
        identity_text = _identity_text(identity)
        legacy_group = legacy_by_identity.get(identity, [])
        quote_group = quotes_by_identity.get(identity, [])
        if not legacy_group:
            for quote in quote_group:
                code, blocking = _difference_code(
                    timestamp=quote.captured_at,
                    legacy_stop_write_at=legacy_stop_write_at,
                    default_code="missing_choice",
                )
                differences.append(
                    MarketReadShadowDifference(code, blocking, identity_text)
                )
            continue
        if not quote_group:
            differences.append(
                MarketReadShadowDifference("missing_quote", True, identity_text)
            )
            continue

        legacy = legacy_group[0]
        if legacy.legacy_side_encoding:
            differences.append(
                MarketReadShadowDifference(
                    "expected_side_split", False, identity_text
                )
            )

        quote = next(
            (
                item
                for item in quote_group
                if legacy.source is not None and item.source == legacy.source
            ),
            quote_group[0],
        )
        if len(quote_group) > 1:
            for extra in quote_group:
                if extra is quote:
                    continue
                differences.append(
                    MarketReadShadowDifference(
                        "expected_source_split",
                        False,
                        f"{identity_text}|source={extra.source or 'mixed'}",
                    )
                )

        legacy_value = (legacy.initial, legacy.current)
        quote_value = (quote.initial, quote.current)
        if legacy_value != quote_value:
            code, blocking = _difference_code(
                timestamp=quote.captured_at,
                legacy_stop_write_at=legacy_stop_write_at,
                default_code="price_mismatch",
            )
            differences.append(
                MarketReadShadowDifference(
                    code=code,
                    blocking=blocking,
                    identity=identity_text,
                    detail=f"legacy={legacy_value} quotes={quote_value}",
                )
            )
        if len(legacy_group) > 1:
            differences.append(
                MarketReadShadowDifference(
                    "unexpected_duplicate", True, identity_text, "legacy read duplicate"
                )
            )

    if not differences:
        differences.append(MarketReadShadowDifference("equal", False, "event"))
    return MarketReadShadowComparison(
        event_id=int(event_id),
        legacy_blocks=len(legacy_list),
        quote_blocks=len(quote_list),
        differences=tuple(differences),
        legacy_duration_ms=legacy_duration_ms,
        quote_duration_ms=quote_duration_ms,
    )


__all__ = ["compare_external_market_reads"]
