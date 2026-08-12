from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from infrastructure.persistence.repositories.market.market_read_comparator import (
    compare_external_market_reads,
)
from infrastructure.persistence.repositories.market.market_read_models import (
    ExternalChoiceQuote,
    ExternalMarketQuoteBlock,
    MarketQuoteReadDiagnostic,
    QuoteFieldOrigin,
)


NOW = datetime(2026, 8, 12, tzinfo=timezone.utc)


def _legacy(*, initial="2", current="1.9", side=None, source="oddspapi"):
    return {
        "bookie_name": "Book",
        "market_name": "Result",
        "market_group": "1X2",
        "market_period": "Full Time",
        "choice_group": side,
        "source": source,
        "choices": [{"name": "1", "initial": initial, "current": current}],
    }


def _block(
    *,
    initial="2",
    current="1.9",
    side=None,
    source=None,
    captured_at=NOW,
):
    origin_source = source or "oddspapi"
    return ExternalMarketQuoteBlock(
        market_id=1,
        bookie_id=2,
        bookie_name="Book",
        market_name="Result",
        market_group="1X2",
        market_period="Full Time",
        choice_group=None,
        is_live=False,
        aggregation="exchange" if side else "field_priority",
        source=source,
        exchange_side=side,
        contributing_sources=(origin_source,),
        choices=(
            ExternalChoiceQuote(
                choice_id=10,
                choice_name="1",
                exchange_level=0 if side else None,
                initial=None if initial is None else Decimal(initial),
                current=None if current is None else Decimal(current),
                movement=-1,
                initial_origin=QuoteFieldOrigin(100, origin_source, captured_at),
                current_origin=QuoteFieldOrigin(100, origin_source, captured_at),
            ),
        ),
    )


def _codes(result):
    return [item.code for item in result.differences]


def test_equal_values_produce_only_equal():
    result = compare_external_market_reads(
        event_id=1, legacy_markets=[_legacy()], quote_blocks=[_block()]
    )
    assert _codes(result) == ["equal"]
    assert result.blocking_count == 0


def test_side_and_source_splits_are_expected_non_blocking():
    result = compare_external_market_reads(
        event_id=1,
        legacy_markets=[_legacy(side="Back", source="oddsportal")],
        quote_blocks=[
            _block(side="back", source="oddsportal"),
            _block(side="back", source="oddspapi", current="1.8"),
        ],
    )
    assert set(_codes(result)) == {"expected_side_split", "expected_source_split"}
    assert result.blocking_count == 0


def test_mismatch_before_stop_time_is_price_mismatch():
    result = compare_external_market_reads(
        event_id=1,
        legacy_markets=[_legacy(current="1.8")],
        quote_blocks=[_block(captured_at=NOW)],
        legacy_stop_write_at=NOW + timedelta(hours=1),
    )
    assert _codes(result) == ["price_mismatch"]
    assert result.blocking_count == 1


def test_mismatch_after_stop_time_is_expected_frozen_legacy():
    result = compare_external_market_reads(
        event_id=1,
        legacy_markets=[_legacy(current="1.8")],
        quote_blocks=[_block(captured_at=NOW)],
        legacy_stop_write_at=NOW - timedelta(hours=1),
    )
    assert _codes(result) == ["expected_frozen_legacy"]
    assert result.blocking_count == 0


def test_timestamped_mismatch_without_stop_time_is_unclassified_and_blocking():
    result = compare_external_market_reads(
        event_id=1,
        legacy_markets=[_legacy(current="1.8")],
        quote_blocks=[_block(captured_at=NOW)],
    )
    assert _codes(result) == ["unclassified_difference"]
    assert result.blocking_count == 1


def test_missing_quote_and_missing_choice_are_distinct():
    missing_quote = compare_external_market_reads(
        event_id=1, legacy_markets=[_legacy()], quote_blocks=[]
    )
    missing_choice = compare_external_market_reads(
        event_id=1,
        legacy_markets=[],
        quote_blocks=[_block(captured_at=None)],
    )
    assert _codes(missing_quote) == ["missing_quote"]
    assert _codes(missing_choice) == ["missing_choice"]


def test_quote_diagnostics_map_to_cutover_classes():
    diagnostics = (
        MarketQuoteReadDiagnostic("redundant_unsided_quote_suppressed", False, 1, 10, (100,)),
        MarketQuoteReadDiagnostic("unexpected_duplicate", True, 1, 10, (100, 101)),
        MarketQuoteReadDiagnostic("unexpected_level", True, 1, 10, (102,)),
    )
    result = compare_external_market_reads(
        event_id=1,
        legacy_markets=[_legacy()],
        quote_blocks=[_block()],
        quote_diagnostics=diagnostics,
    )
    assert _codes(result) == [
        "redundant_unsided_suppressed",
        "unexpected_duplicate",
        "ambiguous_quote",
    ]
    assert result.blocking_count == 2
