from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from modules.pillars import market_snapshot_extractor
from modules.pillars.market_snapshot_extractor import (
    ChoiceRequest,
    MarketIdentity,
    MarketSnapshotRequest,
    extract_market_snapshot,
    select_target_minute,
)
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context


def _row(
    choice_name: str,
    price: object,
    *,
    target_minute: int,
    source: str = "future-feed",
    market_group: str = "Corners Total",
    market_period: str = "2nd Half",
    market_name: str = "Corners Total 2nd Half",
) -> dict[str, object]:
    return {
        "event_id": 8800,
        "market_id": 9900,
        "market_group": market_group,
        "market_period": market_period,
        "market_name": market_name,
        "choice_group": "7.5",
        "bookie_id": 77,
        "bookie_name": "Future Book",
        "source": source,
        "exchange_side": None,
        "exchange_level": 0,
        "choice_id": 1 if choice_name == "over" else 2,
        "choice_name": choice_name,
        "quote_id": 7001 if choice_name == "over" else 7002,
        "odds_value": price,
        "snapshot_id": 9000 + target_minute,
        "collected_at": datetime(2026, 8, 27, 17, 59, tzinfo=timezone.utc),
        "source_collected_at": datetime(2026, 8, 27, 17, 58, tzinfo=timezone.utc),
        "minutes_before_start": target_minute,
        "target_minute": target_minute,
        "distance_from_target": 0,
        "main_line": True,
    }


def _request() -> MarketSnapshotRequest:
    return MarketSnapshotRequest(
        identities=(
            MarketIdentity(
                "Corners Total",
                "2nd Half",
                "Corners Total 2nd Half",
            ),
        ),
        bookie_id=77,
        line_input_name="FUTURE_TOTAL_LINE",
        choices=(
            ChoiceRequest("over", "over", "FUTURE_OVER_PRICE"),
            ChoiceRequest("under", "under", "FUTURE_UNDER_PRICE"),
        ),
    )


def test_shared_extractor_supports_arbitrary_future_market_and_bookie() -> None:
    rows = [
        _row("over", 1.91, target_minute=5),
        _row("under", 1.99, target_minute=5),
        _row("over", 1.88, target_minute=0),
        _row("under", 2.02, target_minute=0),
    ]
    context = build_odds_trajectory_context(rows, target_minutes_expected=[5, 0])

    selection = select_target_minute(context, flow_id="future_pillar")
    extraction = extract_market_snapshot(
        context,
        target_minute=selection.target_minute,
        request=_request(),
    )

    assert selection.target_minute == 0
    assert extraction.missing_inputs == ()
    assert extraction.invalid_inputs == ()
    assert extraction.ambiguous_inputs == ()
    assert len(extraction.candidates) == 1

    candidate = extraction.candidates[0]
    assert candidate.line == Decimal("7.5")
    assert candidate.choices["over"].odds_price == Decimal("1.88")
    assert candidate.choices["under"].odds_price == Decimal("2.02")
    assert candidate.choices["over"].trace.quote_id == 7001
    assert candidate.choices["over"].trace.snapshot_id == 9000
    assert candidate.choices["over"].trace.changed_at == datetime(
        2026,
        8,
        27,
        17,
        58,
        tzinfo=timezone.utc,
    )


def test_target_override_is_per_flow_and_strict(monkeypatch) -> None:
    context = build_odds_trajectory_context(
        [_row("over", 1.88, target_minute=0)],
        target_minutes_expected=[5, 0],
    )
    monkeypatch.setitem(
        market_snapshot_extractor.HARDCODED_TARGET_MINUTE_BY_FLOW,
        "future_pillar",
        5,
    )

    selection = select_target_minute(context, flow_id="future_pillar")
    extraction = extract_market_snapshot(
        context,
        target_minute=selection.target_minute,
        request=_request(),
    )

    assert selection.target_minute == 5
    assert extraction.candidates[0].choices["over"] is None
    assert set(extraction.missing_inputs) == {
        "FUTURE_OVER_PRICE",
        "FUTURE_UNDER_PRICE",
    }


def test_market_identity_is_an_exact_canonical_triple() -> None:
    crossed_rows = [
        _row(
            "over",
            1.88,
            target_minute=0,
            market_group="Over/Under",
        ),
        _row(
            "under",
            2.02,
            target_minute=0,
            market_group="Over/Under",
        ),
    ]
    context = build_odds_trajectory_context(crossed_rows, [0])

    extraction = extract_market_snapshot(
        context,
        target_minute=0,
        request=_request(),
    )

    assert extraction.candidates == ()
    assert set(extraction.missing_inputs) == {
        "FUTURE_TOTAL_LINE",
        "FUTURE_OVER_PRICE",
        "FUTURE_UNDER_PRICE",
    }


def test_duplicate_source_containers_are_reported_as_ambiguous() -> None:
    rows = [
        _row("over", 1.88, target_minute=0, source="source-a"),
        _row("under", 2.02, target_minute=0, source="source-a"),
        _row("over", 1.87, target_minute=0, source="source-b"),
        _row("under", 2.03, target_minute=0, source="source-b"),
    ]
    context = build_odds_trajectory_context(rows, [0])

    extraction = extract_market_snapshot(
        context,
        target_minute=0,
        request=_request(),
    )

    assert extraction.candidates == ()
    assert len(extraction.container_ambiguities) == 1
    assert set(extraction.ambiguous_inputs) == {
        "FUTURE_TOTAL_LINE",
        "FUTURE_OVER_PRICE",
        "FUTURE_UNDER_PRICE",
    }
