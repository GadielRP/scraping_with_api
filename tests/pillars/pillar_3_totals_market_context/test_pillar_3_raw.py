from __future__ import annotations

import logging
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from infrastructure.persistence.catalogs.canonical_market_types import (
    CANONICAL_MARKET_TYPE_SEEDS,
)
from modules.pillars import market_snapshot_extractor
from modules.pillars.market_snapshot_extractor import MarketIdentity
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.pillar_3_totals_market_context.periods import (
    FULL_TIME_TOTALS_SCOPE,
    P3_TOTALS_PERIOD_SCOPES,
    TotalsPeriodScope,
    derived_metric_names,
)
from modules.pillars.pillar_3_totals_market_context.run_pillar_3 import (
    calculate_pillar_3,
)


@pytest.fixture(autouse=True)
def _reset_target_override(monkeypatch) -> None:
    monkeypatch.setitem(
        market_snapshot_extractor.HARDCODED_TARGET_MINUTE_BY_FLOW,
        "pillar_3",
        None,
    )


def _event_context(event_id: int = 3003, evaluation_minute: int = 0):
    return SimpleNamespace(
        event_id=event_id,
        sport="Football",
        participants_label="Home vs Away",
        minutes_until_start=evaluation_minute,
        season_id=77,
        season_name="2026",
        season_year=2026,
        start_time_utc=datetime(2026, 8, 27, 18, 0, tzinfo=timezone.utc),
        context_status="normalized",
        competition=SimpleNamespace(competition_id=99, display_name="League"),
    )


def _rows_for_book(
    *,
    bookie_id: int,
    bookie_name: str,
    line: object = "2.5",
    over: object | None = 1.80,
    under: object | None = 2.20,
    target_minute: int = 0,
    period: str = "Full Time",
    market_name: str = "Over/Under Full Time",
    source: str = "oddspapi",
    event_id: int = 3003,
) -> list[dict]:
    rows = []
    for index, (choice_name, price) in enumerate((("over", over), ("under", under)), 1):
        if price is None:
            continue
        rows.append(
            {
                "event_id": event_id,
                "market_id": 1000 + bookie_id,
                "market_group": "Over/Under",
                "market_period": period,
                "market_name": market_name,
                "choice_group": line,
                "bookie_id": bookie_id,
                "bookie_name": bookie_name,
                "source": source,
                "exchange_side": None,
                "exchange_level": 0,
                "choice_id": bookie_id * 10 + index,
                "choice_name": choice_name,
                "quote_id": bookie_id * 100 + index,
                "odds_value": price,
                "snapshot_id": 9000 + index,
                "collected_at": datetime(2026, 8, 27, 17, 59, tzinfo=timezone.utc),
                "source_collected_at": datetime(2026, 8, 27, 17, 58, tzinfo=timezone.utc),
                "minutes_before_start": target_minute,
                "target_minute": target_minute,
                "distance_from_target": 0,
                "main_line": True,
            }
        )
    return rows


def _complete_rows(**overrides) -> list[dict]:
    target_minute = overrides.get("target_minute", 0)
    period = overrides.get("period", "Full Time")
    market_name = overrides.get("market_name", "Over/Under Full Time")
    line = overrides.get("line", "2.5")
    return [
        *_rows_for_book(
            bookie_id=302,
            bookie_name=overrides.get("pin_name", "Pinnacle Sports"),
            line=overrides.get("pin_line", line),
            over=overrides.get("pin_over", 1.80),
            under=overrides.get("pin_under", 2.20),
            target_minute=target_minute,
            period=overrides.get("pin_period", period),
            market_name=overrides.get("pin_market_name", market_name),
        ),
        *_rows_for_book(
            bookie_id=3,
            bookie_name=overrides.get("b365_name", "bet365"),
            line=overrides.get("b365_line", line),
            over=overrides.get("b365_over", 1.90),
            under=overrides.get("b365_under", 2.10),
            target_minute=target_minute,
            period=overrides.get("b365_period", period),
            market_name=overrides.get("b365_market_name", market_name),
        ),
    ]


def _calculate(rows: list[dict], *, event_id: int = 3003, debug_mode: bool = False):
    context = build_odds_trajectory_context(
        rows,
        target_minutes_expected=[120, 30, 5, 1, 0, -5],
    )
    return calculate_pillar_3(
        _event_context(event_id),
        context,
        debug_mode=debug_mode,
    )


@pytest.mark.parametrize(
    ("pin_prices", "b365_prices", "expected_direction", "expected_context"),
    [
        ((1.8, 2.2), (1.9, 2.1), "OVER", "OPEN_BIAS"),
        ((2.2, 1.8), (2.1, 1.9), "UNDER", "CLOSED_BIAS"),
        ((2.0, 2.0), (2.0, 2.0), "NEUTRAL", "NEUTRAL_BIAS"),
    ],
)
def test_complete_equal_line_calculates_raw_direction(
    pin_prices,
    b365_prices,
    expected_direction,
    expected_context,
) -> None:
    result = _calculate(
        _complete_rows(
            pin_over=pin_prices[0],
            pin_under=pin_prices[1],
            b365_over=b365_prices[0],
            b365_under=b365_prices[1],
        )
    )

    assert result["P3_STATUS"] == "ACTIVE"
    assert result["PERIOD_SCOPE"] == "FULL_TIME"
    assert result["PERIODS"]["full_time"]["status"] == "COMPLETE"
    assert result["P3_FULL_TIME_DIRECTION_RAW"] == expected_direction
    assert result["CONTEXT_FULL_TIME_DIRECTION_RAW"] == expected_context
    assert result["TOTALS_MARKET_FULL_TIME_EDGE"] is not None
    assert result["TOTAL_FULL_TIME_PRICE_GAP"] is not None
    assert result["Q_COMPLETE_TOTALS_FULL_TIME"] == 1.0
    assert result["W_PIN_TOTALS_FULL_TIME"] == result["W_B365_TOTALS_FULL_TIME"] == 0.5

    expected_pin_edge = ((1 / pin_prices[0]) - (1 / pin_prices[1])) / (
        (1 / pin_prices[0]) + (1 / pin_prices[1])
    )
    expected_b365_edge = ((1 / b365_prices[0]) - (1 / b365_prices[1])) / (
        (1 / b365_prices[0]) + (1 / b365_prices[1])
    )
    assert result["PIN_TOTAL_FULL_TIME_EDGE"] == pytest.approx(expected_pin_edge)
    assert result["B365_TOTAL_FULL_TIME_EDGE"] == pytest.approx(expected_b365_edge)
    assert result["TOTAL_FULL_TIME_PRICE_GAP"] == pytest.approx(
        abs(expected_pin_edge - expected_b365_edge)
    )
    assert result["TOTALS_MARKET_FULL_TIME_EDGE"] == pytest.approx(
        (0.5 * expected_pin_edge) + (0.5 * expected_b365_edge)
    )
    assert "TOTALS_MARKET_EDGE" not in result
    assert "P3_DIRECTION_RAW" not in result


def test_period_scopes_declare_full_time_required_and_first_half_unregistered() -> None:
    assert FULL_TIME_TOTALS_SCOPE.required is True
    assert [scope.key for scope in P3_TOTALS_PERIOD_SCOPES] == ["full_time"]

    first_half_scope = TotalsPeriodScope(
        key="first_half",
        display_name="1st Half",
        metric_token="FIRST_HALF",
        required=False,
        identities=(
            MarketIdentity("Over/Under", "1st Half", "Over/Under 1st Half"),
        ),
    )
    names = derived_metric_names(first_half_scope)
    assert names.market_edge == "TOTALS_MARKET_FIRST_HALF_EDGE"

    context = build_odds_trajectory_context(
        _complete_rows(
            period="1st Half",
            market_name="Over/Under 1st Half",
        ),
        target_minutes_expected=[0],
    )
    result = calculate_pillar_3(_event_context(), context)

    assert result["P3_STATUS"] == "INSUFFICIENT_DATA"
    assert result["PERIODS"]["full_time"]["status"] == "INCOMPLETE"
    assert "TOTALS_MARKET_FIRST_HALF_EDGE" not in result
    assert "TOTALS_MARKET_FULL_TIME_EDGE" not in result


def test_different_lines_preserve_individual_branches_but_are_partial() -> None:
    result = _calculate(_complete_rows(pin_line="2.5", b365_line="3.5"))

    assert result["P3_STATUS"] == "PARTIAL"
    assert result["Q_COMPLETE_TOTALS_FULL_TIME"] == 1.0
    assert result["PIN_TOTAL_FULL_TIME_EDGE"] is not None
    assert result["B365_TOTAL_FULL_TIME_EDGE"] is not None
    assert result["TOTAL_FULL_TIME_LINE_DIFF_RAW"] == -1.0
    assert result["TOTAL_FULL_TIME_LINE_GAP"] == 1.0
    assert result["TOTAL_FULL_TIME_PRICE_GAP"] is None
    assert result["TOTALS_MARKET_FULL_TIME_EDGE"] is None
    assert result["P3_FULL_TIME_DIRECTION_RAW"] is None


def test_missing_book_and_missing_price_propagate_null_only_to_dependencies() -> None:
    only_pin = _calculate(
        _rows_for_book(
            bookie_id=302,
            bookie_name="Pinnacle Sports",
        )
    )
    assert only_pin["P3_STATUS"] == "PARTIAL"
    assert only_pin["PIN_TOTAL_FULL_TIME_EDGE"] is not None
    assert only_pin["B365_TOTAL_LINE"] is None
    assert only_pin["Q_COMPLETE_TOTALS_FULL_TIME"] == pytest.approx(0.5)

    missing_under = _calculate(_complete_rows(b365_under=None))
    assert missing_under["PIN_TOTAL_FULL_TIME_EDGE"] is not None
    assert missing_under["B365_TOTAL_FULL_TIME_EDGE"] is None
    assert missing_under["TOTAL_FULL_TIME_LINE_GAP"] == 0.0
    assert missing_under["TOTALS_MARKET_FULL_TIME_EDGE"] is None
    assert missing_under["Q_COMPLETE_TOTALS_FULL_TIME"] == pytest.approx(5 / 6)


@pytest.mark.parametrize("invalid_price", [0, -1, "NaN", "Infinity"])
def test_invalid_price_becomes_null_without_crashing(invalid_price) -> None:
    result = _calculate(_complete_rows(pin_over=invalid_price))

    assert result["P3_STATUS"] == "PARTIAL"
    assert result["PIN_OVER_PRICE"] is None
    assert result["PIN_TOTAL_FULL_TIME_EDGE"] is None
    assert "PIN_OVER_PRICE" in result["INVALID_INPUTS"] or "PIN_OVER_PRICE" in result["MISSING_INPUTS"]


def test_invalid_line_is_null_but_does_not_destroy_price_edge() -> None:
    result = _calculate(_complete_rows(pin_line="not-a-line"))

    assert result["P3_STATUS"] == "PARTIAL"
    assert result["PIN_TOTAL_LINE"] is None
    assert result["PIN_TOTAL_FULL_TIME_EDGE"] is not None
    assert result["TOTAL_FULL_TIME_LINE_DIFF_RAW"] is None
    assert "PIN_TOTAL_LINE" in result["INVALID_INPUTS"]


def test_zero_candidates_is_insufficient_with_complete_output_shape() -> None:
    unrelated = [
        {
            **_rows_for_book(bookie_id=302, bookie_name="Pinnacle Sports")[0],
            "market_group": "1X2",
            "market_name": "1X2 Full Time",
            "choice_group": None,
            "choice_name": "1",
        }
    ]
    result = _calculate(unrelated)

    assert result["P3_STATUS"] == "INSUFFICIENT_DATA"
    assert result["modules"] == []
    assert result["PERIOD"] is None
    assert result["PERIODS"]["full_time"]["status"] == "INCOMPLETE"
    assert "PIN_TOTAL_LINE" not in result
    assert "Q_COMPLETE_TOTALS_FULL_TIME" not in result


def test_structural_abort_conditions_are_insufficient() -> None:
    unavailable = build_odds_trajectory_context([])
    assert calculate_pillar_3(_event_context(), unavailable)["raw"]["reason"] == "odds_trajectory_unavailable"

    mismatched = build_odds_trajectory_context(_complete_rows(), [0])
    assert calculate_pillar_3(_event_context(9999), mismatched)["raw"]["reason"] == "event_id_mismatch"

    no_present = build_odds_trajectory_context(_complete_rows(), [5])
    assert calculate_pillar_3(_event_context(), no_present)["raw"]["reason"] == "no_target_minutes_present"


def test_selects_numerically_smallest_present_minute_without_fallback() -> None:
    rows = [*_complete_rows(target_minute=0), *_complete_rows(target_minute=-5)]
    result = _calculate(rows)

    assert result["TARGET_MINUTE"] == -5
    traces = result["raw"]["p3_raw_engine"]["input_trace"]
    assert {trace["target_minute"] for trace in traces.values()} == {-5}


def test_shared_hardcoded_target_override_can_force_p3_to_zero(monkeypatch) -> None:
    rows = [*_complete_rows(target_minute=0), *_complete_rows(target_minute=-5)]
    monkeypatch.setitem(
        market_snapshot_extractor.HARDCODED_TARGET_MINUTE_BY_FLOW,
        "pillar_3",
        0,
    )

    result = _calculate(rows)

    assert result["P3_STATUS"] == "ACTIVE"
    assert result["TARGET_MINUTE"] == 0
    traces = result["raw"]["p3_raw_engine"]["input_trace"]
    assert {trace["target_minute"] for trace in traces.values()} == {0}


def test_supports_overtime_pair_and_rejects_crossed_pair() -> None:
    assert CANONICAL_MARKET_TYPE_SEEDS[
        "over_under_full_time_including_overtime"
    ]["enabled_for_trajectory"] is True

    overtime = _calculate(
        _complete_rows(
            period="Full Time Including Overtime",
            market_name="Over/Under Full Time Including Overtime",
        )
    )
    assert overtime["P3_STATUS"] == "ACTIVE"
    assert overtime["PERIOD"] == "Full Time Including Overtime"

    crossed = _calculate(
        _complete_rows(
            period="Full Time",
            market_name="Over/Under Full Time Including Overtime",
        )
    )
    assert crossed["P3_STATUS"] == "INSUFFICIENT_DATA"
    assert crossed["modules"] == []
    assert "Q_COMPLETE_TOTALS_FULL_TIME" not in crossed


def test_multiple_lines_period_mismatch_and_duplicate_sources_abort() -> None:
    multiple_lines = [
        *_complete_rows(line="2.5"),
        *_rows_for_book(
            bookie_id=302,
            bookie_name="Pinnacle Sports",
            line="3.5",
        ),
    ]
    assert _calculate(multiple_lines)["raw"]["reason"] == "multiple_candidate_lines"

    period_mismatch = _complete_rows(
        b365_period="Full Time Including Overtime",
        b365_market_name="Over/Under Full Time Including Overtime",
    )
    assert _calculate(period_mismatch)["raw"]["reason"] == "bookmaker_period_mismatch"

    duplicate_source = [
        *_complete_rows(),
        *_rows_for_book(
            bookie_id=302,
            bookie_name="Different display name",
            source="oddsportal",
        ),
    ]
    assert _calculate(duplicate_source)["raw"]["reason"] == "ambiguous_bookie_containers"


def test_bookie_identity_uses_canonical_id_and_preserves_quote_trace() -> None:
    result = _calculate(
        _complete_rows(pin_name="Renamed Pinnacle", b365_name="Renamed bet365")
    )
    traces = result["raw"]["p3_raw_engine"]["input_trace"]

    assert result["P3_STATUS"] == "ACTIVE"
    assert traces["PIN_OVER_PRICE"]["bookie_name"] == "Renamed Pinnacle"
    assert traces["PIN_OVER_PRICE"]["quote_id"] is not None
    assert traces["PIN_OVER_PRICE"]["snapshot_id"] is not None
    assert traces["PIN_OVER_PRICE"]["collected_at"] is not None
    assert traces["PIN_OVER_PRICE"]["changed_at"] is not None
    assert traces["PIN_OVER_PRICE"]["minutes_before_start"] == 0


def test_verbose_natural_language_logging_only_uses_debug_mode(caplog) -> None:
    rows = _complete_rows()
    with caplog.at_level(logging.INFO):
        _calculate(rows, debug_mode=False)
    assert "P3_TOTALS_MARKET DEBUG" not in caplog.text

    caplog.clear()
    with caplog.at_level(logging.INFO):
        _calculate(rows, debug_mode=True)
    assert "Asignación PIN_TOTAL_LINE" in caplog.text
    assert "TOTALS_MARKET_FULL_TIME_EDGE=" in caplog.text
    assert "Q_COMPLETE_TOTALS_FULL_TIME=" in caplog.text
