from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from modules.pillars import market_snapshot_extractor
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.pillar_3_totals_market_context.metrics import ou_edge
from modules.pillars.pillar_3_totals_market_context.periods import (
    FIRST_HALF_TOTALS_SCOPE,
    FULL_TIME_TOTALS_SCOPE,
    P3_TOTALS_PERIOD_SCOPES,
)
from modules.pillars.pillar_3_totals_market_context.relations import (
    context_direction,
    direction,
    relation,
)
from modules.pillars.pillar_3_totals_market_context.run_pillar_3 import (
    calculate_pillar_3,
)


EVENT_ID = 3003
TARGET_MINUTES = [120, 30, 5, 1, 0, -5]
FLOW_ID = "pre_start_signal_profile"


@pytest.fixture(autouse=True)
def _reset_target_override(monkeypatch) -> None:
    monkeypatch.setitem(
        market_snapshot_extractor.HARDCODED_TARGET_MINUTE_BY_FLOW,
        FLOW_ID,
        None,
    )


def _event_context(event_id: int = EVENT_ID):
    return SimpleNamespace(
        event_id=event_id,
        sport="Football",
        participants_label="Home vs Away",
        minutes_until_start=5,
        season_id=77,
        season_name="2026",
        season_year=2026,
        start_time_utc=datetime(2026, 8, 27, 18, 0, tzinfo=timezone.utc),
        context_status="normalized",
        competition=SimpleNamespace(competition_id=99, display_name="League"),
    )


def _book_rows(
    *,
    bookie_id: int,
    bookie_name: str,
    line: object,
    over: object | None,
    under: object | None,
    target_minute: int,
    period: str,
    market_name: str,
    market_id: int,
) -> list[dict]:
    rows: list[dict] = []
    for index, (choice_name, price) in enumerate((('over', over), ('under', under)), 1):
        if price is None:
            continue
        rows.append(
            {
                "event_id": EVENT_ID,
                "market_id": market_id,
                "market_group": "Over/Under",
                "market_period": period,
                "market_name": market_name,
                "choice_group": line,
                "bookie_id": bookie_id,
                "bookie_name": bookie_name,
                "source": "oddspapi",
                "exchange_side": None,
                "exchange_level": 0,
                "choice_id": market_id * 10 + index,
                "choice_name": choice_name,
                "quote_id": market_id * 100 + index,
                "odds_value": price,
                "snapshot_id": market_id * 1000 + index,
                "collected_at": datetime(2026, 8, 27, 17, 59, tzinfo=timezone.utc),
                "source_collected_at": datetime(2026, 8, 27, 17, 58, tzinfo=timezone.utc),
                "minutes_before_start": target_minute,
                "target_minute": target_minute,
                "distance_from_target": 0,
                "main_line": True,
            }
        )
    return rows


def _exchange_rows(
    *,
    exchange_side: str,
    line: object = "2.5",
    over: object | None = 1.85,
    under: object | None = 2.15,
    over_size: object | None = 100,
    under_size: object | None = 80,
    target_minute: int = 5,
    market_id: int = 4000,
    first_half: bool = False,
) -> list[dict]:
    rows: list[dict] = []
    for index, (choice_name, price, size) in enumerate(
        (("over", over, over_size), ("under", under, under_size)),
        1,
    ):
        if price is None:
            continue
        rows.append(
            {
                "event_id": EVENT_ID,
                "market_id": market_id,
                "market_group": "Over/Under",
                "market_period": "1st Half" if first_half else "Full Time",
                "market_name": "Over/Under 1st Half" if first_half else "Over/Under Full Time",
                "choice_group": line,
                "bookie_id": 4,
                "bookie_name": "Betfair",
                "source": "oddspapi",
                "exchange_side": exchange_side,
                "exchange_level": 0,
                "choice_id": market_id * 10 + index,
                "choice_name": choice_name,
                "quote_id": market_id * 100 + index,
                "odds_value": price,
                "exchange_size": size,
                "snapshot_id": market_id * 1000 + index,
                "collected_at": datetime(2026, 8, 27, 17, 59, tzinfo=timezone.utc),
                "minutes_before_start": target_minute,
                "target_minute": target_minute,
                "distance_from_target": 0,
            }
        )
    return rows


def _period_rows(
    *,
    first_half: bool,
    target_minute: int = 5,
    pin_line: object = "2.5",
    b365_line: object = "2.5",
    pin_over: object | None = 1.80,
    pin_under: object | None = 2.20,
    b365_over: object | None = 1.90,
    b365_under: object | None = 2.10,
    market_id_offset: int = 0,
) -> list[dict]:
    period = "1st Half" if first_half else "Full Time"
    market_name = "Over/Under 1st Half" if first_half else "Over/Under Full Time"
    base = 2000 if first_half else 1000
    return [
        *_book_rows(
            bookie_id=302,
            bookie_name="Pinnacle",
            line=pin_line,
            over=pin_over,
            under=pin_under,
            target_minute=target_minute,
            period=period,
            market_name=market_name,
            market_id=base + market_id_offset + 302,
        ),
        *_book_rows(
            bookie_id=3,
            bookie_name="bet365",
            line=b365_line,
            over=b365_over,
            under=b365_under,
            target_minute=target_minute,
            period=period,
            market_name=market_name,
            market_id=base + market_id_offset + 3,
        ),
    ]


def _complete_rows(*, target_minute: int = 5, include_betfair_ou: bool = False, include_betfair_1h_ou: bool = False, **kwargs) -> list[dict]:
    ft_keys = {key[3:]: value for key, value in kwargs.items() if key.startswith("ft_")}
    first_half_keys = {
        key[3:]: value for key, value in kwargs.items() if key.startswith("1h_")
    }
    rows = [
        *_period_rows(first_half=False, target_minute=target_minute, **ft_keys),
        *_period_rows(first_half=True, target_minute=target_minute, **first_half_keys),
    ]
    if include_betfair_ou:
        rows.extend(
            _exchange_rows(exchange_side="back", target_minute=target_minute)
            + _exchange_rows(exchange_side="lay", target_minute=target_minute, market_id=4010)
        )
    if include_betfair_1h_ou:
        rows.extend(
            _exchange_rows(exchange_side="back", target_minute=target_minute, first_half=True, market_id=5000)
            + _exchange_rows(exchange_side="lay", target_minute=target_minute, first_half=True, market_id=5010)
        )
    return rows


def _calculate(rows: list[dict], *, debug_mode: bool = False) -> dict:
    context = build_odds_trajectory_context(
        rows,
        target_minutes_expected=TARGET_MINUTES,
    )
    selection = market_snapshot_extractor.select_target_minute(
        context,
        flow_id=FLOW_ID,
        expected_event_id=EVENT_ID,
        allowed_target_minutes=TARGET_MINUTES,
    )
    return calculate_pillar_3(
        _event_context(),
        context,
        target_selection=selection,
        debug_mode=debug_mode,
    )


def _profile(result: dict) -> dict:
    profile = result["P3_SIGNAL_PROFILE"]
    assert profile is not None
    return profile


def _all_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {
            key for child in value.values() for key in _all_keys(child)
        }
    if isinstance(value, list):
        return {key for child in value for key in _all_keys(child)}
    return set()


def test_ft_and_first_half_complete_produce_active_structural_contract() -> None:
    result = _calculate(_complete_rows())
    profile = _profile(result)

    assert result["P3_STATUS"] == "ACTIVE"
    assert result["engine_version"] == "p3-signal-profile-v1"
    assert result["P3_TARGET_MINUTE"] == 5
    assert result["PERIODS"]["full_time"]["status"] == "COMPLETE"
    assert result["PERIODS"]["first_half"]["status"] == "COMPLETE"
    assert set(profile) == {
        "FT", "1H", "FT_1H", "BETFAIR_FT_OU", "BOOK_EXCHANGE_OU",
        "BETFAIR_1H_OU", "BOOK_EXCHANGE_1H_OU"
    }
    assert set(profile["FT"]) == {
        "PINNACLE",
        "BET365",
        "LINE_STRUCTURE",
        "BOOK_RELATION",
        "REPRESENTATIVE",
        "CONTEXT_DIRECTION_RAW",
    }
    assert profile["1H"] is not None
    assert profile["FT_1H"] is not None
    assert result["modules"][0]["P3_SIGNAL_PROFILE"] is profile


def test_ft_complete_without_first_half_is_partial() -> None:
    result = _calculate(_period_rows(first_half=False))
    profile = _profile(result)

    assert result["P3_STATUS"] == "PARTIAL"
    assert profile["FT"] is not None
    assert profile["1H"] is None
    assert profile["FT_1H"] is None


def test_ft_incomplete_with_complete_first_half_is_insufficient() -> None:
    rows = [
        *_period_rows(first_half=False, pin_under=None),
        *_period_rows(first_half=True),
    ]
    result = _calculate(rows)

    assert result["P3_STATUS"] == "INSUFFICIENT_DATA"
    assert result["P3_SIGNAL_PROFILE"] is None
    assert result["PERIODS"]["first_half"]["status"] == "COMPLETE"


def test_partial_first_half_preserves_each_available_branch() -> None:
    rows = [
        *_period_rows(first_half=False),
        *_period_rows(first_half=True, pin_under=None),
    ]
    result = _calculate(rows)
    first_half = _profile(result)["1H"]

    assert result["P3_STATUS"] == "PARTIAL"
    assert first_half is not None
    assert first_half["PINNACLE"]["LINE"] == 2.5
    assert first_half["PINNACLE"]["OVER_ODDS"] == 1.8
    assert first_half["PINNACLE"]["UNDER_ODDS"] is None
    assert first_half["PINNACLE"]["EDGE"] is None
    assert first_half["PINNACLE"]["DIRECTION"] is None
    assert first_half["BET365"]["EDGE"] is not None
    assert first_half["BET365"]["DIRECTION"] == "OVER"
    assert first_half["LINE_STRUCTURE"] == {
        "LINE_DIFF_RAW": 0.0,
        "LINE_GAP": 0.0,
    }
    assert first_half["BOOK_RELATION"] == {"RELATION": None, "GAP": None}
    assert first_half["REPRESENTATIVE"] == {"EDGE": None, "DIRECTION": None}
    assert _profile(result)["FT_1H"] is None


@pytest.mark.parametrize(
    ("pin_prices", "b365_prices", "expected"),
    [
        ((1.8, 2.2), (1.9, 2.1), "CONVERGENCE_OVER"),
        ((2.2, 1.8), (2.1, 1.9), "CONVERGENCE_UNDER"),
        ((1.8, 2.2), (2.2, 1.8), "DIVERGENCE"),
        ((2.0, 2.0), (1.8, 2.2), "NEUTRAL"),
    ],
)
def test_equal_line_book_relations(pin_prices, b365_prices, expected) -> None:
    result = _calculate(
        _complete_rows(
            ft_pin_over=pin_prices[0],
            ft_pin_under=pin_prices[1],
            ft_b365_over=b365_prices[0],
            ft_b365_under=b365_prices[1],
        )
    )
    assert _profile(result)["FT"]["BOOK_RELATION"]["RELATION"] == expected


def test_different_ft_lines_are_complete_but_not_comparable() -> None:
    result = _calculate(_complete_rows(ft_pin_line="2.5", ft_b365_line="3.0"))
    full_time = _profile(result)["FT"]

    assert result["PERIODS"]["full_time"]["status"] == "COMPLETE"
    assert full_time["PINNACLE"]["EDGE"] is not None
    assert full_time["BET365"]["EDGE"] is not None
    assert full_time["LINE_STRUCTURE"] == {
        "LINE_DIFF_RAW": -0.5,
        "LINE_GAP": 0.5,
    }
    assert full_time["BOOK_RELATION"] == {"RELATION": None, "GAP": None}
    assert full_time["REPRESENTATIVE"] == {"EDGE": None, "DIRECTION": None}
    assert full_time["CONTEXT_DIRECTION_RAW"] is None
    assert _profile(result)["FT_1H"] is None


def test_different_first_half_lines_remain_complete_without_representative() -> None:
    result = _calculate(_complete_rows(**{"1h_pin_line": "1.0", "1h_b365_line": "1.5"}))
    first_half = _profile(result)["1H"]

    assert result["P3_STATUS"] == "ACTIVE"
    assert result["PERIODS"]["first_half"]["status"] == "COMPLETE"
    assert first_half["LINE_STRUCTURE"]["LINE_DIFF_RAW"] == -0.5
    assert first_half["BOOK_RELATION"]["RELATION"] is None
    assert first_half["REPRESENTATIVE"]["EDGE"] is None
    assert _profile(result)["FT_1H"] is None


def test_betfair_ft_ou_is_optional_and_persisted() -> None:
    result = _calculate(_complete_rows(include_betfair_ou=True))
    profile = _profile(result)

    assert result["P3_STATUS"] == "ACTIVE"
    assert result["PERIODS"]["exchange_ou"]["status"] == "COMPLETE"
    exchange = profile["BETFAIR_FT_OU"]
    assert exchange["LINE"] == pytest.approx(2.5)
    assert exchange["BACK"]["OVER_ODDS"] == pytest.approx(1.85)
    assert exchange["BACK"]["OVER_SIZE"] == pytest.approx(100)
    assert exchange["LAY"]["UNDER_ODDS"] == pytest.approx(2.15)
    assert exchange["LAY"]["UNDER_SIZE"] == pytest.approx(80)
    assert exchange["REPRESENTATIVE"]["EDGE"] is not None
    assert exchange["BACK_LAY_RELATION"] is not None
    assert profile["BOOK_EXCHANGE_OU"]["LINE_GAP"] == pytest.approx(0)
    assert profile["BOOK_EXCHANGE_OU"]["RELATION"] is not None
    assert result["raw"]["inputs"]["BF_OU_BACK_OVER_FULL_TIME_ODDS_PRICE"] == pytest.approx(1.85)
    assert result["raw"]["inputs"]["BF_OU_BACK_OVER_FULL_TIME_EXCHANGE_SIZE"] == pytest.approx(100)


def test_betfair_ft_ou_absence_does_not_abort_p3() -> None:
    result = _calculate(_complete_rows())

    assert result["P3_STATUS"] == "ACTIVE"
    assert result["P3_SIGNAL_PROFILE"]["BETFAIR_FT_OU"] is None
    assert result["P3_SIGNAL_PROFILE"]["BOOK_EXCHANGE_OU"] is None
    assert result["P3_SIGNAL_PROFILE"]["BETFAIR_1H_OU"] is None
    assert result["P3_SIGNAL_PROFILE"]["BOOK_EXCHANGE_1H_OU"] is None
    assert result["PERIODS"]["exchange_ou"]["status"] == "INCOMPLETE"
    assert result["raw"]["inputs"]["BF_OU_FULL_TIME_LINE"] is None


def test_betfair_first_half_ou_is_optional_and_persisted() -> None:
    result = _calculate(_complete_rows(include_betfair_1h_ou=True))
    profile = _profile(result)

    assert result["P3_STATUS"] == "ACTIVE"
    assert result["PERIODS"]["exchange_ou_1h"]["status"] == "COMPLETE"
    assert profile["BETFAIR_1H_OU"]["LINE"] == pytest.approx(2.5)
    assert profile["BETFAIR_1H_OU"]["REPRESENTATIVE"]["EDGE"] is not None
    assert profile["BOOK_EXCHANGE_1H_OU"]["LINE_GAP"] == pytest.approx(0)
    assert profile["BOOK_EXCHANGE_1H_OU"]["RELATION"] is not None
    assert result["raw"]["inputs"]["BF_OU_BACK_OVER_1H_ODDS_PRICE"] == pytest.approx(1.85)


def test_betfair_first_half_ou_survives_without_first_half_bookmakers() -> None:
    result = _calculate(_complete_rows(include_betfair_1h_ou=True, **{"1h_pin_over": None, "1h_pin_under": None, "1h_b365_over": None, "1h_b365_under": None}))
    assert result["P3_STATUS"] == "PARTIAL"
    assert result["P3_SIGNAL_PROFILE"]["BETFAIR_1H_OU"] is not None
    assert result["P3_SIGNAL_PROFILE"]["BOOK_EXCHANGE_1H_OU"] is None


def test_betfair_ft_ou_different_line_keeps_readings_but_cross_comparison_is_null() -> None:
    rows = _complete_rows(include_betfair_ou=True)
    rows = [
        {**row, "choice_group": "3.0"}
        if row.get("bookie_id") == 4
        else row
        for row in rows
    ]
    profile = _profile(_calculate(rows))

    assert profile["BETFAIR_FT_OU"]["REPRESENTATIVE"]["EDGE"] is not None
    comparison = profile["BOOK_EXCHANGE_OU"]
    assert comparison["LINE_DIFF_RAW"] == pytest.approx(-0.5)
    assert comparison["LINE_GAP"] == pytest.approx(0.5)
    assert comparison["RELATION"] is None
    assert comparison["GAP"] is None


def test_representative_is_exact_unweighted_pair_mean() -> None:
    result = _calculate(_complete_rows())
    full_time = _profile(result)["FT"]
    pin_edge = ou_edge(Decimal("1.8"), Decimal("2.2"))
    b365_edge = ou_edge(Decimal("1.9"), Decimal("2.1"))

    assert full_time["REPRESENTATIVE"]["EDGE"] == pytest.approx(
        float((pin_edge + b365_edge) / Decimal("2"))
    )


def test_neutral_is_distinct_from_unavailable() -> None:
    assert direction(Decimal("0")) == "NEUTRAL"
    assert relation("NEUTRAL", "OVER") == "NEUTRAL"
    different_lines = _profile(
        _calculate(_complete_rows(ft_pin_line="2.5", ft_b365_line="3.0"))
    )
    neutral = _profile(
        _calculate(
            _complete_rows(
                ft_pin_over=2.0,
                ft_pin_under=2.0,
                ft_b365_over=2.0,
                ft_b365_under=2.0,
            )
        )
    )
    assert different_lines["FT"]["BOOK_RELATION"]["RELATION"] is None
    assert neutral["FT"]["BOOK_RELATION"]["RELATION"] == "NEUTRAL"


def test_context_direction_mapping() -> None:
    assert context_direction("OVER") == "OPEN_BIAS"
    assert context_direction("UNDER") == "CLOSED_BIAS"
    assert context_direction("NEUTRAL") == "NEUTRAL_BIAS"
    assert context_direction(None) is None


@pytest.mark.parametrize(
    ("first_half_prices", "expected_relation"),
    [
        ((1.8, 2.2), "CONVERGENCE_OVER"),
        ((2.2, 1.8), "DIVERGENCE"),
    ],
)
def test_ft_first_half_relation_and_edge_gap(first_half_prices, expected_relation) -> None:
    result = _calculate(
        _complete_rows(
            **{
                "1h_pin_over": first_half_prices[0],
                "1h_pin_under": first_half_prices[1],
                "1h_b365_over": first_half_prices[0],
                "1h_b365_under": first_half_prices[1],
            }
        )
    )
    profile = _profile(result)
    ft_1h = profile["FT_1H"]

    assert ft_1h["FT_1H_OU_RELATION"] == expected_relation
    assert ft_1h["FT_1H_OU_GAP"] == pytest.approx(
        abs(
            profile["FT"]["REPRESENTATIVE"]["EDGE"]
            - profile["1H"]["REPRESENTATIVE"]["EDGE"]
        )
    )
    assert "FT_1H_LINE_GAP" not in _all_keys(profile)
    assert "FT_1H_LINE_DIFF" not in _all_keys(profile)


def test_selected_target_has_no_fallback_to_another_minute() -> None:
    rows = [
        *_complete_rows(target_minute=30),
        *_period_rows(first_half=False, target_minute=5, pin_under=None),
        *_period_rows(first_half=True, target_minute=5),
    ]
    result = _calculate(rows)

    assert result["P3_TARGET_MINUTE"] == 5
    assert result["P3_STATUS"] == "INSUFFICIENT_DATA"
    assert {
        trace["target_minute"] for trace in result["raw"]["input_trace"].values()
    } == {5}


def test_multiple_complete_lines_are_ambiguous() -> None:
    rows = _complete_rows()
    rows.extend(
        _book_rows(
            bookie_id=302,
            bookie_name="Pinnacle",
            line="3.0",
            over=1.9,
            under=1.9,
            target_minute=5,
            period="Full Time",
            market_name="Over/Under Full Time",
            market_id=9999,
        )
    )
    result = _calculate(rows)

    assert result["P3_STATUS"] == "INSUFFICIENT_DATA"
    assert result["PERIODS"]["full_time"]["status"] == "AMBIGUOUS"
    assert result["P3_SIGNAL_PROFILE"] is None


def test_unique_partial_first_half_candidate_is_preserved() -> None:
    result = _calculate(
        [
            *_period_rows(first_half=False),
            *_period_rows(first_half=True, pin_under=None),
        ]
    )
    assert _profile(result)["1H"]["PINNACLE"]["OVER_ODDS"] == 1.8


def test_multiple_partial_first_half_candidates_are_ambiguous() -> None:
    rows = [
        *_period_rows(first_half=False),
        *_period_rows(first_half=True, pin_under=None),
        *_book_rows(
            bookie_id=302,
            bookie_name="Pinnacle",
            line="3.0",
            over=1.85,
            under=None,
            target_minute=5,
            period="1st Half",
            market_name="Over/Under 1st Half",
            market_id=8888,
        ),
    ]
    result = _calculate(rows)

    assert result["P3_STATUS"] == "PARTIAL"
    assert result["PERIODS"]["first_half"]["status"] == "AMBIGUOUS"
    assert _profile(result)["1H"] is None
    assert _profile(result)["FT_1H"] is None


def test_traceability_contains_complete_quote_lineage() -> None:
    result = _calculate(_complete_rows())
    trace = result["raw"]["input_trace"]["PIN_FT_OVER_ODDS"]

    assert trace["target_minute"] == 5
    assert trace["snapshot_id"] is not None
    assert trace["quote_id"] is not None
    assert trace["collected_at"] is not None
    assert trace["changed_at"] is not None
    assert trace["minutes_before_start"] == 5
    assert trace["market_group"] == "Over/Under"
    assert trace["market_period"] == "Full Time"
    assert trace["market_name"] == "Over/Under Full Time"
    assert trace["choice_group"] == "2.5"
    assert trace["bookie_id"] == 302
    assert trace["choice_name"] == "over"
    assert result["raw"]["input_trace"]["PIN_FT_OU_LINE"]["choice_group"] == "2.5"


def test_period_registry_has_ft_required_and_first_half_optional() -> None:
    assert P3_TOTALS_PERIOD_SCOPES == (
        FULL_TIME_TOTALS_SCOPE,
        FIRST_HALF_TOTALS_SCOPE,
    )
    assert FULL_TIME_TOTALS_SCOPE.required is True
    assert FIRST_HALF_TOTALS_SCOPE.required is False
    assert len(FULL_TIME_TOTALS_SCOPE.input_names()) == 6
    assert len(FIRST_HALF_TOTALS_SCOPE.input_names()) == 6


def test_profile_has_no_temporal_analysis_vocabulary() -> None:
    keys = {key.casefold() for key in _all_keys(_profile(_calculate(_complete_rows())))}
    assert not ({"drift", "trend", "movement", "acceleration", "reversal"} & keys)


def test_output_has_exact_structural_envelope_and_inputs() -> None:
    result = _calculate(_complete_rows())

    assert set(result) == {
        "pillar_id",
        "pillar_name",
        "engine_version",
        "event_id",
        "participants",
        "P3_TARGET_MINUTE",
        "PERIODS",
        "MISSING_INPUTS",
        "INVALID_INPUTS",
        "AMBIGUOUS_INPUTS",
        "P3_STATUS",
        "status",
        "P3_SIGNAL_PROFILE",
        "modules",
        "raw",
    }
    assert set(result["raw"]["inputs"]) == {
        "PIN_FT_OU_LINE",
        "PIN_FT_OVER_ODDS",
        "PIN_FT_UNDER_ODDS",
        "B365_FT_OU_LINE",
        "B365_FT_OVER_ODDS",
        "B365_FT_UNDER_ODDS",
        "PIN_1H_OU_LINE",
        "PIN_1H_OVER_ODDS",
        "PIN_1H_UNDER_ODDS",
        "B365_1H_OU_LINE",
        "B365_1H_OVER_ODDS",
        "B365_1H_UNDER_ODDS",
        "BF_OU_FULL_TIME_LINE",
        "BF_OU_BACK_OVER_FULL_TIME_ODDS_PRICE",
        "BF_OU_BACK_UNDER_FULL_TIME_ODDS_PRICE",
        "BF_OU_LAY_OVER_FULL_TIME_ODDS_PRICE",
        "BF_OU_LAY_UNDER_FULL_TIME_ODDS_PRICE",
        "BF_OU_BACK_OVER_FULL_TIME_EXCHANGE_SIZE",
        "BF_OU_BACK_UNDER_FULL_TIME_EXCHANGE_SIZE",
        "BF_OU_LAY_OVER_FULL_TIME_EXCHANGE_SIZE",
            "BF_OU_LAY_UNDER_FULL_TIME_EXCHANGE_SIZE",
            "BF_OU_1H_LINE",
            "BF_OU_BACK_OVER_1H_ODDS_PRICE",
            "BF_OU_BACK_UNDER_1H_ODDS_PRICE",
            "BF_OU_LAY_OVER_1H_ODDS_PRICE",
            "BF_OU_LAY_UNDER_1H_ODDS_PRICE",
            "BF_OU_BACK_OVER_1H_EXCHANGE_SIZE",
            "BF_OU_BACK_UNDER_1H_EXCHANGE_SIZE",
            "BF_OU_LAY_OVER_1H_EXCHANGE_SIZE",
            "BF_OU_LAY_UNDER_1H_EXCHANGE_SIZE",
    }


def test_debug_logging_reports_inputs_formulas_and_structural_profile(caplog) -> None:
    caplog.set_level(logging.INFO)
    _calculate(_complete_rows(), debug_mode=True)

    assert "P3 DEBUG | input assignment | name=PIN_FT_OU_LINE" in caplog.text
    assert "market_name=Over/Under Full Time" in caplog.text
    assert "P3 FORMULA | FT.PINNACLE.EDGE | formula=" in caplog.text
    assert "P3 FORMULA | FT.PINNACLE.EDGE | substitution=" in caplog.text
    assert "P3 SIGNAL | FT.PINNACLE | field=DIRECTION" in caplog.text
