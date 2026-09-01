from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest

from modules.pillars import market_snapshot_extractor
from modules.pillars.context import CompetitionContext, EventContext, ParticipantContext
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.pillar_2_side_market.metrics import side_edge
from modules.pillars.pillar_2_side_market.periods import (
    EXCHANGE_ODDS_INPUT_NAMES,
    EXCHANGE_SIZE_TRACE_INPUT_NAMES,
    FIRST_HALF_SIDE_SCOPE,
    FULL_TIME_SIDE_SCOPE,
)
from modules.pillars.pillar_2_side_market.relations import direction, relation
from modules.pillars.pillar_2_side_market.run_pillar_2 import calculate_pillar_2


TARGET_MINUTES = [120, 30, 5, 1, 0, -5]


@pytest.fixture(autouse=True)
def _default_target_selection(monkeypatch) -> None:
    monkeypatch.setitem(
        market_snapshot_extractor.HARDCODED_TARGET_MINUTE_BY_FLOW,
        "pre_start_signal_profile",
        None,
    )


def _event_context() -> EventContext:
    return EventContext(
        event_id=2002,
        custom_id="event-2002",
        sport="Football",
        season_id=2026,
        season_name="2026",
        season_year=2026,
        start_time_utc=datetime(2026, 8, 22, 18, 0, tzinfo=timezone.utc),
        minutes_until_start=5,
        discovery_source="test",
        home=ParticipantContext(
            participant_id=1,
            source="test",
            source_participant_id=101,
            name="Home",
            slug="home",
            short_name="H",
            source_status="normalized",
        ),
        away=ParticipantContext(
            participant_id=2,
            source="test",
            source_participant_id=202,
            name="Away",
            slug="away",
            short_name="A",
            source_status="normalized",
        ),
        competition=CompetitionContext(
            competition_id=99,
            source="test",
            source_tournament_id=99,
            source_unique_tournament_id=999,
            canonical_name="League",
            display_name="League",
            slug="league",
            unique_slug="league",
            category_id=1,
            category_name="Country",
            number_of_teams=20,
            number_of_teams_source="test",
            total_regular_season_games=38,
            standings_grouping="league",
            league_config_source="test",
            has_standings_source_endpoint=True,
            source_status="normalized",
        ),
        participants_label="Home vs Away",
        context_status="normalized",
        round="regular_season",
    )


def _add_market(
    rows: list[dict],
    *,
    minute: int,
    market_group: str,
    market_period: str,
    market_name: str,
    choice_group: str | None,
    bookie_id: int,
    prices: dict[str, float],
    exchange_side: str | None = None,
    exchange_sizes: dict[str, float | None] | None = None,
) -> None:
    for index, (choice_name, odds_value) in enumerate(prices.items(), start=1):
        rows.append(
            {
                "event_id": 2002,
                "market_id": len(rows) + 1,
                "market_group": market_group,
                "market_period": market_period,
                "market_name": market_name,
                "choice_group": choice_group,
                "choice_name": choice_name,
                "choice_id": index,
                "bookie_id": bookie_id,
                "bookie_name": {302: "Pinnacle", 3: "bet365", 4: "Betfair"}[bookie_id],
                "source": "oddspapi",
                "exchange_side": exchange_side,
                "exchange_level": 0,
                "target_minute": minute,
                "odds_value": odds_value,
                "snapshot_id": minute * 1000 + len(rows),
                "collected_at": "2026-08-22T17:55:00+00:00",
                "minutes_before_start": minute,
                "distance_from_target": 0,
                "exchange_size": (
                    exchange_sizes.get(choice_name)
                    if exchange_sizes is not None
                    else None
                ),
            }
        )


def _complete_rows(
    *,
    minute: int = 5,
    include_first_half: bool = True,
    pin_ft_1x2: tuple[float, float] = (2.0, 4.0),
    b365_ft_1x2: tuple[float, float] = (2.2, 3.3),
    pin_ft_ah: tuple[float, float] = (1.90, 2.00),
    b365_ft_ah: tuple[float, float] = (1.95, 1.95),
    pin_ft_ah_line: str = "-0.5",
    b365_ft_ah_line: str = "-0.5",
    pin_1h_1x2: tuple[float, float] = (2.20, 3.50),
    b365_1h_1x2: tuple[float, float] = (2.30, 3.40),
    pin_1h_ah: tuple[float, float] = (1.92, 1.98),
    b365_1h_ah: tuple[float, float] = (1.94, 1.96),
    pin_1h_ah_line: str = "-0.25",
    b365_1h_ah_line: str = "-0.25",
    back_prices: tuple[float, float, float] = (2.40, 3.20, 3.00),
    lay_prices: tuple[float, float, float] = (2.50, 3.30, 3.10),
    back_sizes: tuple[float | None, float | None, float | None] = (100, 80, 120),
    lay_sizes: tuple[float | None, float | None, float | None] = (90, 70, 110),
) -> list[dict]:
    rows: list[dict] = []
    markets = [
        ("1X2", "Full Time", "1X2 Full Time", None, 302, pin_ft_1x2),
        ("1X2", "Full Time", "1X2 Full Time", None, 3, b365_ft_1x2),
        ("Asian Handicap", "Full Time", "Asian Handicap Full Time", pin_ft_ah_line, 302, pin_ft_ah),
        ("Asian Handicap", "Full Time", "Asian Handicap Full Time", b365_ft_ah_line, 3, b365_ft_ah),
    ]
    if include_first_half:
        markets.extend(
            [
                ("1X2", "1st Half", "1X2 1st Half", None, 302, pin_1h_1x2),
                ("1X2", "1st Half", "1X2 1st Half", None, 3, b365_1h_1x2),
                ("Asian Handicap", "1st Half", "Asian Handicap 1st Half", pin_1h_ah_line, 302, pin_1h_ah),
                ("Asian Handicap", "1st Half", "Asian Handicap 1st Half", b365_1h_ah_line, 3, b365_1h_ah),
            ]
        )
    for group, period, name, line, bookie_id, prices in markets:
        _add_market(
            rows,
            minute=minute,
            market_group=group,
            market_period=period,
            market_name=name,
            choice_group=line,
            bookie_id=bookie_id,
            prices={"1": prices[0], "2": prices[1]},
        )
    for exchange_side, prices, sizes in (
        ("back", back_prices, back_sizes),
        ("lay", lay_prices, lay_sizes),
    ):
        _add_market(
            rows,
            minute=minute,
            market_group="1X2",
            market_period="Full Time",
            market_name="1X2 Full Time",
            choice_group=None,
            bookie_id=4,
            prices={"1": prices[0], "x": prices[1], "2": prices[2]},
            exchange_side=exchange_side,
            exchange_sizes={"1": sizes[0], "x": sizes[1], "2": sizes[2]},
        )
    return rows


def _calculate(rows: list[dict], *, debug_mode: bool = False) -> dict:
    context = build_odds_trajectory_context(rows, target_minutes_expected=TARGET_MINUTES)
    target_selection = market_snapshot_extractor.select_target_minute(
        context,
        flow_id="pre_start_signal_profile",
        expected_event_id=2002,
        allowed_target_minutes=TARGET_MINUTES,
    )
    return calculate_pillar_2(
        _event_context(),
        context,
        target_selection=target_selection,
        debug_mode=debug_mode,
    )


def _profile(result: dict) -> dict:
    profile = result["P2_SIGNAL_PROFILE"]
    assert profile is not None
    return profile


def _all_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {key for child in value.values() for key in _all_keys(child)}
    if isinstance(value, list):
        return {key for child in value for key in _all_keys(child)}
    return set()


def test_full_time_and_first_half_complete_produce_exact_active_contract() -> None:
    result = _calculate(_complete_rows())
    profile = _profile(result)

    assert result["P2_STATUS"] == "ACTIVE"
    assert result["engine_version"] == "p2-signal-profile-v1"
    assert set(profile) == {"FT", "1H", "FT_1H", "EXCHANGE", "BOOK_EXCHANGE"}
    assert set(profile["FT"]) == {"1X2", "AH", "CROSS_MARKET"}
    assert set(profile["FT"]["1X2"]) == {
        "PIN_EDGE", "PIN_DIRECTION", "B365_EDGE", "B365_DIRECTION",
        "BOOK_RELATION", "BOOK_GAP", "REP_EDGE", "DIRECTION",
    }
    assert profile["1H"] is not None
    assert profile["FT_1H"] is not None
    assert result["modules"][0]["P2_SIGNAL_PROFILE"] is profile
    assert result["raw"] is result["modules"][0]["raw"]


def test_full_time_complete_and_first_half_absent_keeps_ft_and_marks_partial() -> None:
    result = _calculate(_complete_rows(include_first_half=False))
    profile = _profile(result)

    assert result["P2_STATUS"] == "PARTIAL"
    assert result["PERIODS"]["full_time"]["status"] == "COMPLETE"
    assert result["PERIODS"]["first_half"]["status"] == "INCOMPLETE"
    assert profile["FT"] is not None
    assert profile["1H"] is None
    assert profile["FT_1H"] is None
    assert result["raw"]["reason"] == "first_half_incomplete"


def test_full_time_incomplete_is_insufficient_even_with_complete_first_half() -> None:
    rows = [
        row for row in _complete_rows()
        if not (row["market_period"] == "Full Time" and row["bookie_id"] == 302 and row["market_group"] == "1X2")
    ]
    result = _calculate(rows)

    assert result["P2_STATUS"] == "INSUFFICIENT_DATA"
    assert result["P2_SIGNAL_PROFILE"] is None
    assert result["PERIODS"]["first_half"]["status"] == "COMPLETE"
    assert result["modules"] == []


@pytest.mark.parametrize(
    ("back_sizes", "lay_sizes"),
    [
        ((None, None, None), (None, None, None)),
        ((100, 50, 80), (None, None, 75)),
    ],
)
def test_exchange_sizes_are_nullable_traceability_only(back_sizes, lay_sizes) -> None:
    result = _calculate(_complete_rows(back_sizes=back_sizes, lay_sizes=lay_sizes))

    assert result["P2_STATUS"] == "ACTIVE"
    expected = dict(zip(EXCHANGE_SIZE_TRACE_INPUT_NAMES, (
        back_sizes[0], lay_sizes[0], back_sizes[1], lay_sizes[1], back_sizes[2], lay_sizes[2]
    )))
    for name, value in expected.items():
        assert result["raw"]["inputs"][name] == value
        assert name not in result["MISSING_INPUTS"]
    assert not (set(EXCHANGE_SIZE_TRACE_INPUT_NAMES) & _all_keys(_profile(result)))


def test_exchange_sizes_do_not_change_signal_profile() -> None:
    small = _calculate(_complete_rows(back_sizes=(1, 2, 3), lay_sizes=(4, 5, 6)))
    large = _calculate(_complete_rows(back_sizes=(10_000, 20_000, 30_000), lay_sizes=(40_000, 50_000, 60_000)))

    assert small["P2_SIGNAL_PROFILE"] == large["P2_SIGNAL_PROFILE"]
    assert small["raw"]["inputs"] != large["raw"]["inputs"]


@pytest.mark.parametrize(
    ("pin_prices", "b365_prices", "expected"),
    [
        ((2.0, 4.0), (2.2, 3.3), "CONVERGENCE_HOME"),
        ((4.0, 2.0), (3.3, 2.2), "CONVERGENCE_AWAY"),
        ((2.0, 4.0), (4.0, 2.0), "DIVERGENCE"),
        ((2.0, 2.0), (2.0, 4.0), "NEUTRAL"),
    ],
)
def test_1x2_book_relations(pin_prices, b365_prices, expected) -> None:
    signal = _profile(_calculate(_complete_rows(pin_ft_1x2=pin_prices, b365_ft_1x2=b365_prices)))["FT"]["1X2"]
    assert signal["BOOK_RELATION"] == expected


def test_neutral_is_a_calculated_direction_not_unavailability() -> None:
    assert direction(side_edge(*map(__import__("decimal").Decimal, ("2", "2")))) == "NEUTRAL"
    assert relation("NEUTRAL", "HOME") == "NEUTRAL"


def test_ah_same_line_exposes_comparable_readings() -> None:
    ah = _profile(_calculate(_complete_rows()))["FT"]["AH"]
    assert ah["PIN_EDGE"] is not None
    assert ah["B365_EDGE"] is not None
    assert ah["BOOK_RELATION"] is not None
    assert ah["PRICE_GAP"] is not None
    assert ah["REP_EDGE"] is not None
    assert ah["DIRECTION"] is not None


def test_ah_different_lines_preserves_individuals_but_marks_comparison_unavailable() -> None:
    profile = _profile(_calculate(_complete_rows(b365_ft_ah_line="-0.75")))
    ah = profile["FT"]["AH"]

    assert ah["PIN_EDGE"] is not None
    assert ah["PIN_DIRECTION"] is not None
    assert ah["B365_EDGE"] is not None
    assert ah["B365_DIRECTION"] is not None
    assert ah["LINE_GAP"] == pytest.approx(0.25)
    assert ah["BOOK_RELATION"] is None
    assert ah["PRICE_GAP"] is None
    assert ah["REP_EDGE"] is None
    assert ah["DIRECTION"] is None
    assert profile["FT"]["CROSS_MARKET"] == {
        "FT_1X2_AH_RELATION": None,
        "FT_CROSS_MARKET_GAP": None,
    }


def test_ft_cross_market_divergence_is_preserved_without_global_score() -> None:
    profile = _profile(_calculate(_complete_rows(
        pin_ft_ah=(3.0, 2.0),
        b365_ft_ah=(3.2, 2.1),
    )))
    assert profile["FT"]["1X2"]["DIRECTION"] == "HOME"
    assert profile["FT"]["AH"]["DIRECTION"] == "AWAY"
    assert profile["FT"]["CROSS_MARKET"]["FT_1X2_AH_RELATION"] == "DIVERGENCE"
    assert profile["FT"]["CROSS_MARKET"]["FT_CROSS_MARKET_GAP"] > 0


@pytest.mark.parametrize(
    ("lay_prices", "expected"),
    [((2.50, 3.30, 3.10), "CONVERGENCE_HOME"), ((4.0, 3.0, 2.0), "DIVERGENCE")],
)
def test_back_lay_relations(lay_prices, expected) -> None:
    exchange = _profile(_calculate(_complete_rows(lay_prices=lay_prices)))["EXCHANGE"]
    assert exchange["BACK_LAY_RELATION"] == expected


@pytest.mark.parametrize(
    ("back_prices", "lay_prices", "expected"),
    [
        ((2.4, 3.2, 3.0), (2.5, 3.3, 3.1), "CONVERGENCE_HOME"),
        ((4.0, 3.0, 2.0), (4.2, 3.1, 2.1), "DIVERGENCE"),
        ((2.0, 3.0, 2.0), (2.0, 3.0, 2.0), "NEUTRAL"),
    ],
)
def test_book_exchange_relations(back_prices, lay_prices, expected) -> None:
    block = _profile(_calculate(_complete_rows(back_prices=back_prices, lay_prices=lay_prices)))["BOOK_EXCHANGE"]
    assert block["RELATION"] == expected
    assert block["GAP"] >= 0


@pytest.mark.parametrize(
    ("pin_1h", "b365_1h", "expected"),
    [
        ((2.2, 3.5), (2.3, 3.4), "CONVERGENCE_HOME"),
        ((4.0, 2.0), (3.8, 2.1), "DIVERGENCE"),
    ],
)
def test_ft_first_half_compares_only_1x2(pin_1h, b365_1h, expected) -> None:
    block = _profile(_calculate(_complete_rows(pin_1h_1x2=pin_1h, b365_1h_1x2=b365_1h)))["FT_1H"]
    assert block["FT_1H_1X2_RELATION"] == expected
    assert "FT_1H_AH_RELATION" not in block
    assert "FT_1H_AH_GAP" not in block


def test_one_strict_target_minute_has_no_per_market_fallback() -> None:
    rows = _complete_rows(minute=30) + _complete_rows(minute=5)
    rows = [
        row for row in rows
        if not (row["target_minute"] == 5 and row["market_period"] == "1st Half" and row["bookie_id"] == 3 and row["choice_name"] == "2")
    ]
    result = _calculate(rows)

    assert result["P2_TARGET_MINUTE"] == 5
    assert result["P2_STATUS"] == "PARTIAL"
    assert {trace["target_minute"] for trace in result["raw"]["input_trace"].values()} == {5}
    assert result["raw"]["inputs"]["B365_HOME_1X2_1H_ODDS_PRICE"] == pytest.approx(2.30)
    assert result["raw"]["inputs"]["B365_AWAY_1X2_1H_ODDS_PRICE"] is None
    assert _profile(result)["1H"] is not None


def test_partial_first_half_1x2_preserves_valid_branch_without_changing_full_time(
    caplog,
) -> None:
    caplog.set_level(logging.INFO)
    complete = _calculate(_complete_rows())
    rows = [
        row for row in _complete_rows()
        if not (
            row["market_period"] == "1st Half"
            and row["market_group"] == "1X2"
            and row["bookie_id"] == 302
            and row["choice_name"] == "2"
        )
    ]
    result = _calculate(rows, debug_mode=True)
    profile = _profile(result)
    one_x_two = profile["1H"]["1X2"]

    assert result["P2_STATUS"] == "PARTIAL"
    assert result["PERIODS"]["first_half"]["status"] == "INCOMPLETE"
    assert profile["FT"] == _profile(complete)["FT"]
    assert one_x_two["PIN_EDGE"] is None
    assert one_x_two["PIN_DIRECTION"] is None
    assert one_x_two["B365_EDGE"] is not None
    assert one_x_two["B365_DIRECTION"] is not None
    assert one_x_two["BOOK_RELATION"] is None
    assert one_x_two["BOOK_GAP"] is None
    assert one_x_two["REP_EDGE"] is None
    assert one_x_two["DIRECTION"] is None
    assert profile["1H"]["AH"]["REP_EDGE"] is not None
    assert profile["1H"]["CROSS_MARKET"]["1H_1X2_AH_RELATION"] is None
    assert profile["FT_1H"] is None
    assert result["raw"]["inputs"]["PIN_HOME_1X2_1H_ODDS_PRICE"] == pytest.approx(2.20)
    assert result["raw"]["inputs"]["PIN_AWAY_1X2_1H_ODDS_PRICE"] is None
    assert "P2 SIGNAL | FT_1H | unavailable because required dependencies are incomplete" in caplog.text


def test_partial_first_half_ah_does_not_remove_complete_1x2_or_ft_1h() -> None:
    rows = [
        row for row in _complete_rows()
        if not (
            row["market_period"] == "1st Half"
            and row["market_group"] == "Asian Handicap"
            and row["bookie_id"] == 302
            and row["choice_name"] == "2"
        )
    ]
    result = _calculate(rows)
    profile = _profile(result)
    asian_handicap = profile["1H"]["AH"]

    assert result["P2_STATUS"] == "PARTIAL"
    assert profile["1H"]["1X2"]["REP_EDGE"] is not None
    assert profile["FT_1H"] is not None
    assert asian_handicap["PIN_LINE"] == pytest.approx(-0.25)
    assert asian_handicap["B365_LINE"] == pytest.approx(-0.25)
    assert asian_handicap["LINE_GAP"] == pytest.approx(0)
    assert asian_handicap["PIN_EDGE"] is None
    assert asian_handicap["B365_EDGE"] is not None
    assert asian_handicap["REP_EDGE"] is None
    assert profile["1H"]["CROSS_MARKET"]["1H_1X2_AH_RELATION"] is None


def test_partial_first_half_ah_line_preserves_individual_price_edges() -> None:
    rows = _complete_rows()
    for row in rows:
        if (
            row["market_period"] == "1st Half"
            and row["market_group"] == "Asian Handicap"
            and row["bookie_id"] == 302
        ):
            row["choice_group"] = None
    result = _calculate(rows)
    asian_handicap = _profile(result)["1H"]["AH"]

    assert result["P2_STATUS"] == "PARTIAL"
    assert asian_handicap["PIN_LINE"] is None
    assert asian_handicap["PIN_EDGE"] is not None
    assert asian_handicap["PIN_DIRECTION"] is not None
    assert asian_handicap["B365_EDGE"] is not None
    assert asian_handicap["LINE_GAP"] is None
    assert asian_handicap["BOOK_RELATION"] is None
    assert asian_handicap["REP_EDGE"] is None


def test_multiple_partial_first_half_candidates_remain_ambiguous() -> None:
    rows = [
        row for row in _complete_rows()
        if not (
            row["market_period"] == "1st Half"
            and row["market_group"] == "Asian Handicap"
            and row["bookie_id"] == 302
            and row["choice_name"] == "2"
        )
    ]
    _add_market(
        rows,
        minute=5,
        market_group="Asian Handicap",
        market_period="1st Half",
        market_name="Asian Handicap 1st Half",
        choice_group="-0.75",
        bookie_id=302,
        prices={"1": 1.91},
    )
    result = _calculate(rows)

    assert result["P2_STATUS"] == "PARTIAL"
    assert result["PERIODS"]["first_half"]["status"] == "AMBIGUOUS"
    assert _profile(result)["1H"] is None
    assert "PIN_AH_1H_LINE" in result["AMBIGUOUS_INPUTS"]


def test_multiple_complete_ah_candidates_remain_ambiguous() -> None:
    rows = _complete_rows()
    _add_market(
        rows,
        minute=5,
        market_group="Asian Handicap",
        market_period="Full Time",
        market_name="Asian Handicap Full Time",
        choice_group="-0.75",
        bookie_id=302,
        prices={"1": 1.91, "2": 1.99},
    )
    result = _calculate(rows)

    assert result["P2_STATUS"] == "INSUFFICIENT_DATA"
    assert result["PERIODS"]["full_time"]["status"] == "AMBIGUOUS"
    assert "PIN_AH_FULL_TIME_LINE" in result["AMBIGUOUS_INPUTS"]


def test_period_scope_separates_required_inputs_from_trace_inputs() -> None:
    assert FULL_TIME_SIDE_SCOPE.required is True
    assert FIRST_HALF_SIDE_SCOPE.required is False
    assert set(EXCHANGE_ODDS_INPUT_NAMES) <= set(FULL_TIME_SIDE_SCOPE.required_input_names())
    assert set(EXCHANGE_SIZE_TRACE_INPUT_NAMES) == set(FULL_TIME_SIDE_SCOPE.trace_input_names())
    assert not (set(EXCHANGE_SIZE_TRACE_INPUT_NAMES) & set(FULL_TIME_SIDE_SCOPE.required_input_names()))
    assert not hasattr(FULL_TIME_SIDE_SCOPE, "metric_names")


def test_debug_logging_reports_structural_blocks_only(caplog) -> None:
    caplog.set_level(logging.INFO)
    _calculate(_complete_rows(), debug_mode=True)

    assert "P2 SIGNAL | FT 1X2" in caplog.text
    assert "P2 SIGNAL | EXCHANGE" in caplog.text
    assert "P2 DEBUG | input assignment | name=PIN_HOME_1X2_FULL_TIME_ODDS_PRICE" in caplog.text
    assert "P2 DEBUG | input lineage | name=PIN_HOME_1X2_FULL_TIME_ODDS_PRICE | market_group=1X2 | period=Full Time | market_name=1X2 Full Time" in caplog.text
    assert "P2 DEBUG | input lineage | name=PIN_AH_HOME_FULL_TIME_ODDS_PRICE | market_group=Asian Handicap | period=Full Time | market_name=Asian Handicap Full Time" in caplog.text
    assert "P2 FORMULA | FT.1X2.PIN_EDGE | formula=" in caplog.text
    assert "P2 FORMULA | FT.1X2.PIN_EDGE | substitution=" in caplog.text
    assert "P2 FORMULA | FT.1X2.PIN_EDGE | result=" in caplog.text
    assert "P2 FORMULA | FT.CROSS_MARKET.RELATION" in caplog.text
    assert "P2 FORMULA | FT.EXCHANGE.SIDE_SPREAD | substitution=" in caplog.text
