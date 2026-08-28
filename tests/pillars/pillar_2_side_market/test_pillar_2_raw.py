from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest

from modules.jobs.pre_start_check_job import pillar_pipeline
from modules.pillars import market_snapshot_extractor
from modules.pillars.context import CompetitionContext, EventContext, ParticipantContext
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.pillar_2_side_market.periods import (
    FIRST_HALF_SIDE_SCOPE,
    FULL_TIME_SIDE_SCOPE,
    P2_SIDE_PERIOD_SCOPES,
    optional_metric_names,
)
from modules.pillars.pillar_2_side_market.run_pillar_2 import calculate_pillar_2


TARGET_MINUTES = [120, 30, 5, 1, 0, -5]


@pytest.fixture(autouse=True)
def _use_default_target_selection_for_tests(monkeypatch) -> None:
    """Keep unit tests independent from the production simulation override."""
    monkeypatch.setitem(
        market_snapshot_extractor.HARDCODED_TARGET_MINUTE_BY_FLOW,
        "pillar_2",
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
    bookie_name: str,
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
                "bookie_name": bookie_name,
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
    pin_1h_prices: tuple[float, float] = (2.20, 3.50),
    b365_1h_prices: tuple[float, float] = (2.30, 3.40),
    pin_ft_ah_line: str = "-0.5",
    b365_ft_ah_line: str = "-0.5",
    pin_1h_ah_line: str = "-0.25",
    b365_1h_ah_line: str = "-0.25",
    ) -> list[dict]:
    rows: list[dict] = []
    regular_markets = (
        (
            "1X2", "Full Time", "1X2 Full Time", None,
            302, "Pinnacle Sports", {"1": 2.0, "2": 4.0},
        ),
        (
            "1X2", "Full Time", "1X2 Full Time", None,
            3, "bet365", {"1": 2.2, "2": 3.3},
        ),
        (
            "Asian Handicap", "Full Time", "Asian Handicap Full Time",
            pin_ft_ah_line, 302, "Pinnacle Sports", {"1": 1.90, "2": 2.00},
        ),
        (
            "Asian Handicap", "Full Time", "Asian Handicap Full Time",
            b365_ft_ah_line, 3, "bet365", {"1": 1.95, "2": 1.95},
        ),
        (
            "1X2", "1st Half", "1X2 1st Half", None,
            302, "Pinnacle Sports", {"1": pin_1h_prices[0], "2": pin_1h_prices[1]},
        ),
        (
            "1X2", "1st Half", "1X2 1st Half", None,
            3, "bet365", {"1": b365_1h_prices[0], "2": b365_1h_prices[1]},
        ),
        (
            "Asian Handicap", "1st Half", "Asian Handicap 1st Half",
            pin_1h_ah_line, 302, "Pinnacle Sports", {"1": 1.92, "2": 1.98},
        ),
        (
            "Asian Handicap", "1st Half", "Asian Handicap 1st Half",
            b365_1h_ah_line, 3, "bet365", {"1": 1.94, "2": 1.96},
        ),
    )
    for market_group, period, name, line, bookie_id, bookie_name, prices in regular_markets:
        _add_market(
            rows,
            minute=minute,
            market_group=market_group,
            market_period=period,
            market_name=name,
            choice_group=line,
            bookie_id=bookie_id,
            bookie_name=bookie_name,
            prices=prices,
        )

    for exchange_side, prices, sizes in (
        ("back", {"1": 2.40, "x": 3.20, "2": 3.00}, {"1": 100.0, "x": 80.0, "2": 120.0}),
        ("lay", {"1": 2.50, "x": 3.30, "2": 3.10}, {"1": 90.0, "x": 70.0, "2": 110.0}),
    ):
        _add_market(
            rows,
            minute=minute,
            market_group="1X2",
            market_period="Full Time",
            market_name="1X2 Full Time",
            choice_group=None,
            bookie_id=4,
            bookie_name="Betfair",
            prices=prices,
            exchange_side=exchange_side,
            exchange_sizes=sizes,
        )
    return rows


def _calculate(rows: list[dict]) -> dict:
    context = build_odds_trajectory_context(
        rows,
        target_minutes_expected=TARGET_MINUTES,
    )
    return calculate_pillar_2(_event_context(), context)


def _edge(home_price: float, away_price: float) -> float:
    home_raw = 1 / home_price
    away_raw = 1 / away_price
    return (home_raw - away_raw) / (home_raw + away_raw)


def test_calculates_all_raw_blocks_from_one_canonical_minute() -> None:
    result = _calculate(_complete_rows())

    pin_edge = _edge(2.0, 4.0)
    b365_edge = _edge(2.2, 3.3)
    book_edge = 0.5 * pin_edge + 0.5 * b365_edge
    back_edge = _edge(2.4, 3.0)
    lay_edge = _edge(2.5, 3.1)
    exchange_edge = 0.5 * back_edge + 0.5 * lay_edge

    assert result["P2_STATUS"] == "ACTIVE"
    assert result["P2_TARGET_MINUTE"] == 5
    assert result["PERIODS"]["full_time"]["status"] == "COMPLETE"
    assert result["PERIODS"]["first_half"]["status"] == "COMPLETE"
    assert result["PIN_SIDE_EDGE"] == pytest.approx(pin_edge)
    assert result["BOOK_EDGE"] == pytest.approx(book_edge)
    assert result["EXCHANGE_EDGE"] == pytest.approx(exchange_edge)
    assert result["SIDE_MARKET_EDGE"] == pytest.approx(
        0.5 * book_edge + 0.5 * exchange_edge
    )
    assert result["P2_DIRECTION_RAW"] == "HOME"


    assert result["Q_COMPLETE"] == 1.0
    assert result["BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE"] == 70.0
    assert result["raw"]["p2_raw_engine"]["mining_context"]["market_type"] == "1X2"
    assert {
        trace["target_minute"]
        for trace in result["raw"]["p2_raw_engine"]["input_trace"].values()
    } == {5}
    for forbidden in ("P2_VALID", "P2_STRENGTH", "P2_CONFIDENCE", "P2_STATE"):
        assert forbidden not in result


def test_period_scopes_declare_full_time_required_and_first_half_optional() -> None:
    assert FULL_TIME_SIDE_SCOPE.required is True
    assert FIRST_HALF_SIDE_SCOPE.required is False
    assert [scope.key for scope in P2_SIDE_PERIOD_SCOPES] == ["full_time", "first_half"]
    assert "SIDE_MARKET_EDGE" in FULL_TIME_SIDE_SCOPE.metric_names
    assert "PIN_1H_SIDE_EDGE" in FIRST_HALF_SIDE_SCOPE.metric_names
    assert "FT_1H_GAP" not in FIRST_HALF_SIDE_SCOPE.metric_names
    assert "FT_1H_GAP" in optional_metric_names()


def test_formula_logging_is_enabled_only_in_debug_mode(caplog) -> None:
    rows = _complete_rows()
    context = build_odds_trajectory_context(
        rows,
        target_minutes_expected=TARGET_MINUTES,
    )

    caplog.set_level(logging.INFO)
    calculate_pillar_2(_event_context(), context, debug_mode=False)
    assert "P2_RAW_ENGINE DEBUG" not in caplog.text

    caplog.clear()
    calculate_pillar_2(_event_context(), context, debug_mode=True)
    assert "P2_RAW_ENGINE DEBUG | PIN_SIDE_EDGE" in caplog.text
    assert "P2_SIDE_MARKET DEBUG | Asignación de input:" in caplog.text


def test_matches_bookmakers_by_canonical_id_not_display_name() -> None:
    rows = _complete_rows()
    for row in rows:
        if row["bookie_id"] == 302:
            row["bookie_id"] = 999

    result = _calculate(rows)

    assert result["P2_STATUS"] == "INSUFFICIENT_DATA"
    assert "PIN_HOME_1X2_FULL_TIME_ODDS_PRICE" in result["MISSING_INPUTS"]
    assert result["PERIODS"]["full_time"]["status"] == "INCOMPLETE"
    assert "SIDE_MARKET_EDGE" not in result


def test_does_not_fallback_when_first_half_input_is_missing_at_latest_minute() -> None:
    rows = _complete_rows(minute=30) + _complete_rows(minute=5)
    rows = [
        row
        for row in rows
        if not (
            row["target_minute"] == 5
            and row["market_name"] == "1X2 1st Half"
            and row["bookie_id"] == 3
            and row["choice_name"] == "2"
        )
    ]

    result = _calculate(rows)

    assert result["P2_STATUS"] == "PARTIAL"
    assert result["P2_TARGET_MINUTE"] == 5
    assert result["PERIODS"]["full_time"]["status"] == "COMPLETE"
    assert result["PERIODS"]["first_half"]["status"] == "INCOMPLETE"
    assert "B365_AWAY_1X2_1H_ODDS_PRICE" in result["MISSING_INPUTS"]
    assert "B365_AWAY_1X2_1H_ODDS_PRICE" in result["PERIODS"]["first_half"]["missing_inputs"]
    assert result["SIDE_MARKET_EDGE"] is not None
    assert result["P2_DIRECTION_RAW"] == "HOME"
    for name in optional_metric_names():
        assert result[name] is None
    assert result["raw"]["excluded_metrics"]["PIN_1H_SIDE_EDGE"] == "first_half_incomplete"
    assert result["modules"]


def test_hardcoded_target_minute_overrides_default_latest_selection(monkeypatch) -> None:
    rows = _complete_rows(minute=5) + _complete_rows(minute=0)
    monkeypatch.setitem(
        market_snapshot_extractor.HARDCODED_TARGET_MINUTE_BY_FLOW,
        "pillar_2",
        0,
    )

    result = _calculate(rows)

    assert result["P2_STATUS"] == "ACTIVE"
    assert result["P2_TARGET_MINUTE"] == 0
    assert {
        trace["target_minute"]
        for trace in result["raw"]["p2_raw_engine"]["input_trace"].values()
    } == {0}


def test_aborts_when_a_required_exchange_size_is_missing() -> None:
    rows = _complete_rows()
    for row in rows:
        if (
            row["bookie_id"] == 4
            and row["exchange_side"] == "lay"
            and row["choice_name"] == "x"
        ):
            row["exchange_size"] = None

    result = _calculate(rows)

    assert result["P2_STATUS"] == "INSUFFICIENT_DATA"
    assert "BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE" in result["MISSING_INPUTS"]
    assert result["PERIODS"]["full_time"]["status"] == "INCOMPLETE"
    assert "Q_COMPLETE" not in result
    assert "SIDE_MARKET_EDGE" not in result
    assert "BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE" not in result


def test_keeps_ah_price_metrics_null_when_lines_are_different() -> None:
    result = _calculate(
        _complete_rows(
            b365_ft_ah_line="-0.75",
            b365_1h_ah_line="0",
        )
    )

    assert result["P2_STATUS"] == "ACTIVE"
    assert result["AH_LINE_GAP"] == pytest.approx(0.25)
    assert result["PIN_AH_EDGE"] is None
    assert result["B365_AH_EDGE"] is None
    assert result["AH_PRICE_GAP"] is None
    assert result["AH_1H_LINE_GAP"] == pytest.approx(0.25)
    assert result["PIN_AH_1H_EDGE"] is None
    assert result["B365_AH_1H_EDGE"] is None
    assert result["AH_1H_PRICE_GAP"] is None


def test_first_half_changes_diagnostics_but_not_the_ft_final_edge() -> None:
    home_1h = _calculate(_complete_rows())
    away_1h = _calculate(
        _complete_rows(
            pin_1h_prices=(4.0, 2.0),
            b365_1h_prices=(3.8, 2.1),
        )
    )

    assert home_1h["BOOK_1H_EDGE"] > 0
    assert away_1h["BOOK_1H_EDGE"] < 0
    assert home_1h["SIDE_MARKET_EDGE"] == pytest.approx(away_1h["SIDE_MARKET_EDGE"])
    assert home_1h["P2_DIRECTION_RAW"] == away_1h["P2_DIRECTION_RAW"]


def _add_extra_ah_line(
    rows: list[dict],
    *,
    market_period: str,
    market_name: str,
    bookie_id: int,
    line: str,
    minute: int = 5,
) -> None:
    _add_market(
        rows,
        minute=minute,
        market_group="Asian Handicap",
        market_period=market_period,
        market_name=market_name,
        choice_group=line,
        bookie_id=bookie_id,
        bookie_name="Pinnacle Sports" if bookie_id == 302 else "bet365",
        prices={"1": 1.91, "2": 1.99},
    )


def test_ambiguous_first_half_keeps_full_time_partial_signal() -> None:
    complete = _calculate(_complete_rows())
    rows = _complete_rows()
    _add_extra_ah_line(
        rows,
        market_period="1st Half",
        market_name="Asian Handicap 1st Half",
        bookie_id=302,
        line="0",
    )

    result = _calculate(rows)

    assert result["P2_STATUS"] == "PARTIAL"
    assert result["PERIODS"]["full_time"]["status"] == "COMPLETE"
    assert result["PERIODS"]["first_half"]["status"] == "AMBIGUOUS"
    assert "PIN_AH_1H_LINE" in result["PERIODS"]["first_half"]["ambiguous_inputs"]
    assert result["SIDE_MARKET_EDGE"] == pytest.approx(complete["SIDE_MARKET_EDGE"])
    assert result["P2_DIRECTION_RAW"] == complete["P2_DIRECTION_RAW"]
    assert result["BOOK_DIRECTION_FT"] == complete["BOOK_DIRECTION_FT"]
    for name in optional_metric_names():
        assert result[name] is None
    assert result["raw"]["excluded_metrics"]["FT_1H_GAP"] == (
        "cross_period_requires_both_periods"
    )


def test_missing_first_half_snapshot_at_target_minute_is_partial() -> None:
    complete = _calculate(_complete_rows())
    rows = _complete_rows(minute=30) + [
        row
        for row in _complete_rows(minute=5)
        if row["market_period"] != "1st Half"
    ]

    result = _calculate(rows)

    assert result["P2_STATUS"] == "PARTIAL"
    assert result["P2_TARGET_MINUTE"] == 5
    assert result["PERIODS"]["full_time"]["status"] == "COMPLETE"
    assert result["PERIODS"]["first_half"]["status"] == "INCOMPLETE"
    assert result["PERIODS"]["first_half"]["missing_inputs"]
    assert result["SIDE_MARKET_EDGE"] == pytest.approx(complete["SIDE_MARKET_EDGE"])
    assert result["P2_DIRECTION_RAW"] == "HOME"
    for name in optional_metric_names():
        assert result[name] is None


def test_ambiguous_full_time_aborts_even_when_first_half_is_complete() -> None:
    rows = _complete_rows()
    _add_extra_ah_line(
        rows,
        market_period="Full Time",
        market_name="Asian Handicap Full Time",
        bookie_id=302,
        line="-0.75",
    )

    result = _calculate(rows)

    assert result["P2_STATUS"] == "INSUFFICIENT_DATA"
    assert result["PERIODS"]["full_time"]["status"] == "AMBIGUOUS"
    assert result["PERIODS"]["first_half"]["status"] == "COMPLETE"
    assert "PIN_AH_FULL_TIME_LINE" in result["PERIODS"]["full_time"]["ambiguous_inputs"]
    assert "SIDE_MARKET_EDGE" not in result
    assert "P2_DIRECTION_RAW" not in result
    assert result["modules"] == []
    assert result["raw"]["first_half"]["note"] == (
        "first_half_complete_but_unused_without_full_time"
    )
    assert result["raw"]["first_half"]["inputs"]["PIN_HOME_1X2_1H_ODDS_PRICE"] == 2.2


def test_pipeline_returns_p2_even_when_p1_history_is_unavailable(monkeypatch) -> None:
    event_context = _event_context()
    event_context.odds_trajectory = _complete_rows()
    monkeypatch.setattr(
        pillar_pipeline.Config,
        "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS",
        False,
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_4",
        lambda **_kwargs: {"P4_STATUS": "INSUFFICIENT_DATA"},
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_5",
        lambda **_kwargs: {
            "P5_STATUS": "INSUFFICIENT_DATA",
            "P5_VALID": False,
            "P5_DIRECTION": "NONE",
            "P5": 0.0,
            "P5_STRENGTH": "NONE",
        },
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "resolve_matchup_streak_analysis",
        lambda **_kwargs: (None, False),
    )

    result = pillar_pipeline.EventPillarProcessor(event_repo=object()).process_event(
        event_context
    )

    assert result is not None
    assert result["pillar_1"] is None
    assert result["pillar_2"]["P2_STATUS"] == "ACTIVE"
    assert result["pillar_2"]["P2_TARGET_MINUTE"] == 5
    assert result["pillar_3"]["P3_STATUS"] == "INSUFFICIENT_DATA"


def test_mining_failure_does_not_prevent_later_pillars(monkeypatch, caplog) -> None:
    event_context = _event_context()
    event_context.odds_trajectory = []
    p4_calls = []

    class FailingMiningService:
        def persist(self, _pillar_id, _event_context, _p2_result):
            raise RuntimeError("mining database unavailable")

    monkeypatch.setattr(
        pillar_pipeline.Config,
        "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS",
        False,
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_2",
        lambda **_kwargs: {
            "pillar_id": "pillar_2_side_market",
            "engine_version": "p2_raw_v1",
            "P2_STATUS": "ACTIVE",
            "P2_TARGET_MINUTE": 5,
            "P2_DIRECTION_RAW": "HOME",
            "SIDE_MARKET_EDGE": 0.1,
            "raw": {},
        },
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_4",
        lambda **_kwargs: p4_calls.append(True) or {
            "P4_STATUS": "INSUFFICIENT_DATA"
        },
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_5",
        lambda **_kwargs: {
            "P5_STATUS": "INSUFFICIENT_DATA",
            "P5_VALID": False,
            "P5_DIRECTION": "NONE",
            "P5": 0.0,
            "P5_STRENGTH": "NONE",
        },
    )

    result = pillar_pipeline.EventPillarProcessor(
        event_repo=object(),
        enabled_pillars={"pillar_1": False},
        mining_service=FailingMiningService(),
    ).process_event(event_context)

    assert result is not None
    assert p4_calls == [True]
    assert "Pillar mining persistence failed" in caplog.text


def test_p3_exception_is_isolated_and_later_pillars_continue(monkeypatch) -> None:
    event_context = _event_context()
    event_context.odds_trajectory = []
    later_calls = []

    monkeypatch.setattr(
        pillar_pipeline.Config,
        "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS",
        False,
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_2",
        lambda **_kwargs: {
            "P2_STATUS": "INSUFFICIENT_DATA",
            "P2_TARGET_MINUTE": None,
        },
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_3",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("P3 exploded")),
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_4",
        lambda **_kwargs: later_calls.append("pillar_4") or {
            "P4_STATUS": "INSUFFICIENT_DATA",
        },
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_5",
        lambda **_kwargs: later_calls.append("pillar_5") or {
            "P5_STATUS": "INSUFFICIENT_DATA",
            "P5_VALID": False,
            "P5_DIRECTION": "NONE",
            "P5": 0.0,
            "P5_STRENGTH": "NONE",
        },
    )

    result = pillar_pipeline.EventPillarProcessor(
        event_repo=object(),
        enabled_pillars={"pillar_1": False},
    ).process_event(event_context)

    assert result is not None
    assert result["pillar_3"]["P3_STATUS"] == "ERROR"
    assert result["pillar_3"]["raw"]["reason"] == "pillar_3_exception"
    assert later_calls == ["pillar_4", "pillar_5"]


@pytest.mark.parametrize(
    "disabled_pillar",
    ["pillar_2", "pillar_3", "pillar_4", "pillar_5"],
)
def test_individual_pillar_toggles_skip_only_selected_calculation(
    monkeypatch,
    disabled_pillar,
) -> None:
    event_context = _event_context()
    event_context.odds_trajectory = []
    calls = []

    monkeypatch.setattr(
        pillar_pipeline.Config,
        "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS",
        False,
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_2",
        lambda **_kwargs: calls.append("pillar_2") or {
            "P2_STATUS": "INSUFFICIENT_DATA",
            "P2_TARGET_MINUTE": 5,
        },
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_3",
        lambda **_kwargs: calls.append("pillar_3") or {
            "P3_STATUS": "INSUFFICIENT_DATA",
            "TARGET_MINUTE": 5,
            "PERIOD": None,
        },
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_4",
        lambda **_kwargs: calls.append("pillar_4") or {
            "P4_STATUS": "INSUFFICIENT_DATA",
        },
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "calculate_pillar_5",
        lambda **_kwargs: calls.append("pillar_5") or {
            "P5_STATUS": "INSUFFICIENT_DATA",
            "P5_VALID": False,
            "P5_DIRECTION": "NONE",
            "P5": 0.0,
            "P5_STRENGTH": "NONE",
        },
    )

    enabled_pillars = {
        "pillar_1": False,
        "pillar_2": disabled_pillar != "pillar_2",
        "pillar_3": disabled_pillar != "pillar_3",
        "pillar_4": disabled_pillar != "pillar_4",
        "pillar_5": disabled_pillar != "pillar_5",
    }

    result = pillar_pipeline.EventPillarProcessor(
        event_repo=object(),
        enabled_pillars=enabled_pillars,
    ).process_event(event_context)

    assert result is not None
    assert calls == [
        pillar_key
        for pillar_key in ("pillar_2", "pillar_3", "pillar_4", "pillar_5")
        if pillar_key != disabled_pillar
    ]
    assert result[disabled_pillar] is None
