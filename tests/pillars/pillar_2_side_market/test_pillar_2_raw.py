from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from modules.pillars.context import CompetitionContext, EventContext, ParticipantContext
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.trajectory_selection import (
    HARDCODED_TARGET_MINUTE_BY_FLOW,
    select_target_minute,
)
from modules.pillars.market_math import side_edge
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
        HARDCODED_TARGET_MINUTE_BY_FLOW, "pre_start_signal_profile", None
    )


def _event_context() -> EventContext:
    return EventContext(
        event_id=2002,
        custom_id="event-2002",
        sport="Football",
        season_id=2026,
        season_name="2026",
        season_year=2026,
        starts_at=datetime(2026, 8, 22, 18, 0, tzinfo=timezone.utc),
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
    line_value: str | None,
    bookie_id: int,
    prices: dict[str, float],
    exchange_side: str | None = None,
    exchange_sizes: dict[str, float | None] | None = None,
) -> None:
    start_at = datetime(2026, 8, 22, 18, 0, tzinfo=timezone.utc)
    observed_at = start_at - timedelta(minutes=minute)
    provider_at = observed_at - timedelta(seconds=15)
    market_id = next(
        (
            row["market_id"]
            for row in rows
            if (
                row["market_group"],
                row["market_period"],
                row["line_value"],
                row["bookie_id"],
            )
            == (market_group, market_period, line_value, bookie_id)
        ),
        len(rows) + 1,
    )
    for index, (choice_name, odds_value) in enumerate(prices.items(), start=1):
        rows.append(
            {
                "event_id": 2002,
                "market_id": market_id,
                "market_group": market_group,
                "market_period": market_period,
                "market_name": market_name,
                "line_value": line_value,
                "choice_name": choice_name,
                "choice_id": index,
                "bookie_id": bookie_id,
                "bookie_name": {302: "Pinnacle", 3: "bet365", 4: "Betfair"}[bookie_id],
                "source": "oddspapi",
                "exchange_side": exchange_side,
                "exchange_level": 0,
                "odds_value": odds_value,
                "snapshot_id": minute * 1000 + len(rows),
                "collected_at": observed_at.isoformat(),
                "source_collected_at": provider_at.isoformat(),
                "observed_minutes_before_start": minute,
                "trajectory_minutes_before_start": (
                    Decimal(minute) + Decimal("0.250000")
                ),
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
    include_betfair_ah: bool = False,
    betfair_ah_line: str = "-0.5",
    betfair_ah_back: tuple[float, float] = (1.98, 2.00),
    betfair_ah_lay: tuple[float, float] = (2.00, 2.02),
    betfair_ah_back_sizes: tuple[float | None, float | None] = (355, 240),
    betfair_ah_lay_sizes: tuple[float | None, float | None] = (240, 348),
    include_betfair_1h_ah: bool = False,
    betfair_1h_ah_line: str = "-0.25",
    betfair_1h_ah_back: tuple[float, float] = (1.99, 2.01),
    betfair_1h_ah_lay: tuple[float, float] = (2.01, 2.03),
) -> list[dict]:
    rows: list[dict] = []
    markets = [
        ("1X2", "Full Time", "1X2 Full Time", None, 302, pin_ft_1x2),
        ("1X2", "Full Time", "1X2 Full Time", None, 3, b365_ft_1x2),
        (
            "Asian Handicap",
            "Full Time",
            "Asian Handicap Full Time",
            pin_ft_ah_line,
            302,
            pin_ft_ah,
        ),
        (
            "Asian Handicap",
            "Full Time",
            "Asian Handicap Full Time",
            b365_ft_ah_line,
            3,
            b365_ft_ah,
        ),
    ]
    if include_first_half:
        markets.extend(
            [
                ("1X2", "1st Half", "1X2 1st Half", None, 302, pin_1h_1x2),
                ("1X2", "1st Half", "1X2 1st Half", None, 3, b365_1h_1x2),
                (
                    "Asian Handicap",
                    "1st Half",
                    "Asian Handicap 1st Half",
                    pin_1h_ah_line,
                    302,
                    pin_1h_ah,
                ),
                (
                    "Asian Handicap",
                    "1st Half",
                    "Asian Handicap 1st Half",
                    b365_1h_ah_line,
                    3,
                    b365_1h_ah,
                ),
            ]
        )
    for group, period, name, line, bookie_id, prices in markets:
        _add_market(
            rows,
            minute=minute,
            market_group=group,
            market_period=period,
            market_name=name,
            line_value=line,
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
            line_value=None,
            bookie_id=4,
            prices={"1": prices[0], "x": prices[1], "2": prices[2]},
            exchange_side=exchange_side,
            exchange_sizes={"1": sizes[0], "x": sizes[1], "2": sizes[2]},
        )
    if include_betfair_ah:
        for exchange_side, prices, sizes in (
            ("back", betfair_ah_back, betfair_ah_back_sizes),
            ("lay", betfair_ah_lay, betfair_ah_lay_sizes),
        ):
            _add_market(
                rows,
                minute=minute,
                market_group="Asian Handicap",
                market_period="Full Time",
                market_name="Asian Handicap Full Time",
                line_value=betfair_ah_line,
                bookie_id=4,
                prices={"1": prices[0], "2": prices[1]},
                exchange_side=exchange_side,
                exchange_sizes={"1": sizes[0], "2": sizes[1]},
            )
    if include_betfair_1h_ah:
        for exchange_side, prices in (
            ("back", betfair_1h_ah_back),
            ("lay", betfair_1h_ah_lay),
        ):
            _add_market(
                rows,
                minute=minute,
                market_group="Asian Handicap",
                market_period="1st Half",
                market_name="Asian Handicap 1st Half",
                line_value=betfair_1h_ah_line,
                bookie_id=4,
                prices={"1": prices[0], "2": prices[1]},
                exchange_side=exchange_side,
                exchange_sizes={"1": 120, "2": 110},
            )
    return rows


def _calculate(rows: list[dict], *, debug_mode: bool = False) -> dict:
    context = build_odds_trajectory_context(
        rows, target_minutes_expected=TARGET_MINUTES
    )
    target_selection = select_target_minute(
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
    profile = next(iter(result["analysis"].values()))
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


def test_exchange_sizes_do_not_change_signal_profile() -> None:
    small = _calculate(_complete_rows(back_sizes=(1, 2, 3), lay_sizes=(4, 5, 6)))
    large = _calculate(
        _complete_rows(
            back_sizes=(10_000, 20_000, 30_000), lay_sizes=(40_000, 50_000, 60_000)
        )
    )

    assert small["analysis"] == large["analysis"]
    assert small["inputs"] != large["inputs"]


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
    signal = _profile(
        _calculate(_complete_rows(pin_ft_1x2=pin_prices, b365_ft_1x2=b365_prices))
    )["FT"]["1X2"]
    assert signal["BOOK_RELATION"] == expected


def test_neutral_is_a_calculated_direction_not_unavailability() -> None:
    assert (
        direction(side_edge(*map(__import__("decimal").Decimal, ("2", "2"))))
        == "NEUTRAL"
    )
    assert relation("NEUTRAL", "HOME") == "NEUTRAL"


def test_ah_same_line_exposes_comparable_readings() -> None:
    ah = _profile(_calculate(_complete_rows()))["FT"]["AH"]
    assert ah["PIN_EDGE"] is not None
    assert ah["B365_EDGE"] is not None
    assert ah["BOOK_RELATION"] is not None
    assert ah["PRICE_GAP"] is not None
    assert ah["REP_EDGE"] is not None
    assert ah["DIRECTION"] is not None


def test_ah_different_lines_preserves_individuals_but_marks_comparison_unavailable() -> (
    None
):
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
    profile = _profile(
        _calculate(
            _complete_rows(
                pin_ft_ah=(3.0, 2.0),
                b365_ft_ah=(3.2, 2.1),
            )
        )
    )
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
    block = _profile(
        _calculate(_complete_rows(back_prices=back_prices, lay_prices=lay_prices))
    )["BOOK_EXCHANGE"]
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
    block = _profile(
        _calculate(_complete_rows(pin_1h_1x2=pin_1h, b365_1h_1x2=b365_1h))
    )["FT_1H"]
    assert block["FT_1H_1X2_RELATION"] == expected
    assert "FT_1H_AH_RELATION" not in block
    assert "FT_1H_AH_GAP" not in block


def test_partial_first_half_ah_line_preserves_individual_price_edges() -> None:
    rows = _complete_rows()
    for row in rows:
        if (
            row["market_period"] == "1st Half"
            and row["market_group"] == "Asian Handicap"
            and row["bookie_id"] == 302
        ):
            row["line_value"] = None
    result = _calculate(rows)
    asian_handicap = _profile(result)["1H"]["AH"]

    assert result["status"] == "ACTIVE"
    assert asian_handicap["PIN_LINE"] is None
    assert asian_handicap["PIN_EDGE"] is not None
    assert asian_handicap["PIN_DIRECTION"] is not None
    assert asian_handicap["B365_EDGE"] is not None
    assert asian_handicap["LINE_GAP"] is None
    assert asian_handicap["BOOK_RELATION"] is None
    assert asian_handicap["REP_EDGE"] is None
