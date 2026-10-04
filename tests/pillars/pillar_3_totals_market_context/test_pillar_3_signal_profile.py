from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.trajectory_selection import (
    HARDCODED_TARGET_MINUTE_BY_FLOW,
    select_target_minute,
)
from modules.pillars.market_math import ou_edge
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
    monkeypatch.setitem(HARDCODED_TARGET_MINUTE_BY_FLOW, FLOW_ID, None)


def _event_context(event_id: int = EVENT_ID):
    return SimpleNamespace(
        event_id=event_id,
        sport="Football",
        participants_label="Home vs Away",
        minutes_until_start=5,
        season_id=77,
        season_name="2026",
        season_year=2026,
        starts_at=datetime(2026, 8, 27, 18, 0, tzinfo=timezone.utc),
        context_status="normalized",
        competition=SimpleNamespace(competition_id=99, display_name="League"),
    )


def _snapshot_times(target_minute: int) -> tuple[datetime, datetime]:
    start_at = datetime(2026, 8, 27, 18, 0, tzinfo=timezone.utc)
    observed_at = start_at - timedelta(minutes=target_minute)
    return observed_at, observed_at - timedelta(seconds=15)


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
    observed_at, provider_at = _snapshot_times(target_minute)
    for index, (choice_name, price) in enumerate((("over", over), ("under", under)), 1):
        if price is None:
            continue
        rows.append(
            {
                "event_id": EVENT_ID,
                "market_id": market_id,
                "market_group": "Over/Under",
                "market_period": period,
                "market_name": market_name,
                "line_value": line,
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
                "collected_at": observed_at,
                "source_collected_at": provider_at,
                "observed_minutes_before_start": target_minute,
                "trajectory_minutes_before_start": (
                    Decimal(target_minute) + Decimal("0.250000")
                ),
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
    observed_at, provider_at = _snapshot_times(target_minute)
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
                "market_name": (
                    "Over/Under 1st Half" if first_half else "Over/Under Full Time"
                ),
                "line_value": line,
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
                "collected_at": observed_at,
                "source_collected_at": provider_at,
                "observed_minutes_before_start": target_minute,
                "trajectory_minutes_before_start": (
                    Decimal(target_minute) + Decimal("0.250000")
                ),
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


def _complete_rows(
    *,
    target_minute: int = 5,
    include_betfair_ou: bool = False,
    include_betfair_1h_ou: bool = False,
    **kwargs,
) -> list[dict]:
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
            + _exchange_rows(
                exchange_side="lay", target_minute=target_minute, market_id=4010
            )
        )
    if include_betfair_1h_ou:
        rows.extend(
            _exchange_rows(
                exchange_side="back",
                target_minute=target_minute,
                first_half=True,
                market_id=5000,
            )
            + _exchange_rows(
                exchange_side="lay",
                target_minute=target_minute,
                first_half=True,
                market_id=5010,
            )
        )
    return rows


def _calculate(rows: list[dict], *, debug_mode: bool = False) -> dict:
    context = build_odds_trajectory_context(
        rows,
        target_minutes_expected=TARGET_MINUTES,
    )
    selection = select_target_minute(
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


@pytest.mark.parametrize(
    ("first_half_prices", "expected_relation"),
    [
        ((1.8, 2.2), "CONVERGENCE_OVER"),
        ((2.2, 1.8), "DIVERGENCE"),
    ],
)
def test_ft_first_half_relation_and_edge_gap(
    first_half_prices, expected_relation
) -> None:
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
