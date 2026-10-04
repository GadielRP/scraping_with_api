from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from modules.pillars.pillar_5.calculation_models import (
    MemoryQueryKey,
    MemorySample,
    PopulationFilters,
)
from modules.pillars.pillar_5.memory_sample import (
    build_memory_sample,
    canonical_price,
)
from modules.pillars.pillar_5.memory_score import calculate_memory_profile

KICKOFF = datetime(2026, 9, 21, 18, 0, tzinfo=timezone.utc)


def _key(*, shape: str = "THREE_WAY") -> MemoryQueryKey:
    return MemoryQueryKey(
        sport="Football",
        bookie_id=302,
        market_group="1X2" if shape == "THREE_WAY" else "Home/Away",
        market_period="Full Time",
        market_shape=shape,
        odds_home=Decimal("1.900"),
        odds_draw=Decimal("3.700") if shape == "THREE_WAY" else None,
        odds_away=Decimal("3.900"),
    )


def _sample(key: MemoryQueryKey, winners: list[str]) -> MemorySample:
    return MemorySample(
        key=key,
        sample_size=len(winners),
        wins_home=winners.count("1"),
        wins_draw=winners.count("X"),
        wins_away=winners.count("2"),
    )


def test_canonical_1x2_example_scores_point_two() -> None:
    profile = calculate_memory_profile(
        bookmaker="pinnacle",
        target_minute=5,
        sample=_sample(_key(), ["1", "1", "1", "2"]),
    )

    payload = profile.to_dict()
    assert payload["P5_STATUS"] == "ACTIVE"
    assert payload["P5_VALID"] is True
    assert payload["P5_DIRECTION"] == "HOME"
    assert payload["P5"] == pytest.approx(0.20)
    assert payload["P5_STRENGTH"] == "WEAK"
    assert payload["HIST_EDGE"] == pytest.approx(0.625)
    assert payload["CONSISTENCY"] == pytest.approx(0.8125)
    assert payload["MSRI_RAW"] == pytest.approx(0.25390625)
    assert payload["MSRI_SIGNAL"] == pytest.approx(0.50)
    assert payload["SAMPLE_WEIGHT"] == pytest.approx(0.40)


def test_two_way_uses_half_baseline_and_never_counts_draw() -> None:
    profile = calculate_memory_profile(
        bookmaker="pinnacle",
        target_minute=5,
        sample=_sample(_key(shape="TWO_WAY"), ["1", "1", "1", "2"]),
    ).to_dict()

    assert profile["market_shape"] == "TWO_WAY"
    assert profile["wins_draw"] == 0
    assert profile["BASELINE"] == pytest.approx(0.5)
    assert profile["P5"] == pytest.approx(0.1)
    assert profile["P5_DIRECTION"] == "HOME"


def test_sufficient_tie_is_valid_without_fabricated_intermediates() -> None:
    profile = calculate_memory_profile(
        bookmaker="sofascore",
        target_minute=5,
        sample=_sample(_key(), ["1", "X", "2"]),
    ).to_dict()

    assert profile["P5_STATUS"] == "ACTIVE"
    assert profile["P5_VALID"] is True
    assert profile["memory_status"] == "TIE"
    assert profile["is_tie"] is True
    assert profile["P5"] == 0
    assert profile["P5_DIRECTION"] == "NONE"
    assert profile["P_hist_DOMINANT"] is None
    assert profile["HIST_EDGE"] is None
    assert profile["MSRI_RAW"] is None


def test_insufficient_sample_preserves_real_counts() -> None:
    profile = calculate_memory_profile(
        bookmaker="bet365",
        target_minute=5,
        sample=_sample(_key(), ["1", "2"]),
    ).to_dict()

    assert profile["P5_STATUS"] == "INSUFFICIENT_DATA"
    assert profile["P5_VALID"] is False
    assert profile["sample_size"] == 2
    assert profile["wins_home"] == 1
    assert profile["wins_away"] == 1
    assert profile["BASELINE"] is None
    assert profile["P5"] is None


def test_price_canonization_uses_round_half_up() -> None:
    assert canonical_price("1.2345") == Decimal("1.235")
    with pytest.raises(ValueError):
        canonical_price("1.000")
