from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from infrastructure.persistence.repositories.pillar_5_price_memory_repository import (
    HistoricalPriceMatch,
)
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


def _match(
    event_id: int,
    winner_side: str,
    *,
    key: MemoryQueryKey | None = None,
    starts_at: datetime | None = None,
) -> HistoricalPriceMatch:
    key = key or _key()
    return HistoricalPriceMatch(
        event_id=event_id,
        sport=key.sport,
        competition_id=11,
        season_id=2025,
        country="Spain",
        bookie_id=key.bookie_id,
        market_group=key.market_group,
        market_period=key.market_period,
        has_draw=key.has_draw,
        starts_at=starts_at or KICKOFF - timedelta(days=event_id + 1),
        odds_home=key.odds_home,
        odds_draw=key.odds_draw,
        odds_away=key.odds_away,
        home_score=1,
        away_score=0,
        winner_side=winner_side,
        last_sync_at=KICKOFF - timedelta(days=event_id + 1, minutes=5),
    )


def _sample(key: MemoryQueryKey, winners: list[str]) -> MemorySample:
    matches = tuple(_match(index + 1, winner, key=key) for index, winner in enumerate(winners))
    return MemorySample(
        key=key,
        historical_matches=matches,
        sample_size=len(matches),
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
    assert profile["P5"] == 0


def test_memory_sample_is_exhaustive_causal_and_deduplicated() -> None:
    key = _key()
    rows = [
        _match(1, "1", key=key),
        _match(1, "1", key=key),
        _match(2, "X", key=key),
        _match(3, "invalid", key=key),
        _match(4, "2", key=key, starts_at=KICKOFF + timedelta(minutes=1)),
    ]

    class Repository:
        def find_exact_matches(self, **kwargs):
            self.kwargs = kwargs
            return rows

    repository = Repository()
    sample = build_memory_sample(
        repository,
        key=key,
        current_event_id=999,
        current_starts_at=KICKOFF,
        population_filters=PopulationFilters(),
    )

    assert repository.kwargs["limit"] is None
    assert repository.kwargs["sport"] == "Football"
    assert sample.sample_size == 2
    assert (sample.wins_home, sample.wins_draw, sample.wins_away) == (1, 1, 0)
    assert {item["reason"] for item in sample.eligibility_diagnostics} == {
        "duplicate_event_excluded",
        "invalid_or_incompatible_result_excluded",
        "non_causal_event_excluded",
    }


def test_price_canonization_uses_round_half_up() -> None:
    assert canonical_price("1.2345") == Decimal("1.235")
    with pytest.raises(ValueError):
        canonical_price("1.000")
