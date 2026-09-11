"""Behavioral boundaries for adaptive historical changes and kickoff closing."""

from datetime import datetime, timedelta, timezone

import pytest

from modules.oddspapi.historical_odds_change_detector import (
    OddspapiHistoricalOddsChangeDetector as Detector,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.settings import (
    OddspapiPreStartSettings,
)
from shared.timezone_utils import convert_utc_to_local

KICKOFF = datetime(2026, 9, 3, 18, tzinfo=timezone.utc)


def tick(minutes, price, **extra):
    timestamp = KICKOFF - timedelta(minutes=minutes)
    return timestamp, {"createdAt": timestamp.isoformat(), "price": price, **extra}


def detect(ticks, **kwargs):
    return Detector.detect_significant_changes(
        ticks,
        kickoff_utc=KICKOFF,
        bookmaker_slug="bet365",
        source_market_id="131",
        source_outcome_id="131",
        player_id="0",
        to_local=convert_utc_to_local,
        **kwargs,
    )


def test_sanitize_borrows_valid_ticks_and_enforces_cutoff():
    valid = tick(1440, 2, active=True)
    unspecified = tick(30, "2.2")
    at_start = tick(0, 2.4, active=None)
    ticks = [valid, tick(1300, 5, active=False)]
    ticks += [tick(1200, value) for value in (1, 1.01, 0, -1, None, "bad", "nan", "inf", "-inf")]
    ticks += [unspecified, at_start, tick(-1, 9)]
    cleaned = Detector.sanitize_ticks(ticks, kickoff_utc=KICKOFF)
    assert cleaned == [valid, unspecified, at_start]
    assert cleaned[0] is valid


@pytest.mark.parametrize("minutes,sufficient", [(1439.999, False), (1440, True), (1441, True)])
def test_history_boundary(minutes, sufficient):
    result = detect([tick(minutes, 2)])
    assert (result is not None) is sufficient


def test_empty_and_stable_series_have_no_changes():
    assert detect([]) == []
    assert detect([tick(1440, 2), tick(0, 2.1)]) == []


@pytest.mark.parametrize("price", [2.4, 1.6])
def test_exact_twenty_percent_in_both_directions(price):
    assert [q.price for q in detect([tick(1440, 2), tick(30, price)])] == [price]


def test_anchor_updates_and_preserves_timestamp_and_fractional_minutes():
    ticks = [tick(1440, 2), tick(60.123456, 2.4), tick(50, 2.8), tick(40, 2.88), tick(10, 2)]
    quotes = detect(ticks)
    assert [q.price for q in quotes] == [2.4, 2.88, 2]
    assert quotes[0].collected_at == convert_utc_to_local(ticks[1][0])
    assert quotes[0].created_at == ticks[1][1]["createdAt"]
    assert quotes[0].minutes_until_start == pytest.approx(60.123456)


def test_no_rounding_before_threshold_comparison():
    assert detect([tick(1440, 2), tick(30, 2.3999)]) == []


def test_flash_reversal_discards_episode_then_detects_later_change():
    ticks = [tick(1440, 2), tick(60, 2.5), tick(59.5, 2.6), tick(59.25, 2.1), tick(30, 2.5)]
    assert [q.minutes_until_start for q in detect(ticks)] == [30]


def test_reversal_exactly_at_deadline_does_not_discard_change():
    quotes = detect([tick(1440, 2), tick(60, 2.5), tick(57, 2)])
    assert [q.price for q in quotes] == [2.5, 2]


def test_dense_volatile_new_zone_confirms_original_tick():
    ticks = [tick(1440, 2), tick(60, 2.5)]
    ticks += [tick(60 - i / 10, 2.5 + (i % 3) / 100) for i in range(1, 51)]
    quotes = detect(ticks)
    assert [(q.minutes_until_start, q.price) for q in quotes] == [(60, 2.5)]


@pytest.mark.parametrize(
    "tail,expected",
    [
        ([(2, 2.5)], [(2, 2.5)]),
        ([(2, 2.5), (1, 2.1)], []),
        ([(2, 2.5), (1, 3)], [(1, 3)]),
        ([(0, 2.4)], [(0, 2.4)]),
        ([(3, 2.5), (2, 2.1)], []),
        ([(3, 2.5)], [(3, 2.5)]),
    ],
)
def test_closing_uses_only_latest_and_requires_threshold(tail, expected):
    quotes = detect([tick(1440, 2)] + [tick(*entry) for entry in tail])
    assert [(q.minutes_until_start, q.price) for q in quotes] == expected


def test_closing_uses_updated_anchor_and_does_not_repeat_confirmed_tick():
    quotes = detect([tick(1440, 2), tick(10, 2.5), tick(1, 2.7)])
    assert [(q.minutes_until_start, q.price) for q in quotes] == [(10, 2.5)]
    assert len(detect([tick(1440, 2), tick(3, 2.5)])) == 1


def test_last_inactive_tick_is_excluded_before_closing():
    ticks = Detector.sanitize_ticks(
        [tick(1440, 2), tick(2, 2.5), tick(0, 3, active=False)], kickoff_utc=KICKOFF
    )
    assert [q.price for q in detect(ticks)] == [2.5]


def test_zero_window_confirms_every_significant_tick():
    assert [q.price for q in detect(
        [tick(1440, 2), tick(1, 2.5), tick(0, 2)], flash_reversal_minutes=0
    )] == [2.5, 2]


@pytest.mark.parametrize("field", [
    "significant_change_min_magnitude_pct", "significant_change_min_history_hours",
    "significant_change_flash_reversal_minutes", "significant_change_min_price",
])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), -1])
def test_invalid_settings(field, value):
    with pytest.raises(ValueError):
        OddspapiPreStartSettings(**{field: value})


def test_setting_zero_boundaries():
    OddspapiPreStartSettings(significant_change_min_history_hours=0, significant_change_flash_reversal_minutes=0)
    with pytest.raises(ValueError):
        OddspapiPreStartSettings(significant_change_min_magnitude_pct=0)
    with pytest.raises(ValueError):
        OddspapiPreStartSettings(significant_change_min_price=1)


def test_empty_opening_historical_moments_disable_classic_enrichment():
    settings = OddspapiPreStartSettings(opening_historical_moments=())

    assert settings.resolved_opening_historical_moments() == []
    assert settings.resolved_opening_historical_moments([]) == []


def test_pre_start_settings_enable_checkpoint_aware_current_deduplication() -> None:
    assert OddspapiPreStartSettings().deduplicate_historical_current_snapshots is True


def test_significant_change_forced_moments_select_only_integer_key_moments():
    settings = OddspapiPreStartSettings(significant_change_forced_moments=(5, 0))

    assert settings.is_significant_change_forced(5)
    assert settings.is_significant_change_forced(0.0)
    assert not settings.is_significant_change_forced(5.5)
    assert not settings.is_significant_change_forced(None)


@pytest.mark.parametrize("value", [(-1,), (1.5,), (True,)])
def test_invalid_significant_change_forced_moments(value):
    with pytest.raises(ValueError):
        OddspapiPreStartSettings(significant_change_forced_moments=value)


def test_observation_cutoff_shifts_closing_window_for_pre_start():
    # Kickoff at 18:00, observation cutoff at 17:55 (T-5)
    cutoff = KICKOFF - timedelta(minutes=5)
    # flash_reversal_minutes=3 means closing_boundary is 17:52 (8 min before kickoff)
    # tick at 9m before kickoff (4m before cutoff) is before closing window and confirmed
    # tick at 6m before kickoff (1m before cutoff) falls into closing window (17:54 > 17:52)
    ticks = [
        tick(1440, 2.0),
        tick(9, 2.5),
        tick(6, 2.8),
    ]
    quotes = detect(ticks, observation_cutoff_utc=cutoff, flash_reversal_minutes=3.0)
    # tick(9, 2.5) is before closing window, candidate jump from 2.0 -> 2.5 (>=20%)
    # In closing window (6m before kickoff), 2.8 is the latest tick through cutoff and significant from 2.5 (>=20% of 2.0 or 2.5?)
    # 2.8 vs 2.5 is +12% from 2.5. If anchor is 2.5, 2.8 is not >=20% of 2.5.
    # But if tick 6 is price 3.2, 3.2 vs 2.5 is +28% (significant).
    # Let's test with price 3.2:
    ticks_sig = [
        tick(1440, 2.0),
        tick(9, 2.5),
        tick(6, 3.2),
    ]
    quotes_sig = detect(ticks_sig, observation_cutoff_utc=cutoff, flash_reversal_minutes=3.0)
    assert [(q.minutes_until_start, q.price) for q in quotes_sig] == [(9, 2.5), (6, 3.2)]
