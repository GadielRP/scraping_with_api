"""The reader shares strict ticks across opening, current and reconstructed odds."""

from datetime import timedelta
from unittest.mock import patch

from modules.oddspapi.historical_odds_as_of import OddspapiHistoricalOddsAsOf
from modules.oddspapi.historical_odds_reader import OddspapiHistoricalOddsReader
from tests.test_historical_odds_change_detector import KICKOFF, tick
from shared.timezone_utils import convert_utc_to_local


def payload(ticks):
    return {"fixtureId": "test", "bookmakers": {"bet365": {"markets": {
        "131": {"outcomes": {"131": {"players": {"0": [q for _, q in ticks]}}}}
    }}}}


def read(ticks, **kwargs):
    options = dict(
        source_sport_id="13",
        as_of_targets=OddspapiHistoricalOddsAsOf.targets_from_start(KICKOFF, [120, 30, 5, 1, 0]),
        require_active_quotes=False,
        enable_significant_changes=True,
        kickoff_utc=KICKOFF,
    )
    options.update(kwargs)
    return OddspapiHistoricalOddsReader.read(payload(ticks), **options)


def player(result):
    return result.normalized_payload["bookmakerOdds"]["bet365"]["markets"]["131"]["outcomes"]["131"]["players"]["0"]


def test_insufficient_history_falls_back_using_strict_ticks_everywhere():
    ticks = [tick(2000, 1), tick(200, 2), tick(60, 8, active=False), tick(0, 1.01), tick(-1, 9)]
    result = read(ticks)
    assert [q.minutes_until_start for q in result.as_of_quotes] == [120, 30, 5, 1, 0]
    assert all(q.price == 2 for q in result.as_of_quotes)
    assert all(q.collected_at == convert_utc_to_local(
        KICKOFF - timedelta(minutes=q.minutes_until_start)
    ) for q in result.as_of_quotes)
    assert all(q.created_at == ticks[1][1]["createdAt"] for q in result.as_of_quotes)
    assert player(result)["initialPrice"] == 2
    assert player(result)["price"] == 2
    assert player(result)["changedAt"] == ticks[1][1]["createdAt"]


def test_disabled_mode_keeps_inactive_and_sentinel_legacy_behavior():
    result = read([tick(200, 2), tick(0, 1, active=False)], enable_significant_changes=False)
    assert result.as_of_quotes[-1].price == 1
    assert player(result)["price"] == 1


def test_missing_kickoff_falls_back_to_legacy_and_logs_reason(caplog):
    result = read([tick(200, 2), tick(0, 1, active=False)], kickoff_utc=None)
    assert result.as_of_quotes[-1].price == 1
    assert "missing kickoff_utc" in caplog.text


def test_sufficient_stable_history_falls_back_when_detector_has_no_changes():
    with patch.object(
        OddspapiHistoricalOddsAsOf,
        "from_ordered_ticks",
        wraps=OddspapiHistoricalOddsAsOf.from_ordered_ticks,
    ) as fallback:
        result = read([tick(1440, 2), tick(0, 2.1)])
    fallback.assert_called_once()
    assert [q.minutes_until_start for q in result.as_of_quotes] == [120, 30, 5, 1, 0]
    assert [q.price for q in result.as_of_quotes] == [2, 2, 2, 2, 2.1]
    assert player(result)["price"] == 2.1


def test_all_invalid_ticks_produce_no_player_or_quotes():
    result = read([tick(1440, 1), tick(0, 4, active=False)])
    assert result.as_of_quotes == ()
    assert result.normalized_payload["bookmakerOdds"] == {}


def test_custom_min_price_is_shared_with_opening_and_fallback():
    result = read([tick(1500, 1.4), tick(120, 2), tick(0, 1.45)], min_price=1.5)
    assert player(result)["price"] == 2
    assert player(result)["initialPrice"] == 2
    assert [q.price for q in result.as_of_quotes] == [2] * 5


def test_each_series_chooses_its_own_strategy_and_excludes_invalid_timestamps():
    raw = payload([tick(1440, 2), tick(30, 2.5)])
    players = raw["bookmakers"]["bet365"]["markets"]["131"]["outcomes"]["131"]["players"]
    players["0"].append({"price": 9, "createdAt": "invalid"})
    players["1"] = [tick(200, 2)[1]]
    result = OddspapiHistoricalOddsReader.read(
        raw, source_sport_id="13", enable_significant_changes=True, kickoff_utc=KICKOFF,
        as_of_targets=OddspapiHistoricalOddsAsOf.targets_from_start(KICKOFF, [120, 0]),
    )
    assert [(q.player_id, q.minutes_until_start) for q in result.as_of_quotes] == [("0", 30), ("1", 120), ("1", 0)]


def test_fractional_timestamp_survives_attach_and_no_targets_are_required():
    ticks = [tick(1440, 2), tick(1.123456, 2.5)]
    result = read(ticks, as_of_targets=())
    OddspapiHistoricalOddsAsOf.attach_to_normalized_payload(result.normalized_payload, result.as_of_quotes)
    quote = player(result)["momentQuotes"][0]
    assert quote["minutesUntilStart"] == result.as_of_quotes[0].minutes_until_start
    assert quote["collectedAt"].microsecond == ticks[1][0].microsecond


def test_order_and_timestamp_ties_follow_existing_normalizer():
    same_time = KICKOFF - timedelta(minutes=30)
    result = read([tick(30, 2.5), tick(1440, 2), (same_time, {"price": 2.6, "createdAt": same_time.isoformat()})])
    # Existing ties process the later payload entry first.
    assert result.as_of_quotes[0].price == 2.6
    assert player(result)["price"] == 2.5
