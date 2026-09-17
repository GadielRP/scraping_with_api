"""Unit tests for reconstructing OddsPapi historical prices at key moments."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from modules.oddspapi.historical_odds_as_of import OddspapiHistoricalOddsAsOf
from modules.oddspapi.historical_odds_normalizer import (
    OddspapiHistoricalOddsNormalizer,
)
from modules.oddspapi.historical_odds_reader import OddspapiHistoricalOddsReader


_FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "odds_papi"
    / "odds_data"
    / "historical_odds_id1100048668096560_betfair-ex_outcome11199.json"
)


def _payload():
    return json.loads(_FIXTURE.read_text(encoding="utf-8"))


def _read(payload, targets, *, require_active_quotes=True, source_sport_id="13"):
    return OddspapiHistoricalOddsReader.read(
        payload,
        source_sport_id=source_sport_id,
        as_of_targets=targets,
        require_active_quotes=require_active_quotes,
    )


def _targets_30_and_5():
    return [
        (
            30,
            datetime(2026, 7, 28, 18, 34, 20, tzinfo=timezone.utc),
            datetime(2026, 7, 28, 18, 34, 20, tzinfo=timezone.utc),
        ),
        (
            5,
            datetime(2026, 7, 28, 19, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 28, 19, 10, 0, tzinfo=timezone.utc),
        ),
    ]


def test_reader_picks_last_tick_at_or_before_target():
    payload = _payload()
    # 18:34:16.181Z is the last active tick at or before 18:34:20Z (1.43).
    # 19:05:53.683Z is the last active tick at or before 19:10:00Z (1.40).
    result = _read(payload, _targets_30_and_5())

    by_moment = {quote.minutes_until_start: quote for quote in result.as_of_quotes}
    assert by_moment[30].price == 1.43
    assert by_moment[30].source_outcome_id == "11199"
    assert by_moment[5].price == 1.4
    assert by_moment[5].created_at.startswith("2026-07-28T19:05:53")


def test_reader_skips_moments_before_first_active_tick():
    payload = _payload()
    targets = [
        (
            120,
            datetime(2026, 7, 27, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 27, 12, 0, 0, tzinfo=timezone.utc),
        )
    ]

    assert _read(payload, targets).as_of_quotes == ()


def test_require_active_quotes_false_uses_inactive_history():
    payload = _payload()
    targets = [
        (
            120,
            datetime(2026, 7, 24, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 24, 12, 0, 0, tzinfo=timezone.utc),
        )
    ]

    quotes = _read(payload, targets, require_active_quotes=False).as_of_quotes

    assert len(quotes) == 1
    assert quotes[0].price == 1.0
    assert quotes[0].active is False


def test_attach_to_normalized_payload_writes_moment_quotes():
    payload = _payload()
    targets = [
        (
            1,
            datetime(2026, 7, 28, 20, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 28, 20, 0, 0, tzinfo=timezone.utc),
        )
    ]
    result = _read(payload, targets)
    attached = OddspapiHistoricalOddsAsOf.attach_to_normalized_payload(
        result.normalized_payload,
        result.as_of_quotes,
    )
    player = attached["bookmakerOdds"]["betfair-ex"]["markets"]["11198"]["outcomes"]["11199"]["players"]["0"]
    assert player["momentQuotes"][0]["minutesUntilStart"] == 1
    assert player["momentQuotes"][0]["price"] == 1.4


def test_reader_one_pass_returns_normalized_and_as_of_from_same_ticks():
    payload = _payload()
    result = _read(payload, _targets_30_and_5())

    player = result.normalized_payload["bookmakerOdds"]["betfair-ex"]["markets"]["11198"][
        "outcomes"
    ]["11199"]["players"]["0"]
    assert "price" in player
    assert "momentQuotes" not in player
    by_moment = {quote.minutes_until_start: quote.price for quote in result.as_of_quotes}
    assert by_moment[30] == 1.43
    assert by_moment[5] == 1.4


def test_targets_from_start_includes_moment_zero():
    start = datetime(2026, 8, 24, 17, 0, 0, tzinfo=timezone.utc)
    targets = OddspapiHistoricalOddsAsOf.targets_from_start(
        start,
        [120, 30, 5, 1, 0, -5],
    )
    moments = [t[0] for t in targets]
    assert moments == [120, 30, 5, 1, 0]
    # For moment 0, target time is exactly event start
    moment_0 = next(t for t in targets if t[0] == 0)
    assert moment_0[2] == start


def test_reader_reconstructs_moment_zero():
    payload = _payload()
    # Match kickoff target at 2026-07-28 20:01:00 UTC (14:01 local)
    targets = [
        (
            0,
            datetime(2026, 7, 28, 20, 1, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 28, 20, 1, 0, tzinfo=timezone.utc),
        )
    ]
    result = _read(payload, targets)
    by_moment = {quote.minutes_until_start: quote for quote in result.as_of_quotes}
    assert 0 in by_moment
    assert by_moment[0].price == 1.4
    assert by_moment[0].collected_at == datetime(
        2026, 7, 28, 20, 1, 0, tzinfo=timezone.utc
    )


def test_reader_empty_targets_matches_normalize_and_skips_as_of():
    payload = _payload()
    with_targets = _read(payload, _targets_30_and_5())
    without_targets = _read(payload, ())

    assert without_targets.as_of_quotes == ()
    assert with_targets.normalized_payload == without_targets.normalized_payload
    assert without_targets.normalized_payload == OddspapiHistoricalOddsNormalizer.normalize(
        payload,
        source_sport_id="13",
    )


