"""A standings cutoff sees only the results that were final at it, not every match that had kicked off.

Reported case: Chelsea v Tottenham kicks off at 19:15 and Manchester City v Bournemouth, which began at
18:30, was still being played. Its stored final score must not enter the table at 19:15.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from modules.standings import calculator as se
from tests.test_standings_timeline import SEASON_ID, _FakeDb

CUTOFF = datetime(2026, 5, 19, 19, 15, tzinfo=timezone.utc)
COMPETITION_ID = 174


def _row(event_id, home, away, starts_at, finished_at=None, scores=(1, 0), sport="Basketball"):
    home_score, away_score = scores
    return SimpleNamespace(
        event_id=event_id, starts_at=starts_at, home_team=home, away_team=away,
        home_score=home_score, away_score=away_score, sport=sport, finished_at=finished_at,
        winner="1" if home_score > away_score else "2" if home_score < away_score else "X",
        result_subtype="REG",
    )


def _calculator(monkeypatch, rows):
    fake_db = _FakeDb(rows)
    monkeypatch.setattr(se.db_manager, "get_session", fake_db.get_session)
    return se.HistoricalStandingsCalculator()


def _games(standings):
    return {team: row["games_played"] for team, row in standings.items()}


def _standings_at(monkeypatch, rows, cutoff):
    return _calculator(monkeypatch, rows).calculate_standings_bundle_at(
        SEASON_ID, cutoff.timestamp(), "Basketball", competition_id=COMPETITION_ID,
    )["standings"]


@pytest.fixture
def finished_long_ago():
    return _row(1, "Hawks", "Bulls", CUTOFF - timedelta(days=7))


def test_match_in_play_at_the_cutoff_is_not_counted_without_a_recorded_end(monkeypatch, finished_long_ago):
    in_play = _row(2, "Celtics", "Knicks", CUTOFF - timedelta(minutes=45), scores=(2, 4))

    standings = _standings_at(monkeypatch, [finished_long_ago, in_play], CUTOFF)

    assert _games(standings) == {"Hawks": 1, "Bulls": 1}


def test_match_in_play_at_the_cutoff_is_not_counted_when_it_ended_afterwards(monkeypatch, finished_long_ago):
    in_play = _row(2, "Celtics", "Knicks", CUTOFF - timedelta(minutes=45),
                   finished_at=CUTOFF + timedelta(minutes=80), scores=(2, 4))

    assert _games(_standings_at(monkeypatch, [finished_long_ago, in_play], CUTOFF)) == {"Hawks": 1, "Bulls": 1}
    assert _games(_standings_at(monkeypatch, [finished_long_ago, in_play], CUTOFF + timedelta(hours=2))) == {
        "Hawks": 1, "Bulls": 1, "Celtics": 1, "Knicks": 1,
    }


def test_match_that_ended_before_the_cutoff_is_counted_even_if_it_started_minutes_before(monkeypatch, finished_long_ago):
    ended = _row(2, "Celtics", "Knicks", CUTOFF - timedelta(minutes=45),
                 finished_at=CUTOFF - timedelta(minutes=5), scores=(2, 4))

    assert _games(_standings_at(monkeypatch, [finished_long_ago, ended], CUTOFF))["Celtics"] == 1


def test_without_a_recorded_end_the_match_counts_once_its_sport_window_has_passed(monkeypatch, finished_long_ago):
    unknown_end = _row(2, "Celtics", "Knicks", CUTOFF - timedelta(minutes=45), scores=(2, 4))

    # Basketball allows 3.5 hours after kickoff.
    assert "Celtics" not in _standings_at(monkeypatch, [finished_long_ago, unknown_end], CUTOFF + timedelta(hours=2))
    assert "Celtics" in _standings_at(monkeypatch, [finished_long_ago, unknown_end], CUTOFF + timedelta(hours=3))


def test_results_join_in_availability_order_not_kickoff_order(monkeypatch):
    # The earlier kickoff is the one that ends later.
    long_game = _row(1, "Celtics", "Knicks", CUTOFF - timedelta(minutes=60),
                     finished_at=CUTOFF + timedelta(minutes=60), scores=(2, 4))
    short_game = _row(2, "Hawks", "Bulls", CUTOFF - timedelta(minutes=20),
                      finished_at=CUTOFF - timedelta(minutes=5))
    rows = [long_game, short_game]
    cutoffs = [CUTOFF, CUTOFF + timedelta(hours=2)]

    timeline = _calculator(monkeypatch, rows).calculate_standings_timeline(
        SEASON_ID, [cutoff.timestamp() for cutoff in cutoffs], competition_id=COMPETITION_ID,
    )

    assert _games(timeline[cutoffs[0].timestamp()]) == {"Hawks": 1, "Bulls": 1}
    assert _games(timeline[cutoffs[1].timestamp()]) == {"Hawks": 1, "Bulls": 1, "Celtics": 1, "Knicks": 1}
    for cutoff in cutoffs:
        assert timeline[cutoff.timestamp()] == _standings_at(monkeypatch, rows, cutoff)


def test_records_exposed_to_consumers_do_not_leak_the_availability_instant(monkeypatch, finished_long_ago):
    bundle = _calculator(monkeypatch, [finished_long_ago]).calculate_standings_bundle_at(
        SEASON_ID, CUTOFF.timestamp(), "Basketball", competition_id=COMPETITION_ID,
    )

    assert bundle["league_totals_context"]["cutoff_rule"] == "available_at < cutoff_dt"
    assert all("available_at" not in record for record in bundle["match_records"])
