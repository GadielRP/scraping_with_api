"""Tests for the single-pass standings timeline and the DB form service.

Covers the fix for the pre-start bottleneck where every historical form game
triggered a full league standings recompute (one league-wide SQL per game).
The timeline must return exactly the same standings that per-cutoff
calculate_standings_at() calls would, while issuing a single league fetch.
"""

from contextlib import contextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace

from modules.alerts.matchup_streak_analysis import standings_engine as se
from modules.alerts.matchup_streak_analysis.historical_form_service import (
    HistoricalFormService,
)

SEASON_ID = 90210
SEASON_START = datetime(2026, 4, 1, 19, 0, 0)


class _FakeDb:
    """In-memory stand-in for db_manager that mimics the SQL filters used."""

    def __init__(self, league_rows):
        self.league_rows = sorted(league_rows, key=lambda row: row.starts_at)
        self.execute_calls = []

    @contextmanager
    def get_session(self):
        yield _FakeSession(self)

    def rows_for(self, query_text, params):
        if "team_name" in params:
            team_name = params["team_name"]
            rows = [
                row
                for row in self.league_rows
                if team_name in (row.home_team, row.away_team)
            ]
            return sorted(rows, key=lambda row: row.starts_at, reverse=True)

        cutoff_dt = params["cutoff_dt"]
        return [row for row in self.league_rows if row.starts_at < cutoff_dt]


class _FakeSession:
    def __init__(self, db):
        self._db = db

    def execute(self, query, params):
        self._db.execute_calls.append((str(query), dict(params)))
        return _FakeResult(self._db.rows_for(str(query), params))


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


def _make_row(event_id, days_offset, home, away, home_score, away_score, winner):
    return SimpleNamespace(
        event_id=event_id,
        starts_at=SEASON_START + timedelta(days=days_offset),
        home_team=home,
        away_team=away,
        home_score=home_score,
        away_score=away_score,
        winner=winner,
        result_subtype="REG",
    )


def _make_league_rows():
    return [
        _make_row(1, 0, "Hawks", "Bulls", 3, 1, "1"),
        _make_row(2, 1, "Celtics", "Knicks", 2, 4, "2"),
        _make_row(3, 2, "Hawks", "Celtics", 1, 2, "2"),
        _make_row(4, 3, "Knicks", "Bulls", 5, 5, "X"),
        _make_row(5, 4, "Bulls", "Celtics", 2, 1, "1"),
        _make_row(6, 5, "Knicks", "Hawks", 0, 3, "2"),
        _make_row(7, 6, "Celtics", "Hawks", 4, 2, "1"),
        _make_row(8, 7, "Bulls", "Knicks", 1, 2, "2"),
    ]


def test_timeline_matches_per_cutoff_standings_with_single_fetch(monkeypatch):
    league_rows = _make_league_rows()
    fake_db = _FakeDb(league_rows)
    monkeypatch.setattr(se.db_manager, "get_session", fake_db.get_session)

    cutoffs = [row.starts_at.timestamp() for row in league_rows]
    cutoffs.append((SEASON_START + timedelta(days=30)).timestamp())

    # Baseline: the legacy per-cutoff path issues one league fetch per cutoff.
    per_cutoff_calculator = se.HistoricalStandingsCalculator()
    expected = {
        cutoff: per_cutoff_calculator.calculate_standings_at(SEASON_ID, cutoff, "Basketball")
        for cutoff in cutoffs
    }
    assert len(fake_db.execute_calls) == len(cutoffs)

    # Single-pass timeline: same standings, exactly one league fetch.
    fake_db.execute_calls.clear()
    timeline_calculator = se.HistoricalStandingsCalculator()
    timeline = timeline_calculator.calculate_standings_timeline(
        SEASON_ID,
        cutoffs,
        "Basketball",
    )

    assert len(fake_db.execute_calls) == 1
    assert set(timeline.keys()) == set(cutoffs)
    for cutoff in cutoffs:
        assert timeline[cutoff] == expected[cutoff], (
            f"Timeline standings diverged at cutoff {cutoff}"
        )


def test_timeline_empty_cutoffs_skips_db(monkeypatch):
    fake_db = _FakeDb(_make_league_rows())
    monkeypatch.setattr(se.db_manager, "get_session", fake_db.get_session)

    calculator = se.HistoricalStandingsCalculator()
    assert calculator.calculate_standings_timeline(SEASON_ID, [], "Basketball") == {}
    assert fake_db.execute_calls == []


def test_timeline_returns_empty_standings_per_cutoff_on_error(monkeypatch):
    @contextmanager
    def _broken_session():
        raise RuntimeError("db down")
        yield

    monkeypatch.setattr(se.db_manager, "get_session", _broken_session)

    calculator = se.HistoricalStandingsCalculator()
    cutoffs = [SEASON_START.timestamp(), (SEASON_START + timedelta(days=1)).timestamp()]
    timeline = calculator.calculate_standings_timeline(SEASON_ID, cutoffs, "Basketball")

    assert timeline == {cutoff: {} for cutoff in cutoffs}


def test_get_team_form_from_db_uses_constant_league_fetches(monkeypatch):
    league_rows = _make_league_rows()
    fake_db = _FakeDb(league_rows)
    monkeypatch.setattr(se.db_manager, "get_session", fake_db.get_session)

    service = HistoricalFormService(
        standings_calculator_instance=se.HistoricalStandingsCalculator()
    )
    results, win_streak = service.get_team_form_from_db(
        team_name="Hawks",
        season_id=SEASON_ID,
        sport="Basketball",
        send_debug_standings=False,
    )

    # One team-games query plus one league-wide timeline fetch, regardless of
    # how many games the team has played.
    assert len(fake_db.execute_calls) == 2

    hawks_games = [
        row for row in league_rows if "Hawks" in (row.home_team, row.away_team)
    ]
    assert len(results) == len(hawks_games)

    # Most recent first; the Hawks lost their last game (event 7) after
    # winning event 6, so the current streak is 0.
    assert [r["event_id"] for r in results] == [7, 6, 3, 1]
    assert win_streak == 0

    # Rankings must match a per-cutoff standings computation.
    reference = se.HistoricalStandingsCalculator()
    fake_db.execute_calls.clear()
    for result in results:
        cutoff = float(result["startTimestamp"])
        standings = reference.calculate_standings_at(SEASON_ID, cutoff, "Basketball")
        expected_own = (standings.get("Hawks") or {}).get("rank") or 0
        expected_opp = (standings.get(result["opponent_name"]) or {}).get("rank") or 0
        assert result["own_ranking"] == expected_own
        assert result["opponent_ranking"] == expected_opp
        assert result["team_standing"]["standings_method"] == "win_pct"


def test_get_team_form_from_db_filters_exclude_and_future_events(monkeypatch):
    league_rows = _make_league_rows()
    fake_db = _FakeDb(league_rows)
    monkeypatch.setattr(se.db_manager, "get_session", fake_db.get_session)

    service = HistoricalFormService(
        standings_calculator_instance=se.HistoricalStandingsCalculator()
    )
    cutoff_ts = (SEASON_START + timedelta(days=6)).timestamp()  # excludes event 7
    results, _ = service.get_team_form_from_db(
        team_name="Hawks",
        season_id=SEASON_ID,
        sport="Basketball",
        exclude_event_id=3,
        current_event_timestamp=cutoff_ts,
        send_debug_standings=False,
    )

    assert [r["event_id"] for r in results] == [6, 1]
