"""Tests for the single-pass standings timeline and the DB form service.

Covers the fix for the pre-start bottleneck where every historical form game
triggered a full league standings recompute (one league-wide SQL per game).
The timeline must return exactly the same standings that per-cutoff
calculate_standings_at() calls would, while issuing a single league fetch.
"""

import importlib
import logging

import pytest

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from modules.alerts.matchup_streak_analysis import standings_engine as se
from modules.alerts.matchup_streak_analysis.historical_form_service import (
    HistoricalFormService,
)
from modules.pillars.pillar_1_team_structure.module_7.opponent_expectation_engine import (
    _extract_opponent_rank,
    calculate_m7_opponent_expectation_engine,
)

SEASON_ID = 90210
SEASON_START = datetime(2026, 4, 1, 19, 0, 0, tzinfo=timezone.utc)


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

    # Independent cutoff requests each issue one league fetch.
    per_cutoff_calculator = se.HistoricalStandingsCalculator()
    expected = {
        cutoff: per_cutoff_calculator.calculate_standings_at(SEASON_ID, cutoff, "Basketball", competition_id=174)
        for cutoff in cutoffs
    }
    assert len(fake_db.execute_calls) == len(cutoffs)

    # Single-pass timeline: same standings, exactly one league fetch.
    fake_db.execute_calls.clear()
    timeline_calculator = se.HistoricalStandingsCalculator()
    timeline = timeline_calculator.calculate_standings_timeline(
        SEASON_ID,
        cutoffs,
        "Basketball", competition_id=174,
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
    assert calculator.calculate_standings_timeline(SEASON_ID, [], "Basketball", competition_id=174) == {}
    assert fake_db.execute_calls == []


def test_timeline_returns_empty_standings_per_cutoff_on_error(monkeypatch):
    @contextmanager
    def _broken_session():
        raise RuntimeError("db down")
        yield

    monkeypatch.setattr(se.db_manager, "get_session", _broken_session)

    calculator = se.HistoricalStandingsCalculator()
    cutoffs = [SEASON_START.timestamp(), (SEASON_START + timedelta(days=1)).timestamp()]
    timeline = calculator.calculate_standings_timeline(SEASON_ID, cutoffs, "Basketball", competition_id=174)

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
        competition_id=174,
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
        standings = reference.calculate_standings_at(SEASON_ID, cutoff, "Basketball", competition_id=174)
        expected_own = (standings.get("Hawks") or {}).get("rank")
        expected_opp = (standings.get(result["opponent_name"]) or {}).get("rank")
        assert result["own_ranking"] == expected_own
        assert result["opponent_ranking"] == expected_opp
        assert result["team_standing"]["standings_method"] == "CBA"
        assert result["standings_as_of_timestamp"] == cutoff


def test_unresolved_historical_rank_stays_missing_and_aborts_m7(caplog):
    row = _make_row(21139, 9, "Villarreal", "Valencia", 2, 0, "1")
    cutoff = row.starts_at.timestamp()
    result = HistoricalFormService._build_form_result(
        row,
        "Villarreal",
        {cutoff: {"Valencia": {
            "rank": None,
            "ranking_status": "UNRESOLVED",
            "ranking_reason": "MISSING_DISCIPLINARY_RECORD",
        }}},
        "LALIGA",
    )
    assert result["opponent_ranking"] is None
    assert result["opponent_standing"]["ranking_reason"] == "MISSING_DISCIPLINARY_RECORD"

    result["opponent_current_rank"] = 12
    result["opponent_final_real_ranking"] = 12
    earlier = dict(result, event_id=1, startTimestamp=cutoff - 86400)
    analysis = SimpleNamespace(
        home_team_results=[result, earlier], away_team_results=[],
        current_standings={"Valencia": {"rank": 12}},
    )
    event = SimpleNamespace(
        event_id=21080, participants_label="Villarreal vs Sevilla",
        home_team_name="Villarreal", away_team_name="Sevilla",
    )
    competition = SimpleNamespace(number_of_teams=20)
    with caplog.at_level(logging.ERROR), pytest.raises(ValueError, match="M7 aborted: missing historical opponent rank"):
        calculate_m7_opponent_expectation_engine(analysis, event, competition)
    assert "event_id=21139" in caplog.text
    assert "MISSING_DISCIPLINARY_RECORD" in caplog.text


def test_m7_requires_a_rank_from_the_same_game_cutoff():
    game = {
        "startTimestamp": 200.0,
        "standings_as_of_timestamp": 200.0,
        "opponent_standing": {"rank": 14, "ranking_status": "RESOLVED"},
        "opponent_ranking": 12,
        "opponent_current_rank": 12,
    }
    assert _extract_opponent_rank(game, max_rank=20) == (14, "game.opponent_standing.rank")
    game["standings_as_of_timestamp"] = 300.0
    assert _extract_opponent_rank(game, max_rank=20) == (None, None)


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
        competition_id=174,
        exclude_event_id=3,
        current_event_timestamp=cutoff_ts,
        send_debug_standings=False,
    )

    assert [r["event_id"] for r in results] == [6, 1]


@pytest.mark.parametrize("debug_mode", [False, True])
def test_matchup_orchestrator_passes_competition_and_debug_to_standings(monkeypatch, caplog, debug_mode):
    runner = importlib.import_module("modules.alerts.matchup_streak_analysis.run_matchup_streak_analysis")
    fake_db = _FakeDb(_make_league_rows())
    monkeypatch.setattr(se.db_manager, "get_session", fake_db.get_session)
    monkeypatch.setattr(runner, "standings_calculator", se.HistoricalStandingsCalculator())
    with caplog.at_level(logging.INFO, logger=se.__name__):
        for _ in range(2):  # Both a fresh calculation and a cache hit respect debug_mode.
            result = runner.build_matchup_streak_context(
                event_id=99, event_custom_id="test", event_start_time=SEASON_START + timedelta(days=9),
                sport="Basketball", discovery_source="test", source_unique_tournament_id=1562,
                source_tournament_id=None, competition_name="CBA", competition_slug="cba",
                season_id=SEASON_ID, season_name="2026", season_year=2026,
                participants="Hawks vs Bulls", home_team_name="Hawks", away_team_name="Bulls",
                matchup_events=[], minutes_until_start=5,
                competition_context=SimpleNamespace(competition_id=174), debug_mode=debug_mode,
            )
            assert result is not None
            assert result.current_standings["Hawks"]["games_played"] == 4
    standings_logs = [r for r in caplog.records if r.name == se.__name__ and "STANDINGS:" in r.message]
    assert len(standings_logs) == (2 if debug_mode else 0)
    assert len(fake_db.execute_calls) == 1
    assert fake_db.execute_calls[0][1]["competition_id"] == 174
    assert fake_db.execute_calls[0][1]["season_year"] == 2026
