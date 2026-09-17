from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from modules.alerts.matchup_streak_analysis import standings_engine as se
from modules.competition.league_config import (
    get_grouping_method,
    get_league_config,
    get_standings_method,
)

NBA_UNIQUE_TOURNAMENT_ID = 132
SHL_UNIQUE_TOURNAMENT_ID = 261
SAUDI_PRO_LEAGUE_UNIQUE_TOURNAMENT_ID = 955
LIGA_MX_APERTURA_UNIQUE_TOURNAMENT_ID = 11621
LIGA_MX_APERTURA_TOURNAMENT_ID = 28
LIGA_MX_CLAUSURA_UNIQUE_TOURNAMENT_ID = 11620
LIGA_MX_CLAUSURA_TOURNAMENT_ID = 16753


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, _query, _params):
        return _FakeResult(self._rows)


@contextmanager
def _fake_session_ctx(rows):
    yield _FakeSession(rows)


def _make_row(home, away, home_score, away_score, winner, result_subtype="REG"):
    return SimpleNamespace(
        event_id=1,
        starts_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        home_team=home,
        away_team=away,
        home_score=home_score,
        away_score=away_score,
        winner=winner,
        result_subtype=result_subtype,
    )


def _build_rows_for_record(team_name, wins, losses, ties=0):
    rows = []
    for i in range(wins):
        rows.append(_make_row(team_name, f"{team_name} Win Opp {i}", 1, 0, "1", "REG"))
    for i in range(losses):
        rows.append(_make_row(f"{team_name} Loss Opp {i}", team_name, 1, 0, "1", "REG"))
    for i in range(ties):
        rows.append(_make_row(team_name, f"{team_name} Tie Opp {i}", 1, 1, "X", "DRAW"))
    return rows


def _compute(monkeypatch, season_id, rows, sport, source_unique_tournament_id=None):
    monkeypatch.setattr(
        se.db_manager,
        "get_session",
        lambda: _fake_session_ctx(rows),
    )
    simulator = se.HistoricalStandingsCalculator()
    return simulator._calculate_standings_internal(
        season_id=season_id,
        cutoff_timestamp=2_000_000_000.0,
        sport=sport,
        source_unique_tournament_id=source_unique_tournament_id,
    )


def test_football_3_1_0_points(monkeypatch):
    rows = [
        _make_row("Alpha FC", "Beta FC", 2, 0, "1", "REG"),
        _make_row("Alpha FC", "Gamma FC", 1, 1, "X", "DRAW"),
    ]
    standings = _compute(monkeypatch, 77559, rows, "Football")

    assert standings["Alpha FC"]["points"] == 4
    assert standings["Alpha FC"]["wins"] == 1
    assert standings["Alpha FC"]["draws"] == 1
    assert standings["Alpha FC"]["pct"] is None


@pytest.mark.parametrize("season_id", [80229, 84695, 85375])
def test_win_pct_ranks_by_percentage_not_raw_wins(monkeypatch, season_id):
    rows = []
    rows.extend(_build_rows_for_record("Team Eight Four", wins=8, losses=4))
    rows.extend(_build_rows_for_record("Team Seven Three", wins=7, losses=3))

    standings = _compute(monkeypatch, season_id, rows, "Basketball")

    assert standings["Team Seven Three"]["pct"] > standings["Team Eight Four"]["pct"]
    assert standings["Team Seven Three"]["position"] < standings["Team Eight Four"]["position"]
    assert standings["Team Eight Four"]["points"] == 8


def test_nfl_win_pct_half_tie_ranking(monkeypatch):
    rows = []
    rows.extend(_build_rows_for_record("Buffalo Bills", wins=8, losses=7, ties=1))
    rows.extend(_build_rows_for_record("Miami Dolphins", wins=8, losses=8, ties=0))

    standings = _compute(monkeypatch, 75522, rows, "American football")

    assert standings["Buffalo Bills"]["points"] == pytest.approx(8.5)
    assert standings["Buffalo Bills"]["pct"] == pytest.approx(0.53125)
    assert standings["Buffalo Bills"]["position"] < standings["Miami Dolphins"]["position"]
    assert standings["Buffalo Bills"]["ties"] == 1
    assert standings["Buffalo Bills"]["draws"] == 1


def test_nhl_ot_loss_adds_point(monkeypatch):
    rows = [
        _make_row("Boston Bruins", "Buffalo Sabres", 4, 2, "1", "REG"),
        _make_row("Detroit Red Wings", "Boston Bruins", 3, 2, "1", "OT"),
    ]
    standings = _compute(monkeypatch, 78476, rows, "Ice hockey")

    assert standings["Boston Bruins"]["wins"] == 1
    assert standings["Boston Bruins"]["losses"] == 1
    assert standings["Boston Bruins"]["ot_losses"] == 1
    assert standings["Boston Bruins"]["points"] == 3
    assert standings["Boston Bruins"]["games_played"] == 2
    assert standings["Boston Bruins"]["pct"] == pytest.approx(0.75)


def test_shl_hockey_3_2_1_0_and_league_wide(monkeypatch):
    rows = [
        _make_row("SHL Team A", "SHL Team A Opp", 3, 1, "1", "REG"),   # 3 points
        _make_row("SHL Team A OT Opp", "SHL Team A", 4, 3, "1", "OT"),  # +1 OTL
        _make_row("SHL Team B", "SHL Team B Opp", 2, 1, "1", "OT"),     # 2 points
        _make_row("SHL Team B Loss Opp", "SHL Team B", 5, 1, "1", "REG"),
    ]

    standings = _compute(
        monkeypatch,
        75679,
        rows,
        "Ice hockey",
        source_unique_tournament_id=SHL_UNIQUE_TOURNAMENT_ID,
    )

    assert standings["SHL Team A"]["points"] == 4
    assert standings["SHL Team B"]["points"] == 2
    assert standings["SHL Team A"]["position"] < standings["SHL Team B"]["position"]
    assert standings["SHL Team A"]["group"] is None
    assert standings["SHL Team A"]["conference"] is None
    assert get_grouping_method(SHL_UNIQUE_TOURNAMENT_ID) == "league_wide"


def test_grouped_unmapped_team_warned_not_dropped(monkeypatch, caplog):
    rows = [
        _make_row("Mystery Team", "Boston Celtics", 101, 99, "1", "REG"),
    ]

    # Pin the toggle so the test is independent of the local .env value.
    monkeypatch.setattr(se.Config, "MATCHUP_STANDINGS_GROUP_BY_CONFERENCE", True)
    caplog.set_level("WARNING")
    standings = _compute(
        monkeypatch,
        80229,
        rows,
        "Basketball",
        source_unique_tournament_id=NBA_UNIQUE_TOURNAMENT_ID,
    )

    assert "Mystery Team" in standings
    assert standings["Mystery Team"]["group"] == "UNKNOWN"
    assert standings["Mystery Team"]["conference"] == "UNKNOWN"
    assert any(
        "season 80229 team 'Mystery Team'" in message
        for message in caplog.messages
    )


def test_saudi_pro_league_uses_football_h2h_method():
    assert get_standings_method(SAUDI_PRO_LEAGUE_UNIQUE_TOURNAMENT_ID) == "football_3_1_0_h2h"


def test_liga_mx_apertura_and_clausura_are_collected_with_gd_method():
    apertura = get_league_config(
        LIGA_MX_APERTURA_UNIQUE_TOURNAMENT_ID,
        LIGA_MX_APERTURA_TOURNAMENT_ID,
    )
    clausura = get_league_config(
        LIGA_MX_CLAUSURA_UNIQUE_TOURNAMENT_ID,
        LIGA_MX_CLAUSURA_TOURNAMENT_ID,
    )

    assert apertura is not None
    assert clausura is not None
    assert apertura.code == "liga_mx_apertura"
    assert clausura.code == "liga_mx_clausura"
    assert apertura.collected is True
    assert clausura.collected is True
    assert apertura.standings_method == "football_3_1_0"
    assert clausura.standings_method == "football_3_1_0"
    assert apertura.number_of_teams == 18
    assert clausura.number_of_teams == 18
    assert apertura.total_regular_season_games == 17
    assert clausura.total_regular_season_games == 17
    assert apertura.standings_grouping == "single_table"
    assert clausura.standings_grouping == "single_table"
    assert get_standings_method(LIGA_MX_APERTURA_UNIQUE_TOURNAMENT_ID) == "football_3_1_0"
    assert get_standings_method(LIGA_MX_CLAUSURA_UNIQUE_TOURNAMENT_ID) == "football_3_1_0"
