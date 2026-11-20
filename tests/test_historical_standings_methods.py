from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from modules.standings import calculator as se
from modules.alerts.matchup_streak_analysis import historical_form

from tests.competition_fixtures import profile as get_competition_profile, IDENTITIES

NBA_UNIQUE_TOURNAMENT_ID = 132
SHL_UNIQUE_TOURNAMENT_ID = 261


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


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


def _compute(monkeypatch, season_id, rows, sport, source_unique_tournament_id=None, competition_id=None):
    monkeypatch.setattr(
        se.db_manager,
        "get_session",
        lambda: _fake_session_ctx(rows),
    )
    competition_id = {SHL_UNIQUE_TOURNAMENT_ID: 180, NBA_UNIQUE_TOURNAMENT_ID: 176}.get(
        source_unique_tournament_id, competition_id or {"Football": 168, "Basketball": 174, "American football": 5153, "Ice hockey": 5196}[sport],
    )
    monkeypatch.setattr(se, "get_competition_profile_for_local_id", get_competition_profile)
    simulator = se.HistoricalStandingsCalculator()
    return simulator._calculate_standings_bundle_internal(
        season_id=season_id,
        cutoff_timestamp=2_000_000_000.0,
        sport=sport,
        source_unique_tournament_id=source_unique_tournament_id,
        competition_id=competition_id,
    )["standings"]


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

    # WNBA ranks by win percentage; CBA now ranks by classification points.
    standings = _compute(monkeypatch, season_id, rows, "Basketball", competition_id=145)

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
    assert get_competition_profile(180).grouping_method == "league_wide"


def test_grouped_unmapped_team_warned_not_dropped(monkeypatch, caplog):
    rows = [
        _make_row("Mystery Team", "Boston Celtics", 101, 99, "1", "REG"),
    ]

    # Pin the toggle so the test is independent of the local .env value.
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


def test_configured_profiles_use_database_season_history(monkeypatch):
    competition_id, method = 76, "MLS"
    expected_history = ([{"event_id": 1}], 1)
    database_history = Mock(return_value=expected_history)
    provider_history = Mock(side_effect=AssertionError("Configured profiles must use database history"))
    monkeypatch.setattr(historical_form, "get_competition_profile", get_competition_profile)
    monkeypatch.setattr(historical_form.historical_form_service, "get_team_form_from_db", database_history)
    monkeypatch.setattr(historical_form.api_client, "get_team_last_results_response", provider_history)
    identity = IDENTITIES[competition_id]
    cutoff = datetime(2026, 11, 2, tzinfo=timezone.utc).timestamp()

    result = historical_form.get_team_last_results_by_id(
        team_id=1, team_name="Club", competition_slug=method.lower(), sport="Football",
        season_id=86668, source_unique_tournament_id=identity.source_unique_tournament_id,
        source_tournament_id=identity.source_tournament_id, competition_id=competition_id,
        season_year=2026, exclude_event_id=455374, event_start_timestamp=cutoff,
    )

    assert result is expected_history
    database_history.assert_called_once_with(
        team_name="Club", season_id=86668, sport="Football",
        source_unique_tournament_id=identity.source_unique_tournament_id,
        source_tournament_id=identity.source_tournament_id, competition_id=competition_id,
        season_year=2026, exclude_event_id=455374, current_event_timestamp=cutoff,
        send_debug_standings=False,
    )
    provider_history.assert_not_called()


def _euroleague_row(home, away, home_score, away_score, home_sets, away_sets):
    row = _make_row(home, away, home_score, away_score, "1" if home_score > away_score else "2", "REG")
    row.home_sets, row.away_sets = home_sets, away_sets
    return row


def test_euroleague_compares_regulation_quarters_even_when_the_view_reports_regular_time(monkeypatch):
    # The source view cannot flag basketball overtime: both games arrive as REG. Game 1 went to overtime
    # (86-86 after four quarters), game 2 was settled in regulation. Final scores would level the
    # head-to-head differential (+2 for A, +2 for B); the regulation scores give B the edge.
    rows = [
        _euroleague_row("Team A", "Team B", 90, 88, "20-22-22-22-(4)", "22-20-22-22-(2)"),
        _euroleague_row("Team B", "Team A", 82, 80, "20-20-21-21", "20-20-20-20"),
    ]
    standings = _compute(monkeypatch=monkeypatch, season_id=1, rows=rows, sport="Basketball", competition_id=429)
    assert standings["Team B"]["position"] == 1
    assert standings["Team A"]["position"] == 2


def test_euroleague_without_period_scores_cannot_use_the_score_differential_but_wins_still_count(monkeypatch):
    rows = [
        _euroleague_row("Team A", "Team B", 90, 88, None, None),
        _euroleague_row("Team B", "Team A", 80, 85, None, None),
    ]
    standings = _compute(monkeypatch=monkeypatch, season_id=1, rows=rows, sport="Basketball", competition_id=429)
    # A wins both meetings: the head-to-head wins decide before any score differential is needed.
    assert (standings["Team A"]["position"], standings["Team B"]["position"]) == (1, 2)
    assert standings["Team A"]["ranking_status"] == "RESOLVED"


def test_euroleague_missing_periods_report_missing_regulation_scores_when_the_differential_is_reached(monkeypatch):
    rows = [
        _euroleague_row("Team A", "Team B", 90, 88, None, None),
        _euroleague_row("Team B", "Team A", 85, 80, None, None),
    ]
    standings = _compute(monkeypatch=monkeypatch, season_id=1, rows=rows, sport="Basketball", competition_id=429)
    assert standings["Team A"]["position"] is None
    assert standings["Team A"]["ranking_reason"] == "MISSING_REGULATION_SCORES"


@pytest.mark.parametrize(("home_sets", "away_sets", "expected"), [
    ("23-23-31-24", "19-35-24-31", (101, 109)),
    ("23-23-31-24-(16)", "19-35-24-31-(10)", (101, 109)),
    ("23-23-31-24", "19-35-24", (None, None)),            # three quarters only
    ("23-23-31-24", None, (None, None)),                  # periods absent
    ("23-23-31-25", "19-35-24-31", (None, None)),         # quarters do not add up to the final score
])
def test_regulation_scores_require_four_quarters_that_reproduce_the_final_score(home_sets, away_sets, expected):
    home_final = 101 + (16 if "(16)" in (home_sets or "") else 0)
    away_final = 109 + (10 if "(10)" in (away_sets or "") else 0)
    assert se._regulation_scores(home_sets, away_sets, home_final, away_final) == expected
