from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest

from modules.competition.competitive_rules import season_key_from_name
from modules.competition.league_config import LEAGUE_CONFIGS, get_league_config
from modules.competition.tracked_competitions import TRACKED_COMPETITION_IDS
from modules.pillars.pillar_1_team_structure.competition_context_resolver import (
    resolve_p1_competition_context,
)
from tests.pillars.pillar_1_team_structure.test_module_5_late_season import (
    _calculate_m5, _event,
)


PROFILES = {config.code: config for config in LEAGUE_CONFIGS if config.competitive_rules}


def _configured_event(code, season=None):
    config = PROFILES[code]
    rules = config.competitive_rules[0]
    event = replace(_event(), season_name=season or rules.season_key)
    event.competition = replace(
        event.competition, source="sofascore",
        source_unique_tournament_id=config.primary_identity.source_unique_tournament_id,
        source_tournament_id=config.primary_identity.source_tournament_id,
        competitive_rules=None,
    )
    event.competition = resolve_p1_competition_context(event.to_identity(), event.competition)
    return event


def _table(size, played, home_rank, away_rank):
    names = {home_rank: "Valencia", away_rank: "Real Sociedad"}
    return SimpleNamespace(current_standings={
        names.get(rank, f"Team {rank}"): {
            "rank": rank, "points": 100 - 2 * rank, "games_played": played,
        }
        for rank in range(1, size + 1)
    })


def test_twenty_profiles_and_operational_allowlist_remain_independent():
    assert len(PROFILES) == 20
    assert len(TRACKED_COMPETITION_IDS) == 17
    assert not {525, 180, 5196} & TRACKED_COMPETITION_IDS
    for config in PROFILES.values():
        rules = config.competitive_rules[0]
        assert rules.source_urls
        assert rules.season_key in {"2026", "2026/27"}
    assert get_league_config(238, 52).code == "liga_portugal"
    assert get_league_config(155, 143625).code.endswith("argentina_apertura")
    assert get_league_config(155, 156467).code.endswith("argentina_clausura")


@pytest.mark.parametrize("code,teams,games,safe", [
    ("laliga", 20, 38, 17), ("premier_league", 20, 38, 17),
    ("serie_a", 20, 38, 17), ("bundesliga", 18, 34, 15),
    ("ligue_1", 18, 34, 15), ("liga_portugal", 18, 34, 15),
    ("eredivisie", 18, 34, 15), ("saudi_pro_league", 18, 34, 15),
    ("brasileirao", 20, 38, 16), ("shl", 14, 52, 12),
    ("liiga", 17, 64, 12),
])
def test_retention_boundaries_are_league_specific(code, teams, games, safe):
    rules = PROFILES[code].competitive_rules[0]
    retention = [rule for rule in rules.objectives if rule.objective_type == "DIVISION_RETENTION"]
    assert (rules.number_of_teams, rules.total_regular_season_games) == (teams, games)
    assert {rule.reference_rank for rule in retention} == {safe}
    assert {rank for rule in retention for rank in range(rule.rank_start, rule.rank_end + 1)} == set(range(safe + 1, teams + 1))


def test_brazil_base_allocations_do_not_invent_effective_sudamericana_range():
    rules = PROFILES["brasileirao"].competitive_rules[0]
    external = [rule for rule in rules.objectives if rule.objective_type == "COMPETITION_QUALIFICATION"]
    assert [(rule.rank_start, rule.rank_end) for rule in external[:2]] == [(1, 4), (5, 5)]
    assert all(rule.status == "PENDING" and rule.target_season == "2027" for rule in external)
    assert external[2].places == 6
    assert external[2].selection_method == "BEST_ELIGIBLE"
    assert external[2].rank_start is None


def test_annual_objectives_do_not_use_a_single_argentine_or_mexican_tournament():
    for code in (
        "liga_profesional_de_argentina_apertura", "liga_profesional_de_argentina_clausura",
        "liga_mx_apertura", "liga_mx_clausura",
    ):
        rules = PROFILES[code].competitive_rules[0]
        external = [rule for rule in rules.objectives if rule.objective_type == "COMPETITION_QUALIFICATION"]
        assert all(rule.aggregation_scope == "SEASON_AGGREGATE" for rule in external)
    argentina = PROFILES["liga_profesional_de_argentina_apertura"].competitive_rules[0]
    retention = [rule for rule in argentina.objectives if rule.objective_type == "DIVISION_RETENTION"]
    assert {rule.aggregation_scope for rule in retention} == {"SEASON_AGGREGATE", "MULTISEASON_RECORD"}
    assert all(rule.status == "PENDING" for rule in retention)


@pytest.mark.parametrize("label,expected", [
    ("2026/2027", "2026/27"), ("2026-27", "2026/27"),
    ("Apertura 2026", "2026"), (None, None), ("2026/28", None),
])
def test_explicit_season_labels(label, expected):
    assert season_key_from_name(label) == expected


def test_resolver_overrides_complete_cached_metadata_only_for_matching_season():
    event = _configured_event("nhl")
    assert event.competition.total_regular_season_games == 84
    assert event.competition.number_of_teams_source == "competition_rules"
    assert asdict(event.competition)["competitive_rules"]["points_per_win"] == 2
    assert not hasattr(event.competition, "current_standings")
    previous = replace(event.to_identity(), season_name="2025/26")
    original = replace(event.competition, total_regular_season_games=82, competitive_rules=None)
    resolved = resolve_p1_competition_context(previous, original)
    assert resolved.competitive_rules is None
    assert resolved.total_regular_season_games == 82
    assert original.competitive_rules is None
    assert _configured_event("liga_mx_clausura", "2027").competition.competitive_rules is None
    missing = replace(event.to_identity(), season_name=None, season_year=2026)
    assert resolve_p1_competition_context(missing, original).competitive_rules is None


def test_input_capture_serializes_competition_rules_without_mutating_the_snapshot():
    from modules.pillars.mining.serialization import to_json_value

    event = _configured_event("nhl")
    captured = to_json_value({
        "event_identity": event.to_identity(),
        "competition_context": event.competition,
        "streak_analysis": {"current_standings": {"Home": {"rank": 1}}},
    })
    event.competition = replace(event.competition, competitive_rules=None)
    assert captured["competition_context"]["competitive_rules"]["total_regular_season_games"] == 84
    assert "current_standings" not in captured["competition_context"]


def test_variant_without_exact_identity_does_not_pick_another_tournament():
    event = _configured_event("liga_profesional_de_argentina_apertura")
    competition = replace(event.competition, source_tournament_id=None)
    assert resolve_p1_competition_context(event.to_identity(), competition).competitive_rules is None


def test_pipeline_resolves_once_before_history_even_when_metadata_is_complete(monkeypatch):
    from modules.jobs.pre_start_check_job import pillar_pipeline as pipeline

    event = _configured_event("nhl")
    event.competition = replace(event.competition, total_regular_season_games=82, competitive_rules=None)
    event.competition_metadata_resolved = True
    processor = object.__new__(pipeline.EventPillarProcessor)
    processor.debug_mode = False
    calls = []
    streak = SimpleNamespace(current_standings={})

    def resolve(identity, competition):
        calls.append(identity.event_id)
        return resolve_p1_competition_context(identity, competition)

    def history(context, **kwargs):
        assert calls == [event.event_id]
        assert context.competition.total_regular_season_games == 84
        assert context.competition.competitive_rules is not None
        return streak, False

    def unexpected_metadata(*args, **kwargs):
        pytest.fail("Complete cached metadata must not require another metadata query")

    monkeypatch.setattr(pipeline, "resolve_p1_competition_context", resolve)
    monkeypatch.setattr(pipeline, "resolve_matchup_streak_analysis", history)
    monkeypatch.setattr(pipeline, "resolve_competition_metadata", unexpected_metadata)
    assert processor._prepare_p1(event) is None
    assert event.streak_analysis is streak
    assert calls == [event.event_id]


@pytest.mark.parametrize("code,size,played,home,away,cutoff,regime", [
    ("laliga", 20, 35, 18, 19, 66, "DIRECT_RELEGATION_POSITION"),
    ("bundesliga", 18, 30, 16, 17, 70, "RELEGATION_PLAYOFF_POSITION"),
    ("shl", 14, 45, 13, 14, 76, "RELEGATION_PLAYOFF_POSITION"),
])
def test_confirmed_retention_wins_over_pending_international_qualification(
    code, size, played, home, away, cutoff, regime,
):
    result = _calculate_m5(_table(size, played, home, away), _configured_event(code))
    assert result.raw["m5_status"] == "ACTIVE"
    assert result.raw["home"]["objective_type"] == "DIVISION_RETENTION"
    assert result.raw["home"]["points_cutoff"] == cutoff
    assert result.raw["home"]["regime_state"] == regime
    assert result.raw["home"]["base_severity"] == 1


def test_one_unresolved_team_suppresses_operational_pressure_for_both_teams():
    result = _calculate_m5(_table(20, 35, 19, 15), _configured_event("laliga"))
    assert result.raw["home"]["objective_type"] == "DIVISION_RETENTION"
    assert result.raw["home"]["rule_reason"] is None
    assert result.raw["away"]["rule_reason"] == "UNRESOLVED_OBJECTIVE_BOUNDARY"
    assert result.raw["m5_status"] == "INSUFFICIENT_DATA"
    assert result.value == 0 and result.bias == "NONE"
    assert result.raw["home"]["final_cost"] == result.raw["away"]["final_cost"] == 0


def test_stage_qualification_and_absent_relegation_use_one_win_unit():
    event = _configured_event("euroleague")
    result = _calculate_m5(_table(20, 35, 10, 19), event)
    assert result.raw["m5_status"] == "ACTIVE"
    assert result.raw["max_points_remaining"] == 3
    assert result.raw["home"]["objective_type"] == "STAGE_QUALIFICATION"
    assert result.raw["home"]["base_severity"] == 0.85
    assert result.raw["away"]["objective_type"] == "NO_APPLICABLE_OBJECTIVE"
    assert result.raw["away"]["final_cost"] == 0


def test_mexico_qualifies_top_eight_without_play_in():
    result = _calculate_m5(_table(18, 15, 7, 8), _configured_event("liga_mx_apertura"))
    assert result.raw["m5_status"] == "ACTIVE"
    for side in ("home", "away"):
        assert result.raw[side]["objective_type"] == "STAGE_QUALIFICATION"
        assert result.raw[side]["objective_rule"]["qualification_path"] == "DIRECT"
        assert result.raw[side]["objective_rule"]["target"] == "QUARTERFINALS"


def test_argentina_uses_each_groups_boundary_and_never_group_relegation():
    analysis = _table(15, 14, 7, 8)
    for row in analysis.current_standings.values():
        row["group"] = "A"
    away = analysis.current_standings.pop("Real Sociedad")
    analysis.current_standings["A8"] = dict(away)
    for rank in range(1, 16):
        name = "Real Sociedad" if rank == 8 else f"B{rank}"
        analysis.current_standings[name] = {
            "rank": rank, "points": 70 - 2 * rank, "games_played": 14, "group": "B",
        }
    result = _calculate_m5(analysis, _configured_event("liga_profesional_de_argentina_apertura"))
    assert result.raw["m5_status"] == "ACTIVE"
    assert result.raw["home"]["points_cutoff"] == 84
    assert result.raw["away"]["points_cutoff"] == 52
    for row in analysis.current_standings.values():
        row.pop("group")
    result = _calculate_m5(analysis, _configured_event("liga_profesional_de_argentina_apertura"))
    assert result.raw["m5_status"] == "INSUFFICIENT_DATA"
    assert result.raw["m5_status_reason"] == "MISSING_RANKING_GROUP"


@pytest.mark.parametrize("code,size,played", [("nba", 30, 75), ("mlb", 30, 150), ("nfl", 32, 15)])
def test_percentage_tables_are_not_treated_as_points(code, size, played):
    result = _calculate_m5(_table(size, played, 5, 8), _configured_event(code))
    assert result.raw["m5_status"] == "INSUFFICIENT_DATA"
    assert result.raw["m5_status_reason"] == "UNSUPPORTED_RANKING_METRIC"
    assert result.raw["m5_edge"] is None and result.bias == "NONE"


def test_wild_card_is_not_a_global_top_eight_cutoff():
    analysis = _table(8, 75, 4, 5)
    for row in analysis.current_standings.values():
        row["division"] = "Atlantic"
    result = _calculate_m5(analysis, _configured_event("nhl"))
    assert result.raw["m5_status"] == "INSUFFICIENT_DATA"
    assert result.raw["m5_status_reason"] == "UNSUPPORTED_OBJECTIVE_SELECTION"
    assert result.raw["max_points_remaining"] == 18
    assert result.bias == "NONE"


def test_missing_and_mismatched_rules_cannot_produce_ordinary_zero_pressure():
    event = _configured_event("laliga")
    event.competition = replace(event.competition, competitive_rules=None)
    result = _calculate_m5(_table(20, 35, 18, 19), event)
    assert result.raw["m5_status_reason"] == "MISSING_COMPETITION_RULES"
    event = _configured_event("laliga")
    event.season_name = "2025/26"
    result = _calculate_m5(_table(20, 35, 18, 19), event)
    assert result.raw["m5_status_reason"] == "COMPETITION_RULES_SEASON_MISMATCH"
