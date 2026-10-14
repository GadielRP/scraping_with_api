from __future__ import annotations
from dataclasses import replace

from copy import deepcopy
from dataclasses import replace
import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from modules.pillars.common import ModuleResult
from modules.pillars.mining.adapters.pillar_1 import P1SideMiningAdapter
from modules.pillars.mining.contracts import validate_mining_run
from modules.pillars.pillar_1_team_structure.module_5 import contextual_competitive_cost_engine as m5_engine
from modules.pillars.pillar_1_team_structure.side import side as side_engine
from modules.pillars.pillar_1_team_structure.side.side import (
    _aggregate_multilayer_side_engine_v3,
)
from tests.pillars.pillar_1_team_structure.test_module_4_quality_adjusted_immediate_state_engine import (
    _build_event_context,
)


def _event(total_games=38):
    event = replace(
        _build_event_context(home_name="Valencia", away_name="Real Sociedad"),
        event_id=303597,
        participants_label="Valencia vs Real Sociedad",
    )
    event.competition = replace(event.competition, total_regular_season_games=total_games)
    return event


def _analysis(games_played=35, home_rank=19, away_rank=15):
    names = {home_rank: "Valencia", away_rank: "Real Sociedad"}
    standings = {
        names.get(rank, f"Team {rank}"): {
            "rank": rank, "points": 60 - 2 * rank, "games_played": games_played,
        }
        for rank in range(1, 21)
    }
    return SimpleNamespace(current_standings=standings)


def _valencia_analysis():
    path = Path(__file__).with_name("fixtures") / "303597_m5_standings.json"
    return SimpleNamespace(current_standings=json.loads(path.read_text(encoding="utf-8"))["current_standings"])


def _modules():
    # Values recorded by the original event 303597 simulation.
    values = (-0.12797112757218695, -0.08417972717745636, 0.12153846153846154,
              -0.2567375886524825, 1.0, 0.0, 0.03010526315789472)
    modules = []
    for ordinal, value in enumerate(values, 1):
        module_id = f"M{ordinal}"
        status = "INSUFFICIENT_DATA" if ordinal == 6 else "DEGRADED" if ordinal == 7 else "ACTIVE"
        raw = {f"m{ordinal}_status": status, f"m{ordinal}_status_reason": "recorded_sample"}
        if ordinal == 5:
            raw["m5_pressure_side"] = "HOME"
        modules.append(ModuleResult(
            "pillar_1_team_structure", module_id, module_id, 303597,
            "Valencia vs Real Sociedad", value, "HOME", "LOW", raw=raw,
        ))
    return modules


@pytest.mark.parametrize("status", ["INACTIVE", "INSUFFICIENT_DATA", "INVALID"])
def test_unusable_m5_cannot_reuse_pressure_or_override_away_core(status):
    modules = _modules()
    modules[4] = replace(modules[4], raw={
        "m5_status": status,
        "m5_status_reason": "OUTSIDE_LATE_SEASON_WINDOW",
        "m5_pressure_side": "HOME",
    })
    value, raw = _aggregate_multilayer_side_engine_v3(modules)

    assert value == pytest.approx(-0.09248072500269389)
    assert value == raw["p1_effective_core"]
    assert raw["m5_edge"] == 0
    assert raw["m5_strength"] == status
    assert raw["m5_pressure_side"] == "NONE"
    assert raw["pressure_relation"] == "NO_PRESSURE"
    assert raw["p1_final_state"] == "CONFIRMED"
    assert raw["p1_final_bias"] == "AWAY"


def test_inactive_m5_cannot_create_context_only_window_without_operative_core():
    modules = [replace(module, value=0.0) for module in _modules()]
    modules[4] = replace(modules[4], value=1.0, raw={
        "m5_status": "INACTIVE",
        "m5_status_reason": "OUTSIDE_LATE_SEASON_WINDOW",
        "m5_pressure_side": "HOME",
    })
    value, raw = _aggregate_multilayer_side_engine_v3(modules)

    assert value == 0
    assert raw["pressure_relation"] == "NO_PRESSURE"
    assert raw["p1_final_state"] == "NO_BET"
    assert raw["p1_final_bias"] == "NONE"


def test_valencia_early_season_stops_before_objectives_and_preserves_away_core(monkeypatch, caplog):
    def unexpected_calculation(**kwargs):
        pytest.fail("An early-season match must not calculate team pressure")

    monkeypatch.setattr(m5_engine, "_build_team_metrics", unexpected_calculation)
    analysis = _valencia_analysis()
    before = deepcopy(analysis.current_standings)
    with caplog.at_level(logging.INFO):
        result = m5_engine.calculate_contextual_competitive_cost_engine(analysis, _event())

    assert result.raw["m5_status"] == "INACTIVE"
    assert result.raw["m5_status_reason"] == "OUTSIDE_LATE_SEASON_WINDOW"
    assert result.raw["games_played_reference"] == 7
    assert result.raw["total_season_games"] == 38
    assert result.raw["games_remaining"] == 31
    assert result.raw["season_applicability"]["completion_ratio"] == pytest.approx(7 / 38)
    assert result.raw["m5_edge"] is None
    assert result.value == 0
    assert result.bias == "NONE"
    assert result.strength == "INACTIVE"
    assert result.components == []
    assert "home" not in result.raw  # No calculated objective, urgency or cost.
    assert analysis.current_standings == before
    assert "reason=OUTSIDE_LATE_SEASON_WINDOW" in caplog.text
    assert "games_played_reference=7" in caplog.text

    modules = _modules()
    modules[4] = result
    value, raw = _aggregate_multilayer_side_engine_v3(modules)
    assert value == pytest.approx(-0.09248072500269389)
    assert raw["p1_final_bias"] == "AWAY"
    assert raw["p1_final_state"] == "CONFIRMED"
    assert raw["m5_strength"] == "INACTIVE"
    assert raw["pressure_relation"] == "NO_PRESSURE"


@pytest.mark.parametrize("total,played,expected", [
    (38, 30, "INACTIVE"), (38, 31, "ACTIVE"),
    (50, 39, "INACTIVE"), (50, 40, "ACTIVE"),
])
def test_late_season_boundary_uses_configured_duration(total, played, expected):
    result = m5_engine.calculate_contextual_competitive_cost_engine(_analysis(played), _event(total))
    assert result.raw["m5_status"] == expected
    assert result.raw["total_season_games"] == total
    assert result.raw["games_remaining"] == total - played
    assert result.raw["season_applicability"]["minimum_completion_ratio"] == 0.8


def test_late_season_survival_preserves_original_cost_formulas():
    result = m5_engine.calculate_contextual_competitive_cost_engine(_analysis(), _event())
    assert result.raw["m5_status"] == "ACTIVE"
    home = result.raw["home"]
    assert home["objective_type"] == "SURVIVAL"
    assert home["regime_state"] == "INSIDE_DANGER"
    assert home["points_cutoff"] == 26
    assert home["gap"] == 4
    assert home["urgency_factor"] == pytest.approx(5 / 9)
    assert home["base_severity"] == 1
    assert home["regime_multiplier"] == 1.25
    assert home["final_cost"] == pytest.approx(25 / 36)
    assert result.raw["away"]["final_cost"] == 0
    assert result.value == 1
    assert result.bias == "HOME"


def test_late_season_europe_calculates_target_pressure():
    result = m5_engine.calculate_contextual_competitive_cost_engine(_analysis(home_rank=7), _event())
    home = result.raw["home"]
    assert result.raw["m5_status"] == "ACTIVE"
    assert home["objective_type"] == "EUROPE"
    assert home["regime_state"] == "ON_TARGET_ZONE"
    assert home["points_cutoff"] == 44
    assert home["gap"] == 2
    assert home["urgency_factor"] == pytest.approx(7 / 9)
    assert home["final_cost"] == pytest.approx(49 / 90)
    assert result.value == 1


def test_applicable_m5_with_no_pressure_is_distinct_from_inactive():
    result = m5_engine.calculate_contextual_competitive_cost_engine(_analysis(home_rank=12), _event())
    assert result.raw["m5_status"] == "ACTIVE"
    assert result.raw["m5_edge"] == 0
    assert result.value == 0
    assert result.bias == "NONE"
    assert result.strength == "IGNORE"


@pytest.mark.parametrize("total,played,status,reason", [
    (None, 35, "INSUFFICIENT_DATA", "MISSING_TOTAL_SEASON_GAMES"),
    (38, None, "INSUFFICIENT_DATA", "MISSING_GAMES_PLAYED"),
    (38, 38, "INACTIVE", "NO_REMAINING_SEASON_GAMES"),
])
def test_unresolved_or_finished_season_cannot_generate_pressure(total, played, status, reason):
    result = m5_engine.calculate_contextual_competitive_cost_engine(_analysis(played), _event(total))
    assert result.raw["m5_status"] == status
    assert result.raw["m5_status_reason"] == reason
    assert result.raw["m5_edge"] is None
    assert result.components == []


def test_applicable_m5_still_reports_missing_competitive_data():
    analysis = _analysis()
    analysis.current_standings["Valencia"]["points"] = None
    result = m5_engine.calculate_contextual_competitive_cost_engine(analysis, _event())
    assert result.raw["season_applicability"]["is_applicable"] is True
    assert result.raw["m5_status"] == "INSUFFICIENT_DATA"
    assert result.raw["m5_status_reason"] == "missing_points_or_rank"


def test_joint_side_execution_keeps_other_module_results_and_shared_inputs(monkeypatch):
    modules = _modules()
    functions = (
        "calculate_base_strength", "calculate_performance_profile",
        "calculate_direct_matchup_profile", "calculate_quality_adjusted_immediate_state_engine",
        "calculate_contextual_competitive_cost_engine", "calculate_structural_drift_engine",
        "calculate_m7_opponent_expectation_engine",
    )
    for function, module in zip(functions, modules):
        if module.module_id != "M5":
            monkeypatch.setattr(side_engine, function, lambda *args, _module=module, **kwargs: _module)
    analysis = _valencia_analysis()
    before = deepcopy(analysis.current_standings)
    result = side_engine.calculate_p1_side(analysis, _event())
    expected = [side_engine._serialize_module_result(module) for module in modules if module.module_id != "M5"]
    actual = [module for module in result["modules"] if module["module_id"] != "M5"]
    assert actual == expected
    assert analysis.current_standings == before
    assert result["value"] == pytest.approx(-0.09248072500269389)
    assert result["raw"]["final"]["p1_final_bias"] == "AWAY"
    assert result["raw"]["engine_version"] == "side_engine_multilayer_v3_2_executive_summary"

    mining_run = P1SideMiningAdapter().build(_event(), result)
    validate_mining_run(replace(mining_run, evaluation_id="evaluation"))
    m5_unit = next(unit for unit in mining_run.units if unit.unit_key == "module:M5")
    assert m5_unit.status == "BLOCKED"
    assert m5_unit.is_valid is False
    assert m5_unit.strength == "INACTIVE"
    assert m5_unit.diagnostics["evidence"]["status_reason"] == "OUTSIDE_LATE_SEASON_WINDOW"
