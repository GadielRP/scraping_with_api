"""Deterministic P1 integration; does not depend on local debug captures."""

from types import SimpleNamespace

import pytest

from modules.pillars.pillar_1_team_structure.side import calculate_p1_side
from modules.pillars.mining.adapters import P1SideMiningAdapter
from tests.pillars.test_structural_target_selection import _event_context


def test_pillar_1_team_structure_with_captured_style_inputs():
    context = _event_context()
    standings = {
        "Home" if rank == 1 else "Away" if rank == 2 else f"Team {rank}": {
            "rank": rank, "points": 20 - rank, "games_played": 7,
            "goals_for": 21 - rank, "goals_against": rank,
        }
        for rank in range(1, 21)
    }
    analysis = SimpleNamespace(
        home_team_results=[{"team_score": 2, "opponent_score": 1} for _ in range(10)],
        away_team_results=[{"team_score": 1, "opponent_score": 2} for _ in range(10)],
        current_standings=standings,
        home_team_current_standing=standings["Home"],
        away_team_current_standing=standings["Away"],
        h2h_matchup_matches=[{"home_score": 2, "away_score": 1}],
        h2h_matchup_matches_analyzed=1,
    )
    result = calculate_p1_side(
        analysis, context.to_identity(), context.competition, debug_mode=True
    )
    assert result["event_id"] == context.event_id
    assert result["participants"] == context.participants_label
    assert [module["module_id"] for module in result["modules"]] == [f"M{i}" for i in range(1, 8)]
    h2h = next(module for module in result["modules"] if module["module_id"] == "M3")
    assert h2h["value"] == pytest.approx(0.16)
    assert h2h["bias"] == "HOME"
    run = P1SideMiningAdapter().build(context.to_identity(), result)
    assert run.status == "ACTIVE"
    assert run.output_payload["EXECUTIVE_SUMMARY"] == result["EXECUTIVE_SUMMARY"]
    assert run.output_payload["raw"]["final"] == result["raw"]["final"]
