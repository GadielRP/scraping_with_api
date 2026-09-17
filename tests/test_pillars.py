from __future__ import annotations

import ast
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from modules.pillars.context import (
    CompetitionContext,
    EventContext,
    ParticipantContext,
)
from modules.pillars.pillar_1_team_structure.side import (
    calculate_p1_side,
)


DEBUG_STREAK_DIR = ROOT_DIR / "debug" / "matchup_streak_analysis"


def _parse_debug_dump(text: str) -> dict[str, Any]:
    """Parse the current pretty-printed debug dump into a plain dictionary.

    The debug files are written as ``key:`` blocks with ``pprint`` output below
    each field. This keeps the test compatible with the current ``.txt`` dump
    and any future ``.json`` fixtures stored in the same directory.
    """
    payload: dict[str, Any] = {}
    current_key: str | None = None
    current_value_lines: list[str] = []

    for raw_line in text.splitlines():
        if not raw_line.strip():
            continue

        is_top_level_key = not raw_line.startswith(" ") and raw_line.endswith(":")
        if is_top_level_key:
            if current_key is not None:
                payload[current_key] = ast.literal_eval("\n".join(current_value_lines))
            current_key = raw_line[:-1].strip()
            current_value_lines = []
            continue

        current_value_lines.append(raw_line)

    if current_key is not None:
        payload[current_key] = ast.literal_eval("\n".join(current_value_lines))

    return payload


def _load_streak_analysis_payload(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    return _parse_debug_dump(path.read_text(encoding="utf-8"))


def _load_streak_analysis(path: Path) -> SimpleNamespace:
    payload = _load_streak_analysis_payload(path)
    # Round-trip through JSON to mimic the simulated pipeline the user asked for.
    payload = json.loads(json.dumps(payload))
    return SimpleNamespace(**payload)


def _available_fixture_paths() -> list[Path]:
    json_paths = sorted(DEBUG_STREAK_DIR.glob("*.json"))
    if json_paths:
        return json_paths

    txt_paths = sorted(DEBUG_STREAK_DIR.glob("*.txt"))
    return txt_paths


def _build_test_event_context(streak_analysis: Any) -> EventContext:
    # Pillar 1 mostly needs competition totals; the rest of the context is a
    # lightweight stand-in so we can exercise the pillar pipeline in isolation.
    competition = CompetitionContext(
        competition_id=None,
        source=None,
        source_tournament_id=None,
        source_unique_tournament_id=None,
        canonical_name=streak_analysis.competition_name,
        display_name=streak_analysis.competition_name,
        slug=streak_analysis.competition_slug,
        unique_slug=streak_analysis.competition_slug,
        category_id=None,
        category_name=None,
        number_of_teams=20,
        number_of_teams_source="test_fixture_json",
        total_regular_season_games=38,
        standings_grouping=None,
        league_config_source="test_fixture_json",
        has_standings_source_endpoint=None,
        source_status="normalized",
    )

    home = ParticipantContext(
        participant_id=None,
        source=None,
        source_participant_id=None,
        name=streak_analysis.home_team_name,
        slug=None,
        short_name=None,
        source_status="normalized",
    )
    away = ParticipantContext(
        participant_id=None,
        source=None,
        source_participant_id=None,
        name=streak_analysis.away_team_name,
        slug=None,
        short_name=None,
        source_status="normalized",
    )

    return EventContext(
        event_id=streak_analysis.event_id,
        custom_id=streak_analysis.custom_id,
        sport=streak_analysis.sport,
        season_id=streak_analysis.season_id,
        season_name=streak_analysis.season_name,
        season_year=2025,
        starts_at=datetime.fromtimestamp(0, tz=timezone.utc),
        minutes_until_start=streak_analysis.minutes_until_start,
        discovery_source=streak_analysis.discovery_source,
        home=home,
        away=away,
        competition=competition,
        participants_label=streak_analysis.participants,
        context_status="normalized",
    )


@pytest.mark.parametrize("fixture_path", _available_fixture_paths())
def test_pillar_1_team_structure_from_debug_fixture(fixture_path: Path) -> None:
    streak_analysis = _load_streak_analysis(fixture_path)
    event_context = _build_test_event_context(streak_analysis)

    result = calculate_p1_side(
        streak_analysis,
        event_context=event_context,
        debug_mode=True,
    )

    assert result["pillar_id"] == "pillar_1_team_structure"
    assert result["pillar_name"] == "Team Structure"
    assert result["event_id"] == streak_analysis.event_id
    assert result["participants"] == streak_analysis.participants
    assert result["raw"]["event_context_present"] is True
    assert result["raw"]["context_status"] == event_context.context_status
    assert len(result["modules"]) == 7
    assert [module["module_id"] for module in result["modules"]] == [
        "M1",
        "M2",
        "M3",
        "M4",
        "M5",
        "M6",
        "M7",
    ]
    assert "module_weights" in result["raw"]
    assert "active_modules" in result["raw"]
    assert "skipped_modules" in result["raw"]
    assert -1.0 <= result["value"] <= 1.0

    for module in result["modules"]:
        assert module["module_id"]
        assert module["module_name"]
        assert isinstance(module["components"], list)
        assert -1.0 <= module["value"] <= 1.0

    # TODO: add tests for pillar 2 when its implementation exists.
    # TODO: add tests for pillar 3 when its implementation exists.
    # TODO: add tests for pillar 4 when its implementation exists.
    # TODO: add tests for pillar 5 when its implementation exists.
