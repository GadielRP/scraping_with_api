from __future__ import annotations

import json
import logging
from contextlib import contextmanager

import pytest

from datetime import datetime, timezone

import modules.jobs.pre_start_check_job.pillar_pipeline as pillar_pipeline
from modules.pillars.context import CompetitionContext, EventContext, ParticipantContext
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext
from modules.pillars.evaluation_contracts import EvaluationResult, SignalResult
from modules.pillars.trajectory_selection import (
    HARDCODED_TARGET_MINUTE_BY_FLOW,
    TargetMinuteSelection,
)


@pytest.fixture(autouse=True)
def isolated_audit_database(monkeypatch, tmp_path):
    from infrastructure.persistence.database import DatabaseManager
    manager=DatabaseManager(f"sqlite:///{tmp_path / 'audit.db'}")
    manager.create_tables()
    monkeypatch.setattr(pillar_pipeline,"db_manager",manager)
    yield manager
    manager.engine.dispose()


def _event_context() -> EventContext:
    event = EventContext(
        event_id=4004,
        custom_id="event-4004",
        sport="Football",
        season_id=2026,
        season_name="2026",
        season_year=2026,
        starts_at=datetime(2026, 8, 31, 18, 0, tzinfo=timezone.utc),
        minutes_until_start=5,
        discovery_source="test",
        home=ParticipantContext(
            participant_id=1,
            source="test",
            source_participant_id=101,
            name="Home",
            slug="home",
            short_name="H",
        ),
        away=ParticipantContext(
            participant_id=2,
            source="test",
            source_participant_id=202,
            name="Away",
            slug="away",
            short_name="A",
        ),
        competition=CompetitionContext(
            competition_id=99,
            source="test",
            source_tournament_id=99,
            source_unique_tournament_id=999,
            canonical_name="League",
            display_name="League",
            slug="league",
            unique_slug="league",
            category_id=1,
            category_name="Country",
            number_of_teams=20,
            number_of_teams_source="test",
            total_regular_season_games=38,
            standings_grouping="league",
            competition_config_source="test",
            has_standings_source_endpoint=True,
        ),
        participants_label="Home vs Away",
        round="regular_season",
    )
    event.odds_trajectory = []
    return event


def _trajectory_context(*, evaluation_as_of=None):
    return OddsTrajectoryContext(
        available=True,
        event_id=4004,
        markets={},
        target_minutes_present=[30, 5],
        target_minutes_expected=[120, 30, 5, 1, 0, -5],
        missing_target_minutes=[120, 1, 0, -5],
        evaluation_as_of=evaluation_as_of,
    )


def _run_pipeline(monkeypatch, selection_spy=None, mining_service=None, debug_mode=False):
    captured = {}
    context = _trajectory_context()
    captured["trajectory_context"] = context
    monkeypatch.setattr(
        pillar_pipeline,
        "_is_pillar_competition_in_scope",
        lambda _competition_id: True,
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "build_odds_trajectory_context",
        lambda _rows, **_kwargs: context,
    )
    if selection_spy is not None:
        monkeypatch.setattr(
            pillar_pipeline,
            "select_target_minute",
            selection_spy,
        )

    def fake_p2(*, target_selection, **_kwargs):
        captured["p2_selection"] = target_selection
        captured["p2_markets"] = _kwargs["market_evaluation"]
        return EvaluationResult(
            4004,
            "pillar_2_side_market",
            "test-v3",
            target_selection.target_minute,
            _kwargs["market_evaluation"].selection,
            (SignalResult("neutral", "COMPUTED", 0),),
            (),
        ).to_dict()

    def fake_p3(*, target_selection, **_kwargs):
        captured["p3_selection"] = target_selection
        captured["p3_markets"] = _kwargs["market_evaluation"]
        return EvaluationResult(
            4004,
            "pillar_3_totals_market_context",
            "test-v3",
            target_selection.target_minute,
            _kwargs["market_evaluation"].selection,
            (SignalResult("neutral", "COMPUTED", 0),),
            (),
        ).to_dict()

    monkeypatch.setattr(pillar_pipeline, "calculate_pillar_2", fake_p2)
    monkeypatch.setattr(pillar_pipeline, "calculate_pillar_3", fake_p3)
    processor = pillar_pipeline.EventPillarProcessor(mining_service=mining_service,
        debug_mode=debug_mode,
        enabled_pillars={
            "pillar_1": False,
            "pillar_2": True,
            "pillar_3": True,
            "pillar_4": False,
            "pillar_5": False,
        },
    )
    result = processor.process_event(_event_context())
    return result, captured


def test_pipeline_selects_target_once_and_injects_same_object(monkeypatch) -> None:
    calls = []

    def selection_spy(*args, **kwargs):
        calls.append((args, kwargs))
        return TargetMinuteSelection(
            target_minute=5,
            diagnostics={"selection": "test"},
        )

    result, captured = _run_pipeline(monkeypatch, selection_spy)

    assert len(calls) == 1
    assert calls[0][1]["flow_id"] == pillar_pipeline.CANONICAL_SIGNAL_FLOW_ID
    assert calls[0][1]["evaluation_minute"] == 5
    assert captured["p2_selection"] is captured["p3_selection"]
    assert captured["p2_markets"] is captured["p3_markets"]
    assert result["pillar_2"]["target_minute"] == 5
    assert result["pillar_3"]["target_minute"] == 5


def test_shared_hardcoded_override_is_consumed_by_both_pillars(
    monkeypatch,
) -> None:
    monkeypatch.setitem(
        HARDCODED_TARGET_MINUTE_BY_FLOW,
        pillar_pipeline.CANONICAL_SIGNAL_FLOW_ID,
        0,
    )
    result, captured = _run_pipeline(monkeypatch)

    assert captured["p2_selection"] is captured["p3_selection"]
    assert captured["p2_selection"].target_minute == 0
    assert result["pillar_2"]["target_minute"] == 0
    assert result["pillar_3"]["target_minute"] == 0


def test_execution_error_keeps_common_ft_and_configuration_skips(monkeypatch):
    from tests.pillars.test_market_evaluation import quotes, FT_OT, START

    monkeypatch.setattr(
        pillar_pipeline, "_is_pillar_competition_in_scope", lambda _: True
    )

    def fail(**kwargs):
        raise RuntimeError("engine failed")

    monkeypatch.setattr(pillar_pipeline, "calculate_pillar_2", fail)
    context = _event_context()
    context.starts_at = START
    context.odds_trajectory = [
        {**r, "event_id": 4004} for r in quotes("Home/Away", FT_OT)
    ]
    processor = pillar_pipeline.EventPillarProcessor(enabled_pillars={
            "pillar_1": False,
            "pillar_2": True,
            "pillar_3": False,
            "pillar_4": False,
            "pillar_5": False,
        },
    )
    result = processor.process_event(context)
    assert result["pillar_2"]["status"] == "ERROR"
    assert result["pillar_2"]["selected_full_time_period"] == FT_OT
    assert all(result[f"pillar_{p}"]["status"] == "SKIPPED" for p in (3, 4, 5))


def test_pipeline_groups_debug_snapshots_using_loaded_sport_and_competition(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pillar_pipeline, "_is_pillar_competition_in_scope", lambda _: True)

    event = _event_context()
    event.sport = "Ice hockey"
    event.competition.slug = "NHL-Preseason"
    processor = pillar_pipeline.EventPillarProcessor(debug_mode=True,
        enabled_pillars={f"pillar_{number}": False for number in range(1, 6)},
    )
    assert processor.process_event(event) is not None
    directory = tmp_path / "debug" / "pillar_pipeline_objects" / "ice_hockey" / "nhl_preseason" / "4004_Home_vs_Away"
    assert {path.name for path in directory.iterdir()} == {
        "4004_event_identity.json", "4004_competition_context.json", "4004_odds_trajectory_context.json",
        "4004_odds_trajectory_context.xlsx",
    }
    payload = json.loads((directory / "4004_event_identity.json").read_text(encoding="utf-8"))
    assert payload["sport"] == "Ice hockey"
    assert payload["participants_label"] == "Home vs Away"
    assert "competition" not in payload  # Preserve the existing identity snapshot.


def test_pillar_snapshot_files_share_the_existing_event_folder(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    event = _event_context().to_identity()
    pillar_pipeline._save_pillar_debug_snapshots(
        event_identity=event, competition_context=_event_context().competition, odds_trajectory_context=_trajectory_context(),
        streak_analysis={"home_streak": 3}, competition_slug="Liga-MX-Apertura",
    )
    directory = tmp_path / "debug" / "pillar_pipeline_objects" / "football" / "liga_mx_apertura" / "4004_Home_vs_Away"
    assert {path.name for path in directory.iterdir()} == {
        "4004_event_identity.json", "4004_competition_context.json", "4004_odds_trajectory_context.json",
        "4004_odds_trajectory_context.xlsx", "4004_streak_analysis.json",
    }
    assert json.loads((directory / "4004_streak_analysis.json").read_text(encoding="utf-8")) == {"home_streak": 3}


@pytest.mark.parametrize("debug_mode", [False, True])
def test_excel_uses_same_in_memory_context_only_in_debug(tmp_path, monkeypatch, debug_mode):
    monkeypatch.chdir(tmp_path)
    exported = []
    exporter = pillar_pipeline.export_odds_trajectory_context_xlsx

    def capture(context, path, **kwargs):
        exported.append(context)
        exporter(context, path, **kwargs)

    monkeypatch.setattr(pillar_pipeline, "export_odds_trajectory_context_xlsx", capture)
    result, captured = _run_pipeline(monkeypatch, debug_mode=debug_mode)
    assert result["pillar_2"]["target_minute"] == 5
    assert result["pillar_3"]["target_minute"] == 5
    if debug_mode:
        assert len(exported) == 1
        assert exported[0].markets is captured["trajectory_context"].markets
        assert exported[0].evaluation_as_of == result["evaluation_as_of"]
        directory = tmp_path / "debug/pillar_pipeline_objects/football/league/4004_Home_vs_Away"
        assert (directory / "4004_odds_trajectory_context.json").exists()
        assert (directory / "4004_odds_trajectory_context.xlsx").exists()
    else:
        assert not exported
        assert not (tmp_path / "debug").exists()


def test_excel_failure_keeps_json_calculations_and_mining(tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)
    persisted = []

    def fail(*args, **kwargs):
        raise RuntimeError("workbook write failed")

    monkeypatch.setattr(pillar_pipeline, "export_odds_trajectory_context_xlsx", fail)
    from modules.pillars.mining.service import PillarMiningService
    from infrastructure.persistence.repositories.pillar_mining_repository import PillarMiningRepository
    class RecordingService(PillarMiningService):
        def persist(self,*args,**kwargs):
            persisted.append(args)
            return super().persist(*args,**kwargs)
    mining_service=RecordingService(PillarMiningRepository(),pillar_pipeline._registered_mining_adapters())
    result, _ = _run_pipeline(monkeypatch, mining_service=mining_service, debug_mode=True)
    assert result["pillar_2"]["signals"][0]["status"] == "COMPUTED"
    assert result["pillar_3"]["signals"][0]["status"] == "COMPUTED"
    assert len(persisted) == 6
    directory = tmp_path / "debug/pillar_pipeline_objects/football/league/4004_Home_vs_Away"
    assert json.loads((directory / "4004_odds_trajectory_context.json").read_text())["event_id"] == 4004
    assert "event_id=4004" in caplog.text
    assert "4004_odds_trajectory_context.xlsx" in caplog.text
    assert "RuntimeError" in caplog.text
    assert "workbook write failed" in caplog.text
    pillar_pipeline._save_pillar_debug_snapshots(
        event_identity=_event_context().to_identity(),
        competition_context=_event_context().competition,
        odds_trajectory_context=_trajectory_context(),
        streak_analysis={"home_streak": 3}, competition_slug="league",
    )
    assert json.loads((directory / "4004_streak_analysis.json").read_text()) == {"home_streak": 3}


def test_streak_debug_does_not_overwrite_trajectory_artifacts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    event = _event_context().to_identity()
    pillar_pipeline._save_pillar_debug_snapshots(
        event_identity=event, competition_context=_event_context().competition, odds_trajectory_context=_trajectory_context(), competition_slug="league",
    )
    directory = tmp_path / "debug/pillar_pipeline_objects/football/league/4004_Home_vs_Away"
    trajectory_files = list(directory.glob("*odds_trajectory_context.*"))
    before = {path: path.read_bytes() for path in trajectory_files}
    assert len(before) == 2
    pillar_pipeline._save_pillar_debug_snapshots(
        event_identity=event, competition_context=_event_context().competition, odds_trajectory_context=None,
        streak_analysis={"home_streak": 3}, competition_slug="league",
    )
    assert {path: path.read_bytes() for path in trajectory_files} == before
    assert json.loads((directory / "4004_streak_analysis.json").read_text()) == {"home_streak": 3}
