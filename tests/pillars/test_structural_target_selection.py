from __future__ import annotations

import json

import pytest

from datetime import datetime, timezone
from types import SimpleNamespace

import modules.jobs.pre_start_check_job.pillar_pipeline as pillar_pipeline
from modules.pillars.context import CompetitionContext, EventContext, ParticipantContext
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext
from modules.pillars.evaluation_contracts import EvaluationResult, SignalResult
from modules.pillars.trajectory_selection import (
    HARDCODED_TARGET_MINUTE_BY_FLOW,
    TargetMinuteSelection,
)


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
            source_status="normalized",
        ),
        away=ParticipantContext(
            participant_id=2,
            source="test",
            source_participant_id=202,
            name="Away",
            slug="away",
            short_name="A",
            source_status="normalized",
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
            league_config_source="test",
            has_standings_source_endpoint=True,
            source_status="normalized",
        ),
        participants_label="Home vs Away",
        context_status="normalized",
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
    processor = pillar_pipeline.EventPillarProcessor(
        event_repo=None,
        mining_service=mining_service,
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


def test_pipeline_persists_both_structural_profiles(monkeypatch) -> None:
    persisted = []

    class _MiningService:
        def persist(self, pillar_id, event_context, result):
            persisted.append((pillar_id, event_context.event_id, result))
            return True

    _run_pipeline(
        monkeypatch,
        selection_spy=lambda *_args, **_kwargs: TargetMinuteSelection(
            target_minute=5,
            diagnostics={"selection": "test"},
        ),
        mining_service=_MiningService(),
    )

    assert [(pillar_id, event_id) for pillar_id, event_id, _ in persisted] == [
        ("pillar_2_side_market", 4004),
        ("pillar_3_totals_market_context", 4004),
    ]
    assert all(
        result["payload_schema_version"] == 4
        and result["signals"][0]["status"] == "COMPUTED"
        for _, _, result in persisted
    )


def test_pipeline_passes_actual_evaluation_time_to_p4(monkeypatch) -> None:
    evaluation_time = datetime(2026, 8, 31, 17, 55, 16, tzinfo=timezone.utc)
    captured = {}
    builder_args = {}
    monkeypatch.setattr(
        pillar_pipeline,
        "_is_pillar_competition_in_scope",
        lambda _competition_id: True,
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "build_odds_trajectory_context",
        lambda _rows, **kwargs: (
            builder_args.update(kwargs)
            or _trajectory_context(evaluation_as_of=kwargs.get("evaluation_as_of"))
        ),
    )

    def fake_p4(**kwargs):
        captured.update(kwargs)
        return EvaluationResult(
            4004,
            "pillar_4_temporal_market_drift",
            "test-v3",
            kwargs["target_selection"].target_minute,
            kwargs["market_evaluation"].selection,
            (SignalResult("neutral", "COMPUTED", 0),),
            (),
        ).to_dict()

    monkeypatch.setattr(pillar_pipeline, "calculate_pillar_4", fake_p4)
    processor = pillar_pipeline.EventPillarProcessor(
        event_repo=None,
        enabled_pillars={
            "pillar_1": False,
            "pillar_2": False,
            "pillar_3": False,
            "pillar_4": True,
            "pillar_5": False,
        },
        evaluation_as_of=evaluation_time,
    )

    processor.process_event(_event_context())

    assert builder_args["evaluation_as_of"] is evaluation_time
    assert captured["target_selection"].target_minute == 5
    assert captured["odds_trajectory_context"].evaluation_as_of is evaluation_time


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
    processor = pillar_pipeline.EventPillarProcessor(
        None,
        enabled_pillars={
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

    def unexpected_query():
        raise AssertionError("Debug snapshot paths must not query the database")

    monkeypatch.setattr(pillar_pipeline.db_manager, "get_session", unexpected_query)
    event = _event_context()
    event.sport = "Ice hockey"
    event.competition.slug = "NHL-Preseason"
    processor = pillar_pipeline.EventPillarProcessor(
        event_repo=None, debug_mode=True,
        enabled_pillars={f"pillar_{number}": False for number in range(1, 6)},
    )
    assert processor.process_event(event) is not None
    directory = tmp_path / "debug" / "pillar_pipeline_objects" / "ice_hockey" / "nhl_preseason" / "4004_Home_vs_Away"
    assert {path.name for path in directory.iterdir()} == {
        "4004_event_context.json", "4004_odds_trajectory_context.json",
        "4004_odds_trajectory_context.xlsx",
    }
    payload = json.loads((directory / "4004_event_context.json").read_text(encoding="utf-8"))
    assert payload["sport"] == "Ice hockey"
    assert payload["participants_label"] == "Home vs Away"
    assert "competition" not in payload  # Preserve the existing identity snapshot.


def test_pillar_snapshot_files_share_the_existing_event_folder(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    event = _event_context().to_identity()
    pillar_pipeline._save_pillar_debug_snapshots(
        event_context=event, odds_trajectory_context=_trajectory_context(),
        streak_analysis={"home_streak": 3}, competition_slug="Liga-MX-Apertura",
    )
    directory = tmp_path / "debug" / "pillar_pipeline_objects" / "football" / "liga_mx_apertura" / "4004_Home_vs_Away"
    assert {path.name for path in directory.iterdir()} == {
        "4004_event_context.json", "4004_odds_trajectory_context.json",
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
        assert exported == [captured["trajectory_context"]]
        assert exported[0] is captured["trajectory_context"]
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
    mining_service = SimpleNamespace(persist=lambda *args: persisted.append(args) or True)
    result, _ = _run_pipeline(monkeypatch, mining_service=mining_service, debug_mode=True)
    assert result["pillar_2"]["signals"][0]["status"] == "COMPUTED"
    assert result["pillar_3"]["signals"][0]["status"] == "COMPUTED"
    assert len(persisted) == 2
    directory = tmp_path / "debug/pillar_pipeline_objects/football/league/4004_Home_vs_Away"
    assert json.loads((directory / "4004_odds_trajectory_context.json").read_text())["event_id"] == 4004
    assert "event_id=4004" in caplog.text
    assert "4004_odds_trajectory_context.xlsx" in caplog.text
    assert "RuntimeError" in caplog.text
    assert "workbook write failed" in caplog.text
    pillar_pipeline._save_pillar_debug_snapshots(
        event_context=_event_context().to_identity(),
        odds_trajectory_context=_trajectory_context(),
        streak_analysis={"home_streak": 3}, competition_slug="league",
    )
    assert json.loads((directory / "4004_streak_analysis.json").read_text()) == {"home_streak": 3}


def test_streak_debug_does_not_overwrite_trajectory_artifacts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    event = _event_context().to_identity()
    pillar_pipeline._save_pillar_debug_snapshots(
        event_context=event, odds_trajectory_context=_trajectory_context(), competition_slug="league",
    )
    directory = tmp_path / "debug/pillar_pipeline_objects/football/league/4004_Home_vs_Away"
    trajectory_files = list(directory.glob("*odds_trajectory_context.*"))
    before = {path: path.read_bytes() for path in trajectory_files}
    assert len(before) == 2
    pillar_pipeline._save_pillar_debug_snapshots(
        event_context=event, odds_trajectory_context=None,
        streak_analysis={"home_streak": 3}, competition_slug="league",
    )
    assert {path: path.read_bytes() for path in trajectory_files} == before
    assert json.loads((directory / "4004_streak_analysis.json").read_text()) == {"home_streak": 3}


@pytest.mark.parametrize("entrypoint", ["production", "simulator"])
@pytest.mark.parametrize("excel_fails", [False, True])
def test_shared_debug_flow_exports_from_production_and_simulator(
    tmp_path, monkeypatch, entrypoint, excel_fails,
):
    # Exercise the real debug propagation, key-moment evaluation, batch,
    # processor and writer. Only external ingestion/loading is replaced.
    from decimal import Decimal
    from openpyxl import load_workbook
    import modules.jobs.pre_start_check_job.run_pre_start_check_job as job
    import modules.jobs.pre_start_check_job.key_moment_evaluation as evaluation
    import scripts.development.simulate_pre_start_check as simulator
    from tests.test_odds_trajectory_excel import choice, context

    monkeypatch.chdir(tmp_path)
    event = _event_context()
    event.minutes_until_start = 30
    c = choice("1")
    c.odds_values[30] = Decimal("2.10")
    trajectory = context([("1X2", "Full Time", None, "SofaScore", None, [c])], targets=(120, 30))
    monkeypatch.setattr(pillar_pipeline, "build_odds_trajectory_context", lambda *args, **kwargs: trajectory)
    alerts = []
    monkeypatch.setattr(evaluation, "evaluate_and_dispatch_alerts_batch", lambda *args, **kwargs: alerts.append(kwargs))
    monkeypatch.setattr(evaluation, "_hydrate_missing_tennis_metadata", lambda *args: None)
    monkeypatch.setattr(evaluation, "_build_evaluation_payloads", lambda *args: [event])
    monkeypatch.setattr(evaluation, "flush_missing_standings_endpoints", lambda *args: None)
    monkeypatch.setattr(evaluation, "_load_trajectory_payloads", lambda ids: {4004: []})
    config = pillar_pipeline.Config
    for name, value in {
        "ENABLE_LEGACY_ALERT_PIPELINE": True, "ENABLE_PILLAR_PIPELINE": True,
        "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS": False,
        "PILLAR_PIPELINE_EXECUTION_MOMENTS": [30], "PRE_START_ODDS_MOMENTS": [120, 30],
        "PILLAR_PIPELINE_WORKERS": 1, "PILLAR_MINING_ENABLED": False,
        "PILLAR_PIPELINE_ENABLED_PILLARS": {f"pillar_{i}": False for i in range(1, 6)},
        "ENABLE_TIMESTAMP_CORRECTION": False,
    }.items():
        monkeypatch.setattr(config, name, value)
    if excel_fails:
        def fail(*args, **kwargs):
            raise RuntimeError("Excel unavailable")
        monkeypatch.setattr(pillar_pipeline, "export_odds_trajectory_context_xlsx", fail)

    event_data = {"id": 4004, "starts_at": event.starts_at, "sport": "Football"}
    loaded = SimpleNamespace(id=4004, home_team="Home", away_team="Away", sport="Football", season_id=2026, starts_at=event.starts_at)
    class Rescheduled(set):
        def cleanup(self):
            pass

    runtime = SimpleNamespace(
        event_repo=SimpleNamespace(get_event_by_id=lambda _: loaded),
        recently_rescheduled=Rescheduled(),
        missing_odds=None,
    )
    candidate = {"event_id": 4004, "minutes_until_start": 30}
    plan = SimpleNamespace(candidates=[candidate], by_event_id={4004: candidate})
    monkeypatch.setattr(evaluation, "_select_pipeline_candidates", lambda candidates, moments: candidates)
    op_context = SimpleNamespace(event_states={}, event_ids=set())
    if entrypoint == "production":
        monkeypatch.setattr(job, "_tracked_competition_ids", lambda: set())
        monkeypatch.setattr(job, "tracked_competition_ids", lambda: set())
        monkeypatch.setattr(job, "_load_upcoming_events", lambda *args: [event_data])
        monkeypatch.setattr(job, "minutes_until_start", lambda _: 30)
        monkeypatch.setattr(job, "start_oddsportal_scrape_for_events", lambda *args, **kwargs: op_context)
        monkeypatch.setattr(job, "load_pre_start_odds_source_states", lambda *args: {})
        monkeypatch.setattr(job, "build_pre_start_event_candidates", lambda *args, **kwargs: plan)
        monkeypatch.setattr(job, "attach_stored_observations", lambda *args: None)
        monkeypatch.setattr(job, "persist_snapshot_observations", lambda *args: None)
        monkeypatch.setattr(job, "_ingest_provider_odds", lambda *args, **kwargs: None)
        monkeypatch.setattr(job, "_maintain_recently_started_events", lambda *args: None)
        monkeypatch.setattr(job, "run_in_game_checks", lambda: None)
        monkeypatch.setattr(job.api_client, "set_challenge_evidence_enabled", lambda _: None)
        job.run_pre_start_check_job(runtime, global_debug_mode=True)
    else:
        monkeypatch.setattr(simulator, "PreStartRuntime", lambda _: runtime)
        monkeypatch.setattr(simulator, "_log_pipeline_eligibility", lambda _: True)
        monkeypatch.setattr(simulator.EventRepository, "_build_event_data_with_legacy_fallback", lambda _: event_data)
        monkeypatch.setattr(simulator, "run_production_odds_phase", lambda *args, **kwargs: SimpleNamespace(event_plan=plan))
        monkeypatch.setattr(simulator, "_wait_for_oddsportal_worker", lambda _: None)
        for name, value in {
            "ENABLE_ODDS_INGESTION_SIMULATION": True, "ENABLE_ODDSPORTAL_ODDS_SIMULATION": False,
            "ENABLE_ALERT_PIPELINE": True, "ENABLE_PILLAR_PIPELINE": True,
            "ENABLE_CUSTOM_PILLAR_FILTER": True,
            **{f"ENABLE_PILLAR_{i}": False for i in range(1, 6)},
        }.items():
            monkeypatch.setattr(simulator, name, value)
        assert simulator._run_pre_start_check_simulation(4004, 30)

    assert len(alerts) == 1
    assert alerts[0]["debug_mode"] is True
    directory = tmp_path / "debug/pillar_pipeline_objects/football/league/4004_Home_vs_Away"
    assert json.loads((directory / "4004_odds_trajectory_context.json").read_text())["target_minutes_present"] == [120, 30]
    path = directory / "4004_odds_trajectory_context.xlsx"
    assert path.exists() is not excel_fails
    if not excel_fails:
        workbook = load_workbook(path)
        assert workbook["Forma actual 1X2"]["G4"].value == 2.1
        assert "T30" in workbook["Forma actual 1X2"]["A2"].value
        assert "Inicio: 2026-08-31 18:00:00 UTC" in workbook["Forma actual 1X2"]["A1"].value
        workbook.close()
