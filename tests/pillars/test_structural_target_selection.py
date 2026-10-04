from __future__ import annotations

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


def _run_pipeline(monkeypatch, selection_spy=None, mining_service=None):
    captured = {}
    context = _trajectory_context()
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
