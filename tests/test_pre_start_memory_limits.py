from types import SimpleNamespace
import pytest

import modules.jobs.pre_start_check_job.alert_pipeline as alert_pipeline
import modules.jobs.pre_start_check_job.pillar_pipeline as pillar_pipeline
from infrastructure.settings import Config
from tests.pillars.test_structural_target_selection import _event_context


def event_context(event_id, competition_id=145):
    context = _event_context()
    context.event_id = event_id
    context.competition.competition_id = competition_id
    return context


def test_alert_pipeline_uses_direct_serial_execution(monkeypatch):
    processed = []
    monkeypatch.setattr(Config, "ALERT_PIPELINE_WORKERS", 1)
    monkeypatch.setattr(
        alert_pipeline.EventAlertProcessor,
        "process_event",
        lambda self, payload: processed.append(payload["id"]),
    )
    monkeypatch.setattr(
        alert_pipeline,
        "ThreadPoolExecutor",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("serial mode must not create a thread pool")
        ),
    )

    alert_pipeline.evaluate_and_dispatch_alerts_batch(
        [{"id": 1}, {"id": 2}],
        [],
        SimpleNamespace(),
    )

    assert processed == [1, 2]


def test_pillar_pipeline_uses_direct_serial_execution(monkeypatch):
    processed = []
    monkeypatch.setattr(Config, "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS", False)
    monkeypatch.setattr(Config, "PILLAR_PIPELINE_WORKERS", 1)
    monkeypatch.setattr(Config, "PILLAR_PIPELINE_EXECUTION_MOMENTS", [])
    monkeypatch.setattr(
        pillar_pipeline.EventPillarProcessor,
        "process_event",
        lambda self, payload, trajectory_points=None: processed.append(
            payload.event_id
        ),
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "ThreadPoolExecutor",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("serial mode must not create a thread pool")
        ),
    )

    pillar_pipeline.evaluate_and_calculate_pillars_batch(
        [
            event_context(1),
            event_context(2),
        ],
    )

    assert processed == [1, 2]


def test_pillar_batch_releases_trajectory_map_entry_before_processing(monkeypatch):
    trajectory = [{"target_minute": 5}]
    trajectories_by_event_id = {1: trajectory}
    processed = []

    class CapturingProcessor:
        def __init__(self, **_kwargs):
            pass

        def process_event(self, event_context, trajectory_points=None):
            processed.append(
                (
                    event_context.event_id,
                    trajectory_points,
                    dict(trajectories_by_event_id),
                )
            )

    monkeypatch.setattr(Config, "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS", False)
    monkeypatch.setattr(Config, "PILLAR_PIPELINE_WORKERS", 1)
    monkeypatch.setattr(pillar_pipeline, "EventPillarProcessor", CapturingProcessor)

    pillar_pipeline.evaluate_and_calculate_pillars_batch(
        [event_context(1)],
        trajectories_by_event_id=trajectories_by_event_id,
    )

    assert processed == [(1, trajectory, {})]


def test_pillar_pipeline_uses_configured_individual_toggles(monkeypatch):
    captured = {}

    class CapturingProcessor:
        def __init__(self, *, enabled_pillars, **_kwargs):
            captured["enabled_pillars"] = enabled_pillars

        def process_event(self, payload, trajectory_points=None):
            captured.setdefault("processed", []).append(payload.event_id)

    monkeypatch.setattr(Config, "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS", False)
    monkeypatch.setattr(Config, "PILLAR_PIPELINE_WORKERS", 1)
    monkeypatch.setattr(Config, "PILLAR_PIPELINE_EXECUTION_MOMENTS", [])
    monkeypatch.setattr(
        Config,
        "PILLAR_PIPELINE_ENABLED_PILLARS",
        {
            "pillar_1": True,
            "pillar_2": False,
            "pillar_3": True,
            "pillar_4": True,
            "pillar_5": False,
        },
    )
    monkeypatch.setattr(pillar_pipeline, "EventPillarProcessor", CapturingProcessor)

    pillar_pipeline.evaluate_and_calculate_pillars_batch(
        [event_context(1)],
    )

    assert captured["enabled_pillars"] == {
        "pillar_1": True,
        "pillar_2": False,
        "pillar_3": True,
        "pillar_4": True,
        "pillar_5": False,
    }
    assert captured["processed"] == [1]


def test_pillar_pipeline_skips_untracked_competition_before_pillar_flow(monkeypatch):
    monkeypatch.setattr(Config, "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS", True)
    monkeypatch.setattr(pillar_pipeline, "is_tracked_competition", lambda _: False)
    processor = pillar_pipeline.EventPillarProcessor()
    assert processor.process_event(event_context(999, 999)) is None


def test_pillar_pipeline_batch_filters_untracked_competitions(monkeypatch):
    processed = []
    monkeypatch.setattr(Config, "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS", True)
    monkeypatch.setattr(
        pillar_pipeline,
        "is_tracked_competition",
        lambda competition_id: competition_id == 145,
    )
    monkeypatch.setattr(Config, "PILLAR_PIPELINE_WORKERS", 1)
    monkeypatch.setattr(Config, "PILLAR_PIPELINE_EXECUTION_MOMENTS", [])
    monkeypatch.setattr(
        pillar_pipeline.EventPillarProcessor,
        "process_event",
        lambda self, payload, trajectory_points=None: processed.append(
            payload.event_id
        ),
    )

    pillar_pipeline.evaluate_and_calculate_pillars_batch(
        [
            event_context(1),
            event_context(2, 999),
        ],
    )

    assert processed == [1]


def test_batch_finishes_independent_events_and_propagates_persistence_failure(
    monkeypatch,
):
    processed = []
    monkeypatch.setattr(Config, "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS", False)
    monkeypatch.setattr(Config, "PILLAR_PIPELINE_WORKERS", 2)

    def process(self, context, trajectory_points=None):
        processed.append(context.event_id)
        if context.event_id == 1:
            raise RuntimeError("persistence failed")

    monkeypatch.setattr(pillar_pipeline.EventPillarProcessor, "process_event", process)
    with pytest.raises(RuntimeError, match="could not be persisted"):
        pillar_pipeline.evaluate_and_calculate_pillars_batch(
            [event_context(1), event_context(2)]
        )
    assert sorted(processed) == [1, 2]
