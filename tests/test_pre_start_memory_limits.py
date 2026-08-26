from types import SimpleNamespace

import modules.jobs.pre_start_check_job.alert_pipeline as alert_pipeline
import modules.jobs.pre_start_check_job.pillar_pipeline as pillar_pipeline
from infrastructure.settings import Config


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
    monkeypatch.setattr(Config, "PILLAR_PIPELINE_WORKERS", 1)
    monkeypatch.setattr(
        pillar_pipeline.EventPillarProcessor,
        "process_event",
        lambda self, payload: processed.append(payload["id"]),
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
            {"id": 1, "event_data": {"competition_id": 145}},
            {"id": 2, "event_data": {"competition_id": 145}},
        ],
        [],
        SimpleNamespace(),
    )

    assert processed == [1, 2]


def test_pillar_pipeline_uses_configured_individual_toggles(monkeypatch):
    captured = {}

    class CapturingProcessor:
        def __init__(self, *, enabled_pillars, **_kwargs):
            captured["enabled_pillars"] = enabled_pillars

        def process_event(self, payload):
            captured.setdefault("processed", []).append(payload["id"])

    monkeypatch.setattr(Config, "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS", False)
    monkeypatch.setattr(Config, "PILLAR_PIPELINE_WORKERS", 1)
    monkeypatch.setattr(
        Config,
        "PILLAR_PIPELINE_ENABLED_PILLARS",
        {
            "pillar_1": True,
            "pillar_2": False,
            "pillar_4": True,
            "pillar_5": False,
        },
    )
    monkeypatch.setattr(pillar_pipeline, "EventPillarProcessor", CapturingProcessor)

    pillar_pipeline.evaluate_and_calculate_pillars_batch(
        [{"id": 1, "event_data": {"competition_id": 145}}],
        [],
        SimpleNamespace(),
    )

    assert captured["enabled_pillars"] == {
        "pillar_1": True,
        "pillar_2": False,
        "pillar_4": True,
        "pillar_5": False,
    }
    assert captured["processed"] == [1]


def test_pillar_pipeline_skips_untracked_competition_before_pillar_flow(monkeypatch):
    monkeypatch.setattr(Config, "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS", True)
    processor = pillar_pipeline.EventPillarProcessor(event_repo=SimpleNamespace())

    event = SimpleNamespace(id=999, competition_id=999, round="regular_season")
    payload = {"success": True, "event_obj": event}

    assert processor.process_event(payload) is None


def test_pillar_pipeline_batch_filters_untracked_competitions(monkeypatch):
    processed = []
    monkeypatch.setattr(Config, "PILLAR_PIPELINE_WORKERS", 1)
    monkeypatch.setattr(
        pillar_pipeline.EventPillarProcessor,
        "process_event",
        lambda self, payload: processed.append(payload["id"]),
    )

    pillar_pipeline.evaluate_and_calculate_pillars_batch(
        [
            {"id": 1, "event_data": {"competition_id": 145}},
            {"id": 2, "event_data": {"competition_id": 999}},
        ],
        [],
        SimpleNamespace(),
    )

    assert processed == [1]
