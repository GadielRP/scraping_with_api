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
        [{"id": 1}, {"id": 2}],
        [],
        SimpleNamespace(),
    )

    assert processed == [1, 2]
