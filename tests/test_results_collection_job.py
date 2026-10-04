from __future__ import annotations

from unittest.mock import Mock, patch
from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from modules.jobs.results_collection_job.run_results_collection_job import (
    run_results_collection,
)


def test_run_results_collection_no_events():
    with patch(
        "modules.jobs.results_collection_job.run_results_collection_job.ResultRepository.pending_batches",
        return_value=[],
    ) as mock_get_events:
        stats = run_results_collection()
        assert mock_get_events.called
        called_date = mock_get_events.call_args[0][0]
        assert called_date.start is None and called_date.default_cutoff is not None
        assert stats == {"updated": 0, "deferred": 0, "failed": 0, "deleted": 0}


def test_run_results_collection_specific_date():
    target = date(2026, 9, 23)
    with patch(
        "modules.jobs.results_collection_job.run_results_collection_job.ResultRepository.pending_batches",
        return_value=[],
    ) as mock_get_events:
        stats = run_results_collection(target)
        assert mock_get_events.call_args[0][0].start is not None
        assert stats == {"updated": 0, "deferred": 0, "failed": 0, "deleted": 0}


def test_run_results_collection_string_format():
    with patch(
        "modules.jobs.results_collection_job.run_results_collection_job.ResultRepository.pending_batches",
        return_value=[],
    ) as mock_get_events:
        stats = run_results_collection("2026-09-23")
        assert mock_get_events.call_args[0][0].start is not None
        assert stats == {"updated": 0, "deferred": 0, "failed": 0, "deleted": 0}


def test_manual_results_uses_previous_local_date_without_starting_workers(monkeypatch):
    from app.runtime import ApplicationRuntime

    monkeypatch.setattr(
        "shared.temporal.now_in_timezone",
        lambda _: datetime(2026, 10, 3, 1, tzinfo=ZoneInfo("America/Mexico_City")),
    )
    runtime = ApplicationRuntime.__new__(ApplicationRuntime)
    runtime.scheduler = None
    runtime.settings = SimpleNamespace()
    runtime.jobs = {"results": Mock(return_value="completed")}
    runtime.close = Mock()
    assert runtime.run("results") == "completed"
    runtime.jobs["results"].assert_called_once_with(target_date=date(2026, 10, 2))
    runtime.close.assert_called_once()
