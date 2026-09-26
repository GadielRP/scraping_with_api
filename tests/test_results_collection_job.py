from __future__ import annotations

from unittest.mock import MagicMock, patch
from datetime import date

from modules.jobs.results_collection_job.run_results_collection_job import (
    run_results_collection_for_date,
    run_results_collection_previous_day,
)


def test_run_results_collection_previous_day_no_events():
    with patch(
        "modules.jobs.results_collection_job.run_results_collection_job.EventRepository.get_events_by_date",
        return_value=[],
    ) as mock_get_events:
        stats = run_results_collection_previous_day()
        assert mock_get_events.called
        called_date = mock_get_events.call_args[0][0]
        assert isinstance(called_date, date)
        assert stats == {"updated": 0, "skipped": 0, "failed": 0, "deleted": 0}


def test_run_results_collection_for_date_specific_date():
    target = date(2026, 9, 23)
    with patch(
        "modules.jobs.results_collection_job.run_results_collection_job.EventRepository.get_events_by_date",
        return_value=[],
    ) as mock_get_events:
        stats = run_results_collection_for_date(target)
        mock_get_events.assert_called_once_with(target)
        assert stats == {"updated": 0, "skipped": 0, "failed": 0, "deleted": 0}


def test_run_results_collection_for_date_string_format():
    with patch(
        "modules.jobs.results_collection_job.run_results_collection_job.EventRepository.get_events_by_date",
        return_value=[],
    ) as mock_get_events:
        stats = run_results_collection_for_date("2026-09-23")
        mock_get_events.assert_called_once_with(date(2026, 9, 23))
        assert stats == {"updated": 0, "skipped": 0, "failed": 0, "deleted": 0}

