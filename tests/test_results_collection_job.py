from __future__ import annotations

from unittest.mock import MagicMock, patch
from datetime import date

from modules.jobs.results_collection_job.run_results_collection_job import (
    run_results_collection_previous_day,
)


def test_run_results_collection_previous_day_no_events():
    with patch(
        "modules.jobs.results_collection_job.run_results_collection_job.EventRepository.get_events_by_date",
        return_value=[],
    ) as mock_get_events:
        run_results_collection_previous_day()
        assert mock_get_events.called
        called_date = mock_get_events.call_args[0][0]
        assert isinstance(called_date, date)
