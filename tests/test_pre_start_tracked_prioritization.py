import pytest
from unittest.mock import MagicMock, patch

from modules.jobs.pre_start_check_job.timing import should_extract_odds_for_event
from modules.jobs.pre_start_check_job.event_candidate_builder import (
    PreStartEventPlan,
    build_pre_start_event_candidates,
)
from modules.jobs.pre_start_check_job.odds_source_state import PreStartOddsSourceStates
from modules.jobs.pre_start_check_job.providers.sofascore.odds_phase import run_sofascore_pre_start_odds
from modules.odds_ingestion import ProviderOddsSummary


def test_orchestrator_assembles_tracked_candidates_first():
    """Verify plan_ts candidates are placed first in the unified plan."""
    plan_ts = PreStartEventPlan(
        candidates=[
            {"event_id": 101, "competition_id": 10, "should_extract_odds": True},
            {"event_id": 102, "competition_id": 20, "should_extract_odds": True},
        ],
        by_event_id={101: {"event_id": 101}, 102: {"event_id": 102}},
    )
    plan_no_ts = PreStartEventPlan(
        candidates=[
            {"event_id": 201, "competition_id": 99, "should_extract_odds": True},
            {"event_id": 202, "competition_id": 88, "should_extract_odds": True},
        ],
        by_event_id={201: {"event_id": 201}, 202: {"event_id": 202}},
    )
    unified_plan = PreStartEventPlan(
        candidates=plan_ts.candidates + plan_no_ts.candidates,
        by_event_id={**plan_ts.by_event_id, **plan_no_ts.by_event_id},
    )
    assert [c["event_id"] for c in unified_plan.candidates] == [101, 102, 201, 202]


@patch("modules.sofascore.api_client.get_event_results")
def test_should_extract_odds_for_event_skips_t30_api_when_fetch_alert_metadata_false(mock_get_results):
    # Event at T-30 with timestamp_correction_enabled=False and fetch_alert_metadata=False
    should_extract, metadata_snapshot, timing_changed, sofa_id = should_extract_odds_for_event(
        event_id=101,
        minutes_until=30,
        sofascore_event_id=10101,
        timestamp_correction_enabled=False,
        fetch_alert_metadata=False,
    )
    assert should_extract is True
    assert metadata_snapshot is None
    assert timing_changed is False
    assert mock_get_results.call_count == 0


@patch("modules.sofascore.api_client.get_event_results")
def test_should_extract_odds_for_event_calls_t30_api_when_fetch_alert_metadata_true(mock_get_results):
    mock_get_results.return_value = (True, {"meta": 1})
    should_extract, metadata_snapshot, timing_changed, sofa_id = should_extract_odds_for_event(
        event_id=101,
        minutes_until=30,
        sofascore_event_id=10101,
        timestamp_correction_enabled=False,
        fetch_alert_metadata=True,
    )
    assert should_extract is True
    assert metadata_snapshot == {"meta": 1}
    assert timing_changed is False
    assert mock_get_results.call_count == 1


@patch("modules.jobs.pre_start_check_job.providers.sofascore.odds_phase.get_tracked_competition_ids", return_value=[10])
@patch("modules.jobs.pre_start_check_job.providers.sofascore.odds_phase.run_provider_odds_phase")
def test_sofascore_odds_phase_splits_tracked_and_untracked_batches(mock_run_phase, mock_tracked_ids, caplog):
    import logging
    caplog.set_level(logging.INFO)
    mock_run_phase.side_effect = [
        ProviderOddsSummary(candidates_seen=1, requests_attempted=1, events_ingested=1),
        ProviderOddsSummary(candidates_seen=2, requests_attempted=2, events_ingested=2),
    ]

    events = [
        {"event_id": 1, "competition_id": 10, "event_data": {"competition_id": 10, "slug": "slug1"}, "sofascore_event_id": 101, "should_extract_odds": True},
        {"event_id": 2, "competition_id": 99, "event_data": {"competition_id": 99, "slug": "slug2"}, "sofascore_event_id": 102, "should_extract_odds": True},
        {"event_id": 3, "competition_id": 88, "event_data": {"competition_id": 88, "slug": "slug3"}, "sofascore_event_id": 103, "should_extract_odds": True},
    ]

    summary = run_sofascore_pre_start_odds(
        events_to_process=events,
        source_states={},
        tracked_competition_ids=None,
    )

    assert mock_run_phase.call_count == 2
    assert summary.candidates_seen == 3
    assert summary.events_ingested == 3
    assert "[TRACKED] START SofaScore odds extraction (1 candidates)" in caplog.text
    assert "[UNTRACKED] START SofaScore odds extraction (2 candidates)" in caplog.text


@patch("modules.jobs.pre_start_check_job.providers.sofascore.odds_phase.get_tracked_competition_ids", return_value=[10])
@patch("modules.jobs.pre_start_check_job.providers.sofascore.odds_phase.run_provider_odds_phase")
def test_sofascore_odds_phase_omits_tracked_log_when_no_tracked_events(mock_run_phase, mock_tracked_ids, caplog):
    import logging
    caplog.set_level(logging.INFO)
    mock_run_phase.return_value = ProviderOddsSummary(candidates_seen=1, requests_attempted=1, events_ingested=1)

    events = [
        {"event_id": 2, "competition_id": 99, "event_data": {"competition_id": 99, "slug": "slug2"}, "sofascore_event_id": 102, "should_extract_odds": True},
    ]

    summary = run_sofascore_pre_start_odds(
        events_to_process=events,
        source_states={},
        tracked_competition_ids=None,
    )

    assert mock_run_phase.call_count == 1
    assert "[TRACKED] START" not in caplog.text
    assert "[UNTRACKED] START SofaScore odds extraction (1 candidates)" in caplog.text


def test_oddspapi_odds_phase_omits_logs_when_no_active_candidates(caplog):
    import logging
    from modules.jobs.pre_start_check_job.providers.oddspapi.odds_phase import run_oddspapi_pre_start_odds
    caplog.set_level(logging.INFO)

    events = [
        {"event_id": 1, "competition_id": 99, "event_data": {"competition_id": 99}, "should_extract_odds": False},
    ]

    summary = run_oddspapi_pre_start_odds(
        events_to_process=events,
        source_states={},
        tracked_competition_ids={10},
    )

    assert summary.candidates_seen == 0
    assert "Oddspapi pre-start odds starting..." not in caplog.text
    assert "[TRACKED] START" not in caplog.text


def test_tennis_events_included_in_timestamp_corrections_even_when_untracked(monkeypatch):
    from infrastructure.settings import Config
    from modules.jobs.pre_start_check_job.run_pre_start_check_job import run_pre_start_odds_moments

    monkeypatch.setattr(Config, "ENABLE_TIMESTAMP_CORRECTION", True)
    monkeypatch.setattr(Config, "TIMESTAMP_CORRECTIONS_TRACKED_COMPETITIONS_ONLY", True)
    monkeypatch.setattr(Config, "TIMESTAMP_CORRECTIONS_INCLUDE_ALL_TENNIS", True)

    upcoming = [
        {"id": 1, "competition_id": 99, "sport": "Football", "starts_at": None},
        {"id": 2, "competition_id": 99, "sport": "Tennis", "starts_at": None},
        {"id": 3, "competition_id": 99, "sport": "Tennis Doubles", "starts_at": None},
    ]

    captured_ts_events = []
    captured_no_ts_events = []

    def mock_build_candidates(scheduler, events, timings, source_states, **kwargs):
        if kwargs.get("timestamp_correction_enabled"):
            captured_ts_events.extend(events)
        else:
            captured_no_ts_events.extend(events)
        return PreStartEventPlan(candidates=[], by_event_id={})

    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.run_pre_start_check_job.build_pre_start_event_candidates",
        mock_build_candidates,
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.run_pre_start_check_job.tracked_competition_ids",
        lambda: [10],
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.run_pre_start_check_job._ingest_provider_odds",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.run_pre_start_check_job.attach_stored_observations",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.run_pre_start_check_job.persist_snapshot_observations",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.run_pre_start_check_job.load_pre_start_odds_source_states",
        lambda *args: {},
    )

    # At T-30, untracked tennis is bypassed (in no_ts_events) to save API calls
    run_pre_start_odds_moments(
        scheduler=MagicMock(),
        upcoming_events=upcoming,
        timings={1: 30, 2: 30, 3: 30},
        key_moments=[30],
        oddsportal_context=MagicMock(),
        evaluate_key_moments=False,
    )
    assert [e["id"] for e in captured_ts_events] == []
    assert [e["id"] for e in captured_no_ts_events] == [1, 2, 3]

    captured_ts_events.clear()
    captured_no_ts_events.clear()

    # At T-5, untracked tennis is promoted to ts_events for final timestamp verification
    run_pre_start_odds_moments(
        scheduler=MagicMock(),
        upcoming_events=upcoming,
        timings={1: 5, 2: 5, 3: 5},
        key_moments=[5],
        oddsportal_context=MagicMock(),
        evaluate_key_moments=False,
    )
    assert [e["id"] for e in captured_ts_events] == [2, 3]
    assert [e["id"] for e in captured_no_ts_events] == [1]
