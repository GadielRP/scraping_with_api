"""Timing failures and unchanged kickoffs must never be reported as corrections."""

from datetime import timedelta

import pytest

from infrastructure.settings import Config
from modules.jobs.pre_start_check_job import timestamp_corrections as corrections
from modules.sofascore import api_client
from shared.temporal import utc_now


@pytest.mark.parametrize("unchanged, written, expected", [(True, 0, True), (False, 1, False), (False, 0, None)])
def test_timing_return_distinguishes_unchanged_committed_and_failed(monkeypatch, unchanged, written, expected):
    original = utc_now().replace(microsecond=0)
    new = original if unchanged else original + timedelta(minutes=30)
    writes = []

    def update(rows):
        writes.extend(rows)
        return written

    monkeypatch.setattr(corrections.EventRepository, "batch_update_starting_times", update)
    assert corrections.check_and_update_starting_time(
        123, int(new.timestamp()), current_starting_time=original
    ) is expected
    assert writes == ([] if unchanged else [(123, new)])


@pytest.mark.parametrize("timing_result, expected_ids, checked, failed", [
    (True, set(), 1, 0), (False, {123}, 1, 0), (None, set(), 0, 1),
])
def test_only_committed_changes_are_returned_and_counted(monkeypatch, caplog, timing_result, expected_ids, checked, failed):
    monkeypatch.setattr(Config, "PRE_START_WORKERS", 1)
    monkeypatch.setattr(corrections, "minutes_since_start", lambda _: -15)
    monkeypatch.setattr(corrections, "resolve_sofascore_event_id", lambda _: 456)
    monkeypatch.setattr(api_client, "get_event_results", lambda *_args, **_kwargs: (timing_result, None))
    caplog.set_level("INFO", logger=corrections.__name__)
    ids = corrections.check_recently_started_events_for_timestamp_corrections([
        {"id": 123, "sport": "Football", "starts_at": utc_now() - timedelta(minutes=15)}
    ])
    assert ids == expected_ids
    assert (
        f"events_checked={checked} timestamps_corrected={len(expected_ids)} timing_failed={failed}"
        in caplog.text
    )


def test_early_result_is_collected_even_when_timing_cannot_be_checked(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(Config, "PRE_START_WORKERS", 1)
    monkeypatch.setattr(corrections, "minutes_since_start", lambda _: -15)
    monkeypatch.setattr(corrections, "resolve_sofascore_event_id", lambda _: 456)
    result = {"home_score": 1, "away_score": 0, "winner": "1"}
    parsed = SimpleNamespace(is_finished=True, result=result)
    monkeypatch.setattr(api_client, "get_event_results", lambda *_args, **_kwargs: (None, parsed))
    saved = []

    def persist(rows):
        saved.extend(rows)
        return rows

    monkeypatch.setattr(corrections.ResultRepository, "batch_upsert_results", persist)
    assert corrections.check_recently_started_events_for_timestamp_corrections([
        {"id": 123, "sport": "Football", "starts_at": utc_now() - timedelta(minutes=15)}
    ]) == set()
    assert saved == [(123, result)]
