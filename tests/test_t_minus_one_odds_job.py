from __future__ import annotations

import logging
import schedule
from datetime import datetime
from importlib import import_module
from types import SimpleNamespace

from infrastructure.scheduler.job_scheduler import JobScheduler
from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.moment_policy import (
    dual_process_moments,
    is_closing_odds_moment,
    regular_pre_start_moments,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.event_selector import (
    OddspapiPreStartCandidate,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor import (
    OddspapiPreStartOddsBatchProcessor,
)
from modules.odds_ingestion.fetch_result import OddsFetchResult

t_minus_one_module = import_module(
    "modules.jobs.pre_start_check_job.run_t_minus_one_odds_job"
)


def test_closing_moment_replaces_zero_everywhere_in_policy():
    assert Config.PRE_START_CLOSING_ODDS_MINUTE == 1
    assert is_closing_odds_moment(1)
    assert not is_closing_odds_moment(0)
    assert dual_process_moments() == {30, 1}
    assert 1 not in regular_pre_start_moments()
    assert 0 not in Config.PRE_START_ODDS_MOMENTS


def test_t_minus_one_job_queries_exact_slot_and_records_dispatch_lag(
    monkeypatch,
    caplog,
):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 8, 13, 18, 59, 3)

    query_windows = []
    calls = []
    scheduler = SimpleNamespace(
        event_repo=SimpleNamespace(
            get_events_starting_between=lambda start, end, **kwargs: query_windows.append(
                (start, end)
            )
            or [{"id": 101, "start_time_utc": datetime(2026, 8, 13, 19, 0)}]
        )
    )
    monkeypatch.setattr(t_minus_one_module, "datetime", FixedDateTime)
    monkeypatch.setattr(
        t_minus_one_module,
        "run_pre_start_odds_moments",
        lambda *args, **kwargs: calls.append((args, kwargs)) or "plan",
    )

    with caplog.at_level(logging.INFO):
        result = t_minus_one_module.run_t_minus_one_odds_job(
            scheduler,
            datetime(2026, 8, 13, 18, 59),
        )

    assert result == "plan"
    assert query_windows == [
        (
            datetime(2026, 8, 13, 19, 0),
            datetime(2026, 8, 13, 19, 0, 1),
        )
    ]
    assert calls[0][1]["key_moments"] == (1,)
    assert calls[0][1]["timestamp_correction_enabled"] is False
    assert calls[0][1]["evaluate_key_moments"] is False
    assert "dispatch_lag_ms=3000" in caplog.text


def test_t_minus_one_job_does_not_replay_after_event_start(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 8, 13, 19, 0)

    scheduler = SimpleNamespace(
        event_repo=SimpleNamespace(
            get_events_starting_between=lambda *_args: (_ for _ in ()).throw(
                AssertionError("missed T-1 must not query or replay")
            )
        )
    )
    monkeypatch.setattr(t_minus_one_module, "datetime", FixedDateTime)

    assert (
        t_minus_one_module.run_t_minus_one_odds_job(
            scheduler,
            datetime(2026, 8, 13, 18, 59),
        )
        is None
    )


def test_critical_scheduler_runs_every_minute_for_asymmetric_start_times(
    monkeypatch,
):
    scheduler = JobScheduler.__new__(JobScheduler)
    scheduler.critical_scheduler = schedule.Scheduler()
    monkeypatch.setattr(Config, "PRE_START_T_MINUS_ONE_INTERVAL_MINUTES", 1)
    monkeypatch.setattr(Config, "PRE_START_CLOSING_ODDS_MINUTE", 1)

    scheduler._setup_t_minus_one_jobs()

    assert [job.at_time.minute for job in scheduler.critical_scheduler.jobs] == list(
        range(0, 60)
    )


def test_current_only_exchange_batch_can_use_all_api_key_workers(monkeypatch):
    processor = OddspapiPreStartOddsBatchProcessor()
    captured = {}
    sentinel = SimpleNamespace(results=[])
    candidates = [
        OddspapiPreStartCandidate(
            event_id=event_id,
            fixture_id=f"fixture-{event_id}",
            minutes_until_start=1,
        )
        for event_id in (1, 2, 3)
    ]

    def process_parallel(selected, **kwargs):
        captured["selected"] = selected
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(processor, "_process_parallel_workers", process_parallel)

    result = processor.process(
        candidates,
        bookmakers=["pinnacle"],
        exchange_bookmakers=["betfair-ex"],
        exchange_historical_moments=[120],
        api_keys=["key-1", "key-2", "key-3"],
        max_workers=3,
        market_mapping_index={},
    )

    assert result is sentinel
    assert captured["max_workers"] == 3
    assert captured["process_kwargs"]["exchange_bookmakers"] == ["betfair-ex"]


def test_oddspapi_t_minus_one_uses_only_current_odds_endpoint():
    requests = []
    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=SimpleNamespace(
            fetch_odds=lambda *_args, **kwargs: requests.append(kwargs)
            or OddsFetchResult.from_payload({})
        )
    )

    processor.process(
        [
            OddspapiPreStartCandidate(
                event_id=1,
                fixture_id="fixture-1",
                minutes_until_start=1,
                source_sport_id="10",
            )
        ],
        bookmakers=["pinnacle"],
        exchange_bookmakers=["betfair-ex"],
        exchange_historical_moments=[120],
        market_mapping_index={},
    )

    assert [request["endpoint"] for request in requests] == ["odds"]
