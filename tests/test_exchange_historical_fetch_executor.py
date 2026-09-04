"""Unit tests for the OddsPapi exchange historical fan-out executor."""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from modules.jobs.pre_start_check_job.providers.oddspapi.exchange_historical_fetch_executor import (
    OddspapiExchangeHistoricalFetchExecutor,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.exchange_outcome_selector import (
    ExchangeHistoricalSelection,
)
from modules.odds_ingestion.fetch_result import OddsFetchResult


class _FakeClient:
    def __init__(self, api_key=None):
        self.api_key = api_key
        self.closed = False

    def close(self):
        self.closed = True


class _FakeFetcher:
    """Records the thread + client each call ran on; simulates client-side pacing."""

    def __init__(self, client, calls, lock):
        self.client = client
        self._calls = calls
        self._lock = lock

    def fetch_odds(self, fixture_id, **kwargs):
        with self._lock:
            self._calls.append(
                {
                    "fixture_id": fixture_id,
                    "api_key": self.client.api_key,
                    "thread": threading.current_thread().name,
                    **kwargs,
                }
            )
        return OddsFetchResult.from_payload(
            {"bookmakerOdds": {kwargs["bookmakers"][0]: {"markets": {}}}}
        )


def _selections(count: int) -> list[ExchangeHistoricalSelection]:
    return [
        ExchangeHistoricalSelection(
            bookmaker_slug="betfair-ex",
            source_market_id="102",
            source_outcome_id=str(1000 + index),
            canonical_market_key="1x2_full_time",
        )
        for index in range(count)
    ]


def _build_executor(calls, lock, *, api_keys, max_workers=None):
    clients = []

    def client_factory(*, key_scheduler):
        client = _FakeClient()
        client.key_scheduler = key_scheduler
        clients.append(client)
        return client

    def fetcher_factory(client):
        return _FakeFetcher(client, calls, lock)

    executor = OddspapiExchangeHistoricalFetchExecutor(
        api_keys=api_keys,
        max_workers=max_workers,
        client_factory=client_factory,
        fetcher_factory=fetcher_factory,
        key_scheduler=SimpleNamespace(available_key_count=lambda endpoint: len(api_keys)),
    )
    return executor, clients


@pytest.mark.parametrize("key_count", [1, 3])
def test_change_options_reach_every_worker(key_count):
    calls = []
    executor, clients = _build_executor(calls, threading.Lock(), api_keys=[f"key-{i}" for i in range(key_count)])
    options = dict(
        enable_significant_changes=True,
        min_change_magnitude_pct=25.0,
        min_history_hours=12.0,
        flash_reversal_minutes=2.0,
        min_price=1.02,
        kickoff_utc=datetime(2026, 9, 3, 18, tzinfo=timezone.utc),
    )
    outcomes = executor.fetch_all(
        "fixture-1", selections=_selections(6), source_sport_id="13",
        minimum_initial_span_minutes=60, **options,
    )
    assert len(outcomes) == len(calls) == 6
    assert all(outcome.error is None for outcome in outcomes)
    assert len(clients) == key_count
    for call in calls:
        assert {name: call[name] for name in options} == options


def test_single_key_fetches_all_selections_serially():
    calls: list[dict] = []
    lock = threading.Lock()
    executor, clients = _build_executor(calls, lock, api_keys=["key-1"])

    outcomes = executor.fetch_all(
        "fixture-1",
        selections=_selections(3),
        source_sport_id="10",
        minimum_initial_span_minutes=60.0,
    )

    assert len(outcomes) == 3
    assert all(outcome.error is None for outcome in outcomes)
    assert len(clients) == 1
    assert clients[0].closed is True
    assert clients[0].key_scheduler is executor._scheduler()


def test_multiple_keys_distribute_requests_across_clients():
    calls: list[dict] = []
    lock = threading.Lock()
    executor, clients = _build_executor(
        calls,
        lock,
        api_keys=["key-1", "key-2", "key-3"],
    )

    outcomes = executor.fetch_all(
        "fixture-1",
        selections=_selections(6),
        source_sport_id="10",
        minimum_initial_span_minutes=60.0,
    )

    assert len(outcomes) == 6
    assert len(clients) == 3
    assert all(client.closed for client in clients)
    assert all(client.key_scheduler is executor._scheduler() for client in clients)
    # Every outcome_id was requested exactly once, regardless of which
    # worker/client handled it.
    requested_outcome_ids = sorted(call["outcome_id"] for call in calls)
    assert requested_outcome_ids == sorted(
        int(selection.source_outcome_id) for selection in _selections(6)
    )


def test_max_workers_caps_concurrency_below_available_keys():
    calls: list[dict] = []
    lock = threading.Lock()
    executor, clients = _build_executor(
        calls,
        lock,
        api_keys=["key-1", "key-2", "key-3"],
        max_workers=2,
    )

    outcomes = executor.fetch_all(
        "fixture-1",
        selections=_selections(4),
        source_sport_id="10",
        minimum_initial_span_minutes=60.0,
    )

    assert len(outcomes) == 4
    assert len(clients) == 2


def test_single_selection_never_spins_up_worker_pool():
    calls: list[dict] = []
    lock = threading.Lock()
    executor, clients = _build_executor(
        calls,
        lock,
        api_keys=["key-1", "key-2"],
    )

    outcomes = executor.fetch_all(
        "fixture-1",
        selections=_selections(1),
        source_sport_id="10",
        minimum_initial_span_minutes=60.0,
    )

    assert len(outcomes) == 1
    assert len(clients) == 1
