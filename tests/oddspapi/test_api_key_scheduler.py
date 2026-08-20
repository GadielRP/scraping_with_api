from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import threading

import pytest
import requests

from modules.oddspapi.account_usage import (
    AccountUsageSnapshot,
    OddspapiAccountUsageService,
)
from modules.oddspapi.api_key_inventory import (
    StaticApiKeyInventory,
    api_key_fingerprint,
)
from modules.oddspapi.api_key_scheduler import (
    OddsPapiApiKeyScheduler,
    PersistedApiKeyUsage,
    RequestOutcome,
)
from modules.oddspapi.client import OddsPapiClient
from modules.oddspapi.exceptions import OddsPapiError
from modules.jobs.pre_start_check_job.providers.oddspapi.exchange_historical_fetch_executor import (
    OddspapiExchangeHistoricalFetchExecutor,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.exchange_outcome_selector import (
    ExchangeHistoricalSelection,
)
from shared.timezone_utils import convert_utc_to_local, get_local_now


class MemoryUsageStore:
    def __init__(self, rows=()):
        self.rows = {row.key_fingerprint: row for row in rows}
        self.refresh_failures = []

    def load(self, fingerprints):
        return [self.rows[value] for value in fingerprints if value in self.rows]

    def apply_account_snapshot(self, snapshot):
        self.rows[snapshot.key_fingerprint] = PersistedApiKeyUsage(
            key_fingerprint=snapshot.key_fingerprint,
            subscription_id=snapshot.subscription_id,
            subscription_valid_from=snapshot.subscription_valid_from,
            subscription_valid_until=snapshot.subscription_valid_until,
            request_limit=snapshot.request_limit,
            reported_request_count=snapshot.request_count,
            estimated_request_count=snapshot.request_count or 0,
            status=snapshot.status,
            account_refreshed_at=snapshot.refreshed_at,
        )

    def increment_estimated_usage(self, fingerprint):
        row = self.rows.get(fingerprint) or PersistedApiKeyUsage(
            key_fingerprint=fingerprint
        )
        estimated = row.estimated_request_count + 1
        status = (
            "exhausted"
            if row.request_limit is not None and estimated >= row.request_limit
            else row.status
        )
        self.rows[fingerprint] = PersistedApiKeyUsage(
            **{
                **row.__dict__,
                "estimated_request_count": estimated,
                "status": status,
            }
        )

    def update_status(self, fingerprint, status, *, error_code=None):
        row = self.rows.get(fingerprint) or PersistedApiKeyUsage(
            key_fingerprint=fingerprint
        )
        self.rows[fingerprint] = PersistedApiKeyUsage(
            **{
                **row.__dict__,
                "status": status,
                "last_error_code": error_code,
            }
        )

    def record_refresh_failure(self, fingerprint, error_code):
        self.refresh_failures.append((fingerprint, error_code))


def _usage_row(key, count, limit=250, *, refreshed_at=None):
    return PersistedApiKeyUsage(
        key_fingerprint=api_key_fingerprint(key),
        request_limit=limit,
        reported_request_count=count,
        estimated_request_count=count,
        status="exhausted" if count >= limit else "active",
        account_refreshed_at=refreshed_at,
    )


def _scheduler(keys, rows=(), **kwargs):
    store = MemoryUsageStore(rows)
    scheduler = OddsPapiApiKeyScheduler(
        inventory=StaticApiKeyInventory(keys),
        store=store,
        endpoint_cooldowns={},
        **kwargs,
    )
    return scheduler, store


def _successful_request(scheduler, endpoint="odds"):
    lease = scheduler.acquire(endpoint)
    scheduler.complete(
        lease,
        RequestOutcome(status_code=200, response_received=True),
    )
    return lease


def test_metered_scheduler_catches_up_uneven_accounts_then_round_robins():
    keys = ["k1", "k2", "k3", "k4"]
    scheduler, _store = _scheduler(
        keys,
        [_usage_row(key, count) for key, count in zip(keys, (241, 136, 80, 0))],
    )

    first_eighty = [_successful_request(scheduler).api_key for _ in range(80)]
    assert set(first_eighty) == {"k4"}
    assert _successful_request(scheduler).api_key == "k3"

    for _ in range(426):
        _successful_request(scheduler)

    states = scheduler.usage_snapshot()
    assert {
        states[api_key_fingerprint(key)].estimated_request_count for key in keys
    } == {241}

    next_four = [_successful_request(scheduler).api_key for _ in range(4)]
    assert set(next_four) == set(keys)


def test_metered_scheduler_balances_by_ratio_when_limits_differ():
    scheduler, _store = _scheduler(
        ["small", "large"],
        [_usage_row("small", 50, 100), _usage_row("large", 100, 400)],
    )

    assert _successful_request(scheduler).api_key == "large"


def test_four_concurrent_leases_use_four_keys_when_utilization_is_equal():
    keys = ["k1", "k2", "k3", "k4"]
    scheduler, _store = _scheduler(
        keys,
        [_usage_row(key, 10) for key in keys],
    )

    leases = [scheduler.acquire("odds") for _ in range(4)]
    assert {lease.api_key for lease in leases} == set(keys)
    for lease in leases:
        scheduler.complete(
            lease,
            RequestOutcome(status_code=200, response_received=True),
        )


def test_historical_round_robin_does_not_increment_quota_and_skips_exhausted():
    keys = ["k1", "k2", "k3", "k4"]
    scheduler, _store = _scheduler(
        keys,
        [
            _usage_row("k1", 250),
            _usage_row("k2", 20),
            _usage_row("k3", 30),
            _usage_row("k4", 40),
        ],
    )

    selected = []
    for _ in range(6):
        lease = scheduler.acquire("historical-odds")
        selected.append(lease.api_key)
        scheduler.complete(
            lease,
            RequestOutcome(status_code=200, response_received=True),
        )

    assert Counter(selected) == {"k2": 2, "k3": 2, "k4": 2}
    states = scheduler.usage_snapshot()
    assert states[api_key_fingerprint("k2")].estimated_request_count == 20
    assert states[api_key_fingerprint("k3")].estimated_request_count == 30
    assert states[api_key_fingerprint("k4")].estimated_request_count == 40


@pytest.mark.parametrize("status_code", [200, 404, 500])
def test_processed_metered_response_increments_usage(status_code):
    scheduler, _store = _scheduler(["key"], [_usage_row("key", 10)])
    lease = scheduler.acquire("odds")
    scheduler.complete(
        lease,
        RequestOutcome(status_code=status_code, response_received=True),
    )
    assert (
        scheduler.usage_snapshot()[api_key_fingerprint("key")]
        .estimated_request_count
        == 11
    )


def test_quota_rejection_marks_key_exhausted_without_incrementing():
    scheduler, _store = _scheduler(
        ["almost-full", "healthy"],
        [_usage_row("almost-full", 249), _usage_row("healthy", 249)],
    )
    lease = scheduler.acquire("odds")
    scheduler.complete(
        lease,
        RequestOutcome(
            status_code=429,
            error_code="REQUEST_LIMIT_EXCEEDED",
            response_received=True,
        ),
    )
    state = scheduler.usage_snapshot()[lease.key_fingerprint]
    assert state.status == "exhausted"
    assert state.estimated_request_count == 249
    assert scheduler.diagnostic_counts() == {
        f"{lease.key_id}:quota_exhausted": 1
    }
    assert scheduler.acquire("odds").api_key != lease.api_key


def test_invalid_key_is_disabled_without_counting_a_rejected_request():
    scheduler, _store = _scheduler(["key"], [_usage_row("key", 10)])
    lease = scheduler.acquire("fixtures")
    scheduler.complete(
        lease,
        RequestOutcome(status_code=401, response_received=True),
    )

    state = scheduler.usage_snapshot()[lease.key_fingerprint]
    assert state.status == "invalid"
    assert state.estimated_request_count == 10
    assert scheduler.diagnostic_counts() == {f"{lease.key_id}:invalid": 1}


def test_refresh_uses_persisted_ttl_across_scheduler_restart():
    keys = ["k1", "k2"]
    now = get_local_now()
    calls = []

    class UsageService:
        def fetch(self, api_key):
            calls.append(api_key)
            return AccountUsageSnapshot(
                key_fingerprint=api_key_fingerprint(api_key),
                subscription_id="sub",
                subscription_valid_from=now - timedelta(days=1),
                subscription_valid_until=None,
                request_limit=250,
                request_count=5,
                status="active",
                refreshed_at=now,
            )

    store = MemoryUsageStore()
    first = OddsPapiApiKeyScheduler(
        inventory=StaticApiKeyInventory(keys),
        store=store,
        account_usage_service=UsageService(),
        refresh_hours=24,
    )
    assert first.refresh_if_due() is True
    assert calls == keys

    restarted = OddsPapiApiKeyScheduler(
        inventory=StaticApiKeyInventory(keys),
        store=store,
        account_usage_service=UsageService(),
        refresh_hours=24,
    )
    assert restarted.refresh_if_due() is False
    assert calls == keys


def test_refresh_ttl_uses_configured_local_database_timestamp():
    calls = []

    class UsageService:
        def fetch(self, api_key):
            calls.append(api_key)
            raise AssertionError("fresh naive timestamp should prevent refresh")

    fresh_local = get_local_now()
    scheduler, _store = _scheduler(
        ["key"],
        [_usage_row("key", 40, refreshed_at=fresh_local)],
        account_usage_service=UsageService(),
        refresh_hours=24,
    )

    assert scheduler.refresh_if_due() is False
    assert calls == []


def test_refresh_failure_keeps_stale_state_and_observes_retry_backoff():
    stale = get_local_now() - timedelta(days=2)
    calls = []

    class FailingUsageService:
        def fetch(self, api_key):
            calls.append(api_key)
            raise TimeoutError("account unavailable")

    scheduler, store = _scheduler(
        ["key"],
        [_usage_row("key", 40, refreshed_at=stale)],
        account_usage_service=FailingUsageService(),
        refresh_hours=24,
        refresh_retry_minutes=60,
    )

    assert scheduler.refresh_if_due() is False
    assert scheduler.refresh_if_due() is False
    assert calls == ["key"]
    assert store.refresh_failures == [
        (api_key_fingerprint("key"), "ACCOUNT_REFRESH_TimeoutError")
    ]
    assert scheduler.acquire("odds").api_key == "key"


def test_unknown_key_usage_advances_instead_of_receiving_permanent_priority():
    scheduler, _store = _scheduler(
        ["known", "unknown"],
        [_usage_row("known", 1, limit=250)],
    )

    first = _successful_request(scheduler)
    second = _successful_request(scheduler)

    assert first.api_key == "unknown"
    assert second.api_key == "known"


def test_rate_limited_key_is_avoided_and_reported_without_exposing_secret():
    scheduler, _store = _scheduler(
        ["secret-one", "secret-two"],
        [_usage_row("secret-one", 0), _usage_row("secret-two", 100)],
    )
    lease = scheduler.acquire("odds")
    scheduler.complete(
        lease,
        RequestOutcome(
            status_code=429,
            response_received=True,
            retry_after_seconds=60,
        ),
    )

    assert scheduler.acquire("odds").api_key == "secret-two"
    diagnostics = scheduler.diagnostic_counts()
    assert diagnostics == {f"{lease.key_id}:rate_limited": 1}
    assert "secret-one" not in repr(diagnostics)


def test_account_parser_selects_current_subscription_and_never_returns_raw_key():
    snapshot = OddspapiAccountUsageService.parse(
        {
            "api_key": "secret-key",
            "current_subscription_id": "current",
            "subscriptions": [
                {
                    "subscription_id": "old",
                    "is_active": False,
                    "request_limit": 100,
                    "request_count": 99,
                },
                {
                    "subscription_id": "current",
                    "is_active": True,
                    "valid_from": "2026-08-01T00:00:00Z",
                    "valid_until": None,
                    "request_limit": 250,
                    "request_count": 80,
                },
            ],
        },
        api_key="secret-key",
    )

    assert snapshot.subscription_id == "current"
    assert snapshot.request_count == 80
    assert snapshot.refreshed_at.tzinfo is None
    assert snapshot.subscription_valid_from == convert_utc_to_local(
        datetime(2026, 8, 1, tzinfo=timezone.utc)
    )
    assert snapshot.key_fingerprint == api_key_fingerprint("secret-key")
    assert "secret-key" not in repr(snapshot)


def test_dynamic_client_fails_over_after_request_limit_rejection():
    scheduler, _store = _scheduler(
        ["full", "healthy"],
        [_usage_row("full", 0), _usage_row("healthy", 100)],
    )
    responses = iter(
        [
            SimpleNamespace(
                status_code=429,
                text="request limit exceeded",
                headers={},
                json=lambda: {"code": "REQUEST_LIMIT_EXCEEDED"},
            ),
            SimpleNamespace(
                status_code=200,
                text="",
                headers={},
                json=lambda: {"fixtureId": "fixture-1"},
            ),
        ]
    )
    requested_keys = []
    client = OddsPapiClient(
        key_scheduler=scheduler,
        max_retries=1,
        request_delay_seconds=0,
        endpoint_cooldowns={},
    )

    def get(*_args, **kwargs):
        requested_keys.append(kwargs["params"]["apiKey"])
        return next(responses)

    client.session.get = get
    assert client.get_odds("fixture-1") == {"fixtureId": "fixture-1"}
    assert requested_keys == ["full", "healthy"]


def test_dynamic_client_counts_network_ambiguity_and_releases_lease():
    scheduler, _store = _scheduler(["key"], [_usage_row("key", 10)])
    client = OddsPapiClient(
        key_scheduler=scheduler,
        max_retries=1,
        request_delay_seconds=0,
    )

    def fail(*_args, **_kwargs):
        raise requests.ConnectionError("connection dropped")

    client.session.get = fail
    with pytest.raises(OddsPapiError):
        client.get_odds("fixture-1")

    state = scheduler.usage_snapshot()[api_key_fingerprint("key")]
    assert state.estimated_request_count == 11
    # A second acquisition proves the in-flight reservation was released.
    next_lease = scheduler.acquire("odds")
    assert next_lease.api_key == "key"
    scheduler.complete(next_lease, RequestOutcome())
    client.close()


def test_successful_large_payload_is_decoded_only_once():
    scheduler, _store = _scheduler(["key"], [_usage_row("key", 10)])
    client = OddsPapiClient(
        key_scheduler=scheduler,
        max_retries=1,
        request_delay_seconds=0,
    )
    json_calls = 0

    def decode():
        nonlocal json_calls
        json_calls += 1
        return {"fixtureId": "fixture-1", "bookmakerOdds": {}}

    client.session.get = lambda *_args, **_kwargs: SimpleNamespace(
        status_code=200,
        text="",
        headers={},
        json=decode,
    )

    assert client.get_odds("fixture-1")["fixtureId"] == "fixture-1"
    assert json_calls == 1
    client.close()


def test_exchange_fanout_uses_four_dynamic_workers_for_eight_outcomes():
    keys = ["k1", "k2", "k3", "k4"]
    scheduler, _store = _scheduler(
        keys,
        [_usage_row(key, 10) for key in keys],
    )
    requested_keys = []
    clients_created = []
    concurrency_lock = threading.Lock()
    first_wave = threading.Barrier(4)
    active_requests = 0
    max_active_requests = 0

    class FakeClient:
        def __init__(self, *, key_scheduler):
            self.scheduler = key_scheduler
            self.first_request = True
            clients_created.append(self)

        def get_historical_odds(self, **kwargs):
            nonlocal active_requests, max_active_requests
            lease = self.scheduler.acquire("historical-odds")
            with concurrency_lock:
                requested_keys.append(lease.api_key)
                active_requests += 1
                max_active_requests = max(max_active_requests, active_requests)
            try:
                if self.first_request:
                    self.first_request = False
                    first_wave.wait(timeout=2)
            finally:
                with concurrency_lock:
                    active_requests -= 1
                self.scheduler.complete(
                    lease,
                    RequestOutcome(status_code=200, response_received=True),
                )
            return {"fixtureId": kwargs["fixture_id"], "bookmakers": {}}

        def close(self):
            return None

    executor = OddspapiExchangeHistoricalFetchExecutor(
        api_keys=keys,
        max_workers=4,
        key_scheduler=scheduler,
        client_factory=FakeClient,
    )
    selections = [
        ExchangeHistoricalSelection(
            bookmaker_slug="betfair-ex",
            source_market_id="101",
            source_outcome_id=str(1000 + index),
            canonical_market_key="1x2_full_time",
        )
        for index in range(8)
    ]

    outcomes = executor.fetch_all(
        "fixture-1",
        selections=selections,
        source_sport_id="10",
        minimum_initial_span_minutes=60,
    )

    assert len(clients_created) == 4
    assert len(outcomes) == 8
    assert all(outcome.error is None for outcome in outcomes)
    assert Counter(requested_keys) == {key: 2 for key in keys}
    assert max_active_requests == 4
