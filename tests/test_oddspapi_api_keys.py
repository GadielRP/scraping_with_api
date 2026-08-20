"""OddsPapi key pool and request-slot assignment."""

from modules.oddspapi import api_keys as oddspapi_api_keys
from modules.oddspapi.api_keys import (
    api_key_for_slot,
    configured_api_keys,
    free_endpoint_api_keys,
    odds_endpoint_api_keys,
    parallel_worker_count,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.odds_fetcher import (
    OddspapiOddsFetcher,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.constants import (
    ODDSPAPI_CURRENT_ODDS_ENDPOINT,
    ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
)
from types import SimpleNamespace


def test_configured_keys_use_paid_then_free(monkeypatch):
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_PAID_KEY", "paid")
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_FREE_KEYS", ["free-a", "paid", "free-b"])
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_KEYS", ["legacy"])
    assert configured_api_keys() == ["paid", "free-a", "free-b"]


def test_configured_keys_are_all_free_when_paid_is_empty(monkeypatch):
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_PAID_KEY", "")
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_FREE_KEYS", ["k1", "k2", "k3"])
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_KEYS", [])
    assert configured_api_keys() == ["k1", "k2", "k3"]


def test_configured_keys_fall_back_to_oddspapi_key(monkeypatch):
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_PAID_KEY", "")
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_FREE_KEYS", [])
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_KEYS", ["k1", "k2"])
    assert configured_api_keys() == ["k1", "k2"]


def test_paid_key_owns_odds_and_free_keys_own_historical(monkeypatch):
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_PAID_KEY", "paid")
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_FREE_KEYS", ["k1", "k2"])
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_KEYS", [])
    assert odds_endpoint_api_keys() == ["paid"]
    assert free_endpoint_api_keys() == ["k1", "k2"]


def test_empty_paid_lets_every_key_call_odds(monkeypatch):
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_PAID_KEY", "")
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_FREE_KEYS", ["k1", "k2", "k3"])
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_KEYS", [])
    assert odds_endpoint_api_keys() == ["k1", "k2", "k3"]
    assert free_endpoint_api_keys() == ["k1", "k2", "k3"]


def test_api_key_for_slot_wraps_across_the_pool():
    keys = ["k1", "k2", "k3"]
    assert api_key_for_slot(0, keys) == "k1"
    assert api_key_for_slot(1, keys) == "k2"
    assert api_key_for_slot(2, keys) == "k3"
    assert api_key_for_slot(3, keys) == "k1"
    assert api_key_for_slot(4, keys) == "k2"


def test_parallel_worker_count_caps_at_keys_and_items():
    assert parallel_worker_count(max_workers=10, api_key_count=3, item_count=8) == 3
    assert parallel_worker_count(max_workers=2, api_key_count=5, item_count=8) == 2
    assert parallel_worker_count(max_workers=10, api_key_count=5, item_count=2) == 2
    assert parallel_worker_count(max_workers=10, api_key_count=0, item_count=8) == 1
    assert parallel_worker_count(max_workers=10, api_key_count=8, item_count=8) == 4


def test_fetcher_routes_odds_and_historical_to_separate_clients(monkeypatch):
    odds_calls = []
    historical_calls = []
    odds_client = SimpleNamespace(
        get_odds=lambda **kwargs: odds_calls.append("odds") or {"bookmakerOdds": {}},
    )
    historical_client = SimpleNamespace(
        get_historical_odds=lambda **kwargs: historical_calls.append("historical")
        or {"bookmakerOdds": {}},
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_fetcher."
        "OddspapiHistoricalOddsReader.read",
        lambda *_args, **_kwargs: SimpleNamespace(
            normalized_payload={"ok": True},
            as_of_quotes=(),
        ),
    )
    fetcher = OddspapiOddsFetcher(
        odds_client=odds_client,
        historical_client=historical_client,
    )
    fetcher.fetch_odds("fixture-1", bookmakers=["pinnacle"], endpoint=ODDSPAPI_CURRENT_ODDS_ENDPOINT)
    fetcher.fetch_odds(
        "fixture-1",
        bookmakers=["pinnacle"],
        endpoint=ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
    )
    assert odds_calls == ["odds"]
    assert historical_calls == ["historical"]


def test_serial_batch_rotates_dynamic_leases_with_one_worker_session(monkeypatch):
    created = []
    requested_keys = []

    from modules.oddspapi.api_key_inventory import StaticApiKeyInventory
    from modules.oddspapi.api_key_scheduler import (
        OddsPapiApiKeyScheduler,
        RequestOutcome,
    )

    scheduler = OddsPapiApiKeyScheduler(
        inventory=StaticApiKeyInventory(["k1", "k2", "k3"]),
    )

    class FakeClient:
        def __init__(self, *args, api_key=None, **kwargs):
            self.api_key = api_key
            self.scheduler = kwargs["key_scheduler"]
            created.append((api_key, self.scheduler))

        def close(self):
            pass

        def get_odds(self, **kwargs):
            lease = self.scheduler.acquire("odds")
            requested_keys.append(lease.api_key)
            self.scheduler.complete(
                lease,
                RequestOutcome(status_code=200, response_received=True),
            )
            return None

        def get_historical_odds(self, **kwargs):
            return None

    from modules.jobs.pre_start_check_job.providers.oddspapi.event_selector import (
        OddspapiPreStartCandidate,
    )
    from modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor import (
        OddspapiPreStartOddsBatchProcessor,
    )

    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor."
        "mark_missing_endpoints_unavailable",
        lambda *_args, **_kwargs: None,
    )
    processor = OddspapiPreStartOddsBatchProcessor(
        client_factory=FakeClient,
        key_scheduler=scheduler,
    )
    processor.process(
        [
            OddspapiPreStartCandidate(
                event_id=event_id,
                fixture_id=f"fixture-{event_id}",
                minutes_until_start=1,
            )
            for event_id in (1, 2, 3)
        ],
        bookmakers=["pinnacle"],
        api_keys=["k1", "k2", "k3"],
        max_workers=1,
        market_mapping_index={},
        enable_exchange_historical=False,
    )
    assert created == [(None, scheduler)]
    assert requested_keys == ["k1", "k2", "k3"]


def test_serial_t_minus_one_uses_only_paid_key_for_odds_clients(monkeypatch):
    created = []
    requested_keys = []

    from modules.oddspapi.api_key_inventory import ApiKeyInventory
    from modules.oddspapi.api_key_scheduler import (
        OddsPapiApiKeyScheduler,
        RequestOutcome,
    )

    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_PAID_KEY", "paid")
    monkeypatch.setattr(
        oddspapi_api_keys.Config,
        "ODDSPAPI_FREE_KEYS",
        ["k1", "k2", "k3"],
    )
    monkeypatch.setattr(oddspapi_api_keys.Config, "ODDSPAPI_KEYS", [])
    scheduler = OddsPapiApiKeyScheduler(inventory=ApiKeyInventory())

    class FakeClient:
        def __init__(self, *args, api_key=None, **kwargs):
            self.api_key = api_key
            self.scheduler = kwargs["key_scheduler"]
            created.append((api_key, self.scheduler))

        def close(self):
            pass

        def get_odds(self, **kwargs):
            lease = self.scheduler.acquire("odds")
            requested_keys.append(lease.api_key)
            self.scheduler.complete(
                lease,
                RequestOutcome(status_code=200, response_received=True),
            )
            return None

        def get_historical_odds(self, **kwargs):
            return None

    from modules.jobs.pre_start_check_job.providers.oddspapi.event_selector import (
        OddspapiPreStartCandidate,
    )
    from modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor import (
        OddspapiPreStartOddsBatchProcessor,
    )

    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor."
        "odds_endpoint_api_keys",
        lambda: ["paid"],
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor."
        "free_endpoint_api_keys",
        lambda: ["k1", "k2", "k3"],
    )

    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor."
        "mark_missing_endpoints_unavailable",
        lambda *_args, **_kwargs: None,
    )
    processor = OddspapiPreStartOddsBatchProcessor(
        client_factory=FakeClient,
        key_scheduler=scheduler,
    )
    processor.process(
        [
            OddspapiPreStartCandidate(
                event_id=event_id,
                fixture_id=f"fixture-{event_id}",
                minutes_until_start=1,
            )
            for event_id in (1, 2, 3)
        ],
        bookmakers=["pinnacle"],
        max_workers=1,
        market_mapping_index={},
        enable_exchange_historical=False,
    )
    assert created == [(None, scheduler)]
    assert requested_keys == ["paid", "paid", "paid"]
