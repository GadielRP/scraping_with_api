"""Wiring tests: the batch processor should hand the acquisition service an
exchange fan-out executor whenever more than one OddsPapi API key is
available, even for a single candidate/event."""

from __future__ import annotations

import pytest
from dataclasses import replace
from types import SimpleNamespace

from infrastructure.settings.config import Config

from modules.jobs.pre_start_check_job.providers.oddspapi.event_selector import (
    OddspapiPreStartCandidate,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.exchange_historical_fetch_executor import (
    OddspapiExchangeHistoricalFetchExecutor,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.odds_acquisition_service import (
    OddspapiOddsAcquisitionResult,
    OddspapiPreStartOddsAcquisitionService,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor import (
    OddspapiPreStartOddsBatchProcessor,
)


@pytest.fixture(autouse=True)
def _isolate_unavailable_endpoint_writes(monkeypatch):
    module = "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor"
    monkeypatch.setattr(f"{module}.free_endpoint_api_keys", lambda: [])
    monkeypatch.setattr(f"{module}.odds_endpoint_api_keys", lambda: [])
    monkeypatch.setattr(
        f"{module}.get_oddspapi_key_scheduler",
        lambda: SimpleNamespace(available_key_count=lambda endpoint: 0),
    )
    monkeypatch.setattr(
        f"{module}.OddspapiMainlineCacheRepository.event_ids_with_cache",
        lambda ids: set(ids),
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi."
        "odds_batch_processor.mark_missing_endpoints_unavailable",
        lambda *args: None,
    )


def _candidate(event_id=101) -> OddspapiPreStartCandidate:
    return OddspapiPreStartCandidate(
        event_id=event_id,
        fixture_id="fixture-1",
        minutes_until_start=120,
        has_odds=True,
        source_sport_id="10",
    )


def _capture_acquire_calls(monkeypatch):
    calls: list[dict] = []

    def fake_acquire(self, fixture_id, **kwargs):
        calls.append({"fixture_id": fixture_id, **kwargs})
        return OddspapiOddsAcquisitionResult()

    monkeypatch.setattr(
        OddspapiPreStartOddsAcquisitionService,
        "acquire",
        fake_acquire,
    )
    return calls


def _process(candidate, **kwargs):
    # No fetcher/acquisition_service injected: this keeps _custom_pipeline
    # False, matching the real production wiring in odds_phase.py.
    processor = OddspapiPreStartOddsBatchProcessor()
    return processor.process(
        [candidate],
        bookmakers=["pinnacle"],
        exchange_bookmakers=["betfair-ex"],
        market_mapping_index={},
        **kwargs,
    )


def test_single_event_gets_exchange_fetch_executor_with_multiple_keys(monkeypatch):
    calls = _capture_acquire_calls(monkeypatch)

    _process(
        _candidate(),
        api_keys=["key-1", "key-2", "key-3"],
        max_workers=3,
    )

    assert len(calls) == 1
    executor = calls[0]["exchange_fetch_executor"]
    assert isinstance(executor, OddspapiExchangeHistoricalFetchExecutor)


def test_single_api_key_does_not_build_an_executor(monkeypatch):
    calls = _capture_acquire_calls(monkeypatch)

    _process(
        _candidate(),
        api_keys=["key-1"],
        max_workers=3,
    )

    assert calls[0]["exchange_fetch_executor"] is None


def test_no_api_keys_does_not_build_an_executor(monkeypatch):
    calls = _capture_acquire_calls(monkeypatch)

    _process(_candidate())

    assert calls[0]["exchange_fetch_executor"] is None


@pytest.mark.parametrize("persist,shadow", [(False, False), (False, True), (True, False)])
def test_change_flag_does_not_override_shadow_or_persist_controls(monkeypatch, persist, shadow):
    monkeypatch.setattr(Config, "ENABLE_ODDSPAPI_SIGNIFICANT_CHANGE_SNAPSHOTS", True)
    monkeypatch.setattr(Config, "ENABLE_ODDSPAPI_HISTORICAL_AS_OF_PERSIST", persist)
    monkeypatch.setattr(Config, "ENABLE_ODDSPAPI_HISTORICAL_AS_OF_SHADOW", shadow)
    calls = _capture_acquire_calls(monkeypatch)
    candidate = replace(_candidate(), minutes_until_start=0)
    _process(candidate)
    assert calls[0]["attach_as_of"] is persist
    assert calls[0]["as_of_moments"] == (list(Config.PRE_START_ODDS_MOMENTS) if persist or shadow else None)


def test_custom_pipeline_never_builds_an_executor_even_with_multiple_keys():
    """A caller-supplied fetcher/acquisition_service marks a custom
    test/pipeline path; we must not silently spin up real OddsPapiClient
    instances behind it."""
    captured: list[dict] = []

    class _CapturingAcquisitionService:
        def acquire(self, fixture_id, **kwargs):
            captured.append({"fixture_id": fixture_id, **kwargs})
            return OddspapiOddsAcquisitionResult()

    processor = OddspapiPreStartOddsBatchProcessor(
        acquisition_service=_CapturingAcquisitionService(),
    )

    processor.process(
        [_candidate()],
        bookmakers=["pinnacle"],
        exchange_bookmakers=["betfair-ex"],
        market_mapping_index={},
        api_keys=["key-1", "key-2"],
        max_workers=2,
    )

    # This processor was built with a custom acquisition_service, so
    # _custom_pipeline is True and the executor must stay disabled.
    assert captured[0]["exchange_fetch_executor"] is None
