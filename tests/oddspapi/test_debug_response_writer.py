"""Unit tests for OddspapiDebugResponseWriter."""

from __future__ import annotations

import json
from pathlib import Path

from modules.jobs.pre_start_check_job.providers.oddspapi.debug_response_writer import (
    OddspapiDebugResponseWriter,
)


def test_debug_response_writer_with_minutes_until_start(tmp_path, monkeypatch):
    monkeypatch.setattr(OddspapiDebugResponseWriter, "OUTPUT_DIRECTORY", tmp_path)

    payload = {"fixtureId": "id12345", "bookmakerOdds": {}}
    saved_path = OddspapiDebugResponseWriter.save(
        event_id=207699,
        fixture_id="id1000000872478460",
        bookmakers=["pinnacle", "bet365", "betfair-ex"],
        payload=payload,
        endpoint="odds",
        minutes_until_start=5,
        home_participant="Real Madrid",
        away_participant="Real Sociedad",
    )

    assert saved_path is not None
    assert saved_path.parent.name == "207699_Real_Madrid_Real_Sociedad"
    assert saved_path.name == "207699_id1000000872478460_t_5_odds_pinnacle_bet365_betfair-ex.json"
    assert saved_path.exists()

    with saved_path.open(encoding="utf-8") as f:
        loaded = json.load(f)
        assert loaded == payload


def test_debug_response_writer_without_minutes_until_start(tmp_path, monkeypatch):
    monkeypatch.setattr(OddspapiDebugResponseWriter, "OUTPUT_DIRECTORY", tmp_path)

    payload = {"fixtureId": "id12345"}
    saved_path = OddspapiDebugResponseWriter.save(
        event_id=207699,
        fixture_id="id1000000872478460",
        bookmakers=["pinnacle", "bet365"],
        payload=payload,
        endpoint="historical-odds",
        minutes_until_start=None,
    )

    assert saved_path is not None
    assert saved_path.parent.name == "207699"
    assert saved_path.name == "207699_id1000000872478460_historical_pinnacle_bet365.json"
    assert saved_path.exists()


def test_acquisition_service_captures_raw_for_odds_when_save_odds_responses_enabled(monkeypatch):
    from infrastructure.settings.config import Config
    from modules.jobs.pre_start_check_job.providers.oddspapi.odds_acquisition_service import (
        OddspapiPreStartOddsAcquisitionService,
    )
    from modules.jobs.pre_start_check_job.providers.oddspapi.odds_fetcher import (
        OddspapiOddsFetcher,
    )
    from infrastructure.persistence.repositories.market_mapping_repository import (
        MarketMappingIndex,
    )
    from modules.odds_ingestion.fetch_result import OddsFetchResult

    monkeypatch.setattr(Config, "ENABLE_ODDSPAPI_SAVE_ODDS_RESPONSES", True)

    fetched_capture_raw = []

    def mock_fetch(self, fixture_id, **kwargs):
        fetched_capture_raw.append(kwargs.get("capture_raw_response"))
        return OddsFetchResult.from_payload(
            {"fixtureId": fixture_id},
            raw_payload={"fixtureId": fixture_id, "raw": True},
        )

    monkeypatch.setattr(OddspapiOddsFetcher, "fetch_odds", mock_fetch)

    from unittest.mock import MagicMock

    mock_repo = MagicMock()
    mock_repo.event_ids_with_cache.return_value = {100}
    mock_repo.get_exchange_mainline_selections.return_value = []

    service = OddspapiPreStartOddsAcquisitionService(
        fetcher=OddspapiOddsFetcher(client=None),
        mainline_cache_repository=mock_repo,
    )

    # 1. Non-live acquisition (/odds) with debug_mode=False:
    # Must capture raw response because ENABLE_ODDSPAPI_SAVE_ODDS_RESPONSES is True
    res_non_live = service.acquire(
        fixture_id="fix-1",
        event_id=100,
        source_sport_id="10",
        minutes_until_start=30,
        is_live=False,
        regular_bookmakers=["pinnacle"],
        exchange_bookmakers=[],
        market_mapping_index=MarketMappingIndex({}, {}),
        exchange_market_keys=[],
        exchange_main_line_only=True,
        exchange_include_player_props=False,
        exchange_historical_moments=[],
        exchange_max_outcomes_per_event=10,
        exchange_request_budget=None,
        minimum_initial_span_minutes=0.0,
        current_odds_available=True,
        debug_mode=False,
    )
    assert fetched_capture_raw[-1] is True
    assert res_non_live.debug_raw_payload == {"fixtureId": "fix-1", "raw": True}

    # 2. Live acquisition (/historical-odds) with debug_mode=True:
    # Must NOT capture historical raw response when ENABLE_ODDSPAPI_SAVE_ODDS_RESPONSES is True
    res_live = service.acquire(
        fixture_id="fix-1",
        event_id=100,
        source_sport_id="10",
        minutes_until_start=0,
        is_live=True,
        regular_bookmakers=["pinnacle"],
        exchange_bookmakers=[],
        market_mapping_index=MarketMappingIndex({}, {}),
        exchange_market_keys=[],
        exchange_main_line_only=True,
        exchange_include_player_props=False,
        exchange_historical_moments=[],
        exchange_max_outcomes_per_event=10,
        exchange_request_budget=None,
        minimum_initial_span_minutes=0.0,
        current_odds_available=False,
        debug_mode=True,
    )
    assert fetched_capture_raw[-1] is False


def test_batch_processor_saves_only_odds_when_save_odds_responses_enabled(monkeypatch):
    from unittest.mock import MagicMock
    from infrastructure.settings.config import Config
    from modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor import (
        OddspapiPreStartOddsBatchProcessor,
    )
    from modules.jobs.pre_start_check_job.providers.oddspapi.event_selector import (
        OddspapiPreStartCandidate,
    )
    from modules.jobs.pre_start_check_job.providers.oddspapi.constants import (
        ODDSPAPI_CURRENT_ODDS_ENDPOINT,
        ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
    )
    from modules.jobs.pre_start_check_job.providers.oddspapi.odds_acquisition_service import (
        OddspapiOddsAcquisitionResult,
    )

    monkeypatch.setattr(Config, "ENABLE_ODDSPAPI_SAVE_ODDS_RESPONSES", True)

    saved_calls = []

    def mock_save(**kwargs):
        saved_calls.append(kwargs)

    monkeypatch.setattr(OddspapiDebugResponseWriter, "save", mock_save)

    # Candidate 1: non-live (30 mins until start) -> /odds
    cand_1 = OddspapiPreStartCandidate(
        event_id=101,
        fixture_id="fix-101",
        minutes_until_start=30,
        source_sport_id="10",
        home_participant="Team A",
        away_participant="Team B",
    )
    # Candidate 2: live (0 mins until start) -> /historical-odds
    cand_2 = OddspapiPreStartCandidate(
        event_id=102,
        fixture_id="fix-102",
        minutes_until_start=0,
        source_sport_id="10",
    )

    mock_ingestion_service = MagicMock()
    mock_ingestion_service.event_ids_with_cache.return_value = {101, 102}

    def mock_acquire(*args, **kwargs):
        fixture_id = args[0] if args else kwargs.get("fixture_id")
        endpoint = (
            ODDSPAPI_HISTORICAL_ODDS_ENDPOINT
            if kwargs.get("is_live")
            else ODDSPAPI_CURRENT_ODDS_ENDPOINT
        )
        return OddspapiOddsAcquisitionResult(
            payload={"fixtureId": fixture_id},
            debug_raw_payload={"fixtureId": fixture_id, "raw": True},
            debug_endpoint=endpoint,
            debug_bookmakers=["pinnacle"],
        )

    mock_acquisition_service = MagicMock()
    mock_acquisition_service.acquire.side_effect = mock_acquire

    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=MagicMock(),
        ingestion_service=mock_ingestion_service,
        acquisition_service=mock_acquisition_service,
    )

    # Process with debug_mode=False
    processor.process(
        [cand_1, cand_2],
        bookmakers=["pinnacle"],
        market_mapping_index=MagicMock(),
        dry_run=False,
        debug_mode=False,
    )

    # Only candidate 1 (/odds) must be saved via OddspapiDebugResponseWriter.save
    assert len(saved_calls) == 1
    assert saved_calls[0]["event_id"] == 101
    assert saved_calls[0]["endpoint"] == ODDSPAPI_CURRENT_ODDS_ENDPOINT
    assert saved_calls[0]["minutes_until_start"] == 30
    assert saved_calls[0]["home_participant"] == "Team A"
    assert saved_calls[0]["away_participant"] == "Team B"


