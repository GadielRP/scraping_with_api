from threading import Barrier, Lock
from types import SimpleNamespace

from infrastructure.persistence.repositories.market_mapping_repository import (
    CanonicalMarketResolution,
    CanonicalOutcomeResolution,
    MarketMappingIndex,
)
from modules.jobs.oddspapi.pre_start_odds.exchange_outcome_selector import (
    OddspapiExchangeOutcomeSelector,
)
from modules.jobs.oddspapi.pre_start_odds.odds_acquisition_service import (
    OddspapiPreStartOddsAcquisitionService,
)
from modules.jobs.oddspapi.pre_start_odds.odds_batch_processor import (
    OddspapiPreStartOddsBatchProcessor,
)
from modules.jobs.oddspapi.pre_start_odds.event_selector import (
    OddspapiPreStartCandidate,
)
from modules.odds_ingestion.fetch_result import OddsFetchResult


def _mapping_index(*, requires_choice_group=False):
    return MarketMappingIndex(
        market_mappings={
            (
                "oddspapi",
                "10",
                "101",
            ): CanonicalMarketResolution(
                resolved=True,
                mapping_id=1,
                canonical_market_key="1x2_full_time",
                canonical_market_name="1X2 Full Time",
                canonical_market_group="1X2",
                canonical_market_period="Full Time",
                market_family="side_3way",
                requires_choice_group=requires_choice_group,
                source_handicap="2.5" if requires_choice_group else None,
            )
        },
        outcome_mappings={
            (1, "101"): CanonicalOutcomeResolution(
                resolved=True,
                canonical_choice_name="1",
                display_order=1,
            ),
            (1, "102"): CanonicalOutcomeResolution(
                resolved=True,
                canonical_choice_name="x",
                display_order=2,
            ),
        },
    )


def _player(price, *, main_line=False, player_name=None):
    return {
        "active": True,
        "price": price,
        "limit": 10,
        "mainLine": main_line,
        "playerName": player_name,
        "exchangeMeta": {
            "availableToBack": [{"price": price, "size": 10}],
            "availableToLay": [{"price": price + 0.1, "size": 8}],
        },
    }


def _current_response():
    return {
        "fixtureId": "fixture-1",
        "sportId": "10",
        "bookmakerOdds": {
            "pinnacle": {
                "markets": {
                    "101": {
                        "marketActive": True,
                        "outcomes": {
                            "101": {"players": {"0": _player(1.95)}},
                        },
                    }
                }
            },
            "betfair-ex": {
                "markets": {
                    "101": {
                        "marketActive": True,
                        "outcomes": {
                            "101": {"players": {"0": _player(2.1)}},
                            "102": {"players": {"0": _player(3.2)}},
                        },
                    }
                }
            },
        },
    }


def _historical_normalized(outcome_id, opening_price):
    return {
        "fixtureId": "fixture-1",
        "sportId": "10",
        "bookmakerOdds": {
            "betfair-ex": {
                "markets": {
                    "101": {
                        "marketActive": True,
                        "outcomes": {
                            str(outcome_id): {
                                "players": {
                                    "0": {
                                        "active": True,
                                        "price": opening_price + 0.2,
                                        "changedAt": "2026-07-01T12:00:00Z",
                                        "initialPrice": opening_price,
                                        "initialChangedAt": (
                                            "2026-07-01T10:00:00Z"
                                        ),
                                        "initialLimit": 12,
                                        "exchangeMeta": None,
                                    }
                                }
                            }
                        },
                    }
                }
            }
        },
    }


def _regular_historical_normalized():
    payload = _historical_normalized(101, 1.7)
    payload["bookmakerOdds"]["pinnacle"] = payload["bookmakerOdds"].pop(
        "betfair-ex"
    )
    return payload


def test_exchange_selector_uses_fixture_offers_and_canonical_market_keys():
    result = OddspapiExchangeOutcomeSelector.select(
        _current_response(),
        exchange_bookmakers=["betfair-ex"],
        market_mapping_index=_mapping_index(),
        allowed_market_keys=["1x2_full_time"],
        main_line_only=True,
        include_player_props=False,
        max_outcomes=8,
    )

    assert [
        (item.bookmaker_slug, item.source_outcome_id)
        for item in result.selections
    ] == [
        ("betfair-ex", "101"),
        ("betfair-ex", "102"),
    ]


def test_exchange_selector_requires_main_line_only_for_line_markets():
    result = OddspapiExchangeOutcomeSelector.select(
        _current_response(),
        exchange_bookmakers=["betfair-ex"],
        market_mapping_index=_mapping_index(requires_choice_group=True),
        allowed_market_keys=["1x2_full_time"],
        main_line_only=True,
        include_player_props=False,
        max_outcomes=8,
    )

    assert result.selections == []
    assert result.skipped_non_main_line == 2


def test_exchange_historical_runs_only_at_configured_key_moment():
    calls = []

    class Fetcher:
        def fetch_odds(self, fixture_id, **kwargs):
            calls.append((fixture_id, kwargs))
            if kwargs["endpoint"] == "odds":
                return OddsFetchResult.from_payload(_current_response())
            return OddsFetchResult.from_payload(
                _historical_normalized(
                    kwargs["outcome_id"],
                    1.8 if kwargs["outcome_id"] == 101 else 2.9,
                )
            )

    service = OddspapiPreStartOddsAcquisitionService(fetcher=Fetcher())
    common = {
        "fixture_id": "fixture-1",
        "source_sport_id": "10",
        "selected_endpoint": "odds",
        "regular_bookmakers": ["pinnacle"],
        "exchange_bookmakers": ["betfair-ex"],
        "market_mapping_index": _mapping_index(),
        "exchange_market_keys": ["1x2_full_time"],
        "exchange_main_line_only": True,
        "exchange_include_player_props": False,
        "exchange_historical_moments": [120],
        "exchange_max_outcomes_per_event": 8,
        "exchange_request_budget": 40,
        "minimum_initial_span_minutes": 60,
        "current_odds_available": True,
    }

    at_30 = service.acquire(minutes_until_start=30, **common)
    assert at_30.exchange_historical_requests_attempted == 0
    assert len(calls) == 1

    calls.clear()
    at_120 = service.acquire(minutes_until_start=120, **common)
    assert at_120.exchange_historical_requests_attempted == 2
    assert [call[1]["endpoint"] for call in calls] == [
        "odds",
        "historical-odds",
        "historical-odds",
    ]
    assert calls[0][1]["bookmakers"] == ["pinnacle", "betfair-ex"]
    assert [
        call[1]["outcome_id"]
        for call in calls[1:]
    ] == [101, 102]

    player = (
        at_120.payload["bookmakerOdds"]["betfair-ex"]["markets"]["101"]
        ["outcomes"]["101"]["players"]["0"]
    )
    assert player["price"] == 2.1
    assert player["initialPrice"] == 1.8
    assert player["exchangeMeta"]["availableToLay"][0]["price"] == 2.2


def test_historical_mode_enriches_regular_opening_and_preserves_current_price():
    calls = []

    class Fetcher:
        def fetch_odds(self, fixture_id, **kwargs):
            calls.append(kwargs)
            if kwargs["endpoint"] == "odds":
                return OddsFetchResult.from_payload(_current_response())
            return OddsFetchResult.from_payload(
                _regular_historical_normalized()
            )

    result = OddspapiPreStartOddsAcquisitionService(
        fetcher=Fetcher()
    ).acquire(
        "fixture-1",
        source_sport_id="10",
        minutes_until_start=30,
        selected_endpoint="historical-odds",
        regular_bookmakers=["pinnacle"],
        exchange_bookmakers=["betfair-ex"],
        market_mapping_index=_mapping_index(),
        exchange_market_keys=["1x2_full_time"],
        exchange_main_line_only=True,
        exchange_include_player_props=False,
        exchange_historical_moments=[120],
        exchange_max_outcomes_per_event=8,
        exchange_request_budget=40,
        minimum_initial_span_minutes=60,
        current_odds_available=True,
    )

    assert [
        (call["endpoint"], call["bookmakers"])
        for call in calls
    ] == [
        ("odds", ["pinnacle", "betfair-ex"]),
        ("historical-odds", ["pinnacle"]),
    ]
    player = (
        result.payload["bookmakerOdds"]["pinnacle"]["markets"]["101"]
        ["outcomes"]["101"]["players"]["0"]
    )
    assert player["price"] == 1.95
    assert player["initialPrice"] == 1.7


def test_historical_mode_without_exchange_uses_only_historical_endpoint():
    calls = []

    class Fetcher:
        def fetch_odds(self, fixture_id, **kwargs):
            calls.append(kwargs)
            return OddsFetchResult.from_payload(
                _regular_historical_normalized()
            )

    result = OddspapiPreStartOddsAcquisitionService(
        fetcher=Fetcher()
    ).acquire(
        "fixture-1",
        source_sport_id="10",
        minutes_until_start=120,
        selected_endpoint="historical-odds",
        regular_bookmakers=["pinnacle", "bet365"],
        exchange_bookmakers=None,
        market_mapping_index=_mapping_index(),
        exchange_market_keys=None,
        exchange_main_line_only=True,
        exchange_include_player_props=False,
        exchange_historical_moments=[120],
        exchange_max_outcomes_per_event=8,
        exchange_request_budget=40,
        minimum_initial_span_minutes=60,
        current_odds_available=True,
    )

    assert [
        (call["endpoint"], call["bookmakers"])
        for call in calls
    ] == [
        ("historical-odds", ["pinnacle", "bet365"]),
    ]
    assert result.http_requests_attempted == 1
    player = (
        result.payload["bookmakerOdds"]["pinnacle"]["markets"]["101"]
        ["outcomes"]["101"]["players"]["0"]
    )
    assert player["price"] == 1.9
    assert player["initialPrice"] == 1.7


def test_historical_mode_skips_current_and_exchange_at_non_positive_moments():
    calls = []

    class Fetcher:
        def fetch_odds(self, fixture_id, **kwargs):
            calls.append(kwargs)
            return OddsFetchResult.from_payload(
                _regular_historical_normalized()
            )

    service = OddspapiPreStartOddsAcquisitionService(fetcher=Fetcher())
    common = {
        "fixture_id": "fixture-1",
        "source_sport_id": "10",
        "selected_endpoint": "historical-odds",
        "regular_bookmakers": ["pinnacle"],
        "exchange_bookmakers": ["betfair-ex"],
        "market_mapping_index": _mapping_index(),
        "exchange_market_keys": ["1x2_full_time"],
        "exchange_main_line_only": True,
        "exchange_include_player_props": False,
        "exchange_historical_moments": [120],
        "exchange_max_outcomes_per_event": 8,
        "exchange_request_budget": 40,
        "minimum_initial_span_minutes": 60,
        "current_odds_available": True,
    }

    for minutes_until_start in (0, -5):
        calls.clear()
        result = service.acquire(
            minutes_until_start=minutes_until_start,
            **common,
        )

        assert [
            (call["endpoint"], call["bookmakers"])
            for call in calls
        ] == [
            ("historical-odds", ["pinnacle"]),
        ]
        assert result.http_requests_attempted == 1
        assert result.exchange_historical_requests_attempted == 0


def test_two_api_keys_process_two_events_concurrently_with_bounded_clients():
    barrier = Barrier(2)
    lock = Lock()
    calls = []
    clients = []
    active_requests = 0
    maximum_active_requests = 0

    class Client:
        def __init__(self, api_key):
            self.api_key = api_key
            self.closed = False
            clients.append(self)

        def get_historical_odds(self, fixture_id, **_kwargs):
            nonlocal active_requests, maximum_active_requests
            with lock:
                active_requests += 1
                maximum_active_requests = max(
                    maximum_active_requests,
                    active_requests,
                )
                calls.append((self.api_key, fixture_id))
            try:
                barrier.wait(timeout=2)
                return {
                    "fixtureId": fixture_id,
                    "bookmakers": {},
                }
            finally:
                with lock:
                    active_requests -= 1

        def close(self):
            self.closed = True

    class IngestionService:
        @staticmethod
        def save_from_oddspapi_response(*_args, **_kwargs):
            return SimpleNamespace(skipped=False)

    candidates = [
        OddspapiPreStartCandidate(
            event_id=1,
            fixture_id="fixture-1",
            minutes_until_start=120,
            has_odds=True,
            source_sport_id="10",
        ),
        OddspapiPreStartCandidate(
            event_id=99,
            fixture_id=None,
            minutes_until_start=120,
        ),
        OddspapiPreStartCandidate(
            event_id=2,
            fixture_id="fixture-2",
            minutes_until_start=120,
            has_odds=True,
            source_sport_id="10",
        ),
    ]
    processor = OddspapiPreStartOddsBatchProcessor(
        ingestion_service=IngestionService,
        client_factory=Client,
    )

    summary = processor.process(
        candidates,
        bookmakers=["pinnacle", "bet365"],
        endpoint="historical-odds",
        api_keys=["key-1", "key-2"],
        max_workers=2,
        market_mapping_index=_mapping_index(),
    )

    assert maximum_active_requests == 2
    assert sorted(calls) == [
        ("key-1", "fixture-1"),
        ("key-2", "fixture-2"),
    ]
    assert summary.responses_received == 2
    assert summary.events_ingested == 2
    assert summary.events_skipped == 1
    assert [result.event_id for result in summary.results] == [1, 99, 2]
    assert summary.results[1].skip_reason == "missing_oddspapi_mapping"
    assert len(clients) == 2
    assert all(client.closed for client in clients)


def test_exchange_request_budget_truncates_outcomes():
    calls = []

    class Fetcher:
        def fetch_odds(self, fixture_id, **kwargs):
            calls.append(kwargs)
            if kwargs["endpoint"] == "odds":
                return OddsFetchResult.from_payload(_current_response())
            return OddsFetchResult.from_payload(
                _historical_normalized(kwargs["outcome_id"], 1.8)
            )

    result = OddspapiPreStartOddsAcquisitionService(
        fetcher=Fetcher()
    ).acquire(
        "fixture-1",
        source_sport_id="10",
        minutes_until_start=120,
        selected_endpoint="odds",
        regular_bookmakers=["pinnacle"],
        exchange_bookmakers=["betfair-ex"],
        market_mapping_index=_mapping_index(),
        exchange_market_keys=["1x2_full_time"],
        exchange_main_line_only=True,
        exchange_include_player_props=False,
        exchange_historical_moments=[120],
        exchange_max_outcomes_per_event=8,
        exchange_request_budget=1,
        minimum_initial_span_minutes=60,
        current_odds_available=True,
    )

    assert result.exchange_historical_requests_attempted == 1
    assert result.exchange_outcomes_skipped_budget == 1


def test_exhausted_exchange_request_budget_makes_no_historical_calls():
    calls = []

    class Fetcher:
        def fetch_odds(self, fixture_id, **kwargs):
            calls.append(kwargs)
            return OddsFetchResult.from_payload(_current_response())

    result = OddspapiPreStartOddsAcquisitionService(
        fetcher=Fetcher()
    ).acquire(
        "fixture-1",
        source_sport_id="10",
        minutes_until_start=120,
        selected_endpoint="odds",
        regular_bookmakers=["pinnacle"],
        exchange_bookmakers=["betfair-ex"],
        market_mapping_index=_mapping_index(),
        exchange_market_keys=["1x2_full_time"],
        exchange_main_line_only=True,
        exchange_include_player_props=False,
        exchange_historical_moments=[120],
        exchange_max_outcomes_per_event=8,
        exchange_request_budget=0,
        minimum_initial_span_minutes=60,
        current_odds_available=True,
    )

    assert [call["endpoint"] for call in calls] == ["odds"]
    assert result.exchange_historical_requests_attempted == 0
    assert result.exchange_outcomes_skipped_budget == 2
