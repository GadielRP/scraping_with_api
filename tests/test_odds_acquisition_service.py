"""Unit tests for OddsPapi dual-endpoint pre-start acquisition."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from dataclasses import replace

import pytest

from infrastructure.settings.config import Config
from modules.jobs.pre_start_check_job.providers.oddspapi import odds_acquisition_service as acquisition_module
from tests.test_historical_odds_change_detector import KICKOFF, tick

from modules.jobs.pre_start_check_job.providers.oddspapi.constants import (
    ODDSPAPI_CURRENT_ODDS_ENDPOINT,
    ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.exchange_outcome_selector import (
    ExchangeHistoricalSelection,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.odds_acquisition_service import (
    OddspapiOddsAcquisitionResult,
    OddspapiPreStartOddsAcquisitionService,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.odds_fetcher import (
    OddspapiOddsFetcher,
)
from modules.odds_ingestion.fetch_result import OddsFetchResult


def _current_payload():
    return {
        "fixtureId": "fixture-1",
        "sportId": "10",
        "bookmakerOdds": {
            "pinnacle": {
                "slug": "pinnacle",
                "markets": {
                    "101": {
                        "marketActive": True,
                        "outcomes": {
                            "201": {
                                "players": {
                                    "0": {
                                        "price": 1.9,
                                        "active": True,
                                        "mainLine": True,
                                    }
                                }
                            }
                        },
                    }
                },
            },
            "betfair-ex": {
                "slug": "betfair-ex",
                "markets": {
                    "102": {
                        "marketActive": True,
                        "outcomes": {
                            "301": {
                                "players": {
                                    "0": {
                                        "price": 2.1,
                                        "active": True,
                                        "mainLine": True,
                                        "exchangeMeta": {"lays": []},
                                    }
                                }
                            }
                        },
                    }
                },
            },
        },
    }


def _historical_payload():
    return {
        "fixtureId": "fixture-1",
        "sportId": "10",
        "bookmakerOdds": {
            "pinnacle": {
                "markets": {
                    "101": {
                        "marketActive": True,
                        "outcomes": {
                            "201": {
                                "players": {
                                    "0": {
                                        "price": 1.85,
                                        "active": True,
                                        "initialPrice": 2.05,
                                        "initialChangedAt": "2026-08-01T10:00:00Z",
                                    }
                                }
                            }
                        },
                    }
                },
            }
        },
    }


class _RecordingFetcher:
    def __init__(self):
        self.calls = []

    def fetch_odds(self, fixture_id, **kwargs):
        self.calls.append({"fixture_id": fixture_id, **kwargs})
        endpoint = kwargs.get("endpoint")
        if endpoint == ODDSPAPI_CURRENT_ODDS_ENDPOINT:
            return OddsFetchResult.from_payload(_current_payload())
        if kwargs.get("outcome_id") is not None:
            return OddsFetchResult.from_payload(
                {
                    "fixtureId": "fixture-1",
                    "sportId": "10",
                    "bookmakerOdds": {
                        "betfair-ex": {
                            "markets": {
                                "102": {
                                    "marketActive": True,
                                    "outcomes": {
                                        "301": {
                                            "players": {
                                                "0": {
                                                    "price": 2.2,
                                                    "active": True,
                                                    "initialPrice": 2.4,
                                                }
                                            }
                                        }
                                    },
                                }
                            }
                        }
                    },
                }
            )
        return OddsFetchResult.from_payload(_historical_payload())


class _FakeMainlineCache:
    saved = []
    exchange_selections = [
        {
            "bookmaker_slug": "betfair-ex",
            "source_market_id": "102",
            "source_outcome_id": "301",
            "canonical_market_key": "1x2_full_time",
        }
    ]

    @staticmethod
    def event_ids_with_cache(event_ids):
        return {
            int(event_id)
            for event_id in (event_ids or [])
            if event_id is not None
        }

    @staticmethod
    def save_mainline_outcomes(event_id, fixture_id, source_sport_id, mainline_outcomes):
        _FakeMainlineCache.saved.append(
            {
                "event_id": event_id,
                "fixture_id": fixture_id,
                "source_sport_id": source_sport_id,
                "mainline_outcomes": mainline_outcomes,
            }
        )
        return len(mainline_outcomes)

    @staticmethod
    def get_exchange_mainline_selections(event_id, exchange_bookmakers, *, allowed_market_keys=None):
        return list(_FakeMainlineCache.exchange_selections)


def _acquire_kwargs(**overrides):
    values = {
        "event_id": 99,
        "source_sport_id": "10",
        "minutes_until_start": 120,
        "is_live": False,
        "enable_exchange_historical": True,
        "regular_bookmakers": ["pinnacle"],
        "exchange_bookmakers": ["betfair-ex"],
        "market_mapping_index": None,
        "exchange_market_keys": ["1x2_full_time"],
        "exchange_main_line_only": True,
        "exchange_include_player_props": False,
        "exchange_historical_moments": [120],
        "exchange_max_outcomes_per_event": 8,
        "exchange_request_budget": 40,
        "minimum_initial_span_minutes": 60.0,
        "current_odds_available": True,
        "debug_mode": False,
    }
    values.update(overrides)
    return values


@pytest.mark.parametrize("attach", [False, True])
def test_live_significant_changes_preserve_persistence_control(monkeypatch, attach):
    monkeypatch.setattr(Config, "ENABLE_ODDSPAPI_SIGNIFICANT_CHANGE_SNAPSHOTS", True)
    raw = _raw_as_of_historical_payload()
    ticks = [tick(1440, 2), tick(5, 2.5), tick(1, 3.1), tick(0, 9, active=False), tick(-1, 10)]
    raw["bookmakers"]["pinnacle"]["markets"]["101"]["outcomes"]["201"]["players"]["0"] = [q for _, q in ticks]
    client = SimpleNamespace(get_historical_odds=lambda **kwargs: raw)
    service = OddspapiPreStartOddsAcquisitionService(
        fetcher=OddspapiOddsFetcher(client=client), mainline_cache_repository=_FakeMainlineCache,
    )
    result = service.acquire("fixture-1", **_acquire_kwargs(
        is_live=True, start_time_utc=KICKOFF, exchange_bookmakers=None,
        as_of_moments=[120, 5, 0], attach_as_of=attach,
        require_active_quotes=False, filter_post_kickoff_ticks=False,
    ))
    player = result.payload["bookmakerOdds"]["pinnacle"]["markets"]["101"]["outcomes"]["201"]["players"]["0"]
    assert player["price"] == 3.1
    assert [q.price for q in result.as_of_quotes] == [2.5, 3.1]
    assert ("momentQuotes" in player) is attach


def test_forced_significant_change_routes_non_live_candidate_to_historical_lane(monkeypatch):
    service = OddspapiPreStartOddsAcquisitionService(
        fetcher=OddspapiOddsFetcher(client=SimpleNamespace()),
        mainline_cache_repository=_FakeMainlineCache,
    )
    calls = []

    def fake_live(*args, **kwargs):
        calls.append(("historical", kwargs))
        return OddspapiOddsAcquisitionResult()

    def fake_pre_start(*args, **kwargs):
        calls.append(("classic", kwargs))
        return OddspapiOddsAcquisitionResult()

    monkeypatch.setattr(service, "_acquire_live", fake_live)
    monkeypatch.setattr(service, "_acquire_pre_start", fake_pre_start)

    service.acquire(
        "fixture-1",
        **_acquire_kwargs(
            minutes_until_start=5,
            is_live=False,
            start_time_utc=KICKOFF,
            force_significant_changes=True,
        ),
    )

    assert [kind for kind, _ in calls] == ["classic", "historical"]
    assert calls[1][1]["force_significant_changes"] is True


def test_forced_significant_change_overrides_global_flag(monkeypatch):
    monkeypatch.setattr(Config, "ENABLE_ODDSPAPI_SIGNIFICANT_CHANGE_SNAPSHOTS", False)
    fetcher = _RecordingFetcher()
    service = OddspapiPreStartOddsAcquisitionService(
        fetcher=fetcher,
        mainline_cache_repository=_FakeMainlineCache,
    )

    service.acquire(
        "fixture-1",
        **_acquire_kwargs(
            minutes_until_start=5,
            is_live=False,
            start_time_utc=KICKOFF,
            exchange_bookmakers=None,
            current_odds_available=False,
            force_significant_changes=True,
        ),
    )

    assert [call["endpoint"] for call in fetcher.calls] == [
        ODDSPAPI_CURRENT_ODDS_ENDPOINT,
        ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
    ]
    assert fetcher.calls[1]["enable_significant_changes"] is True


@pytest.mark.parametrize("concurrent", [False, True])
def test_live_propagates_custom_change_settings_to_regular_and_exchange(monkeypatch, concurrent):
    monkeypatch.setattr(Config, "ENABLE_ODDSPAPI_SIGNIFICANT_CHANGE_SNAPSHOTS", True)
    monkeypatch.setattr(acquisition_module, "ODDSPAPI_PRE_START_SETTINGS", replace(
        acquisition_module.ODDSPAPI_PRE_START_SETTINGS,
        significant_change_min_magnitude_pct=25,
        significant_change_min_history_hours=12,
        significant_change_flash_reversal_minutes=2,
        significant_change_min_price=1.02,
    ))
    rows = [dict(_FakeMainlineCache.exchange_selections[0], source_outcome_id=str(n)) for n in (301, 302)]
    cache = SimpleNamespace(
        event_ids_with_cache=lambda ids: set(ids),
        get_exchange_mainline_selections=lambda *args, **kwargs: rows,
    )
    calls = []
    def fetch_all(fixture_id, **kwargs):
        calls.append(kwargs)
        return []
    fetcher = _RecordingFetcher()
    service = OddspapiPreStartOddsAcquisitionService(fetcher=fetcher, mainline_cache_repository=cache)
    service.acquire("fixture-1", **_acquire_kwargs(
        is_live=True, start_time_utc=KICKOFF,
        exchange_fetch_executor=SimpleNamespace(fetch_all=fetch_all) if concurrent else None,
    ))
    assert len(fetcher.calls) == (1 if concurrent else 3)
    for call in fetcher.calls + calls:
        assert call["enable_significant_changes"] is True
        assert call["kickoff_utc"] == KICKOFF
        assert call["min_change_magnitude_pct"] == 25
        assert call["min_history_hours"] == 12
        assert call["flash_reversal_minutes"] == 2
        assert call["min_price"] == 1.02


def test_flag_does_not_activate_changes_for_pre_start(monkeypatch):
    monkeypatch.setattr(Config, "ENABLE_ODDSPAPI_SIGNIFICANT_CHANGE_SNAPSHOTS", True)
    fetcher = _RecordingFetcher()
    service = OddspapiPreStartOddsAcquisitionService(fetcher=fetcher, mainline_cache_repository=_FakeMainlineCache)
    service.acquire("fixture-1", **_acquire_kwargs(exchange_bookmakers=None))
    assert fetcher.calls
    assert all(call["enable_significant_changes"] is False for call in fetcher.calls)


def test_pre_start_opening_moment_fetches_odds_and_historical(monkeypatch):
    _FakeMainlineCache.saved = []
    fetcher = _RecordingFetcher()
    service = OddspapiPreStartOddsAcquisitionService(
        fetcher=fetcher,
        mainline_cache_repository=_FakeMainlineCache,
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi."
        "odds_acquisition_service.OddspapiExchangeOutcomeSelector.select",
        lambda *_args, **_kwargs: SimpleNamespace(
            selections=[
                ExchangeHistoricalSelection(
                    bookmaker_slug="betfair-ex",
                    source_market_id="102",
                    source_outcome_id="301",
                    canonical_market_key="1x2_full_time",
                )
            ],
            skipped_unmapped_markets=0,
            skipped_unmapped_outcomes=0,
            skipped_market_key=0,
            skipped_non_main_line=0,
            skipped_player_props=0,
            truncated=0,
        ),
    )

    result = service.acquire("fixture-1", **_acquire_kwargs(minutes_until_start=120))

    endpoints = [call["endpoint"] for call in fetcher.calls]
    assert endpoints.count(ODDSPAPI_CURRENT_ODDS_ENDPOINT) == 1
    assert endpoints.count(ODDSPAPI_HISTORICAL_ODDS_ENDPOINT) >= 2
    assert result.payload is not None
    assert result.mainline_outcomes_cached == 2
    assert _FakeMainlineCache.saved
    player = (
        result.payload["bookmakerOdds"]["pinnacle"]["markets"]["101"]["outcomes"]["201"][
            "players"
        ]["0"]
    )
    assert player["price"] == 1.9
    assert player["initialPrice"] == 2.05


def test_pre_start_non_opening_skips_regular_historical():
    _FakeMainlineCache.saved = []
    fetcher = _RecordingFetcher()
    service = OddspapiPreStartOddsAcquisitionService(
        fetcher=fetcher,
        mainline_cache_repository=_FakeMainlineCache,
    )

    result = service.acquire(
        "fixture-1",
        **_acquire_kwargs(
            minutes_until_start=30,
            exchange_bookmakers=None,
            enable_exchange_historical=False,
        ),
    )

    endpoints = [call["endpoint"] for call in fetcher.calls]
    assert endpoints == [ODDSPAPI_CURRENT_ODDS_ENDPOINT]
    assert result.payload is not None
    assert result.mainline_outcomes_cached == 2


def test_live_moment_uses_historical_and_cache_exchange_selections():
    fetcher = _RecordingFetcher()
    service = OddspapiPreStartOddsAcquisitionService(
        fetcher=fetcher,
        mainline_cache_repository=_FakeMainlineCache,
    )

    result = service.acquire(
        "fixture-1",
        **_acquire_kwargs(
            minutes_until_start=-5,
            is_live=True,
            exchange_bookmakers=["betfair-ex"],
        ),
    )

    endpoints = [call["endpoint"] for call in fetcher.calls]
    assert ODDSPAPI_CURRENT_ODDS_ENDPOINT not in endpoints
    assert endpoints[0] == ODDSPAPI_HISTORICAL_ODDS_ENDPOINT
    assert any(call.get("outcome_id") == 301 for call in fetcher.calls)
    assert result.exchange_outcomes_selected == 1
    assert "betfair-ex" in (result.payload or {}).get("bookmakerOdds", {})


def test_live_moment_skips_historical_when_mainline_cache_empty():
    fetcher = _RecordingFetcher()
    cache = SimpleNamespace(
        event_ids_with_cache=lambda _event_ids: set(),
        get_exchange_mainline_selections=lambda *_args, **_kwargs: (
            (_ for _ in ()).throw(
                AssertionError("exchange cache must not be read when empty")
            )
        ),
    )
    service = OddspapiPreStartOddsAcquisitionService(
        fetcher=fetcher,
        mainline_cache_repository=cache,
    )

    result = service.acquire(
        "fixture-1",
        **_acquire_kwargs(
            minutes_until_start=-5,
            is_live=True,
            exchange_bookmakers=["betfair-ex"],
        ),
    )

    assert fetcher.calls == []
    assert result.payload is None
    assert result.http_requests_attempted == 0
    assert result.exchange_historical_requests_attempted == 0


def _raw_as_of_historical_payload():
    return {
        "fixtureId": "fixture-1",
        "bookmakers": {
            "pinnacle": {
                "markets": {
                    "101": {
                        "outcomes": {
                            "201": {
                                "players": {
                                    "0": [
                                        {
                                            "createdAt": "2026-06-20T10:00:00Z",
                                            "price": 1.85,
                                            "active": True,
                                        },
                                        {
                                            "createdAt": "2026-06-20T11:50:00Z",
                                            "price": 1.91,
                                            "active": True,
                                        },
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        },
    }


def test_live_attach_as_of_quotes_onto_normalized_payload():
    client = SimpleNamespace(
        get_historical_odds=lambda **_kwargs: _raw_as_of_historical_payload(),
        get_odds=lambda **_kwargs: {},
    )
    service = OddspapiPreStartOddsAcquisitionService(
        fetcher=OddspapiOddsFetcher(client=client),
        mainline_cache_repository=_FakeMainlineCache,
    )
    result = service.acquire(
        "fixture-1",
        **_acquire_kwargs(
            minutes_until_start=-5,
            is_live=True,
            exchange_bookmakers=None,
            enable_exchange_historical=False,
            start_time_utc=datetime(2026, 6, 20, 12, 0, tzinfo=timezone.utc),
            as_of_moments=[120, 5],
            attach_as_of=True,
        ),
    )

    player = result.payload["bookmakerOdds"]["pinnacle"]["markets"]["101"]["outcomes"]["201"]["players"]["0"]
    by_moment = {
        item["minutesUntilStart"]: item["price"]
        for item in player["momentQuotes"]
    }
    assert by_moment[120] == 1.85
    assert by_moment[5] == 1.91
    assert len(result.as_of_quotes) == 2


def test_live_as_of_without_attach_does_not_mutate_ingest_payload():
    client = SimpleNamespace(
        get_historical_odds=lambda **_kwargs: _raw_as_of_historical_payload(),
        get_odds=lambda **_kwargs: {},
    )
    service = OddspapiPreStartOddsAcquisitionService(
        fetcher=OddspapiOddsFetcher(client=client),
        mainline_cache_repository=_FakeMainlineCache,
    )
    result = service.acquire(
        "fixture-1",
        **_acquire_kwargs(
            minutes_until_start=-5,
            is_live=True,
            exchange_bookmakers=None,
            enable_exchange_historical=False,
            start_time_utc=datetime(2026, 6, 20, 12, 0, tzinfo=timezone.utc),
            as_of_moments=[120, 5],
            attach_as_of=False,
        ),
    )

    player = result.payload["bookmakerOdds"]["pinnacle"]["markets"]["101"]["outcomes"]["201"]["players"]["0"]
    assert "momentQuotes" not in player
    assert len(result.as_of_quotes) == 2
    assert not hasattr(result, "raw_historical_payloads")


def test_enable_exchange_historical_false_skips_exchange_requests():
    fetcher = _RecordingFetcher()
    service = OddspapiPreStartOddsAcquisitionService(
        fetcher=fetcher,
        mainline_cache_repository=_FakeMainlineCache,
    )

    result = service.acquire(
        "fixture-1",
        **_acquire_kwargs(
            minutes_until_start=120,
            enable_exchange_historical=False,
            exchange_bookmakers=["betfair-ex"],
        ),
    )

    assert all(call.get("outcome_id") is None for call in fetcher.calls)
    assert result.exchange_historical_requests_attempted == 0
