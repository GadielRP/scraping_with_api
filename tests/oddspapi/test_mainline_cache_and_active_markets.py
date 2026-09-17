from __future__ import annotations

from infrastructure.persistence.repositories.market_mapping_repository import (
    CanonicalMarketResolution,
    CanonicalOutcomeResolution,
    MarketMappingIndex,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.mainline_outcome_extractor import (
    OddspapiMainlineOutcomeExtractor,
)
from modules.oddspapi.mainline_cache_ids import resolve_mainline_outcome_ids
from modules.oddspapi.quote_activity import should_skip_inactive_market
from modules.odds_ingestion.adapters.oddspapi_market_adapter import OddspapiMarketAdapter


FALLBACK = ("pinnacle", "bet365", "betfair-ex")


def _home_away_index() -> MarketMappingIndex:
    market = CanonicalMarketResolution(
        resolved=True,
        mapping_id=1,
        canonical_market_key="home_away_full_time",
        canonical_market_name="Home/Away",
        canonical_market_group="Main",
        canonical_market_period="Full Time",
        requires_choice_group=False,
    )
    return MarketMappingIndex(
        market_mappings={
            ("oddspapi", "13", "131"): market,
            ("oddspapi", None, "131"): market,
        },
        outcome_mappings={
            (1, "131"): CanonicalOutcomeResolution(
                resolved=True,
                canonical_choice_name="1",
            ),
            (1, "132"): CanonicalOutcomeResolution(
                resolved=True,
                canonical_choice_name="2",
            ),
        },
    )


def _bet365_payload(*, market_active: bool, include_main_line: bool) -> dict:
    player = {
        "active": True,
        "price": 1.9,
        "mainLine": True if include_main_line else None,
    }
    away = {
        "active": True,
        "price": 1.8,
        "mainLine": True if include_main_line else None,
    }
    if not include_main_line:
        player.pop("mainLine")
        away.pop("mainLine")
    return {
        "fixtureId": "fx-1",
        "sportId": 13,
        "bookmakerOdds": {
            "bet365": {
                "markets": {
                    "131": {
                        "marketActive": market_active,
                        "outcomes": {
                            "131": {"players": {"0": player}},
                            "132": {"players": {"0": away}},
                        },
                    }
                }
            }
        },
    }


def test_should_skip_inactive_market_honors_require_active_quotes():
    market = {"marketActive": False}
    assert should_skip_inactive_market(market, require_active_quotes=True) is True
    assert should_skip_inactive_market(market, require_active_quotes=False) is False
    assert should_skip_inactive_market({"marketActive": True}, require_active_quotes=True) is False


def test_resolve_prefers_own_cache_then_fallback_order():
    cache = {
        "bet365": {"999"},
        "pinnacle": {"131", "132"},
        "betfair-ex": {"1372"},
    }
    own_ids, own_source = resolve_mainline_outcome_ids("bet365", cache, FALLBACK)
    assert own_source == "bet365"
    assert own_ids == {"999"}

    ids, source = resolve_mainline_outcome_ids("bet365", {"pinnacle": {"131"}}, FALLBACK)
    assert source == "pinnacle"
    assert ids == {"131"}

    ids, source = resolve_mainline_outcome_ids(
        "pinnacle",
        {"betfair-ex": {"1372"}},
        FALLBACK,
    )
    assert source == "betfair-ex"
    assert ids == {"1372"}

    ids, source = resolve_mainline_outcome_ids("bet365", {"other": {"1"}}, FALLBACK)
    assert source is None
    assert ids == set()


def test_extractor_keeps_inactive_markets_when_require_active_quotes_is_false():
    payload = _bet365_payload(market_active=False, include_main_line=True)
    skipped = OddspapiMainlineOutcomeExtractor.extract(
        payload,
        require_active_quotes=True,
    )
    kept = OddspapiMainlineOutcomeExtractor.extract(
        payload,
        require_active_quotes=False,
    )
    assert skipped == []
    assert {(row["bookmaker_slug"], row["source_outcome_id"]) for row in kept} == {
        ("bet365", "131"),
        ("bet365", "132"),
    }


def test_adapter_persists_market_active_false_when_require_active_quotes_is_false():
    payload = _bet365_payload(market_active=False, include_main_line=True)
    skipped = OddspapiMarketAdapter.from_odds_response(
        payload,
        market_mapping_index=_home_away_index(),
        require_active_quotes=True,
    )
    kept = OddspapiMarketAdapter.from_odds_response(
        payload,
        market_mapping_index=_home_away_index(),
        require_active_quotes=False,
    )
    assert skipped.get("bookmakers") == []
    assert [book["slug"] for book in kept["bookmakers"]] == ["bet365"]
    assert kept["bookmakers"][0]["markets"][0]["choices"][0]["mainLine"] is True


def test_adapter_uses_bookmaker_own_cache_then_pinnacle_fallback():
    payload = _bet365_payload(market_active=True, include_main_line=False)
    mapping = _home_away_index()

    own = OddspapiMarketAdapter.from_odds_response(
        payload,
        market_mapping_index=mapping,
        use_mainline_cache=True,
        persist_main_line_only=True,
        require_active_quotes=False,
        mainline_outcome_ids_by_bookmaker={"bet365": {"131", "132"}},
        mainline_fallback_bookmakers=FALLBACK,
    )
    assert [book["slug"] for book in own["bookmakers"]] == ["bet365"]
    assert own.get("diagnostics", {}).get("mainline_cache_fallbacks_used") in (None, [])

    fallback = OddspapiMarketAdapter.from_odds_response(
        payload,
        market_mapping_index=mapping,
        use_mainline_cache=True,
        persist_main_line_only=True,
        require_active_quotes=False,
        mainline_outcome_ids_by_bookmaker={"pinnacle": {"131", "132"}},
        mainline_fallback_bookmakers=FALLBACK,
    )
    assert [book["slug"] for book in fallback["bookmakers"]] == ["bet365"]
    assert fallback["diagnostics"]["mainline_cache_fallbacks_used"] == [
        {"bookmakerSlug": "bet365", "cacheSourceSlug": "pinnacle"}
    ]


def test_adapter_skips_bookmaker_when_no_own_cache_or_fallback():
    payload = _bet365_payload(market_active=True, include_main_line=False)
    adapted = OddspapiMarketAdapter.from_odds_response(
        payload,
        market_mapping_index=_home_away_index(),
        use_mainline_cache=True,
        persist_main_line_only=True,
        require_active_quotes=False,
        mainline_outcome_ids_by_bookmaker={},
        mainline_fallback_bookmakers=FALLBACK,
    )
    assert adapted.get("bookmakers") == []
    assert adapted["diagnostics"]["skipped_missing_mainline_cache"] == [
        {
            "bookmakerSlug": "bet365",
            "fallbackPriority": ["pinnacle", "bet365", "betfair-ex"],
            "reason": "no_mainline_cache_or_fallback",
        }
    ]


def test_extractor_resolves_canonical_market_key():
    payload = _bet365_payload(market_active=True, include_main_line=True)
    mapping = _home_away_index()

    # With mapping index provided:
    with_mapping = OddspapiMainlineOutcomeExtractor.extract(
        payload,
        require_active_quotes=True,
        market_mapping_index=mapping,
        source_sport_id="13",
    )
    assert len(with_mapping) == 2
    for row in with_mapping:
        assert row["canonical_market_key"] == "home_away_full_time"
        assert row["bookmaker_slug"] == "bet365"

    # Without mapping index provided:
    without_mapping = OddspapiMainlineOutcomeExtractor.extract(
        payload,
        require_active_quotes=True,
    )
    assert len(without_mapping) == 2
    for row in without_mapping:
        assert row["canonical_market_key"] is None

    # Unmapped markets are skipped when mapping index is provided:
    payload_with_unmapped = {
        "fixtureId": "fx-1",
        "sportId": 13,
        "bookmakerOdds": {
            "bet365": {
                "markets": {
                    "131": {
                        "marketActive": True,
                        "outcomes": {
                            "131": {"players": {"0": {"mainLine": True, "price": 1.9}}},
                        },
                    },
                    "9999": {
                        "marketActive": True,
                        "outcomes": {
                            "9999": {"players": {"0": {"mainLine": True, "price": 2.1}}},
                        },
                    },
                }
            }
        },
    }
    extracted = OddspapiMainlineOutcomeExtractor.extract(
        payload_with_unmapped,
        require_active_quotes=True,
        market_mapping_index=mapping,
        source_sport_id="13",
    )
    assert len(extracted) == 1
    assert extracted[0]["source_market_id"] == "131"
    assert extracted[0]["canonical_market_key"] == "home_away_full_time"


def test_get_exchange_mainline_selections_filters_by_allowed_keys():
    from unittest.mock import MagicMock, patch
    from infrastructure.persistence.repositories.oddspapi_mainline_cache_repository import (
        OddspapiMainlineCacheRepository,
    )

    mock_row_1 = MagicMock(
        bookmaker_slug="betfair-ex",
        source_market_id="101",
        source_outcome_id="101",
        canonical_market_key="1x2_full_time",
    )

    mock_session = MagicMock()
    mock_query = mock_session.query.return_value
    mock_filter1 = mock_query.filter.return_value
    mock_filter2 = mock_filter1.filter.return_value
    mock_order = mock_filter2.order_by.return_value
    mock_order.all.return_value = [mock_row_1]

    with patch("infrastructure.persistence.repositories.oddspapi_mainline_cache_repository.db_manager.get_session") as get_session_mock:
        get_session_mock.return_value.__enter__.return_value = mock_session

        selections = OddspapiMainlineCacheRepository.get_exchange_mainline_selections(
            event_id=123,
            exchange_bookmakers=["betfair-ex"],
            allowed_market_keys=["1x2_full_time"],
        )

        assert len(selections) == 1
        assert selections[0]["canonical_market_key"] == "1x2_full_time"
        assert selections[0]["source_outcome_id"] == "101"
        assert mock_filter1.filter.called


def test_acquire_live_forwards_exchange_market_keys():
    from unittest.mock import MagicMock
    from modules.jobs.pre_start_check_job.providers.oddspapi.odds_acquisition_service import (
        OddspapiPreStartOddsAcquisitionService,
    )

    service = OddspapiPreStartOddsAcquisitionService()
    service.mainline_cache_repository = MagicMock()
    service.mainline_cache_repository.event_ids_with_cache.return_value = {123}
    service.mainline_cache_repository.get_exchange_mainline_selections.return_value = []

    service.acquire(
        fixture_id="fx-1",
        event_id=123,
        source_sport_id=10,
        minutes_until_start=0,
        is_live=True,
        regular_bookmakers=[],
        exchange_bookmakers=["betfair-ex"],
        market_mapping_index=MagicMock(),
        exchange_market_keys=["1x2_full_time"],
        exchange_main_line_only=True,
        exchange_include_player_props=False,
        exchange_historical_moments=[120],
        exchange_max_outcomes_per_event=8,
        exchange_request_budget=40,
        minimum_initial_span_minutes=60.0,
        current_odds_available=False,
    )

    service.mainline_cache_repository.get_exchange_mainline_selections.assert_called_once_with(
        123,
        ["betfair-ex"],
        allowed_market_keys=["1x2_full_time"],
    )


