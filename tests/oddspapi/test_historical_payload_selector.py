"""Comprehensive test suite for OddsPapi historical payload pre-selection.

Verifies execution order, isolation from normalizers/detectors, mainline cache
preservation, unmapped markets/outcomes pruning, completeness safety, and fallback handling.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from infrastructure.persistence.repositories.market_mapping_repository import (
    CanonicalMarketResolution,
    CanonicalOutcomeResolution,
    MarketMappingIndex,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.constants import (
    ODDSPAPI_CURRENT_ODDS_ENDPOINT,
    ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.historical_payload_selector import (
    HistoricalPayloadSelectionContext,
    HistoricalPayloadSelectionResult,
    HistoricalPayloadSelector,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.odds_acquisition_service import (
    OddspapiOddsAcquisitionResult,
    OddspapiPreStartOddsAcquisitionService,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.odds_fetcher import (
    OddspapiOddsFetcher,
)
from modules.odds_ingestion.adapters.oddspapi_market_adapter import OddspapiMarketAdapter
from modules.odds_ingestion.fetch_result import OddsFetchResult
from modules.oddspapi.historical_odds_reader import OddspapiHistoricalOddsReader


def _sample_tick(created_at: str = "2026-06-20T10:00:00Z", price: float = 1.95):
    return {
        "price": price,
        "createdAt": created_at,
        "active": True,
    }


def _build_test_index():
    """Build a rich in-memory MarketMappingIndex for tests."""
    index = MarketMappingIndex(market_mappings={}, outcome_mappings={})

    # Market 101: 1X2 Full Time (mapping_id = 1)
    index.market_mappings[("oddspapi", "10", "101")] = CanonicalMarketResolution(
        resolved=True,
        mapping_id=1,
        canonical_market_key="1x2_full_time",
        canonical_market_name="1X2",
        canonical_market_group="1X2",
        canonical_market_period="Full Time",
    )
    index.outcome_mappings[(1, "101")] = CanonicalOutcomeResolution(
        resolved=True, canonical_choice_name="Home", display_order=1
    )
    index.outcome_mappings[(1, "102")] = CanonicalOutcomeResolution(
        resolved=True, canonical_choice_name="Draw", display_order=2
    )
    index.outcome_mappings[(1, "103")] = CanonicalOutcomeResolution(
        resolved=True, canonical_choice_name="Away", display_order=3
    )

    # Market 201: Over/Under 2.5 Full Time (mapping_id = 2)
    index.market_mappings[("oddspapi", "10", "201")] = CanonicalMarketResolution(
        resolved=True,
        mapping_id=2,
        canonical_market_key="over_under_full_time",
        canonical_market_name="Over/Under",
        canonical_market_group="Over/Under",
        canonical_market_period="Full Time",
        requires_choice_group=True,
        source_handicap="2.5",
    )
    index.outcome_mappings[(2, "201")] = CanonicalOutcomeResolution(
        resolved=True, canonical_choice_name="Over", display_order=1
    )
    index.outcome_mappings[(2, "202")] = CanonicalOutcomeResolution(
        resolved=True, canonical_choice_name="Under", display_order=2
    )

    # Market 202: Over/Under 3.5 Full Time (mapping_id = 3)
    index.market_mappings[("oddspapi", "10", "202")] = CanonicalMarketResolution(
        resolved=True,
        mapping_id=3,
        canonical_market_key="over_under_full_time",
        canonical_market_name="Over/Under",
        canonical_market_group="Over/Under",
        canonical_market_period="Full Time",
        requires_choice_group=True,
        source_handicap="3.5",
    )
    index.outcome_mappings[(3, "301")] = CanonicalOutcomeResolution(
        resolved=True, canonical_choice_name="Over", display_order=1
    )
    index.outcome_mappings[(3, "302")] = CanonicalOutcomeResolution(
        resolved=True, canonical_choice_name="Under", display_order=2
    )

    return index


# --------------------------------------------------------------------------
# 1. Mercado mapeado vs mercado no mapeado
# --------------------------------------------------------------------------
def test_mapped_vs_unmapped_market():
    index = _build_test_index()
    raw = {
        "fixtureId": "fix-1",
        "sportId": "10",
        "bookmakers": {
            "bet365": {
                "markets": {
                    "101": {
                        "outcomes": {
                            "101": {"players": {"0": [_sample_tick()]}},
                            "102": {"players": {"0": [_sample_tick()]}},
                            "103": {"players": {"0": [_sample_tick()]}},
                        }
                    },
                    "999_unmapped": {
                        "outcomes": {
                            "901": {"players": {"0": [_sample_tick()]}},
                        }
                    },
                }
            }
        },
    }
    raw_copy = deepcopy(raw)
    ctx = HistoricalPayloadSelectionContext(
        source_sport_id="10",
        market_mapping_index=index,
    )
    result = HistoricalPayloadSelector.select(raw, ctx)

    # El mercado no mapeado no llega al resultado
    assert "101" in result.payload["bookmakers"]["bet365"]["markets"]
    assert "999_unmapped" not in result.payload["bookmakers"]["bet365"]["markets"]
    assert result.selected_markets == 1
    assert result.skipped_unmapped_markets == 1
    assert result.raw_markets_seen == 2
    assert result.raw_outcomes_seen == 4

    # El payload original no se modifica
    assert raw == raw_copy


# --------------------------------------------------------------------------
# 2. Outcome mapeado vs outcome no mapeado
# --------------------------------------------------------------------------
def test_mapped_vs_unmapped_outcome():
    index = _build_test_index()
    raw = {
        "fixtureId": "fix-1",
        "sportId": "10",
        "bookmakers": {
            "pinnacle": {
                "markets": {
                    "101": {
                        "outcomes": {
                            "101": {"players": {"0": [_sample_tick()]}},
                            "999_unmapped_out": {"players": {"0": [_sample_tick()]}},
                        }
                    }
                }
            }
        },
    }
    ctx = HistoricalPayloadSelectionContext(
        source_sport_id="10",
        market_mapping_index=index,
    )
    result = HistoricalPayloadSelector.select(raw, ctx)

    outcomes = result.payload["bookmakers"]["pinnacle"]["markets"]["101"]["outcomes"]
    assert "101" in outcomes
    assert "999_unmapped_out" not in outcomes
    assert result.selected_outcomes == 1
    assert result.skipped_unmapped_outcomes == 1


# --------------------------------------------------------------------------
# 3. Filtro de mercados por canonical key, group y period
# --------------------------------------------------------------------------
def test_allowed_market_filters():
    index = _build_test_index()
    raw = {
        "fixtureId": "fix-1",
        "sportId": "10",
        "bookmakers": {
            "pinnacle": {
                "markets": {
                    "101": {
                        "outcomes": {
                            "101": {"players": {"0": [_sample_tick()]}},
                        }
                    },
                    "201": {
                        "outcomes": {
                            "201": {"players": {"0": [_sample_tick()]}},
                        }
                    },
                }
            }
        },
    }
    # Filtro por group: sólo 1X2
    ctx_1x2 = HistoricalPayloadSelectionContext(
        source_sport_id="10",
        market_mapping_index=index,
        allowed_market_groups=["1X2"],
    )
    res_1x2 = HistoricalPayloadSelector.select(raw, ctx_1x2)
    assert "101" in res_1x2.payload["bookmakers"]["pinnacle"]["markets"]
    assert "201" not in res_1x2.payload["bookmakers"]["pinnacle"]["markets"]

    # Filtro por key: sólo over_under_full_time
    ctx_ou = HistoricalPayloadSelectionContext(
        source_sport_id="10",
        market_mapping_index=index,
        allowed_market_keys=["over_under_full_time"],
    )
    res_ou = HistoricalPayloadSelector.select(raw, ctx_ou)
    assert "101" not in res_ou.payload["bookmakers"]["pinnacle"]["markets"]
    assert "201" in res_ou.payload["bookmakers"]["pinnacle"]["markets"]


# --------------------------------------------------------------------------
# 4. Mainline cache (dos líneas del mismo canonical market)
# --------------------------------------------------------------------------
def test_mainline_cache_two_lines_preserves_full_selected_market():
    index = _build_test_index()
    raw = {
        "fixtureId": "fix-1",
        "sportId": "10",
        "bookmakers": {
            "bet365": {
                "markets": {
                    "201": {  # line 2.5: outcomes 201 (Over) and 202 (Under)
                        "outcomes": {
                            "201": {"players": {"0": [_sample_tick(price=1.85)]}},
                            "202": {"players": {"0": [_sample_tick(price=1.95)]}},
                        }
                    },
                    "202": {  # line 3.5: outcomes 301 (Over) and 302 (Under)
                        "outcomes": {
                            "301": {"players": {"0": [_sample_tick(price=2.40)]}},
                            "302": {"players": {"0": [_sample_tick(price=1.55)]}},
                        }
                    },
                }
            }
        },
    }
    # Cache selects only line 2.5 (outcome "201")
    ctx = HistoricalPayloadSelectionContext(
        source_sport_id="10",
        market_mapping_index=index,
        use_mainline_cache=True,
        persist_main_line_only=True,
        mainline_outcome_ids_by_bookmaker={"bet365": {"201"}},
    )
    result = HistoricalPayloadSelector.select(raw, ctx)

    # Line 2.5 is kept; line 3.5 is discarded
    markets = result.payload["bookmakers"]["bet365"]["markets"]
    assert "201" in markets
    assert "202" not in markets
    assert result.skipped_non_mainline_markets == 1

    # CRITICAL: Both mapped outcomes of line 2.5 are preserved in preselected payload!
    line_201_outcomes = markets["201"]["outcomes"]
    assert "201" in line_201_outcomes
    assert "202" in line_201_outcomes

    # Pass preselected payload through reader and adapter to verify no skipped_incomplete_markets
    read_res = OddspapiHistoricalOddsReader.read(result.payload, source_sport_id="10")
    adapted = OddspapiMarketAdapter.from_odds_response(
        read_res.normalized_payload,
        market_mapping_index=index,
        use_mainline_cache=True,
        persist_main_line_only=True,
        mainline_outcome_ids_by_bookmaker={"bet365": {"201"}},
    )
    diagnostics = adapted.get("diagnostics", {})
    assert not diagnostics.get("skipped_incomplete_markets")

    # In adapted output with persist_main_line_only=True, only choice Over (from 201) is persisted
    choices = adapted["bookmakers"][0]["markets"][0]["choices"]
    assert len(choices) == 1
    assert choices[0]["name"] == "Over"


# --------------------------------------------------------------------------
# 5. Fallback de cache
# --------------------------------------------------------------------------
def test_cache_fallback_bookmaker():
    index = _build_test_index()
    raw = {
        "fixtureId": "fix-1",
        "sportId": "10",
        "bookmakers": {
            "pinnacle": {
                "markets": {
                    "201": {
                        "outcomes": {
                            "201": {"players": {"0": [_sample_tick()]}},
                            "202": {"players": {"0": [_sample_tick()]}},
                        }
                    }
                }
            }
        },
    }
    # pinnacle has no cache, but bet365 has cache for 201
    ctx = HistoricalPayloadSelectionContext(
        source_sport_id="10",
        market_mapping_index=index,
        use_mainline_cache=True,
        persist_main_line_only=True,
        mainline_outcome_ids_by_bookmaker={"bet365": {"201"}},
        mainline_fallback_bookmakers=["bet365"],
    )
    result = HistoricalPayloadSelector.select(raw, ctx)
    assert "201" in result.payload["bookmakers"]["pinnacle"]["markets"]
    assert result.diagnostics["cache_sources_by_bookmaker"]["pinnacle"] == "bet365"


# --------------------------------------------------------------------------
# 6. Cache inexistente (bypass sin vaciar payload)
# --------------------------------------------------------------------------
def test_nonexistent_cache_triggers_bypass():
    index = _build_test_index()
    raw = {
        "fixtureId": "fix-1",
        "sportId": "10",
        "bookmakers": {
            "bet365": {
                "markets": {
                    "101": {
                        "outcomes": {
                            "101": {"players": {"0": [_sample_tick()]}},
                        }
                    }
                }
            }
        },
    }
    ctx = HistoricalPayloadSelectionContext(
        source_sport_id="10",
        market_mapping_index=index,
        use_mainline_cache=True,
        persist_main_line_only=True,
        mainline_outcome_ids_by_bookmaker={},  # empty cache
    )
    result = HistoricalPayloadSelector.select(raw, ctx)
    assert result.bypassed is True
    assert result.bypass_reason == "empty_mainline_cache"
    assert result.payload == raw


# --------------------------------------------------------------------------
# 7. Mapping index ausente (bypass sin pérdida de datos)
# --------------------------------------------------------------------------
def test_missing_mapping_index_triggers_bypass():
    raw = {"fixtureId": "fix-1", "sportId": "10", "bookmakers": {}}
    ctx = HistoricalPayloadSelectionContext(
        source_sport_id="10",
        market_mapping_index=None,
    )
    result = HistoricalPayloadSelector.select(raw, ctx)
    assert result.bypassed is True
    assert result.bypass_reason == "missing_market_mapping_index"
    assert result.payload == raw


# --------------------------------------------------------------------------
# 8. El selector NO ejecuta normalizer, as_of ni change detector
# --------------------------------------------------------------------------
def test_selector_isolation_from_reader_components():
    index = _build_test_index()
    raw = {
        "fixtureId": "fix-1",
        "sportId": "10",
        "bookmakers": {
            "bet365": {
                "markets": {
                    "101": {
                        "outcomes": {
                            "101": {"players": {"0": [_sample_tick()]}},
                        }
                    }
                }
            }
        },
    }
    ctx = HistoricalPayloadSelectionContext(
        source_sport_id="10",
        market_mapping_index=index,
    )

    with patch(
        "modules.oddspapi.historical_odds_normalizer.OddspapiHistoricalOddsNormalizer.from_ordered_ticks"
    ) as mock_norm, patch(
        "modules.oddspapi.historical_odds_change_detector.OddspapiHistoricalOddsChangeDetector.detect_significant_changes"
    ) as mock_detector, patch(
        "modules.oddspapi.historical_odds_as_of.OddspapiHistoricalOddsAsOf.from_ordered_ticks"
    ) as mock_as_of:
        result = HistoricalPayloadSelector.select(raw, ctx)
        assert result.selected_markets == 1
        assert mock_norm.call_count == 0
        assert mock_detector.call_count == 0
        assert mock_as_of.call_count == 0


# --------------------------------------------------------------------------
# 9. Orden de ejecución en OddspapiOddsFetcher
# --------------------------------------------------------------------------
def test_odds_fetcher_execution_order():
    raw_historical = {
        "fixtureId": "fix-1",
        "sportId": "10",
        "bookmakers": {
            "bet365": {
                "markets": {
                    "101": {
                        "outcomes": {
                            "101": {"players": {"0": [_sample_tick()]}},
                        }
                    }
                }
            }
        },
    }
    mock_client = MagicMock()
    mock_client.get_historical_odds.return_value = raw_historical
    fetcher = OddspapiOddsFetcher(historical_client=mock_client)

    execution_calls = []

    def spy_select(payload, context):
        execution_calls.append(("selector", payload))
        return HistoricalPayloadSelectionResult(payload=payload, diagnostics={"test": 1})

    def spy_read(payload, **kwargs):
        execution_calls.append(("reader", payload))
        return MagicMock(normalized_payload={"normalized": True}, as_of_quotes=())

    index = _build_test_index()
    ctx = HistoricalPayloadSelectionContext(
        source_sport_id="10",
        market_mapping_index=index,
    )

    with patch.object(
        HistoricalPayloadSelector, "select", side_effect=spy_select
    ), patch.object(
        OddspapiHistoricalOddsReader, "read", side_effect=spy_read
    ):
        result = fetcher.fetch_odds(
            fixture_id="fix-1",
            endpoint=ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
            capture_raw_response=True,
            selection_context=ctx,
        )

        # Verificar que el selector se invoca antes del reader
        assert len(execution_calls) == 2
        assert execution_calls[0][0] == "selector"
        assert execution_calls[1][0] == "reader"

        # Verificar que raw_payload sigue siendo el payload completo
        assert result.raw_payload == raw_historical
        assert result.selection_diagnostics == {"test": 1}


# --------------------------------------------------------------------------
# 10. Regresión Mercado 101 (1X2 con outcomes 101, 102 y 103)
# --------------------------------------------------------------------------
def test_market_101_regression_preserves_all_outcomes_and_generates_quotes():
    index = _build_test_index()
    raw = {
        "fixtureId": "fix-1",
        "sportId": "10",
        "bookmakers": {
            "bet365": {
                "markets": {
                    "101": {
                        "outcomes": {
                            "101": {"players": {"0": [_sample_tick("2026-06-20T08:00:00Z", 2.10), _sample_tick("2026-06-20T10:00:00Z", 1.95)]}},
                            "102": {"players": {"0": [_sample_tick("2026-06-20T08:00:00Z", 3.40), _sample_tick("2026-06-20T10:00:00Z", 3.50)]}},
                            "103": {"players": {"0": [_sample_tick("2026-06-20T08:00:00Z", 3.80), _sample_tick("2026-06-20T10:00:00Z", 4.00)]}},
                        }
                    }
                }
            },
            "pinnacle": {
                "markets": {
                    "101": {
                        "outcomes": {
                            "101": {"players": {"0": [_sample_tick("2026-06-20T08:00:00Z", 2.05), _sample_tick("2026-06-20T10:00:00Z", 1.98)]}},
                            "102": {"players": {"0": [_sample_tick("2026-06-20T08:00:00Z", 3.35), _sample_tick("2026-06-20T10:00:00Z", 3.45)]}},
                            "103": {"players": {"0": [_sample_tick("2026-06-20T08:00:00Z", 3.90), _sample_tick("2026-06-20T10:00:00Z", 4.10)]}},
                        }
                    }
                }
            },
        },
    }

    # Cache selects outcome 101 for both books
    ctx = HistoricalPayloadSelectionContext(
        source_sport_id="10",
        market_mapping_index=index,
        use_mainline_cache=True,
        persist_main_line_only=True,
        mainline_outcome_ids_by_bookmaker={
            "bet365": {"101"},
            "pinnacle": {"101"},
        },
    )
    result = HistoricalPayloadSelector.select(raw, ctx)

    # Verificar que bet365 y pinnacle conservan 101, 102 y 103
    for bookie in ("bet365", "pinnacle"):
        market_101 = result.payload["bookmakers"][bookie]["markets"]["101"]["outcomes"]
        assert "101" in market_101
        assert "102" in market_101
        assert "103" in market_101

    # Procesar con el reader y verificar que genera precios normalizados
    read_res = OddspapiHistoricalOddsReader.read(
        result.payload,
        source_sport_id="10",
        enable_significant_changes=False,
    )
    norm = read_res.normalized_payload["bookmakerOdds"]
    assert "bet365" in norm and "pinnacle" in norm
    assert "101" in norm["bet365"]["markets"]["101"]["outcomes"]
    assert "102" in norm["bet365"]["markets"]["101"]["outcomes"]
    assert "103" in norm["bet365"]["markets"]["101"]["outcomes"]


# --------------------------------------------------------------------------
# 11. Endpoint /odds no ejecuta el selector histórico
# --------------------------------------------------------------------------
def test_odds_endpoint_bypasses_historical_selector():
    mock_client = MagicMock()
    mock_client.get_odds.return_value = {"bookmakerOdds": {"bet365": {}}}
    fetcher = OddspapiOddsFetcher(odds_client=mock_client)

    with patch.object(HistoricalPayloadSelector, "select") as mock_selector:
        result = fetcher.fetch_odds(
            fixture_id="fix-1",
            endpoint=ODDSPAPI_CURRENT_ODDS_ENDPOINT,
        )
        assert mock_selector.call_count == 0
        assert result.payload == {"bookmakerOdds": {"bet365": {}}}


# --------------------------------------------------------------------------
# 12. Exchange fan-out mantiene solicitudes y selecciones intactas
# --------------------------------------------------------------------------
def test_exchange_fan_out_preserves_requests():
    """Verify that exchange historical fetches do not undergo secondary destructive filtering."""
    raw_exchange = {
        "fixtureId": "fix-1",
        "sportId": "10",
        "bookmakers": {
            "betfair-ex": {
                "markets": {
                    "102": {
                        "outcomes": {
                            "301": {
                                "players": {
                                    "0": [_sample_tick(price=2.10)]
                                }
                            }
                        }
                    }
                }
            }
        },
    }
    mock_fetcher = MagicMock(spec=OddspapiOddsFetcher)
    mock_fetcher.fetch_odds.return_value = OddsFetchResult.from_payload(
        {
            "fixtureId": "fix-1",
            "sportId": "10",
            "bookmakerOdds": {
                "betfair-ex": {
                    "markets": {
                        "102": {
                            "outcomes": {
                                "301": {
                                    "players": {
                                        "0": {
                                            "price": 2.10,
                                            "active": True,
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
        }
    )

    fake_repo = MagicMock()
    fake_repo.event_ids_with_cache.return_value = {99}
    fake_repo.get_exchange_mainline_selections.return_value = [
        {
            "bookmaker_slug": "betfair-ex",
            "source_market_id": "102",
            "source_outcome_id": "301",
            "canonical_market_key": "1x2_full_time",
        }
    ]

    service = OddspapiPreStartOddsAcquisitionService(
        fetcher=mock_fetcher,
        mainline_cache_repository=fake_repo,
    )

    index = _build_test_index()
    result = service.acquire(
        fixture_id="fix-1",
        event_id=99,
        source_sport_id="10",
        minutes_until_start=0,
        is_live=True,
        enable_exchange_historical=True,
        regular_bookmakers=[],
        exchange_bookmakers=["betfair-ex"],
        market_mapping_index=index,
        exchange_market_keys=None,
        exchange_main_line_only=True,
        exchange_include_player_props=False,
        exchange_historical_moments=[],
        exchange_max_outcomes_per_event=8,
        exchange_request_budget=10,
        minimum_initial_span_minutes=60.0,
        current_odds_available=True,
    )

    assert result.exchange_outcomes_selected == 1
    # Verify that mock_fetcher was called for outcome 301 without destructive selection_context
    assert mock_fetcher.fetch_odds.call_count == 1
    call_kwargs = mock_fetcher.fetch_odds.call_args[1]
    assert call_kwargs["outcome_id"] == 301
    assert call_kwargs.get("selection_context") is None
