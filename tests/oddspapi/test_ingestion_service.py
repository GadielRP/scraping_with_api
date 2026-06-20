from unittest.mock import patch

from modules.oddspapi import OddspapiEventResolution
from modules.odds_ingestion.market_odds_ingestion_service import MarketOddsIngestionService
from infrastructure.persistence.repositories.bookie_repository import BookieResolution


SERVICE = "modules.odds_ingestion.market_odds_ingestion_service"
ADAPTED = {
    "fixtureId": "fixture-1",
    "bookmakers": [
        {
            "slug": "pinnacle",
            "name": "Pinnacle Sports",
            "markets": [
                {
                    "marketName": "Full-time",
                    "marketGroup": "1X2",
                    "marketPeriod": "Full-time",
                    "choiceGroup": None,
                    "isLive": False,
                    "choices": [{"name": "1", "decimalValue": 1.9}],
                }
            ],
        }
    ],
}


def resolution(resolved=True, reason=None, created_mappings=None):
    return OddspapiEventResolution(
        oddspapi_fixture_id="fixture-1",
        canonical_event_id=55 if resolved else None,
        resolved=resolved,
        skipped_reason=reason,
        created_mappings=[] if created_mappings is None else created_mappings,
    )


def test_dry_run_performs_no_writes():
    with patch(
        f"{SERVICE}.OddspapiEventResolver.resolve_from_odds_response",
        return_value=resolution(created_mappings=[]),
    ) as resolve, patch(
        f"{SERVICE}.OddspapiMarketAdapter.from_odds_response", return_value=ADAPTED
    ), patch(f"{SERVICE}.BookieRepository.resolve_bookie_from_source") as resolve_bookie, patch(
        f"{SERVICE}.MarketRepository.save_markets_from_response_with_stats"
    ) as save:
        result = MarketOddsIngestionService.save_from_oddspapi_response({}, dry_run=True)

    resolve.assert_called_once_with({}, create_mappings=False)
    resolve_bookie.assert_not_called()
    save.assert_not_called()
    assert result.event_id == 55
    assert result.markets_detected == 1
    assert result.choices_detected == 1
    assert result.snapshots_detected == 1
    assert result.markets_saved == 0
    assert result.bookies_detected == 1
    assert result.bookies_processed == 0
    assert result.event_mappings_created == 0
    assert result.mappings_created == 0


def test_unresolved_event_is_skipped_before_adaptation():
    with patch(
        f"{SERVICE}.OddspapiEventResolver.resolve_from_odds_response",
        return_value=resolution(False, "sofascore_mapping_not_found"),
    ), patch(f"{SERVICE}.OddspapiMarketAdapter.from_odds_response") as adapt:
        result = MarketOddsIngestionService.save_from_oddspapi_response({})
    adapt.assert_not_called()
    assert result.skipped is True
    assert result.reason == "sofascore_mapping_not_found"


def test_commit_uses_source_resolution_and_skips_unresolved_bookmaker():
    adapted = {
        "fixtureId": "fixture-1",
        "bookmakers": [
            {
                "slug": "pinnacle",
                "name": "Pinnacle Sports",
                "markets": ADAPTED["bookmakers"][0]["markets"],
            },
            {
                "slug": "unknown",
                "name": "Unknown Bookmaker",
                "markets": ADAPTED["bookmakers"][0]["markets"],
            },
        ],
    }
    with patch(
        f"{SERVICE}.OddspapiEventResolver.resolve_from_odds_response",
        return_value=resolution(created_mappings=["oddspapi"]),
    ), patch(
        f"{SERVICE}.OddspapiMarketAdapter.from_odds_response", return_value=adapted
    ), patch(
        f"{SERVICE}.BookieRepository.resolve_bookie_from_source",
        side_effect=[
            BookieResolution(bookie=type("Bookie", (), {"bookie_id": 23})(), resolved=True, reused=True, mapping_created=True),
            BookieResolution(bookie=None, resolved=False, reason="canonical_bookie_not_found"),
        ],
    ) as resolve_bookie, patch(
        f"{SERVICE}.MarketRepository.save_markets_from_response_with_stats",
        return_value=type("SaveResult", (), {"markets_saved": 1, "choices_saved": 1, "snapshots_saved": 1})(),
    ) as save, patch(
        f"{SERVICE}.DualProcessOddsRepository.event_has_dual_process_odds", return_value=True
    ):
        result = MarketOddsIngestionService.save_from_oddspapi_response({})

    assert resolve_bookie.call_args_list[0].kwargs["source"] == "oddspapi"
    assert resolve_bookie.call_args_list[0].kwargs["source_bookie_name"] == "Pinnacle Sports"
    assert resolve_bookie.call_args_list[0].kwargs["source_bookie_slug"] == "pinnacle"
    assert save.call_count == 1
    assert save.call_args.kwargs["event_id"] == 55
    assert save.call_args.kwargs["bookie_id"] == 23
    assert result.markets_saved == 1
    assert result.choices_saved == 1
    assert result.snapshots_saved == 1
    assert result.bookies_detected == 2
    assert result.bookies_processed == 1
    assert result.bookies_created == 0
    assert result.bookies_reused == 1
    assert result.bookie_mappings_created == 1
    assert result.event_mappings_created == 1
    assert result.mappings_created == 1
