from types import SimpleNamespace
from unittest.mock import patch

from modules.oddspapi import OddspapiEventResolution
from modules.odds_ingestion.market_odds_ingestion_service import MarketOddsIngestionService


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


def resolution(resolved=True, reason=None):
    return OddspapiEventResolution(
        oddspapi_fixture_id="fixture-1",
        canonical_event_id=55 if resolved else None,
        resolved=resolved,
        skipped_reason=reason,
        created_mappings=["oddspapi"] if resolved else [],
    )


def test_dry_run_performs_no_writes():
    with patch(
        f"{SERVICE}.OddspapiEventResolver.resolve_from_odds_response",
        return_value=resolution(),
    ) as resolve, patch(
        f"{SERVICE}.OddspapiMarketAdapter.from_odds_response", return_value=ADAPTED
    ), patch(f"{SERVICE}.MarketRepository.get_or_create_bookie_by_slug") as get_bookie, patch(
        f"{SERVICE}.MarketRepository.save_markets_from_response"
    ) as save:
        result = MarketOddsIngestionService.save_from_oddspapi_response({}, dry_run=True)

    resolve.assert_called_once_with({}, create_mappings=False)
    get_bookie.assert_not_called()
    save.assert_not_called()
    assert result.event_id == 55
    assert result.markets_saved == 1
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


def test_commit_uses_bookie_slug_and_resolved_bookie_id():
    with patch(
        f"{SERVICE}.OddspapiEventResolver.resolve_from_odds_response",
        return_value=resolution(),
    ), patch(
        f"{SERVICE}.OddspapiMarketAdapter.from_odds_response", return_value=ADAPTED
    ), patch(
        f"{SERVICE}.MarketRepository.get_or_create_bookie_by_slug",
        return_value=SimpleNamespace(bookie_id=23),
    ) as get_bookie, patch(
        f"{SERVICE}.MarketRepository.save_markets_from_response", return_value=1
    ) as save, patch(
        f"{SERVICE}.DualProcessOddsRepository.event_has_dual_process_odds", return_value=True
    ):
        result = MarketOddsIngestionService.save_from_oddspapi_response({})

    get_bookie.assert_called_once_with("Pinnacle Sports", "pinnacle")
    assert save.call_args.kwargs["event_id"] == 55
    assert save.call_args.kwargs["bookie_id"] == 23
    assert result.markets_saved == 1
    assert result.bookies_saved == 1
    assert result.mappings_created == 1
