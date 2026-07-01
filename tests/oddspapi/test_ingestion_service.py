from datetime import datetime
from unittest.mock import patch

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie,
    Event,
    MarketChoice,
    MarketChoiceSnapshot,
)
from infrastructure.persistence.repositories.market_repository import MarketRepository
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
                    "marketName": "1X2 Full Time",
                    "marketGroup": "1X2",
                    "marketPeriod": "Full Time",
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
        f"{SERVICE}.MarketMappingRepository.build_index",
        return_value=type("Index", (), {"market_mappings": {("oddspapi", "10", "101"): object()}})(),
    ) as build_index, patch(
        f"{SERVICE}.OddspapiMarketAdapter.from_odds_response", return_value=ADAPTED
    ) as adapt, patch(f"{SERVICE}.BookieRepository.resolve_bookie_from_source") as resolve_bookie, patch(
        f"{SERVICE}.MarketRepository.save_markets_from_response_with_stats"
    ) as save:
        result = MarketOddsIngestionService.save_from_oddspapi_response({}, dry_run=True)

    resolve.assert_called_once_with({}, create_mappings=False)
    build_index.assert_called_once_with(source="oddspapi", enabled_only=True)
    assert adapt.call_args.kwargs["market_mapping_index"] is not None
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


def test_filter_normalized_oddspapi_response_applies_cli_aliases():
    adapted = {
        "fixtureId": "fixture-1",
        "bookmakers": [
            {
                "slug": "pinnacle",
                "name": "Pinnacle Sports",
                "markets": [
                    {
                        "marketName": "1X2 Full Time",
                        "marketGroup": "1X2",
                        "marketPeriod": "Full Time",
                        "choiceGroup": None,
                        "isLive": False,
                        "choices": [{"name": "1", "decimalValue": 1.9}],
                    },
                    {
                        "marketName": "Full time",
                        "marketGroup": "Home/Away",
                        "marketPeriod": "Full Time",
                        "choiceGroup": None,
                        "isLive": False,
                        "choices": [{"name": "1", "decimalValue": 1.9}],
                    },
                ],
            }
        ],
    }

    filtered = MarketOddsIngestionService.filter_normalized_oddspapi_response(
        adapted,
        allowed_market_groups={"ml"},
        allowed_market_periods={"Match"},
    )

    assert [bookmaker["slug"] for bookmaker in filtered["bookmakers"]] == ["pinnacle"]
    assert [market["marketGroup"] for market in filtered["bookmakers"][0]["markets"]] == ["Home/Away"]
    assert [market["marketPeriod"] for market in filtered["bookmakers"][0]["markets"]] == ["Full Time"]


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
        f"{SERVICE}.MarketMappingRepository.build_index",
        return_value=type("Index", (), {"market_mappings": {("oddspapi", "10", "101"): object()}})(),
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


def test_save_from_oddspapi_response_skips_when_market_mapping_index_unavailable():
    with patch(
        f"{SERVICE}.OddspapiEventResolver.resolve_from_odds_response",
        return_value=resolution(created_mappings=[]),
    ), patch(
        f"{SERVICE}.MarketMappingRepository.build_index",
        return_value=type("Index", (), {"market_mappings": {}})(),
    ) as build_index, patch(f"{SERVICE}.OddspapiMarketAdapter.from_odds_response") as adapt:
        result = MarketOddsIngestionService.save_from_oddspapi_response({}, dry_run=True)

    build_index.assert_called_once_with(source="oddspapi", enabled_only=True)
    adapt.assert_not_called()
    assert result.skipped is True
    assert result.reason == "market_mapping_index_unavailable"


def test_commit_skips_unmapped_markets_from_mapping_mode():
    adapted = {
        "fixtureId": "fixture-1",
        "bookmakers": [],
        "diagnostics": {
            "unmapped_markets": [{"sourceMarketId": "999"}],
            "unmapped_outcomes": [],
            "skipped_missing_handicap": [],
        },
    }
    with patch(
        f"{SERVICE}.OddspapiEventResolver.resolve_from_odds_response",
        return_value=resolution(created_mappings=[]),
    ), patch(
        f"{SERVICE}.MarketMappingRepository.build_index",
        return_value=type("Index", (), {"market_mappings": {("oddspapi", "10", "101"): object()}})(),
    ), patch(
        f"{SERVICE}.OddspapiMarketAdapter.from_odds_response",
        return_value=adapted,
    ), patch(f"{SERVICE}.BookieRepository.resolve_bookie_from_source") as resolve_bookie:
        result = MarketOddsIngestionService.save_from_oddspapi_response({})

    resolve_bookie.assert_not_called()
    assert result.skipped is True
    assert result.reason == "no normalized markets found"
    assert result.unmapped_markets_detected == 1


def test_commit_passes_canonical_market_payload_to_repository():
    with patch(
        f"{SERVICE}.OddspapiEventResolver.resolve_from_odds_response",
        return_value=resolution(created_mappings=[]),
    ), patch(
        f"{SERVICE}.MarketMappingRepository.build_index",
        return_value=type("Index", (), {"market_mappings": {("oddspapi", "10", "101"): object()}})(),
    ), patch(
        f"{SERVICE}.OddspapiMarketAdapter.from_odds_response",
        return_value=ADAPTED,
    ), patch(
        f"{SERVICE}.BookieRepository.resolve_bookie_from_source",
        return_value=BookieResolution(bookie=type("Bookie", (), {"bookie_id": 23})(), resolved=True, reused=True),
    ), patch(
        f"{SERVICE}.MarketRepository.save_markets_from_response_with_stats",
        return_value=type("SaveResult", (), {"markets_saved": 1, "choices_saved": 1, "snapshots_saved": 1})(),
    ) as save, patch(
        f"{SERVICE}.DualProcessOddsRepository.event_has_dual_process_odds", return_value=True
    ):
        result = MarketOddsIngestionService.save_from_oddspapi_response({})

    saved_payload = save.call_args.kwargs["odds_response"]
    saved_market = saved_payload["markets"][0]
    assert saved_market["marketName"] == "1X2 Full Time"
    assert saved_market["marketGroup"] == "1X2"
    assert saved_market["marketPeriod"] == "Full Time"
    assert result.markets_saved == 1


def _repository_manager(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'exchange-markets.db'}")
    manager.create_tables()
    return manager


def _seed_repository_entities(manager):
    with manager.get_session() as session:
        event = Event(
            slug="exchange-test-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Football",
            competition="Premier League",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="Betfair Exchange", slug="betfair-ex")
        session.add_all([event, bookie])
        session.flush()
        return event.id, bookie.bookie_id


def _repository_response(choice):
    return {
        "markets": [
            {
                "marketName": "1X2 Full Time",
                "marketGroup": "1X2",
                "marketPeriod": "Full Time",
                "choiceGroup": None,
                "isLive": False,
                "choices": [choice],
            }
        ]
    }


def test_repository_sportsbook_choice_keeps_single_null_exchange_snapshot(tmp_path):
    manager = _repository_manager(tmp_path)
    event_id, bookie_id = _seed_repository_entities(manager)
    response_data = _repository_response(
        {"name": "1", "decimalValue": 1.9, "exchangeQuotes": None}
    )

    with patch("infrastructure.persistence.repositories.market_repository.db_manager", manager):
        result = MarketRepository.save_markets_from_response_with_stats(
            event_id=event_id,
            odds_response=response_data,
            bookie_id=bookie_id,
            source="oddspapi",
        )

    assert result.snapshots_saved == 1
    with manager.get_session() as session:
        snapshot = session.query(MarketChoiceSnapshot).one()
    assert snapshot.exchange_side is None
    assert snapshot.exchange_level is None
    assert snapshot.exchange_size is None


def test_repository_exchange_choice_persists_ladder_and_best_back_current_odds(tmp_path):
    manager = _repository_manager(tmp_path)
    event_id, bookie_id = _seed_repository_entities(manager)
    response_data = _repository_response(
        {
            "name": "2",
            "decimalValue": 4.8,
            "changedAt": "2026-06-19T12:34:56Z",
            "limit": 64.9,
            "exchangeQuotes": [
                {"side": "back", "level": 0, "price": 4.8, "size": 64.9},
                {"side": "back", "level": 1, "price": 4.7, "size": 2091.25},
                {"side": "lay", "level": 0, "price": 5.0, "size": 100.92},
                {"side": "lay", "level": 1, "price": 5.1, "size": 103.6},
            ],
        }
    )

    with patch("infrastructure.persistence.repositories.market_repository.db_manager", manager):
        result = MarketRepository.save_markets_from_response_with_stats(
            event_id=event_id,
            odds_response=response_data,
            bookie_id=bookie_id,
            source="oddspapi",
        )

    assert result.snapshots_saved == 4
    with manager.get_session() as session:
        choice = session.query(MarketChoice).one()
        snapshots = (
            session.query(MarketChoiceSnapshot)
            .order_by(MarketChoiceSnapshot.snapshot_id)
            .all()
        )

    assert choice.choice_name == "2"
    assert float(choice.current_odds) == 4.8
    assert [
        (
            snapshot.exchange_side,
            snapshot.exchange_level,
            float(snapshot.odds_value),
            float(snapshot.exchange_size),
        )
        for snapshot in snapshots
    ] == [
        ("back", 0, 4.8, 64.9),
        ("back", 1, 4.7, 2091.25),
        ("lay", 0, 5.0, 100.92),
        ("lay", 1, 5.1, 103.6),
    ]
