from __future__ import annotations

from datetime import datetime
from types import MappingProxyType, SimpleNamespace
from unittest.mock import patch

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Bookie, Event, Market, MarketChoice
from infrastructure.persistence.repositories.canonical_market_type_repository import (
    CanonicalMarketTypeResolution,
)
from infrastructure.persistence.repositories.market_repository import MarketRepository
from modules.odds_ingestion.adapters.oddsportal_market_adapter import (
    OddsPortalMarketAdapter,
)
from modules.odds_ingestion.market_odds_ingestion_service import (
    MarketOddsIngestionService,
    OddsPortalIngestionReferenceData,
)
from modules.oddsportal.dataclasses import BookieOdds, MarketExtraction, MatchOddsData


def _canonical_type(key, name, group, period, family, requires_group=False):
    return CanonicalMarketTypeResolution(
        canonical_market_key=key,
        canonical_market_name=name,
        canonical_market_group=group,
        canonical_market_period=period,
        market_family=family,
        requires_choice_group=requires_group,
        enabled_for_ingestion=True,
    )


CANONICAL_TYPES = {
    "1x2_full_time": _canonical_type(
        "1x2_full_time", "1X2 Full Time", "1X2", "Full Time", "side_3way"
    ),
    "home_away_full_time_including_overtime": _canonical_type(
        "home_away_full_time_including_overtime",
        "Home/Away Full Time Including Overtime",
        "Home/Away",
        "Full Time Including Overtime",
        "side_2way",
    ),
}


def _bookie(*, draw=True):
    return BookieOdds(
        name="bet365",
        odds_1="1.90",
        odds_x="3.40" if draw else "-",
        odds_2="4.20",
        initial_odds_1="2.00",
        initial_odds_x="3.30" if draw else None,
        initial_odds_2="4.00",
    )


def test_adapter_uses_route_identity_for_football_canonical_terms():
    odds_data = MatchOddsData(
        sport="football",
        extractions=[
            MarketExtraction(
                market_name="Full Time",
                market_group="1X2",
                market_period="Full Time",
                source_group_key="1X2",
                source_period_key="FULL_TIME",
                bookie_odds=[_bookie(draw=True)],
            )
        ],
    )

    response = OddsPortalMarketAdapter.from_match_odds_data(
        odds_data,
        canonical_types=CANONICAL_TYPES,
    )

    market = response.bookmakers[0].markets[0]
    assert market.canonical_market_key == "1x2_full_time"
    assert market.market_name == "1X2 Full Time"
    assert market.market_group == "1X2"
    assert market.market_period == "Full Time"
    assert [choice.name for choice in market.choices] == ["1", "x", "2"]


def test_adapter_preserves_including_overtime_semantics():
    odds_data = MatchOddsData(
        sport="basketball",
        extractions=[
            MarketExtraction(
                market_name="Full Time",
                market_group="Home/Away",
                market_period="Full Time",
                source_group_key="HOME_AWAY",
                source_period_key="FT_INC_OT",
                bookie_odds=[_bookie(draw=False)],
            )
        ],
    )

    response = OddsPortalMarketAdapter.from_match_odds_data(
        odds_data,
        canonical_types=CANONICAL_TYPES,
    )

    market = response.bookmakers[0].markets[0]
    assert market.canonical_market_key == "home_away_full_time_including_overtime"
    assert market.market_name == "Home/Away Full Time Including Overtime"
    assert market.market_period == "Full Time Including Overtime"
    assert [choice.name for choice in market.choices] == ["1", "2"]


def test_adapter_rejects_missing_route_identity_instead_of_guessing():
    odds_data = MatchOddsData(
        extractions=[
            MarketExtraction(
                market_name="Full Time",
                market_group="1X2",
                market_period="Full Time",
                bookie_odds=[_bookie(draw=True)],
            )
        ]
    )

    response = OddsPortalMarketAdapter.from_match_odds_data(
        odds_data,
        canonical_types=CANONICAL_TYPES,
    )

    assert response.bookmakers == ()
    assert response.diagnostics[0]["reason"] == "missing_oddsportal_route_identity"


def _make_manager(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'oddsportal-canonical.db'}")
    manager.create_tables()
    return manager


def _seed_event_and_bookie(manager):
    with manager.get_session() as session:
        event = Event(
            slug="home-away",
            start_time_utc=datetime(2026, 8, 4, 12, 0),
            sport="Football",
            competition="Test League",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="bet365", slug="bet365")
        session.add_all([event, bookie])
        session.flush()
        return event.id, bookie.bookie_id


def test_service_persists_one_canonical_event_batch_with_one_session(tmp_path):
    manager = _make_manager(tmp_path)
    event_id, bookie_id = _seed_event_and_bookie(manager)
    odds_data = MatchOddsData(
        sport="football",
        extractions=[
            MarketExtraction(
                source_group_key="1X2",
                source_period_key="FULL_TIME",
                bookie_odds=[_bookie(draw=True)],
            )
        ],
    )
    references = OddsPortalIngestionReferenceData(
        canonical_types=MappingProxyType(CANONICAL_TYPES),
        bookie_ids_by_source_slug=MappingProxyType({"bet365": bookie_id}),
    )

    original_get_session = manager.get_session
    session_calls = 0

    def counted_get_session():
        nonlocal session_calls
        session_calls += 1
        return original_get_session()

    manager.get_session = counted_get_session
    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        result = MarketOddsIngestionService.save_from_oddsportal_data(
            event_id,
            odds_data,
            reference_data=references,
        )

    assert session_calls == 1
    assert result.markets_saved == 1
    assert result.choices_saved == 3
    assert result.snapshots_saved == 3

    with original_get_session() as session:
        market = session.query(Market).one()
        choices = session.query(MarketChoice).order_by(MarketChoice.choice_name).all()
    assert market.market_name == "1X2 Full Time"
    assert market.market_period == "Full Time"
    assert [choice.choice_name for choice in choices] == ["1", "2", "x"]


def test_canonical_write_upgrades_unique_legacy_full_time_row(tmp_path):
    manager = _make_manager(tmp_path)
    event_id, bookie_id = _seed_event_and_bookie(manager)
    with manager.get_session() as session:
        session.add(
            Market(
                event_id=event_id,
                bookie_id=bookie_id,
                market_name="Full Time",
                market_group="1X2",
                market_period="Full Time",
                choice_group=None,
                is_live=False,
            )
        )

    batches = [
        {
            "bookie_id": bookie_id,
            "markets": [
                {
                    "marketName": "1X2 Full Time",
                    "marketGroup": "1X2",
                    "marketPeriod": "Full Time",
                    "choiceGroup": None,
                    "isLive": False,
                    "choices": [
                        {"name": "1", "currentOdds": "1.9"},
                        {"name": "x", "currentOdds": "3.4"},
                        {"name": "2", "currentOdds": "4.2"},
                    ],
                }
            ],
        }
    ]

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        MarketRepository.save_canonical_bookmaker_batches(
            event_id,
            batches,
            source="oddsportal",
        )

    with manager.get_session() as session:
        markets = session.query(Market).all()
    assert len(markets) == 1
    assert markets[0].market_name == "1X2 Full Time"


def test_worker_loads_reference_data_once_and_requests_streaming(monkeypatch):
    from modules.jobs.pre_start_check_job import oddsportal_worker

    loads = []
    calls = []
    reference_data = SimpleNamespace(unresolved_bookie_slugs=())

    monkeypatch.setattr(
        oddsportal_worker.MarketOddsIngestionService,
        "load_oddsportal_reference_data",
        staticmethod(lambda source_bookies: loads.append(source_bookies) or reference_data),
    )
    monkeypatch.setattr(
        oddsportal_worker.MarketOddsIngestionService,
        "save_from_oddsportal_data",
        staticmethod(
            lambda event_id, data, reference_data: SimpleNamespace(
                markets_saved=1,
                choices_saved=2,
                snapshots_saved=2,
                skipped=False,
                reason=None,
            )
        ),
    )

    def fake_dispatch(tasks, **kwargs):
        calls.append(kwargs)
        for task in tasks:
            kwargs["on_result"](task["event_id"], object())
        return {task["event_id"]: None for task in tasks}

    monkeypatch.setattr(
        oddsportal_worker,
        "scrape_multiple_matches_parallel_sync",
        fake_dispatch,
    )
    events = [
        {
            "event_id": event_id,
            "should_extract_odds": True,
            "event_data": {
                "id": event_id,
                "season_id": 10,
                "competition_id": 168,
                "home_team": "Home",
                "away_team": "Away",
            },
        }
        for event_id in (1, 2)
    ]

    saved = oddsportal_worker.scrape_oddsportal_batch(events)

    assert saved == {1: 1, 2: 1}
    assert len(loads) == 1
    assert calls[0]["collect_results"] is False
