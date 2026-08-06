from __future__ import annotations

from datetime import datetime
from types import MappingProxyType, SimpleNamespace
from unittest.mock import patch

import pytest

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie,
    Event,
    Market,
    MarketChoice,
    MarketChoiceSnapshot,
)
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
from modules.oddsportal.dataclasses import (
    BetfairExchangeOdds,
    BookieOdds,
    MarketExtraction,
    MatchOddsData,
)


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


def test_adapter_transports_per_choice_tooltip_timestamps():
    source_bookie = _bookie(draw=True)
    source_bookie.initial_odds_1_time = "04 Aug, 14:41"
    source_bookie.odds_1_time = "05 Aug, 10:01"
    odds_data = MatchOddsData(
        sport="football",
        extractions=[
            MarketExtraction(
                source_group_key="1X2",
                source_period_key="FULL_TIME",
                bookie_odds=[source_bookie],
            )
        ],
    )

    response = OddsPortalMarketAdapter.from_match_odds_data(
        odds_data,
        canonical_types=CANONICAL_TYPES,
    )

    choice = response.bookmakers[0].markets[0].choices[0]
    assert choice.initial_changed_at.endswith("-08-04T14:41")
    assert choice.source_collected_at.endswith("-08-05T10:01")
    assert choice.as_repository_dict()["sourceCollectedAt"].endswith(
        "-08-05T10:01"
    )


def test_adapter_transports_exchange_tooltip_current_for_back_and_lay():
    exchange = BetfairExchangeOdds(
        back_1="1.42",
        back_x="3.5",
        back_2="5.2",
        lay_1="1.44",
        lay_x="3.6",
        lay_2="5.4",
        initial_back_1="1.86",
        initial_back_x="3.4",
        initial_back_2="4.8",
        initial_lay_1="1.9",
        initial_lay_x="3.5",
        initial_lay_2="5.0",
        back_1_time="05 Aug, 10:01",
        lay_1_time="05 Aug, 10:02",
    )
    odds_data = MatchOddsData(
        sport="football",
        extractions=[
            MarketExtraction(
                source_group_key="1X2",
                source_period_key="FULL_TIME",
                betfair=exchange,
            )
        ],
    )

    response = OddsPortalMarketAdapter.from_match_odds_data(
        odds_data,
        canonical_types=CANONICAL_TYPES,
    )

    betfair = response.bookmakers[0]
    markets = {market.choice_group: market for market in betfair.markets}
    assert betfair.source_slug == "betfair-ex"
    assert markets["Back"].choices[0].current_odds == "1.42"
    assert markets["Back"].choices[0].source_collected_at.endswith(
        "-08-05T10:01"
    )
    assert markets["Lay"].choices[0].current_odds == "1.44"
    assert markets["Lay"].choices[0].source_collected_at.endswith(
        "-08-05T10:02"
    )


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


def _oddspapi_market_response(*, include_uncovered_market=False):
    markets = [
        {
            "marketName": "1X2 Full Time",
            "marketGroup": "1X2",
            "marketPeriod": "Full Time",
            "choiceGroup": None,
            "isLive": False,
            "choices": [
                {"name": "1", "initialOdds": "1.82", "currentOdds": "1.44"},
                {"name": "x", "initialOdds": "3.10", "currentOdds": "3.25"},
                {"name": "2", "initialOdds": "1.91", "currentOdds": "2.85"},
            ],
        }
    ]
    if include_uncovered_market:
        markets.append(
            {
                "marketName": "Over/Under Full Time",
                "marketGroup": "Over/Under",
                "marketPeriod": "Full Time",
                "choiceGroup": "8.5",
                "isLive": False,
                "choices": [
                    {"name": "over", "initialOdds": "1.90", "currentOdds": "1.95"},
                    {"name": "under", "initialOdds": "1.90", "currentOdds": "1.85"},
                ],
            }
        )
    return {"markets": markets}


def _oddsportal_references(bookie_id):
    return OddsPortalIngestionReferenceData(
        canonical_types=MappingProxyType(CANONICAL_TYPES),
        bookie_ids_by_source_slug=MappingProxyType({"bet365": bookie_id}),
    )


def _oddsportal_opening_data():
    source_bookie = _bookie(draw=True)
    # Deliberately impossible currents prove that the opening-only policy never
    # leaks OddsPortal current values into canonical choices or trajectories.
    source_bookie.odds_1 = "9.91"
    source_bookie.odds_x = "9.92"
    source_bookie.odds_2 = "9.93"
    source_bookie.initial_odds_1 = "1.86"
    source_bookie.initial_odds_x = "3.20"
    source_bookie.initial_odds_2 = "1.86"
    source_bookie.initial_odds_1_time = "04 Aug, 14:41"
    source_bookie.initial_odds_x_time = "04 Aug, 14:42"
    source_bookie.initial_odds_2_time = "04 Aug, 14:43"
    return MatchOddsData(
        sport="football",
        extractions=[
            MarketExtraction(
                source_group_key="1X2",
                source_period_key="FULL_TIME",
                bookie_odds=[source_bookie],
            )
        ],
    )


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
    references = _oddsportal_references(bookie_id)

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
    assert result.snapshots_detected == 0
    assert result.snapshots_saved == 0

    with original_get_session() as session:
        market = session.query(Market).one()
        choices = session.query(MarketChoice).order_by(MarketChoice.choice_name).all()
    assert market.market_name == "1X2 Full Time"
    assert market.market_period == "Full Time"
    assert [choice.choice_name for choice in choices] == ["1", "2", "x"]
    assert all(choice.initial_odds is not None for choice in choices)
    assert all(choice.current_odds is None for choice in choices)


@pytest.mark.parametrize("write_order", ["oddspapi_first", "oddsportal_first"])
def test_oddsportal_opening_and_oddspapi_current_are_order_independent(
    tmp_path,
    write_order,
):
    manager = DatabaseManager(f"sqlite:///{tmp_path / f'{write_order}.db'}")
    manager.create_tables()
    event_id, bookie_id = _seed_event_and_bookie(manager)
    references = _oddsportal_references(bookie_id)

    def save_oddspapi():
        return MarketRepository.save_markets_from_response_with_stats(
            event_id,
            _oddspapi_market_response(),
            bookie_id,
            source="oddspapi",
        )

    def save_oddsportal():
        return MarketOddsIngestionService.save_from_oddsportal_data(
            event_id,
            _oddsportal_opening_data(),
            reference_data=references,
        )

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        if write_order == "oddspapi_first":
            save_oddspapi()
            oddsportal_result = save_oddsportal()
        else:
            oddsportal_result = save_oddsportal()
            save_oddspapi()

    assert oddsportal_result.snapshots_saved == 0
    with manager.get_session() as session:
        choices = {
            choice.choice_name: choice
            for choice in session.query(MarketChoice).all()
        }
        snapshots = session.query(MarketChoiceSnapshot).all()

    assert float(choices["1"].initial_odds) == 1.86
    assert float(choices["x"].initial_odds) == 3.20
    assert float(choices["2"].initial_odds) == 1.86
    assert float(choices["1"].current_odds) == 1.44
    assert float(choices["x"].current_odds) == 3.25
    assert float(choices["2"].current_odds) == 2.85
    assert choices["1"].change == -1
    assert choices["x"].change == 1
    assert choices["2"].change == 1
    assert len(snapshots) == 3
    assert {snapshot.source for snapshot in snapshots} == {"oddspapi"}


def test_market_not_covered_by_oddsportal_keeps_oddspapi_initial(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'partial-coverage.db'}")
    manager.create_tables()
    event_id, bookie_id = _seed_event_and_bookie(manager)

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        MarketRepository.save_markets_from_response_with_stats(
            event_id,
            _oddspapi_market_response(include_uncovered_market=True),
            bookie_id,
            source="oddspapi",
        )
        MarketOddsIngestionService.save_from_oddsportal_data(
            event_id,
            _oddsportal_opening_data(),
            reference_data=_oddsportal_references(bookie_id),
        )

    with manager.get_session() as session:
        uncovered_market = (
            session.query(Market)
            .filter(Market.market_name == "Over/Under Full Time")
            .one()
        )
        uncovered_choices = {
            choice.choice_name: choice
            for choice in uncovered_market.choices
        }

    assert float(uncovered_choices["over"].initial_odds) == 1.90
    assert float(uncovered_choices["under"].initial_odds) == 1.90
    assert float(uncovered_choices["over"].current_odds) == 1.95
    assert float(uncovered_choices["under"].current_odds) == 1.85


@pytest.mark.parametrize(
    ("oddsportal_enabled", "expected_initials"),
    [
        (True, {"1": 1.86, "x": 3.20, "2": 1.86}),
        (False, {"1": 1.82, "x": 3.10, "2": 1.91}),
    ],
)
def test_oddsportal_toggle_selects_opening_owner_without_losing_oddspapi_current(
    tmp_path,
    monkeypatch,
    oddsportal_enabled,
    expected_initials,
):
    """Keep OddsPAPI as the complete fallback when browser scraping is off.

    The historical normalizer separately guarantees that the OddsPAPI opening
    supplied here is the earliest active quote after the configured 60-minute
    credibility span.
    """

    from modules.jobs.pre_start_check_job import oddsportal_worker

    manager = DatabaseManager(
        f"sqlite:///{tmp_path / f'oddsportal-{oddsportal_enabled}.db'}"
    )
    manager.create_tables()
    event_id, bookie_id = _seed_event_and_bookie(manager)
    competition_id = next(iter(oddsportal_worker.ODDSPORTAL_COMPETITION_ROUTES))
    monkeypatch.setattr(
        oddsportal_worker.Config,
        "ODDSPORTAL_SCRAPING_ENABLED",
        oddsportal_enabled,
    )
    monkeypatch.setattr(
        oddsportal_worker.Config,
        "ODDSPORTAL_OPENING_CAPTURE_MINUTES",
        120,
    )
    candidates = oddsportal_worker.build_oddsportal_scrape_candidates(
        [{"id": event_id, "competition_id": competition_id}],
        {event_id: 120},
    )

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        MarketRepository.save_markets_from_response_with_stats(
            event_id,
            _oddspapi_market_response(),
            bookie_id,
            source="oddspapi",
        )
        if candidates:
            MarketOddsIngestionService.save_from_oddsportal_data(
                event_id,
                _oddsportal_opening_data(),
                reference_data=_oddsportal_references(bookie_id),
            )

    assert bool(candidates) is oddsportal_enabled
    with manager.get_session() as session:
        choices = {
            choice.choice_name: choice
            for choice in session.query(MarketChoice).all()
        }
        snapshots = session.query(MarketChoiceSnapshot).all()

    assert {
        name: float(choice.initial_odds)
        for name, choice in choices.items()
    } == expected_initials
    assert float(choices["1"].current_odds) == 1.44
    assert float(choices["x"].current_odds) == 3.25
    assert float(choices["2"].current_odds) == 2.85
    assert len(snapshots) == 3
    assert {snapshot.source for snapshot in snapshots} == {"oddspapi"}


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
                        {"name": "1", "initialOdds": "2.0", "currentOdds": "1.9"},
                        {"name": "x", "initialOdds": "3.3", "currentOdds": "3.4"},
                        {"name": "2", "initialOdds": "4.0", "currentOdds": "4.2"},
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


def test_oddsportal_candidates_use_configured_opening_capture_minute(monkeypatch):
    from modules.jobs.pre_start_check_job import oddsportal_worker

    competition_id = next(iter(oddsportal_worker.ODDSPORTAL_COMPETITION_ROUTES))
    events = [
        {"id": 1, "competition_id": competition_id},
        {"id": 2, "competition_id": competition_id},
    ]
    monkeypatch.setattr(oddsportal_worker.Config, "ODDSPORTAL_SCRAPING_ENABLED", True)
    monkeypatch.setattr(
        oddsportal_worker.Config,
        "ODDSPORTAL_OPENING_CAPTURE_MINUTES",
        120,
    )

    candidates = oddsportal_worker.build_oddsportal_scrape_candidates(
        events,
        {1: 120, 2: -5},
    )

    assert [candidate["event_id"] for candidate in candidates] == [1]
    assert candidates[0]["minutes_until_start"] == 120


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

    saved = oddsportal_worker.scrape_oddsportal_batch(events, debug_mode=True)

    assert saved == {1: 1, 2: 1}
    assert len(loads) == 1
    assert calls[0]["collect_results"] is False
    assert calls[0]["debug_mode"] is True
    assert calls[0]["debug_dir"] == "debug"


def test_worker_propagates_debug_mode_into_background_scrape(monkeypatch):
    from modules.jobs.pre_start_check_job import oddsportal_worker

    captured = {}
    monkeypatch.setattr(
        oddsportal_worker,
        "build_oddsportal_scrape_candidates",
        lambda *_args: [{"event_id": 7}],
    )
    monkeypatch.setattr(
        oddsportal_worker,
        "create_oddsportal_scrape_state",
        lambda _candidates: {},
    )

    def fake_start(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(
        oddsportal_worker,
        "start_oddsportal_scrape_thread",
        fake_start,
    )

    oddsportal_worker.start_oddsportal_scrape_for_events(
        SimpleNamespace(),
        [],
        {},
        debug_mode=True,
    )

    assert captured["kwargs"]["debug_mode"] is True
