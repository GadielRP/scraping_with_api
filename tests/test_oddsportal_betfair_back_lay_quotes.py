"""Integration test: OddsPortal Betfair back/lay end-to-end through the real
adapter + MarketOddsIngestionService + MarketRepository pipeline.

Fase 3 fix (docs/refactors/db-schema-odds-refactor.md): back and lay used to
be persisted as choice_group='Back'/'Lay', splitting one outcome into two
disconnected Market rows with no shared identity. This test proves the fix:
back and lay now share ONE market/choice, disambiguated only by
MarketChoiceQuote.exchange_side.
"""

from __future__ import annotations

import logging
from datetime import datetime
from types import MappingProxyType
from unittest.mock import patch

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie,
    Event,
    Market,
    MarketChoice,
    MarketChoiceQuote,
)
from infrastructure.persistence.repositories.canonical_market_type_repository import (
    CanonicalMarketTypeResolution,
)
from modules.odds_ingestion.market_odds_ingestion_service import (
    MarketOddsIngestionService,
    OddsPortalIngestionReferenceData,
)
from modules.oddsportal.dataclasses import (
    BetfairExchangeOdds,
    MarketExtraction,
    MatchOddsData,
)

CANONICAL_TYPES = MappingProxyType(
    {
        "1x2_full_time": CanonicalMarketTypeResolution(
            canonical_market_key="1x2_full_time",
            canonical_market_name="1X2 Full Time",
            canonical_market_group="1X2",
            canonical_market_period="Full Time",
            market_family="side_3way",
            requires_choice_group=False,
            enabled_for_ingestion=True,
        )
    }
)


def _seed_event_and_betfair_bookie(manager):
    with manager.get_session() as session:
        event = Event(
            slug="oddsportal-betfair-back-lay",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Football",
            competition="Test League",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="Betfair Exchange", slug="betfair-ex")
        session.add_all([event, bookie])
        session.flush()
        return event.id, bookie.bookie_id


def test_oddsportal_betfair_back_lay_share_one_market_and_choice(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'oddsportal-betfair.db'}")
    manager.create_tables()
    event_id, betfair_bookie_id = _seed_event_and_betfair_bookie(manager)

    odds_data = MatchOddsData(
        sport="football",
        extractions=[
            MarketExtraction(
                source_group_key="1X2",
                source_period_key="FULL_TIME",
                betfair=BetfairExchangeOdds(
                    back_1="1.90",
                    back_x="3.40",
                    back_2="4.20",
                    lay_1="1.95",
                    lay_x="3.50",
                    lay_2="4.30",
                    initial_back_1="1.86",
                    initial_back_x="3.30",
                    initial_back_2="4.00",
                    initial_lay_1="1.90",
                    initial_lay_x="3.40",
                    initial_lay_2="4.10",
                ),
            )
        ],
    )
    references = OddsPortalIngestionReferenceData(
        canonical_types=CANONICAL_TYPES,
        bookie_ids_by_source_slug=MappingProxyType({"betfair-ex": betfair_bookie_id}),
    )

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        result = MarketOddsIngestionService.save_from_oddsportal_data(
            event_id,
            odds_data,
            reference_data=references,
        )

    assert result.markets_saved == 1

    with manager.get_session() as session:
        markets = (
            session.query(Market)
            .filter(Market.bookie_id == betfair_bookie_id)
            .all()
        )
        assert len(markets) == 1
        market = markets[0]
        assert market.choice_group is None

        choices = {choice.choice_name: choice for choice in market.choices}
        assert set(choices) == {"1", "x", "2"}

        choice_1 = choices["1"]
        back = (
            session.query(MarketChoiceQuote)
            .filter(
                MarketChoiceQuote.choice_id == choice_1.choice_id,
                MarketChoiceQuote.exchange_side == "back",
            )
            .one()
        )
        lay = (
            session.query(MarketChoiceQuote)
            .filter(
                MarketChoiceQuote.choice_id == choice_1.choice_id,
                MarketChoiceQuote.exchange_side == "lay",
            )
            .one()
        )

    # ODDSPORTAL_OPENING_ONLY_POLICY never persists current odds - OddsPortal
    # is the opening-price source only, so only initial_odds is expected here.
    assert back.current_odds is None
    assert float(back.initial_odds) == 1.86
    assert lay.current_odds is None
    assert float(lay.initial_odds) == 1.90


def test_current_only_betfair_logs_why_opening_only_policy_skips_it(
    tmp_path,
    caplog,
):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'current-only.db'}")
    manager.create_tables()
    event_id, betfair_bookie_id = _seed_event_and_betfair_bookie(manager)
    odds_data = MatchOddsData(
        sport="football",
        extractions=[
            MarketExtraction(
                source_group_key="1X2",
                source_period_key="FULL_TIME",
                betfair=BetfairExchangeOdds(
                    back_1="1.90",
                    back_x="3.40",
                    back_2="4.20",
                    lay_1="1.95",
                    lay_x="3.50",
                    lay_2="4.30",
                ),
            )
        ],
    )
    references = OddsPortalIngestionReferenceData(
        canonical_types=CANONICAL_TYPES,
        bookie_ids_by_source_slug=MappingProxyType(
            {"betfair-ex": betfair_bookie_id}
        ),
    )

    with (
        patch(
            "infrastructure.persistence.repositories.market_repository.db_manager",
            manager,
        ),
        caplog.at_level(
            logging.WARNING,
            logger="infrastructure.persistence.repositories.market_repository",
        ),
    ):
        result = MarketOddsIngestionService.save_from_oddsportal_data(
            event_id,
            odds_data,
            reference_data=references,
        )

    assert result.markets_saved == 0
    assert result.choices_saved == 0
    assert "bookmaker=Betfair Exchange" in caplog.text
    assert "bookmaker_slug=betfair-ex" in caplog.text
    assert "reason=required_initial_odds_missing" in caplog.text
    assert "exchange_side': 'back'" in caplog.text
    assert "current_odds': 1.9" in caplog.text
    assert "reason=no_choices_satisfied_write_policy" in caplog.text
    assert "policy=oddsportal_opening_only" in caplog.text
