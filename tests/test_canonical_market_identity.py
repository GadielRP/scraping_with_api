from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from infrastructure.persistence.catalogs.canonical_market_types import (
    CANONICAL_MARKET_TYPE_IDS,
)
from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Bookie, Event, Market
from infrastructure.persistence.repositories.market.market_quote_read_policy import (
    QuoteFieldPriority,
    QuoteReadPriorityPolicy,
)
from infrastructure.persistence.repositories.market.market_read_queries import (
    MarketReadQueries,
)
from infrastructure.persistence.repositories.market_repository import MarketRepository


POLICY = QuoteReadPriorityPolicy(
    version=1,
    default=QuoteFieldPriority(
        initial=("provider-a",),
        current=("provider-a",),
    ),
)


def _setup(manager: DatabaseManager) -> tuple[int, int]:
    with manager.get_session() as session:
        event = Event(
            slug="canonical-market-identity",
            starts_at=datetime(2026, 9, 20, 18, 0, tzinfo=timezone.utc),
            sport="Football",
            competition="Test League",
            home_team="Home",
            away_team="Away",
        )
        internal = Bookie(name="Internal", slug="internal")
        bookie = Bookie(name="External", slug="external")
        session.add_all([event, internal, bookie])
        session.flush()
        return int(event.id), int(bookie.bookie_id)


def _batch(bookie_id: int, *, line_value=None) -> list[dict]:
    return [
        {
            "bookie_id": bookie_id,
            "markets": [
                {
                    "canonicalMarketKey": "1x2_full_time",
                    "marketName": "provider label",
                    "marketGroup": "provider group",
                    "marketPeriod": "provider period",
                    "lineValue": line_value,
                    "isLive": False,
                    "choices": [
                        {
                            "name": "1",
                            "initialOdds": "2.10",
                            "currentOdds": "2.00",
                            "sourceMarketId": "provider-market-1",
                            "sourceOutcomeId": "home",
                        }
                    ],
                }
            ],
        }
    ]


def test_canonical_key_is_the_market_identity(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'canonical-identity.db'}")
    manager.create_tables()
    event_id, bookie_id = _setup(manager)

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        first = MarketRepository.save_canonical_bookmaker_batches(
            event_id, _batch(bookie_id), source="provider-a"
        )
        second = MarketRepository.save_canonical_bookmaker_batches(
            event_id, _batch(bookie_id), source="provider-a"
        )

    assert first.markets_saved == second.markets_saved == 1
    with manager.get_session() as session:
        market = session.query(Market).one()
        assert market.market_type_id == CANONICAL_MARKET_TYPE_IDS["1x2_full_time"]
        assert market.canonical_market_type.canonical_market_name == "1X2 Full Time"
        assert market.canonical_market_type.canonical_market_group == "1X2"
        assert market.canonical_market_type.canonical_market_period == "Full Time"


def test_market_reader_projects_catalog_labels(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'canonical-read.db'}")
    manager.create_tables()
    event_id, bookie_id = _setup(manager)

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        MarketRepository.save_canonical_bookmaker_batches(
            event_id, _batch(bookie_id), source="provider-a"
        )

    with patch(
        "infrastructure.persistence.repositories.market.market_read_queries.db_manager",
        manager,
    ):
        result = MarketReadQueries.get_external_market_quotes_for_event(
            event_id, POLICY
        )

    assert len(result.blocks) == 1
    block = result.blocks[0]
    assert block.market_name == "1X2 Full Time"
    assert block.market_group == "1X2"
    assert block.market_period == "Full Time"
    assert block.line_value is None


def test_line_value_is_nullable_numeric_identity(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'line-value.db'}")
    manager.create_tables()
    event_id, bookie_id = _setup(manager)

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        result = MarketRepository.save_canonical_bookmaker_batches(
            event_id,
            _batch(bookie_id, line_value="-2.50"),
            source="provider-a",
        )

    assert result.markets_saved == 1
    with manager.get_session() as session:
        market = session.query(Market).one()
        assert market.line_value == Decimal("-2.50")


def test_different_line_values_are_distinct_canonical_markets(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'line-identity.db'}")
    manager.create_tables()
    event_id, bookie_id = _setup(manager)

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        for line_value in ("-2.5", "-20"):
            MarketRepository.save_canonical_bookmaker_batches(
                event_id, _batch(bookie_id, line_value=line_value), source="provider-a"
            )

    with manager.get_session() as session:
        lines = {Decimal(str(row[0])) for row in session.query(Market.line_value).all()}
    assert lines == {Decimal("-2.5"), Decimal("-20")}


def test_invalid_nonempty_line_value_is_rejected(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'invalid-line-value.db'}")
    manager.create_tables()
    event_id, bookie_id = _setup(manager)

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        result = MarketRepository.save_canonical_bookmaker_batches(
            event_id,
            _batch(bookie_id, line_value="not-a-number"),
            source="provider-a",
        )

    assert result.markets_saved == 0


def test_market_model_contains_only_canonical_identity_columns():
    columns = set(Market.__table__.columns.keys())
    assert {
        "market_id",
        "event_id",
        "bookie_id",
        "market_type_id",
        "line_value",
        "is_live",
        "collected_at",
    } <= columns
    assert not {
        "market_name",
        "market_group",
        "market_period",
        "choice_group",
    } & columns
