from unittest.mock import MagicMock

import pytest

from infrastructure.persistence.backfill.binary_choice_structure_backfill import (
    DEFAULT_BINARY_CHOICE_SPORTS,
    DRAW_CHOICE_NAMES,
    STRATEGY_NAME,
    BinaryChoiceStructureBackfillService,
)
from infrastructure.persistence.backfill.canonical_period_backfill import (
    FULL_TIME_PERIOD_VARIANT_PAIRS,
    BackfillScope,
)
from infrastructure.persistence.backfill.registry import create_strategy
from infrastructure.persistence.odds_models import Market, MarketChoice


def test_binary_choice_structure_service_default_scope():
    scope = BackfillScope()
    service = BinaryChoiceStructureBackfillService(scope=scope)

    assert service.strategy_name == STRATEGY_NAME
    assert service.scope.sport_names == DEFAULT_BINARY_CHOICE_SPORTS
    assert service.period_pairs == FULL_TIME_PERIOD_VARIANT_PAIRS
    assert service.require_binary_home_away is False


def test_binary_choice_structure_service_custom_sports():
    scope = BackfillScope(sport_names=frozenset({"Ice Hockey", "Handball"}))
    service = BinaryChoiceStructureBackfillService(scope=scope)

    assert service.scope.sport_names == frozenset({"ice hockey", "handball"})


def test_binary_choice_structure_service_registry_creation():
    scope = BackfillScope(sport_names=frozenset({"ice hockey"}))
    strategy = create_strategy(
        STRATEGY_NAME,
        scope=scope,
        parameters={
            "period_pairs": {5: 8, 12: 14},
            "require_binary_home_away": True,
        },
    )

    assert isinstance(strategy, BinaryChoiceStructureBackfillService)
    assert strategy.period_pairs == {5: 8, 12: 14}
    assert strategy.require_binary_home_away is True


def test_validate_home_away_markets_valid_binary():
    scope = BackfillScope(sport_names=frozenset({"ice hockey"}))
    service = BinaryChoiceStructureBackfillService(scope=scope)

    market_5 = Market(market_id=101, market_type_id=5)
    market_5.choices = [
        MarketChoice(choice_id=1, choice_name="1"),
        MarketChoice(choice_id=2, choice_name="2"),
    ]

    detail = service._validate_home_away_markets([market_5], event_id=2001)
    assert detail is None


def test_validate_home_away_markets_rejects_3_way():
    scope = BackfillScope(sport_names=frozenset({"ice hockey"}))
    service = BinaryChoiceStructureBackfillService(scope=scope)

    market_5 = Market(market_id=101, market_type_id=5)
    market_5.choices = [
        MarketChoice(choice_id=1, choice_name="1"),
        MarketChoice(choice_id=2, choice_name="X"),
        MarketChoice(choice_id=3, choice_name="2"),
    ]

    detail = service._validate_home_away_markets([market_5], event_id=2001)
    assert detail is not None
    assert "has 3 choices; expected exactly 2" in detail


def test_validate_home_away_markets_rejects_draw_choice_name():
    scope = BackfillScope(sport_names=frozenset({"ice hockey"}))
    service = BinaryChoiceStructureBackfillService(scope=scope)

    market_5 = Market(market_id=101, market_type_id=5)
    market_5.choices = [
        MarketChoice(choice_id=1, choice_name="Home"),
        MarketChoice(choice_id=2, choice_name="Draw"),
    ]

    detail = service._validate_home_away_markets([market_5], event_id=2001)
    assert detail is not None
    assert "contains draw choice 'Draw'" in detail


def test_validate_home_away_markets_require_anchor():
    scope = BackfillScope(sport_names=frozenset({"ice hockey"}))
    service = BinaryChoiceStructureBackfillService(
        scope=scope, require_binary_home_away=True
    )

    # Event only has Over/Under (12), no Home/Away (5 or 8)
    market_12 = Market(market_id=102, market_type_id=12)
    market_12.choices = [
        MarketChoice(choice_id=1, choice_name="Over"),
        MarketChoice(choice_id=2, choice_name="Under"),
    ]

    detail = service._validate_home_away_markets([market_12], event_id=2001)
    assert detail is not None
    assert "no Home/Away market" in detail


def test_event_guard_detail_result_draw():
    scope = BackfillScope(sport_names=frozenset({"ice hockey"}))
    service = BinaryChoiceStructureBackfillService(scope=scope)

    mock_session = MagicMock()
    mock_query = mock_session.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_filter.scalar.return_value = "X"

    detail = service._event_guard_detail(mock_session, event_id=999)
    assert detail is not None
    assert "winner='X'" in detail
