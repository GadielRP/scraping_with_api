from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from infrastructure.persistence.repositories.market.market_quote_read_policy import (
    QuoteFieldPriority,
    QuoteReadPriorityPolicy,
)
from infrastructure.persistence.repositories.market.market_read_queries import (
    MarketReadQueries,
    project_external_market_quote_rows,
)


POLICY = QuoteReadPriorityPolicy(
    version=1,
    default=QuoteFieldPriority(
        initial=("oddsportal", "oddspapi", "sofascore"),
        current=("oddspapi", "sofascore", "oddsportal"),
    ),
)
NOW = datetime(2026, 8, 12, tzinfo=timezone.utc)


def _row(
    *,
    choice_id: int,
    choice_name: str,
    quote_id: int,
    source: str,
    initial=None,
    current=None,
    side=None,
    level: int = 0,
    market_id: int = 100,
):
    return {
        "event_id": 7,
        "sport": "Football",
        "market_id": market_id,
        "bookie_id": 9,
        "bookie_name": "Example Book",
        "market_name": "Match Result",
        "market_group": "1X2",
        "market_period": "Full Time",
        "choice_group": None,
        "is_live": False,
        "choice_id": choice_id,
        "choice_name": choice_name,
        "quote_id": quote_id,
        "source": source,
        "exchange_side": side,
        "exchange_level": level,
        "initial": None if initial is None else Decimal(str(initial)),
        "initial_captured_at": NOW,
        "current": None if current is None else Decimal(str(current)),
        "current_updated_at": NOW,
    }


def test_normal_market_merges_fields_by_priority_and_keeps_provenance():
    rows = [
        _row(choice_id=1, choice_name="1", quote_id=10, source="oddsportal", initial="2.10"),
        _row(choice_id=1, choice_name="1", quote_id=11, source="oddspapi", current="1.95"),
        _row(choice_id=2, choice_name="2", quote_id=12, source="oddsportal", initial="1.80"),
        _row(choice_id=2, choice_name="2", quote_id=13, source="oddspapi", current="1.90"),
    ]

    result = project_external_market_quote_rows(7, rows, POLICY)

    assert not result.diagnostics
    assert len(result.blocks) == 1
    block = result.blocks[0]
    assert block.aggregation == "field_priority"
    assert block.source is None
    assert block.contributing_sources == ("oddspapi", "oddsportal")
    assert [choice.choice_name for choice in block.choices] == ["1", "2"]
    home, away = block.choices
    assert (home.initial, home.current, home.movement) == (
        Decimal("2.10"),
        Decimal("1.95"),
        -1,
    )
    assert home.initial_origin.quote_id == 10
    assert home.current_origin.quote_id == 11
    assert away.movement == 1


def test_exchange_keeps_sources_and_sides_separate_and_suppresses_unsided():
    rows = []
    quote_id = 20
    for source in ("oddsportal", "oddspapi"):
        for side in ("back", "lay"):
            rows.append(
                _row(
                    choice_id=1,
                    choice_name="1",
                    quote_id=quote_id,
                    source=source,
                    side=side,
                    initial="2.00",
                    current=None if source == "oddsportal" else "1.99",
                )
            )
            quote_id += 1
    rows.append(
        _row(
            choice_id=1,
            choice_name="1",
            quote_id=99,
            source="oddsportal",
            initial="2.00",
        )
    )

    result = project_external_market_quote_rows(7, rows, POLICY)

    assert [(block.source, block.exchange_side) for block in result.blocks] == [
        ("oddspapi", "back"),
        ("oddspapi", "lay"),
        ("oddsportal", "back"),
        ("oddsportal", "lay"),
    ]
    assert {block.choices[0].initial_origin.quote_id for block in result.blocks} == {
        20,
        21,
        22,
        23,
    }
    opening_only = [block for block in result.blocks if block.source == "oddsportal"]
    assert all(block.choices[0].movement is None for block in opening_only)
    diagnostic = next(item for item in result.diagnostics if item.code == "redundant_unsided_quote_suppressed")
    assert diagnostic.blocking is False
    assert diagnostic.quote_ids == (99,)


def test_top_of_book_uses_lowest_level_without_mixing_fields():
    rows = [
        _row(choice_id=1, choice_name="1", quote_id=31, source="oddspapi", side="back", level=1, initial="4", current="5"),
        _row(choice_id=1, choice_name="1", quote_id=30, source="oddspapi", side="back", level=0, initial="2", current="3"),
    ]

    result = project_external_market_quote_rows(7, rows, POLICY)

    assert len(result.blocks) == 1
    choice = result.blocks[0].choices[0]
    assert choice.exchange_level == 0
    assert choice.initial_origin.quote_id == 30
    assert (choice.initial, choice.current) == (Decimal("2"), Decimal("3"))


def test_duplicate_identity_is_blocking_and_omits_choice():
    rows = [
        _row(choice_id=1, choice_name="1", quote_id=40, source="oddspapi", current="2"),
        _row(choice_id=1, choice_name="1", quote_id=41, source="oddspapi", current="3"),
    ]

    result = project_external_market_quote_rows(7, rows, POLICY)

    assert result.blocks == ()
    assert result.has_blocking_diagnostics
    assert result.diagnostics[0].code == "unexpected_duplicate"
    assert result.diagnostics[0].quote_ids == (40, 41)


def test_unconfigured_source_is_last_resort_and_reported():
    rows = [
        _row(choice_id=1, choice_name="1", quote_id=50, source="zz-provider", current="2.2"),
        _row(choice_id=1, choice_name="1", quote_id=51, source="oddsportal", initial="2.4"),
    ]

    result = project_external_market_quote_rows(7, rows, POLICY)

    choice = result.blocks[0].choices[0]
    assert choice.initial_origin.source == "oddsportal"
    assert choice.current_origin.source == "zz-provider"
    assert any(item.code == "unconfigured_source_fallback" for item in result.diagnostics)


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.execute_count = 0

    def execute(self, _query):
        self.execute_count += 1
        return _Rows(self.rows)


class _SessionContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, *_args):
        return False


def test_repository_materializes_quote_read_with_one_statement(monkeypatch):
    session = _Session([
        _row(choice_id=1, choice_name="1", quote_id=60, source="oddspapi", current="1.9")
    ])
    monkeypatch.setattr(
        "infrastructure.persistence.repositories.market.market_read_queries.db_manager.get_session",
        lambda: _SessionContext(session),
    )

    result = MarketReadQueries.get_external_market_quotes_for_event(7, POLICY)

    assert session.execute_count == 1
    assert len(result.blocks) == 1


class _ScalarResult:
    def scalar_one(self):
        return True


class _AvailabilitySession:
    statement = None

    def execute(self, statement):
        self.statement = str(statement)
        return _ScalarResult()


def test_availability_uses_select_exists(monkeypatch):
    session = _AvailabilitySession()
    monkeypatch.setattr(
        "infrastructure.persistence.repositories.market.market_read_queries.db_manager.get_session",
        lambda: _SessionContext(session),
    )

    assert MarketReadQueries.has_external_market_quotes_for_event(7) is True
    assert "EXISTS" in session.statement
