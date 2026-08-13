from __future__ import annotations

from decimal import Decimal

import pytest

from infrastructure.persistence.repositories.market.market_read_models import (
    ExternalChoiceQuote,
    ExternalMarketQuoteBlock,
)
from modules.alerts.alerts_formatter.odds_alert import (
    _format_external_markets_section,
)


def _choice(name, initial, current, movement, *, level=None):
    return ExternalChoiceQuote(
        choice_id=1,
        choice_name=name,
        exchange_level=level,
        initial=None if initial is None else Decimal(initial),
        current=None if current is None else Decimal(current),
        movement=movement,
        initial_origin=None,
        current_origin=None,
    )


def _block(
    *,
    aggregation="field_priority",
    source=None,
    side=None,
    choices=(),
    bookie="Pinnacle",
    choice_group=None,
):
    return ExternalMarketQuoteBlock(
        market_id=100,
        bookie_id=5,
        bookie_name=bookie,
        market_name="Match Result",
        market_group="1X2",
        market_period="Full Time",
        choice_group=choice_group,
        is_live=False,
        aggregation=aggregation,
        source=source,
        exchange_side=side,
        contributing_sources=(source,) if source else ("oddspapi", "oddsportal"),
        choices=tuple(choices),
    )


def test_consolidated_quote_block_renders_selected_fields_and_movements():
    message = _format_external_markets_section(
        [
            _block(
                choices=(
                    _choice("1", "2.10", "1.95", -1),
                    _choice("X", "3.20", "3.20", 0),
                    _choice("2", "4.00", "4.25", 1),
                )
            )
        ]
    )

    assert "CONSOLIDATED ODDS" in message
    assert "Pinnacle: 2.10\u21921.95\u2193 | 3.20\u21923.20= | 4.00\u21924.25\u2191" in message
    assert "ODDSPORTAL ODDS" not in message


def test_exchange_uses_explicit_source_and_side_and_preserves_opening_only():
    message = _format_external_markets_section(
        [
            _block(
                aggregation="exchange",
                source="oddsportal",
                side="back",
                bookie="Betfair Exchange",
                choices=(_choice("1", "1.89", None, None, level=0),),
            ),
            _block(
                aggregation="exchange",
                source="oddsportal",
                side="lay",
                bookie="Betfair Exchange",
                choices=(_choice("1", "1.91", None, None, level=0),),
            ),
        ]
    )

    assert "ODDSPORTAL EXCHANGE ODDS" in message
    assert "Betfair Exchange (Back): 1.89\u2192N/A" in message
    assert "Betfair Exchange (Lay): 1.91\u2192N/A" in message


def test_current_only_does_not_fabricate_movement():
    message = _format_external_markets_section(
        [_block(choices=(_choice("1", None, "2.05", None),))]
    )
    assert "Pinnacle: 2.05" in message
    assert "2.05=" not in message


def test_formatter_rejects_legacy_dict_contract():
    with pytest.raises(TypeError):
        _format_external_markets_section([{"choices": []}])
