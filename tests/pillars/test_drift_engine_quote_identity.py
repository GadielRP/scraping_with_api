from decimal import Decimal

from modules.pillars.odds_trajectory_context import ChoiceOddsTrajectory
from modules.pillars.pillar_4.drift_engine.drift_engine import (
    _build_choice_result_key,
    _get_required_inputs,
)


def test_two_sources_by_two_sides_produce_four_distinct_drift_keys():
    keys = {
        _build_choice_result_key(
            market_name="Match Result",
            choice_group=None,
            bookie_name="Betfair Exchange",
            choice_name="1",
            source=source,
            exchange_side=side,
            exchange_level=0,
            quote_id=quote_id,
        )
        for quote_id, (source, side) in enumerate(
            (
                ("oddspapi", "back"),
                ("oddspapi", "lay"),
                ("oddsportal", "back"),
                ("oddsportal", "lay"),
            ),
            start=700,
        )
    }

    assert len(keys) == 4
    assert any("|oddspapi|back|0|700|" in key for key in keys)
    assert any("|oddspapi|lay|0|701|" in key for key in keys)
    assert any("|oddsportal|back|0|702|" in key for key in keys)
    assert any("|oddsportal|lay|0|703|" in key for key in keys)


def test_drift_closing_input_uses_t_minus_one_instead_of_zero():
    inputs = _get_required_inputs(
        ChoiceOddsTrajectory(
            choice_name="1",
            choice_id=1,
            initial_odds=Decimal("2.00"),
            odds_values={
                120: Decimal("1.98"),
                30: Decimal("1.95"),
                5: Decimal("1.92"),
                1: Decimal("1.90"),
                0: Decimal("9.99"),
            },
            meta_by_minute={},
        )
    )

    assert inputs["kickoff_odds"] == Decimal("1.90")
