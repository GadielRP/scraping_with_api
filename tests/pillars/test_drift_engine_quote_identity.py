from modules.pillars.pillar_4.drift_engine.drift_engine import (
    _build_choice_result_key,
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
