from __future__ import annotations

import json

import pytest

from infrastructure.persistence.repositories.market.market_quote_read_policy import (
    load_quote_read_priority_policy,
)


def _write_policy(tmp_path, overrides):
    path = tmp_path / "policy.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "default": {
                    "initial": ["OddsPortal", "Oddspapi"],
                    "current": ["Oddspapi", "SofaScore"],
                },
                "overrides": overrides,
            }
        ),
        encoding="utf-8",
    )
    return path


def test_policy_normalizes_sources_and_resolves_most_specific_override(tmp_path):
    path = _write_policy(
        tmp_path,
        [
            {"sport": "Football", "initial": ["sofascore"], "current": ["sofascore"]},
            {"bookie_id": 9, "initial": ["oddspapi"], "current": ["oddspapi"]},
            {"sport": "Football", "bookie_id": 9, "initial": ["oddsportal"], "current": ["oddsportal"]},
        ],
    )

    policy = load_quote_read_priority_policy(str(path))

    assert policy.default.initial == ("oddsportal", "oddspapi")
    assert policy.resolve(sport="football", bookie_id=9).current == ("oddsportal",)
    assert policy.resolve(sport="Basketball", bookie_id=9).current == ("oddspapi",)


@pytest.mark.parametrize(
    "overrides",
    [
        [{"sport": "Football", "initial": [], "current": ["sofascore"]}],
        [{"sport": "Football", "initial": ["sofascore", "SOFASCORE"], "current": ["sofascore"]}],
        [
            {"sport": "Football", "initial": ["sofascore"], "current": ["sofascore"]},
            {"sport": "football", "initial": ["oddspapi"], "current": ["oddspapi"]},
        ],
    ],
)
def test_policy_rejects_invalid_priority_or_duplicate_scope(tmp_path, overrides):
    path = _write_policy(tmp_path, overrides)
    with pytest.raises(ValueError):
        load_quote_read_priority_policy(str(path))
