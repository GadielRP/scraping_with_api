import pytest

from infrastructure.settings.config import _parse_env_choice


def test_pillar_mining_status_choice_is_case_insensitive(monkeypatch) -> None:
    monkeypatch.setenv("TEST_PILLAR_MINING_MODE", "ACTIVE_ONLY")

    assert _parse_env_choice(
        "TEST_PILLAR_MINING_MODE",
        "all",
        {"all", "active_only"},
    ) == "active_only"


def test_pillar_mining_status_choice_rejects_unknown_value(monkeypatch) -> None:
    monkeypatch.setenv("TEST_PILLAR_MINING_MODE", "sometimes")

    with pytest.raises(ValueError, match="TEST_PILLAR_MINING_MODE"):
        _parse_env_choice(
            "TEST_PILLAR_MINING_MODE",
            "all",
            {"all", "active_only"},
        )
