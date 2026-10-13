from infrastructure.settings.config import (
    _parse_pillar_toggle_map,
)


def test_pillar_toggle_map_defaults_all_known_pillars_to_enabled(monkeypatch) -> None:
    for pillar_key in ("pillar_1", "pillar_2", "pillar_3", "pillar_4", "pillar_5"):
        monkeypatch.delenv(
            f"TEST_PIPELINE_{pillar_key.upper()}_ENABLED",
            raising=False,
        )

    assert _parse_pillar_toggle_map(
        env_prefix="TEST_PIPELINE",
    ) == {
        "pillar_1": True,
        "pillar_2": True,
        "pillar_3": True,
        "pillar_4": True,
        "pillar_5": True,
    }


def test_pillar_toggle_map_supports_individual_rollouts(monkeypatch) -> None:
    monkeypatch.setenv("TEST_PIPELINE_PILLAR_1_ENABLED", "false")
    monkeypatch.setenv("TEST_PIPELINE_PILLAR_2_ENABLED", "0")
    monkeypatch.setenv("TEST_PIPELINE_PILLAR_3_ENABLED", "off")
    monkeypatch.setenv("TEST_PIPELINE_PILLAR_4_ENABLED", "yes")

    assert _parse_pillar_toggle_map(
        env_prefix="TEST_PIPELINE",
    ) == {
        "pillar_1": False,
        "pillar_2": False,
        "pillar_3": False,
        "pillar_4": True,
        "pillar_5": True,
    }
