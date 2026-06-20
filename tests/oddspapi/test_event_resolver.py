from unittest.mock import patch

from modules.oddspapi.event_resolver import OddspapiEventResolver


REPOSITORY = "modules.oddspapi.event_resolver.EventSourceMappingRepository"


def test_missing_fixture_id_is_skipped_without_database_access():
    with patch(f"{REPOSITORY}.get_event_id_by_source") as lookup, patch(
        f"{REPOSITORY}.upsert_mapping"
    ) as upsert:
        result = OddspapiEventResolver.resolve_from_odds_response({})
    assert result.resolved is False
    assert result.skipped_reason == "missing_oddspapi_fixture_id"
    lookup.assert_not_called()
    upsert.assert_not_called()


def test_existing_oddspapi_mapping_resolves_directly():
    with patch(f"{REPOSITORY}.get_event_id_by_source", return_value=9) as lookup, patch(
        f"{REPOSITORY}.upsert_mapping"
    ) as upsert:
        result = OddspapiEventResolver.resolve_from_odds_response({"fixtureId": "fixture-1"})
    assert result.resolved is True
    assert result.canonical_event_id == 9
    assert result.match_method == "existing_oddspapi_mapping"
    lookup.assert_called_once_with("oddspapi", "fixture-1")
    upsert.assert_not_called()


def test_known_sofascore_id_creates_oddspapi_and_secondary_mappings():
    def lookup(source, source_event_id):
        return 77 if source == "sofascore" and source_event_id == "1234" else None

    payload = {
        "fixtureId": "fixture-2",
        "sportId": 10,
        "tournamentId": 17,
        "seasonId": 2026,
        "externalProviders": {
            "sofascoreId": 1234,
            "pinnacleId": "pin-1",
            "flashscoreId": "flash-1",
            "oddinId": "",
        },
    }
    with patch(f"{REPOSITORY}.get_event_id_by_source", side_effect=lookup), patch(
        f"{REPOSITORY}.upsert_mapping"
    ) as upsert:
        result = OddspapiEventResolver.resolve_from_odds_response(payload)

    assert result.resolved is True
    assert result.canonical_event_id == 77
    assert result.created_mappings == ["oddspapi", "pinnacle", "flashscore"]
    assert [call.kwargs["source"] for call in upsert.call_args_list] == [
        "oddspapi",
        "pinnacle",
        "flashscore",
    ]
    assert upsert.call_args_list[0].kwargs["event_id"] == 77


def test_missing_sofascore_id_is_skipped():
    with patch(f"{REPOSITORY}.get_event_id_by_source", return_value=None), patch(
        f"{REPOSITORY}.upsert_mapping"
    ) as upsert:
        result = OddspapiEventResolver.resolve_from_odds_response({"fixtureId": "fixture-3"})
    assert result.resolved is False
    assert result.skipped_reason == "missing_sofascore_external_provider_id"
    upsert.assert_not_called()


def test_unknown_sofascore_mapping_is_skipped():
    with patch(f"{REPOSITORY}.get_event_id_by_source", return_value=None), patch(
        f"{REPOSITORY}.upsert_mapping"
    ) as upsert:
        result = OddspapiEventResolver.resolve_from_odds_response(
            {"fixtureId": "fixture-4", "externalProviders": {"sofascoreId": 999}}
        )
    assert result.resolved is False
    assert result.skipped_reason == "sofascore_mapping_not_found"
    upsert.assert_not_called()


def test_dry_resolution_does_not_create_mappings_or_events():
    with patch(
        f"{REPOSITORY}.get_event_id_by_source", side_effect=[None, 81]
    ), patch(f"{REPOSITORY}.upsert_mapping") as upsert:
        result = OddspapiEventResolver.resolve_from_fixture_response(
            {"fixtureId": "fixture-5", "externalProviders": {"sofascoreId": 100}},
            create_mappings=False,
        )
    assert result.resolved is True
    assert result.canonical_event_id == 81
    assert result.created_mappings == []
    upsert.assert_not_called()
