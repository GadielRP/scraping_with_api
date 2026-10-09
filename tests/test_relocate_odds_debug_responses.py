"""Verify debug relocation preserves files and uses bulk canonical ID lookups."""

import pytest

from contextlib import nullcontext
from unittest.mock import MagicMock
from types import SimpleNamespace

from scripts.maintenance import relocate_odds_debug_responses as migration


def write_response(path, content='{"raw": true}'):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_collects_legacy_event_directories_and_canonical_sofascore_filenames(tmp_path):
    oddspapi = write_response(tmp_path / "oddspapi_odds_responses" / "238868_Home_Away" / "238868_id123_t_5_odds.json")
    sofascore = write_response(tmp_path / "sofascore_odds_responses" / "238868_9001_t_5.json")
    grouped = write_response(tmp_path / "sofascore_odds_responses" / "liga_mx_apertura" / "238869_9002_t_5.json")
    write_response(tmp_path / "sofascore_odds_responses" / "16317959_odds_reponse.json")

    write_response(tmp_path / "pillar_pipeline_objects" / "4004_Home_vs_Away" / "4004_event_context.json")
    responses = migration.collect_responses(tmp_path)
    assert {path: event_id for _, path, event_id in responses} == {
        oddspapi: 238868, sofascore: 238868, grouped: 238869,
    }


def test_bulk_lookup_collects_all_event_ids_before_querying(monkeypatch):
    session = MagicMock()
    queried_ids = []

    def filter_batch(expression):
        ids = expression.right.value
        queried_ids.extend(ids)
        result = MagicMock()
        result.all.return_value = [(SimpleNamespace(id=event_id, home_participant=None, away_participant=None, home_team="Home", away_team="Away", slug="home-away", sport="Football"), "liga-mx-apertura") for event_id in ids]
        return result

    session.query.return_value.join.return_value.options.return_value.filter.side_effect = filter_batch
    monkeypatch.setattr(migration.db_manager, "get_session", lambda: nullcontext(session))
    event_ids = set(range(1, 1003))
    assert set(migration.load_event_debug_paths(event_ids)) == event_ids
    assert queried_ids == sorted(event_ids)
    assert session.query.call_count == 2
    assert migration.load_event_debug_paths({1})[1][2] == "Football"


def test_preview_and_apply_preserve_event_folders_and_are_rerunnable(tmp_path):
    oddspapi = write_response(tmp_path / "oddspapi_odds_responses" / "238868_Home_Away" / "238868_id123_t_5_odds.json")
    sofascore = write_response(tmp_path / "sofascore_odds_responses" / "238868_9001_t_5.json")
    responses = migration.collect_responses(tmp_path)
    slugs = {238868: ("Liga-MX-Apertura", "238868_Home_Away", "Football")}

    assert migration.relocate_responses(responses, slugs)["relocated"] == 2
    assert oddspapi.exists() and sofascore.exists()
    assert not (oddspapi.parent.parent / "football" / "liga_mx_apertura").exists()
    assert migration.relocate_responses(responses, slugs, apply=True)["relocated"] == 2
    for root, old_path, _ in responses:
        assert (root / "football" / "liga_mx_apertura" / "238868_Home_Away" / old_path.name).read_text(encoding="utf-8") == '{"raw": true}'
        assert not old_path.exists()
    assert not oddspapi.parent.exists()
    assert migration.relocate_responses(migration.collect_responses(tmp_path), slugs, apply=True)["already_grouped"] == 2


def test_unresolved_events_and_destination_conflicts_preserve_sources(tmp_path):
    root = tmp_path / "oddspapi_odds_responses"
    source = write_response(root / "101_Home_Away" / "101_id123_odds.json", "source")
    target = write_response(root / "football" / "liga_mx_apertura" / "101_Home_Away" / source.name, "existing")
    unknown = write_response(root / "102_Home_Away" / "102_id456_odds.json")
    missing_slug = write_response(root / "103_Home_Away" / "103_id789_odds.json")
    responses = migration.collect_responses(tmp_path)
    counts = migration.relocate_responses(responses, {101: ("liga-mx-apertura", "101_Home_Away", "Football"), 103: (None, "103_Home_Away", "Football")}, apply=True)

    assert counts == {"relocated": 0, "already_grouped": 1, "unresolved": 2, "conflicts": 1}
    assert source.read_text(encoding="utf-8") == "source"
    assert target.read_text(encoding="utf-8") == "existing"
    assert unknown.exists() and missing_slug.exists()


def test_repairs_flat_competition_files_and_preserves_existing_event_names(tmp_path):
    root = tmp_path / "oddspapi_odds_responses"
    loose = write_response(root / "nba_preseason" / "379309_id123_odds.json")
    existing = write_response(root / "379309_Old_Home_Away" / "379309_id123_historical.json")
    metadata = {379309: ("nba-preseason", "379309_Cleveland_Cavaliers_Boston_Celtics", "Basketball")}
    counts = migration.relocate_responses(migration.collect_responses(tmp_path), metadata, apply=True)
    assert counts["relocated"] == 2
    assert (root / "basketball" / "nba_preseason" / metadata[379309][1] / loose.name).exists()
    assert (root / "basketball" / "nba_preseason" / existing.parent.name / existing.name).exists()
    assert migration.relocate_responses(migration.collect_responses(tmp_path), metadata, apply=True)["already_grouped"] == 2


@pytest.mark.parametrize("apply", [False, True])
def test_limit_caps_moves_across_providers_and_skips_grouped_files(tmp_path, apply):
    oddspapi = tmp_path / "oddspapi_odds_responses"
    grouped = write_response(oddspapi / "basketball" / "nba_preseason" / "101_Home_Away" / "101_id123_odds.json")
    first = write_response(oddspapi / "102_Home_Away" / "102_id456_odds.json")
    second = write_response(tmp_path / "sofascore_odds_responses" / "103_9001_t_5.json")
    metadata = {event_id: ("nba-preseason", f"{event_id}_Home_Away", "Basketball") for event_id in (101, 102, 103)}
    responses = [(oddspapi, grouped, 101), (oddspapi, first, 102), (second.parent, second, 103)]

    counts = migration.relocate_responses(responses, metadata, apply=apply, limit=1)
    assert counts["already_grouped"] == 1
    assert counts["relocated"] == 1
    assert first.exists() is (not apply)
    assert second.exists()
    assert grouped.exists()
    if apply:
        counts = migration.relocate_responses(migration.collect_responses(tmp_path), metadata, apply=True, limit=1)
        assert counts["relocated"] == 1
        assert not second.exists()


@pytest.mark.parametrize("limit", ["0", "-1"])
def test_cli_rejects_nonpositive_limit(limit):
    with pytest.raises(SystemExit) as exc:
        migration.main(["--limit", limit])
    assert exc.value.code == 2


def test_relocates_existing_competition_event_folder_under_stored_sport(tmp_path):
    root = tmp_path / "sofascore_odds_responses"
    event_folder = "374280_Boston_Bruins_Ottawa_Senators"
    source = write_response(root / "nhl" / event_folder / "374280_16546314_t_120.json")
    metadata = {374280: ("nhl", event_folder, "Ice hockey")}
    counts = migration.relocate_responses(migration.collect_responses(tmp_path), metadata, apply=True)
    destination = root / "ice_hockey" / "nhl" / event_folder / source.name
    assert counts["relocated"] == 1
    assert destination.exists()
    assert not source.exists()
    assert migration.relocate_responses(migration.collect_responses(tmp_path), metadata, apply=True)["already_grouped"] == 1
