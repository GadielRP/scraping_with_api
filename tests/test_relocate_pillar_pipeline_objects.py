"""Pillar relocation scans its own directory and retains preview and limit behavior."""

import pytest
from scripts.maintenance import relocate_pillar_pipeline_objects as migration


def write_response(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"raw": true}', encoding="utf-8")
    return path


def test_relocates_pillar_event_folder_and_keeps_all_snapshot_files(tmp_path):
    root = tmp_path / "pillar_pipeline_objects"
    original_directory = root / "4004_Home_vs_Away"
    filenames = ["4004_event_context.json", "4004_odds_trajectory_context.json", "4004_streak_analysis.json"]
    for filename in filenames:
        write_response(original_directory / filename)
    responses = migration.collect_responses(tmp_path)
    assert len(responses) == 3
    metadata = {4004: ("Liga-MX-Apertura", "4004_Home_Away", "Football")}
    counts = migration.relocate_responses(responses, metadata, apply=True)
    destination = root / "football" / "liga_mx_apertura" / original_directory.name
    assert counts["relocated"] == 3
    assert not original_directory.exists()
    assert {path.name for path in destination.iterdir()} == set(filenames)
    assert migration.relocate_responses(migration.collect_responses(tmp_path), metadata, apply=True)["already_grouped"] == 3


@pytest.mark.parametrize("apply", [False, True])
def test_pillar_cli_has_independent_scope_and_file_limit(tmp_path, monkeypatch, apply):
    pillar = tmp_path / "pillar_pipeline_objects" / "4004_Home_vs_Away"
    first = write_response(pillar / "4004_event_context.json")
    second = write_response(pillar / "4004_streak_analysis.json")
    oddspapi = write_response(tmp_path / "oddspapi_odds_responses" / "4004_Home_Away" / "4004_id123_odds.json")
    sofascore = write_response(tmp_path / "sofascore_odds_responses" / "4004_9001_t_5.json")
    assert {path for _, path, _ in migration.collect_responses(tmp_path)} == {first, second}
    monkeypatch.setattr(migration, "load_event_debug_paths", lambda ids: {4004: ("league", "4004_Home_Away", "Football")})
    args = ["--debug-dir", str(tmp_path), "--limit", "1"]
    if apply:
        args.append("--apply")
    assert migration.main(args) == 0
    assert first.exists() is (not apply)
    assert second.exists() and oddspapi.exists() and sofascore.exists()
    destination = tmp_path / "pillar_pipeline_objects" / "football" / "league" / pillar.name / first.name
    assert destination.exists() is apply


@pytest.mark.parametrize("limit", ["0", "-1"])
def test_pillar_cli_rejects_nonpositive_limit(limit):
    with pytest.raises(SystemExit) as exc:
        migration.main(["--limit", limit])
    assert exc.value.code == 2
