from __future__ import annotations

import json

from modules.oddspapi.fixture_response_debug_writer import (
    OddspapiFixtureResponseDebugWriter,
)


def test_reports_each_field_that_prevents_participant_persistence():
    issues = OddspapiFixtureResponseDebugWriter.issues_for_payload(
        {
            "fixtureId": "fixture-1",
            "participant1Id": "not-a-number",
            "participant1Name": "Home",
            "participant2Id": 22,
            "participant2Name": " ",
            "participant2ShortName": "Away",
        }
    )

    assert issues == ["participant1_invalid_id", "participant2_missing_name"]


def test_does_not_capture_complete_participants():
    assert OddspapiFixtureResponseDebugWriter.issues_for_payload(
        {
            "fixtureId": "fixture-1",
            "participant1Id": 11,
            "participant1Name": "Home",
            "participant2Id": 22,
            "participant2Name": "Away",
        }
    ) == []


def test_saves_raw_fixture_and_diagnostics_under_configured_directory(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("ODDSPAPI_EMPTY_PARTICIPANT_DEBUG_DIR", str(tmp_path))
    raw_fixture = {
        "fixtureId": "fixture/1",
        "participant1Id": "11",
        "participant1Name": "Home",
        "participant2Id": None,
        "participant2Name": "Away",
        "providerField": {"preserved": True},
    }

    path = OddspapiFixtureResponseDebugWriter.save_if_incomplete(raw_fixture)

    assert path is not None
    assert path.parent.parent == tmp_path
    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["fixture_id"] == "fixture/1"
    assert record["issues"] == ["participant2_missing_id"]
    assert record["raw_response"] == raw_fixture

