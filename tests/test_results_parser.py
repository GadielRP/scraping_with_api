"""Regression coverage for SofaScore results parsing."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from modules.sofascore import event_details
from modules.sofascore.results_parser import (
    extract_results_from_response,
    is_event_status_deletable,
    parse_event_result,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
WALKOVER_DOUBLES_FIXTURE = (
    FIXTURES_DIR / "sofascore_walkover_tennis_doubles_16782259.json"
)


def _load_walkover_doubles_response() -> dict:
    return json.loads(WALKOVER_DOUBLES_FIXTURE.read_text(encoding="utf-8-sig"))


def test_real_walkover_tennis_doubles_payload_is_classified_as_canceled():
    response = _load_walkover_doubles_response()
    event = response["event"]

    assert event["id"] == 16782259
    assert event["status"] == {
        "code": 91,
        "description": "Walkover",
        "type": "finished",
    }
    assert event["homeScore"] == {}
    assert event["awayScore"] == {}

    assert is_event_status_deletable(event) is True

    parsed = parse_event_result(response)
    assert parsed.kind == "canceled"
    assert parsed.result is None
    assert parsed.status_code == 91
    assert parsed.status_type == "finished"
    assert parsed.status_description == "walkover"

    legacy = extract_results_from_response(response)
    assert legacy == {
        "_canceled": True,
        "status_code": 91,
        "status_description": "walkover",
    }


def test_real_walkover_tennis_doubles_payload_is_queued_for_deletion(monkeypatch):
    response = _load_walkover_doubles_response()
    deferred_deletion_event_ids: set[int] = set()
    queued_reasons: list[str] = []

    client = SimpleNamespace(request_json=lambda *_args, **_kwargs: response)

    def _tracking_queue(canonical_event_id, sofascore_event_id, reason, deferred_ids):
        queued_reasons.append(reason)
        deferred_ids.add(canonical_event_id)
        return True

    monkeypatch.setattr(
        event_details,
        "_queue_canonical_event_for_deletion",
        _tracking_queue,
    )

    result = event_details.get_event_results(
        client,
        16782259,
        update_event_info=False,
        canonical_event_id=170220,
        deferred_deletion_event_ids=deferred_deletion_event_ids,
    )

    assert result is None
    assert deferred_deletion_event_ids == {170220}
    assert queued_reasons == ["walkover"]
