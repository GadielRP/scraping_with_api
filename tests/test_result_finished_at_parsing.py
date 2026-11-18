"""The final instant is read from the provider only while the payload still shows it."""

from datetime import datetime, timezone

import pytest

from modules.sofascore.results_parser import extract_results_from_response, parse_event_result

# Bournemouth v Manchester City, 19 May 2026: the whistle came at about 20:24 UTC.
FINAL_UPDATE = 1779222283
FINAL_INSTANT = datetime(2026, 5, 19, 20, 24, 43, tzinfo=timezone.utc)
STATUS_FIELDS = ["status.code", "status.description", "status.type"]


def _finished_football(changes=None, status_type="finished"):
    event = {
        "id": 14023936,
        "tournament": {"category": {"sport": {"name": "Football"}}},
        "status": {"code": 100, "description": "Ended", "type": status_type},
        "winnerCode": 3,
        "homeScore": {"current": 1, "display": 1, "normaltime": 1},
        "awayScore": {"current": 1, "display": 1, "normaltime": 1},
    }
    if changes is not None:
        event["changes"] = changes
    return {"event": event}


def test_final_status_change_gives_the_finished_instant():
    parsed = parse_event_result(_finished_football({"changes": [*STATUS_FIELDS, "awayScore.normaltime"],
                                                    "changeTimestamp": FINAL_UPDATE}))

    assert parsed.kind == "finished"
    assert parsed.result["finished_at"] == FINAL_INSTANT
    assert extract_results_from_response(_finished_football(
        {"changes": STATUS_FIELDS, "changeTimestamp": FINAL_UPDATE}))["finished_at"] == FINAL_INSTANT


@pytest.mark.parametrize("changes", [
    # A later edit overwrote the final update: the timestamp no longer marks the end of the event.
    {"changes": ["time.injuryTime2"], "changeTimestamp": FINAL_UPDATE},
    {"changes": ["status.code", "status.description"], "changeTimestamp": FINAL_UPDATE},
    {"changes": STATUS_FIELDS},
    {"changes": STATUS_FIELDS, "changeTimestamp": None},
    {"changes": STATUS_FIELDS, "changeTimestamp": True},
    {"changes": STATUS_FIELDS, "changeTimestamp": "1779222283"},
    {"changeTimestamp": FINAL_UPDATE},
    {},
    None,
])
def test_finished_instant_is_unknown_unless_the_update_changed_the_status_type(changes):
    parsed = parse_event_result(_finished_football(changes))

    assert parsed.kind == "finished"
    assert "finished_at" not in parsed.result


def test_a_later_edit_is_reported_as_the_last_change_not_as_the_end():
    parsed = parse_event_result(_finished_football({"changes": ["homeScore.normaltime"],
                                                    "changeTimestamp": FINAL_UPDATE}))

    assert parsed.result["last_changed_at"] == FINAL_INSTANT
    assert "finished_at" not in parsed.result


def test_event_without_changes_has_no_instant_at_all():
    result = parse_event_result(_finished_football()).result

    assert "finished_at" not in result and "last_changed_at" not in result


def test_unfinished_event_never_carries_a_finished_instant():
    parsed = parse_event_result(_finished_football(
        {"changes": STATUS_FIELDS, "changeTimestamp": FINAL_UPDATE}, status_type="inprogress"))

    assert parsed.kind == "in_progress"
    assert parsed.result is None
