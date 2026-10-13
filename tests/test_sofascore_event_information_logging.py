"""Information-sync logs must describe the classification actually persisted."""

import logging
from types import SimpleNamespace

import pytest

from modules.sofascore import event_details


@pytest.mark.parametrize("persisted_round", ["regular_season", "knockouts/playoffs"])
def test_information_sync_logs_persisted_and_incoming_round(
    monkeypatch, caplog, persisted_round
):
    payload = {
        "event": {
            "id": 16416297,
            "season_id": 97268,
            "round": "regular_season",
            "season_year": 2026,
        }
    }
    monkeypatch.setattr(
        event_details, "normalize_event_payload", lambda *args, **kwargs: payload
    )
    monkeypatch.setattr(
        event_details.EventRepository, "upsert_event",
        lambda data: SimpleNamespace(id=303597, round=persisted_round),
    )
    with caplog.at_level(logging.INFO, logger=event_details.__name__):
        assert event_details.update_event_information_from_response(
            {"event": {"id": 16416297}}
        )
    assert "canonical_event_id=303597" in caplog.text
    assert f", round={persisted_round}, incoming_round=regular_season," in caplog.text
