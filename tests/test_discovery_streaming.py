from io import BytesIO
from datetime import datetime, timezone
from types import SimpleNamespace
import json
import pytest
from infrastructure.persistence.transient.discovery_run_store import DiscoveryRunStore
from modules.sofascore.streaming import document_entries, BoundedBody
from modules.sofascore import streaming
from infrastructure.settings.job_execution import JobExecutionSettings


def test_document_entries_validates_empty_missing_and_truncated_collections():
    assert list(document_entries(BytesIO(b'{"events":[]}'), "events")) == []
    with pytest.raises(ValueError, match="missing required"):
        list(document_entries(BytesIO(b'{"wrong":[]}'), "events"))
    with pytest.raises(Exception):
        list(document_entries(BytesIO(b'{"events":[{"id":1}],'), "events"))


def test_streams_member_keys_nested_payloads_and_pagination_controls():
    controls = {}
    data = {"scheduled": [{"a": [1, 2]}], "hasNextPage": True}
    assert list(
        document_entries(BytesIO(json.dumps(data).encode()), "scheduled", controls=controls)
    ) == [{"a": [1, 2]}]
    assert controls == {"hasNextPage": True}
    assert list(
        document_entries(BytesIO(b'{"odds":{"1":{"markets":[]},"2":null}}'), "odds", mapping=True)
    ) == [("1", {"markets": []}), ("2", None)]


def test_response_limit_aborts_without_materializing_body():
    file = BytesIO()
    body = BoundedBody(file, 5)
    assert body.write(b"1234") == 4
    assert body.write(b"56") == 0
    assert isinstance(body.error, ValueError)
    assert file.getvalue() == b"1234"


def test_disk_membership_deduplicates_and_keeps_final_calendar():
    with DiscoveryRunStore() as store:
        event = SimpleNamespace(
            id=1, sport="Football", starts_at=datetime(2026, 10, 3, 12, tzinfo=timezone.utc)
        )
        store.record_mapping({"99": event})
        store.record_mapping({"99": event})
        assert store.source_members(["99", "missing"]) == {"99": 1}
        assert list(store.counts()) == [("Football", "2026-10-03", 1)]
        event.starts_at = datetime(2026, 10, 4, 12, tzinfo=timezone.utc)
        store.record_mapping({"99": event})
        assert list(store.counts()) == [("Football", "2026-10-04", 1)]
        assert store.first_tournament("football", 1)
        assert not store.first_tournament("football", 1)
        store.record_mapping({"100": event})
        assert store.source_members(["99", "100"]) == {"99": 1, "100": 1}


def test_oversized_unknown_scalar_is_rejected_before_sax_materialization(monkeypatch):
    monkeypatch.setattr(streaming, "JobExecutionSettings", lambda: JobExecutionSettings(json_item_max_bytes=1024))
    with pytest.raises(ValueError, match="token exceeds"):
        list(document_entries(BytesIO(b'{"ignored":"' + b"x" * 2048 + b'","events":[]}'), "events"))


def test_unknown_nested_field_is_bounded_before_sax_builds_prefixes(monkeypatch):
    monkeypatch.setattr(streaming, "JobExecutionSettings", lambda: JobExecutionSettings(json_max_depth=16))
    raw = b'{"ignored":' + b"[" * 20 + b"0" + b"]" * 20 + b',"events":[]}'
    with pytest.raises(ValueError, match="nesting exceeds"):
        list(document_entries(BytesIO(raw), "events"))


def test_token_validation_keeps_escape_state_across_transport_chunks(monkeypatch):
    class Fragmented(BytesIO):
        def read(self, size=-1):
            return super().read(min(size, 3) if size >= 0 else 3)

    monkeypatch.setattr(streaming, "JobExecutionSettings", lambda: JobExecutionSettings(json_max_depth=2))
    label = 'escaped " and \\ {{{[[['
    raw = json.dumps({"events": [label]}).encode()
    assert list(document_entries(Fragmented(raw), "events")) == [label]


def test_abandoned_cleanup_skips_active_owner(tmp_path, monkeypatch):
    import os
    import time
    from infrastructure.persistence.transient import run_directory

    monkeypatch.setattr(run_directory, "RUNTIME_DIRECTORY", tmp_path)
    active = run_directory.RunDirectory()
    abandoned = run_directory.RunDirectory()
    abandoned._lease.__exit__(None, None, None)  # Simulate a dead owner releasing its OS lock.
    for path in (active.path, abandoned.path):
        os.utime(path, (time.time() - 90000, time.time() - 90000))
    try:
        assert run_directory.clean_abandoned_runs() == 1
        assert active.path.exists()
        assert not abandoned.path.exists()
    finally:
        active.close()
