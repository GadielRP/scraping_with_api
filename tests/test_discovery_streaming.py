from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone
from types import SimpleNamespace
import json
import pytest
from infrastructure.persistence.transient.discovery_run_store import DiscoveryRunStore
from infrastructure.network.json_document import document_entries, JsonDocument
from infrastructure.network import json_document as streaming
from infrastructure.settings.job_execution import JobExecutionSettings


def test_temporary_body_stays_linked_during_resets_and_is_removed_on_close(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    paths = []

    class Client:
        def download_json(self, endpoint, document, params=None):
            path = Path(document.file.name)
            paths.append(path)
            assert path.is_file()
            document.reset()
            document.write(b'{"events":[{"broken":')
            document.reset()
            document.write(b'{"events":[{"id":1}]}')

    with streaming.open_json_document(Client(), "events") as document:
        assert paths[0].is_file()
        assert list(document_entries(document, "events")) == [{"id": 1}]
    assert document.file.closed
    assert not paths[0].exists()


def test_failed_download_removes_its_named_temporary_body(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    paths = []

    class Client:
        def download_json(self, endpoint, document, params=None):
            paths.append(Path(document.file.name))
            document.reset()
            document.write(b"partial")
            raise RuntimeError("download failed")

    with pytest.raises(RuntimeError, match="download failed"):
        with streaming.open_json_document(Client(), "events"):
            pytest.fail("Failed downloads must not reach parsing")
    assert not paths[0].exists()


def test_document_entries_validates_empty_missing_and_truncated_collections():
    assert list(document_entries(BytesIO(b'{"events":[]}'), "events")) == []
    with pytest.raises(ValueError, match="missing required"):
        list(document_entries(BytesIO(b'{"wrong":[]}'), "events"))
    with pytest.raises(Exception):
        list(document_entries(BytesIO(b'{"events":[{"id":1}],'), "events"))


def test_streams_member_keys_nested_payloads_and_pagination_controls():
    controls = {"hasNextPage": False}
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
    body = JsonDocument(file, 5)
    assert body.write(b"1234") == 4
    with pytest.raises(ValueError, match="byte budget"):
        body.write(b"56")
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
        assert store.first_seen("oddspapi:10", "fixture-1")
        assert not store.first_seen("oddspapi:10", "fixture-1")
        assert store.first_seen("oddspapi:11", "fixture-1")
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
