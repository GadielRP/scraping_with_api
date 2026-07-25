import json
import os
import time

import pytest

import shared.runtime_observability as runtime_observability


def test_observe_operation_captures_short_peak_and_headroom(monkeypatch, tmp_path):
    rss_values = iter([100.0, 120.0, 340.0, 150.0, 150.0])
    cgroup_values = iter([180.0, 200.0, 410.0, 230.0, 230.0])

    def fake_rss():
        try:
            return next(rss_values)
        except StopIteration:
            return 150.0

    def fake_cgroup_snapshot():
        try:
            current_mb = next(cgroup_values)
        except StopIteration:
            current_mb = 230.0
        return {
            "current_mb": current_mb,
            "kernel_peak_mb": current_mb,
            "anon_mb": 160.0,
            "file_mb": 40.0,
            "oom": 0,
            "oom_kill": 0,
        }

    state_path = tmp_path / "runtime_state.json"
    monkeypatch.setattr(runtime_observability, "_STATE_PATH", state_path)
    monkeypatch.setattr(runtime_observability, "get_rss_mb", fake_rss)
    monkeypatch.setattr(
        runtime_observability,
        "get_cgroup_memory_snapshot",
        fake_cgroup_snapshot,
    )
    monkeypatch.setattr(
        runtime_observability,
        "get_memory_limit_mb",
        lambda: 1024.0,
    )
    runtime_observability._STATE.clear()

    with runtime_observability.observe_operation("memory-test"):
        time.sleep(0.35)

    assert runtime_observability._STATE["last_operation"] == "memory-test"
    assert runtime_observability._STATE["last_operation_peak_rss_mb"] == 340.0
    assert (
        runtime_observability._STATE["last_operation_peak_cgroup_memory_mb"]
        == 410.0
    )
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["last_operation_peak_rss_mb"] == 340.0
    assert persisted["last_operation_peak_cgroup_memory_mb"] == 410.0
    assert persisted["memory_limit_mb"] == 1024.0


def test_get_cgroup_memory_snapshot_reads_v2_files(monkeypatch, tmp_path):
    (tmp_path / "memory.current").write_text(str(256 * 1024 * 1024))
    (tmp_path / "memory.peak").write_text(str(300 * 1024 * 1024))
    (tmp_path / "memory.stat").write_text(
        f"anon {120 * 1024 * 1024}\nfile {100 * 1024 * 1024}\n"
    )
    (tmp_path / "memory.events").write_text("low 0\noom 2\noom_kill 1\n")
    monkeypatch.setattr(runtime_observability, "_CGROUP_V2_ROOT", tmp_path)

    snapshot = runtime_observability.get_cgroup_memory_snapshot()

    assert snapshot == {
        "current_mb": 256.0,
        "kernel_peak_mb": 300.0,
        "anon_mb": 120.0,
        "file_mb": 100.0,
        "oom": 2,
        "oom_kill": 1,
    }


@pytest.mark.skipif(os.name != "nt", reason="Windows working-set fallback only")
def test_get_rss_mb_works_on_windows():
    rss_mb = runtime_observability.get_rss_mb()

    assert rss_mb is not None
    assert rss_mb > 0


def test_observe_operation_logs_rss_when_available(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(runtime_observability, "_STATE_PATH", tmp_path / "runtime_state.json")
    monkeypatch.setattr(runtime_observability, "get_rss_mb", lambda: 115.0)
    monkeypatch.setattr(
        runtime_observability,
        "get_cgroup_memory_snapshot",
        lambda: {
            "current_mb": None,
            "kernel_peak_mb": None,
            "anon_mb": None,
            "file_mb": None,
            "oom": None,
            "oom_kill": None,
        },
    )
    monkeypatch.setattr(runtime_observability, "get_memory_limit_mb", lambda: None)
    runtime_observability._STATE.clear()

    with caplog.at_level("INFO", logger="shared.runtime_observability"):
        with runtime_observability.observe_operation("pre_start_check"):
            pass

    finished = [
        record.message
        for record in caplog.records
        if "Operation finished name=pre_start_check" in record.getMessage()
    ]
    assert finished
    assert "rss_mb=115.0" in finished[0]
    assert "peak_rss_mb=115.0" in finished[0]
