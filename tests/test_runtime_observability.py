import json
import time

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
