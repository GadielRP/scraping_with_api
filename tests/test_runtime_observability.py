import json
import os
import time

import pytest

import shared.runtime_observability as runtime_observability




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


