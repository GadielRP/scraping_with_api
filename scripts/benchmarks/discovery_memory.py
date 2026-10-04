"""Compare bulk-payload retention with bounded parsing and disk-backed membership.

Synthetic component benchmark; it does not estimate production PostgreSQL refresh memory.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryFile
from time import monotonic
import tracemalloc
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def measure(count, mode):
    from infrastructure.persistence.transient.discovery_run_store import DiscoveryRunStore
    from modules.sofascore.streaming import document_entries
    from shared.batching import chunks
    from shared.runtime_observability import get_rss_mb

    kickoff = datetime(2026, 10, 3, 12, tzinfo=timezone.utc)
    with TemporaryFile() as document:
        document.write(b'{"events":[')
        for index in range(count):
            if index:
                document.write(b",")
            document.write(
                json.dumps(
                    dict(
                        id=index,
                        homeTeam={"id": 1, "name": "Home"},
                        awayTeam={"id": 2, "name": "Away"},
                        status={"type": "notstarted"},
                        metadata="x" * 512,
                    )
                ).encode()
            )
        document.write(b"]}")
        document.seek(0)
        start = monotonic()
        start_rss = get_rss_mb()
        peak_rss = start_rss or 0
        tracemalloc.start()
        with DiscoveryRunStore() as store:
            values = (
                json.load(document)["events"]
                if mode == "materialized"
                else document_entries(document, "events")
            )
            for batch in chunks(values, 100):
                store.record_mapping(
                    {
                        str(row["id"]): SimpleNamespace(
                            id=row["id"], sport="Football", starts_at=kickoff
                        )
                        for row in batch
                    }
                )
                peak_rss = max(peak_rss, get_rss_mb() or 0)
            assert sum(row[2] for row in store.counts()) == count
            _, peak_allocated = tracemalloc.get_traced_memory()
            tracemalloc.stop()
        return dict(
            events=count,
            mode=mode,
            duration_s=round(monotonic() - start, 3),
            peak_python_allocated_mb=round(peak_allocated / 1024**2, 3),
            process_start_rss_mb=start_rss,
            sampled_peak_rss_mb=peak_rss,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int)
    parser.add_argument("--mode", choices=["materialized", "incremental"])
    parser.add_argument("--output", default="scratch/discovery-memory-benchmark.json")
    args = parser.parse_args()
    if args.count:
        print(json.dumps(measure(args.count, args.mode)))
    else:
        rows = []
        for count in (1000, 10000, 50000):
            for mode in ("materialized", "incremental"):
                result = subprocess.run(
                    [sys.executable, __file__, "--count", str(count), "--mode", mode],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                row = json.loads(result.stdout)
                rows.append(row)
                print(json.dumps(row), flush=True)
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(rows, indent=2), encoding="utf-8")
