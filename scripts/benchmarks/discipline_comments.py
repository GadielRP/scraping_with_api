"""Run each mode in a fresh process; tracemalloc measures Python allocations, not RSS.

python -m scripts.benchmarks.discipline_comments --comments 100000 --events 3
python -m scripts.benchmarks.discipline_comments --comments 100000 --mode materialized
"""
import argparse
from contextlib import contextmanager
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import perf_counter
import tracemalloc

from modules.discipline.contracts import DisciplineEventContext, Outcome
from modules.discipline.providers.sofascore import SofaScoreDisciplineProvider


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comments", type=int, default=100000)
    parser.add_argument("--events", type=int, default=1)
    parser.add_argument("--mode", choices=("incremental", "materialized"), default="incremental")
    args = parser.parse_args()
    if args.comments < 1 or args.events < 1:
        parser.error("comments and events must be positive")
    directory = Path("data/runtime")
    directory.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=directory) as body:
        body.write(b'{"comments":[{"type":"matchStarted"},')
        irrelevant = json.dumps(dict(type="shotBlocked", text="irrelevant commentary " * 10)).encode() + b","
        for _ in range(args.comments):
            body.write(irrelevant)
        body.write(b'{"id":1,"type":"yellowCard","isHome":true,"player":{"id":1,"name":"Player"}},'
                   b'{"type":"matchEnded"}]}')
        body.flush()
        size = body.tell()
        class Client:
            @contextmanager
            def open_event_comments(self, _):
                body.seek(0)
                yield body
        provider = SofaScoreDisciplineProvider(Client())
        context = DisciplineEventContext(1, 1, "123", "sofascore", "Football", None, None, None, None)
        tracemalloc.start()
        started, rows = perf_counter(), 0
        for _ in range(args.events):
            if args.mode == "incremental":
                acquisition = provider.acquire(context)
                if acquisition.outcome != Outcome.READY:
                    raise RuntimeError(acquisition.reason)
                rows += len(acquisition.incidents)
                del acquisition
            else:
                # Minimal materialized baseline: decode and count the same explicit card.
                body.seek(0)
                decoded = json.load(body)
                rows += sum(item.get("type") == "yellowCard" for item in decoded["comments"])
                del decoded
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(json.dumps(dict(mode=args.mode, comments=args.comments, events=args.events,
                              response_bytes=size, rows=rows, elapsed_seconds=round(perf_counter() - started, 3),
                              python_peak_bytes=peak)))


if __name__ == "__main__":
    main()
