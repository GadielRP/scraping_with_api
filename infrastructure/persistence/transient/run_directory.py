"""Disk scratch ownership: remove stale runs only after acquiring their lease."""

from contextlib import contextmanager
from pathlib import Path
import logging
import os
import shutil
import tempfile
import time

logger = logging.getLogger(__name__)
RUNTIME_DIRECTORY = Path("data/runtime")


@contextmanager
def _lease(path):
    with path.open("a+b") as handle:
        if path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            yield False
            return
        try:
            yield True
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)


class RunDirectory:
    def __init__(self):
        RUNTIME_DIRECTORY.mkdir(parents=True, exist_ok=True)
        self.path = Path(tempfile.mkdtemp(prefix="discovery-", dir=RUNTIME_DIRECTORY)).resolve()
        self._lease = _lease(self.path / ".lease")
        if not self._lease.__enter__():
            raise RuntimeError("Cannot acquire new scratch directory")

    def close(self):
        self._lease.__exit__(None, None, None)
        shutil.rmtree(self.path)


def clean_abandoned_runs(max_age_seconds=86400):
    root = RUNTIME_DIRECTORY.resolve()
    if not root.exists():
        return 0
    removed = 0
    for candidate in root.glob("discovery-*"):
        if candidate.is_symlink() or not candidate.is_dir() or candidate.resolve().parent != root:
            continue
        if time.time() - candidate.stat().st_mtime < max_age_seconds:
            continue
        try:
            with _lease(candidate / ".lease") as acquired:
                if not acquired:
                    continue
            # Only old, inactive directories owned by this storage format qualify.
            shutil.rmtree(candidate)
            removed += 1
        except FileNotFoundError:
            continue
    logger.info("Temporary discovery cleanup removed=%s", removed)
    return removed
