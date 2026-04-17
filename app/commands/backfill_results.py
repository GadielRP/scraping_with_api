import logging
import subprocess
import sys
from pathlib import Path


_ROOT_DIR = Path(__file__).resolve().parents[2]
_BACKFILL_SCRIPT = _ROOT_DIR / "backfill_results.py"


def run_backfill_results(limit: int = 100):
    """Run backfill results script to collect missing data."""
    logger = logging.getLogger(__name__)
    logger.info(f"Running backfill results (limit: {limit})...")

    try:
        cmd = [sys.executable, str(_BACKFILL_SCRIPT), "--limit", str(limit)]
        logger.info(f"Executing: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
    except Exception as exc:
        logger.error(f"Error running backfill results: {exc}")

