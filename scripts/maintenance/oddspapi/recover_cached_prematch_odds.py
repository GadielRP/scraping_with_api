"""Backfill OddsPapi pre-match odds for events that have mainline cache.

Reuses the nightly recovery flow (modules/jobs/oddspapi/prematch_odds_recovery.py):
/historical-odds up to T-5 persists initial, the latest current before the cutoff,
significant changes and the fixed pre-start moments. Only quotes that already exist
in the database are updated; duplicate snapshots are skipped, so reruns are safe.

Without --commit it only lists the selected fixtures (no requests, no writes).

    python scripts/maintenance/oddspapi/recover_cached_prematch_odds.py --from 2026-11-20 --to 2026-11-20
    python scripts/maintenance/oddspapi/recover_cached_prematch_odds.py --event-id 123 --commit
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from itertools import groupby
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from sqlalchemy import and_  # noqa: E402

from app.logging_setup import setup_logging  # noqa: E402
from infrastructure.persistence import database  # noqa: E402
from infrastructure.persistence.models import Event, EventSourceMapping, OddspapiMainlineOutcomeCache  # noqa: E402
from infrastructure.settings import Config  # noqa: E402
from modules.jobs.oddspapi.prematch_odds_recovery import recover_prematch_odds  # noqa: E402
from modules.jobs.pre_start_check_job.providers.oddspapi.constants import RESTRICTED_CAPTURE_MINUTE  # noqa: E402
from shared.temporal import local_day_bounds_utc, utc_now  # noqa: E402


def select_cached_rows(*, from_date=None, to_date=None, event_ids=(), limit=None):
    mapping, cache = EventSourceMapping, OddspapiMainlineOutcomeCache
    with database.db_manager.get_session() as session:
        query = (
            session.query(Event.id, mapping.source_event_id, Event.starts_at, mapping.source_sport_id, cache.bookmaker_slug)
            .select_from(Event)
            .join(mapping, and_(mapping.event_id == Event.id, mapping.source == "oddspapi"))
            .join(cache, and_(cache.event_id == Event.id, cache.fixture_id == mapping.source_event_id,
                              cache.captured_at < Event.starts_at))
            .filter(Event.starts_at <= utc_now() - timedelta(minutes=RESTRICTED_CAPTURE_MINUTE))
        )
        if from_date:
            query = query.filter(Event.starts_at >= local_day_bounds_utc(from_date, Config.TIMEZONE)[0])
        if to_date:
            query = query.filter(Event.starts_at < local_day_bounds_utc(to_date, Config.TIMEZONE)[1])
        if event_ids:
            query = query.filter(Event.id.in_(event_ids))
        rows = query.distinct().order_by(Event.id, mapping.source_event_id).all()
    if limit is not None:
        keep = list(dict.fromkeys(row[0] for row in rows))[:limit]
        rows = [row for row in rows if row[0] in set(keep)]
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--from", dest="from_date", type=date.fromisoformat, help="First kickoff day (local, YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", type=date.fromisoformat, help="Last kickoff day (local, YYYY-MM-DD)")
    parser.add_argument("--event-id", dest="event_ids", type=int, action="append", default=[])
    parser.add_argument("--limit", type=int, help="Process at most N events")
    parser.add_argument("--commit", action="store_true", help="Request /historical-odds and persist")
    args = parser.parse_args(argv)
    setup_logging()

    rows = select_cached_rows(from_date=args.from_date, to_date=args.to_date,
                              event_ids=args.event_ids, limit=args.limit)
    fixtures = [(key, sorted({row[4] for row in group})) for key, group in groupby(rows, key=lambda row: row[:4])]
    for (event_id, fixture_id, kickoff, _sport), books in fixtures:
        print(f"event={event_id} fixture={fixture_id} kickoff={kickoff.isoformat()} cached_bookmakers={','.join(books)}")
    print(f"{len(fixtures)} fixture(s) selected")
    if args.commit:
        recover_prematch_odds(rows)
    elif fixtures:
        print("Dry run: add --commit to request /historical-odds and persist")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
