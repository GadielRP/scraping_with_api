"""Backfill OddsPapi pre-match odds for events that have mainline cache.

Reuses the nightly recovery flow (modules/jobs/oddspapi/prematch_odds_recovery.py):
/historical-odds up to kickoff replaces, for every existing quote it returns, the
initial, the current, and all snapshots (the opening, fixed PRE_START_ODDS_MOMENTS,
and significant changes). Quotes are never created. Quotes the response does not
return keep their old data and are listed in
data/backfills/oddspapi_prematch_odds/<run>_untouched_quotes.jsonl.

Deleted snapshots cannot be restored: take a backup first
(scripts/operations/canonical/backup_server.py).

Without --commit it only lists the selected fixtures (no requests, no writes).

    python -m scripts.maintenance.oddspapi.recover_cached_prematch_odds --from 2026-11-20 --to 2026-11-20
    python -m scripts.maintenance.oddspapi.recover_cached_prematch_odds --event-id 123 --commit
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from itertools import groupby
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from sqlalchemy import and_, case, func  # noqa: E402

from app.logging_setup import setup_logging  # noqa: E402
from infrastructure.persistence import database  # noqa: E402
from infrastructure.persistence.models import (  # noqa: E402
    Bookie, Event, EventSourceMapping, Market, MarketChoice, MarketChoiceQuote, MarketChoiceSnapshot,
    OddspapiMainlineOutcomeCache,
)
from infrastructure.settings import Config  # noqa: E402
from modules.jobs.oddspapi.prematch_odds_recovery import recover_prematch_odds  # noqa: E402
from shared.temporal import local_day_bounds_utc, utc_now  # noqa: E402

REPORT_DIRECTORY = REPOSITORY_ROOT / "data" / "backfills" / "oddspapi_prematch_odds"


def select_cached_rows(*, from_date=None, to_date=None, event_ids=(), limit=None):
    mapping, cache = EventSourceMapping, OddspapiMainlineOutcomeCache
    with database.db_manager.get_session() as session:
        query = (
            session.query(Event.id, mapping.source_event_id, Event.starts_at, mapping.source_sport_id, cache.bookmaker_slug)
            .select_from(Event)
            .join(mapping, and_(mapping.event_id == Event.id, mapping.source == "oddspapi"))
            .join(cache, and_(cache.event_id == Event.id, cache.fixture_id == mapping.source_event_id,
                              cache.captured_at < Event.starts_at))
            .filter(Event.starts_at <= utc_now())
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


def untouched_quotes(event_ids, run_started_at):
    """Quotes with old snapshots that got nothing in this run, from bookmakers that did."""
    snapshot = MarketChoiceSnapshot
    fresh = func.sum(case((snapshot.collected_at >= run_started_at, 1), else_=0))
    with database.db_manager.get_session() as session:
        rows = (
            session.query(Market.event_id, Bookie.slug, MarketChoiceQuote.quote_id,
                          MarketChoiceQuote.source_market_id, MarketChoiceQuote.source_outcome_id,
                          MarketChoiceQuote.exchange_side, MarketChoiceQuote.exchange_level,
                          MarketChoiceQuote.main_line, func.count(snapshot.snapshot_id), fresh)
            .select_from(MarketChoiceQuote)
            .join(MarketChoice, MarketChoice.choice_id == MarketChoiceQuote.choice_id)
            .join(Market, Market.market_id == MarketChoice.market_id)
            .join(Bookie, Bookie.bookie_id == Market.bookie_id)
            .join(snapshot, snapshot.quote_id == MarketChoiceQuote.quote_id)
            .filter(Market.event_id.in_(event_ids), Market.is_live.is_(False),
                    MarketChoiceQuote.source == "oddspapi")
            .group_by(Market.event_id, Bookie.slug, MarketChoiceQuote.quote_id)
            .order_by(Market.event_id, Bookie.slug, MarketChoiceQuote.quote_id)
            .all()
        )
    refreshed = {(row[0], row[1]) for row in rows if row[9]}
    return [row for row in rows if not row[9] and (row[0], row[1]) in refreshed]


def write_untouched_report(rows, fixtures_by_event, run_started_at):
    path = REPORT_DIRECTORY / f"{run_started_at:%Y%m%dT%H%M%S}_untouched_quotes.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event_id, slug, quote_id, market_id, outcome_id, side, level, main_line, snapshots, _ in rows:
            handle.write(json.dumps({
                "event_id": event_id, "fixture_id": ",".join(fixtures_by_event.get(event_id, ())),
                "quote_id": quote_id, "bookmaker": slug, "source_market_id": market_id,
                "source_outcome_id": outcome_id, "exchange_side": side, "exchange_level": level,
                "main_line": main_line, "snapshots": snapshots,
            }) + "\n")
    return path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--from", dest="from_date", type=date.fromisoformat, help="First kickoff day (local, YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", type=date.fromisoformat, help="Last kickoff day (local, YYYY-MM-DD)")
    parser.add_argument("--event-id", dest="event_ids", type=int, action="append", default=[])
    parser.add_argument("--limit", type=int, help="Process at most N events")
    parser.add_argument("--commit", action="store_true", help="Request /historical-odds and replace stored odds")
    args = parser.parse_args(argv)
    setup_logging()

    rows = select_cached_rows(from_date=args.from_date, to_date=args.to_date,
                              event_ids=args.event_ids, limit=args.limit)
    fixtures = [(key, sorted({row[4] for row in group})) for key, group in groupby(rows, key=lambda row: row[:4])]
    for (event_id, fixture_id, kickoff, _sport), books in fixtures:
        print(f"event={event_id} fixture={fixture_id} kickoff={kickoff.isoformat()} cached_bookmakers={','.join(books)}")
    print(f"{len(fixtures)} fixture(s) selected")
    if not args.commit:
        if fixtures:
            print("Dry run: add --commit to request /historical-odds and replace stored odds")
        return 0
    run_started_at = utc_now()
    recover_prematch_odds(rows)
    fixtures_by_event = {}
    for (event_id, fixture_id, _kickoff, _sport), _books in fixtures:
        fixtures_by_event.setdefault(event_id, []).append(fixture_id)
    untouched = untouched_quotes(list(fixtures_by_event), run_started_at)
    if untouched:
        path = write_untouched_report(untouched, fixtures_by_event, run_started_at)
        print(f"{len(untouched)} quote(s) not returned by /historical-odds kept their old data: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
