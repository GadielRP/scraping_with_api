"""Reset legacy Oddspapi availability to unknown for events without markets.

Preview by default. --apply commits one bounded batch at a time. No API calls.
The cutoff concerns mapping creation, not the event kickoff or updated_at.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import exists

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import EventSourceMapping
from infrastructure.persistence.odds_models import Market
from shared.temporal import utc_now


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError('must be greater than zero')
    return number


def utc_datetime(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if result.tzinfo is None:
            raise ValueError('timezone required')
        return result.astimezone(timezone.utc)
    except ValueError as exc:
        raise argparse.ArgumentTypeError('use an ISO datetime with timezone') from exc


def legacy_filter(created_before):
    return (
        (EventSourceMapping.source == 'oddspapi')
        & EventSourceMapping.has_odds.is_(True)
        & (EventSourceMapping.created_at < created_before)
        & ~exists().where(Market.event_id == EventSourceMapping.event_id)
    )


def run(manager, *, created_before, batch_size=100, limit=None, after_id=0, apply=False):
    predicate = legacy_filter(created_before)
    with manager.get_session() as session:
        upper_id = session.query(EventSourceMapping.mapping_id).order_by(
            EventSourceMapping.mapping_id.desc()).limit(1).scalar()
    print(json.dumps({'mode': 'apply' if apply else 'preview', 'upper_mapping_id': upper_id,
        'created_before': created_before.isoformat(), 'batch_size': batch_size,
        'limit': limit, 'after_id': after_id}), flush=True)
    processed = changed = batches = scanned = 0
    cursor = after_id
    while upper_id is not None and cursor < upper_id and (limit is None or processed < limit):
        size = batch_size if limit is None else min(batch_size, limit - processed)
        with manager.get_session() as session:
            window = [row[0] for row in session.query(EventSourceMapping.mapping_id).filter(
                EventSourceMapping.mapping_id > cursor,
                EventSourceMapping.mapping_id <= upper_id,
            ).order_by(EventSourceMapping.mapping_id).limit(size).all()]
            if not window:
                break
            ids = [row[0] for row in session.query(EventSourceMapping.mapping_id).filter(
                EventSourceMapping.mapping_id.in_(window), predicate,
            ).order_by(EventSourceMapping.mapping_id).limit(size).all()]
            affected = 0
            if apply and ids:
                # Re-evaluate availability, cutoff and absence of markets at UPDATE.
                affected = session.query(EventSourceMapping).filter(
                    EventSourceMapping.mapping_id.in_(ids), predicate,
                ).update({'has_odds': None, 'updated_at': utc_now()}, synchronize_session=False)
        cursor = window[-1]
        scanned += len(window)
        processed += len(ids)
        changed += affected
        batches += 1
        print(json.dumps({'batch': batches, 'scanned': len(window), 'selected': len(ids), 'updated': affected,
            'last_mapping_id': cursor}), flush=True)
    summary = {'scanned': scanned, 'selected': processed, 'updated': changed,
        'batches': batches, 'last_mapping_id': cursor}
    print(json.dumps(summary), flush=True)
    return summary


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--created-before', type=utc_datetime,
        default=datetime(2026, 9, 24, tzinfo=timezone.utc),
        help='Exclusive mapping creation cutoff; default: 2026-09-24T00:00:00Z. '
             'Set the actual deployment cutoff if different.')
    parser.add_argument('--batch-size', type=positive_int, default=100, help='Maximum IDs per scan/write batch (default: 100)')
    parser.add_argument('--limit', type=positive_int, help='Maximum matching mappings selected this run')
    parser.add_argument('--after-id', type=int, default=0, help='Resume after this mapping_id')
    parser.add_argument('--apply', action='store_true', help='Write NULL; default only previews')
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.after_id < 0:
        parser.error('--after-id must be nonnegative')
    run(db_manager, **vars(args))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
