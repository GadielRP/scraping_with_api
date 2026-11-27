"""Preview or delete event source mappings using an explicit provider selection.

Choose --source to delete those providers, or --keep-source to delete all others.
No provider selection is supplied by default. Only event_source_mappings is
modified; events, participants, results and odds remain intact.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import EventSourceMapping


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError('must be greater than zero')
    return number


def source_name(value):
    source = value.strip().lower()
    if not source:
        raise argparse.ArgumentTypeError('source must not be blank')
    return source


def run(manager, *, sources=(), keep_sources=(), batch_size=100, limit=None,
        after_id=0, apply=False):
    sources = sorted({source_name(value) for value in sources})
    keep_sources = sorted({source_name(value) for value in keep_sources})
    if bool(sources) == bool(keep_sources):
        raise ValueError('Specify sources to delete OR keep_sources to preserve')
    if batch_size < 1 or (limit is not None and limit < 1) or after_id < 0:
        raise ValueError('batch_size/limit must be positive and after_id nonnegative')
    mapping = EventSourceMapping
    predicate = mapping.source.in_(sources) if sources else ~mapping.source.in_(keep_sources)
    with manager.get_session() as session:
        upper_id = session.query(mapping.mapping_id).order_by(
            mapping.mapping_id.desc()).limit(1).scalar()
    print(json.dumps({'mode': 'apply' if apply else 'preview',
        'upper_mapping_id': upper_id, 'sources': sources, 'keep_sources': keep_sources,
        'batch_size': batch_size, 'limit': limit, 'after_id': after_id}), flush=True)
    scanned = selected = deleted = batches = 0
    cursor = after_id
    while upper_id is not None and cursor < upper_id and (limit is None or selected < limit):
        size = batch_size if limit is None else min(batch_size, limit - selected)
        with manager.get_session() as session:
            window = [row[0] for row in session.query(mapping.mapping_id).filter(
                mapping.mapping_id > cursor, mapping.mapping_id <= upper_id,
            ).order_by(mapping.mapping_id).limit(size).all()]
            if not window:
                break
            ids = [row[0] for row in session.query(mapping.mapping_id).filter(
                mapping.mapping_id.in_(window), predicate,
            ).order_by(mapping.mapping_id).limit(size).all()]
            affected = 0
            if apply and ids:
                # Recheck the source in the DELETE, and touch no parent rows.
                affected = session.query(mapping).filter(
                    mapping.mapping_id.in_(ids), predicate,
                ).delete(synchronize_session=False)
        cursor = window[-1]
        scanned += len(window)
        selected += len(ids)
        deleted += affected
        batches += 1
        print(json.dumps({'batch': batches, 'scanned': len(window),
            'selected': len(ids), 'deleted': affected, 'last_mapping_id': cursor}), flush=True)
    summary = {'scanned': scanned, 'selected': selected, 'deleted': deleted,
        'batches': batches, 'last_mapping_id': cursor}
    print(json.dumps(summary), flush=True)
    return summary


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument('--source', '--sources', dest='sources', type=source_name,
        action='extend', nargs='+', help='Delete only these sources; repeatable, no default')
    selection.add_argument('--keep-source', '--keep-sources', dest='keep_sources', type=source_name,
        action='extend', nargs='+', help='Keep these sources and delete all others; no default')
    parser.add_argument('--batch-size', type=positive_int, default=100,
        help='Maximum mapping IDs per scan/write batch (default: 100)')
    parser.add_argument('--limit', type=positive_int,
        help='Maximum matching mappings selected this run; default: no total limit')
    parser.add_argument('--after-id', type=int, default=0,
        help='Resume after this mapping_id (not event_id)')
    parser.add_argument('--apply', action='store_true',
        help='Commit deletions per batch; default only previews')
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.after_id < 0:
        parser.error('--after-id must be nonnegative')
    run(db_manager, sources=args.sources or (), keep_sources=args.keep_sources or (),
        batch_size=args.batch_size, limit=args.limit, after_id=args.after_id, apply=args.apply)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
