"""Preview or delete events using explicit source, market and competition filters.

No population is selected by default. Discovery exclusions are opt-in.
--apply commits bounded batches through the existing event deletion writer.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import and_, exists, func, or_, true
from sqlalchemy.orm import aliased

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Competition, Event, EventSourceMapping
from infrastructure.persistence.odds_models import Market
from infrastructure.persistence.repositories.event_deletion_writer import delete_events
from infrastructure.settings import discovery
from modules.sports.catalog import canonical_sport_id, sport_display_name

def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError('must be greater than zero')
    return number


def source_state(value):
    source, separator, state = value.lower().partition('=')
    source, state = source.strip(), state.strip()
    if not separator or not source or state not in {'true', 'false', 'unknown', 'missing'}:
        raise argparse.ArgumentTypeError('expected SOURCE=true|false|unknown|missing')
    return source, state


def category_pair(value):
    sport, separator, category = value.partition('=')
    sport_id = canonical_sport_id(sport.strip())
    category = category.strip()
    if not separator or sport_id is None or not category:
        raise argparse.ArgumentTypeError('expected SPORT=CATEGORY, e.g. football=Romania Amateur')
    return sport_id, category


def source_predicate(source, state):
    mapping = aliased(EventSourceMapping)
    base = and_(mapping.event_id == Event.id, mapping.source == source)
    present = exists().where(base)
    yes = exists().where(base, mapping.has_odds.is_(True))
    if state == 'missing':
        return ~present
    if state == 'true':
        return yes
    if state == 'unknown':
        return ~yes & exists().where(base, mapping.has_odds.is_(None))
    if state == 'false':
        # A false row alone is insufficient if another mapping is true or NULL.
        return present & ~exists().where(base, mapping.has_odds.is_not(False))
    raise ValueError(f'unsupported availability state: {state}')


def discovery_tournament_ids():
    return set(discovery.SOFASCORE.filters.excluded_sofascore_unique_tournament_ids) | set(
        discovery.ODDSPAPI.filters.excluded_sofascore_unique_tournament_ids)


def discovery_category_pairs():
    return set(discovery.SOFASCORE.filters.excluded_categories) | set(
        discovery.ODDSPAPI.filters.excluded_categories)


def category_sport_labels(sport_id):
    """Match the canonical event labels used by discovery category filtering."""
    display = sport_display_name(sport_id)
    labels = {display.casefold()} if display else {sport_id.casefold()}
    if sport_id == 'ice_hockey':
        labels.add('hockey')  # Historical SofaScore event spelling.
    return labels


def build_filters(states=(), competition_ids=(), tournament_ids=(), all_competitions=False,
                  markets='any', category_pairs=()):
    filters = [source_predicate(source, state) for source, state in states]
    if markets not in {'any', 'present', 'absent'}:
        raise ValueError(f'unsupported markets filter: {markets}')
    if markets != 'any':
        has_markets = exists().where(Market.event_id == Event.id)
        filters.append(has_markets if markets == 'present' else ~has_markets)
    population = and_(true(), *filters)
    if all_competitions:
        return population, population
    selectors = []
    if competition_ids:
        selectors.append(Event.competition_id.in_(competition_ids))
    if tournament_ids:
        selectors.append(exists().where(
            Competition.competition_id == Event.competition_id,
            Competition.source == 'sofascore',
            Competition.source_unique_tournament_id.in_(tournament_ids),
        ))
    for sport_id, category_name in category_pairs:
        selectors.append(exists().where(
            Competition.competition_id == Event.competition_id,
            Competition.source == 'sofascore',
            Competition.category_name == category_name,
            func.lower(Event.sport).in_(category_sport_labels(sport_id)),
        ))
    if not selectors:
        raise ValueError('No competition selectors; supply IDs or explicitly use --all-competitions')
    return population, and_(population, or_(*selectors))


def run(manager, *, states=(), competition_ids=(), tournament_ids=(), markets='any',
        all_competitions=False, batch_size=100, limit=None, after_id=0, apply=False,
        category_pairs=()):
    _, predicate = build_filters(states, competition_ids, tournament_ids, all_competitions,
                                 markets, category_pairs)
    with manager.get_session() as session:
        upper_id = session.query(Event.id).order_by(Event.id.desc()).limit(1).scalar()
    print(json.dumps({'mode': 'apply' if apply else 'preview', 'upper_event_id': upper_id,
        'source_states': dict(states), 'markets': markets,
        'competition_ids': sorted(competition_ids), 'source_unique_tournament_ids': sorted(tournament_ids),
        'categories': [list(pair) for pair in sorted(category_pairs)],
        'all_competitions': all_competitions, 'batch_size': batch_size, 'limit': limit,
        'after_id': after_id}), flush=True)
    processed = deleted = batches = scanned = 0
    cursor = after_id
    while upper_id is not None and cursor < upper_id and (limit is None or processed < limit):
        size = batch_size if limit is None else min(batch_size, limit - processed)
        with manager.get_session() as session:
            window = [row[0] for row in session.query(Event.id).filter(
                Event.id > cursor, Event.id <= upper_id,
            ).order_by(Event.id).limit(size).all()]
            if not window:
                break
            ids = [row[0] for row in session.query(Event.id).filter(
                Event.id.in_(window), predicate,
            ).order_by(Event.id).limit(size).all()]
        count = delete_events(manager, ids, event_filter=predicate) if apply and ids else 0
        cursor = window[-1]
        scanned += len(window)
        processed += len(ids)
        deleted += count
        batches += 1
        print(json.dumps({'batch': batches, 'scanned': len(window), 'selected': len(ids), 'deleted': count,
            'last_event_id': cursor, 'event_ids': ids}), flush=True)
    summary = {'scanned': scanned, 'selected': processed, 'deleted': deleted,
        'batches': batches, 'last_event_id': cursor}
    print(json.dumps(summary), flush=True)
    return summary


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-state', type=source_state, action='append',
        help='Repeatable SOURCE=true|false|unknown|missing. Default: no source-state filter.')
    parser.add_argument('--markets', choices=['any', 'absent', 'present'], default='any',
        help='Market presence filter. Default: any (no restriction).')
    parser.add_argument('--competition-id', '--competition-ids', type=positive_int,
        action='extend', nargs='+', default=[], help='Canonical competition IDs to delete (OR tournament selectors)')
    parser.add_argument('--source-unique-tournament-id', '--source-unique-tournament-ids',
        type=positive_int, action='extend', nargs='+', default=[], help='SofaScore uniqueTournament IDs to delete')
    parser.add_argument('--discovery-exclusions', action=argparse.BooleanOptionalAction, default=False,
        help='Add current excluded uniqueTournament IDs from both discovery settings (default: off)')
    parser.add_argument('--discovery-excluded-categories', action=argparse.BooleanOptionalAction, default=False,
        help='Add current excluded (sport, category) pairs from both discovery settings (default: off)')
    parser.add_argument('--category', type=category_pair, action='append', default=[],
        help='Add an exact SPORT=CATEGORY selector; repeat to add pairs')
    parser.add_argument('--all-competitions', action='store_true',
        help='Explicitly select the entire filtered population, ignoring default discovery selectors')
    parser.add_argument('--batch-size', type=positive_int, default=100, help='Maximum IDs per scan/write batch (default: 100)')
    parser.add_argument('--limit', type=positive_int, help='Maximum matching events selected this run')
    parser.add_argument('--after-id', type=int, default=0, help='Resume after this canonical event ID')
    parser.add_argument('--apply', action='store_true', help='Delete selected events; default only previews')
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.after_id < 0:
        parser.error('--after-id must be nonnegative')
    states = args.source_state or ()
    if len(dict(states)) != len(states):
        parser.error('each source may appear only once in --source-state')
    if args.all_competitions and (args.competition_id or args.source_unique_tournament_id or args.category):
        parser.error('--all-competitions cannot be combined with explicit competition or category selectors')
    tournament_ids = set(args.source_unique_tournament_id)
    if args.discovery_exclusions and not args.all_competitions:
        tournament_ids.update(discovery_tournament_ids())
    category_pairs = set(args.category)
    if args.discovery_excluded_categories and not args.all_competitions:
        category_pairs.update(discovery_category_pairs())
    try:
        run(db_manager, states=states, competition_ids=set(args.competition_id),
            tournament_ids=tournament_ids, markets=args.markets, all_competitions=args.all_competitions,
            batch_size=args.batch_size, limit=args.limit, after_id=args.after_id, apply=args.apply,
            category_pairs=category_pairs)
    except ValueError as exc:
        parser.error(str(exc))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
