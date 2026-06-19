"""
Sport Seasons Processing - Generic event extractor for any sport/competition

Usage:
    python sport_seasons_processing.py <season_id> --tournament <tournament_id>
    
Example:
    python sport_seasons_processing.py 65360 --tournament 132  # NBA 2024/2025
    python sport_seasons_processing.py 58766 --tournament 17   # Premier League 2024/2025
    
The script fetches all events for a given season and:
1. Creates/updates events in the database
2. Saves all available betting markets (using MarketRepository)
3. Extracts and saves results for finished events
"""

from modules.sofascore import api_client
from modules.sofascore.event_identity import resolve_sofascore_event_id
from modules.competition.league_config import get_included_season_ids
from infrastructure.persistence.repositories import EventRepository, ResultRepository, MarketRepository
from infrastructure.persistence.models import Event, Result
from infrastructure.persistence.database import db_manager
from shared.timezone_utils import get_local_now
from sqlalchemy import or_
from typing import Dict, List
import logging
import argparse
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

season_to_process = [
    {"season_name": "NBA 25/26", "tournament_id": 132, "season_id": 80229},
    {"season_name": "NBA 24/25", "tournament_id": 132, "season_id": 65360},
    {"season_name": "NBA 23/24", "tournament_id": 132, "season_id": 54105},
    {"season_name": "NBA 22/23", "tournament_id": 132, "season_id": 45096},
    {"season_name": "NBA 21/22", "tournament_id": 132, "season_id": 38191},
    {"season_name": "NBA 20/21", "tournament_id": 132, "season_id": 34951},
    {"season_name": "Laliga 2025", "tournament_id": 8, "season_id": 77559},
    {"season_name": "Laliga 2024", "tournament_id": 8, "season_id": 61643},
    {"season_name": "Laliga 2023", "tournament_id": 8, "season_id": 52376},
    {"season_name": "Laliga 2022", "tournament_id": 8, "season_id": 42409},
    {"season_name": "Laliga 2021", "tournament_id": 8, "season_id": 37223},
    {"season_name": "Laliga 2020", "tournament_id": 8, "season_id": 32501},
    {"season_name": "Premier League 2025", "tournament_id": 17, "season_id": 76986},
    {"season_name": "Premier League 2024", "tournament_id": 17, "season_id": 61627},
    {"season_name": "Premier League 2023", "tournament_id": 17, "season_id": 52186},
    {"season_name": "Premier League 2022", "tournament_id": 17, "season_id": 41886},
    {"season_name": "Premier League 2021", "tournament_id": 17, "season_id": 37036},
    {"season_name": "Premier League 2020", "tournament_id": 17, "season_id": 29415},
    {"season_name": "NFL 2025", "tournament_id": 9464, "season_id": 75522},
    {"season_name": "NFL 2024", "tournament_id": 9464, "season_id": 60592},
    {"season_name": "NFL 2023", "tournament_id": 9464, "season_id": 51361},
    {"season_name": "NFL 2022", "tournament_id": 9464, "season_id": 46786},
    {"season_name": "NFL 2021", "tournament_id": 9464, "season_id": 36422},
    {"season_name": "NFL 2020", "tournament_id": 9464, "season_id": 27719},
    {"season_name": "MLB 2026", "tournament_id": 11205, "season_id": 84695},
    {"season_name": "MLB 2025", "tournament_id": 11205, "season_id": 68611},
    {"season_name": "MLB 2026", "tournament_id": 11205, "season_id": 84695},
    {"season_name": "MLB 2024", "tournament_id": 11205, "season_id": 57577},
    {"season_name": "NHL 2025", "tournament_id": 234, "season_id": 78476},
    {"season_name": "Serie A 2025", "tournament_id": 23, "season_id": 76457},
    {"season_name": "Bundesliga 2025", "tournament_id": 35, "season_id": 77333},
    {"season_name": "League 1 2025", "tournament_id": 34, "season_id": 77356},
    {"season_name": "Saudi Pro League 2025", "tournament_id": 955, "season_id": 80443},
    {"season_name": "SHL 2025", "tournament_id": 261, "season_id": 75679},
    {"season_name": "PFL 2025", "tournament_id": 1654, "season_id": 81520},
    {"season_name": "CBA 2025", "tournament_id": 1566, "season_id": 85375},
    {"season_name": "LPF Argentina Apertura 2026", "tournament_id": 155, "season_id": 87913},
    {"season_name": "LPF Argentina Clausura 2025", "tournament_id": 155, "season_id": 77826},
    {"season_name": "LPF Argentina Apertura 2025", "tournament_id": 155, "season_id": 70268},
    
]


def initialize_database():
    """
    Initialize database schema - creates tables and applies migrations.
    
    Steps:
    1. Test database connection
    2. Create tables if they don't exist
    3. Run schema migrations (adds missing columns like season_id, round, etc.)
    """
    logger.info("Initializing database schema...")
    
    try:
        if not db_manager.test_connection():
            logger.error("❌ Database connection failed - cannot proceed")
            return False
        
        logger.info("Creating database tables if they don't exist...")
        db_manager.create_tables()
        
        logger.info("Checking and applying schema migrations...")
        if not db_manager.check_and_migrate_schema():
            logger.warning("⚠️ Schema migration check failed, but continuing...")
        
        logger.info("✅ Database schema initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize database: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def deduplicate_event_payloads(events: List[Dict]) -> tuple[List[Dict], list[int]]:
    by_id: Dict[int, Dict] = {}
    duplicate_ids: list[int] = []

    for event_data in events:
        event_payload = event_data.get("event", event_data) if event_data else {}
        event_id = event_payload.get("id")
        if not event_id:
            continue

        if event_id in by_id:
            duplicate_ids.append(event_id)

        by_id[event_id] = event_data

    return list(by_id.values()), duplicate_ids


def describe_event_status(raw_event: Dict) -> str:
    status = raw_event.get("status") or {}
    code = status.get("code")
    status_type = status.get("type")
    description = status.get("description")
    return f"code={code}, type={status_type}, description={description}"


def should_delete_canceled_event(raw_event: Dict) -> bool:
    status = raw_event.get("status") or {}
    status_type = str(status.get("type") or "").lower().strip()
    status_description = str(status.get("description") or "").lower().strip()

    return status_type in {"canceled", "cancelled"} or status_description in {"canceled", "cancelled"}


def get_processing_season_ids(source_unique_tournament_id: int, season_id: int) -> tuple[int, ...]:
    included_season_ids = get_included_season_ids(
        source_unique_tournament_id=source_unique_tournament_id,
        source_tournament_id=None,
        season_id=season_id,
    )
    if not included_season_ids:
        return (int(season_id),)
    return tuple(int(value) for value in included_season_ids)


def get_existing_season_event_ids_from_db(season_ids: tuple[int, ...]) -> set[int]:
    if not season_ids:
        return set()

    with db_manager.get_session() as session:
        rows = session.query(Event.id).filter(Event.season_id.in_(season_ids)).all()
        return {row[0] for row in rows if row[0]}


def get_season_events_missing_from_view(
    season_ids: tuple[int, ...],
    cutoff_time=None,
) -> list[int]:
    if not season_ids:
        return []

    with db_manager.get_session() as session:
        query = (
            session.query(Event.id)
            .outerjoin(Result, Result.event_id == Event.id)
            .filter(Event.season_id.in_(season_ids))
            .filter(
                or_(
                    Result.event_id.is_(None),
                    Result.home_score.is_(None),
                    Result.away_score.is_(None),
                    Event.home_participant_id.is_(None),
                    Event.away_participant_id.is_(None),
                    Event.competition_id.is_(None),
                )
            )
        )

        if cutoff_time is not None:
            query = query.filter(Event.start_time_utc <= cutoff_time)

        rows = query.order_by(Event.start_time_utc).all()
        return [row[0] for row in rows if row[0]]


def reconcile_existing_season_events(
    season_ids: tuple[int, ...],
    fetched_event_ids: set[int],
    canceled_event_ids_to_delete: set[int],
    cutoff_time=None,
) -> dict:
    if cutoff_time is None:
        cutoff_time = get_local_now()

    candidate_ids = get_season_events_missing_from_view(
        season_ids=season_ids,
        cutoff_time=cutoff_time,
    )
    existing_season_event_ids = get_existing_season_event_ids_from_db(season_ids)
    candidate_ids = [event_id for event_id in candidate_ids if event_id in existing_season_event_ids]

    logger.info(
        "Reconciliation scope: season_ids=%s | cutoff_time=%s | candidates=%s",
        season_ids,
        cutoff_time,
        len(candidate_ids),
    )

    reconciled_metadata = 0
    results_inserted_or_updated = 0
    still_pending = 0
    failed_direct_fetch_count = 0

    for event_id in candidate_ids:
        try:
            if event_id not in fetched_event_ids:
                logger.info("Reconciling season event %s not returned by current season endpoint", event_id)

            sofascore_event_id = resolve_sofascore_event_id(event_id)
            response = api_client._request_json(f"/event/{sofascore_event_id}", no_retry_on_404=True)
            if not response or "event" not in response:
                failed_direct_fetch_count += 1
                logger.warning("Could not fetch direct event response for reconciliation event %s", event_id)
                continue

            raw_event = response["event"]
            event_data = api_client.get_event_information(raw_event, discovery_source="scraping_on_command")
            event_payload = dict(event_data.get("event", event_data))
            event_payload.pop("discovery_source", None)
            event_data["event"] = event_payload
            if EventRepository.upsert_event(event_data):
                reconciled_metadata += 1

            result_data = api_client.extract_results_from_response({"event": raw_event})

            if result_data and result_data.get("_canceled"):
                if should_delete_canceled_event(raw_event):
                    canceled_event_ids_to_delete.add(event_id)
                    logger.info(
                        "Queued canceled reconciliation event %s for batch deletion. status=%s",
                        event_id,
                        describe_event_status(raw_event),
                    )
                else:
                    still_pending += 1
                    logger.info(
                        "Reconciliation event %s has no result but is not canceled. status=%s",
                        event_id,
                        describe_event_status(raw_event),
                    )
                continue

            if result_data:
                if ResultRepository.upsert_result(event_id, result_data):
                    results_inserted_or_updated += 1
            else:
                still_pending += 1
                logger.info(
                    "Reconciliation event %s has no finished result yet. status=%s",
                    event_id,
                    describe_event_status(raw_event),
                )
        except Exception as exc:
            failed_direct_fetch_count += 1
            logger.error("Error reconciling event %s: %s", event_id, exc)

    return {
        "reconciled_metadata": reconciled_metadata,
        "results_inserted_or_updated": results_inserted_or_updated,
        "queued_for_deletion": len(canceled_event_ids_to_delete),
        "still_pending": still_pending,
        "failed_direct_fetch_count": failed_direct_fetch_count,
    }


def fetch_season_events(tournament_id: int, season_id: int) -> List[Dict]:
    """
    Fetch all events for a given season by incrementing fetch_number until 404.
    Works for any sport/competition.
    
    Args:
        tournament_id: The SofaScore unique tournament ID (e.g., 132 for NBA, 17 for Premier League)
        season_id: The SofaScore season ID
    
    Returns:
        List of event dictionaries extracted from the API responses
    """
    all_events = []
    fetch_number = 0
    
    logger.info(f"Starting to fetch events for tournament {tournament_id}, season {season_id}")
    
    #set to True for full season, set currently to 5 for latest 5 batches per season.
    while True:
        endpoint = f"/unique-tournament/{tournament_id}/season/{season_id}/events/last/{fetch_number}"
        
        
        # Use the safe request facade so 404s still end the fetch loop cleanly.
        response = api_client._request_json(endpoint, no_retry_on_404=True)
        
        if not response:
            logger.info(f"No more events found (received 404 or error). Total batches fetched: {fetch_number}")
            break
        
        # Extract events from response
        if 'events' in response:
            batch_events = response['events']
            logger.info(f"Batch {fetch_number}: Found {len(batch_events)} events")
            
            # Process each event through get_event_information
            for event in batch_events:
                try:
                    event_data = api_client.get_event_information(event, discovery_source='scraping_on_command')
                    event_payload = event_data.get('event', event_data)
                    
                    # Validate required fields
                    required_fields = ['id', 'slug', 'startTimestamp', 'sport', 'competition', 'homeTeam', 'awayTeam']
                    if all(event_payload.get(field) for field in required_fields):
                        # Store the raw event for later result extraction (avoids redundant API calls)
                        event_data['_raw_event'] = event
                        all_events.append(event_data)
                    else:
                        logger.warning(f"Event {event.get('id')} missing required fields, skipping")
                        
                except Exception as e:
                    logger.error(f"Error processing event in batch {fetch_number}: {e}")
                    continue
        else:
            logger.warning(f"Batch {fetch_number}: No 'events' key in response")
        
        fetch_number += 1
    
    logger.info(f"Fetching complete. Total events extracted: {len(all_events)}")
    return all_events

def process_season(tournament_id: int, season_id: int):
    """
    Fetch and process all events for a given season.
    Works for any sport/competition.
    
    Uses the new market-based odds flow (MarketRepository) to save ALL available
    betting markets, not just 1X2 odds.
    
    Args:
        tournament_id: The SofaScore unique tournament ID
        season_id: The SofaScore season ID
    """
    logger.info("=" * 80)
    logger.info(f"Processing Season: Tournament {tournament_id}, Season {season_id}")
    logger.info("=" * 80)

    # In this script, tournament_id is the SofaScore uniqueTournament.id used by /unique-tournament/{id}/...
    processing_season_ids = get_processing_season_ids(
        source_unique_tournament_id=tournament_id,
        season_id=season_id,
    )
    logger.info(
        "Season scope resolved: requested season_id=%s | processing_season_ids=%s",
        season_id,
        processing_season_ids,
    )

    # Step 1: Fetch all events for every included season in the bundle.
    events = []
    for scoped_season_id in processing_season_ids:
        logger.info(
            "Fetching events for scoped season_id=%s under unique_tournament_id=%s",
            scoped_season_id,
            tournament_id,
        )
        scoped_events = fetch_season_events(tournament_id, scoped_season_id)
        logger.info(
            "Fetched %s entries for scoped season_id=%s",
            len(scoped_events),
            scoped_season_id,
        )
        events.extend(scoped_events)

    if not events:
        logger.warning(f"No events found for tournament {tournament_id}, season {season_id}")
        return

    raw_count = len(events)
    events, duplicate_ids = deduplicate_event_payloads(events)
    logger.info(
        "Fetched season entries: %s | unique events: %s | duplicate entries removed: %s",
        raw_count,
        len(events),
        len(duplicate_ids),
    )
    if duplicate_ids:
        logger.warning("Duplicate event IDs returned by season endpoint: %s", sorted(set(duplicate_ids)))

    # Step 2: Sort events by startTimestamp (oldest to newest)
    events.sort(key=lambda x: x.get('event', x)['startTimestamp'])
    logger.info("Events sorted by start timestamp (oldest to newest)")

    fetched_event_ids = {
        event_data.get("event", event_data).get("id")
        for event_data in events
        if event_data.get("event", event_data).get("id")
    }

    # Step 3: Process events individually and fetch odds for each
    logger.info(f"Starting to process {len(events)} events with discovery_source='scraping_on_command'")

    processed_count = 0
    skipped_count = 0
    markets_processed_count = 0
    markets_skipped_count = 0
    results_processed_count = 0
    canceled_event_ids_to_delete = set()

    for event_data in events:
        try:
            # Upsert event
            event = EventRepository.upsert_event(event_data)

            if event:
                processed_count += 1
                event_payload = event_data.get('event', event_data)
                sofascore_event_id = event_payload['id']
                event_id = event.id
                logger.debug(
                    f"Event sofascore_event_id={sofascore_event_id} upserted as canonical_event_id={event_id}: {event_payload.get('homeTeam')} vs {event_payload.get('awayTeam')}"
                )

                # Check if event already has markets stored before fetching odds
                existing_market_count = MarketRepository.get_market_count(event_id)

                if existing_market_count > 0:
                    # Event already has markets stored, skip odds fetching
                    markets_skipped_count += 1
                    logger.debug(f"Event {event_id} already has {existing_market_count} markets stored, skipping odds fetch")
                else:
                    # Fetch and save ALL markets using MarketRepository (new flow)
                    try:
                        final_odds_response = api_client.get_event_final_odds(sofascore_event_id, event_payload.get('slug'))

                        if final_odds_response:
                            # Save all markets using the new market-based flow
                            saved_markets = MarketRepository.save_markets_from_response(event_id, final_odds_response, bookie_id=1)
                            if saved_markets > 0:
                                markets_processed_count += 1
                                logger.debug(f"Saved {saved_markets} markets for event {event_id}")
                            else:
                                logger.debug(f"No markets saved for event {event_id}")
                        else:
                            logger.debug(f"No odds response for event {event_id}")
                    except Exception as e:
                        logger.error(f"Error processing markets for event {event_id}: {e}")

                # Extract and upsert results for this event (using already-fetched data)
                try:
                    raw_event = event_data.get('_raw_event')
                    if raw_event:
                        result_data = api_client.extract_results_from_response({'event': raw_event})

                        if result_data and result_data.get('_canceled'):
                            if should_delete_canceled_event(raw_event):
                                canceled_event_ids_to_delete.add(event_id)
                                logger.info(
                                    "Queued canceled event %s for batch deletion. status=%s",
                                    event_id,
                                    describe_event_status(raw_event),
                                )
                            else:
                                logger.info(
                                    "Event %s has no result but will NOT be deleted because it is not canceled. status=%s",
                                    event_id,
                                    describe_event_status(raw_event),
                                )
                            continue

                        if result_data:
                            if ResultRepository.upsert_result(event_id, result_data):
                                results_processed_count += 1
                                logger.debug(
                                    "Results upserted for event %s: %s-%s",
                                    event_id,
                                    result_data.get('home_score'),
                                    result_data.get('away_score'),
                                )
                            else:
                                logger.warning(f"Failed to upsert results for event {event_id}")
                        else:
                            logger.debug(f"No results data for event {event_id} (may not be finished)")
                    else:
                        logger.warning(f"No raw event data for event {event_id}")
                except Exception as e:
                    logger.error(f"Error processing results for event {event_id}: {e}")
            else:
                skipped_count += 1
                event_payload = event_data.get('event', event_data)
                logger.warning(f"Failed to upsert event {event_payload.get('id')}")

        except Exception as e:
            skipped_count += 1
            event_payload = event_data.get('event', event_data)
            logger.error(f"Error processing event {event_payload.get('id')}: {e}")
            continue

    reconciliation_cutoff_time = get_local_now()
    reconciliation_stats = reconcile_existing_season_events(
        season_ids=processing_season_ids,
        fetched_event_ids=fetched_event_ids,
        canceled_event_ids_to_delete=canceled_event_ids_to_delete,
        cutoff_time=reconciliation_cutoff_time,
    )

    canceled_events_deleted = 0
    if canceled_event_ids_to_delete:
        canceled_events_deleted = EventRepository.batch_delete_events(sorted(canceled_event_ids_to_delete))
        logger.info(
            "Deleted %s canceled events in batch: %s",
            canceled_events_deleted,
            sorted(canceled_event_ids_to_delete),
        )

    logger.info("=" * 80)
    logger.info(f"Season Processing Complete! (Tournament: {tournament_id}, Season: {season_id})")
    logger.info("requested_season_id: %s", season_id)
    logger.info("processing_season_ids: %s", processing_season_ids)
    logger.info("reconciliation_cutoff_time: %s", reconciliation_cutoff_time)
    logger.info("fetched_entries_raw: %s", raw_count)
    logger.info("fetched_unique_events: %s", len(events))
    logger.info("duplicate_entries_removed: %s", len(duplicate_ids))
    logger.info("processed_count: %s", processed_count)
    logger.info("skipped_count: %s", skipped_count)
    logger.info("markets_processed_count: %s", markets_processed_count)
    logger.info("markets_skipped_count: %s", markets_skipped_count)
    logger.info("results_processed_count: %s", results_processed_count)
    logger.info("reconciliation_metadata_updated: %s", reconciliation_stats["reconciled_metadata"])
    logger.info("reconciliation_results_upserted: %s", reconciliation_stats["results_inserted_or_updated"])
    logger.info("reconciliation_pending_not_finished: %s", reconciliation_stats["still_pending"])
    logger.info("canceled_events_queued: %s", len(canceled_event_ids_to_delete))
    logger.info("canceled_events_deleted: %s", canceled_events_deleted)
    logger.info("failed_direct_fetch_count: %s", reconciliation_stats["failed_direct_fetch_count"])
    logger.info("=" * 80)

def main():
    """
    Main CLI interface for season processing.
    
    Usage:
        python sport_seasons_processing.py <season_id> --tournament <tournament_id>
        
    Examples:
        python sport_seasons_processing.py 65360 -t 132      # NBA 2024/2025
        python sport_seasons_processing.py 58766 -t 17       # Premier League 2024/2025
        python sport_seasons_processing.py 61644 -t 8        # La Liga 2024/2025
    
    Common tournament IDs:
        NBA: 132
        Premier League: 17
        La Liga: 8
        Serie A: 23
        Bundesliga: 35
        Ligue 1: 34
        Champions League: 7
        Euroleague: 138
    """
    parser = argparse.ArgumentParser(
        description='Extract and process season events from SofaScore API (any sport)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python sport_seasons_processing.py 65360 -t 132    # NBA 2024/2025
  python sport_seasons_processing.py 58766 -t 17     # Premier League 2024/2025
  
Common tournament IDs:
  NBA: 132 | Premier League: 17 | La Liga: 8 | Serie A: 23
  Bundesliga: 35 | Ligue 1: 34 | Champions League: 7 | Euroleague: 138
        """
    )
    parser.add_argument(
        'season_id',
        type=int,
        nargs='?',
        help='SofaScore season ID to process'
    )
    parser.add_argument(
        '-t', '--tournament',
        type=int,
        required=False,
        help='SofaScore unique tournament ID (e.g., 132 for NBA, 17 for Premier League)'
    )
    parser.add_argument(
        '--auto',
        action='store_true',
        help='Automatically process all seasons defined in season_to_process list'
    )
    parser.add_argument(
        '--season_name',
        type=str,
        help='Process a specific season by name from the season_to_process list'
    )
    parser.add_argument(
        '--skip-db-init',
        action='store_true',
        help='Skip database initialization (use only if schema is up to date)'
    )
    
    args = parser.parse_args()
    
    # Initialize database schema first (unless skipped)
    if not args.skip_db_init:
        if not initialize_database():
            logger.error("Database initialization failed. Use --skip-db-init to bypass.")
            return
    else:
        logger.warning("⚠️ Skipping database initialization - ensure schema is up to date!")
    
    # Logic for different execution modes
    if args.auto:
        logger.info(f"Starting auto-processing for {len(season_to_process)} seasons...")
        for i, season in enumerate(season_to_process):
            t_id = season['tournament_id']
            s_id = season['season_id']
            s_name = season['season_name']
            
            logger.info(f"[{i+1}/{len(season_to_process)}] Auto-processing: {s_name} (ID: {s_id}, Tournament: {t_id})")
            process_season(t_id, s_id)
            
            if i < len(season_to_process) - 1:
                logger.info("Waiting 60 seconds before next season to avoid detection...")
                time.sleep(60)
        
        logger.info("✅ Auto-processing complete!")

    elif args.season_name:
        # Find matching season
        matched_season = next((s for s in season_to_process if s['season_name'].lower() == args.season_name.lower()), None)
        
        if matched_season:
            logger.info(f"Found match for season name: {matched_season['season_name']}")
            process_season(matched_season['tournament_id'], matched_season['season_id'])
        else:
            logger.error(f"❌ No season found with name: '{args.season_name}' in season_to_process list.")
            logger.info("Available seasons:")
            for s in season_to_process:
                logger.info(f" - {s['season_name']}")
    
    elif args.season_id and args.tournament:
        # Process the single season provided via CLI
        process_season(args.tournament, args.season_id)
    
    else:
        logger.error("❌ Missing arguments. Provide EITHER (--auto) OR (--season_name NAME) OR (season_id -t tournament_id)")
        parser.print_help()


if __name__ == "__main__":
    main()
