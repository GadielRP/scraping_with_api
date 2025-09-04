import logging
from datetime import datetime
from typing import Dict, Optional, Tuple, Any, List

from sofascore_api import api_client
from repository import EventRepository, OddsRepository
from odds_utils import fractional_to_decimal


logger = logging.getLogger(__name__)


# Fixed cutoff per user requirement
CUTOFF_DATETIME = datetime(2025, 9, 3, 14, 0, 0)


def _fetch_event_final_odds(event_id: int, slug: str) -> Optional[Dict[str, Any]]:
    """
    Fetch odds for an event using the existing API client (which already handles
    session, headers, proxy, rate limiting, and retries).

    Returns the response JSON dict or None if unavailable.
    """
    try:
        return api_client.get_event_final_odds(event_id, slug)
    except Exception as e:
        logger.error(f"Error fetching final odds for event {event_id}: {e}")
        return None


def _extract_open_and_final_odds(response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract BOTH opening and final odds from /event/{id}/odds/1/all response.
    Sport-agnostic: adapts to 2-choice, 3-choice, or multi-choice markets.

    Returns a dict that includes:
    - one_open, x_open, two_open
    - one_final, x_final, two_final
    - one_cur, x_cur, two_cur (aliases for final, to enable complete snapshots)
    """
    try:
        if not response or 'markets' not in response:
            logger.warning("No markets found in odds response")
            return None

        for market in response['markets']:
            choices = market.get('choices', [])
            if not choices:
                continue

            # Build maps for initial and current odds keyed by choice name order
            initial_map: Dict[str, Optional[Any]] = {}
            current_map: Dict[str, Optional[Any]] = {}
            ordered_names: List[str] = []

            for choice in choices:
                name = choice.get('name')
                if not name:
                    continue
                initial_fractional = choice.get('initialFractionalValue')
                current_fractional = choice.get('fractionalValue')
                initial_map[name] = fractional_to_decimal(initial_fractional) if initial_fractional else None
                current_map[name] = fractional_to_decimal(current_fractional) if current_fractional else None
                ordered_names.append(name)

            # Decide mapping based on number of available choices
            num = len(ordered_names)
            if num < 2:
                continue

            def pick(index: int, value_map: Dict[str, Optional[Any]]):
                key = ordered_names[index]
                return value_map.get(key)

            if num == 3:
                result = {
                    'one_open': pick(0, initial_map),
                    'x_open': pick(1, initial_map),
                    'two_open': pick(2, initial_map),
                    'one_final': pick(0, current_map),
                    'x_final': pick(1, current_map),
                    'two_final': pick(2, current_map),
                }
            elif num == 2:
                result = {
                    'one_open': pick(0, initial_map),
                    'x_open': None,
                    'two_open': pick(1, initial_map),
                    'one_final': pick(0, current_map),
                    'x_final': None,
                    'two_final': pick(1, current_map),
                }
            else:  # num > 3
                result = {
                    'one_open': pick(0, initial_map),
                    'x_open': None,
                    'two_open': pick(1, initial_map),
                    'one_final': pick(0, current_map),
                    'x_final': None,
                    'two_final': pick(1, current_map),
                }

            # Also provide cur aliases to enable "complete" snapshot path
            result.update({
                'one_cur': result.get('one_final'),
                'x_cur': result.get('x_final'),
                'two_cur': result.get('two_final'),
            })

            return result

        logger.warning("No suitable market found for odds extraction")
        return None

    except Exception as e:
        logger.error(f"Error extracting open/final odds: {e}")
        return None


def run_final_odds_all():
    """
    Fetch and update final odds for all finished events up to the cutoff.
    - Select finished events from DB
    - Apply cutoff: only events with start_time_utc <= CUTOFF_DATETIME
    - For each event: fetch odds, extract open+final, upsert finals (and opening if provided),
      and create a snapshot (complete if open+cur present)
    - Sequential processing with built-in API rate limiting
    - Skip/log 404s
    """
    event_repo = EventRepository()
    odds_repo = OddsRepository()

    logger.info("Starting final-odds-all collection")

    events = event_repo.get_all_finished_events()
    if not events:
        logger.info("No finished events found")
        return

    # Apply cutoff filter
    targets = [e for e in events if e.start_time_utc <= CUTOFF_DATETIME]
    if not targets:
        logger.info("No finished events before cutoff to process")
        return

    logger.info(f"Processing {len(targets)} finished events (cutoff: {CUTOFF_DATETIME})")

    updated = 0
    skipped = 0
    failed = 0

    for idx, event in enumerate(targets, start=1):
        try:
            logger.info(f"[{idx}/{len(targets)}] Fetching final odds for event {event.id}: {event.home_team} vs {event.away_team}")

            data = _fetch_event_final_odds(event.id, event.slug)
            if not data:
                logger.warning(f"Skipping event {event.id}: final odds unavailable (possibly 404 or temporary issue)")
                skipped += 1
                continue

            # Extract opening and final odds in a sport-agnostic way
            odds = _extract_open_and_final_odds(data)
            if not odds:
                logger.warning(f"No extractable odds for event {event.id}")
                failed += 1
                continue

            # Upsert finals (and opening if present — harmless since opening is immutable)
            upsert_id = odds_repo.upsert_event_odds(event.id, odds)
            if not upsert_id:
                logger.error(f"Failed to upsert odds for event {event.id}")
                failed += 1
                continue

            # Create snapshot: providing open + cur enables complete snapshot path
            snapshot = odds_repo.create_odds_snapshot(event.id, odds)
            if snapshot:
                logger.info(f"✅ Updated odds and created snapshot for event {event.id}")
            else:
                logger.warning(f"⚠️ Odds updated but snapshot not created for event {event.id}")

            updated += 1

        except Exception as e:
            logger.error(f"Error processing event {event.id}: {e}")
            failed += 1

    logger.info(f"Final-odds-all completed: {updated} updated, {skipped} skipped, {failed} failed")


# Convenience alias for main entry
def run():
    run_final_odds_all()


