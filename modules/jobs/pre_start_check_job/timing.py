"""Timing helpers for the pre-start job."""

from __future__ import annotations

import logging
from datetime import datetime

from infrastructure.settings import Config
from shared.timezone_utils import get_local_now_aware, TIMEZONE

logger = logging.getLogger(__name__)


def minutes_until_start(start_time_utc) -> int:
    """Calculate minutes until event start."""
    if start_time_utc is None:
        return 0

    if start_time_utc.tzinfo is None:
        start_local = TIMEZONE.localize(start_time_utc)
    else:
        start_local = start_time_utc

    now = get_local_now_aware()
    return round((start_local - now).total_seconds() / 60)


def minutes_since_start(start_time_utc) -> int:
    """Calculate minutes since event start as a negative number."""
    return minutes_until_start(start_time_utc)


def should_extract_odds_for_event(event_id: int, minutes_until: int, event_start_time: datetime = None):
    """Determine whether odds should be extracted at this moment."""
    from modules.oddsportal.oddsportal_config import SEASON_ODDSPORTAL_MAP
    from modules.sofascore import api_client

    if not Config.ENABLE_ODDS_EXTRACTION:
        logger.info(f"🚫 ODDS EXTRACTION DISABLED: Skipping odds extraction for event {event_id}")
        return False, None, False

    key_moments = [120, 30, 5, 0, -5]
    if minutes_until not in key_moments:
        logger.debug(
            f"⏭️ Not a key moment for event {event_id}: {minutes_until} minutes until start - SKIPPING API CALL AND ODDS EXTRACTION"
        )
        return False, None, False

    if not Config.ENABLE_TIMESTAMP_CORRECTION:
        logger.info(f"🎯 Key moment detected for event {event_id}: {minutes_until} minutes until start - WILL EXTRACT ODDS")
        return True, None, False

    is_timing_consistent, metadata_snapshot = api_client.get_event_results(
        event_id,
        update_time=True,
        return_snapshot=True,
        current_start_time=event_start_time,
        minutes_until_start=minutes_until,
    )

    if is_timing_consistent is None:
        logger.warning(f"⏭️ API error for event {event_id} - skipping odds extraction")
        return False, None, False

    if is_timing_consistent:
        logger.info(f"✅ Timing verified for event {event_id} ({minutes_until}m until start). Proceeding with odds extraction.")
        return True, metadata_snapshot, False

    logger.info(f"🔄 [TIME UPDATE] Timing mismatch detected for event {event_id}. DB corrected.")
    return False, metadata_snapshot, True
