"""OddsPortal worker helpers for the pre-start job."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def start_oddsportal_worker(*_args, **_kwargs):
    """OddsPortal worker is currently disabled; keep the hook in place."""
    logger.info("OddsPortal worker hook invoked, but scraping is disabled in the refactored flow.")
    return None
