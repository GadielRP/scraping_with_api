#!/usr/bin/env python3
"""
Timezone utilities for the SofaScore system.
Provides consistent timezone handling across all components.
"""

from datetime import datetime
import pytz
from infrastructure.settings import Config

# Get the configured timezone
TIMEZONE = pytz.timezone(Config.TIMEZONE)


def get_local_now():
    """
    Get current time in Mexico City timezone.
    
    Returns:
        datetime: Current time in local timezone (naive, for database storage)
    """
    return datetime.now(TIMEZONE).replace(tzinfo=None)


def get_local_now_aware():
    """
    Get current time in Mexico City timezone (timezone-aware).
    
    Returns:
        datetime: Current time in local timezone (with timezone info)
    """
    return datetime.now(TIMEZONE)


def get_local_now_iso():
    """
    Get current time in Mexico City timezone as ISO string.
    
    Returns:
        str: Current time in ISO format
    """
    return get_local_now_aware().isoformat()


def convert_utc_to_local(utc_dt, keep_tzinfo=False):
    """
    Convert UTC datetime to local timezone.
    
    Args:
        utc_dt: UTC datetime object
        keep_tzinfo: If True, return timezone-aware datetime; if False, return naive datetime
        
    Returns:
        datetime: Local timezone datetime (naive or aware depending on keep_tzinfo)
    """
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=pytz.UTC)
    local_dt = utc_dt.astimezone(TIMEZONE)
    if keep_tzinfo:
        return local_dt
    return local_dt.replace(tzinfo=None)


def convert_local_to_utc(local_dt):
    """
    Convert local datetime to UTC.
    
    Args:
        local_dt: Local datetime object (naive)
        
    Returns:
        datetime: UTC datetime (naive)
    """
    # Localize to Mexico City timezone
    local_aware = TIMEZONE.localize(local_dt)
    # Convert to UTC
    utc_dt = local_aware.astimezone(pytz.UTC)
    return utc_dt.replace(tzinfo=None)
