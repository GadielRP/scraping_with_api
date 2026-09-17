#!/usr/bin/env python3
"""
Timezone utilities for the SofaScore system.
Provides consistent timezone handling across all components.
"""

from datetime import datetime
import pytz
from infrastructure.settings import Config
from shared.temporal import UTC, as_utc, in_timezone, interpret_local_naive, utc_now

# Get the configured timezone
TIMEZONE = pytz.timezone(Config.TIMEZONE)


def get_local_now():
    """
    Get current time in Mexico City timezone.
    
    Returns:
        datetime: Current time in local timezone (naive, for database storage)
    """
    return get_local_now_aware().replace(tzinfo=None)


def get_local_now_aware():
    """
    Get current time in Mexico City timezone (timezone-aware).
    
    Returns:
        datetime: Current time in local timezone (with timezone info)
    """
    return in_timezone(utc_now(), Config.TIMEZONE)


def get_utc_now():
    """Return the current absolute instant as an aware UTC datetime."""
    return utc_now()


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
    if utc_dt.tzinfo is None or utc_dt.utcoffset() is None:
        utc_dt = utc_dt.replace(tzinfo=UTC)
    local_dt = in_timezone(utc_dt, Config.TIMEZONE)
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
    if local_dt.tzinfo is None or local_dt.utcoffset() is None:
        utc_dt = interpret_local_naive(local_dt, Config.TIMEZONE)
    else:
        utc_dt = as_utc(local_dt)
    return utc_dt.replace(tzinfo=None)
