"""Compatibility facade for historical standings and form helpers.

The real implementation now lives in:
- standings_rules.py
- standings_engine.py
- historical_form_service.py
"""

from datetime import datetime

from infrastructure.persistence.database import db_manager
from sqlalchemy import text

from .constants import *  # noqa: F401,F403
from .historical_form_service import *  # noqa: F401,F403
from .standings_engine import *  # noqa: F401,F403
from .standings_rules import *  # noqa: F401,F403
