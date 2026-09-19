"""Shared SQLAlchemy declarative registry.

Keeping the registry outside any domain model module prevents the database
bootstrap layer from depending on a monolithic ``models.py`` module.  Domain
model modules may import this registry without importing engine/session code.
"""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base shared by every persistence model."""


__all__ = ["Base"]
