"""Record the pre-Alembic application schema as a deployment baseline.

Revision ID: 20260918_00
Revises: None
Create Date: 2026-09-18

This revision intentionally contains no DDL. Existing installations must be
verified and stamped at this revision before applying later migrations. It is
not a replacement for creating the historical schema on an empty database.
"""

from __future__ import annotations

from typing import Sequence, Union


revision: str = "20260918_00"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
