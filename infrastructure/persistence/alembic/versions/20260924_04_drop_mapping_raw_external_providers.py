"""Drop unused raw provider metadata from event source mappings.

Revision ID: 20260924_04
Revises: 20260924_03
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260924_04"
down_revision: Union[str, None] = "20260924_03"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.drop_column("event_source_mappings", "raw_external_providers")


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.add_column(
        "event_source_mappings",
        sa.Column(
            "raw_external_providers",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
