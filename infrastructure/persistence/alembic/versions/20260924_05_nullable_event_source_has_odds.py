"""Make provider odds availability explicitly unknown until checked.

Revision ID: 20260924_05
Revises: 20260924_04
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "20260924_05"
down_revision: Union[str, None] = "20260924_04"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.alter_column(
        "event_source_mappings",
        "has_odds",
        existing_type=sa.Boolean(),
        nullable=True,
        server_default=None,
    )
    # Future mappings may still carry the old default without any odds check.
    # Preserve past-event values and confirmed negatives; only reset future
    # positives, which may be unverified, to unknown.
    op.execute(
        sa.text(
            "UPDATE event_source_mappings AS esm "
            "SET has_odds = NULL "
            "FROM events AS e "
            "WHERE e.id = esm.event_id "
            "  AND esm.has_odds IS TRUE "
            "  AND e.starts_at > CURRENT_TIMESTAMP"
        )
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    # A non-nullable downgrade needs a value for mappings that remain unknown.
    op.execute(
        sa.text(
            "UPDATE event_source_mappings "
            "SET has_odds = TRUE "
            "WHERE has_odds IS NULL"
        )
    )
    op.alter_column(
        "event_source_mappings",
        "has_odds",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=sa.text("true"),
    )
