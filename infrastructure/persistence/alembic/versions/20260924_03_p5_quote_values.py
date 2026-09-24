"""Build P5 historical prices from persisted non-live quote values.

Revision ID: 20260924_03
Revises: 20260924_02
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from infrastructure.persistence.views.p5_price_memory_view import (
    MV_P5_PRICE_MEMORY_INDEXES_SQL,
    build_p5_price_memory_view_sql,
)

revision: str = "20260924_03"
down_revision: Union[str, None] = "20260924_02"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _rebuild_view(*, quote_values_only: bool) -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    bind.execute(
        sa.text("DROP MATERIALIZED VIEW IF EXISTS mv_p5_price_memory CASCADE")
    )
    bind.execute(
        sa.text(
            build_p5_price_memory_view_sql(
                include_quote_fallbacks=False,
                quote_values_only=quote_values_only,
            )
        )
    )
    for index_sql in MV_P5_PRICE_MEMORY_INDEXES_SQL:
        bind.execute(sa.text(index_sql))


def upgrade() -> None:
    _rebuild_view(quote_values_only=True)


def downgrade() -> None:
    # Restore the immediately preceding snapshot-only definition.
    _rebuild_view(quote_values_only=False)
